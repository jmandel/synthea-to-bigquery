import requests
import re
import ujson as json
import time
import generate_schema
import subprocess
import threading
import sys
import tempfile
from itertools import groupby

from multiprocessing import Pool, TimeoutError
import tempfile
import os

BUCKET = 'gs://fhir-bulk-data'
#BASE="http://test.fhir.org/r3"
BASE="https://bulk-data.smarthealthit.org/eyJlcnIiOiIiLCJwYWdlIjoxMDAwLCJkdXIiOjAsInRsdCI6MTUsIm0iOjF9/fhir"

wait = requests.get(url=BASE+"/Patient/$everything", headers={
        "Prefer": "respond-async",
        "Accept": "application/fhir+ndjson"
    })

poll_url = wait.headers["Content-Location"]


links = []
while True:
    done = requests.get(poll_url).headers
    if 'Link' in done:
        links = [re.findall("<(.*)?>", l)[0] for l in done['Link'].split(",") if "rel=item" in l or "rel=" not in l]
        break
    time.sleep(2)


def resource_for_link(l):
    return re.findall(".*?([A-Z][A-z]+)\.ndjson", l)[0]

def digest_and_sink(request, tracer, bucket, sink_file):
    proc = subprocess.Popen(['gsutil', 'cp', '-', bucket + '/' + sink_file],
                            stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE,)
    for l in request.iter_lines():
        resource = json.loads(l)
        if 'contained' in resource:
            resource['contained'] = [json.dumps(r) for r in resource['contained']]
        tracer.digest(resource)
        proc.stdin.write(json.dumps(resource).encode() + b"\n")
    #for line in proc.stdout:
    #    sys.stdout.write(line)
    proc.stdin.close()
    proc.wait()

links_by_resource = groupby(sorted(links, key=resource_for_link), resource_for_link)

def process_resource_type(resource_type, resource_links):
    tracer = generate_schema.PathTracer()
    resource_count = 0
    print("Start", resource_type)
    for link in resource_links:
        resource_filename = '%08d-%s.ndjson'%(resource_count, resource_type)
        resource_count += 1
        resource_request = requests.get(link, stream=True)
        digest_and_sink(resource_request, tracer, BUCKET, resource_filename)
    print("Done", resource_type)
    with tempfile.NamedTemporaryFile('w', delete=False) as tabledef_file:
        print("tmp file", resource_type, fp.name)
        tabledef = {
            'schema': {
                'fields': tracer.generate_schema([resource_type])
            },
            'autodetect': False,
            'sourceFormat': 'NEWLINE_DELIMITED_JSON',
            'sourceUris': [ BUCKET + '/*' + resource_type + ".ndjson"]
            }
        tabledef_str = json.dumps(tabledef, indent=2).replace("\/", "/") # BQ doesn't like valid JSON strings :/
        #print("TAble def", tabledef)
        tabledef_file.write(tabledef_str)
        tabledef_file.flush()

        print("wrote to file", resource_type, fp.name)
        os.system("cp %s /tmp/tabledef"%tabledef_file.name)

        #print(tabledef_str)
        table_name = "bulk_data_test_ds.%s"%resource_type
        
        cmd_str = "bq rm " + table_name
        print(cmd_str) 
        os.system(cmd_str)

        cmd_str = "bq mk --external_table_definition=%s %s"%(tabledef_file.name, table_name)
        print(cmd_str) 
        os.system(cmd_str)
        
    print("End", resource_type)


with Pool(processes=16) as pool:
    resource_counts = {}
    for resource_type, resource_links in links_by_resource:
        #if resource_type != "Patient": continue
        pool.apply_async(process_resource_type, (resource_type, list(resource_links)))
        #print(resource_type)
    pool.close()
    pool.join()

