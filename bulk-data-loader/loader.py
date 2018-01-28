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

def digest_and_sink(request, tracer, bucket, sink_file):
    count=0
    proc = subprocess.Popen(['gsutil', 'cp', '-', bucket + '/' + sink_file],
                            stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE,)
    for l in request.iter_lines():
        count += 1
        resource = json.loads(l)
        if 'contained' in resource:
            resource['contained'] = [json.dumps(r) for r in resource['contained']]
        tracer.digest(resource)
        if count%1000 == 0:
            print("On line %s"%count)
        proc.stdin.write(json.dumps(resource).encode() + b"\n")
    #for line in proc.stdout:
    #    sys.stdout.write(line)
    proc.stdin.close()
    proc.wait()


def process_resource_type(gcs_bucket, bigquery_dataset, resource_type, resource_links):
    tracer = generate_schema.PathTracer()
    resource_count = 0
    print("Start", resource_type)
    for link in resource_links:
        print("Fetch link", link)
        resource_filename = '%08d-%s.ndjson'%(resource_count, resource_type)
        resource_count += 1
        resource_request = requests.get(link, stream=True)
        print("made req to link", link)
        digest_and_sink(resource_request, tracer, gcs_bucket, resource_filename)
    print("Done", resource_type)
    with tempfile.NamedTemporaryFile('w', delete=False) as tabledef_file:
        print("tmp file", resource_type, tabledef_file.name)
        tabledef = {
            'schema': {
                'fields': tracer.generate_schema([resource_type])
            },
            'autodetect': False,
            'sourceFormat': 'NEWLINE_DELIMITED_JSON',
            'sourceUris': [ gcs_bucket + '/*' + resource_type + ".ndjson"]
            }
        tabledef_str = json.dumps(tabledef, indent=2).replace("\/", "/") # BQ doesn't like valid JSON strings :/
        #print("TAble def", tabledef)
        tabledef_file.write(tabledef_str)
        tabledef_file.flush()

        print("wrote to file", resource_type, tabledef_file.name)
        os.system("cp %s /tmp/tabledef"%tabledef_file.name)

        #print(tabledef_str)
        table_name = "%s.%s"%(bigquery_dataset,resource_type)

        cmd_str = "bq rm -f " + table_name
        print(cmd_str)
        os.system(cmd_str)

        cmd_str = "bq mk --external_table_definition=%s %s"%(tabledef_file.name, table_name)
        print(cmd_str)
        os.system(cmd_str)

    print("End", resource_type)

def do_sync(fhir_server, gcs_bucket, bigquery_dataset, pool_size):
    wait = requests.get(url=fhir_server+"/Patient/$everything", headers={
            "Prefer": "respond-async",
            "Accept": "application/fhir+json"
        })

    print(wait.headers)
    poll_url = wait.headers["Content-Location"]
    print("Got poll url", poll_url)

    links = []
    while True:
        done = requests.get(poll_url)
        if done.status_code == 200:
            links = done.json().get('output', [])
            break
        time.sleep(2)


    links_by_resource = groupby(sorted(links, key=lambda r: r['type']), lambda r: r['type'])

    cmd_str = "gsutil mb %s"%(gcs_bucket)
    print(cmd_str)
    os.system(cmd_str)

    cmd_str = "bq mk %s"%(bigquery_dataset)
    print(cmd_str)
    os.system(cmd_str)

    with Pool(processes=pool_size) as pool:
        resource_counts = {}
        for resource_type, resource_links in links_by_resource:
            #if resource_type != "Patient": continue
            #process_resource_type(gcs_bucket, bigquery_dataset, resource_type, list([l['url'] for l in resource_links]))
            pool.apply_async(process_resource_type, (gcs_bucket, bigquery_dataset, resource_type, list([l['url'] for l in resource_links])))
            #print(resource_type)
        pool.close()
        pool.join()



if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Load bulk data into BigQuery')
    parser.add_argument('--fhir-server', help='FHIR server base url', required=True)
    parser.add_argument('--gcs-bucket', help='Google Cloud Storage bucket', required=True)
    parser.add_argument('--bigquery-dataset', help='BigQuery Data Set', required=True)
    parser.add_argument('--parallelism', type=int, help='Number of resource types to process in parallel', default=4, required=True)
    args = parser.parse_args()
    if not args.gcs_bucket.startswith("gs://"):
        args.gcs_bucket = "gs://" + args.gcs_bucket
    do_sync(args.fhir_server, args.gcs_bucket, args.bigquery_dataset, args.parallelism)
