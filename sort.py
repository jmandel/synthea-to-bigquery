import os
import glob
import ujson as json
import gzip

numfile = 0
handles = {}

def get_handle(t):
    if t not in handles:
        handles[t] = gzip.open('sorted/'+t+'.ndjson.gz', 'a')
    return handles[t]

def close_handles():
    hkeys = list(handles.keys())
    for t in hkeys:
        handles[t].close()
        del handles[t]

for segment in sorted(glob.glob('synthea_1m_fhir_3_0_May_24/*.tar.gz')):
    segment_dir = segment.split("/")[1].rsplit("_", 1)[0]
    print "Try", segment, segment_dir
    if not os.path.exists(segment_dir):
        os.system("tar -xzf %s  --wildcards --no-anchored '*.json'"%(segment))
    for patient_file in  glob.glob('%s/fhir/*.json'%(segment_dir)):
        numfile += 1
        with open(patient_file) as one_file:
            for e in json.load(one_file).get('entry', []):
                r = e['resource']
                get_handle(r['resourceType']).write(json.dumps(r) + '\n')
        if numfile%1000 == 0:
            print numfile
    os.system("rm -rf %s"%segment_dir)
    close_handles()
