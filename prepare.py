import os
import glob
import ujson as json
import gzip

numfile = 0
handles = {}

def get_handle(t):
    if t not in handles:
        handles[t] = gzip.open('ndjson/'+t+'.ndjson.gz', 'a')
    return handles[t]

def close_handles():
    hkeys = list(handles.keys())
    for t in hkeys:
        handles[t].close()
        del handles[t]

def remap(s, idmap):
    for k, v in idmap.iteritems():
        s = s.replace('"%s"'%k,'"%s"'%v)
    return s

for segment in sorted(glob.glob('synthea_1m_fhir_3_0_May_24/*.tar.gz')):
    segment_dir = segment.split("/")[1].rsplit("_", 1)[0]
    print "Try", segment, segment_dir
    if not os.path.exists(segment_dir):
        os.system("tar -xzf %s  --wildcards --no-anchored '*.json'"%(segment))
    for patient_file in  glob.glob('%s/fhir/*.json'%(segment_dir)):
        numfile += 1

        with open(patient_file) as one_file:
            json_file = json.load(one_file)

            idmap = {
                e['fullUrl']: e['resource']['resourceType'] + '/' + e['resource']['id']
                for e in json_file.get('entry', []) if 'id' in e['resource']
            }

            for e in json_file.get('entry', []):
                r = e['resource']
                get_handle(r['resourceType']).write(remap(json.dumps(r), idmap) + '\n')

        if numfile%1000 == 0:
            print "Done", numfile

    os.system("rm -rf %s"%segment_dir)
    close_handles()


