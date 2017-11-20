import os
import glob
import ujson as json
import gzip

numfile = 0
handles = {}

def load_definitions(definitions=None, basepath=""):
    if definitions == None:
        definitions = {
            'paths': {},
            'edges': {},
            'primitives': {}
        }

    entries = []
    for filename in ["profiles-resources.json", "profiles-types.json"]:
        entries.extend(json.load(open(basepath+filename))['entry'])

    for r in entries:
        if not r['fullUrl'].startswith("http://hl7.org/fhir/StructureDefinition/"):
            continue

        if r['resource']['type'] != r['resource']['id']: # Avoid profiles on datatypes (SimpleQuantiy)
            continue


        for e in r['resource']['snapshot']['element']:
            assert e['path'] not in definitions['paths'], "Repeat of %s"%e['path']

            definitions['paths'][e['path']] =  e

            if '.' not in e['path']:
                continue

            prefix, common_suffix = e['path'].rsplit('.', 1)

            if prefix not in definitions['edges']:
                definitions['edges'][prefix] = {}
            if e.get('contentReference'):
                definitions['edges'][prefix][common_suffix] = {'definition': e['contentReference'][1:], 'documentation': e['path']}
            for t in e.get('type', []):
                if '_code' in t: # primitive type -- get JSON representation!:
                    #print "PATH", e['path']
                    primitive_name = e['path'].split(".value")[0]
                    definitions['primitives'][primitive_name] = [
                        e for e in t['_code']['extension']
                        if e['url'] == 'http://hl7.org/fhir/StructureDefinition/structuredefinition-json-type'
                    ][0]['valueString']
                    continue
                suffix = common_suffix.replace("[x]", t['code'][0].upper() + t['code'][1:])
                if t['code'] in ['Element', 'BackboneElement']:
                    definitions['edges'][prefix][suffix] = {'definition': e['path'], 'documentation': e['path']}
                else:
                    definitions['edges'][prefix][suffix] = {'definition': t['code'], 'documentation': e['path']}
        definitions['edges'][r['resource']['id']]['resourceType'] =  {'definition': 'string', 'documentation': 'string'}
        #definitions['edges'][r['resource']['id']]['meta'] =  {'definition': 'Meta', 'documentation': 'Meta'}

    return definitions

class PathTracer():
    def __init__(self, conformance=None):
        self.conformance = conformance or load_definitions()
        self.schema = { }
        self.paths = set()

    def type_for(self, conformance, edef):
        if edef['path'] in conformance['primitives']:
            return {
                "uuid": "STRING",
                "code": "STRING",
                "instant": "TIMESTAMP",
                "string": "STRING",
                "markdown": "STRING",
                "decimal": "FLOAT",
                "oid": "STRING",
                "uri": "STRING",
                "dateTime": "STRING",
                "base64Binary": "STRING",
                "boolean": "BOOLEAN",
                "time": "STRING",
                "date": "STRING",
                "integer": "INTEGER",
                "xhtml": "STRING",
                "positiveInt": "INTEGER",
                "id": "STRING",
                "unsignedInt": "NUMBER"
            }[edef['path']]
        return "RECORD"

    def digest_helper(self, resource, stack=None):
        for k, v in resource.iteritems():
            self.paths |= set(['.'.join(stack) + '.' + k])
            decent_targets = v if type(v) == list else [v]
            for t in decent_targets:
                if type(t) == dict:
                    self.digest_helper(t, stack + [k])

    def digest(self, resource):
        self.digest_helper(resource,[resource['resourceType']])

    def reachable_from(self, segments):
        path = '.'.join(segments)
        return [
          self.fhir_path_for(p.split('.'))
          for p in self.paths if p.startswith(path + '.') and len(p[len(path)+1:].split('.')) == 1]

    def fhir_path_for(self, segments):
        fhir_path = None
        prev_edges = None
        edges = self.conformance['edges'][segments[0]]
        for s in segments[1:]:
            fhir_path = edges[s]['documentation']
            prev_edges = edges
            edges = self.conformance['edges'][edges[s]['definition']]
        return prev_edges, s, fhir_path

    def is_repeated(self, fhir_path):
        return self.conformance['paths'][fhir_path]['max'] != '1'

    def generate_schema(self, segments):
        assert type(segments) == list
        ret = [{
            'mode': "REPEATED" if self.is_repeated(target_path) else "NULLABLE",
            'name': last_segment,
            'type': self.type_for(self.conformance, {'path': edges[last_segment]['definition']}),
            'description': self.conformance['paths'][edges[last_segment]['documentation']]['short'],
            'fields': self.generate_schema(segments + [last_segment])
        } for edges, last_segment, target_path in self.reachable_from(segments)] or None

        for r in ret:
            if not r['fields']:
                del r['fields']
        return ret

def get_handle(t):
    if t not in handles:
        handles[t] = gzip.open('prepared/'+t+'.ndjson.gz', 'a')
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

tracer = PathTracer()
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
                tracer.digest(r)
                get_handle(r['resourceType']).write(remap(json.dumps(r), idmap) + '\n')

        if numfile%1000 == 0:
            print "Done", numfile

    os.system("rm -rf %s"%segment_dir)
    close_handles()

for r in set([p.split(".")[0] for p in tracer.paths]):
    schema = tracer.generate_schema([r])
    with open('prepared/%s.schema.json'%r, 'w') as schema_file:
        json.dump(schema, schema_file, indent=2)

