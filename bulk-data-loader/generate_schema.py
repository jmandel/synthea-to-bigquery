import os
import glob
import ujson as json
import gzip

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
        definitions['edges'][r['resource']['id']]['contained'] =  {'definition': 'string', 'documentation': 'string'}
        #definitions['edges'][r['resource']['id']]['meta'] =  {'definition': 'Meta', 'documentation': 'Meta'}

    return definitions

def schema_for(conformance, path="Patient", depth=0, stack=None):
    if stack == None:
        stack = []
    assert depth < 10, stack

    ret = []

    if len(stack) > len(set(stack)) + 1:
        return ret

    if 'Extension' in stack and len(stack) > len(set(stack)):
        return ret

    if stack[-2:] == ['Reference', 'Identifier']:
        return ret

    for ename, deets in conformance['edges'][path].items():
        #print path, ename, bool(deets), deets
        edoc = conformance['paths'][deets['documentation']]
        edef = conformance['paths'][deets['definition']]
        if ename in ['extension', 'modifierExtension'] and 'Extension' in stack:
            continue

        field = {
            'mode': "NULLABLE",
            'name': ename,
            'type': self.type_for(conformance, edef)
        }

        if field['type'] == 'RECORD':
            sub_fields = schema_for(conformance, edef['path'], depth+1, stack + [path])
            if sub_fields:
                field['fields'] = sub_fields
        ret += [field]
    return ret

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
        for k, v in resource.items():
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
            if s.startswith("_"):
                prev_edges = edges
                fhir_path = 'BackboneElement'
                edges = self.conformance['edges']['BackboneElement']
            else:
                prev_edges = edges
                fhir_path = edges[s]['documentation']
                edges = self.conformance['edges'][edges[s]['definition']]
        return prev_edges, s

    def is_repeated(self, edges, last_segment):
        if last_segment == 'resourceType':
            return False
        if last_segment.startswith("_"):
            last_segment = last_segment[1:]
        return self.conformance['paths'][edges[last_segment]['documentation']]['max'] != '1'

    def definition_for(self, edges, last_segment):
        if last_segment.startswith("_"):
            return "BackboneElement"
        return edges[last_segment]['definition']

    def description_for(self, edges, last_segment):
        if last_segment.startswith("_"):
            return "Container"
        return self.conformance['paths'][edges[last_segment]['documentation']]['short']

    def generate_schema(self, segments):
        assert type(segments) == list
        ret = [{
            'mode': "REPEATED" if self.is_repeated(edges, last_segment) else "NULLABLE",
            'name': last_segment,
            'type': self.type_for(self.conformance, {'path': self.definition_for(edges, last_segment)}),
            'description': self.description_for(edges, last_segment),
            'fields': self.generate_schema(segments + [last_segment])
        } for edges, last_segment in self.reachable_from(segments)] or []

        for r in ret:
            if not r['fields']:
                del r['fields']
        return ret

