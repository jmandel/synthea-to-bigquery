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
from datetime import datetime, timedelta
import secrets
from multiprocessing import Pool, TimeoutError
import tempfile
import os
import jwt

SERVERS = json.load(open('config/servers.json'))
SIGNING_KEY = open('config/signing-key.private.pem').read()
ISSUER = os.environ.get('ISSUER', 'https://bulk-data-loader.smarthealthit.org')


def get_security_headers(server_authorization):
    if server_authorization['type'] != 'smart-backend-services':
        return {}
    return {
        'Authorization': 'Bearer ' + get_access_token(server_authorization)
    }


def run_command(cmd):
    print(cmd)
    os.system(cmd)


def digest_and_sink(request, tracer, bucket, sink_file):
    count = 0
    print("Digesting", sink_file)
    with tempfile.NamedTemporaryFile('w') as one_file:
        for l in request.iter_lines():
            count += 1
            try:
                resource = json.loads(l)
            except:
                print("Failed to prase line", count - 1, l)
            if 'contained' in resource:
                resource['contained'] = [json.dumps(
                    r) for r in resource['contained']]
            tracer.digest(resource)
            if count % 1000 == 0:
                print("On line %s" % count)
                import sys
                sys.stdout.flush()
            one_file.write(json.dumps(resource) + "\n")
        run_command("gsutil -m cp %s %s/%s" %
                    (one_file.name, bucket, sink_file))
        print("Done file", sink_file)


def process_resource_type(gcs_bucket, bigquery_dataset, resource_type, resource_links, server_authorization):
    print("Start", resource_type)
    tracer = generate_schema.PathTracer()
    resource_count = 0

    for link in resource_links:
        print("Fetch link", link)
        resource_filename = '%08d-%s.ndjson' % (resource_count, resource_type)
        resource_count += 1
        resource_request = requests.get(link, stream=True, headers={
            **{'Accept': 'application/ndjson'},
            **get_security_headers(server_authorization)
        })
        digest_and_sink(resource_request, tracer,
                        gcs_bucket, resource_filename)

    print("Digested", resource_type)

    with tempfile.NamedTemporaryFile('w', delete=False) as tabledef_file:
        print("tmp file", resource_type, tabledef_file.name)
        tabledef = {
            'schema': {
                'fields': tracer.generate_schema([resource_type])
            },
            'autodetect': False,
            'sourceFormat': 'NEWLINE_DELIMITED_JSON',
            'sourceUris': [gcs_bucket + '/*' + resource_type + ".ndjson"]
        }
        tabledef_str = json.dumps(tabledef, indent=2).replace(
            "\/", "/")  # BQ doesn't like valid JSON strings :/
        tabledef_file.write(tabledef_str)
        tabledef_file.flush()

        table_name = "%s.%s" % (bigquery_dataset, resource_type)
        run_command("bq rm -f " + table_name)
        run_command("bq mk --external_table_definition=%s %s" %
                    (tabledef_file.name, table_name))
    print("End", resource_type)


def make_authn_jwt(server_authorization):
    return jwt.encode({
        'iss': ISSUER,
        'sub': server_authorization['client_id'],
        'aud': server_authorization['token_uri'],
        'exp': time.mktime((datetime.now() + timedelta(days=5)).timetuple()),
        'jti': secrets.token_hex(32)
    }, SIGNING_KEY, 'RS256')


def get_access_token(server_authorization):
    jwt = make_authn_jwt(server_authorization)
    token = requests.post(server_authorization['token_uri'], data={
        'scope': 'system/*.read',
        'grant_type': 'client_credentials',
        'client_assertion_type': 'urn:ietf:params:oauth:client-assertion-type:jwt-bearer',
        'client_assertion': jwt
    })
    print("Got access token", token.json())
    return token.json()['access_token']


def do_sync(fhir_server, gcs_bucket, bigquery_dataset, pool_size, server_authorization):
    wait = requests.get(url=fhir_server + "/Patient/$everything", headers={**{
        "Prefer": "respond-async",
        "Accept": "application/fhir+json"
    }, **get_security_headers(server_authorization)})

    print(wait.headers)
    poll_url = wait.headers["Content-Location"]
    print("Got poll url", poll_url)

    links = []
    slept_for = 0
    while True:
        done = requests.get(poll_url, headers={
            **{'Accept': 'application/json'},
            **get_security_headers(server_authorization)
        })
        print(done.status_code, done.text, done.headers)
        if done.status_code == 200:
            links = done.json().get('output', [])
            break
        slept_for += 2
        time.sleep(2)

    run_command("gsutil mb %s" % (gcs_bucket))
    run_command("bq mk %s" % (bigquery_dataset))

    links_by_resource = groupby(
        sorted(links, key=lambda r: r['type']), lambda r: r['type'])

    with Pool(processes=pool_size) as pool:
        for resource_type, resource_links in links_by_resource:
            pool.apply_async(process_resource_type, (
                gcs_bucket,
                bigquery_dataset,
                resource_type,
                list([l['url'] for l in resource_links]),
                server_authorization))
        pool.close()
        pool.join()


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
        description='Load bulk data into BigQuery')
    parser.add_argument(
        '--source', help='Source shortname from config', required=True)
    parser.add_argument(
        '--gcs-bucket', help='Google Cloud Storage bucket', required=True)
    parser.add_argument('--bigquery-dataset',
                        help='BigQuery Data Set', required=True)
    parser.add_argument('--parallelism', type=int,
                        help='Number of resource types to process in parallel', default=4, required=True)

    args = parser.parse_args()

    if not args.gcs_bucket.startswith("gs://"):
        args.gcs_bucket = "gs://" + args.gcs_bucket

    print("Connecting to resolve metadata")
    server = [s for s in SERVERS if s['shortname'] == args.source][0]
    server_url = server['fhir_base_uri']
    print("Trying to connect", server_url + '/metadata')
    while True:
        try:
            metadata = requests.get(server_url + '/metadata', headers={
                'Accept': 'application/fhir+json'}).json()
            break
        except Exception as e:
            print("Trying to connect", server_url + '/metadata', e)
            time.sleep(1)
    print("Connected")

    print("Syncing", metadata.keys())
    if server['authorization']['type'] == 'smart-backend-services':
        smart_extension = [e for e in metadata['rest'][0]['security']['extension'] if e['url'] ==
                           'http://fhir-registry.smarthealthit.org/StructureDefinition/oauth-uris'][0]
        token_uri = [e for e in smart_extension['extension']
                     if e['url'] == 'token'][0]['valueUri']
        server['authorization']['token_uri'] = token_uri
    print("Authz", server)
    do_sync(server_url, args.gcs_bucket, args.bigquery_dataset,
            args.parallelism, server['authorization'])
