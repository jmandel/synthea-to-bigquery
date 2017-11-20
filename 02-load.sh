#!/bin/bash

gsutil cp -r sorted/* gs://fhir-synthetic-ma/2017-05-24/

bq mk synthea_test

for resource in $(gsutil ls gs://fhir-synthetic-ma/2017-05-24 | egrep -o '[A-Z][^.]+')
do
    echo "Load $resource"
    pushd schema
    python create_schema ${resource} > ${resource}.schema.json
    popd

    bq load --replace  --source_format=NEWLINE_DELIMITED_JSON \
        synthea.${resource} gs://fhir-synthetic-ma/2017-05-24/${resource}.ndjson.gz \
        schema/${resource}.schema.json &
done
