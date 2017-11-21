#!/bin/bash

gsutil cp -r ndjson/* gs://fhir-synthetic-ma/2017-05-24/

bq mk synthea

pushd ndjson
for resource in $(gsutil ls gs://fhir-synthetic-ma/2017-05-24/*.ndjson.gz | egrep -o '[A-Z][^.]+')
do
    echo "Load $resource"

    bq load --replace  --source_format=NEWLINE_DELIMITED_JSON \
        synthea.${resource} \
        gs://fhir-synthetic-ma/2017-05-24/${resource}.ndjson.gz \
        ${resource}.schema.json &
done
popd
