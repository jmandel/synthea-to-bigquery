#!/bin/bash

gsutil cp -r sorted/* gs://fhir-synthetic-ma/2017-05-24/

bq mk synthea

for resource in $(gsutil ls gs://fhir-synthetic-ma/2017-05-24 | egrep -o '[A-Z][^.]+')
do
    echo "Load $resource"
    bq load --replace  --source_format=NEWLINE_DELIMITED_JSON   --autodetect \
        synthea.${resource} gs://fhir-synthetic-ma/2017-05-24/${resource}.ndjson.gz &
done
