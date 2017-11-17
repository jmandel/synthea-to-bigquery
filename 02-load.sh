#!/bin/bash

gsutil cp -r sorted/* gs://fhir-synthetic-ma/2017-05-24/

bq mk synthea

bq load --replace  --source_format=NEWLINE_DELIMITED_JSON   --autodetect \
    synthea.condition gs://fhir-synthetic-ma/2017-05-24/Condition.ndjson.gz

bq load --replace  --source_format=NEWLINE_DELIMITED_JSON   --autodetect \
    synthea.Patient gs://fhir-synthetic-ma/2017-05-24/Patient.ndjson.gz
