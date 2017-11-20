#!/bin/sh

# First, Manually download `LOINC_2.61.zip` from https://loinc.org/downloads/

python schema.py

bq mk loinc

bq load --skip_leading_rows 1  --replace \
    loinc.loinc \
    LoincTable/loinc.csv \
    schema/loinc.schema.json

bq load --skip_leading_rows 1  --replace \
    loinc.map_to \
    LoincTable/map_to.csv \
    schema/map_to.schema.json

bq load --skip_leading_rows 1  --replace \
    loinc.source_organization \
    LoincTable/source_organization.csv \
    schema/source_organization.schema.json
