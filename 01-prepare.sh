#!/bin/bash

wget https://syntheticmass.mitre.org/downloads/2017_05_24/synthea_1m_fhir_3_0_May_24.tar.gz
tar -xzf synthea_1m_fhir_3_0_May_24.tar.gz
rm synthea_1m_fhir_3_0_May_24.tar.gz

wget http://hl7.org/fhir/STU3/definitions.json.zip
unzip definitions.json.zip profiles-resources.json profiles-types.json
rm definitions.json.zip

mkdir ndjson
python prepare.py
python generate_schema.py
