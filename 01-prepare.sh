#!/bin/bash

wget https://syntheticmass.mitre.org/downloads/2017_05_24/synthea_1m_fhir_3_0_May_24.tar.gz
tar -xzf synthea_1m_fhir_3_0_May_24.tar.gz
rm synthea_1m_fhir_3_0_May_24.tar.gz
mkdir sorted
python sort.py
