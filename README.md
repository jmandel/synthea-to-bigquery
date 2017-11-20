## Quick and Dirty Data Loader

Download the Synthea data release and prepare for BigQuery:

* Translate patient bundles --> resource-specific .ndjson files like `Patient.ndjson.gz` containing all resources of a given type
* Generate BigQuery schema files for each resource, based on the complete hierarchy of fields used with the resource-specific .ndjson files
* Load all resource-specific .ndjson files into BigQuery

```sh
./01-prepare.sh
./02-load.sh
```
*Note: This takes O(3h) as currently written.*

## TODO
* Clean up the logic for schema generation (currently it's unreadable / unmaintainable)
* Make it easy to parallelize (e.g. multile files per resource type so generation can run in multiple processes at once
* Move schema generation to a post-processing step to avoid runtime overhead of walking each JSON hierachy when generating resource-spscific .ndjson files
