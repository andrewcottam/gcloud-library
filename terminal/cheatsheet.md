# Cheatsheet for Overstory Google Cloud SDK terminal commands
## Listing files
gcloud storage ls gs://overstory-customer-husqvarna/inputs/city_boundaries/original/all_cities
### Listing how many folders in a folder
gcloud storage ls gs://overstory-customer-husqvarna/inputs/city_boundaries/original/all_cities | wc 
### Listing files with wildcards and returning only certain fields
gcloud storage objects list 'gs://overstory-customer-sce/outputs/**/gazer/*.geojson' --format="json(bucket,name)"

## Copying files
```
gcloud storage cp gs://overstory-customer-sce/outputs/trial/platform/poles_and_lines/lines_1_vv2.geojson /Users/andrewcottam/Downloads/overstory/PolesLines/lines_1_vv2.geojson
gcloud storage cp gs://overstory-customer-sce/outputs/trial/platform/poles_and_lines/poles_1_vv2.geojson /Users/andrewcottam/Downloads/overstory/PolesLines/poles_1_vv2.geojsonpy
```

## Connecting to a Google Cloud Storage bucket using gdal
### Reading a geojson file from one of my Gists
```
ogrinfo -al /vsicurl/https://gist.githubusercontent.com/andrewcottam/d56ef29492672cb4b49e4b2d27dc1fc0/raw/8be559deb54b67a58b0187eeb1c9dd37acb1760b/MapBox_vt_14_8586_5836.geojson
```

### Google Cloud Storage geojson file
Set the path the the service account file using export, e.g. export GOOGLE_APPLICATION_CREDENTIALS='/Users/andrewcottam/Documents/GitHub/gcloud-library/keys/andrewcottam-default-b032bcec754a.json'
```
ogrinfo -al /vsigs/andrewcottam-public/MapBox_vt_14_8586_5836.geojson
```

# REST API Requests
There are 2 ways you can authenticate - using REST and using OAuth2.0.

## Using REST
This is the easiest and uses your local gcloud credentials that are put in by the gcloud auth print-access-token expression. See https://cloud.google.com/docs/authentication/rest#user-creds
```
curl -X GET -H "Authorization: Bearer $(gcloud auth print-access-token)" "https://cloudresourcemanager.googleapis.com/v3/projects/tree-266510"
curl -X GET -H "Authorization: Bearer $(gcloud auth print-access-token)" "https://storage.googleapis.com/storage/v1/b/overstory-customer-sce/o?matchGlob=outputs%2F**%2Fgazer%2F*.geojson&fields=items(bucket,name)"
```

## Using OAuth2.0
To get an OAuth2.0 Authorization token see here https://cloud.google.com/storage/docs/authentication then put it into the request.
```
curl -H "Authorization: Bearer ya29.a0AWY7CknFdwZN20VpeQnUuETFqOWDQ74w7QrObUMLgdy2hncCV-lOAMcr81WvT4CdzcQsvSWo5obFaWTu30A8dMixRAFC6s-ak6a7b7n8a0HViABAZV81fjJqfy8LEPia3AZRHOkgrs7RSa-tLMBpSRICt0XKaCgYKAT0SARISFQG1tDrpuqkJn_Vw184xdi9ktKBugQ0163" "https://storage.googleapis.com/storage/v1/b/overstory-customer-sce/o?matchGlob=outputs%2F**%2Fgazer%2F*.geojson&fields=items(bucket,name)"
```