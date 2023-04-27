# Cheatsheet for Overstory Google Cloud SDK terminal commands
## Copying files
gcloud storage cp gs://overstory-customer-sce/outputs/trial/platform/poles_and_lines/lines_1_vv2.geojson /Users/andrewcottam/Downloads/overstory/PolesLines/lines_1_vv2.geojson
gcloud storage cp gs://overstory-customer-sce/outputs/trial/platform/poles_and_lines/poles_1_vv2.geojson /Users/andrewcottam/Downloads/overstory/PolesLines/poles_1_vv2.geojsonpy

## Connecting to a Google Cloud Storage bucket using gdal
### Reading a geojson file from one of my Gists
ogrinfo -al /vsicurl/https://gist.githubusercontent.com/andrewcottam/d56ef29492672cb4b49e4b2d27dc1fc0/raw/8be559deb54b67a58b0187eeb1c9dd37acb1760b/MapBox_vt_14_8586_5836.geojson

### Google Cloud Storage geojson file
## Set the path the the service account file using export, e.g. export GOOGLE_APPLICATION_CREDENTIALS='/Users/andrewcottam/Documents/GitHub/gcloud-library/keys/andrewcottam-default-b032bcec754a.json'
ogrinfo -al /vsigs/andrewcottam-public/MapBox_vt_14_8586_5836.geojson