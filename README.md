# gcloud-library
Library of gcloud commands for automation and managing Google Cloud Platform resources and services

# Setup
```
pip install -r requirements.txt
```

# Example usage
```
gcloud config configurations activate gainforest
gcloud auth application-default login
export GOOGLE_CLOUD_PROJECT=tree-mapping-93fd7
python automation/service-management/delete_retired_revisions.py
```