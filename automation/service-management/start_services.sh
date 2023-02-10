# shell script to publish a new version of various Google Cloud Run services where the min-instances is set to 1 so that there are no cold starts

# postgis
gcloud sql instances patch andrew-postgis \
--activation-policy=ALWAYS \
--quiet 

# geoserver 
gcloud run deploy geoserver \
--image=europe-west8-docker.pkg.dev/andrewcottam-default/andrew-docker/geoserver \
--min-instances=1 \
--set-cloudsql-instances=andrewcottam-default:europe-west8:andrew-postgis \
--region=europe-west8 \
--project=andrewcottam-default \
--quiet \
 && gcloud run services update-traffic geoserver --to-latest --region=europe-west8 --quiet

# google-earth-engine-server
gcloud run deploy google-earth-engine-server \
--image=europe-west8-docker.pkg.dev/andrewcottam-default/andrew-docker/google-earth-engine-server:latest \
--min-instances=1 \
--region=europe-west8 \
--project=andrewcottam-default \
--quiet \
 && gcloud run services update-traffic google-earth-engine-server --to-latest --region=europe-west8 --quiet

# pgadmin4
gcloud run deploy pgadmin4 \
--image=europe-west8-docker.pkg.dev/andrewcottam-default/andrew-docker/pgadmin4:latest \
--min-instances=1 \
--set-cloudsql-instances=andrewcottam-default:europe-west8:andrew-postgis \
--region=europe-west8 \
--project=andrewcottam-default \
--quiet \
 && gcloud run services update-traffic pgadmin4 --to-latest --region=europe-west8 --quiet

 # python-rest-server
 gcloud run deploy python-rest-server \
--image=europe-west8-docker.pkg.dev/andrewcottam-default/andrew-docker/python-rest-server:latest \
--min-instances=1 \
--set-cloudsql-instances=andrewcottam-default:europe-west8:andrew-postgis \
--region=europe-west8 \
--project=andrewcottam-default \
--quiet \
 && gcloud run services update-traffic python-rest-server --to-latest --region=europe-west8 --quiet