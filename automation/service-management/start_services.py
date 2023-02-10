import subprocess, requests, os

# Start all the Cloud Run services and Cloud SQL
path = os.path.dirname(os.path.realpath(__file__)) 
result = subprocess.run(['sh', path + os.sep + 'start_services.sh'], stdout=subprocess.PIPE)
result.stdout

# Request the Geoserver start page - this will block until Geoserver is ready
print('Waiting for Geoserver to be up..')
requests.get('https://geoserver-ny43uciwwa-oc.a.run.app/geoserver/web/')
print('Geoserver is up')
# Restore the database
print('Restoring the database')
result = subprocess.run(['sh', '/Users/andrewcottam/Documents/GitHub/gcloud-library/automation/restore_1.sh'], stdout=subprocess.PIPE)
result.stdout