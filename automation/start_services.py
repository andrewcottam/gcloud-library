import subprocess, requests

# Start all the Cloud Run services
# result = subprocess.run(['sh', 'start_services.sh'], stdout=subprocess.PIPE)
# result.stdout

# Request the Geoserver start page - this will block until Geoserver is ready
print('Waiting for Geoserver to be up..')
requests.get('https://geoserver-ny43uciwwa-oc.a.run.app/geoserver/web/')
print('Geoserver is up')
# Restore the database
print('Restoring the database')
result = subprocess.run(['sh', '/Users/andrewcottam/Documents/GitHub/gcloud-library/automation/restore_1.sh'], stdout=subprocess.PIPE)
result.stdout