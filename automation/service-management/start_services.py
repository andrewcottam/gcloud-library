import subprocess, requests, os
from Geoserver import GeoserverREST

# Start all the Cloud Run services and Cloud SQL
path = os.path.dirname(os.path.realpath(__file__)) 
result = subprocess.run(['sh', path + os.sep + 'start_services.sh'], stdout=subprocess.PIPE)
result.stdout

# Request the Geoserver start page - this will block until Geoserver is ready
print('Waiting for Geoserver to be up ..')
requests.get('https://geoserver-ny43uciwwa-oc.a.run.app/geoserver/web/')
print('Geoserver is up')

# Restore the database
gs = GeoserverREST('https://geoserver-ny43uciwwa-oc.a.run.app/geoserver/rest', 'admin', 'geoserver')
gs.restor_from_backup_file('/workspace.zip', '/workspaces/cloud_sql?quietOnNotFound=true')
gs.restor_from_backup_file('/workspace_database.zip', '/workspaces/cloud_sql/datastores/andrew-postgis.xml')
gs.restor_from_backup_file('/workspace_database_layers.zip', '/layers/gee_spectral_data?quietOnNotFound=true')
print(gs.summary())