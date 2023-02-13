import subprocess, requests, os, argparse, time
from Geoserver import GeoserverREST

def main(args):

    # Start all the Cloud Run services and Cloud SQL
    print('\nStarting services and resetting in ' + str(args.time) + ' minutes ..\n')
    path = os.path.dirname(os.path.realpath(__file__)) 
    result = subprocess.run(['sh', path + os.sep + 'start_services.sh'], stdout=subprocess.PIPE)
    result.stdout

    # Request the Geoserver start page - this will block until Geoserver is ready
    print('\nWaiting for Geoserver to be up ..')
    requests.get('https://geoserver-ny43uciwwa-oc.a.run.app/geoserver/web/')
    print('Geoserver is up\n')

    # Restore the Geoserver resources
    gs = GeoserverREST('https://geoserver-ny43uciwwa-oc.a.run.app/geoserver/rest', 'admin', 'geoserver')
    gs.restor_from_backup_file('/workspace.zip', '/workspaces/cloud_sql?quietOnNotFound=true')
    gs.restor_from_backup_file('/workspace_database.zip', '/workspaces/cloud_sql/datastores/andrew-postgis.xml')
    gs.restor_from_backup_file('/workspace_database_layers.zip', '/layers/gee_spectral_data?quietOnNotFound=true')
    print(gs.summary())

    wait = args.time * 60
    while wait > 0:
        if (wait % 60 == 0):
            print(str(int(wait/60)) + ' minutes remaining until resetting services')
        time.sleep(1)
        wait = wait - 1

    print('\nResetting services ..')
    # Reset all the Cloud Run services to min-instances of 0 and stop Cloud SQL
    result = subprocess.run(['sh', path + os.sep + 'reset_services.sh'], stdout=subprocess.PIPE)
    result.stdout    
    print('Services stopped\n')

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--time", type=int, choices=range(20, 120), help="The time in minutes before the services revert to their min-instances of 0. Must be greater than 20 and less than 120.", required=True)
    args = parser.parse_args()
    main(args)
