import requests, time

# CONSTANT DECLARATIONS
FAILED_STATUS_CODE = 404
GS_HEADERS = {'Content-Type': 'application/json'}
GS_DATA_DIR = '/opt/geoserver/data_dir'

class GeoserverREST(object):
    """ Helper class for working with the Geoserver REST API
    """
    
    def __init__(self, rest_url, user, password):
        """Initialises the class and attempts to connect to the Geoserver REST API endpoint using the passed credentials.

        Args:
            rest_url (string): The REST API endpoint, e.g. https://geoserver-ny43uciwwa-oc.a.run.app/geoserver/rest
            user (string): The user name used to authenticate to Geoserver 
            password (string): The password used to authenticate to Geoserver

        Raises:
            Exception: Raises and Exception if it fails to connect to Geoserver or the credentials are wrong
        """
        print('Connecting to the Geoserver REST API ..')
        # try and connect to the rest endpoint
        try:
            response = requests.get(rest_url, auth=(user, password))
        except requests.exceptions.ConnectionError as e:
            raise Exception('Unable to connect to ' + rest_url + '. Please check the url and credentials and try again.')
        else:
            # Check that is a valid url
            if (response.status_code == 200):
                # Check the user/password
                if (response.text.find('Geoserver Configuration API') > 0):
                    print('Connected to Geoserver REST API')
                    self.rest_url = rest_url
                    self.restore_url = rest_url + '/br/restore/'
                    self.user = user
                    self.password = password
                else:
                    raise Exception('Invalid credentials')
            else:
                raise Exception('Unable to connect to ' + rest_url + '. Please check the url and credentials and try again.')

    def restor_from_backup_file(self, backup_filename, check_url, item_name):
        """Makes a request to the Geoserver REST API to restore an existing backup file from disk.

        Args:
            backup_filename (string): The filename of the zipped backup file in the Geoserver data directory on the server from which the data will be restored, e.g. workspace.zip
            check_url (string): The Geoserver REST API endpoint that will be polled to see when the item is restored. This url is relative to the REST API url, e.g. '/workspaces/cloud_sql?quietOnNotFound=true' 
            message (string): A description of the item being restored
        """
        # Run the request to restore the backup file
        requests.post(self.restore_url, auth=(self.user, self.password), headers=GS_HEADERS, data='{"restore": {"archiveFile":"' + GS_DATA_DIR + backup_filename + '"}}')

        # Loop until the item has been restored
        print('Polling to see when the ' + item_name + ' has been restored ..')
        status_code = FAILED_STATUS_CODE
        while (status_code == FAILED_STATUS_CODE):
            status_code = requests.get(self.rest_url + check_url, auth=(self.user, self.password)).status_code
            print('Waiting for the ' + item_name + ' to be restored ..')
            time.sleep(1)
        print(item_name + ' restored')    

if __name__ == '__main__':
    gs = GeoserverREST('https://geoserver-ny43uciwwa-oc.a.run.app/geoserver/rest', 'admin', 'geoserver')
    # Restor the Geoserver resources
    gs.restor_from_backup_file('/workspace.zip', '/workspaces/cloud_sql?quietOnNotFound=true', 'workspace')
    gs.restor_from_backup_file('/workspace_database.zip', '/workspaces/cloud_sql/datastores/andrew-postgis.xml', 'workspace and store')
    gs.restor_from_backup_file('/workspace_database_layers.zip', '/layers/gee_spectral_data?quietOnNotFound=true', 'workspace, store and layers')