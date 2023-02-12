import subprocess, requests, os, time

# Get the path to the current file
path = os.path.dirname(os.path.realpath(__file__)) 

def restor_item(shell_cmd_path, check_url, item_name):
    """Makes a request to the Geoserver REST API to restore an existing file from disk.

    Args:
        shell_cmd_path (string): The path to the shell command on the local computer which contains the curl request to make
        check_url (string): The url endpoint that will not return a 404 error when the item has been restored
        message (string): A description of the item being restored
    """
    FAILED_STATUS_CODE = 404
    # Run the shell restore command not outputting the results to stdout
    result = subprocess.run(['sh', path + os.sep + shell_cmd_path], stdout=subprocess.DEVNULL)

    # Loop until the item has been restored
    print('Polling to see when the ' + item_name + ' has been restored ..')
    status_code = FAILED_STATUS_CODE
    while (status_code == FAILED_STATUS_CODE):
        status_code = requests.get(check_url, auth=('admin', 'geoserver')).status_code
        print('\tWaiting for the ' + item_name + ' to be restored ..')
        time.sleep(5)
    print(item_name + ' restored')    

# Restor the Geoserver resources
restor_item('restore_1.sh', 'https://geoserver-ny43uciwwa-oc.a.run.app/geoserver/rest/workspaces/cloud_sql?quietOnNotFound=true', 'workspace')
restor_item('restore_2.sh', 'https://geoserver-ny43uciwwa-oc.a.run.app/geoserver/rest/workspaces/cloud_sql/datastores/andrew-postgis.xml', 'workspace and store')
restor_item('restore_3.sh', 'https://geoserver-ny43uciwwa-oc.a.run.app/geoserver/rest/layers/gee_spectral_data?quietOnNotFound=true', 'workspace, store and layers')


# url = 'https://geoserver-ny43uciwwa-oc.a.run.app/geoserver/rest/br/restore/'
# headers = {'content-type': 'application/json'}
# data = {
#     "restore":{
#        "archiveFile":"/opt/geoserver/data_dir/workspace.zip",
#        "options":{
#        }
#     }
#  }
# x = requests.post(url, auth=('admin', 'geoserver'), headers=headers, data=data)
# print(x.status_code)
# print(dir(x))