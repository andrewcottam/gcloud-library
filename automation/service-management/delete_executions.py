# Deletes all retired revisions across all Cloud Run services - install run_v2 with pip install google-cloud-run
import time, os
from google.cloud import run_v2

def delete_executions():
    # Create a client
    client = run_v2.ExecutionsClient()
    # Initialize request argument(s) to get all executions
    # TODO: Update this to not being hard-coded
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT') # you can use the project ID or the project number
    print(f"Project ID: {project_id}")
    request = run_v2.ListExecutionsRequest(parent=f"projects/{project_id}/locations/europe-west6/jobs/tree-detector")
    # Make the request
    page_result = client.list_executions(request=request)
    # Handle the response
    write_requests = 0    
    for response in page_result:
        request = run_v2.DeleteExecutionRequest(name=response.name)
        operation = client.delete_execution(request=request)
        write_requests += 1
        if (write_requests >59):
            # There is a quota of 60 write operations per minute using the Cloud Run API, so wait 1 minute before continuing
            print('Pausing to wait for the quota limit per minute to expire..')
            time.sleep(60)
            write_requests = 0
        print(f"Deleting {response.name}...")
        response = operation.result()

delete_executions()
