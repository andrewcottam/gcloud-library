# Deletes all retired revisions across all Cloud Run services - install run_v2 with pip install google-cloud-run
import time, os
from google.cloud import run_v2
from google.cloud.run_v2.types import Condition

def delete_retired_revisions():
    # Create a client
    client = run_v2.RevisionsClient()
    # Initialize request argument(s) to get all revisions
    # TODO: Update this to not being hard-coded
    project_id = os.getenv('GOOGLE_CLOUD_PROJECT') # you can use the project ID or the project number
    print(f"Project ID: {project_id}")
    request = run_v2.ListRevisionsRequest(parent=f"projects/{project_id}/locations/europe-west6/services/-",) # gainforest
    # request = run_v2.ListRevisionsRequest(parent="projects/162666128137/locations/europe-west8/services/tree-detection-server",)
    # Make the request to get the list of revisions
    page_result = client.list_revisions(request=request)
    write_requests = 0
    # Iterate through the revisions and if they are retired, delete them
    for i, revision in enumerate(page_result):
        print(f"\nRevision: {revision.name[revision.name.rfind('/')+1:]}")
        for c in revision.conditions:
            if c.type == "Ready":
                if (c.revision_reason == Condition.RevisionReason.RETIRED):
                    print('Deleting revision')
                    # Initialize request argument(s)
                    request = run_v2.DeleteRevisionRequest(name=revision.name,)
                    # Make the request to delete the revision
                    client.delete_revision(request=request)
                    write_requests += 1
                    print(f"Deleted ({write_requests} write requests)")
                    if (write_requests >59):
                        # There is a quota of 60 write operations per minute using the Cloud Run API, so wait 1 minute before continuing
                        print('Pausing to wait for the quota limit per minute to expire..')
                        time.sleep(60)
                        write_requests = 0
                else:
                    print('Revision is currently serving')

delete_retired_revisions()
