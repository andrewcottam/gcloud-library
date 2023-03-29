# Deletes all retired revisions across all Cloud Run services - install run_v2 with pip install google-cloud-run
from google.cloud import run_v2
from google.cloud.run_v2.types import Condition

def delete_retired_revisions():
    # Create a client
    client = run_v2.RevisionsClient()
    # Initialize request argument(s) to get all revisions
    request = run_v2.ListRevisionsRequest(parent="projects/162666128137/locations/europe-west8/services/-",)
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
                    operation = client.delete_revision(request=request)
                    write_requests += 1
                    print(f"Deleted ({write_requests} write requests)")
                else:
                    print('Revision is currently serving')

delete_retired_revisions()
