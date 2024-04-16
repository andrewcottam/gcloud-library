# Deletes all the artefacts from the Artefact Registry
from google.cloud import artifactregistry_v1

def delete_registries():
    # Create a client
    client = artifactregistry_v1.ArtifactRegistryClient()
    # Initialize request argument(s)
    # TODO: Update these from being hard-coded
    # request = artifactregistry_v1.ListDockerImagesRequest(parent="projects/andrewcottam-default/locations/europe-west8/repositories/andrew-docker")
    # gainforest
    request = artifactregistry_v1.ListDockerImagesRequest(parent="projects/tree-mapping-93fd7/locations/europe-west6/repositories/tree-mapping-default")
    # Make the request
    page_result = client.list_docker_images(request=request)
    # Iterate through the docker images
    for i, image in enumerate(page_result):
        name = image.name[image.name.rfind('/'):]
        if hasattr(image, 'tags'):
            if len(image.tags)>0:
                print(f"Docker image: {name} has the tag: {image.tags[0]}")
            else:
                print(f"Docker image: {name} has no tags - deleting")
                # Initialize request argument(s)
                request = artifactregistry_v1.DeleteVersionRequest(name=image.name,)
                # Make the request
                client.delete_version(request=request)
                print('Deleted')

delete_registries()