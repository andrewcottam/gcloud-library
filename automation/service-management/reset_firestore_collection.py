from google.cloud import firestore

def delete_collection(collection_ref, batch_size=10):
    """Deletes all documents in a Firestore Collection

    Args:
        collection_ref (string): A reference to the collection
        batch_size (int, optional): The size of the batch. Defaults to 10.

    Returns:
        _type_: _description_
    """
    docs = collection_ref.limit(batch_size).stream()
    deleted = 0

    for doc in docs:
        print(f'Deleting document: {doc.id}')
        doc.reference.delete()
        deleted += 1

    if deleted >= batch_size:
        return delete_collection(collection_ref, batch_size)

# Initialize Firestore
db = firestore.Client()

# Replace 'your-collection-name' with the name of your collection
collection_name = 'drone-images'
collection_ref = db.collection(collection_name)

# Call the function to delete all documents
delete_collection(collection_ref)
