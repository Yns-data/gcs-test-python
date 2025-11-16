from google.cloud import storage
from io import BytesIO
import gzip
import json

# Configuration
BUCKET_NAME = "airfrance-bucket"  # nom du bucket
SOURCE_BLOB_NAME = "data/afklm_api_data_collection_destination=AMS&origin=SVQ&endRange=2025-10-14T23_59_59Z&startRange=2025-05-15T09_00_00Z_0.json.gzip"     # chemin du gzip
DESTINATION_BLOB_NAME = "df_call_parameters.csv"  # chemin du CSV modifié


def main():
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(SOURCE_BLOB_NAME)
    gzip_data = blob.download_as_bytes()
    with gzip.GzipFile(fileobj=BytesIO(gzip_data)) as gz :
        data = json.load(gz)
    
    print(json.dumps(data))


if __name__ == "__main__":
    main()
