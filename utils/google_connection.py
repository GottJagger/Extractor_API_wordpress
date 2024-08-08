import os
from google.cloud import storage
from google.oauth2.service_account import Credentials

def authenticate_gcs():
    
    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    credentials = Credentials.from_service_account_file(credentials_path)
    client = storage.Client(credentials=credentials)
    return client

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    client = authenticate_gcs()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name) 
    print(f"File {source_file_name} uploaded to {destination_blob_name}.")
