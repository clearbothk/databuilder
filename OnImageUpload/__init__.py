import logging
import os

import azure.functions as func
from azure.storage.blob import BlobServiceClient


def main(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.length} bytes")

    acceptable_extensions = ["jpg", "png", "jpeg"]

    # Make a blob client to put the image files
    _connect_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    source_container_name = "uploads"
    target_container_name = "unlabelled"

    if myblob.name.lower().endswith(tuple(acceptable_extensions)):
        blob_service_client = BlobServiceClient.from_connection_string(
            _connect_str)
        try:
            blob_service_client.create_container(target_container_name)
        except:
            print("Did not create container. Container might already exist")
        blob_client = blob_service_client.get_blob_client(
            container=target_container_name, blob=myblob.name.split("/")[-1])
        blob_client.upload_blob(myblob)

        # Cleanup the blob from the uploads folder
        cleanup_blob_client = blob_service_client.get_blob_client(
            container=source_container_name, blob=myblob.name.split("/")[-1])
        cleanup_blob_client.delete_blob()
