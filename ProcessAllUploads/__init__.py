import logging
import os

import azure.functions as func
from azure.storage.blob import BlobServiceClient, BlobClient, StorageStreamDownloader


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # Get all the ZIP files in the Storage
    _connect_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    container_name = "uploads"
    blob_service_client = BlobServiceClient.from_connection_string(
        _connect_str)
    upload_container_client = blob_service_client.get_container_client(
        container_name)
    blobs = list(filter(lambda el: el.name.split(
        '.')[-1], list(upload_container_client.list_blobs())))
    # Extract the contents of each one of them to the unlabelled
    for b in blobs:
        blob_client: BlobClient = blob_service_client.get_blob_client(
            container=container_name, blob=b)
        file_stream: StorageStreamDownloader = blob_client.download_blob()
        file_stream.readinto()
    # Cleanup the ZIP
    return func.HttpResponse(
        "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
        status_code=200
    )
