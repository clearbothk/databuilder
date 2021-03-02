import logging
import os

import azure.functions as func
from azure.storage.blob import BlobServiceClient


def main(req: func.HttpRequest) -> func.HttpResponse:
    _connect_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    source_container = "review"
    target_container = "unlabelled"
    logging.info('Python HTTP trigger function processed a request.')
    req_body = req.get_json()
    image_name = req_body["image_name"]
    approved = req_body["approved"]

    if approved:
        review_container = BlobServiceClient.from_connection_string(
            _connect_str).get_container_client(source_container)
        unlabelled_container = BlobServiceClient.from_connection_string(
            _connect_str).get_container_client(target_container)
        source_blob_client = review_container.get_blob_client(image_name)
        source_blob_url = source_blob_client.url
        target_blob_client = unlabelled_container.get_blob_client(image_name)
        target_blob_client.start_copy_from_url(source_blob_url)
        props = target_blob_client.get_blob_properties()
        while not props.copy.status:
            logging.info(props.copy.status)
            continue
        source_blob_client.delete_blob()
    elif not approved:
        review_container = BlobServiceClient.from_connection_string(
            _connect_str).get_container_client(source_container)
        source_blob_client = review_container.get_blob_client(image_name)
        source_blob_client.delete_blob()

    return func.HttpResponse(
        "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
        status_code=200
    )
