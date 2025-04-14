import logging
import os
import json
import azure.functions as func
import datetime
from azure.storage.blob import BlobServiceClient, AccountSasPermissions, generate_blob_sas


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    _connect_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    container_name = "review"
    review_container = BlobServiceClient.from_connection_string(
        _connect_str).get_container_client(container_name)

    review_blobs = []
    for blob in review_container.list_blobs():
        review_blob_client = review_container.get_blob_client(
            blob)
        url = review_blob_client.url
        sas = generate_blob_sas(
            review_blob_client.account_name,
            container_name,
            review_blob_client.blob_name,
            account_key=review_blob_client.credential.account_key,
            permission=AccountSasPermissions(read=True),
            start=datetime.datetime.utcnow(),
            expiry=datetime.datetime.utcnow() + datetime.timedelta(hours=2)
        )
        review_blobs.append(
            {"uri": url, "filename": review_blob_client.blob_name, "sas": sas})
    return json.dumps(review_blobs)
