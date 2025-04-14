import logging
import os
import json
import azure.functions as func
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions, BlobClient
from io import BytesIO
import base64
import datetime

def moveBlob(blob_service_client : BlobServiceClient, blob, old_blob: BlobClient):
    container_name = "labelling"
    filename = blob["name"]
    try:
        blob_service_client.create_container(container_name)
    except:
        print("Did not create a container")
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(filename)
    blob_client.start_copy_from_url(old_blob.url)
    old_blob.delete_blob()
    return blob_client.url

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    _connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    access_key = os.getenv('STORAGE_ACCESS_KEY')
    account_name = "vision4458150196"
    container_name = "unlabelled"

    # Create a Blob Service Client
    blob_service_client = BlobServiceClient.from_connection_string(_connect_str)

    # Create a Container client for the unlabelled container
    container_client = blob_service_client.get_container_client("unlabelled")

    # Get the blob client for one image
    blob_generator = container_client.list_blobs()
    blob = None
    try:
        blob = blob_generator.next()
        logging.info(f"Using the blob: {blob}")
    except StopIteration:
        logging.info("Empty container")
        return func.HttpResponse(mimetype="application/json", body=json.dumps({}))
    blob_client = container_client.get_blob_client(blob)
    labelling_blob_url = moveBlob(blob_service_client, blob, blob_client)

    # Generate blob link
    permssions = BlobSasPermissions(read=True)
    sas = generate_blob_sas(account_name, "labelling", blob["name"], account_key=access_key, permission=permssions, start=datetime.datetime.utcnow(), expiry=datetime.datetime.utcnow() + datetime.timedelta(minutes=2))
    sas_url = f"{labelling_blob_url}?{sas}"

    response = json.dumps({
        "filename": blob["name"],
        "image": sas_url
    })

    return func.HttpResponse(mimetype="application/json", body=response)