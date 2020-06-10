import logging
import os
import io
import json
import azure.functions as func
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions, BlobClient

def moveBlob(filename, old_blob_client):
    _connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    access_key = os.getenv('STORAGE_ACCESS_KEY')
    account_name = "vision4458150196"

    container_name = "labelled"
    blob_service_client = BlobServiceClient.from_connection_string(_connect_str)

    try:
        blob_service_client.create_container(container_name)
    except:
        print("Did not create a container")
    
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(filename)
    try:
        blob_client.start_copy_from_url(old_blob_client.url)
        old_blob_client.delete_blob()
    except:
        logging.info("Could not start copy and delete. Maybe the blob is not in labelling anymore")
    return blob_client.url

def constructTextFileString(labels):
    result = ""
    for label in labels:
        result += f"{label['data']['text']} {label['geometry']['x']} {label['geometry']['y']} {label['geometry']['width']} {label['geometry']['height']}\r\n"
    return result

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    _connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    access_key = os.getenv('STORAGE_ACCESS_KEY')
    account_name = "vision4458150196"
    container_name = "labelled"
    image_url = ""
    labels = []
    try:
        req_body = req.get_json()
        image_url = req_body["image"]
        labels = req_body["labels"]
    except ValueError:
        response = func.HttpResponse("Labels were not provided")
        response.status_code = 400
        return response
    
    url_no_sas = image_url.split('?')[0]
    file_name = url_no_sas.split('/')[-1]
    file_name_no_ext = file_name.split('.')[-2]

    logging.info(url_no_sas)
    # Move image to labelled
    old_blob_client = BlobClient.from_blob_url(url_no_sas, credential=access_key)
    moveBlob(file_name, old_blob_client)

    # Write a labels file
    labels_file_name = file_name_no_ext + ".txt"
    text_file_str = constructTextFileString(labels)
    logging.info(f"Labelling contents:\n{text_file_str}")
    text_file = io.BytesIO(bytes(text_file_str, 'utf-8'))
    text_file.seek(0)
    # logging.info(f"Labelling contents from stream:\n{data}")
    # Create a Blob Service Client
    blob_service_client = BlobServiceClient.from_connection_string(_connect_str)

    # Create a Container client for the labelled container
    container_client = blob_service_client.get_container_client(container_name)

    # Create a blob client for the labels file
    blob_client = container_client.get_blob_client(labels_file_name)

    # Upload the text file using the blob client
    blob_client.upload_blob(text_file)

    response = func.HttpResponse("DONE")
    return response