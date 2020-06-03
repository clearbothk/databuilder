import os
import logging
import tarfile
import uuid
from io import BufferedReader
import azure.functions as func
from azure.storage.blob import BlobServiceClient

def handle_tar(myblob: func.InputStream, blob_service_client, container_name):
    with tarfile.open(fileobj=myblob, mode='r|*') as tar:
        tar.extractall()
        for f in tar.getmembers():
            file_ext = f.name.split('.')[-1]
            file_ext = file_ext.lower()
            if file_ext == "jpeg" or file_ext == "jpg" or file_ext == "png":
                with open(f.name, mode='rb') as img:
                    blob_client = blob_service_client.get_blob_client(container=container_name, blob=f"{str(uuid.uuid4())}.{file_ext}")
                    blob_client.upload_blob(img)

def main(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.length} bytes")


    
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_name = "unlabelled"

    try:
        container_client = blob_service_client.create_container(container_name)
    except:
        print("Did not create container. Container might already exist")
    
    with tarfile.open(fileobj=myblob, mode='r|*') as tar:
        out = tar.extractall()
        for f in tar.getmembers():
            file_ext = f.name.split('.')[-1]
            file_ext = file_ext.lower()
            if file_ext == "jpeg" or file_ext == "jpg" or file_ext == "png":
                with open(f.name, mode='rb') as img:
                    blob_client = blob_service_client.get_blob_client(container=container_name, blob=f"{str(uuid.uuid4())}.{file_ext}")
                    blob_client.upload_blob(img)
            