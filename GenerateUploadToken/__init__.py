import logging
import os
import uuid
import datetime
import json
import azure.functions as func
from azure.storage.blob import generate_container_sas, ContainerSasPermissions
from azure.cosmosdb.table import Entity, TableService


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    payload = req.get_json()
    email: str = payload["email"]
    filename: str = payload["filename"]
    filename_extension = filename.split(".")[-1]
    file_name = f"{str(uuid.uuid4())}.{filename_extension}"
    account_name = "vision4458150196"
    container_name = "uploads"

    _connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    access_key = os.getenv('STORAGE_ACCESS_KEY')

    permissions = ContainerSasPermissions(
        read=True, write=True, delete=False, list=True)

    now = datetime.datetime.utcnow()
    end = now + datetime.timedelta(hours=1)

    sas_token = generate_container_sas(
        account_name, container_name, account_key=access_key, permission=permissions, start=now, expiry=end)

    task = {'PartitionKey': email,
            'RowKey': file_name}

    table_service = TableService(connection_string=_connect_str)
    table_service.insert_entity('uploads', task)

    print(sas_token)
    print(email)

    responseData = json.dumps({
        "token": sas_token,
        "name": file_name
    })

    return func.HttpResponse(mimetype="application/json", body=responseData)
