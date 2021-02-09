import logging
import os
import json
import azure.functions as func
from azure.cosmosdb.table.tableservice import TableService
from azure.storage.blob import BlobServiceClient


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    _connect_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    unlabelled_container = BlobServiceClient.from_connection_string(
        _connect_str).get_container_client("unlabelled")
    labelled_container = BlobServiceClient.from_connection_string(
        _connect_str).get_container_client("labelled")
    table_service = TableService(connection_string=_connect_str)
    data_generator = table_service.query_entities("uploads")

    unlabelled_blobs = list(map(lambda x: x["name"], list(
        unlabelled_container.list_blobs())))
    labelled_blobs = list(map(lambda x: x["name"], list(
        labelled_container.list_blobs())))

    unlabelled_count = 0
    labelled_count = 0
    labelling_count = 0
    total_uploads = 0

    for entry in data_generator:
        total_uploads += 1
        filename = entry["RowKey"]
        if filename in unlabelled_blobs:
            unlabelled_count += 1
        elif filename in labelled_blobs:
            labelled_count += 1
        else:
            labelling_count += 1

    return json.dumps({"uploads": total_uploads, "unlabelled": unlabelled_count, "labelled": labelled_count, "labelling": labelling_count})
