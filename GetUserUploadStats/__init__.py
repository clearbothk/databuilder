import logging
import os
import json
import io
import csv
import datetime
import azure.functions as func
from azure.cosmosdb.table.tableservice import TableService
from azure.storage.blob import BlobServiceClient


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    _connect_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    unlabelled_container = BlobServiceClient.from_connection_string(
        _connect_str).get_container_client("unlabelled")
    labelling_container = BlobServiceClient.from_connection_string(
        _connect_str).get_container_client("labelling")
    labelled_container = BlobServiceClient.from_connection_string(
        _connect_str).get_container_client("labelled")
    table_service = TableService(connection_string=_connect_str)
    data_generator = table_service.query_entities("uploads")

    unlabelled_blobs = list(map(lambda x: x["name"], list(
        unlabelled_container.list_blobs())))
    labelling_blobs = list(map(lambda x: x["name"], list(
        labelling_container.list_blobs())))
    labelled_blobs = list(map(lambda x: x["name"], list(
        labelled_container.list_blobs())))

    unlabelled_count = 0
    labelled_count = 0
    labelling_count = 0
    other_count = 0
    total_uploads = 0

    email_mapping = {}
    csvfile = io.StringIO()
    for entry in data_generator:
        total_uploads += 1
        filename = entry["RowKey"]
        timestamp = entry["Timestamp"]
        if filename in unlabelled_blobs:
            unlabelled_count += 1
        elif filename in labelled_blobs:
            labelled_count += 1
        elif filename in labelling_blobs:
            labelled_count += 1
        else:
            other_count += 1
        current_email_count = email_mapping.get(entry["PartitionKey"], 0)
        email_mapping[entry["PartitionKey"]] = current_email_count + 1
        writer = csv.writer(csvfile)
        writer.writerow(
            [entry["PartitionKey"], timestamp.strftime("%c"), filename])

    csvfile.seek(0)
    return csvfile.read()
    # json.dumps({"uploads": total_uploads, "unlabelled": unlabelled_count, "labelled": labelled_count, "labelling": labelling_count, "others": other_count, "emails": email_mapping})
