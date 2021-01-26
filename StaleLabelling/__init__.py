import os
import datetime
from datetime import timedelta
import logging

import azure.functions as func
from azure.storage.blob import ContainerClient


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    _connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')

    labelling_container_client = ContainerClient.from_connection_string(
        _connect_str, "labelling")

    for blob in labelling_container_client.list_blobs():
        labelling_blob_client = labelling_container_client.get_blob_client(
            blob)
        if datetime.datetime.utcnow() - blob.creation_time >= timedelta(hours=1):
            unlabelled_container_client = ContainerClient.from_connection_string(
                _connect_str, "unlabelled")
            unlabelled_blob_client = unlabelled_container_client.get_blob_client(
                blob=blob.name)
            unlabelled_blob_client.start_copy_from_url(
                labelling_blob_client.url)
            props = unlabelled_blob_client.get_blob_properties()
            while props.copy.status != 'success':
                pass
            labelling_blob_client.delete_blob(delete_snapshots="include")
    logging.info('Python timer trigger function ran at %s', utc_timestamp)
