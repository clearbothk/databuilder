import os
import uuid
from azure.storage.blob import BlobServiceClient


class Storage:
    _connect_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    container_name = "unlabelled"
    blob_service_client = None
    container_client = None

    def __init__(self):
        self.blob_service_client = BlobServiceClient.from_connection_string(
            self._connect_str
        )
        try:
            self.container_client = self.blob_service_client.create_container(
                self.container_name
            )
        except:
            print("Did not create container. Container might already exist")

    def write(self, blob, file_ext):
        blob_client = self.blob_service_client.get_blob_client(
            container=self.container_name, blob=f"{str(uuid.uuid4())}.{file_ext}"
        )
        blob_client.upload_blob(blob)

