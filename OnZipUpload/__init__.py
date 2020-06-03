import os
import logging
import tarfile
import zipfile
import re
from zipfile import ZipFile
import uuid
from io import BufferedReader, BytesIO
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from .storage import Storage

def handle_tar(myblob: func.InputStream, storage: Storage):
    with tarfile.open(fileobj=myblob, mode='r|*') as tar:
        tar.extractall()
        for f in tar.getmembers():
            file_ext = f.name.split('.')[-1]
            file_ext = file_ext.lower()
            if file_ext == "jpeg" or file_ext == "jpg" or file_ext == "png":
                with open(f.name, mode='rb') as img:
                    storage.write(img, file_ext)

def handle_zip(myblob: func.InputStream, storage: Storage):
    regex = re.compile("^.*(\/)?\._.*")
    zip_blob = BytesIO(myblob.read())
    zip_file = ZipFile(zip_blob)
    contents = zip_file.namelist()
    contents_filtered = [item for item in contents if item not in list(filter(regex.search, contents))]
    for f in contents_filtered:
        print(f)
        file_ext = f.split('.')[-1]
        file_ext = file_ext.lower()

        if file_ext == "jpeg" or file_ext == "jpg" or file_ext == "png":
            with zip_file.open(f) as myfile:
                storage.write(myfile, file_ext)

def main(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.length} bytes")

    storage = Storage()

    archive_extension = myblob.name.split('.')[-1]
    if archive_extension == 'tar':
        handle_tar(myblob, storage)
    elif archive_extension == 'zip':
        handle_zip(myblob, storage)