from .abstract_classes import AbstractDestination
import os
from datetime import datetime
import json
from google.cloud import storage
import re

def rename_blob(bucket_name, blob_name, new_name):
    """
    Rename a block

    Args:
        bucket_name (str): bucket name on google cloud storage
        blob_name (str): current name
        new_name (str): new name
    """

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    new_blob = bucket.rename_blob(blob, new_name)

    print("Blob {} has been renamed to {}".format(blob.name, new_blob.name))

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """
    Upload a file to a bucket

    Args:
        bucket_name (str): bucket name on google cloud storage
        source_file_name (str): local/path/to/file
        destination_blob_name (str): storage-object-name
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        "File {} uploaded to {}.".format(
            source_file_name, destination_blob_name
        )
    )

def list_blobs(bucket_name):
    """
    Lists all the blobs in the bucket.

    Args:
        bucket_name (str): bucket name on google cloud platform
    """
    storage_client = storage.Client()

    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(bucket_name)

    for blob in blobs:
        print(blob.name)

def list_blobs_for_rollback(bucket_name):
    """
    Lists all the blobs in the bucket.

    Args:
        bucket_name (str): bucket name on google cloud platform
    """
    storage_client = storage.Client()

    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(bucket_name)

    for blob in blobs:
        if (re.match('.*.tmp$', blob.name)):
            delete_blob(bucket_name, blob.name)

def delete_blob(bucket_name, blob_name):
    """ 
    Deletes a blob from the bucket.

    Args:
        bucket_name (str): bucket name on google cloud platform
        blob_name (str): object name
    """

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.delete()

    print("Blob {} deleted.".format(blob_name))

def download_blob(bucket_name, source_blob_name, destination_file_name):
    """
    Downloads a blob from the bucket.

    Args:
        bucket_name (str) = your-bucket-name
        source_blob_name (str) = storage-object-name
        destination_file_name (str) = local/path/to/file
    """

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)

    # Construct a client side representation of a blob.
    # Note `Bucket.blob` differs from `Bucket.get_blob` as it doesn't retrieve
    # any content from Google Cloud Storage. As we don't need additional data,
    # using `Bucket.blob` is preferred here.
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)


class CloudStorage (AbstractDestination):
    """
    Google cloud storage implementation.
    The class manages writes and commits on a 
    GCP bucket.
    """
    def __init__(self, dir_path, bucket="songs-lastfm"):
        """
        Constructor

        Args:
            dir_path (str): local directory to use as a base for the GCP
            bucket (str, optional): bucket name. Defaults to "songs-lastfm".
        """
        self.bucket = bucket
        if not os.path.isdir(dir_path):
            os.makedirs(dir_path)
        self.dir_path = dir_path
        self.path = f"{dir_path}/update_{datetime.today().strftime('%Y%m%d')}.tmp"

        self.sync_path = f"{self.dir_path}/sync.json"
        try:
            download_blob(bucket_name=self.bucket, source_blob_name=self.sync_path, destination_file_name=self.sync_path)
        except:
            os.remove(self.sync_path)
            pass #for first commit on bucket


	# writes rows in files .tmp
    def write(self, rows):
        """
        Write rows to the google cloud platform.
        It basically write locally and then 
        updates the local update files.

        Args:
            rows (list): new rows to write on cloud 
        """
        with open(self.path, 'a') as f:
            f.write(json.dumps(rows, indent=4, sort_keys=True, ensure_ascii=False))

        upload_blob(bucket_name=self.bucket, source_file_name=self.path, destination_blob_name=self.path)
        
    def commit(self):
        """
        Commit files on cloud, it changes tmp extensions to json
        """
        file_names = [e for e in os.listdir(self.dir_path) if '.tmp' in e]
        for f in file_names:
            rename_blob(bucket_name=self.bucket, blob_name=f'{self.dir_path}/{f}', new_name=f"{self.dir_path}/{f.replace('.tmp','.json')}")

        os.remove(self.path)

        upload_blob(bucket_name=self.bucket, source_file_name=self.sync_path, destination_blob_name=self.sync_path)
	
    def upload_songs_to_request(self, path_songs_to_request):
        upload_blob(bucket_name=self.bucket, source_file_name=path_songs_to_request, destination_blob_name=path_songs_to_request)


    def rollback(self):
        """
        Rollback tmp files if still present after a commit operation
        """
        list_blobs_for_rollback(bucket_name=self.bucket)

		
