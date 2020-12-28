from .abstract_classes import AbstractDestination
import os
from datetime import datetime
import json
from google.cloud import storage


def rename_blob(bucket_name, blob_name, new_name):
    """Renames a blob."""
    # bucket_name = "your-bucket-name"
    # blob_name = "your-object-name"
    # new_name = "new-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    new_blob = bucket.rename_blob(blob, new_name)

    print("Blob {} has been renamed to {}".format(blob.name, new_blob.name))

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # bucket_name = "your-bucket-name"
    # source_file_name = "local/path/to/file"
    # destination_blob_name = "storage-object-name"

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
    """Lists all the blobs in the bucket."""
    # bucket_name = "your-bucket-name"

    storage_client = storage.Client()

    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(bucket_name)

    for blob in blobs:
        print(blob.name)


def delete_blob(bucket_name, blob_name):
    """Deletes a blob from the bucket."""
    # bucket_name = "your-bucket-name"
    # blob_name = "your-object-name"

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.delete()

    print("Blob {} deleted.".format(blob_name))

def download_blob(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""
    # bucket_name = "your-bucket-name"
    # source_blob_name = "storage-object-name"
    # destination_file_name = "local/path/to/file"

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)

    # Construct a client side representation of a blob.
    # Note `Bucket.blob` differs from `Bucket.get_blob` as it doesn't retrieve
    # any content from Google Cloud Storage. As we don't need additional data,
    # using `Bucket.blob` is preferred here.
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)


class CloudStorage (AbstractDestination):
    def __init__(self, dir_path, bucket="songs-lastfm"):
        self.bucket = bucket
        if not os.path.isdir(dir_path):
            os.makedirs(dir_path)
        self.dir_path = dir_path
        self.path = f"{dir_path}/update_{datetime.today().strftime('%Y%m%d')}.tmp"

	# writes rows in files .tmp
    def write(self, rows):
        with open(self.path, 'a') as f:
            f.write(json.dumps(rows, indent=4, sort_keys=True))

        upload_blob(bucket_name=self.bucket, source_file_name=self.path, destination_blob_name=self.path)
        
	# commits tmp files to json files
    def commit(self):
        # print("\tDATALAKE: Committing tmp to json")
        file_names = [e for e in os.listdir(self.dir_path) if '.tmp' in e]
        for f in file_names:
            rename_blob(bucket_name=self.bucket, blob_name=f'{self.dir_path}/{f}', new_name=f"{self.dir_path}/{f.replace('.tmp','.json')}")

        os.remove(self.path)  #rimuoviamo da "locale" dopo upload su bucket

            #os.rename(f'{self.dir_path}/{f}', f"{self.dir_path}/{f.replace('.tmp','.json')}")
            # TODO controllare se riscrive file o cambia effettivamente solo il nome
	
    def upload_songs_to_request(self, path_songs_to_request):
        upload_blob(bucket_name=self.bucket, source_file_name=path_songs_to_request, destination_blob_name=path_songs_to_request)


    # checking for existence of tmp files and removes them
    def rollback(self):
        # print("\tDATALAKE: Checking if tmp file are on datalake")
        #file_names = [e for e in os.listdir(self.dir_path) if '.tmp' in e]
        # if file_names:
            # print("\tDATALAKE: Removing inconsistent data")
        #for f in file_names:
        #    os.remove(f'{self.dir_path}/{f}')
        list_blobs(bucket_name=self.bucket)
        # TODO eliminare tmp in locale, fare download e upload del sync bucket

		
