import os

import concurrent.futures

import settings

# from Logger.logger import log


from azure.storage.blob import BlobServiceClient

from azure.identity import ClientSecretCredential, InteractiveBrowserCredential

from azure.core.exceptions import ResourceNotFoundError

import utils.utils
from exceptions.file_upload_exception import FileUploadException


# from LLMAzurePython.Utils import settings


class BlobStorageService:
    """
    class to handle operation to azure blob storage
    """
    def __init__(self, storage_account_name, container_name):
        """
            Initialize the BlobStorageUploader with the specified storage account name and container name.
            Parameters:
            - storage_account_name (str): Azure Storage account name.
            - container_name (str): Azure Storage container name.
        """
        self.storage_account_name = storage_account_name.strip()
        self.container_name = container_name.strip()
        print("intializing blob uploader")

    def __get_credentials(self):
        """
            Obtain Azure credentials using ClientSecretCredential.
            Returns:
            - ClientSecretCredential: Azure credentials.
        """
        # avoid circular imports
        from utils.utils import Utils
        tenant_id, client_id, client_secret = Utils.get_azure_service_principal_credentials()
        __credential = ClientSecretCredential(tenant_id=tenant_id,
                                              client_id=client_id,
                                              client_secret=client_secret)
        return __credential

    def __get_blob_service_client(self):

        """
            Obtain a BlobServiceClient using the specified Azure credentials.
            Returns:
            - BlobServiceClient: Azure Blob Service client.
        """
        __blob_service_client = BlobServiceClient(account_url=settings.azure_blob_url.format(
            account_name=self.storage_account_name),
            credential=self.__get_credentials())
        return __blob_service_client

    def __get_container_client(self):
        """
            Obtain a ContainerClient for the specified Azure Storage container.
            Returns:
            - ContainerClient: Azure Blob Storage container client.
        """
        __container_client = self.__get_blob_service_client().get_container_client(self.container_name)
        return __container_client

    def __get_blob(self, blob_path, blob_file_name):
        """
            Generate the full blob name using the provided blob path and file name.
            Parameters:
            - blob_path (str): Blob path within the container.
            - blob_file_name (str): Name of the file to be uploaded.
            Returns:
            - str: Full blob name.
        """
        blob_name = os.path.join(blob_path, blob_file_name)
        return blob_name

    def __get_blob_client(self, blob_name):
        """
        Obtain a BlobClient for the specified blob.
            Parameters:
            - blob_name (str): Full blob name.
            Returns:
            - BlobClient: Azure Blob client.
        """
        return self.__get_container_client().get_blob_client(blob=blob_name)

    def _upload_chunk(self, blob_client, chunk):
        """
            Upload a chunk of data to the specified blob.
            Parameters:
            - blob_client (BlobClient): Azure Blob client.
            - chunk (bytes): Data chunk to be uploaded.
        """
        blob_client.append_block(chunk)

    def _create_or_get_append_blob(self, blob_client):
        """
            Create an append blob or raise an exception if it already exists.
            Parameters:
            - blob_client (BlobClient): Azure Blob client.
        """
        try:
            # Try to get blob properties (checks if the content exists)
            blob_client.get_blob_properties()
            print("Blob exists, delete and overwrite")
            self.__get_container_client().delete_blob(blob_client.get_blob_properties().name)
            print("deletion completed, overwriting...")
            blob_client.create_append_blob()
            # raise Exception("file already exists upload rename the file")
        except ResourceNotFoundError:
            # Blob does not exist, create it
            print("Blob does not exist")
            blob_client.create_append_blob()

    def upload_file_from_local(self,
                               blob_upload_path,
                               local_file_path,
                               file_name,
                               chunk_size=4 * 1024 * 1024,
                               num_threads=8):

        """
            Upload a file from the local file system to Azure Blob Storage using concurrent execution.
            Parameters:
            - blob_upload_path (str): Blob path within the container.
            - local_file_path (str): Local file path.
            - file_name (str): Name of the file to be uploaded.
            - chunk_size (int): Size of each data chunk for parallel upload.
            - num_threads (int): Number of threads for parallel execution.
        """
        # Ensure the append blob exists or create it
        print("in method start: upload_file_from_local")
        blob_name = self.__get_blob(blob_path=blob_upload_path,
                                    blob_file_name=file_name)
        print(f"upload target location: {blob_name}")
        blob_client = self.__get_blob_client(blob_name=blob_name)
        self._create_or_get_append_blob(blob_client)
        print(f"uploading from {local_file_path} with chunk size: {chunk_size} and {num_threads} threads")
        with open(local_file_path, "rb") as data:
            chunks = iter(lambda: data.read(chunk_size), b"")
            with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
                # Create a list of future objects for parallel uploads
                futures = [executor.submit(self._upload_chunk, blob_client, chunk) for chunk in chunks]
                # Wait for all uploads to complete and handle any exceptions
                completed, not_completed = concurrent.futures.wait(futures,return_when=concurrent.futures.ALL_COMPLETED)
                # Handle any exceptions that occurred during the uploads
                for future in completed:
                    if future.exception() is not None:
                        print(f"Exception during upload: {future.exception()}")
                        raise FileUploadException(f"Exception during upload: {future.exception()}")
        print("in method end: upload_file_from_local")
        return blob_client.url

    def upload_file_from_stream(self,
                                blob_upload_path,
                                file_stream,
                                file_name,
                                chunk_size=4 * 1024 * 1024,
                                num_threads=8):
        """
            Upload a file from the file stream to Azure Blob Storage using concurrent execution.
            Parameters:
            - blob_upload_path (str): Blob path within the container.
            - local_file_path (str): Local file path.
            - file_name (str): Name of the file to be uploaded.
            - chunk_size (int): Size of each data chunk for parallel upload.
            - num_threads (int): Number of threads for parallel execution.
        """

        # Ensure the append blob exists or create it
        print("in method start: upload_file_from_stream")
        blob_name = self.__get_blob(blob_path=blob_upload_path,
                                    blob_file_name=file_name)
        print(f"upload target location: {blob_name}")
        print(blob_name)
        blob_client = self.__get_blob_client(blob_name=blob_name)
        self._create_or_get_append_blob(blob_client)
        print(f"uploading with chunk size: {chunk_size} and {num_threads} threads")

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            # Create a list of future objects for parallel uploads
            futures = [executor.submit(self._upload_chunk, blob_client, chunk) for chunk in
                       iter(lambda: file_stream.read(chunk_size), b"")]
            # Wait for all uploads to complete and handle any exceptions
            completed, not_completed = concurrent.futures.wait(futures, return_when=concurrent.futures.ALL_COMPLETED)
            # Handle any exceptions that occurred during the uploads
            for future in completed:
                if future.exception() is not None:
                    print(f"Exception during upload: {future.exception()}")
                    raise FileUploadException(f"Exception during upload: {future.exception()}")
        print("in method end: upload_file_from_stream")
        return blob_client.url

    def delete_file(self, blob_path, file_name):
        """
        Delete a file from Azure Blob Storage.

        Parameters:
        - blob_upload_path (str): Blob path within the container.
        - file_name (str): Name of the file to be deleted.
        """
        print("In method start: delete_file")
        blob_name = self.__get_blob(blob_path=blob_path, blob_file_name=file_name)
        print(f"Delete target location: {blob_name}")
        blob_client = self.__get_blob_client(blob_name=blob_name)
        try:
            blob_client.delete_blob()
            print(f"Blob {blob_name} deleted successfully.")
        except ResourceNotFoundError:
            print(f"Blob {blob_name} not found.")
        print("In method end: delete_file")
