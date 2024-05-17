import os
import settings
from services.blob_storage_service import BlobStorageService
from flask import request
from azure.ai.ml import MLClient
from azure.identity import ClientSecretCredential
from exceptions.username_exception import UserNameException
from utils.constants import Messages


class Utils:

    @staticmethod
    def get_azure_service_principal_credentials():
        return os.environ["AZURE_TENANT_ID"], os.environ["AZURE_CLIENT_ID"], os.environ["AZURE_CLIENT_SECRET"]

    @staticmethod
    def get_client_credentials():
        tenant_id, client_id, client_secret = Utils.get_azure_service_principal_credentials()
        return ClientSecretCredential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)

    @staticmethod
    def get_workspace_client(workspace_name: str):
        return MLClient(subscription_id=os.getenv('AZURE_SUBS_ID'),
                        resource_group_name=os.getenv('AZURE_RG'),
                        workspace_name=workspace_name,
                        credential=Utils.get_client_credentials())

    @staticmethod
    def get_user_from_headers():
        user = request.headers.get('UserName')
        print(f"UserName is : {user}")
        if not user:
            raise UserNameException(Messages.user_name_exception.value)
        return user

    @staticmethod
    def get_datastore_base_uri(subscription_id: str,
                               resource_group_name: str,
                               datastore_name: str,
                               workspace_name: str):
        base_uri = settings.datastore_base_uri.format(subscription_id=subscription_id,
                                                      resource_group_name=resource_group_name,
                                                      datastore_name=datastore_name,
                                                      workspace_name=workspace_name)
        return base_uri

    @staticmethod
    def get_blob_file_uri(ml_client: MLClient, file_path: str, file_name: str):
        file_url = (Utils.get_datastore_base_uri(subscription_id=ml_client.subscription_id,
                                                 resource_group_name=ml_client.resource_group_name,
                                                 datastore_name=ml_client.datastores.get_default().name,
                                                 workspace_name=ml_client.workspace_name) +
                    "{datastore_path}/{filename}".format(datastore_path=file_path.strip('/\\'),
                                                         filename=file_name))
        return file_url

    @staticmethod
    def get_datastore_base_uri(subscription_id: str,
                               resource_group_name: str,
                               datastore_name: str,
                               workspace_name: str):
        base_uri = settings.datastore_base_uri.format(subscription_id=subscription_id,
                                                      resource_group_name=resource_group_name,
                                                      datastore_name=datastore_name,
                                                      workspace_name=workspace_name)
        return base_uri

    @staticmethod
    def get_datastore_file_uri(ml_client: MLClient, file_path: str, file_name: str):
        file_url = Utils.get_datastore_base_uri(subscription_id=ml_client.subscription_id,
                                                resource_group_name=ml_client.resource_group_name,
                                                datastore_name=ml_client.datastores.get_default().name,
                                                workspace_name=ml_client.workspace_name) + \
                   "{datastore_path}/{filename}".format(datastore_path=file_path.strip('/\\'),
                                                        filename=file_name)
        return file_url

    def upload_file_stream_to_workspace_blob_storage(self,
                                                     file_stream: str,
                                                     ml_client: MLClient
                                                     ) -> dict:
        """
        :rtype: object
        :param file_stream:
        """
        store = ml_client.datastores.get_default()
        target_path = settings.default_blob_folder_path.format(workspace_name=ml_client.workspace_name,
                                                               user=Utils.get_user_from_headers())
        blob_service = BlobStorageService(storage_account_name=store.account_name,
                                          container_name=store.container_name)
        file, file_name = file_stream, file_stream.filename
        blob_url = blob_service.upload_file_from_stream(blob_upload_path=target_path,
                                                        file_stream=file,
                                                        file_name=file_name)
        data_store_url = Utils.get_datastore_file_uri(ml_client=ml_client,
                                                      file_name=file_name,
                                                      file_path=target_path)

        print("in method end: upload_file_stream_to_workspace_blob_storage")
        return {"datastore_url": data_store_url,
                "blob_url": blob_url}

    def upload_local_file_to_workspace_blob_storage(self,
                                                    file_name: str,
                                                    local_file_path: str,
                                                    ml_client: MLClient
                                                    ) -> dict:
        """
        :param local_file_path:
        :param file_name:
        :rtype: object
        """
        store = ml_client.datastores.get_default()
        target_path = settings.default_blob_folder_path.format(workspace_name=ml_client.workspace_name,
                                                               user=Utils.get_user_from_headers())
        blob_service = BlobStorageService(storage_account_name=store.account_name,
                                          container_name=store.container_name)

        blob_url = blob_service.upload_file_from_local(blob_upload_path=target_path,
                                                       local_file_path=local_file_path,
                                                       file_name=file_name)
        data_store_url = Utils.get_datastore_file_uri(ml_client=ml_client,
                                                      file_name=file_name,
                                                      file_path=target_path)
        urls = {"datastore_url": data_store_url,
                "blob_url": blob_url}
        print(urls)
        print("in method end: upload_file_stream_to_workspace_blob_storage")
        return urls

    def delete_file_workspace_blob_storage(self,
                                           file_name: str,
                                           ml_client: MLClient
                                           ) -> None:
        """
        :param file_name:
        :rtype: None
        """

        store = ml_client.datastores.get_default()
        target_path = settings.default_blob_folder_path.format(workspace_name=ml_client.workspace_name,
                                                               user=Utils.get_user_from_headers())
        blob_service = BlobStorageService(storage_account_name=store.account_name,
                                          container_name=store.container_name)
        blob_service.delete_file(blob_path=target_path,
                                 file_name=file_name)

    def get_aml_job_status(self, job_name, ml_client: MLClient):
        job = ml_client.jobs.get(job_name)
        status = job.status
        return status.lower()
    
    def get_child_job(self,
                      parent_job_name,
                      ml_client: MLClient):
        return next(ml_client.jobs.list(parent_job_name=parent_job_name))
