import os
import settings
from services.blob_storage_service import BlobStorageService
from flask import request
from azure.ai.ml import MLClient
from azure.identity import ClientSecretCredential
from exceptions.username_exception import UserNameException
from utils.constants import ExceptionMessages


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
            raise UserNameException(ExceptionMessages.user_name_exception.value)
        return user

    def upload_file_stream_to_workspace_blob_storage(self,
                                                     file_stream: str,
                                                     workspace_name: str
                                                     ) -> object:
        """
        :param workspace_name:
        :rtype: object
        :param file_stream:
        """

        ml_client = self.get_workspace_client(workspace_name=workspace_name)
        store = ml_client.datastores.get_default()
        target_path = settings.default_blob_folder_path.format(workspace_name=workspace_name,
                                                               user=Utils.get_user_from_headers())
        blob_service = BlobStorageService(storage_account_name=store.account_name,
                                          container_name=store.container_name)
        file, file_name = file_stream, file_stream.filename
        blob_service.upload_file_from_stream(blob_upload_path=target_path,
                                             file_stream=file,
                                             file_name=file_name)

        # if return_url:

        #     print("generating urls")

        #     # target_path should have trailing slash '/'

        #     data_store_url = self.get_datastore_asset_url(subscription_id=ml_client.subscription_id,

        #                                                   resource_group_name=ml_client.resource_group_name,

        #                                                   workspace_name=ml_client.workspace_name,

        #                                                   datastore_name=store.name,

        #                                                   datastore_path=target_path,

        #                                                   filename=file_name)

        #     # target_path should not have any trailing slashes '/'

        #     blob_url = self.get_blob_url_for_datastore_asset(storage_account_name=store.account_name,

        #                                                      container_name=store.container_name,

        #                                                      datastore_path=target_path,

        #                                                      filename=file_name)

        #     print("in method end: upload_file_stream_to_workspace_blob_storage")

        #     return {"datastore_url:": data_store_url,

        #             "blob_url": blob_url}

        print("in method end: upload_file_stream_to_workspace_blob_storage")

    def upload_local_file_to_workspace_blob_storage(self,
                                                    file_name: str,
                                                    local_file_path: str,
                                                    workspace_name: str
                                                    ) -> object:
        """
        :param local_file_path:
        :param file_name:
        :param workspace_name:
        :rtype: object
        """

        ml_client = self.get_workspace_client(workspace_name=workspace_name)
        store = ml_client.datastores.get_default()
        target_path = settings.default_blob_folder_path.format(workspace_name=workspace_name,
                                                               user=Utils.get_user_from_headers())
        blob_service = BlobStorageService(storage_account_name=store.account_name,
                                          container_name=store.container_name)

        blob_service.upload_file_from_local(blob_upload_path=target_path,
                                            local_file_path=local_file_path,
                                            file_name=file_name)

        # if return_url:

        #     print("generating urls")

        #     # target_path should have trailing slash '/'

        #     data_store_url = self.get_datastore_asset_url(subscription_id=ml_client.subscription_id,

        #                                                   resource_group_name=ml_client.resource_group_name,

        #                                                   workspace_name=ml_client.workspace_name,

        #                                                   datastore_name=store.name,

        #                                                   datastore_path=target_path,

        #                                                   filename=file_name)

        #     # target_path should not have any trailing slashes '/'

        #     blob_url = self.get_blob_url_for_datastore_asset(storage_account_name=store.account_name,

        #                                                      container_name=store.container_name,

        #                                                      datastore_path=target_path,

        #                                                      filename=file_name)

        #     print("in method end: upload_file_stream_to_workspace_blob_storage")

        #     return {"datastore_url:": data_store_url,

        #             "blob_url": blob_url}

        print("in method end: upload_file_stream_to_workspace_blob_storage")

    def delete_file_workspace_blob_storage(self,
                                           file_name: str,
                                           workspace_name: str
                                           ) -> None:
        """
        :param file_name:
        :param workspace_name:
        :rtype: None
        """

        ml_client = self.get_workspace_client(workspace_name=workspace_name)
        store = ml_client.datastores.get_default()
        target_path = settings.default_blob_folder_path.format(workspace_name=workspace_name,
                                                               user=Utils.get_user_from_headers())
        blob_service = BlobStorageService(storage_account_name=store.account_name,
                                          container_name=store.container_name)

        blob_service.delete_file(blob_path=target_path,
                                 file_name=file_name)
