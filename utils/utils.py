import os
import settings
import mlflow
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
    def get_blob_base_uri(container_name: str,
                          account_name: str):
        base_uri = settings.azure_blob_url.format(account_name=account_name,
                                                  container_name=container_name)
        return base_uri

    @staticmethod
    def get_blob_file_uri(ml_client: MLClient, file_path: str, file_name: str):
        file_url = (Utils.get_blob_base_uri(container_name=ml_client.datastores.get_default().container_name,
                                            account_name=ml_client.datastores.get_default().account_name) +
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
                      ml_client: MLClient,
                      all_jobs=False):
        return next(ml_client.jobs.list(parent_job_name=parent_job_name)) if not all_jobs \
            else ml_client.jobs.list(parent_job_name=parent_job_name)

    def is_job_reused(self,
                      job_instance: object):
        output_job_name = None
        reused = job_instance.properties.get('azureml.isreused')
        reused_flag = reused == 'true'
        if not reused_flag:
            output_job_name = job_instance.name
        elif reused_flag and job_instance.properties.get('azureml.reusedrunid'):
            output_job_name = job_instance.properties.get('azureml.reusedrunid')
        if not output_job_name:
            raise Exception("unable to configure reused job check")
        return {'is_reused': reused_flag, 'output_job_name': output_job_name}

    def set_ml_flow_tracking_uri(self, ml_client: MLClient):
        mlflow.set_tracking_uri(ml_client.workspaces.get(ml_client.workspace_name).mlflow_tracking_uri)
        print("ML flow URI set successfully", ml_client.workspaces.get(ml_client.workspace_name).mlflow_tracking_uri)

    def unset_ml_flow_uri(self):
        mlflow.set_tracking_uri(None)
        print("ML flow URI unset successfully")

    def retrieve_metrics_from_jobs_recursively(self, ml_client: MLClient,
                                               job_name, data_dict):
        """
        Recursively retrieves metrics from ML flow runs for a given job and its embedded jobs.
        Parameters:
        - ml_client (MLClient): An instance of the MLClient class.
        - job_name (str): The name of the job to retrieve metrics for.
        - data_dict (dict): A dictionary to store the retrieved metrics.

        Returns:
        None
        The method fetches metrics data for the specified job and its embedded jobs recursively.
        It populates the provided data_dict with the metrics information.
        Metrics are collected in key, value pairs for the latest iteration, and then the method
        fetches all historic iterations for each key. The metrics data is organized into a main_list
        containing dictionaries for each metric key, where each dictionary includes information
        about different iterations, such as step, timestamp, key, and value.
        The method recursively calls itself to retrieve metrics for all embedded jobs within the
        specified job_name.

        NOTE: Set the MLFlow tracking uri before calling the method and
        post execution unset the MLFlow tracking uri
        """
        # NOTE: Set the MLFlow tracking uri before calling the method and
        # NOTE: post execution unset the MLFlow tracking uri
        main_list = []
        # get the metrics in key, values pairs for latest iteration
        # we will use the keys to fetch all the historic iterations
        run = mlflow.get_run(run_id=job_name)
        display_name = run.data.tags['mlflow.runName']
        # check if metrics are available
        if run.data.metrics:
            # loop through the keys and get all iterations for each keys
            # graph or table data will have multiple iterations
            # single static metrics will have only one iteration
            for key in run.data.metrics.keys():
                temp = []
                sub_run = mlflow.tracking.MlflowClient().get_metric_history(job_name, key)
                for step, i in enumerate(sub_run):
                    temp.append({i.key: Utils.replace_nan(obj=i.value, value=None), "step": step,
                                 "timestamp": i.timestamp})
                main_list.append({key: temp})
            # update the dictionary inplace
            data_dict.update({display_name: main_list})
            # recursively check for all embedded jobs
            [self.retrieve_metrics_from_jobs_recursively(ml_client=ml_client,
                                                         job_name=jobs.name, data_dict=data_dict) for jobs in \
             self.get_child_job(ml_client=ml_client, parent_job_name=job_name, all_jobs=True)]
        else:
            # recursively check for all embedded jobs
            [self.retrieve_metrics_from_jobs_recursively(ml_client=ml_client,
                                                         job_name=jobs.name, data_dict=data_dict) for jobs in
             self.get_child_job(
                 ml_client=ml_client, parent_job_name=job_name, all_jobs=True)]
