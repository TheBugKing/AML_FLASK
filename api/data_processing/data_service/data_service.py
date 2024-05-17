import json
import os
from abc import ABC
from pprint import pprint

from azure.ai.ml import MLClient, Input
from azure.ai.ml.constants import AssetTypes

import settings
from services.aml_job_base_service import RunAmlJobService, CheckAmlJobService
from utils.constants import StringConstants, AmlJobStatusConstants
from exceptions.file_upload_exception import FileUploadException


class DataProcessingService(RunAmlJobService):
    def __init__(self, workspace, input_file, config_data):
        super().__init__(workspace=workspace,
                         input_file=input_file,
                         config_data=config_data,
                         experiment=settings.data_processing_experiment)
        self.data_pr_comp = settings.data_processing_component

    def prepare_pipeline_inputs(self):
        try:
            model_input = None
            # Create the Input configurations
            # TO DO:
            # retrieve the params model name, version, pii masking flag
            # pii_masking = self.config_data['pii_masking']
            # if pii_masking:
            #     model_name, model_version =  self.config_data['model_name'],  self.config_data['model_name']
            #     model = ml_client.models.get(model_name, version=model_version)
            #     model_input = Input(path=model.id, type=AssetTypes.CUSTOM_MODEL)

            try:
                config_file_upload_data = self.upload_config(config_data=self.config_data)
                input_file_upload_data = self.upload_input_file(file_stream=self.input_file)
            except FileUploadException as e:
                raise e
            
            input_datastore_url = input_file_upload_data["input_file_urls"]["datastore_url"]
            input_blob_url = input_file_upload_data["input_file_urls"]["blob_url"]
            config_datastore_url = config_file_upload_data["config_urls"]["datastore_url"]
            config_blob_url = config_file_upload_data["config_urls"]["blob_url"]

            input_data = Input(path=input_datastore_url, type=AssetTypes.URI_FILE)
            config_file = Input(path=config_datastore_url, type=AssetTypes.URI_FILE)
            trigger_meta_data = {"component_input": {"input_data": input_data,
                                                     "config_file": config_file,
                                                     # TO DO: replace from config file
                                                     "model_input": model_input,
                                                     "pii_masking": False,
                                                     "compute": self.config_data["compute"]
                                                     },
                                 "input_file_urls": {"name": self.input_file.filename,
                                                     "datastore_url": input_datastore_url,
                                                     "blob_url": input_blob_url},
                                 "config_file": {"name": self.config_name,
                                                 "datastore_url": config_datastore_url,
                                                 "blob_url": config_blob_url
                                                 }
                                 }

            pprint(trigger_meta_data, indent=2)
            return trigger_meta_data
        except Exception as e:
            raise e

    def run_pipeline(self):
        try:
            meta_data = self.prepare_pipeline_inputs()
            data_pro_component = self.ml_client.components.get(name=self.data_pr_comp,
                                                               label=StringConstants.latest.value.lower())
            # Create a job instance from pipeline
            print("Creating a job instance for pipeline")
            if meta_data["component_input"]["pii_masking"]:
                pipeline_job = data_pro_component(
                    input_data=meta_data["component_input"]["input_data"],
                    model_input=meta_data["component_input"]["model_input"],
                    config_file=meta_data["component_input"]["config_file"])
            else:
                pipeline_job = data_pro_component(
                    input_data=meta_data["component_input"]["input_data"],
                    config_file=meta_data["component_input"]["config_file"])
            # Assign Compute
            print("Assigning Compute")
            # TO DO:
            # get from config
            pipeline_job.settings.default_compute = meta_data["component_input"]["compute"]
            # submit job to workspace
            print("submitting the pipeline to the workspace")
            pipeline_job = self.ml_client.jobs.create_or_update(
                pipeline_job, experiment_name=self.experiment)

            # TO DO: INSERT Partial data in database table
            # here
        except FileUploadException as e:
            raise e
        except Exception as e:
            self.utils.delete_file_workspace_blob_storage(file_name=self.input_file.filename,
                                                          ml_client=self.ml_client)
            self.utils.delete_file_workspace_blob_storage(file_name=self.config_name,
                                                          ml_client=self.ml_client)
            raise e
        

class RetrieveDataProcessingJob(CheckAmlJobService):
    def __init__(self, workspace, job_name, job_db_id):
        super().__init__(workspace=workspace,
                         job_name=job_name,
                         job_db_id=job_db_id)
    
    def retrieve_job_status(self):
        status = self.utils.get_aml_job_status(job_name=self.job_name,
                                      ml_client=self.ml_client)
        if status.lower() == AmlJobStatusConstants.completed.value.lower():
            self.post_completion()
        else:
            # TO DO:
            # Update data base status
            return status
        
    def post_completion(self):
        pass
    


    
