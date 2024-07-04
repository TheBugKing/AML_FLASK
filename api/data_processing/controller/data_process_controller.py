from azure.ai.ml import Input
from azure.ai.ml.constants import AssetTypes

import settings
from services.aml_job_base_service import RunAmlJobService, CheckAmlJobService
from utils.constants import StringConstants, AmlJobStatusConstants
from exceptions.file_upload_exception import FileUploadException
from dbo.llm_repository import LLMRepository


class DataProcessingController(RunAmlJobService):
    def __init__(self, workspace, input_file, config_data, compute):
        super().__init__(workspace=workspace,
                         config_data=config_data,
                         experiment=settings.data_processing_experiment)
        self.input_file = input_file
        self.data_pr_comp = settings.data_processing_component
        self.compute = compute

    def prepare_pipeline_inputs(self):
        try:
            model_input = None
            # retrieve the params model name, version, pii masking flag
            pii_masking = self.config_data['preprocess_configs']['pii_masking']
            if pii_masking:
                model_name, model_version = self.config_data['pii_configuration']['model_version'], \
                    self.config_data['pii_configuration']['model_name']
                model = self.ml_client.models.get(model_name, version=model_version)
                model_input = Input(path=model.id, type=AssetTypes.CUSTOM_MODEL)

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
                                                     "pii_masking": pii_masking,
                                                     },
                                 "input_file": {"name": self.input_file.filename,
                                                "datastore_url": input_datastore_url,
                                                "blob_url": input_blob_url},
                                 "config_file": {"name": self.config_name,
                                                 "datastore_url": config_datastore_url,
                                                 "blob_url": config_blob_url
                                                 }
                                 }
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
            pipeline_job.settings.default_compute = self.compute
            # submit job to workspace
            print("submitting the pipeline to the workspace")
            pipeline_job = self.ml_client.jobs.create_or_update(
                pipeline_job, experiment_name=self.experiment)
            # TO DO: INSERT Partial data in database table
            params = {'file_name': self.input_file.filename,
                      'file_blob_url': meta_data['input_file']['blob_url'],
                      'file_data_url': meta_data['input_file']['datastore_url'],
                      'config_blob_url': meta_data['config_file']['blob_url'],
                      'config_data_url': meta_data['config_file']['datastore_url'],
                      'ml_job_name': pipeline_job.name,
                      'ml_job_status': pipeline_job.status,
                      'workspace_name': self.workspace
                      }
            dbo = LLMRepository()
            dbo.insert_file_upload_record(data=params, job_complete=False)
        except FileUploadException as e:
            raise e
        except Exception as e:
            self.utils.delete_file_workspace_blob_storage(file_name=self.input_file.filename,
                                                          ml_client=self.ml_client)
            self.utils.delete_file_workspace_blob_storage(file_name=self.config_name,
                                                          ml_client=self.ml_client)
            raise e


class RetrieveDataProcessingJob(CheckAmlJobService):
    def __init__(self, workspace, job_name, table_id_data):
        super().__init__(workspace=workspace,
                         job_name=job_name,
                         table_id_data=table_id_data)
        self.status = None

    def retrieve_job_status(self):
        try:
            self.status = self.utils.get_aml_job_status(job_name=self.job_name,
                                                        ml_client=self.ml_client)
            if self.status.lower() == AmlJobStatusConstants.completed.value.lower():
                return self.post_completion()
            else:
                dbo = LLMRepository()
                dbo.update_job_status(table_name=settings.ftfiles_table,
                                      id_column=settings.ftfiles_table_id,
                                      status=self.status)
                return self.status
        except Exception as e:
            raise e

    def post_completion(self):
        # TO DO:
        # use this only if child job produces output
        child_job = self.utils.get_child_job(parent_job_name=self.job_name,
                                             ml_client=self.ml_client)
        reused = self.utils.is_job_reused(job_instance=child_job)
        output_job_name = reused['output_job_name']
        data_processing_output_path = settings.data_processing_output_path.format(job_name=output_job_name)
        output_datastore_url = self.utils.get_datastore_file_uri(ml_client=self.ml_client,
                                                                 file_path=data_processing_output_path,
                                                                 file_name=StringConstants.output_preprocessed.value)
        output_blob_url = self.utils.get_blob_file_uri(ml_client=self.ml_client,
                                                       file_path=data_processing_output_path,
                                                       file_name=StringConstants.output_preprocessed.value)
        print("output_datastore_url", output_datastore_url)
        print("output_blob_url", output_blob_url)
        train_datastore_url = self.utils.get_datastore_file_uri(ml_client=self.ml_client,
                                                                file_path=data_processing_output_path,
                                                                file_name=StringConstants.train_preprocessed.value)
        train_blob_url = self.utils.get_blob_file_uri(ml_client=self.ml_client,
                                                      file_path=data_processing_output_path,
                                                      file_name=StringConstants.train_preprocessed.value)
        print("train_datastore_url", train_datastore_url)
        print("train_blob_url", train_blob_url)
        test_datastore_url = self.utils.get_datastore_file_uri(ml_client=self.ml_client,
                                                               file_path=data_processing_output_path,
                                                               file_name=StringConstants.test_preprocessed.value)
        test_blob_url = self.utils.get_blob_file_uri(ml_client=self.ml_client,
                                                     file_path=data_processing_output_path,
                                                     file_name=StringConstants.test_preprocessed.value)
        print("test_datastore_url", test_datastore_url)
        print("test_blob_url", test_blob_url)
        validation_datastore_url = self.utils.get_datastore_file_uri(ml_client=self.ml_client,
                                                                     file_path=data_processing_output_path,
                                                                     file_name=StringConstants.validation_preprocessed.value)
        validation_blob_url = self.utils.get_blob_file_uri(ml_client=self.ml_client,
                                                           file_path=data_processing_output_path,
                                                           file_name=StringConstants.validation_preprocessed.value)
        print("validation_datastore_url", validation_datastore_url)
        print("validation_blob_url", validation_blob_url)

        params = {'output_datastore_url': output_datastore_url,
                  'output_blob_url': output_blob_url,
                  'train_datastore_url': train_datastore_url,
                  'train_blob_url': train_blob_url,
                  'test_datastore_url': test_datastore_url,
                  'test_blob_url': test_blob_url,
                  'validation_datastore_url': validation_datastore_url,
                  'validation_blob_url': validation_blob_url,
                  'fild_id': self.table_id_data}
        dbo = LLMRepository()
        dbo.insert_file_upload_record(data=params, job_complete=True)
        return self.status
