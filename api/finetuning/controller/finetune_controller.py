from azure.ai.ml import Input
from azure.ai.ml.constants import AssetTypes

import settings
from services.aml_job_base_service import RunAmlJobService, CheckAmlJobService
from utils.constants import StringConstants, AmlJobStatusConstants
from exceptions.file_upload_exception import FileUploadException
from dbo.llm_repository import LLMRepository


class FinetuneController(RunAmlJobService):
    def __init__(self, workspace, config_data):
        super().__init__(workspace=workspace,
                         config_data=config_data,
                         experiment=settings.data_processing_experiment)
        self.finetune_component_name = settings.finetune_component_name

    def prepare_pipeline_inputs(self):
        try:
            try:
                config_file_upload_data = self.upload_config(
                    config_data=self.config_data)
            except FileUploadException as e:
                raise e

            trigger_meta_data = {
                "config_datastore_url": config_file_upload_data["config_urls"]["datastore_url"],
                "config_blob_url": config_file_upload_data["config_urls"]["blob_url"],
                "train_file_uri": self.config_data['input_data']['paths']['train'].strip(),
                "test_file_uri": self.config_data['input_data']['paths']['test'].strip(),
                "validation_file_uri": self.config_data['input_data']['paths']['validate'].strip(),
                "model_input": self.config_data['model']['model_uri'].strip(),
                "ft_compute_name": self.config_data['compute_name'],
                "compute_instance_count": self.config_data['finetuning_config']['optimizations']['deepspeed']['compute_instance_count'],
                "num_of_gpu_per_instance": self.config_data['finetuning_config']['optimizations']['deepspeed']['num_of_gpu_per_instance'],
                "task_type": self.config_data['finetune_task_type'],
                "ft_task_id": self.config_data['finetune_task_id'],
                "workspace_id": self.config_data['workspace_id'],
                "file_id": self.config_data['file_id'],
                "trainer_config": self.config_data['finetune_config'],
                "llm_model_id": self.config_data['model']['model_id'],
                "trained_model_instance_name": self.config_data['model']["trained_model_instance_name"],
            }

            return trigger_meta_data
        except Exception as e:
            raise e

    def run_pipeline(self):
        try:
            meta_data = self.prepare_pipeline_inputs()
            finetune_component = self.ml_client.components.get(name=self.finetune_component_name,
                                                               label=StringConstants.latest.value.lower())
            # Create a job instance from pipeline
            print("Creating a job instance for pipeline")
            ft_compute_name = meta_data["ft_compute_name"]
            pipeline_job = finetune_component(
                ft_config=Input(path=meta_data["config_datastore_url"],
                                type=AssetTypes.URI_FILE),
                train_data=Input(path=meta_data["train_file_uri"],
                                 type=AssetTypes.URI_FILE),
                test_data=Input(path=meta_data["test_file_uri"],
                                type=AssetTypes.URI_FILE),
                validate_data=Input(path=meta_data["validation_file_uri"],
                                    type=AssetTypes.URI_FILE),
                model_input=Input(path=meta_data["model_input"],
                                  type=AssetTypes.MLFLOW_MODEL),
                ft_compute_name=ft_compute_name,
                compute_instance_count=meta_data["compute_instance_count"],
                num_of_gpu_per_instance=meta_data['num_of_gpu_per_instance'])
            
            # Assign Compute
            print("Assigning Compute")
            # TO DO:
            # get from config
            pipeline_job.settings.default_compute = ft_compute_name
            # submit job to workspace
            print("submitting the pipeline to the workspace")
            pipeline_job = self.ml_client.jobs.create_or_update(
                pipeline_job, experiment_name=self.experiment)
            # TO DO: INSERT Partial data in database table
            params = {
                      }
            dbo = LLMRepository()
        except FileUploadException as e:
            raise e
        except Exception as e:
            self.utils.delete_file_workspace_blob_storage(file_name=self.config_name,
                                                          ml_client=self.ml_client)
            raise e


class RetrieveFineTuneJob(CheckAmlJobService):
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
        metrics = {}
        dbo = LLMRepository()
        # get metrics from mlflow
        # adding metrics to the dictionary in place
        self.utils.retrieve_metrics_from_jobs_recursively(ml_client=self.ml_client,
                                                          job_name=self.job_name,
                                                          data_dict=metrics)
        
        child_job = self.utils.get_child_job(parent_job_name=self.job_name,
                                             ml_client=self.ml_client)
        reused = self.utils.is_job_reused(job_instance=child_job)
        output_job_name = reused['output_job_name']
        model_path = settings.finetune_model_path.format(job_name=output_job_name)
        model_output_datastore_url = self.utils.get_datastore_model_path(ml_client=self.ml_client,
                                                                 model_path=model_path)
        model_output_blob_url = self.utils.get_blob_model_uri(ml_client=self.ml_client,
                                                                 model_path=model_path)
        
        # add your database logic here
        # TO DO

        return
