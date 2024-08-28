file_extension = {
    'json': 'json'
}
config_file_base_name = 'aml_config'
azure_blob_host_url = 'https://{account_name}.blob.core.windows.net'
azure_blob_url = azure_blob_host_url + '/{container_name}/'
default_blob_folder_path = '{workspace_name}/{user}'
datastore_base_uri = ('azureml://subscriptions/{subscription_id}/resourcegroups/{resource_group_name}/workspaces/{'
                      'workspace_name}/datastores/{datastore_name}/paths/')
# TO DO:
# CHECK JOB OUTPUT PATH
data_processing_output_path = 'azureml/{job_name}/process_output'
data_processing_component = "data_processing_pipeline"
data_processing_experiment = "Data Processing Experiment"
ftfiles_table = 'FtFiles'
ftfiles_table_id = ftfiles_table + 'Id'
finetune_model_path = 'azureml/{job_name}/model_outputs'

