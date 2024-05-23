from dbo.dbo import Dbo


class LLMRepository(Dbo):
    def __init__(self):
        # initialize super constructor of Dbo class
        # use cursor object inherited from dbo to run queries
        super().__init__()

    def get_workspace_data(self, is_active=None):
        try:
            if is_active:
                params = (is_active,)
                sql = 'SELECT * FROM "WorkspaceLlm" WHERE "IsActive" = %s;'
                data = self.run_select_query(sql, params=params)
                return data
            else:
                sql = 'SELECT * FROM "WorkspaceLlm";'
                data = self.run_select_query(sql)
                return data
        except Exception as e:
            raise e
        finally:
            self.connection_close()

    def get_finetuning_tasks(self, task_id=None, task_type=None):
        try:
            if task_id:
                params = (task_id,)
                sql = 'SELECT * FROM "LlmAzureFineTuningTasks" WHERE "LlmAzureFineTuningTasksId" = %s;'
                data = self.run_select_query(sql, params=params)
                return data
            if task_type:
                params = (task_type,)
                sql = 'SELECT * FROM "LlmAzureFineTuningTasks" WHERE "FineTuningTasks" = %s;'
                data = self.run_select_query(sql, params=params)
                return data
            else:
                sql = 'SELECT * FROM "LlmAzureFineTuningTasks";'
                data = self.run_select_query(sql)
                return data
        except Exception as e:
            raise e
        finally:
            self.connection_close()

    def get_all_llm_model_data(self, collections=None, finetune_task=None,
                               model_architecture=None, model_id=None):
        try:
            sql = 'SELECT * FROM public.sp_get_model_by_filter(%s, %s, %s, %s);'
            data = self.run_select_query(sql, params=(collections, model_architecture,
                                                      finetune_task, model_id))
            return data
        except Exception as e:
            raise e
        finally:
            self.connection_close()

    def get_files(self, workspace_id=None, file_id=None):
        try:
            if workspace_id:
                params = (workspace_id,)
                sql = 'SELECT * FROM "FtFiles" WHERE "WorkspaceLlmId" = %s;'
                data = self.run_select_query(sql, params=params)
                return data
            if file_id:
                params = (file_id,)
                sql = 'SELECT * FROM "FtFiles" WHERE "FtFilesId" = %s;'
                data = self.run_select_query(sql, params=params)
                return data
            else:
                sql = 'SELECT * FROM "FtFiles";'
                data = self.run_select_query(sql)
                return data
        except Exception as e:
            raise e
        finally:
            self.connection_close()

    def get_computes(self, workspace_id=None):
        try:
            if workspace_id:
                params = (workspace_id,)
                sql = 'SELECT * FROM "LlmComputeClusters" WHERE "WorkspaceLlmId" = %s;'
                data = self.run_select_query(sql, params=params)
                return data
        except Exception as e:
            raise e
        finally:
            self.connection_close()

    def get_workspace_id_by_name(self, workspace_name):
        try:
            params = (workspace_name.strip(),)
            sql = 'SELECT "WorkspaceLlmId" FROM "WorkspaceLlm" WHERE "WorkspaceName" = %s;'
            workspace_id = self.run_select_query(sql, params=params)
            return workspace_id['WorkspaceId'] if workspace_id else None
        except Exception as e:
            raise e
        finally:
            self.connection_close()

    def insert_file_upload_record(self, data, job_complete=False):
        try:
            if job_complete:
                fild_id = data['fild_id']
                output_datastore_url, output_blob_url = data['output_datastore_url'], data['output_blob_url']
                train_datastore_url, train_blob_url = data['train_datastore_url'], data['train_blob_url']
                test_datastore_url, test_blob_url = data['test_datastore_url'], data['test_blob_url']
                val_datastore_url, val_blob_url = data['validation_datastore_url'], data['validation_blob_url']
                sql = """UPDATE "FtFiles"
                SET "MlOutputDatastorePath" = %s, "MlOutputBlobPath" = %s,
                "MlTrainFileDatastorePath" = %s, "MlTrainFileBlobPath" = %s,
                "MlTestFileDatastorePath" = %s, "MlTestFileBlobPath" = %s,
                "MlValidationFileDatastorePath" = %s, "MlValidationFileBlobPath" = %s
                WHERE "FtFilesId" = %s
                """
                self.run_sql_query(sql, params=(output_datastore_url, output_blob_url,
                                                train_datastore_url, train_blob_url,
                                                test_datastore_url, test_blob_url,
                                                val_datastore_url, val_blob_url,
                                                fild_id))
            else:
                file_name, workspace_name = data['file_name'], data['workspace_name'],
                file_blob_url, file_data_url = data['file_blob_url'], data['file_data_url']
                config_blob_url, config_data_url = data['config_blob_url'], data['config_data_url']
                ml_job_name, ml_job_status = data['ml_job_name'], data['ml_job_status']

                workspace_id = self.get_workspace_id_by_name(workspace_name=workspace_name)
                if not workspace_id:
                    raise Exception(f"Id not found for workspace: {workspace_name}")
                sql = """INSERT INTO "FtFiles" ("WorkspaceLlmId", "FileName", "InputFileBlobUrl", 
                                "InputFileDatastoreUrl", "ConfigFileBlobUrl", "ConfigFileDatastoreUrl", "MlJobName", 
                                MlJobStatus) Values (%s, %s , %s, %s, %s, %s, %s)"""
                self.run_insert_query(sql, params=(workspace_id, file_name, file_blob_url, file_data_url,
                                                   config_blob_url, config_data_url, ml_job_name, ml_job_status))
        except Exception as e:
            raise e
        finally:
            self.connection_close()

    def update_job_status(self, table_name, id_column, status):
        try:
            params = (status.strip(),)
            sql = f'UPDATE "MlJobStatus" FROM "{table_name}" WHERE "{id_column}" = %s;'
            self.run_sql_query(sql, params=params)
        except Exception as e:
            raise e
        finally:
            self.connection_close()
