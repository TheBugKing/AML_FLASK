from utils.utils import Utils


class FileOperationService:
    def __int__(self, storage_account_name, container_name):
        self.utils = Utils()
        self.storage_account_name = storage_account_name
        self.container_name = container_name

    def file_stream_upload(self, data, run_data_processing=False):
        self.utils.upload_file_stream_to_workspace_blob_storage(
            file_stream='',
            storage_account_name=self.storage_account_name,
            container_name=self.container_name)

