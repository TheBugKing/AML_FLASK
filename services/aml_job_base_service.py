import os
import json
import settings
from uuid import uuid4
from abc import ABC, abstractmethod
from utils.utils import Utils
import tempfile


class RunAmlJobService(ABC):
    def __init__(self, workspace, config_data, experiment):
        self.workspace = workspace
        self.utils = Utils()
        self.config_data = json.loads(config_data)
        self.experiment = experiment
        self.ml_client = Utils.get_workspace_client(workspace_name=self.workspace)
        self.config_name = None

    @abstractmethod
    def prepare_pipeline_inputs(self):
        pass

    @abstractmethod
    def run_pipeline(self):
        pass
    
    def upload_config(self, config_data: dict) -> object:
        """
        :param config_data:
        """
        current_file_dir = os.path.dirname(os.path.abspath(__file__))
        with tempfile.TemporaryDirectory(dir=current_file_dir) as temp_dir:
            print("Temporary directory:", temp_dir)
            local_file_path = temp_dir
            self.config_name = settings.config_file_base_name + '_' + str(uuid4()) + '.' + settings.file_extension['json']
            file_path = os.path.join(local_file_path, self.config_name)
            with open(file_path, 'w') as json_file:
                json.dump(config_data, json_file, indent=2)
            urls = self.utils.upload_local_file_to_workspace_blob_storage(file_name=self.config_name,
                                                                          local_file_path=file_path,
                                                                          ml_client=self.ml_client)
        return {"config_urls": urls}

    def upload_input_file(self, file_stream: object):
        urls = self.utils.upload_file_stream_to_workspace_blob_storage(file_stream=file_stream,
                                                                       ml_client=self.ml_client)
        return {"input_file_urls": urls}
    

class CheckAmlJobService(ABC):
    def __init__(self, workspace, job_name, table_id_data):
        self.workspace = workspace
        self.utils = Utils()
        self.job_name = job_name
        self.table_id_data = table_id_data
        self.ml_client = Utils.get_workspace_client(workspace_name=self.workspace)

    @abstractmethod
    def retrieve_job_status(self):
        pass

    @abstractmethod
    def post_completion(self):
        pass
