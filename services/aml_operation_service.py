import os
import json
import settings
from uuid import uuid4
from abc import ABC, abstractmethod
from utils.utils import Utils
import tempfile


class AmlService(ABC):
    def __init__(self, workspace):
        self.workspace = workspace
        self.ml_client = Utils.get_workspace_client(workspace_name=self.workspace)

    def get_pipeline_component(self, key):
        comp_name = os.getenv(key)
        if not comp_name:
            raise Exception(f"component {key} not found in environment variables")
        return comp_name

    # @abstractmethod
    # def prepare_pipeline_inputs(self):
    #     pass
    #
    # @abstractmethod
    # def run_pipeline(self, experiment_name:str,
    #                  pipeline_job_instance: object):
        pass

    def upload_config(self, config_data: dict) -> object:
        """

        :param config_data:
        """
        current_file_dir = os.path.dirname(os.path.abspath(__file__))
        with tempfile.TemporaryDirectory(dir=current_file_dir) as temp_dir:
            print("Temporary directory:", temp_dir)
            local_file_path = temp_dir
            config_name = settings.config_file_base_name + '_' + str(uuid4()) + '.' + settings.file_extension['json']
            file_path = os.path.join(local_file_path, config_name)
            with open(file_path, 'w') as json_file:
                json.dump(config_data, json_file, indent=2)
            Utils().upload_local_file_to_workspace_blob_storage(file_name=config_name,
                                                                local_file_path=file_path,
                                                                workspace_name=self.workspace)
            return config_name
