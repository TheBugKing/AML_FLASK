import json

from flask import request
from flask import Blueprint

import settings
from utils.utils import Utils

data_pro_bp = Blueprint('data_pro_bp', __name__)


@data_pro_bp.route('/upload/dataset', methods=['GET', 'POST'])
def root():
    if request.method == 'GET':
        return "SERVER iS RUNNING", 200
    elif request.method == 'POST':
        file = request.files.get('File')
        run_data_job = request.form.get('RunDataProcessJob').lower() == 'true' \
            if request.form.get('RunDataProcessJob') else None

        config = request.form.get('ConfigFile')
        print(config)
        workspace = request.form.get('Workspace')
        if run_data_job:
            pass
        else:
            utils = Utils()
            # utils.upload_file_stream_to_workspace_blob_storage(file_stream=config,
            #                                                    workspace_name=workspace)
            from services.aml_operation_service import AmlService
            aml = AmlService(workspace=workspace)
            aml.upload_config(config_data=json.loads(config))
        return "ok", 200


@data_pro_bp.route('/delete/dataset', methods=['DELETE'])
def delete_file():
    if request.method == 'DELETE':
        file = request.files.get('File')
        workspace = request.form.get('Workspace')
        utils = Utils()
        utils.delete_file_workspace_blob_storage(file_name=file.filename,
                                                 workspace_name=workspace)
    return "ok", 200
