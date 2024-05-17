import json
from flask import request, jsonify
from flask import Blueprint
from services.response_service import ResponseService

import settings
from utils.utils import Utils
from api.data_processing.data_service.data_service import DataProcessingService

data_pro_bp = Blueprint('data_pro_bp', __name__)


@data_pro_bp.route('/upload/dataset', methods=['GET', 'POST'])
def dataprocess_file():
    try:
        if request.method == 'POST':
            file = request.files.get('File')
            workspace_name = request.form.get('WorkspaceName')
            data_pr_config = request.form.get("ConfigFile")
            validate_param = [param_name for param_name, param_value in {
                'File': file, 'WorkspaceName': workspace_name, 'ConfigFile': data_pr_config}.items() if not param_value]
            if validate_param:
                status_code=400
                return jsonify(ResponseService.get_bad_request_message(params=validate_param,
                                                                    status_code=status_code)), status_code
            dp = DataProcessingService(workspace=workspace_name,
                                        input_file=file,
                                        config_data=data_pr_config)
            res = dp.run_pipeline()
            status_code =200
        elif request.method == 'GET':
            pass
        return jsonify(ResponseService.get_success_message(status_code=status_code)), status_code
    except Exception as e:
        status_code=500
        return jsonify(ResponseService.get_exception_message(msg=str(e), status_code=status_code)), status_code


@data_pro_bp.route('/delete/dataset', methods=['DELETE'])
def delete_file():
    try:
        if request.method == 'DELETE':
            file_name = request.form.get('file_name')
            workspace_name = request.form.get('WorkspaceName')
            validate_param = [param_name for param_name, param_value in {'workspace_name': workspace_name, 'file_name': file_name}.items() if not param_value]
            if validate_param:
                status_code=400
                return jsonify(ResponseService.get_bad_request_message(params=validate_param,
                                                                    status_code=status_code)), status_code
            utils = Utils()
            utils.delete_file_workspace_blob_storage(file_name=file_name,
                                                    workspace_name=workspace_name)
            status_code =200
            return jsonify(ResponseService.get_success_message(status_code=status_code)), status_code
    except Exception as e:
        status_code=500
        return jsonify(ResponseService.get_exception_message(msg=str(e), status_code=status_code)), status_code

