from flask import request, jsonify
from flask import Blueprint
from services.response_service import ResponseService
from utils.utils import Utils
from api.data_processing.controller.data_process_controller import DataProcessingController, RetrieveDataProcessingJob

data_pro_bp = Blueprint('data_pro_bp', __name__)


@data_pro_bp.route('/upload/dataset', methods=['GET', 'POST'])
def dataprocess_file():
    try:
        if request.method == 'POST':
            file = request.files.get('File')
            workspace_name = request.form.get('WorkspaceName')
            data_pr_config = request.form.get("ConfigFile")
            compute = request.form.get("Compute")
            validate_param = [param_name for param_name, param_value in {
                'File': file, 'WorkspaceName': workspace_name, 'ConfigFile': data_pr_config, 'Compute': compute}.items()
                              if not param_value]
            if validate_param:
                status_code = 400
                return jsonify(ResponseService.get_bad_request_message(params=validate_param,
                                                                       status_code=status_code)), status_code
            dp = DataProcessingController(workspace=workspace_name,
                                          input_file=file,
                                          config_data=data_pr_config)
            res = dp.run_pipeline()
            status_code = 200
            return jsonify(ResponseService.get_success_message(status_code=status_code)), status_code
        elif request.method == 'GET':
            workspace_name = request.args.get('WorkspaceName')
            job_name = request.args.get('JobName')
            file_id = request.args.get("FileId")
            validate_param = [param_name for param_name, param_value in
                              {'WorkspaceName': workspace_name, 'JobName': job_name,
                               'FileId': file_id}.items() if not param_value]
            if validate_param:
                status_code = 400
                return jsonify(ResponseService.get_bad_request_message(params=validate_param,
                                                                       status_code=status_code)), status_code
            aml_job_status = RetrieveDataProcessingJob(workspace=workspace_name,
                                                       job_name=job_name,
                                                       table_id_data=file_id)
            status_code = 200
            return jsonify({'status': aml_job_status.retrieve_job_status()}), status_code
    except Exception as e:
        status_code = 500
        return jsonify(ResponseService.get_exception_message(msg=str(e), status_code=status_code)), status_code


@data_pro_bp.route('/delete/dataset', methods=['DELETE'])
def delete_file():
    try:
        if request.method == 'DELETE':
            file_name = request.args.get('file_name')
            workspace_name = request.args.get('WorkspaceName')
            validate_param = [param_name for param_name, param_value in
                              {'workspace_name': workspace_name, 'file_name': file_name}.items() if not param_value]
            if validate_param:
                status_code = 400
                return jsonify(ResponseService.get_bad_request_message(params=validate_param,
                                                                       status_code=status_code)), status_code
            utils = Utils()
            utils.delete_file_workspace_blob_storage(file_name=file_name,
                                                     workspace_name=workspace_name)
            status_code = 200
            return jsonify(ResponseService.get_success_message(status_code=status_code)), status_code
    except Exception as e:
        status_code = 500
        return jsonify(ResponseService.get_exception_message(msg=str(e), status_code=status_code)), status_code
