from flask import request

from flask import Blueprint

from dbo.llm_repository import LLMRepository

from services.response_service import ResponseService

from flask import jsonify

repository_bp = Blueprint('repository_bp_bp', __name__)


@repository_bp.route('/workspaces', methods=['GET'])
def get_workspaces():
    try:
        if request.method == 'GET':
            is_active = request.args.get('isActive')
            rs = LLMRepository()
            return rs.get_workspace_data(is_active=is_active), 200
    except Exception as e:
        status_code = 500
        return jsonify(ResponseService.get_exception_message(msg=str(e), status_code=status_code)), status_code


@repository_bp.route('/files', methods=['GET'])
def get_all_ft_files():
    if request.method == 'GET':
        try:
            if request.method == 'GET':
                workspace_id = request.args.get('workspaceId')
                file_id = request.args.get('fileId')

                if workspace_id:
                    rs = LLMRepository()
                    return rs.get_files(workspace_id=workspace_id), 200

                if file_id:
                    rs = LLMRepository()
                    return rs.get_files(file_id=file_id), 200
                else:
                    rs = LLMRepository()
                    return rs.get_files(), 200
        except Exception as e:
            status_code = 500
            return jsonify(ResponseService.get_exception_message(msg=str(e), status_code=status_code)), status_code


@repository_bp.route('/models', methods=['GET'])
def get_all_llm_models():
    try:
        if request.method == 'GET':
            collections = request.args.get('collections')
            model_architecture = request.args.get('modelArchitecture')
            finetune_task = request.args.get("finetuneTask")
            model_id = request.args.get('modelId')
            rs = LLMRepository()
            return rs.get_all_llm_model_data(collections=collections, model_architecture=model_architecture,
                                             finetune_task=finetune_task, model_id=model_id), 200
    except Exception as e:
        status_code = 500
        return jsonify(ResponseService.get_exception_message(msg=str(e), status_code=status_code)), status_code


@repository_bp.route('/finetune/tasks', methods=['GET'])
def get_all_ft_tasks():
    if request.method == 'GET':
        try:
            if request.method == 'GET':
                task_id = request.args.get('taskId')
                task_type = request.args.get('taskType')

                if task_id:
                    rs = LLMRepository()
                    return rs.get_finetuning_tasks(task_id=task_id), 200

                if task_type:
                    rs = LLMRepository()
                    return rs.get_finetuning_tasks(task_type=task_type), 200
                else:
                    rs = LLMRepository()
                    return rs.get_finetuning_tasks(), 200
        except Exception as e:
            status_code = 500
            return jsonify(ResponseService.get_exception_message(msg=str(e), status_code=status_code)), status_code


@repository_bp.route('/compute', methods=['GET'])
def get_all_computes():
    if request.method == 'GET':
        try:
            if request.method == 'GET':
                workspace_id = request.args.get('workspaceId')
                if workspace_id:
                    rs = LLMRepository()
                    return rs.get_computes(workspace_id=workspace_id), 200
                else:
                    status_code = 400
                    return jsonify(ResponseService.get_bad_request_message(params='workspaceId',
                                                                           status_code=status_code)), status_code
        except Exception as e:
            status_code = 500
            return jsonify(ResponseService.get_exception_message(msg=str(e), status_code=status_code)), status_code
