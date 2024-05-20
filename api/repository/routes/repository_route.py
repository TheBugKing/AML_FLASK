from flask import request
from flask import Blueprint

repository_bp = Blueprint('repository_bp_bp', __name__)


@repository_bp.route('/workspaces', methods=['GET'])
def get_workspaces():
    if request.method == 'GET':
        # This API should execute an SP on Workspace Table to based on params WorkspaceName, active
        # if no params is provided from request then return all available workspaces
        # else filter by active true false
        return "SERVER iS RUNNING", 200


@repository_bp.route('/files/<workspace_id:int>', methods=['GET'])
def get_all_ft_files():
    if request.method == 'GET':
        # This API should execute an SP on files to based on params ID and MLjobstatus
        # if no params is provided from request then return all files of a given workspace
        # else filter by WorkspaceName or ID or MLjobstatus
        return "SERVER iS RUNNING", 200


@repository_bp.route('/file/<file_id:int>', methods=['GET'])
def get_ft_files_by_id(file_id):
    if request.method == 'GET':
        # This API should execute an SP on files to based on params file_id
        # must return
        # returns a details of a specific file
        return "SERVER iS RUNNING", 200


@repository_bp.route('/models', methods=['GET'])
def get_all_llm_models(model_id):
    if request.method == 'GET':
        # This API should execute an SP on llmmodel
        # returns a details of all
        return "SERVER iS RUNNING", 200


@repository_bp.route('/models/<model_id:int>', methods=['GET'])
def get_all_llm(file_id):
    if request.method == 'GET':
        # This API should execute an SP on llmodel to based on params model_id
        # returns a details of a specific model
        return "SERVER iS RUNNING", 200


@repository_bp.route('/finetune/tasks', methods=['GET'])
def get_all_ft_tasks(file_id):
    if request.method == 'GET':
        # This API should execute an SP on fine-tuning tasks
        # returns a details all available tasks
        return "SERVER iS RUNNING", 200


@repository_bp.route('/finetune/tasks/<task_id:int>', methods=['GET'])
def get_ft_task_by_id(task_id):
    if request.method == 'GET':
        # This API should execute an SP on fine-tuning tasks based on task id
        # returns a details specific tasks
        return "SERVER iS RUNNING", 200


@repository_bp.route('/llm/configs', methods=['GET'])
def get_llmgw_config_by_key(key):
    if request.method == 'GET':
        # This API should execute an SP on llmgw config table tasks based on key
        # returns a details specific tasks
        key = request.args.get('key')
        return "SERVER iS RUNNING", 200
