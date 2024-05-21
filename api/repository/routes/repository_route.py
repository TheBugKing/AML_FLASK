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
        model_type = request.args.get('model_type')
        model_id = request.args.get('model_id')
        if model_type:
            # pull data by model datat by type
            return
        if model_id:
            # pull data by model datat by id
            return
        else:
            # returns a details of all
            return


@repository_bp.route('/finetune/tasks', methods=['GET'])
def get_all_ft_tasks(file_id):
    if request.method == 'GET':
        # This API should execute an SP on fine-tuning tasks
        # returns a details all available tasks
        task_id = request.args.get('task_id')
        if task_id:
            return
        else:
            # all tasks
            return
        return "SERVER iS RUNNING", 200

@repository_bp.route('/llm/configs', methods=['GET'])
def get_llmgw_config_by_key(key):
    if request.method == 'GET':
        # This API should execute an SP on llmgw config table tasks based on key
        # returns a details specific tasks
        key = request.args.get('key')
        return "SERVER iS RUNNING", 200
