from flask import request
from flask import Blueprint

root_bp = Blueprint('root_bp', __name__)


@root_bp.route('/', methods=['GET'])
def root():
    if request.method == 'GET':
        return "SERVER iS RUNNING", 200
