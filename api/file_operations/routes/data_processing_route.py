from flask import request
from flask import Blueprint

data_pro_bp = Blueprint('data_pro_bp', __name__)


@data_pro_bp.route('/', methods=['GET'])
def root():
    if request.method == 'GET':
        return "SERVER iS RUNNING", 200
