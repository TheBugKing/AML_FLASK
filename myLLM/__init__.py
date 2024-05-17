from flask import Flask
from api.root.routes.root_route import root_bp
from api.data_processing.routes.data_processing_route import data_pro_bp

app = Flask(__name__.split('.')[0])

app.register_blueprint(root_bp, url_prefix='/api')
app.register_blueprint(data_pro_bp, url_prefix='/api/data')
