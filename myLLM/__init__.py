from flask import Flask
from api.root.routes.root_route import root_bp
from api.data_processing.routes.data_processing_route import data_pro_bp
from api.repository.routes.repository_route import repository_bp

app = Flask(__name__.split('.')[0])

app.register_blueprint(root_bp, url_prefix='/api')
app.register_blueprint(data_pro_bp, url_prefix='/api/data')
app.register_blueprint(repository_bp, url_prefix='/api/repo')
