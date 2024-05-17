from myLLM import app
import os
from dotenv import load_dotenv
load_dotenv()

if __name__ == '__main__':
    debug = os.getenv('DEBUG', '0') == '1'
    host = os.getenv('HOST', '0.0.0.0')
    server_port = os.environ.get('SERVER_PORT', '8000')
    app.run(debug=True, host=host, port=server_port)
