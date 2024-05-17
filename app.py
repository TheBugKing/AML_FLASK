from myLLM import app
import os
from dotenv import load_dotenv
load_dotenv()

if __name__ == '__main__':
    debug = os.getenv('DEBUG', '0') == '1'
    host = os.getenv('HOST', '0.0.0.0')
    server_port = os.getenv('SERVER_PORT', '8000')
    print(debug, host, server_port)
    app.run(debug=debug, host=host, port=server_port)
