import requests
from flask import request
from flask import Flask
app = Flask(__name__)

@app.route('/', defaults={'path':''})
@app.route('/<path:path>')
def catch_all(path):
    target = 'http://localhost:9200/' + path
    return requests.get(target, params = request.args).content

if __name__ == '__main__':
    app.run()
