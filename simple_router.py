from datetime import datetime
import json
import signal
import requests
from flask import make_response
from flask import request
from flask import Flask
from flask.ext.cors import CORS
from flask.ext.cors import cross_origin
from expand import expand

app = Flask(__name__)
app.config['CORS_HEADERS'] = 'Content-Type'
cors = CORS(app)
logfile = None

@app.route('/', defaults={'path':''})
@app.route('/<path:path>', methods=['GET', 'POST', 'UPDATE', 'DELETE'])
@cross_origin()
def catch_all(path):
    target = 'http://compute-1-34:9200/' + path
    method = request.method
    data = request.data
    if 'from' in request.args and request.args['from'] == '0':
        logfile.write("{0}\t{1}\n".format(
            str(datetime.now()), json.loads(data)['query']['multi_match']['query']))
    if method == 'POST':
        content = requests.post(target, data = data, params = request.args).content
        data = expand(data, content)
        content = requests.post(target, data = data, params = request.args).content
        res = make_response(content)
    elif method == 'GET':
        res = make_response(requests.get(target, data = data, params = request.args).content)
    else:
        res = make_response()
    return res

def signal_handler(signal, frame):
    global logfile
    print "quiting..."
    logfile.close()
    exit()

if __name__ == '__main__':
    logfile = open('../log/{0}.log'.format(str(datetime.now()).split('.')[0]), 'w')
    signal.signal(signal.SIGINT, signal_handler)
    app.run(host='0.0.0.0', debug=True)
