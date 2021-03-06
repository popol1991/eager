#! /opt/python27/bin/python
from datetime import datetime
import sys
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
server = None

@app.route('/', defaults={'path':''})
@app.route('/<path:path>', methods=['GET', 'POST', 'UPDATE', 'DELETE'])
@cross_origin()
def catch_all(path):
    """  Listen to all requests and redirect to the real ElasticSearch server. """
    global server
    target = server + path
    method = request.method
    data = request.data
    if 'from' in request.args and request.args['from'] == '0':
        logfile.write("{0}\t{1}\n".format(
            str(datetime.now()), json.loads(data)['query']['multi_match']['query']))
    if method == 'POST':
        content = requests.post(target, data = data, params = request.args).content
        #data = expand(data, content) # query expansion
        #content = requests.post(target, data = data, params = request.args).content
        res = make_response(content)
    elif method == 'GET':
        res = make_response(requests.get(target, data = data, params = request.args).content)
    else:
        res = make_response()
    return res

def signal_handler(signal, frame):
    """ Execute before program exit. """
    global logfile
    print "quiting..."
    logfile.close()
    exit()

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print "Usage: proxy.py server_host:port listened_port"
        exit()
    server = sys.argv[1]
    port = sys.argv[2]
    if not server.endswith('/'):
        server += '/'
    if not server.startswith("http://"):
        server = "http://" + server
    logfile = open('/bos/usr0/yingkaig/Eager/log/{0}.log'.format(str(datetime.now()).split('.')[0]), 'w')
    signal.signal(signal.SIGINT, signal_handler)
    app.run(host='0.0.0.0', debug=True, port=int(port))
