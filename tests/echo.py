from flask import Flask, request
from threading import Thread
from time import sleep
import sys, os
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
from scheduler.common import Task, Conn

app = Flask(__name__)

def run_app():
	app.run(host='127.0.0.1', port=5000, threaded=True)

@app.route('/echo', methods=['GET','HEAD','PUT','POST'])
def echo():
	print(request)
	if request.method == 'GET':
		return request.args.get('val', 'empty input')
	else:
		return 'OK'

if __name__ == '__main__':
	t = Thread(target=run_app, daemon=True)
	t.start()

	sleep(1)

	conn = Conn(host='127.0.0.1', port=5000, timeout=2)
	print(conn.send_recv('/echo?val=hello'))
	print(conn.send('/echo?val=hello'))

	t.join()