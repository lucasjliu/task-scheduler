from flask import Flask, request
from common import Task, Conn, get_db, FLASK_PORT, Logger, Status
from time import sleep
from sys import argv
from bson.objectid import ObjectId

app = Flask(__name__)

class Worker:
	def __init__(self, hostname):
		self.hostname = hostname
		self.msg_queue = get_db()[self.hostname]
		self.logger = Logger('./logs/' + self.hostname)
	
	def init(self):
		self.notify()

	def do_task(self, task, master_host, master_port):
		self.logger.debug(self.hostname + ' received ' + str(task))
		entry_id = self.msg_queue.insert_one({
			'taskid': task.taskid,
			'sleep_time': task.sleep_time,
			'master_host': master_host,
			'master_port': master_port,
			'status': Status.FAILED
		}).inserted_id
		sleep(int(task.sleep_time))
		self.msg_queue.find_and_modify(
			query={'_id': entry_id}, 
			update={"$set": {'status': Status.SUCCESS}}
		)

	def notify(self, master_host=None):
		for task in self.msg_queue.find(sort=[('_id',1)]):
			if not master_host or task['master_host'] == master_host:
				conn = Conn(task['master_host'], int(task['master_port']))
				conn.send('/notify?' + 
					str(Task(task['taskid'], task['sleep_time'])) +
					'&&status=' + task['status'] + '&&worker=' + self.hostname)
				self.msg_queue.delete_one({'_id': ObjectId(task['_id'])})

worker = Worker(argv[1]) # prompt

@app.route('/doTask', methods=['GET'])
def doTask():
	master_host = request.args.get('from', 'master')
	master_port = int(request.args.get('port', FLASK_PORT))
	taskid = request.args.get('taskid', None)
	sleep_time = request.args.get('sleep_time', 0)
	worker.do_task(Task(taskid, sleep_time), master_host, master_port)
	worker.notify(master_host)
	return 'OK'

@app.route('/ping', methods=['GET'])
def ping():
	master_host = request.args.get('from', 'master')
	worker.notify(master_host)
	return 'OK'

if __name__ == '__main__':
	worker.init()
	app.run(host=worker.hostname, port=FLASK_PORT, threaded=True)