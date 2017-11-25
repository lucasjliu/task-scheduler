from flask import Flask, request
from common import Task, Conn, get_db, FLASK_PORT, Status
from time import sleep
from sys import argv
from bson.objectid import ObjectId

app = Flask(__name__)

class Worker:
	def __init__(self, hostname):
		self.hostname = hostname
		self.msg_queue = get_db()[self.hostname]

	def do_task(self, task, master_host, master_port):
		entry_id = self.msg_queue.insert_one({
			'taskid': task.taskid,
			'sleep_time': task.sleep_time,
			'master_host': master_host,
			'master_port': master_port,
			'status': Status.RUNNING
		}).inserted_id
		sleep(int(task.sleep_time))
		# not interrupted means success
		self.msg_queue.find_and_modify(
			query={'_id': entry_id}, 
			update={"$set": {'status': Status.SUCCESS}}
		)
		# send the success message back to master
		self.notify(master_host)

	def recover(self):
		# all previous on-going tasks are considered failed
		self.msg_queue.update(
			{'status': Status.RUNNING}, 
			{"$set": {'status': Status.FAILURE}}
		)
		self.notify()

	def notify(self, master_host=None):
		for task in self.msg_queue.find(sort=[('_id',1)]):
			if ((not master_host or task['master_host'] == master_host) 
					and task['status'] != Status.RUNNING):
				params = Task(task['taskid'], task['sleep_time']).__dict__.copy()
				params.update({'status': task['status'], 'worker': self.hostname})
				status, _ = Conn(task['master_host'], task['master_port']).send_recv('/notify', params)
				if status == 200:
					# a message is removed only if it is sent successfully
					# otherwise wait for master to ping and gather this message later
					self.msg_queue.delete_one({'_id': ObjectId(task['_id'])})

if (len(argv) < 2):
	print('input should specify a master.')
	exit(1)
worker = Worker(argv[1]) # prompt?

@app.route('/doTask', methods=['GET'])
def doTask():
	master_host = request.args.get('from', 'master')
	master_port = int(request.args.get('port', FLASK_PORT))
	taskid = request.args.get('taskid', None)
	sleep_time = request.args.get('sleep_time', 0)
	worker.do_task(Task(taskid, sleep_time), master_host, master_port)
	return 'OK'

@app.route('/ping', methods=['GET'])
def ping():
	master_host = request.args.get('from', 'master')
	worker.notify(master_host)
	return 'OK'

if __name__ == '__main__':
	worker.recover()
	app.run(host=worker.hostname, port=FLASK_PORT, threaded=True)