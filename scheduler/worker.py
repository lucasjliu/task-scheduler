from flask import Flask, request
from common import Task, Conn, get_db, FLASK_PORT, Status, QueueFactory
from time import sleep
from sys import argv
from bson.objectid import ObjectId

app = Flask(__name__)

class Worker:
	def __init__(self, host, persistent=True):
		self.host = host
		#self.msg_queue = get_db()[self.host]
		self.msg_queue = QueueFactory.createQueue(persistent, self.host)
		self.cooldown_period = 3

	def do_task(self, task, master_host):
		entry_id = self.msg_queue.insert({
			'taskid': task.taskid,
			'sleep_time': task.sleep_time,
			'master_host': master_host,
			'status': Status.RUNNING
		})
		sleep(int(task.sleep_time))
		# not interrupted means success
		self.msg_queue.update(
			{'_id': entry_id}, 
			{'status': Status.SUCCESS}
		)
		# send the success message back to master
		self.notify(master_host)

	def recover(self):
		# all previous on-going tasks are considered failed
		self.msg_queue.update(
			{'status': Status.RUNNING}, 
			{'status': Status.FAILURE}
		)
		self.notify()

	def notify(self, master_host=None):
		filter = (lambda task: (not master_host or task['master_host'] == master_host) 
				and task['status'] != Status.RUNNING)
		for task in self.msg_queue.find(filter):
			params = Task(task['taskid'], task['sleep_time']).__dict__.copy()
			params.update({'status': task['status'], 'worker': self.host})
			status, _ = Conn(task['master_host']).send_recv('/notify', params)
			if status == 200:
				# a message is removed only if it is sent successfully
				# otherwise wait for master to ping and gather this message later
				self.msg_queue.delete(task['_id'])

worker = None

@app.route('/doTask', methods=['GET'])
def doTask():
	master_host = request.args.get('from', 'master')
	taskid = request.args.get('taskid', None)
	sleep_time = request.args.get('sleep_time', 0)
	worker.do_task(Task(taskid, sleep_time), master_host)
	return 'OK'

@app.route('/ping', methods=['GET'])
def ping():
	master_host = request.args.get('from', 'master')
	worker.notify(master_host)
	return 'OK'

if __name__ == '__main__':
	if (len(argv) < 3):
		print('input should specify a master.')
		exit(1)
	worker_host = argv[1]
	worker_port = int(argv[2])
	worker = Worker(worker_host + ':' + str(worker_port))
	worker.recover()
	app.run(host=worker_host, port=worker_port, threaded=True)