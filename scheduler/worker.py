from flask import Flask, request
from common import Task, Conn, get_db, FLASK_PORT, Status, QueueFactory, KVTableFactory
from time import sleep
from sys import argv
from threading import Thread

app = Flask(__name__)

class Worker:
	def __init__(self, port, persistent=True):
		self.port = port
		self.name = 'worker_' + port
		#self.msg_queue = get_db()[self.name]
		self.msg_queue = QueueFactory.createQueue(persistent, self.name + '_msg')
		self.uid_table = KVTableFactory.createTable(persistent, self.name + '_uid')
		self.conn_cooldown_period = 5

	def do_task(self, task, master_host):
		entry_id = self.msg_queue.insert({
			'taskid': 		task.taskid,
			'sleep_time': 	task.sleep_time,
			'master_host': 	master_host,
			'status': 		Status.RUNNING,
			'uid': 			self.uid_table[master_host]
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
			params.update({'status': task['status'], 'worker': task['uid']})
			status, _ = Conn(task['master_host']).send_recv('/notify', params)
			if status == 200:
				# a message is removed only if it is sent successfully
				# otherwise wait for master to ping and gather this message later
				self.msg_queue.delete(task['_id'])

	def register(self, master_hosts):
		while master_hosts:
			master_set = set(master_hosts)
			for master in master_hosts:
				params = {'port': self.port}
				if master in self.uid_table:
					params.update({'uid': self.uid_table[master]})
				status, uid = Conn(master).send_recv('/register', params)
				if status == 200:
					master_set.remove(master)
					self.uid_table[master] = int(uid)
			if master_set:
				sleep(self.conn_cooldown_period)
			master_hosts = list(master_set)

worker = None

@app.route('/doTask', methods=['GET'])
def doTask():
	master_host = request.args.get('from')
	taskid = request.args.get('taskid', -1)
	sleep_time = request.args.get('sleep_time', 0)
	if master_host:
		worker.do_task(Task(taskid, sleep_time), master_host)
		return 'OK' ,200
	return 'Rejected, parameters error', 203

@app.route('/ping', methods=['GET'])
def ping():
	master_host = request.args.get('from')
	if master_host:
		worker.notify(master_host)
	return 'OK', 200

if __name__ == '__main__':
	if (len(argv) < 3):
		print('input should specify a master.')
		exit(1)
	local_host = argv[1]
	local_port = argv[2]
	master_hosts = argv[3:]
	t = Thread(
		target=lambda: app.run(host=local_host, port=int(local_port), threaded=True), 
		daemon=True)
	t.start()
	worker = Worker(local_port)
	worker.recover()
	worker.register(master_hosts)
	t.join()