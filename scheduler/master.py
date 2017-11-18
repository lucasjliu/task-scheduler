from flask import Flask, request
from common import Task, Conn, get_db, FLASK_PORT, Logger, Status
from threading import Thread, RLock, Semaphore
from random import choice
from urllib.parse import parse_qs
from time import time
from bson.objectid import ObjectId

app = Flask(__name__)

class Master:
	def __init__(self, host, port, worker_hosts, taskpool_host):
		self.host = host
		self.port = port
		self.worker_hosts = worker_hosts
		self.taskpool_host = taskpool_host
		self.heartbeat_period = 5 # second
		self.last_heartbeat_time = time()

		self.current_tasks = get_db()[self.host]
		self.avail_workers = set()
		self.lock = RLock()
		self.sem_avail_workers = Semaphore(0)

		self.logger = Logger('./logs/' + self.host)

	def init(self):
		self.logger.info(self.host + ' is restarted.')
		self.heartbeat()

	def notified(self, worker, taskid, status):
		self.logger.debug(status + ' notification from ' + worker + ': ' + taskid)
		(self.handle_task_success_ if status == Status.SUCCESS else 
			self.handle_task_failure_)(worker, taskid)

	def do_schedule(self):
		while True:
			self.try_assign_()
			if time() - self.last_heartbeat_time > self.heartbeat_period:
				self.heartbeat()

	def heartbeat(self):
		self.last_heartbeat_time = time()
		for worker_host in self.worker_hosts:
			conn = Conn(worker_host, self.port)
			status = conn.send('/ping?' + self.host)
			if status == 200:
				self.handle_worker_up_(worker_host)
			else:
				self.handle_worker_down_(worker_host)

	def handle_worker_up_(self, worker):
		with self.lock:
			if (worker not in self.avail_workers and 
				self.current_tasks.find_one({'worker': worker}) is None):
				self.logger.info(worker + ' is up.')
				self.avail_workers.add(worker)
				self.sem_avail_workers.release()

	def handle_worker_down_(self, worker):
		with self.lock:
			if worker in self.avail_workers:
				self.logger.info(worker + ' is unavailable.')
				self.avail_workers.remove(worker)
				self.sem_avail_workers.acquire()
			elif self.current_tasks.find_one({'worker': worker}):
				self.logger.info(worker + ' is down.')
				self.handle_task_failure_(worker)

	def try_assign_(self):
		if self.sem_avail_workers.acquire(timeout=self.heartbeat_period):
			task = self.get_task_()
			if not task:
				self.sem_avail_workers.release()
				return False
			with self.lock:
				worker = choice(tuple(self.avail_workers))
				self.avail_workers.remove(worker)
			self.logger.debug('Trying to assign ' + str(task) + ' to ' + worker)
			entry_id = self.current_tasks.insert_one({
				'worker': worker,
				'taskid': task.taskid,
				'sleep_time': task.sleep_time
			}).inserted_id
			conn = Conn(worker, self.port)
			status = conn.send('/doTask?' + str(task) + '&&from=master&&port=' + str(self.port))
			if status == 200:
				return True
			self.handle_worker_down_(worker)
		return False

	def handle_task_failure_(self, worker, taskid=None):
		with self.lock:
			for task in self.current_tasks.find({'worker': worker}):
				if taskid is None or task['taskid'] == taskid:
					self.logger.info('Task ' + task['taskid'] + ' failure by ' + worker)
					self.put_task_(Task(task['taskid'], task['sleep_time']))
					self.current_tasks.delete_one({'_id': ObjectId(task['_id'])})

	def handle_task_success_(self, worker, taskid):
		with self.lock:
			for task in self.current_tasks.find({'worker': worker}):
				if task['taskid'] == taskid:
					self.logger.info('Task ' + task['taskid'] + ' success by ' + worker)
					self.current_tasks.delete_one({'_id': ObjectId(task['_id'])})
					self.avail_workers.add(worker)
					self.sem_avail_workers.release()
					break

	def get_task_(self):
		conn = Conn(self.taskpool_host, self.port)
		status, data = conn.send_recv('/get')
		if status != 200 or not data:
			return None
		qs = parse_qs(data)
		self.logger.debug('getting task: ' + str(qs))
		return Task(qs['taskid'][0], qs['sleep_time'][0])

	def put_task_(self, task):
		conn = Conn(self.taskpool_host, self.port)
		self.logger.debug('putting task: ' + str(task.taskid))
		return conn.send('/put?' + str(task))

master = Master('master', FLASK_PORT, ['worker1', 'worker2', 'worker3'], 'taskpool')

@app.route('/notify', methods=['GET'])
def notify():
	worker = request.args.get('worker', None)
	taskid = request.args.get('taskid', None)
	status = request.args.get('status', None)
	if worker and taskid and status:
		master.notified(worker, taskid, status)
	return 'OK'

if __name__ == '__main__':
	t = Thread(
		target=lambda: app.run(host=master.host, port=FLASK_PORT, threaded=True), 
		daemon=True)
	t.start()
	master.init()
	master.do_schedule()
	t.join()