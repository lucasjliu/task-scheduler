from flask import Flask, request
from common import Task, Conn, get_db, FLASK_PORT, Logger, Status
from threading import Thread, RLock, Semaphore
from random import choice
from urllib.parse import parse_qs
from time import time, sleep
from bson.objectid import ObjectId
from sys import argv, exit

app = Flask(__name__)

class Master:
	def __init__(self, host, worker_hosts, taskpool_host):
		self.host = host
		self.worker_hosts = worker_hosts
		self.taskpool_host = taskpool_host
		self.heartbeat_period = 5 # second
		self.last_heartbeat_time = time()

		# maintain current_tasks to make sure each task can only be ended (success/failure) once
		# because we have two ways to determine a failure: detected by master and worker it self
		# following three structures should be consistent, so always bind them with a lock
		self.current_tasks = get_db()[self.host]
		self.avail_workers = set()
		self.sem_avail_workers = Semaphore(0)
		self.worker_lock = RLock()
		# number of available (up but not working) workers

		self.logger = Logger.create(self.host)

	def init(self):
		self.logger.info('master ' + self.host +  ' is restarted.')
		# upon each restart, ping each worker and get notified the existing results 
		# before start listening, therefore no race condition on these results
		self.heartbeat()

	def do_schedule(self):
		while True:
			self.try_assign() # try to get a task and assign to random available worker
			if time() - self.last_heartbeat_time > self.heartbeat_period:
				self.heartbeat()

	def heartbeat(self):
		self.last_heartbeat_time = time()
		for worker_host in self.worker_hosts:
			status = Conn(worker_host).send('/ping', {'from': self.host})
			with self.worker_lock:
				(self.handle_worker_up_ if status == 200 else 
					self.handle_worker_down_)(worker_host)

	def handle_worker_up_(self, worker):
		# an 'down' worker is neither working nor available
		if (worker not in self.avail_workers and 
			not self.current_tasks.find_one({'worker': worker})):
			self.logger.info(worker + ' is up.')
			self.avail_workers.add(worker)
			self.sem_avail_workers.release()

	def handle_worker_down_(self, worker):
		# an 'up' worker can be either working or available (not working)
		if worker in self.avail_workers:
			self.logger.info(worker + ' is unavailable.')
			self.avail_workers.remove(worker)
			self.sem_avail_workers.acquire() # should not block
		elif self.current_tasks.find_one({'worker': worker}):
			self.logger.info(worker + ' is down.')
			# a working worker is down, so restart all its tasks
			self.handle_task_failure_(worker)

	def try_assign(self):
		if self.worker_lock.acquire(blocking=False):
			if self.sem_avail_workers.acquire(timeout=self.heartbeat_period):
				task = self.get_task_()
				if not task:
					# no more tasks
					self.sem_avail_workers.release()
					self.worker_lock.release()
					return False
				worker = choice(tuple(self.avail_workers)) # simple load balancing
				self.avail_workers.remove(worker)
				entry_id = self.current_tasks.insert_one({
					'worker': worker,
					'taskid': task.taskid,
					'sleep_time': task.sleep_time
				}).inserted_id
				self.worker_lock.release()
				params = task.__dict__.copy()
				params.update({'from': self.host})
				status = Conn(worker).send('/doTask', params)
				if status == 200:
					self.handle_task_assigned_(worker, task.taskid)
					return True
				self.handle_worker_down_(worker)
			else:
				self.worker_lock.release()
		return False

	def get_task_(self):
		status, data = Conn(self.taskpool_host).send_recv('/get')
		if status != 200 or not data:
			return None
		qs = parse_qs(data)
		self.logger.debug('getting task: ' + str(qs))
		return Task(qs['taskid'][0], qs['sleep_time'][0])

	def handle_task_assigned_(self, worker, taskid):
		self.logger.debug('Task ' + str(taskid) + ' assigned to ' + worker)
		Conn(self.taskpool_host).send('/update', 
			{'taskid': taskid, 'status': Status.RUNNING})

	def notified(self, worker, taskid, status):
		with self.worker_lock:
			(self.handle_task_success_ if status == Status.SUCCESS else 
				self.handle_task_failure_)(worker, taskid)

	def handle_task_failure_(self, worker, taskid=None):
		for task in self.current_tasks.find({'worker': worker}):
			if taskid is None or task['taskid'] == taskid:
				self.logger.info('Task ' + task['taskid'] + ' failure by ' + worker)
				Conn(self.taskpool_host).send('/update', 
					{'taskid': task['taskid'], 'status': Status.FAILURE})
				self.current_tasks.delete_one({'_id': ObjectId(task['_id'])})

	def handle_task_success_(self, worker, taskid):
		for task in self.current_tasks.find({'worker': worker}):
			if task['taskid'] == taskid:
				self.logger.info('Task ' + task['taskid'] + ' success by ' + worker)
				Conn(self.taskpool_host).send('/update', 
					{'taskid': task['taskid'], 'status': Status.SUCCESS})
				self.current_tasks.delete_one({'_id': ObjectId(task['_id'])})
				self.avail_workers.add(worker)
				self.sem_avail_workers.release()
				break

master = None

@app.route('/notify', methods=['GET'])
def notify():
	worker = request.args.get('worker', None)
	taskid = request.args.get('taskid', None)
	status = request.args.get('status', None)
	if worker and taskid and status:
		master.notified(worker, taskid, status)
	return 'OK'

# /register

if __name__ == '__main__':
	if (len(argv) < 2):
		print('input should specify at least a worker')
		exit(1)
	t = Thread(
		target=lambda: app.run(host='master', port=FLASK_PORT, threaded=True), 
		daemon=True)
	t.start()
	master = Master('master:5000', argv[1:], 'taskpool:5000')
	master.init()
	master.do_schedule()
	t.join()