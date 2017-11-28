from flask import Flask, request
from common import Task, Conn, get_db, FLASK_PORT, Logger, Status, MongoTable, SeqTableFactory
from threading import Thread, RLock, Semaphore
from random import choice
from urllib.parse import parse_qs
from time import time, sleep
from bson.objectid import ObjectId
from sys import argv, exit

app = Flask(__name__)

class Master:
	def __init__(self, host, taskpool_host):
		self.host = host
		self.workers = SeqTableFactory.createTable(True, self.host + '_workers')
		self.taskpool_host = taskpool_host
		self.heartbeat_period = 5 # second
		self.tasks_cooldown_period = 10
		self.last_heartbeat_time = time()

		# maintain current_tasks to make sure each task can only be ended (success/failure) once
		# because we have two ways to determine a failure: detected by master and worker it self
		# following structures should be consistent, so always bind them with a lock
		self.current_tasks = MongoTable(self.host + '_current_tasks').getTable() #### Queue
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
			if not self.try_assign(): # try to get a task and assign to random available worker
				sleep(self.tasks_cooldown_period) # no more tasks, wait for more to be created
			if time() - self.last_heartbeat_time > self.heartbeat_period:
				self.heartbeat()

	def heartbeat(self):
		self.last_heartbeat_time = time()
		for worker_id in self.workers:
			if Conn(self.workers[worker_id]).send('/ping', {'from': self.host}) != 200:
				self.handle_worker_down_(worker_id)

	def handle_worker_new_(self, worker_host):
		with self.worker_lock:
			for worker_id in self.workers:
				if self.workers[worker_id] == worker_host:
					# tasks from a previously killed worker should be failure
					# otherwise must have been handled properly by worker
					self.handle_worker_down_(worker_id)
			worker_id = self.workers.insert(worker_host)
			self.avail_workers.add(worker_id)
			self.sem_avail_workers.release()
		self.logger.info('receive worker ' + worker_host + ' as uid ' + str(worker_id))
		return worker_id

	def handle_worker_down_(self, worker_id):
		with self.worker_lock:
			# workers can be either working or available
			if worker_id in self.avail_workers:
				self.logger.info(self.workers[worker_id] + ' is unavailable.')
				self.sem_avail_workers.acquire() # should not block
				self.avail_workers.remove(worker_id)
			elif self.current_tasks.find_one({'worker': worker_id}):
				# a working worker is down, so restart all its tasks
				self.logger.info(self.workers[worker_id] + ' is down.')
				self.handle_task_failure_(worker_id)
			else: # sanity check
				assert False
			# a restarted worker will be assigned a new uid
			self.workers.remove(worker_id)

	def try_assign(self): # return: if there are remaining tasks
		if self.worker_lock.acquire(blocking=False):
			if self.sem_avail_workers.acquire(blocking=False):
				self.worker_lock.release()
				task = self.get_task_()
				if not task:
					# no more tasks
					self.sem_avail_workers.release()
					return False
				self.worker_lock.acquire()
				assert self.avail_workers # sanity check
				worker_id = choice(tuple(self.avail_workers)) # simple load balancing
				self.avail_workers.remove(worker_id)
				entry_id = self.current_tasks.insert_one({
					'worker': worker_id,
					'taskid': task.taskid,
					'sleep_time': task.sleep_time
				}).inserted_id
				params = task.__dict__.copy()
				params.update({'from': self.host})
				status = Conn(self.workers[worker_id]).send('/doTask', params)
				if status == 200:
					self.handle_task_assigned_(worker_id, task.taskid)
					self.worker_lock.release()
				else:
					self.worker_lock.release()
					self.handle_worker_down_(worker_id)
			else:
				self.worker_lock.release()
		return True

	def get_task_(self):
		status, data = Conn(self.taskpool_host).send_recv('/get')
		if status != 200 or not data:
			return None
		qs = parse_qs(data)
		return Task(qs['taskid'][0], qs['sleep_time'][0])

	def handle_task_assigned_(self, worker_id, taskid):
		self.logger.debug('Task ' + str(taskid) + ' assigned to ' + self.workers[worker_id])
		Conn(self.taskpool_host).send('/update', 
			{'taskid': taskid, 'status': Status.RUNNING})

	def notified(self, worker_id, taskid, status):
		(self.handle_task_success_ if status == Status.SUCCESS else 
			self.handle_task_failure_)(worker_id, taskid)
	
	def registered(self, worker_host):
		return self.handle_worker_new_(worker_host)

	def handle_task_failure_(self, worker_id, taskid=None):
		with self.worker_lock:
			for task in self.current_tasks.find({'worker': worker_id}):
				if taskid is None or task['taskid'] == taskid:
					self.logger.info('Task ' + task['taskid'] + ' failure by ' + self.workers[worker_id])
					Conn(self.taskpool_host).send('/update', 
						{'taskid': task['taskid'], 'status': Status.FAILURE})
					self.current_tasks.delete_one({'_id': ObjectId(task['_id'])})

	def handle_task_success_(self, worker_id, taskid):
		with self.worker_lock:
			for task in self.current_tasks.find({'worker': worker_id}):
				if task['taskid'] == taskid:
					self.logger.info('Task ' + task['taskid'] + ' success by ' + self.workers[worker_id])
					Conn(self.taskpool_host).send('/update', 
						{'taskid': task['taskid'], 'status': Status.SUCCESS})
					self.current_tasks.delete_one({'_id': ObjectId(task['_id'])})
					self.avail_workers.add(worker_id)
					self.sem_avail_workers.release()
					break

master = None

@app.route('/notify', methods=['GET'])
def notify():
	worker_id = request.args.get('worker')
	taskid = request.args.get('taskid')
	status = request.args.get('status')
	if worker_id and taskid and status:
		master.notified(int(worker_id), taskid, status)
		return 'OK', 200
	return 'Rejected, parameters error', 203

@app.route('/register', methods=['GET'])
def register():
	worker_ip = request.remote_addr
	worker_port = request.args.get('port')
	if worker_port:
		worker_host = str(worker_ip) + ':' + str(worker_port)
		worker_id = master.registered(worker_host)
		return str(worker_id), 200
	return 'Rejected, parameters error', 203

if __name__ == '__main__':
	t = Thread(
		target=lambda: app.run(host='master', port=FLASK_PORT, threaded=True), 
		daemon=True)
	t.start()
	master = Master('master:5000', 'taskpool:5000')
	master.init()
	master.do_schedule()
	t.join()