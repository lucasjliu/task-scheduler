from flask import Flask, request
from common import Task, get_db, FLASK_PORT, Status
from random import randint
from pymongo import ASCENDING

app = Flask(__name__)

class Taskpool:
	def __init__(self, host):
		self.host = host
		self.tasks = get_db()[self.host]
		self.tasks.create_index([('taskid', ASCENDING)])
		self.tasks.remove() # erase history tasks
		self.random_init()

	def get(self):
		entry = self.tasks.find_one({"$or": 
			[{'status': Status.CREATED},
			{'status': Status.FAILURE}]
		}, sort=[('taskid',1)])
		return Task(entry['taskid'], entry['sleep_time']) if entry else None

	def put(self, task):
		return self.tasks.insert_one({
			'taskid': task.taskid, 
			'sleep_time': task.sleep_time,
			'status': Status.CREATED
		})

	def update(self, taskid, status):
		self.tasks.find_and_modify(
			query={'taskid': int(taskid)}, 
			update={"$set": {'status': status}}
		)

	def random_init(self, num=100):
		# insert tasks with random sleep time
		for i in range(0, num):
			self.put(Task(i, randint(1, 5)))

taskpool = None

@app.route('/get', methods=['GET'])
def getTask():
	task = taskpool.get()
	return str(task) if task else ('Accepted, no more tasks', 202) # accepted

@app.route('/put', methods=['GET'])
def putTask():
	num = request.args.get('num', 0)
	taskpool.random_init(int(num))
	return 'OK'

@app.route('/update', methods=['GET'])
def updateTask():
	taskid = request.args.get('taskid')
	status = request.args.get('status', Status.FAILURE)
	taskpool.update(taskid, status)
	return 'OK'

if __name__ == '__main__':
	taskpool = Taskpool('taskpool:5000')
	app.run(host='taskpool', port=FLASK_PORT, threaded=True)