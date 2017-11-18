from flask import Flask, request
from common import Task, get_db, FLASK_PORT
from random import randint

app = Flask(__name__)

class Taskpool:
	def __init__(self, hostname):
		self.hostname = hostname
		self.tasks = get_db()[self.hostname]
		self.random_init()

	def getTask(self):
		entry = self.tasks.find_and_modify(sort={'_id':1}, remove=True)
		return Task(entry['taskid'], entry['sleep_time']) if entry else None

	def putTask(self, task):
		return self.tasks.insert_one({
			'taskid': task.taskid, 
			'sleep_time': task.sleep_time
		})

	def random_init(self, num=10):
		for i in range(0, num):
			self.putTask(Task(i, randint(1, 5)))

taskpool = Taskpool('taskpool')

@app.route('/get', methods=['GET'])
def getTask():
	task = taskpool.getTask()
	return str(task) if task else ''

@app.route('/put', methods=['GET'])
def putTask():
	taskid = request.args.get('taskid', None)
	sleep_time = request.args.get('sleep_time', 0)
	taskpool.putTask(Task(taskid, sleep_time))
	return 'OK'

if __name__ == '__main__':
	app.run(host=taskpool.hostname, port=FLASK_PORT, threaded=True)