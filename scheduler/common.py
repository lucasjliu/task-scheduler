from urllib.parse import urlencode
from http.client import HTTPConnection
from pymongo import MongoClient
import time
import datetime

FLASK_PORT = 5000

NODE_IS_DOWN = 521
OK = 200

class Task:
	def __init__(self, taskid, sleep_time):
		self.taskid = taskid
		self.sleep_time = sleep_time

	def __str__(self):
		return urlencode(self.__dict__)

class Status:
	CREATED = 'created'
	RUNNING = 'running'
	SUCCESS = 'success'
	FAILURE = 'failure'

class Conn:
	def __init__(self, host='127.0.0.1' + str(FLASK_PORT), port=None, timeout=10):
		self.conn = HTTPConnection(host, int(port), timeout)

	def send_recv(self, uri, params={}, recv_size=8*128):
		try:
			if '?' not in uri and len(params):
				uri += '?'
			uri += urlencode(params, encoding='utf-8')
			self.conn.request('GET', uri)
			res = self.conn.getresponse()
			return res.status, res.read(recv_size).decode('utf-8')
		except:
			return NODE_IS_DOWN, "Node is down"

	def send(self, uri, params={}):
		try:
			if '?' not in uri and len(params):
				uri += '?'
			uri += urlencode(params, encoding='utf-8')
			self.conn.request('GET', uri)
			return OK
		except:
			return NODE_IS_DOWN

DB_NAME = 'matroid'
DB_IP = '127.0.0.1'
DB_PORT = 27017

def get_db(db_ip=DB_IP, db_port=DB_PORT, db_name=DB_NAME):
	return MongoClient(db_ip, db_port)[db_name]

class PersistentTable:
	def __init__(self, collection, db_ip=DB_IP, db_port=DB_PORT, db_name=DB_NAME):
		self.collection = MongoClient(db_ip, db_port)[db_name].collection
	
	def getCollection():
		return self.collection

class Logger:
	def __init__(self, fpath):
		self.file = open(fpath, 'a')

	def debug(self, msg):
		line = "[{}][{}]{}".format(self.time_stamp_(), "DEBUG", msg)
		print(line)

	def info(self, msg):
		line = "[{}][{}]{}".format(self.time_stamp_(), "INFO", msg)
		self.file.write(line + '\n')
		self.file.flush()
		print(line)

	def time_stamp_(self):
		return datetime.datetime.fromtimestamp(time.time()).strftime('%H:%M:%S')

	@staticmethod
	def create(name):
		return Logger('./logs/' + name)