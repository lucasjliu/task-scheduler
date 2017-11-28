from urllib.parse import urlencode
from http.client import HTTPConnection
from pymongo import MongoClient
import time
import datetime
from threading import Lock

#===================================
# task basics and IPC connection
#===================================
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
	def __init__(self, host, port=None, timeout=10):
		self.conn = HTTPConnection(host, port, timeout)

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

#===================================
# db basics
#===================================
DB_NAME = 'matroid'
DB_IP = '127.0.0.1'
DB_PORT = 27017

def get_db(db_ip=DB_IP, db_port=DB_PORT, db_name=DB_NAME):
	return MongoClient(db_ip, db_port)[db_name]

class MongoTable:
	def __init__(self, table_name, db_ip=DB_IP, db_port=DB_PORT, db_name=DB_NAME):
		self.dbtable = MongoClient(db_ip, db_port)[db_name][table_name]

	def getTable(self):
		return self.dbtable

#===================================
# key-value storage container, in-memory and persistent
#===================================
class KVTable:
	def __getitem__(self, key):
		raise NotImplementedError
	def __setitem__(self, key, value):
		raise NotImplementedError
	def remove(self, key):
		raise NotImplementedError
	def __iter__(self):
		raise NotImplementedError
	def __contains__(self, key):
		raise NotImplementedError

class InMemKVTable(KVTable):
	def __init__(self):
		self.table = {}

	def __getitem__(self, key):
		return self.table.get(key)

	def __setitem__(self, key, value):
		self.table[key] = value

	def remove(self, key):
		 # return true if key is found
		return self.table.pop(key, None)

	def __iter__(self):
		for key in list(self.table):
			yield key

	def __contains__(self, key):
		return key in self.table

class PersistKVTable(KVTable, MongoTable):
	def __init__(self, table_name, db_ip=DB_IP, db_port=DB_PORT, db_name=DB_NAME):
		self.KEY_FIELD = 'key'
		self.VALUE_FIELD = 'value'
		MongoTable.__init__(self, table_name, db_ip, db_port, db_name)
		self.dbtable.create_index(self.KEY_FIELD)
		self.init_cache()
		self.table_lock = Lock()

	def init_cache(self):
		self.cache = InMemKVTable()
		self.cache.table = {entry[self.KEY_FIELD]: entry[self.VALUE_FIELD] 
			for entry in self.dbtable.find(sort=[('_id',1)])}

	def __getitem__(self, key):
		return self.cache[key]

	def __setitem__(self, key, value):
		with self.table_lock:
			self.cache[key] = value
			self.dbtable.find_and_modify(
				query={self.KEY_FIELD: key}, 
				update={"$set": {self.VALUE_FIELD: value}},
				upsert=True
			)

	def __iter__(self):
		for key in self.cache:
			yield key

	def remove(self, key):
		with self.table_lock:
			self.dbtable.delete_one({self.KEY_FIELD: key})
			return self.cache.remove(key)
	
	def __contains__(self, key):
		return key in self.cache

class KVTableFactory:
	@staticmethod
	def createTable(persistent=True, table_name=None, db_ip=DB_IP, db_port=DB_PORT, db_name=DB_NAME):
		return PersistKVTable(table_name, db_ip, db_port, db_name) if persistent else InMemKVTable()

#===================================
# key-value container with increasing integer key,
# in-memory and persistent
#===================================
class SeqTable(KVTable):
	def __setitem__(self, key, value):
		raise NotImplementedError
	def insert(self, value):
		raise NotImplementedError

class AtomicCounter:
	def __init__(self, start=0):
		self.counter = start
		self.counter_lock = Lock()

	def next(self):
		with self.counter_lock:
			curr = self.counter
			self.counter = curr + 1 if curr < 1e9 else 0
		return curr

class InMemSeqTable(SeqTable, InMemKVTable, AtomicCounter):
	def __init__(self):
		InMemKVTable.__init__(self)
		AtomicCounter.__init__(self, 0)

	def init_counter(self):
		self.counter = (0 if not self.table else
			max([key for key in self.table]) + 1)

	def __setitem__(self, key, value):
		if key in self.table:
			self.table[key] = value

	def insert(self, value):
		key = self.next()
		self.table[key] = value
		return key

class PersistSeqTable(SeqTable, PersistKVTable):
	def __init__(self, table_name, db_ip=DB_IP, db_port=DB_PORT, db_name=DB_NAME):
		PersistKVTable.__init__(self, table_name, db_ip, db_port, db_name)

	def init_cache(self):
		self.cache = InMemSeqTable()
		self.cache.table = {entry[self.KEY_FIELD]: entry[self.VALUE_FIELD] 
			for entry in self.dbtable.find(sort=[('_id',1)])}
		self.cache.init_counter()

	def __setitem__(self, key, value):
		with self.table_lock:
			self.cache[key] = value
			self.dbtable.find_and_modify(
				query={self.KEY_FIELD: key}, 
				update={"$set": {self.VALUE_FIELD: value}}
			)

	def insert(self, value):
		with self.table_lock:
			key = self.cache.insert(value)
			self.dbtable.insert_one({
				self.KEY_FIELD: key,
				self.VALUE_FIELD: value
			})
		return key

class SeqTableFactory:
	@staticmethod
	def createTable(persistent=True, table_name=None, db_ip=DB_IP, db_port=DB_PORT, db_name=DB_NAME):
		return PersistSeqTable(table_name, db_ip, db_port, db_name) if persistent else InMemSeqTable()

#===================================
# queue container, in-memory and persistent
#===================================
class Queue:
	def insert():
		raise NotImplementedError
	def find():
		raise NotImplementedError
	def update():
		raise NotImplementedError
	def delete():
		raise NotImplementedError

class InMemQueue(Queue, InMemSeqTable):
	def __init__(self):
		SeqTable.__init__(self)

	def find(self, filter=None):
		for key in list(self.table):
			if filter(table[key]):
				yield table[key]

	def update(self, filter, update):
		for key in list(self.table):
			if filter(self.table[key]):
				for field, value in update:
					self.table[key][field] = value

	def delete(self, _id):
		self.table.pop(_id, None)

class PersistQueue(Queue, MongoTable):
	def __init__(self, table_name, db_ip=DB_IP, db_port=DB_PORT, db_name=DB_NAME):
		MongoTable.__init__(self, table_name, db_ip, db_port, db_name)

	def insert(self, document):
		return self.dbtable.insert_one(document).inserted_id

	def find(self, filter=None):
		for entry in self.dbtable.find(sort=[('_id',1)]):
			if filter(entry):
				yield entry

	def update(self, filter, update):
		self.dbtable.update_many(filter, {"$set": update})

	def delete(self, _id):
		self.dbtable.delete_one({'_id': _id})

class QueueFactory:
	@staticmethod
	def createQueue(persistent=True, table_name=None, db_ip=DB_IP, db_port=DB_PORT, db_name=DB_NAME):
		return PersistQueue(table_name, db_ip, db_port, db_name) if persistent else InMemQueue()

#===================================
# logging
#===================================
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