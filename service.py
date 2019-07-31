#service.py
from threading import Lock, RLock, ThreadError
from flask_api import status
from pymongo.errors import (
        PyMongoError,
        InvalidURI,
        ConnectionFailure
    )
import traceback
from datetime import timedelta
import random
import time
import re
from pymongo.errors import ( PyMongoError)
import settings as s
from mongo import Mymongo



class Service(object):
	token_pool_size = 8
	tokens_pre = ["abc", "cde", "efg", "ghi"]
	count=0

	def __init__(self, mongo_uri, mongo_db, mongo_coll):
		try:
			self.mongo = Mymongo(mongo_uri).database(mongo_db).collectn(mongo_coll)
			self.rlock = RLock()
			self.lock = Lock()
		except ThreadError as the:
			raise ThreadError(the)
		except InvalidURI as iuri:
			raise InvalidURI(iuri)
		except Exception as e:
			raise Exception(e)



	def generate_token(self):
		def gen_token():
			locked = None
			try:
				locked= self.lock.acquire()
				results = self.read_from_mongo({})
				if results.count() < self.token_pool_size:
					pin = random.randint(999, 9999)
					if self.count>=len(self.tokens_pre):
						self.count=0
					token = self.tokens_pre[self.count]+str(pin)
					self.count+=1
					if self.read_from_mongo({'token': token}).count()>0:
						return False
					else:
						
						results = self.mongo.insert({"token" : token, "state" : 0, "allocation_time" : None, "cid" : None, "validation_time" : time.time()})
						if results:
							return True
						else:
							return
				else:
					return
			except Exception as e:
				print e.message
				pass
			finally:
				try:
					if locked:
						self.lock.release()
				except ThreadError as tde:
					raise ThreadError(tde)
		return gen_token()

	def get_and_assign(self, cid):
		locked = None
		try:
			locked = self.lock.acquire()
			free_tokens = self.read_from_mongo({"state" : 0})
			if free_tokens and free_tokens.count()==0:
				return {"status": "No Tokens Available", "status_code" : status.HTTP_404_NOT_FOUND}
			try:
				free_token = free_tokens[0]
				if self.mongo.update({"token": free_token["token"]}, {"allocation_time": time.time(), "state": 1, "cid": cid}):
					return {"status" : "Success", "status_code" : status.HTTP_200_OK, "token" : free_token["token"], "cid" : cid}
			except IndexError as ie:
				raise IndexError(ie)
		except Exception as e:
			raise Exception(e)
		finally:
			try:
				if locked:
					self.lock.release()
			except:
				pass

	def unblock(self, token):
		try:
			updated = self.update_token({"token" :{"$in" : token}}, {"state" : 0, "allocation_time" : None, "cid" : None}) #Need to look for locking required or not
			if updated:
				return {"status": "Success", "status_code" : status.HTTP_200_OK}
			else:
				return {"status": "No token found", "status_code" : status.HTTP_404_NOT_FOUND}
		except Exception as e:
			raise Exception(e)

	def update_token(self, query_cond, update_cond):
		locked = None
		try:
			locked=	self.lock.acquire()
			result = self.mongo.update(query_cond, update_cond)
			if result and result['nModified'] > 0 and result['updatedExisting']:
				return True
			return False
		except PyMongoError as pme:
			raise PyMongoError(pme)
		except Exception as e:
			raise Exception(e)
		finally:
			try:
				if locked:
					self.lock.release()
			except:
				pass


	def keep_alive(self, token):
		#result= self.read_from_mongo({"token": token, "allocation_time" : {"$gt" : time.time()-timeout}})
		try:
			result = self.mongo.update({'token' : token}, {'allocation_time' : time.time(), 'validation_time': time.time()})
			print result
			if result and result['nModified'] > 0 and result['updatedExisting']:
				return {"status": "Success", "status_code" : status.HTTP_200_OK}
			return {"status": "No token found!!", "status_code" : status.HTTP_404_NOT_FOUND}
		except PyMongoError as pme:
			raise PyMongoError(pme)


	def delete_tok(self, tokens):
		locked = None
		try:
			locked= self.lock.acquire()
			deleted= self.mongo.delete_document({"token" : {"$in" : tokens}})
			print deleted
			if deleted and deleted['n']==1:
				return {"status": "Success", "status_code" : status.HTTP_200_OK}
			else:
				return {"status": "No token found", "status_code" : status.HTTP_404_NOT_FOUND}
		except PyMongoError as pme:
			raise PyMongoError(pme)
		finally:
			if locked:
				self.lock.release()

	def read_from_mongo(self, query):
		try:
			return self.mongo.find(query)
		except PyMongoError as pyme:
			raise PyMongoError(pyme)

	def write_to_mongo(self, data):
		try:
			results = self.mongo.insert(data)
			if results and results.count()>0:
				return True
			else:
				raise PyMongoError("Could Not Insert Document "+str(data))
		except PyMongoError as pyme:
			raise PyMongoError(pyme)