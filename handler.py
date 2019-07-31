from flask import Flask, request, jsonify
import traceback
from service import Service
import datetime
import time
import threading
import settings as s
from flask_api import status




app = Flask(__name__)


delete_timeout = 3000
free_timeout = 60

@app.route('/api/generateToken', methods=['GET'])
def generate_token():
	try:
		token_created = service.generate_token()
		if token_created:
			return {"status": "Token Created", "status_code" : status.HTTP_201_CREATED}
		else:
			return {"status" : "Could Not Generate Token", "status_code" : status.HTTP_200_OK}
	except:
		traceback.print_exc()
		return {"status": "Failed", "status_code" : status.HTTP_500_INTERNAL_SERVER_ERROR}

  
@app.route('/api/assignToken', methods=['GET'])
def assign_token():
	if 'cid' not in request.args:
		return {"status": "Provide client id", "status_code" : status.HTTP_404_NOT_FOUND}
	cid = request.args['cid'].strip()
	try:
		return service.get_and_assign(cid)
	except:
		traceback.print_exc()
		return {"status": "Failed", "status_code" : status.HTTP_500_INTERNAL_SERVER_ERROR}


@app.route('/api/unblockToken', methods=['GET'])
def unblock_token():
	try:
		if 'token' not in request.args:
			return {"status": "provide token", "status_code" : status.HTTP_400_BAD_REQUEST}
		token = request.args['token'].strip()
		return service.unblock([token])
	except:
		traceback.print_exc()
		return {"status": "Failed", "status_code" : status.HTTP_500_INTERNAL_SERVER_ERROR}


@app.route('/api/deleteToken', methods=['DELETE'])
def delete_token():#Need to see for the locking
	try:
		if 'token' not in request.args:
			return {"status": "provide token", "status_code" : status.HTTP_404_NOT_FOUND}
		token = request.args['token'].strip()
		return service.delete_tok([token])
	except Exception as e:
		traceback.print_exc()
		return {"status": "Failed", "status_code" : status.HTTP_500_INTERNAL_SERVER_ERROR}



@app.route('/api/keepAlive', methods=['GET'])
def keep_alive():
	token = request.args['token'].strip()
	if not token:
		return {"status": "Failed", "status_code" : status.HTTP_400_BAD_REQUEST}
	try:
		return service.keep_alive(token)
	except:
		traceback.print_exc()
		return {"status": "Failed", "status_code" : status.HTTP_500_INTERNAL_SERVER_ERROR}



def delete_expired_token():
	time.sleep(delete_timeout)
	while True:
		now = time.time()
		try:
			del_tokens = service.read_from_mongo({"validation_time": {"$lt" : now-delete_timeout}})
			print "delete_tokens", del_tokens.count()
			if del_tokens.count()>0:
				result = service.delete_tok([t["token"] for t in del_tokens])
				print result
		except Exception as e:
			traceback.print_exc()
			pass
		time.sleep(delete_timeout)

def free_expired_token():
	time.sleep(free_timeout)
	while True:
		now = time.time()
		try:
			free_tokens = service.read_from_mongo({"allocation_time": {"$lt" : now-free_timeout}})
			print "free tokens", free_tokens.count()
			if free_tokens.count()>0:
				result = service.unblock([t["token"] for t in free_tokens])
				print result
		except Exception as e:
			traceback.print_exc()
			pass
		time.sleep(free_timeout)

if __name__ == "__main__":
	#app = create_app("config")
	try:
		global service
		service = Service(s.Mongo_URL, s.Mongo_Db, s.Mongo_Coll)
		# timer = threading.Timer(free_timeout, free_expired_token)
		# timer.start()
		freeThread = threading.Thread(target=free_expired_token)
		freeThread.setDaemon(True)
		freeThread.start()
		deleteThread = threading.Thread(target=delete_expired_token)
		deleteThread.setDaemon(True)
		deleteThread.start()
		app.run(host="0.0.0.0", port=8000, debug=True)
	except Exception as e:
		raise Exception(e)