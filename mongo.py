import sys
from pymongo import MongoClient
from pymongo.errors import (
        PyMongoError,
        InvalidURI,
        ConnectionFailure
    )

def exception_decorator_pymongo(func):

    def exec_func(*args, **kwargs):
        try:
            temp_func = func(*args, **kwargs)
        except PyMongoError as pme:
            raise PyMongoError(pme)
        except Exception as e:
            raise Exception(e)
        finally:
            try:
                self.close()
            except:
                pass
        return temp_func
    return exec_func



class Mymongo:

    # def __init__(self, host, port, username, password):
    #     self.host = host
    #     self.port = port
    #     self.client = MongoClient(host, port)
    #     self.client.

    def __init__(self, mongoUri):
        try:
            self.client = MongoClient(mongoUri)    
        except PyMongoError as pme:
            raise PyMongoError(pme)

    @exception_decorator_pymongo
    def get_client(self):
        return self.client

    @exception_decorator_pymongo
    def get_db(self):
        return self.db


    @exception_decorator_pymongo
    def database(self, database):
        self.db = self.client[database]
        return self

    @exception_decorator_pymongo
    def collectn(self, collection):
        if self.db:
            if not collection in self.db.collection_names():
                print 'The requested collection "{0}" is not found in database "{1}".'.format(collection, self.db)
            else:
                self.collection = self.db[collection]
            return self
        else:
            print 'Invalid request for collection "{0}". No database has been selected.'.format(self.collection)
            return None

    @exception_decorator_pymongo
    def find(self, condition=None, lim=0):
        if isinstance(condition, dict):
            return self.collection.find(condition, {'_id': 0}, no_cursor_timeout=False).limit(lim)
        else:
            return self.collection.find(dict(), {'_id': 0} ,no_cursor_timeout=False).limit(lim)

    @exception_decorator_pymongo
    def update(self, select_cond, update_args, **kwargs):
        if not update_args:
            return
        if not isinstance(select_cond, dict):
            select_cond = {}
        if self.collection:
            result = self.collection.update(
                select_cond, {'$set': update_args}, j=True, **kwargs)
            return result
        else:
            raise PyMongoError("No collection specified")

    @exception_decorator_pymongo
    def delete_fields(self, select_cond, delete_condition, **kwargs):
        if not isinstance(select_cond, dict):
            select_cond = {}
        if self.collection:
            result = self.collection.update(
                select_cond, {'$unset': update_args}, j=True, **kwargs)
            return result
        else:
            raise PyMongoError("No collection specified")

    def delete_document(self, select_cond, **kwargs):
        if not isinstance(select_cond, dict):
            select_cond = {}
        if self.collection:
            result = self.collection.remove(
                select_cond, j=True, **kwargs)
            return result
        else:
            raise PyMongoError("No collection specified")


    @exception_decorator_pymongo
    def insert(self, doc):
        if self.collection:
            objid = self.collection.insert(doc)
            return objid
        else:
            raise PyMongoError("No collection specified")


    def update_and_insert(self, select_cond, update_args, doc, **kwargs):
        if self.update(select_cond, update_args, **kwargs):
            objid = self.insert(doc)
            if objid:
                return objid
            else:
                print "Problem while inserting doc to mongo collection"
        else:
            print "Problem while updating doc"
        return None

    @exception_decorator_pymongo
    def close(self):
        self.client.close()


# sel_dict = { "ack":0 }
# update_dict = {'time':['Tue Nov 26 2013 12:07:09 GMT+0530 (India Standard Time)']}
# dao = NotificationDao()
# dao.database('work').collectn('notifications').update(sel_dict, update_dict)
