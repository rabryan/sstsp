import sstsp
import pandas as pd
import time
import logging as log
import os.path




class User(object):
    def __init__(self, user_id):
        self.user_id = user_id

def get_user_page(user_id):
    log.debug("getting user page {}".format(user_id))
    return user_id

def _valid_user_id(user_id):
    #FIXME - ensure user_id is correctly formed before file operations
    return True

def user_store_path(user_id):
    return str(user_id) + ".hf5"

def user_store_exists(user_id):
    return os.path.exists(user_store_path(user_id))

def create_user_store(user_id):
    """ get user dataframe"""
    f = pd.HDFStore(user_store_path(user_id), 'w')
    return f

def get_user_store(user_id, mode='r'):
    if not _valid_user_id(user_id):
        error(404, "invalid user_id")
    
    if not user_store_exists(user_id):
        error(404, "unknown user_id")

    """ get user dataframe"""
    f = pd.HDFStore(user_store_path(user_id), mode)
    return f

def data_page_exists(user_id, data_id):
    store = get_user_store(user_id)
    exists = data_id in store
    store.close()
    return exists

def get_data_page(user_id, data_id, create_if_missing=False):
    store = get_user_store(user_id)
    if data_id not in store:
        store.close()
        error(404, "unknown data_id {}".format(data_id))
     
    d = store[data_id]
    store.close() 
    return d

def get_data_page_latest(user_id, data_id):
    return get_data_page(user_id, data_id).tail()

def create_data_page(user_id, data_id, freq=sstsp.DEFAULT_FREQ, start_time = None, start_val = None):
    if user_store_exists(user_id):
        store = get_user_store(user_id, 'r+')
    else:
        store = create_user_store(user_id)

    if data_id in store:
        internal_error("Error creating dataframe - already exists")
    
    if start_time is None: start_time = time.time()
    
    idx = pd.to_datetime(start_time, unit='s')
    s = pd.Series(data=[start_val], index=[idx])
    s.name = data_id
    
    store[data_id] = s
    log.debug("created dataframe {} freq={} start={} val={} for user {}".format(data_id, 
        freq, idx, start_val, user_id))
    
    store.close() 

def append_data(user_id, data_id, t, val):
    store = get_user_store(user_id, 'r+')
    if data_id not in store:
        internal_error("{} not in store for {} as expected".format(data_id, user_id))
    
    d = store[data_id]
    idx = pd.to_datetime(t, unit='s')
    new_s = pd.Series(data=[val], index=[idx])
    new_s.name = data_id

    store[data_id] = d.append(new_s)
    store.close()


