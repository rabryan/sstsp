from __future__ import print_function

import os.path
import random
from functools import partial
import datetime as dt

import flask
from flask import Flask, json, Response, request
import h5py
import numpy as np
import pandas as pd
import dask.array as da
from io import StringIO
import logging as log
import time
import hashlib
import uuid

fromtimestamp = dt.datetime.fromtimestamp

USER_ID_LEN = 16
DEFAULT_FREQ = '1s'
app = Flask(__name__)


def to_seconds(ts):
    if isinstance(ts, dt.datetime):
        return (ts - dt.datetime(1970, 1, 1)).total_seconds() * 1000
    else:
        return 1000 * ((ts - np.datetime64('1970-01-01T00:00:00Z')) / np.timedelta64(1, 's'))

aapl = pd.read_csv('data/raw.csv')

def coarsen(reduction, x, factor):
    """

    >>> x = np.arange(10)
    >>> coarsen(np.max, x, 2)
    array([1, 3, 5, 7, 9])

    >>> coarsen(np.min, x, 5)
    array([0, 5])
    """
    axis = {0: factor, 1: factor}
    # Ensure that shape is divisible by coarsening factor
    slops = [-(d % factor) for d in x.shape]
    slops = [slop or None for slop in slops]
    x = x[tuple(slice(0, slop) for slop in slops)]
 
    if isinstance(x, np.ndarray):
        return da.chunk.coarsen(reduction, x, axis)
    if isinstance(x, da.Array):
        return da.coarsen(reduction, x, axis)
    raise NotImplementedError()

# build some data
factor = len(aapl) 
resampled = coarsen(np.mean, np.asarray(aapl.Price), factor)
tss = coarsen(np.min, np.asarray(aapl.Date), factor)
ftss = [fromtimestamp(x//1000).strftime("%Y-%m-%d %H:%M:%S") for x in tss]

curr_ds = dict(
    Date=[x for x in tss],
    DateFmt=[ts for ts in ftss],
    Price=[float(x) for x in resampled],
)

details = {
    "start": curr_ds['DateFmt'][0],
    "end": curr_ds['DateFmt'][-1],
    "factor": factor,
    "samples_no": len(curr_ds['DateFmt']),
    "original_samples_no": len(aapl),
}


@app.route('/subsample/<start>/<end>', methods=['GET', 'OPTIONS'])
def subsample(start, end):
    start = int(start)
    end = int(end)
    global curr_ds
    global details

    # use minutes dataset if timedelta of selected period is "short" enough
    # (note that this "short enough" is just arbitrary for this example)
    if end-start < 43383600000:
        xs = aapl_min[(aapl_min.Date > start) & (aapl_min.Date < end)]
    else:
        xs = aapl[(aapl.Date > start) & (aapl.Date < end)]

    factor = len(xs) // FACTOR_BASE
    if factor <= 1:
        tss = xs.Date
        resampled = xs.Price
    else:
        resampled = coarsen(np.mean, np.asarray(xs.Price), factor)
        tss = coarsen(np.min, np.asarray(xs.Date), factor)
    
    print(len(resampled))
    curr_ds = dict(
        Date=[x for x in tss],
        Price=[float(x) for x in resampled],
        DateFmt=[fromtimestamp(x//1000) for x in tss],
    )
    details = {
        "start": curr_ds['DateFmt'][0],
        "end": curr_ds['DateFmt'][-1],
        "factor": factor,
        "samples_no": len(tss),
        "original_samples_no": len(xs),
    }
    return json.jsonify(curr_ds)


def _valid_user_id(user_id):
    #FIXME - ensure user_id is correctly formed before file operations
    return True

def error(code, msg=""):
    log.info("{} error - {}".format(code, msg))
    raise flask.abort(code)

def internal_error(msg=""):
    error(500, msg)

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
        error(404, "unknown data_id {}".format(data_id))
     
    d = store[data_id]
    
    return d

def create_data_page(user_id, data_id, freq=DEFAULT_FREQ, start_time = None, start_val = None):
    if user_store_exists(user_id):
        store = get_user_store(user_id, 'r+')
    else:
        store = create_user_store(user_id)

    if data_id in store:
        internal_error("Error creating dataframe - already exists")
    
    if start_time is None: start_time = time.time()
    
    idx = pd.to_datetime(start_time, unit='s')
    s = pd.Series(data=[start_val], index=[idx])
    
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

    store[data_id] = d.append(new_s)
    store.close()


def get_user_page(user_id):
    log.debug("getting user page {}".format(user_id))
    return user_id

@app.errorhandler(401)
def custom_401(error):
    return Response('Valid API key required to update data', 401, {'WWWAuthenticate':'Basic realm="Valid API Key required"'})

@app.route('/d/<user_id>/<data_id>', methods=['GET', 'POST'])
def data(user_id, data_id):
    if len(user_id) != USER_ID_LEN:
        abort(400, "invalid user id - expecting {} char string".format(USER_ID_LEN))

    if request.method == 'GET':
        d = get_data_page(user_id, data_id)
        return d.to_json()
    elif request.method == 'POST':
        if 'v' not in request.form:
            error(400, "data post missing required value field 'v'")

        key = request.form.get('key', None)
        log.debug("Received POST to {}/{} form:{}".format(user_id, data_id, request.form))
        try:
            key_uid = uuid.UUID(key)
        except ValueError:
            error(400, "badly formed hexadecimal UUID string") 
            
        hash_id = hashlib.sha256(key_uid.bytes).hexdigest()[:USER_ID_LEN]

        if hash_id != user_id:
            error(401, "API key hash does not match user_id")
       

        v_str = request.form['v']
        try: 
            v = float(v_str)
        except ValueError:
            error(400, "expecting float for value - got {}".format(v_str))
        
        if "t" not in request.form:
            t = time.time()
        else:
            t_str = request.form['t']
            try:
                t = float(t_str)
            except ValueError:
                error(400, "expecting float or int for time - got {}".format(t_str))

        freq = request.form.get("f", "1s")
        log.debug("valid api key.  storing ({},{})".format(t,v))
        
        if not user_store_exists(user_id):
            log.debug("creating data store for new user {}".format(user_id))
            store = create_user_store(user_id)
            store.close()

        if not data_page_exists(user_id, data_id):
            log.debug("creating new data series '{}'".format(data_id))
            create_data_page(user_id, data_id, freq, t, v)
        else:
            append_data(user_id, data_id, t, v)
        
        return json.dumps({'success':True}), 200, {'ContentType':'application/json'} 
        


@app.route('/d/<user_id>', methods=['GET', 'OPTIONS'])
def get_user(user_id):
    log.debug("request args is {}".format(request.args))
    data_id = request.args.get("data_id", None)
    if data_id is None:
        return get_user_page(user_id) 
    else:
        log.debug("getting {} for {}".format(data_id, user_id))
    return str(user_id) + "." + data_id


@app.route('/alldata', methods=['GET', 'OPTIONS'])
def get_alldata():
    global curr_ds
    global details
    curr_ds = dict(
        Date=list(tss),
        DateFmt=list(ftss),
        Price=[float(x) for x in resampled],
    )
    details = {
        "start": curr_ds['DateFmt'][0],
        "end": curr_ds['DateFmt'][-1],
        "factor": factor,
        "samples_no": len(curr_ds['DateFmt']),
        "original_samples_no": len(aapl),
    }
    return json.jsonify(curr_ds)


@app.route('/details', methods=['GET', 'OPTIONS'])
def get_details():
    return json.jsonify(details)


@app.route('/alldata.csv', methods=['GET', 'OPTIONS'])
def get_csv_data():
    df = pd.DataFrame(curr_ds)

    dfbuffer = StringIO()
    df.to_csv(dfbuffer, encoding='utf-8', index=False)
    dfbuffer.seek(0)
    values = dfbuffer.getvalue()
    return Response(values, mimetype='text/csv')



if __name__ == '__main__':
    log.basicConfig(level=log.DEBUG)
    app.run(debug=True)
