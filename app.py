from __future__ import print_function

import random
import datetime as dt

import flask
from flask import Flask, json, Response, render_template, jsonify, request, abort
from flask_cors import CORS, cross_origin
from io import StringIO
import logging as log
import time
import hashlib
import uuid

from bokeh.embed import components
from bokeh.resources import Resources
from bokeh.templates import JS_RESOURCES, CSS_RESOURCES
from bokeh.models.sources import ColumnDataSource, AjaxDataSource
from util import error, internal_error, tz_offset_seconds
from user import *
import plot

import sstsp

fromtimestamp = dt.datetime.fromtimestamp

app = Flask("sstsp")
CORS(app)

@app.errorhandler(401)
def custom_401(error):
    return Response('Valid API key required to update data', 401, {'WWWAuthenticate':'Basic realm="Valid API Key required"'})

@app.route('/d/<user_id>/<data_id>/latest', methods=['GET', 'PUT'])
def latest_data(user_id, data_id):
    if len(user_id) != sstsp.USER_ID_LEN:
        abort(400, "invalid user id - expecting {} char string".format(sstsp.USER_ID_LEN))

    if request.method == 'GET':# or request.method == 'POST':
        start = time.time()
        d = get_data_page_latest(user_id, data_id)
        log.debug("data page retrieve took {} ms".format(1000*(time.time() - start)))
        return d.to_json(orient='split')
    elif request.method == 'PUT':
        if 'v' not in request.form:
            error(400, "data post missing required value field 'v'")

        key = request.form.get('key', None)
        log.debug("Received PUT to {}/{} form:{}".format(user_id, data_id, request.form))
        try:
            key_uid = uuid.UUID(key)
        except ValueError:
            error(400, "badly formed hexadecimal UUID string") 
            
        hash_id = hashlib.sha256(key_uid.bytes).hexdigest()[:sstsp.USER_ID_LEN]

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

@app.route('/d/<user_id>/<data_id>', methods=['GET', 'PUT'])
def data(user_id, data_id):
    if len(user_id) != sstsp.USER_ID_LEN:
        abort(400, "invalid user id - expecting {} char string".format(sstsp.USER_ID_LEN))

    if request.method == 'GET':# or request.method == 'POST':
        start = time.time()
        d = get_data_page(user_id, data_id)
        log.debug("data page retrieve took {} ms".format(1000*(time.time() - start)))
        return d.to_json(orient='split')
    elif request.method == 'PUT':
        #FIXME -- this should allow post of more than 1 data point
        if 'v' not in request.form:
            error(400, "data post missing required value field 'v'")

        key = request.form.get('key', None)
        log.debug("Received PUT to {}/{} form:{}".format(user_id, data_id, request.form))
        try:
            key_uid = uuid.UUID(key)
        except ValueError:
            error(400, "badly formed hexadecimal UUID string") 
            
        hash_id = hashlib.sha256(key_uid.bytes).hexdigest()[:sstsp.USER_ID_LEN]

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

def get_data_source(user_id, data_id, tz_str=None):
  
    d = get_data_page(user_id, data_id)
    d.name = 'data'
    
    if tz_str is not None:
        offset_s = tz_offset_seconds(tz_str)
        log.debug("offseting date with {} seconds for  {}".format(offset_s, tz_str))
        d = d.tshift(offset_s, freq='s')
    
    source = ColumnDataSource(pd.DataFrame(d))
    
    def on_latest_change(attr, old, new):
        log.info('on_latest_change {}  old={} new={}'.format(attr, old, new))

    source.on_change('data', on_latest_change)

    return source

@app.route("/p/<user_id>/<data_id>")
def newplot(user_id, data_id):
    theme = request.args.get('theme', 'default')
    CDN = Resources(mode="cdn", minified=True,)
    templname = "plot.html"

    js_resources = JS_RESOURCES.render(
        js_raw=CDN.js_raw,
        js_files=CDN.js_files
    )

    css_resources = CSS_RESOURCES.render(
        css_raw=CDN.css_raw,
        css_files=CDN.css_files
    )
    
    tz = request.args.get("tz", None)
    source = get_data_source(user_id, data_id, tz)
    #ajax_source = get_ajax_latest_source(user_id, data_id)
    p = plot.create_main_plot(theme, source)
    plot_script, extra_divs = components(
        {
            "main_plot": p,
        }
    )
    
    themes = ["default", "dark"]
    options = { k: 'selected="selected"' if theme == k else "" for k in themes}

    return render_template(
        templname,
        theme = theme,
        extra_divs = extra_divs,
        plot_script = plot_script,
        js_resources=js_resources,
        css_resources=css_resources,
        theme_options=options,
    )

if __name__ == '__main__':
    log.basicConfig(level=log.DEBUG)
    app.run(debug=True)
