#!/usr/bin/env python
#future comptability
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from builtins import (ascii, bytes, chr, dict, filter, hex, input,
                      int, map, next, oct, open, pow, range, round,
                      str, super, zip)

from flask import Flask
from flask import render_template
from flask import g
from flask import request
from flask import jsonify
from werkzeug.serving import WSGIRequestHandler
import re
from dateutil import parser

from database import Database
from database import dict_factory

app = Flask(__name__)

"""
note - the trailing \ on the first two lines iso8601Pattern are only there to break the string into multiple
lines and are not apart of the regex expression.
http://www.pelagodesign.com/blog/2009/05/20/iso-8601-date-validation-that-doesnt-suck/
"""
iso8601Pattern = r"""^([\+-]?\d{4}(?!\d{2}\b))((-?)((0[1-9]|1[0-2])(\3([12]\d|0[1-9]|3[01]))?|W([0-4]\d|5[0-2])\ 
(-?[1-7])?|(00[1-9]|0[1-9]\d|[12]\d{2}|3([0-5]\d|6[1-6])))([T\s]((([01]\d|2[0-3])((:?)[0-5]\d)?|24\:?00)([\.,]\\
d+(?!:))?)?(\17[0-5]\d([\.,]\d+)?)?([zZ]|([\+-])([01]\d|2[0-3]):?([0-5]\d)?)?)?)?$"""
#compile the regex pattern now and use it globally for a better performance
iso8601Test = re.compile(iso8601Pattern)

def arg_valid(**kwargs):
    isValid = False
    if kwargs.get('iso8601', None):
        if iso8601Test.match(kwargs['iso8601']):
            isValid = True
    elif kwargs.get('records', None):
        if kwargs['records'].isnumeric():
            val = int(kwargs['records'])
            if val > 0 and val <= 1000:
                isValid = True
    return isValid

@app.before_request
def before_request():
    """
    always log the request path
    """
    logger.info('request path:{}   remote address:{}'.format(request.full_path, request.remote_addr))

def get_db():
    """
    opens a new database connection if there is none yet for the
    current application context
    """
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = Database('logs.db')
        db.set_row_factory()
#         db.cursor.row_factory = dict_factory
    return db

@app.teardown_appcontext
def close_db(exception):
    """
    closes the database at the end of the request
    """
    db = getattr(g, '_database', None)
    if db is not None:
        db.connection.close()

@app.errorhandler(404)
def page_not_found(e):
    """
    any route not defined in this api will land here
    """
    logger.warning(e)
    return 'HTTP/1.1 404 Not Found', 404

@app.route('/')
def index():    
    user = request.args.get('user', None)
    print(request.args)
    if user:
        response = 'Welcome {} ;)'.format(user)
    else:
        response = 'This is the index page...'
    return response

@app.route('/read/json/<records>/<start>')
def get_data_logs(records, start):
    db = get_db()
    if (start!='last' and not arg_valid(iso8601=start)) or not arg_valid(records=records):
        response = 'HTTP/1.1 500 Internal Server Error'
        logger.warning(response)
        return response, 500
    else:
        if start == 'last':
            #determine how many records exist in the db
            records = int(records)
            recordCount = db.cursor.execute('SELECT COUNT(*) FROM {}'.format(db.table)).fetchone()[0]
            if records > recordCount:
                #if requesting more records than are present in the db, just return everything in db
                db.cursor.execute('SELECT * FROM {}'.format(db.table))
            else:
                #grab the time stamp from the 'records' from last entry in the db
                startTime = db.cursor.execute('SELECT Time FROM {} WHERE rowid==({}-{}+1) LIMIT 1'.format(db.table, recordCount, records)).fetchone()[0]
                #select all with timestamp >= startTime
                db.cursor.execute('SELECT * FROM {} WHERE Time>="{}"'.format(db.table, startTime))      
        else:
            start = parser.parse(start).isoformat()
            #grab time 'records' number of time stamps from 'start' time forward
            timestamps = 'SELECT Time FROM {} WHERE Time>"{}" ORDER BY rowid ASC LIMIT {}'.format(db.table, start, records)
            #select the time stamp that is the newest from the timestamps set of records
            endTime = db.cursor.execute('SELECT max(Time) FROM ({})'.format(timestamps)).fetchone()[0]
            #select records between 'start' and 'endtime'
            db.cursor.execute('SELECT * FROM {} WHERE (Time>"{}" AND Time<="{}")'.format(db.table, start, endTime))
        #jsonify cannot serialize sqlite3.Row objects, so use list comprehension to convert to a list of dictionaries
        return jsonify([dict(row) for row in db.cursor.fetchall()])
        

@app.route('/alarm/json/<int:records>/<start>')
def get_alarm_logs(records, start):
    return jsonify([])

#@app.route('/dbreset')
#def dbreset():
#    response = dict(message='call not supported; not needed')
#    return jsonify(response)
#
#@app.route('/exit')
#def kill_datalogger():
#    response = dict(message='call not supported yet; need to implement')
#    return jsonify(response)

@app.route('/killkenny')
def kill_kenny():
    """
    special endpoint to kill the rest api server
    """
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError('Not running with the Werkzeug Server')
    func()
    return 'OMG! ...you killed kenny!' 

def main():
    pass

if __name__ == "__main__":
    import logging
    logger = logging.getLogger(__name__)
#     logging.basicConfig(stream=sys.stderr, level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)-5s - %(message)s')
    loggingWhitelist = ('root', 'plcclient', 'snap7', 'database', 'datalogger', 'restapi', '__main__')
    loggingFilename = 'restapi.log'
    loggingFormat = '%(asctime)s %(name) -15s %(levelname)-9s %(message)s'
    loggingDateFormat = '%Y-%m-%d %H:%M:%S'
    loggingLevel = logging.DEBUG
    
    class Whitelist(logging.Filter):
        def __init__(self, *whitelist):
            self.whitelist = [logging.Filter(name) for name in whitelist]
    
        def filter(self, record):
            return any(f.filter(record) for f in self.whitelist)

    logging.basicConfig(  
#         stream=sys.stderr, 
        filename=loggingFilename,
        level=loggingLevel,
        format=loggingFormat,
        datefmt=loggingDateFormat
        )
    for handler in logging.root.handlers:
        handler.addFilter(Whitelist(*loggingWhitelist))
    main()
    logger.info('starting app...')
    WSGIRequestHandler.protocol_version = 'HTTP/1.1'
    app.run(host='0.0.0.0', port=5000, debug=True)

  
