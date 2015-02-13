#!/usr/bin/env python

try:
    import json
except ImportError:
    import simplejson as json

import math
import datetime 
import sqlite3
import os
import re 
import sys
import cgi 
import logging
import inspect
import time
import pprint 

sys.path.append( os.path.dirname(os.path.abspath(__file__)) + '/../../' )
from utils.Common  import *

def __get_jobs_data__(db_fpath, table, c_timestamp, clauses):
    '''gets data from SQLite database'''

    conn = sqlite3.connect(db_fpath)
    conn.row_factory = sqlite3.Row
    c    = conn.cursor()
    qry  = 'SELECT * FROM %s WHERE (1==1)' % table

    data = []

    ## composing the query clause
    for cls in clauses:
        qry += ' AND %s' % cls

    ## output is sorted by timestamp
    qry += ' ORDER BY %s DESC' % c_timestamp

    logger.debug('SQL query: %s' % qry)

    try:
        for r in c.execute(qry):
            d = {}
            for k in r.keys():
                d[k] = r[k]
            data.append(d)
    except sqlite3.OperationalError,e:
        logger.warning('SQL error: %s' % repr(e))

    if conn:
        conn.close()

    return data  

def cgiFieldStorageToDict( fieldStorage ):
    """Get a plain dictionary, rather than the '.value' system used by the cgi module."""
    params = {}
    for key in fieldStorage.keys():
       params[ key ] = fieldStorage[ key ].value
    return params

## execute the main program
if __name__ == "__main__":

    global logger

    logger = getMyLogger(os.path.basename(__file__))
    logger.setLevel(logging.INFO)
    
    ## load config file and global settings
    c = getConfig( os.path.dirname(os.path.abspath(__file__)) + '/../../etc/config.ini' )
    DB_DATA_DIR = c.get('TorqueTracker','DB_DATA_DIR')

    ### get HTTP parameters ###
    params = cgiFieldStorageToDict( cgi.FieldStorage() )
    clause = ''

    ## default: show job statistics
    table  = 'statistics'
    c_timestamp = 'timestamp'

    clauses = []

    ## search for a specific job id in a given period
    try:
        if params['jid']:
            table       = 'jobs'
            c_timestamp = 't_submit'
            clauses.append('jid like \'%%%s%%\'' % params['jid'])
    except KeyError, e:
        pass 

    ## search for a specific job name in a given period
    try:
        if params['jname']:
            table       = 'jobs'
            c_timestamp = 't_submit'
            clauses.append('jname like \'%%%s%%\'' % params['jname'])
    except KeyError, e:
        pass 

    ## list all jobs in a given period 
    try:
        if params['type'] == 'jobs':
            table       = 'jobs'
            c_timestamp = 't_submit'
    except KeyError, e:
        pass 

    ## set default period of 7 days
    if 'period' not in params.keys():
        params['period'] = 7
  
    try:
        clauses.append('%s > %d' % (c_timestamp, int(time.time() - 86400*float(params['period']))))
    except KeyError, e:
        pass 

    data = []
    for d in getTimeInvolvement(mode='month', tdelta='-%fD' % float(params['period'])):
        sql_db_path = '%s/mm_trackTorqueJobs_%s.db' % (DB_DATA_DIR, d)
        logger.info( 'retrieving data from %s ... ' % sql_db_path )
        data += __get_jobs_data__(sql_db_path, table=table, c_timestamp=c_timestamp, clauses=clauses)

    ### merge data from the same day into one row
    if table == 'statistics':
        merged_data = []
        for d in data:
            t = d['timestamp']
 
            del d['timestamp']
            
            try:
                idx = map(lambda x:x['timestamp'],merged_data).index(t)
                merged_data[idx]['data'].append(d)
  
            except ValueError,e:
                merged_data.append({'timestamp': t, 'data': [d]})
 
        logger.info('first row representing date %d has %d sets of data' % (merged_data[0]['timestamp'], len(merged_data[0]['data'])) )
 
        body = json.dumps(merged_data)
    else:
        body = json.dumps(data)

    ### compress the data
    content = json.JSONEncoder().encode(body)
    gzipped = gzipContent(content)

    logger.info('content size: %d' % len(gzipped))

    ### return header and context ###
    print "Status: 200 OK"
    print "Content-Type: application/json"
    print "Content-Encoding: gzip"
    #print "Length:", len(body)
    print "Length:", len(gzipped)
    print
    #print json.JSONEncoder().encode(body)
    print gzipped
