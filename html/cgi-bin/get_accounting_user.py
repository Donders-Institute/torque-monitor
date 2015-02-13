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
import gzip
import pprint 

sys.path.append( os.path.dirname(os.path.abspath(__file__)) + '/../../' )
from utils.Common  import *

def __qry_accounting_data__(db_fpath, table, clause=''):
    '''gets data from SQLite database'''

    conn = sqlite3.connect(db_fpath)
    conn.row_factory = sqlite3.Row
    c    = conn.cursor()
    qry  = 'SELECT * from %s' % table

    data = []

    if clause:
        qry += ' WHERE %s' % clause
  
    ## output is sorted by timestamp
    qry += ' ORDER BY timestamp DESC'

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

## return relevant part of data back to the client
def getEmptyData(ts, uid, gid='unknow'):
    '''get an default/empty data point for certain timestamp'''

    _queues = ['batch','inter','vgl','matlab']

    _d = {'timestamp': ts, 'uid': uid, 'gid':gid, 'nj': 0}

    for _s in ['s','f','k']:
        _d['cwt_%s' % _s] = 0
        _d['cct_%s' % _s] = 0

    for _q in _queues:
        for _s in ['s','f','k']:
            _d['avg_rmem_%s_%s'    % (_q,_s)] = -1.
            _d['avg_cmem_%s_%s'    % (_q,_s)] = -1.
            _d['avg_cvmem_%s_%s'   % (_q,_s)] = -1.
            _d['avg_eff_mem_%s_%s' % (_q,_s)] = -1. 
            _d['avg_cpu_util_%s_%s'% (_q,_s)] = -1.

    return _d; 

def getReducedData(data, queue):
    '''reduce the data and keep only information relevent to the job type (indicated by queue)'''

    _dlist  = []
    _queues = ['batch','inter','vgl','matlab']

    for _d in data:
        _dtmp = {}
        for k in ['timestamp','uid','gid']:
            _dtmp[k] = _d[k]

        if queue in _queues:
            _dtmp['nj'] = _d['nj_%s'  % queue]
            for _s in ['s','f','k']:
                _dtmp['cwt_%s' % _s] = _d['cwt_%s_%s' % (queue,_s)]
                _dtmp['cct_%s' % _s] = _d['cct_%s_%s' % (queue,_s)]
        else:
            _dtmp['nj'] = sum( [ _d['nj_%s'  % _q] for _q in _queues ] )
            for _s in ['s','f','k']:
                _dtmp['cwt_%s' % _s] = sum( [ _d['cwt_%s_%s' % (_q,_s)] for _q in _queues ] )
                _dtmp['cct_%s' % _s] = sum( [ _d['cct_%s_%s' % (_q,_s)] for _q in _queues ] )

        ## there is no average among jobs in different queue, therefore, preserve the information for each queue  
        for _q in _queues:
            for _s in ['s','f','k']:
                _dtmp['avg_rmem_%s_%s'   % (_q,_s)]  = _d['avg_rmem_%s_%s'     % (_q,_s)]
                _dtmp['avg_cmem_%s_%s'   % (_q,_s)]  = _d['avg_cmem_%s_%s'     % (_q,_s)]
                _dtmp['avg_cvmem_%s_%s'   % (_q,_s)] = _d['avg_cvmem_%s_%s'    % (_q,_s)]
                _dtmp['avg_eff_mem_%s_%s' % (_q,_s)] = _d['avg_eff_mem_%s_%s'  % (_q,_s)]
                _dtmp['avg_cpu_util_%s_%s'% (_q,_s)] = _d['avg_cpu_util_%s_%s' % (_q,_s)]

        ## remove records with 0 jobs  
        #if _dtmp['nj'] > 0:
        _dlist.append( _dtmp )

    return _dlist        

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
    table  = 'accounting'
  
    t_delta  = 7
    try:
        t_delta = float(params['period'])
    except KeyError, e:
        pass 

    ## limit to the given user
    clause = "uid == '%s'" % params['uid']

    ## limit to the specified time range
    t_min = datetime.date.today() - datetime.timedelta(days=t_delta) 
    clause += 'AND timestamp >= %d' % time.mktime(t_min.timetuple())

    ## limit to jobs in certain queue 
    queue = 'all'
    try:
        queue = params['queue']
    except KeyError, e:
        pass

    data  = []
    for d in getTimeInvolvement(mode='month', tdelta='-%fD' % t_delta):
        sql_db_path = '%s/mm_trackTorqueJobs_%s.db' % (DB_DATA_DIR, d)
        logger.debug( 'retrieving data from %s ... ' % sql_db_path )
        if os.path.exists( sql_db_path ):
            data += getReducedData( __qry_accounting_data__(sql_db_path, table=table, clause=clause), queue )
   
    ## try to get gid from the retrieved data
    gid = ''
    try:
        gid = data[0]['gid'] 
    except Exception, e:
        pass
 
    ## fill in data points if not available (when the user is not active on some days)
    dates  = map(lambda x:(datetime.date.today() - datetime.timedelta(x)).strftime('%Y%m%d'), range(1,t_delta))
    dplist = map(lambda x:x['timestamp'], data)

    for d in dates:
        ## make the d into timestamp
        ts = int(time.mktime(datetime.datetime.strptime( '%s 12:00:00' % d, '%Y%m%d %H:%M:%S').timetuple()))
        if ts not in dplist:
            ## append empty data for the date
            data.append( getEmptyData(ts, params['uid'], gid) )
 
    logger.info( 'number of data rows: %d' % len(data) )
    logger.info( 'first data row: %s'      % repr(data[0]) )

    ### compress the data
    body    = json.dumps(data)
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
