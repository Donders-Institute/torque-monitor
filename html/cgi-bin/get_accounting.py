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
  
    t_delta  = 60
    try:
        t_delta = float(params['period'])
    except KeyError, e:
        pass 

    ## determine the minimun timestamp for the query
    t_min = datetime.date.today() - datetime.timedelta(days=t_delta) 
    clause  = 'timestamp >= %d' % time.mktime(t_min.timetuple())

    ## ad-hoc class allowing quick record identification
    class HashDict(dict):
        def __hash__(self):
            return hash()
        def __eq__(self,other):
            return self['uid'] == other['uid'] and self['gid'] == other['gid']

    data = []
    for d in getTimeInvolvement(mode='month', tdelta='-%fD' % t_delta):
        sql_db_path = '%s/mm_trackTorqueJobs_%s.db' % (DB_DATA_DIR, d)
        logger.debug( 'retrieving data from %s ... ' % sql_db_path )
        if os.path.exists( sql_db_path ):
            data += map( lambda x:HashDict(x), __qry_accounting_data__(sql_db_path, table=table, clause=clause) )

    ### sum up accounting of different days
    beg_t = sys.maxint
    end_t = 0
 
    merged_data = []
    for d in data:
        if d['timestamp'] < beg_t:
            beg_t = d['timestamp']

        if d['timestamp'] > end_t:
            end_t = d['timestamp']
       
        del d['timestamp']  ## remove useless timestamp in the merged data 

        for c in ['matlab', 'batch', 'inter', 'vgl']:
            d[c] = {'nj'    : d['nj_%s' % c],
                    'rwt_s' : d['rwt_%s_s' % c], 'rwt_f' : d['rwt_%s_f' % c], 'rwt_k' : d['rwt_%s_k' % c],
                    'cwt_s' : d['cwt_%s_s' % c], 'cwt_f' : d['cwt_%s_f' % c], 'cwt_k' : d['cwt_%s_k' % c],
                    'cct_s' : d['cct_%s_s' % c], 'cct_f' : d['cct_%s_f' % c], 'cct_k' : d['cct_%s_k' % c]}
        try:
            idx = merged_data.index(d)
            ## add up values to the existing entry 
            for c in ['matlab', 'batch', 'inter', 'vgl']:
                for k in d[c].keys():
                    merged_data[idx][c][k] += d[c][k]

        except ValueError, e:
            ## initialize a new entry
            merged_data.append( dict(filter(lambda i:i[0] in ['uid','gid','matlab','batch','inter','vgl'], d.iteritems())) )

    logger.info( 'number of data rows: %d' % len(merged_data) )
    logger.info( 'first data row: %s'      % repr(merged_data[0]) )
    
    ### compress the data
    body    = json.dumps({'from':beg_t, 'to':end_t, 'data': merged_data})
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
