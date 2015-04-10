#!/usr/bin/env python

try:
    import json
except ImportError:
    import simplejson as json

import sqlite3
import os
import sys
import cgi

sys.path.append( os.path.dirname(os.path.abspath(__file__)) + '/../../' )
from utils.Common  import *

def __get_data_sqlite__(db_fpath, table, clause=''):
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

    ## for resource table, also sort by host
    if table == 'rsrc':
        qry += ',host'
 
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

    ## load config file and global settings
    c = getConfig( os.path.dirname(os.path.abspath(__file__)) + '/../../etc/config.ini' )
    DB_DATA_DIR = c.get('TorqueTracker','DB_DATA_DIR')

    ### get HTTP parameters ###
    params = cgiFieldStorageToDict( cgi.FieldStorage() )
    clause = ''
    table  = 'rsrc'
  
    try:
        if params['mode'] == 'last':
            clause = 'timestamp in (SELECT max(timestamp) from rsrc)'
    except KeyError, e:
        pass 

    try:
        if params['type'] == 'stat':
            table = 'summeas'
    except KeyError, e:
        pass 

    try:
        if params['type'] == 'head':
            table  = 'hnode'
    except KeyError, e:
        pass 

    t_delta  = 1
    try:
        t_delta = float(params['period'])
        clause  = 'timestamp > %d' % int(time.time() - 86400*t_delta)
    except KeyError, e:
        pass 

    data    = []
    for d in getTimeInvolvement(mode='year', tdelta='-%fD' % t_delta):
        sql_db_path = '%s/mm_trackTorque_%s.db' % (DB_DATA_DIR, d)
        logger.debug( 'retrieving data from %s ... ' % sql_db_path )
        data += __get_data_sqlite__(sql_db_path, table=table, clause=clause)

    body = json.dumps(data)

    ### return header and context ###
    print "Status: 200 OK"
    print "Content-Type: application/json"
    print "Length:", len(body)
    print
    print json.JSONEncoder().encode(body) 
