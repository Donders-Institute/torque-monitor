#!/usr/bin/env python
import os
import re 
import sys
import cgi 
import logging
import inspect
import time

sys.path.append( os.path.dirname(os.path.abspath(__file__)) + '/../../' )
from utils.Common  import *

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
    IMAGE_DIR = os.path.join( DB_DATA_DIR, 'plots')

    ### get HTTP parameters ###
    params = cgiFieldStorageToDict( cgi.FieldStorage() )

    ### arguments
    period = time.strftime('%Y%m')
    try:
        period = params['period']
    except KeyError, e:
        pass 

    stat = 'qtime'
    try:
        stat = params['stat']
    except KeyError, e:
        pass

    type = 'scatter'
    try:
        type = params['type']
    except KeyError, e:
        pass

    if type == 'evolution':
        img_fpath = os.path.join( IMAGE_DIR, '%s_evolution_monthly.png' % stat )
    else:
        img_fpath = os.path.join( IMAGE_DIR, '%s_queue_scatter_%s.png' % (stat, period) )

    logger.debug('loading image: %s' % img_fpath)

    if os.path.exists( img_fpath ):

        f = open(img_fpath, 'rb')
        c = f.read()
        f.close()

        ### return HTTP header and context on success ###
        print "Status: 200 OK"
        print "Content-Type: image/png"
        print
        print c
    else:
        ### return HTTP header for error 404: file not found ###
        print "Status: 404 Not Found"
        print "Content-Type: text/html"
        print
        print "404 Not Found"