#!/usr/bin/env python

import os
import sys
import json
import time 

sys.path.append(os.path.dirname(os.path.abspath(__file__))+'/../../external/lib/python')
from prettytable import PrettyTable

## execute the main program
if __name__ == "__main__":

    '''usage: curl -s 'http://torquemon.dccn.nl/cgi-bin/get_trackTorque_data.py?type=stat&period=3' |  ./cluster-util-history.py'''

    data = json.loads( json.load(sys.stdin) )

    t = PrettyTable()

    ## TODO: make the filed description/key mapping more flexible
    t.field_names = ['time', 'queued', 'running', 'exiting', 'util']
    t.align['queued']  = 'r'
    t.align['running'] = 'r'
    t.align['exiting'] = 'r'
    t.align['util']    = 'r'

    for d in sorted( data, key=lambda x:x['timestamp'] ):
        data = []
        data.append( time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(d['timestamp'])) )
        data.append( d['njobs_queue'] )
        data.append( d['njobs_running'] )
        data.append( d['njobs_exiting'] )
        if int(d['cores_total']) > 0:
            data.append( '%.2f' % (1. * int(d['njobs_running']) / int(d['cores_total'])) )
        else:
            data.append( 'NaN' )
        t.add_row( data )

    print t
