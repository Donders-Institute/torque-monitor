#!/usr/bin/env python

import os
import sys
import json
import time 

sys.path.append(os.path.dirname(os.path.abspath(__file__))+'/../../external/lib/python')
from prettytable import PrettyTable

## execute the main program
if __name__ == "__main__":

    '''usage: curl -s --compressed 'http://torquemon.dccn.nl/cgi-bin/get_trackTorqueJobs_data.py?type=jobs&period=3' |  ./cluster-find-jobs.py
              - OR -
              curl -s --compressed 'http://torquemon.dccn.nl/cgi-bin/get_trackTorqueJobs_data.py?jid=6309555' |  ./cluster-find-jobs.py'''

    data = json.loads( json.load(sys.stdin) )

    t = PrettyTable()

    ## TODO: make the filed description/key mapping more flexible
    t.field_names = ['id', 'user', 'queue', 'node', 'status', 'exitcode', 't_submit', 't_start', 't_finish']
    t.align['id']        = 'l'
    t.align['user']      = 'l'
    t.align['queue']     = 'l'
    t.align['node']      = 'l'
    t.align['status']    = 'l'
    t.align['exitcode']  = 'r'
    t.align['t_submit']  = 'r'
    t.align['t_start']   = 'r'
    t.align['t_finish']  = 'r'

    for d in sorted( data, key=lambda x:x['jid'] ):
        data = []
        data.append( d['jid'].split('.')[0] )
        data.append( d['uid']   )
        data.append( d['queue'] )


        ## handling multinodes jobs
        if d['node']:
            n_str = ''
            nlist = map(lambda x:x.split('/')[0], d['node'].split('+'))
            uniq_nlist = list(set(nlist))
            n_str += '%s:%d' % (uniq_nlist[0],nlist.count(uniq_nlist[0]))

            ## only show total number of nprocs additional to the first node in the list 
            n_add = sum( map(lambda x:nlist.count(x), uniq_nlist[1:]) )
            n_str += '+{n:%d,p:%d}' % (len(uniq_nlist)-1, n_add)
         
            data.append( n_str )
        else:
            data.append( d['node'] )

        data.append( d['cstat'] )
        data.append( d['jec']   )

        for ts in ['t_submit','t_start','t_finish']:
            if d[ts]:
                data.append( time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(d[ts])) )
            else:
                data.append( 'NA' )

        t.add_row( data )

    print t
