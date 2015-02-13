#!/bin/env python

import subprocess
import os
import sys 
import re 
import numpy as np
import cPickle as pickle
from datetime import date, timedelta
from argparse import ArgumentParser

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from utils.Common import *
from utils.Shell import *

sys.path.append(os.path.dirname(os.path.abspath(__file__))+'/external/lib/python')
from prettytable import PrettyTable

# reporting resource usage per user in past 7 days
def report_rsrc_usage(trackusage):
    t = PrettyTable()
    t.field_names = ['id'] + map(lambda x:(date.today() - timedelta(x)).strftime('%Y-%m-%d'), range(1,7))
    for id in sorted(trackusage.keys()):
        data = []
        data.append(id)
        for d in map(lambda x:(date.today() - timedelta(x)).strftime('%y%m%d'), range(1,7)):
            try:
                data.append( trackusage[id][d] )
            except KeyError, e:
                data.append( 'NA' )
        t.add_row( data ) 
    print t

## execute the main program
if __name__ == "__main__":

    parg = ArgumentParser(description='script for collecting resource usage per user', version="0.1")

    parg.add_argument('-l','--loglevel',
                      action  = 'store',
                      dest    = 'verbose',
                      choices = ['-1', '0', '1', '2'],  ## choices work only with str
                      default = '0',
                      help    = 'set one of the following verbosity levels. -1:ERROR, 0|default:WARNING, 1:INFO, 2:DEBUG')

    parg.add_argument('-c', '--config',
                      action  = 'store',
                      dest    = 'fconfig',
                      default = os.path.dirname(os.path.abspath(__file__)) + '/etc/config.ini',
                      help    = 'set the configuration parameters, see "config.ini"')

    parg.add_argument('-r', '--rpt_usage',
                      action  = 'store_true',
                      dest    = 'rpt_usage',
                      default = False,
                      help    = 'report resource usage per user')

#    parg.add_argument('-f', '--format',
#                      action = 'store',
#                      dest   = 'format',
#                      choices = ['plain', 'json'],
#                      default = 'plain',
#                      help    = 'set output format')

    args = parg.parse_args()

    ## load config file and global settings
    c = getConfig(args.fconfig)
    TORQUE_LOG_DIR   = c.get('TorqueTracker','TORQUE_LOG_DIR') 
    BIN_QSTAT_ALL    = c.get('TorqueTracker','BIN_QSTAT_ALL') 
    BIN_FSHARE_ALL   = c.get('TorqueTracker','BIN_FSHARE_ALL') 
    DB_DATA_DIR    = c.get('TorqueTracker','DB_DATA_DIR')

    ## load logger and set the verbosity level
    logger = getMyLogger(os.path.basename(__file__))
    vlv = int(args.verbose)
    if vlv < 0:
        logger.setLevel(logging.ERROR)
    elif vlv == 1:
        logger.setLevel(logging.INFO)
    elif vlv >= 2:
        logger.setLevel(logging.DEBUG)

    # obtain fairshare reading
    fs = subprocess.check_output(BIN_FSHARE_ALL, shell=True).split('\n')
    
    # obtain rownames
    ## TODO: improve the parsing of data from BIN_FSHARE_ALL
    names = []
    re_user_beg  = re.compile('^USER$')
    re_group_beg = re.compile('^GROUP$')
    re_class_beg = re.compile('^CLASS$')

    idx = {'user':[],'group':[],'class':[]}
    k   = None
    for i in xrange(len(fs)):

        if re_user_beg.match(fs[i].strip()):
            k = 'user'
            idx[k].append(i+2)
            continue

        if re_group_beg.match(fs[i].strip()):
            k = 'group'
            idx[k].append(i+2)
            continue

        if re_group_beg.match(fs[i].strip()):
            k = 'class'
            idx[k].append(i+2)
            continue

        if k and fs[i].strip() == '':
            idx[k].append(i)
            k = None 
            continue

    logger.debug('line indices on %s output for USER/GROUP/CLASS' % BIN_FSHARE_ALL )
    logger.debug(' |_ %s ' % repr(idx))

#        try:
#            names.append(x.split()[0])
#        except:
#            pass
#
#    # exclude uninteresting ones
#    exclude = ['FairShare',
#               'Depth:',
#               'FS',
#               'System',
#               'FSInterval',
#               'FSWeight',
#               'TotalUsage',
#               'USER',
#               '-------------',
#               'GROUP',
#               'CLASS']
#    names = [x for x in names if not x in exclude]
    
    # Add yesterdays usage to tracking
    myfile = os.path.join(DB_DATA_DIR, 'userusage.p')
    if os.path.exists(myfile):
        f = open(myfile, 'r')
        trackusage = pickle.load(f)
        f.close()
    else:
        trackusage = dict()
    
    yesterday = date.today() - timedelta(1)
    yesterday = yesterday.strftime('%y%m%d')
   
    re_skip = re.compile('^DEFAULT')
  
    for line in fs[idx['user'][0]:idx['user'][1]]:
 
        data  = line.strip().split()
        name  = data[0]
        ## TODO: check carefully which data column of BIN_FSHARE_ALL corresponding to the usage of yesterday
        usage = data[4]

        if re_skip.match( name ):
            continue

        try:
            trackusage[name].keys()
        except KeyError:
            trackusage[name] = dict()

        if '-' in usage:
            usage = 0
        else:
            usage = float(usage)

        trackusage[name][yesterday] = usage

    if args.rpt_usage:
        report_rsrc_usage(trackusage)

    f = open(myfile, 'w')
    pickle.dump(trackusage, f)
    f.close()
