#!/bin/env python

import sys
import subprocess
import datetime
import itertools
import glob
import os
import sqlite3 
from argparse import ArgumentParser

from numpy import mean, median, std, var, histogram
import cPickle as pickle

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from utils.Common  import *
from utils.Shell   import *
from utils.Cluster import *

sys.path.append(os.path.dirname(os.path.abspath(__file__))+'/external/lib/python')
from prettytable import PrettyTable

def __sqlite_job_info__(jlist):
    '''saving job information into SQLite database'''

    conn    = sqlite3.connect(SQLite_DB_PATH)
    c       = conn.cursor()

    try:
        sql = '''CREATE TABLE jobs (jid      TEXT,
                                    jname    TEXT,
                                    jstat    TEXT,
                                    jec      INTEGER,
                                    cstat    TEXT,
                                    uid      TEXT,
                                    queue    TEXT,
                                    rmem     REAL,
                                    rwtime   INTEGER,
                                    cmem     REAL,
                                    cvmem    REAL,
                                    cwtime   INTEGER,
                                    cctime   INTEGER,
                                    node     TEXT,
                                    t_submit INTEGER,
                                    t_queue  INTEGER,
                                    t_start  INTEGER,
                                    t_finish INTEGER,
                                    PRIMARY KEY (jid DESC)
                                    )'''
        c.execute(sql)
    except sqlite3.OperationalError,e:
        logger.warning('SQL error: %s' % repr(e)) 

    sql  = '''INSERT INTO jobs VALUES (''' + ','.join(['?']*18) + ''')'''
    data = []

    for j in jlist:
        data.append( (j.jid    , j.jname  , j.jstat , j.jec     , j.cstat, j.uid,
                      j.queue  , j.rmem   , j.rwtime, j.cmem    , j.cvmem,
                      j.cwtime , j.cctime , j.node  , j.t_submit,
                      j.t_queue, j.t_start, j.t_finish) )

    logger.debug(data[-1])

    ## insert and commit multiple rows into the table
    try:
        c.executemany(sql, data)
        conn.commit()
    except Exception, e:
        print e
        conn.rollback()

    ## close the database connection 
    if conn:
        conn.close()
 
    return


def __sqlite_job_stat__(summeas):
    '''saving job information into SQLite database'''

    conn    = sqlite3.connect(SQLite_DB_PATH)
    c       = conn.cursor()

    try:
        sql = '''CREATE TABLE statistics (timestamp   INTEGER,
                                          queue       TEXT,
                                          status      TEXT,
                                          njobs       INTEGER,
                                          nusers      INTEGER,
                                          rwtime_min  REAL,
                                          rwtime_max  REAL,
                                          rwtime_mean REAL,
                                          rwtime_std  REAL,
                                          cwtime_min  REAL,
                                          cwtime_max  REAL,
                                          cwtime_mean REAL,
                                          cwtime_std  REAL,
                                          cctime_min  REAL,
                                          cctime_max  REAL,
                                          cctime_mean REAL,
                                          cctime_std  REAL,
                                          twait_min   REAL,
                                          twait_max   REAL,
                                          twait_mean  REAL,
                                          twait_std   REAL,
                                          rmem_min    REAL,
                                          rmem_max    REAL,
                                          rmem_mean   REAL,
                                          rmem_std    REAL,
                                          cmem_min    REAL,
                                          cmem_max    REAL,
                                          cmem_mean   REAL,
                                          cmem_std    REAL,
                                          cvmem_min   REAL,
                                          cvmem_max   REAL,
                                          cvmem_mean  REAL,
                                          cvmem_std   REAL,
                                          eff_wtime_min  REAL,
                                          eff_wtime_max  REAL,
                                          eff_wtime_mean REAL,
                                          eff_wtime_std  REAL,
                                          eff_wtime_var  REAL,
                                          eff_mem_min    REAL,
                                          eff_mem_max    REAL,
                                          eff_mem_mean   REAL,
                                          eff_mem_std    REAL,
                                          eff_mem_var    REAL,
                                          cpu_util_min   REAL,
                                          cpu_util_max   REAL,
                                          cpu_util_mean  REAL,
                                          cpu_util_std   REAL,
                                          cpu_util_var   REAL,
                                          UNIQUE (timestamp,queue,status) ON CONFLICT REPLACE
                                          )'''
        c.execute(sql)
    except sqlite3.OperationalError,e:
        logger.warning('SQL error: %s' % repr(e)) 

    sql  = '''INSERT INTO statistics VALUES (''' + ','.join(['?']*48) + ''')'''
    data = []

    ts = int(time.mktime(datetime.datetime.strptime( '%s 12:00:00' % args.jobdate, '%Y%m%d %H:%M:%S').timetuple()))

    for d in summeas:
        data.append( (ts,
                      d['queue']        , d['stat']         , d['njobs']         , d['nusers'],
                      d['rwtime']['min'], d['rwtime']['max'], d['rwtime']['mean'], d['rwtime']['std'],
                      d['cwtime']['min'], d['cwtime']['max'], d['cwtime']['mean'], d['cwtime']['std'],
                      d['cctime']['min'], d['cctime']['max'], d['cctime']['mean'], d['cctime']['std'],
                      d['twait']['min'] , d['twait']['max'] , d['twait']['mean'], d['twait']['std'],
                      d['rmem']['min']  , d['rmem']['max']  , d['rmem']['mean'], d['rmem']['std'],
                      d['cmem']['min']  , d['cmem']['max']  , d['cmem']['mean'], d['cmem']['std'],
                      d['cvmem']['min'] , d['cvmem']['max'] , d['cvmem']['mean'], d['cvmem']['std'],
                      d['eff_wtime']['min'] , d['eff_wtime']['max'] , d['eff_wtime']['mean'],
                      d['eff_wtime']['std'] , d['eff_wtime']['var'],
                      d['eff_mem']['min']   , d['eff_mem']['max']   , d['eff_mem']['mean'],
                      d['eff_mem']['std']   , d['eff_mem']['var'],
                      d['cpu_util']['min']  , d['cpu_util']['max']  , d['cpu_util']['mean'],
                      d['cpu_util']['std']  , d['cpu_util']['var']) )

    logger.debug(data[-1])

    ## insert and commit multiple rows into the table
    try:
        c.executemany(sql, data)
        conn.commit()
    except Exception, e:
        print e
        conn.rollback()

    ## close the database connection 
    if conn:
        conn.close()
 
    return

def __get_2sigma__(histo):
    '''calculate the lower and upper bound of an area within which the integrated propability is about 0.9545'''
 
    cpdf  = []

    pdf  = histo[0]
    edgs = histo[1]
    
    bound = [None, None]
    
    cv = 0
    for i in xrange( len(pdf) ):
        cpdf.append( cv + pdf[i]*( edgs[i+1] - edgs[i] ) )  ## should multiply the bin width to get normalization to 1 
        cv = cpdf[i]

    #print cpdf
    
    for i in xrange( len(cpdf) ):
        
        if cpdf[i] >= 0.021 and not bound[0]:
           bound[0] = edgs[i]    # take the lower edg of this bin 
           continue
    
        if cpdf[i] >= 0.979 and not bound[1]:
           bound[1] = edgs[i+1]  # take the upper edge of this bin (ie. lower edge of the next bin)
           break

    return bound

def __get_accounting__(_jlist):
    '''Make accounting table for resource usage by users and groups'''

    ## check with jobs having resource consumption information
    _jlist = filter(lambda x:x.cmem is not None and x.cwtime is not None and x.cctime is not None, _jlist)

    ## TODO: exclude jobs with non-sense information, e.g.
    ##        - cput overflow when job runs over the requested walltime,
    ##          but is still allowed to continue as the CPU speed is slower
    _jlist = filter(lambda x:x.cctime < 4 * x.cwtime, _jlist)

    # get a list of job owners and sort by group
    owners = list(set(map(lambda x:HashableDict({'uid':x.uid, 'gid':x.gid}), _jlist)))
    owners = sorted(owners, key=lambda x:x['gid'], reverse = False)

    logger.info('number of active users: %d' % len(owners))

    t = PrettyTable()

    ## TODO: make the filed description/key mapping more flexible
    t.field_names = ['uid', 'gid', 'njobs (m/b/i/v)', 'r_wtime in hrs. (s/f/k/t)', 'c_wtime in hrs. (s/f/k/t)', 'c_ctime in hrs. (s/f/k/t)']
    t.align['uid'] = 'r'
    t.align['gid'] = 'r'

    sql_data = []

    q_cat = {'matlab' : ['matlab'],
             'inter'  : ['interactive'], 
             'vgl'    : ['vgl'], 
             'batch'  : TORQUE_BATCH_QUEUES}

    def __make_data_row__(_ts, _oid, _gid, _olist ):
        '''function to prepare each data row for PrettyTable'''

        _d = {}
        for c,q in q_cat.iteritems():
            _sd = {'nj':0,
                   'r_wtime_s'     :0 , 'r_wtime_f'     :0 , 'r_wtime_k'     :0 ,  # requested walltime
                   'c_wtime_s'     :0 , 'c_wtime_f'     :0 , 'c_wtime_k'     :0 ,  # consumed walltime
                   'c_ctime_s'     :0 , 'c_ctime_f'     :0 , 'c_ctime_k'     :0 ,  # consumed cputime
                   'avg_rmem_s'    :-1, 'avg_rmem_f'    :-1, 'avg_rmem_k'    :-1, 'avg_rmem_t'    :-1,  # average of requested physical memory
                   'avg_cmem_s'    :-1, 'avg_cmem_f'    :-1, 'avg_cmem_k'    :-1, 'avg_cmem_t'    :-1,  # average of consumed physical memory
                   'avg_cvmem_s'   :-1, 'avg_cvmem_f'   :-1, 'avg_cvmem_k'   :-1, 'avg_cvmem_t'   :-1,  # average of consumed virtual memory
                   'avg_eff_mem_s' :-1, 'avg_eff_mem_f' :-1, 'avg_eff_mem_k' :-1, 'avg_eff_mem_t' :-1,  # average of memory efficiency (i.e. consumed memory / requested memory)
                   'avg_cpu_util_s':-1, 'avg_cpu_util_f':-1, 'avg_cpu_util_k':-1, 'avg_cpu_util_t':-1,  # average of cpu utilization (i.e. consumed cputime / consumed walltime)
                  }
            
            _sub_olist   = filter(lambda x:x.queue in q, _olist )
            _sub_olist_s = filter(lambda x:x.cstat == 'csuccess', _sub_olist ) 
            _sub_olist_f = filter(lambda x:x.cstat == 'cfailed' , _sub_olist ) 
            _sub_olist_k = filter(lambda x:x.cstat == 'killed'  , _sub_olist ) 
            _sd['nj'] = len( _sub_olist )

            _sd['r_wtime_s'] = sum( map( lambda x:x.rwtime, _sub_olist_s ) ) / 3600.
            _sd['r_wtime_f'] = sum( map( lambda x:x.rwtime, _sub_olist_f ) ) / 3600.
            _sd['r_wtime_k'] = sum( map( lambda x:x.rwtime, _sub_olist_k ) ) / 3600.
            _sd['r_wtime_t'] = sum( map( lambda x:x.rwtime, _sub_olist   ) ) / 3600.
            _sd['c_wtime_s'] = sum( map( lambda x:x.cwtime, _sub_olist_s ) ) / 3600.
            _sd['c_wtime_f'] = sum( map( lambda x:x.cwtime, _sub_olist_f ) ) / 3600.
            _sd['c_wtime_k'] = sum( map( lambda x:x.cwtime, _sub_olist_k ) ) / 3600.
            _sd['c_wtime_t'] = sum( map( lambda x:x.cwtime, _sub_olist   ) ) / 3600.
            _sd['c_ctime_s'] = sum( map( lambda x:x.cctime, _sub_olist_s ) ) / 3600.
            _sd['c_ctime_f'] = sum( map( lambda x:x.cctime, _sub_olist_f ) ) / 3600.
            _sd['c_ctime_k'] = sum( map( lambda x:x.cctime, _sub_olist_k ) ) / 3600.
            _sd['c_ctime_t'] = sum( map( lambda x:x.cctime, _sub_olist   ) ) / 3600.

            for s, jl in zip( ['s','f','k','t'],
                              [ _sub_olist_s, _sub_olist_f, _sub_olist_k, _sub_olist ] ):

                if len( jl ) > 0:
                    l_rmem = map( lambda x:x.rmem, jl )
                    l_cmem = map( lambda x:x.cmem, jl )

                    _sd['avg_rmem_%s' % s]     = mean( map( lambda x:x.rmem if x.rmem else 0, jl ) )
                    _sd['avg_cmem_%s' % s]     = mean( map( lambda x:x.cmem , jl ) )
                    _sd['avg_cvmem_%s' % s]    = mean( map( lambda x:x.cvmem, jl ) )
                    _sd['avg_eff_mem_%s' % s]  = mean( [ 1.0*a/b if b else 0 for a,b in zip(l_cmem, l_rmem) ] )

                    if len( filter( lambda x:x.cwtime > 60, jl ) ) > 0:
                        # For avoiding the bias due to extremely short jobs, only the jobs with at-least 1 mins. wallclock time consumption are accounted
                        l_cwt  = map( lambda x:x.cwtime, filter( lambda x:x.cwtime > 60, jl ) )
                        l_cct  = map( lambda x:x.cctime, filter( lambda x:x.cwtime > 60, jl ) )
                        _sd['avg_cpu_util_%s' % s] = mean( [ 1.0*a/b for a,b in zip(l_cct , l_cwt ) ] )
                    else:
                        # but if there are only those extremely short jobs are available, calculate the avg. cpu utilization anyway 
                        l_cwt  = map( lambda x:x.cwtime, jl )
                        l_cct  = map( lambda x:x.cctime, jl )
                        _sd['avg_cpu_util_%s' % s] = mean( [ 1.0*a/b for a,b in zip(l_cct , l_cwt ) ] )

            _d[c] = _sd;

        d_sql = (_ts, _oid, _gid)

        for c in ['matlab','batch','inter','vgl']:
            d_sql += (_d[c]['nj'],
                      _d[c]['r_wtime_s'],
                      _d[c]['r_wtime_f'],
                      _d[c]['r_wtime_k'],
                      _d[c]['c_wtime_s'],
                      _d[c]['c_wtime_f'],
                      _d[c]['c_wtime_k'],
                      _d[c]['c_ctime_s'],
                      _d[c]['c_ctime_f'],
                      _d[c]['c_ctime_k'],
                      _d[c]['avg_rmem_s'],
                      _d[c]['avg_rmem_f'],
                      _d[c]['avg_rmem_k'],
                      _d[c]['avg_rmem_t'],
                      _d[c]['avg_cmem_s'],
                      _d[c]['avg_cmem_f'],
                      _d[c]['avg_cmem_k'],
                      _d[c]['avg_cmem_t'],
                      _d[c]['avg_cvmem_s'],
                      _d[c]['avg_cvmem_f'],
                      _d[c]['avg_cvmem_k'],
                      _d[c]['avg_cvmem_t'],
                      _d[c]['avg_eff_mem_s'],
                      _d[c]['avg_eff_mem_f'],
                      _d[c]['avg_eff_mem_k'],
                      _d[c]['avg_eff_mem_t'],
                      _d[c]['avg_cpu_util_s'],
                      _d[c]['avg_cpu_util_f'],
                      _d[c]['avg_cpu_util_k'],
                      _d[c]['avg_cpu_util_t']
                     )

        d_tab = [_oid, _gid, '/'.join( ['%7d'   % _d[k]['nj'] for k in ['matlab','batch','inter','vgl']] ),
                             '/'.join( ['%8.1f' % sum( _d[k][t] for k in q_cat.keys()) for t in ['r_wtime_s','r_wtime_f','r_wtime_k','r_wtime_t']] ),
                             '/'.join( ['%8.1f' % sum( _d[k][t] for k in q_cat.keys()) for t in ['c_wtime_s','c_wtime_f','c_wtime_k','c_wtime_t']] ),
                             '/'.join( ['%8.1f' % sum( _d[k][t] for k in q_cat.keys()) for t in ['c_ctime_s','c_ctime_f','c_ctime_k','c_ctime_t']] ) ]

        return (d_sql, d_tab)

    for o in owners:
        ts = int(time.mktime(datetime.datetime.strptime( '%s 12:00:00' % args.jobdate, '%Y%m%d %H:%M:%S').timetuple()))

        d_sql,d_tab = __make_data_row__(ts, o['uid'], o['gid'], filter(lambda x:x.uid == o['uid'], _jlist))
        sql_data.append( d_sql )
        t.add_row( d_tab )

    ## storing data to sqlite database
    if args.monitor: 

        conn    = sqlite3.connect(SQLite_DB_PATH)
        c       = conn.cursor()
 
        try:
            sql  = 'CREATE TABLE accounting (timestamp INTEGER, uid TEXT, gid TEXT'
            for k in ['matlab','batch','inter','vgl']:
                sql += ', nj_%s INTEGER' % k
                sql += ', ' + ' ,'.join( ['rwt_%s_%s REAL' % (k,s) for s in ['s','f','k'] ] )
                sql += ', ' + ' ,'.join( ['cwt_%s_%s REAL' % (k,s) for s in ['s','f','k'] ] )
                sql += ', ' + ' ,'.join( ['cct_%s_%s REAL' % (k,s) for s in ['s','f','k'] ] )
                sql += ', ' + ' ,'.join( ['avg_rmem_%s_%s REAL'  % (k,s) for s in ['s','f','k','t'] ] )
                sql += ', ' + ' ,'.join( ['avg_cmem_%s_%s REAL'  % (k,s) for s in ['s','f','k','t'] ] )
                sql += ', ' + ' ,'.join( ['avg_cvmem_%s_%s REAL' % (k,s) for s in ['s','f','k','t'] ] )
                sql += ', ' + ' ,'.join( ['avg_eff_mem_%s_%s REAL' % (k,s) for s in ['s','f','k','t'] ] )
                sql += ', ' + ' ,'.join( ['avg_cpu_util_%s_%s REAL' % (k,s) for s in ['s','f','k','t'] ] )
            sql += ', UNIQUE (timestamp,uid,gid) ON CONFLICT REPLACE)'

            logger.debug(sql)

            c.execute(sql)
        except sqlite3.OperationalError,e:
            logger.warning('SQL error: %s' % repr(e)) 
 
        sql  = '''INSERT INTO accounting VALUES (''' + ','.join(['?']*123) + ''')'''
 
        logger.debug(sql_data[-1])
 
        ## insert and commit multiple rows into the table
        try:
            c.executemany(sql, sql_data)
            conn.commit()
        except Exception, e:
            print e
            conn.rollback()
 
        ## close the database connection 
        if conn:
            conn.close()
    else:
        ## print the accounting table on the screen if not in monitoring mode 
        print t


## execute the main program
if __name__ == "__main__":

## parsing opt/args using optparse
## TODO: optparse has been deprecated in Python 2.7, migrate to argparse

    parg = ArgumentParser(description='script for collecting torque/maui-cluster job information', version="0.1")

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

    parg.add_argument('-m', '--monitor',
                      action  = 'store_true',
                      dest    = 'monitor',
                      default = False,
                      help    = 'store data in SQLite database for monitoring purpose')

    parg.add_argument('-d', '--date',
                      action  = 'store',
                      dest    = 'jobdate',
                      default = (datetime.date.today() - datetime.timedelta(1)).strftime('%Y%m%d'),
                      help    = 'set the date on which the jobs were submitted in a format of, e.g. 20140101')

    parg.add_argument('-p', '--period',
                      action  = 'store',
                      dest    = 'dateperiod',
                      default = 1,
                      help    = 'set the backward period (in days) in which the jobs were submitted, using the starting date given by -d|--date option.')

    parg.add_argument('-a', '--accounting',
                      action  = 'store_true',
                      dest    = 'accounting',
                      default = False,
                      help    = 'display resource utilization accounting')

#    parg.add_argument('-f', '--format',
#                      action = 'store',
#                      dest   = 'format',
#                      choices = ['plain', 'json'],
#                      default = 'plain',
#                      help    = 'set output format')

    global args

    args = parg.parse_args()

    ## load config file and global settings
    c = getConfig(args.fconfig)

    global TORQUE_LOG_DIR
    global TORQUE_BATCH_QUEUES
    global DB_DATA_DIR
    global SQLite_DB_PATH
    global logger 

    TORQUE_LOG_DIR      = c.get('TorqueTracker','TORQUE_LOG_DIR') 
    TORQUE_BATCH_QUEUES = c.get('TorqueTracker','TORQUE_BATCH_QUEUES').split(',')
    DB_DATA_DIR         = c.get('TorqueTracker','DB_DATA_DIR')

    ## load logger and set the verbosity level
    logger = getMyLogger(os.path.basename(__file__))
    vlv = int(args.verbose)
    if vlv < 0:
        logger.setLevel(logging.ERROR)
    elif vlv == 1:
        logger.setLevel(logging.INFO)
    elif vlv >= 2:
        logger.setLevel(logging.DEBUG)

    SQLite_DB_PATH   = os.path.join( DB_DATA_DIR, '%s_%s.db' % (os.path.basename(__file__).replace('.py',''), args.jobdate[:-2]) )

    # parsing the information of jobs submitted during the given period of time
    d_beg = datetime.datetime.strptime(args.jobdate, '%Y%m%d')
    
    # it makes more sense to check just one time point when the monitor argument is on
    if args.monitor:
        args.dataperiod = 1

    dates = map(lambda x:x.strftime('%Y%m%d'), [ d_beg - datetime.timedelta(days=d) for d in range(0, int(args.dateperiod)) ])
    jlist = []
    for d in dates:
        logger.info('collecting jobs on %s' % d)
        jlist += get_complete_jobs(TORQUE_LOG_DIR, d, debug=(vlv >= 2))


    count             = len(jlist)
    no_r_count        = len( filter(lambda x:x.cmem   is None, jlist) )  ## jobs without resource consumption information
    no_walltime_count = len( filter(lambda x:x.cwtime is None, jlist) )  ## jobs without wallclock time consumption information 
    no_cputime_count  = len( filter(lambda x:x.cctime is None, jlist) )  ## jobs without wallclock time consumption information 

    logger.info('number of jobs: %d' % count)
    logger.info('number of jobs w/t resource consumption info: %d' % no_r_count)
    logger.info('number of jobs w/t walltime consumption info: %d' % no_walltime_count)
    logger.info('number of jobs w/t cputime  consumption info: %d' % no_cputime_count)
 
    if args.accounting:
        __get_accounting__(jlist)

    if args.monitor:
        __sqlite_job_info__(jlist)

    def __filter_jlist__(jlist, queue, stat):
        ''' filter jlist with selection on queue and stat '''

        myjlist = jlist

        if queue == 'batch':
            myjlist = filter( lambda x:x.queue in TORQUE_BATCH_QUEUES and x.cstat == stat, myjlist )
        else:
            myjlist = filter( lambda x:x.queue == queue and x.cstat == stat, myjlist )

        return myjlist

    ## statistical measurement (only on jobs with resource consumption information)
    jlist = filter(lambda x:x.cmem is not None and x.cwtime is not None and x.cctime is not None, jlist)

    hbins_mem  = range(0,   101,  1) + range(150,    10*50,   50) + [550, 1000]
    hbins_time = range(0, 7*300,300) + range(3600, 24*3600, 3600) + [3600 * 72]
    hbins_eff  = map(lambda x:0.001*x, range(0, 1001, 1))

    summeas = []
    for queue in ['interactive', 'matlab' , 'vgl', 'batch']:
        for stat in ['csuccess', 'cfailed', 'killed']:

            data = {'queue'    : queue,
                    'stat'     : stat,
                    'njobs'    : 0,
                    'nusers'   : 0,
                    'rwtime'   : {'min':0,'max':0,'mean':0, 'std':0, 'hist':None},
                    'cwtime'   : {'min':0,'max':0,'mean':0, 'std':0, 'hist':None},
                    'cctime'   : {'min':0,'max':0,'mean':0, 'std':0, 'hist':None},
                    'twait'    : {'min':0,'max':0,'mean':0, 'std':0, 'hist':None},
                    'rmem'     : {'min':0,'max':0,'mean':0, 'std':0, 'hist':None},
                    'cmem'     : {'min':0,'max':0,'mean':0, 'std':0, 'hist':None},
                    'cvmem'    : {'min':0,'max':0,'mean':0, 'std':0, 'hist':None},
                    'eff_wtime': {'min':0,'max':0,'mean':0, 'std':0, 'median':0, 'bnd':[], 'var': 0, 'hist':None},
                    'eff_mem'  : {'min':0,'max':0,'mean':0, 'std':0, 'median':0, 'bnd':[], 'var': 0, 'hist':None},
                    'cpu_util' : {'min':0,'max':0,'mean':0, 'std':0, 'median':0, 'bnd':[], 'var': 0, 'hist':None}}

            summeas.append( data )

            my_jlist = __filter_jlist__( jlist, queue, stat )

            data['njobs'] = len(my_jlist)

            if data['njobs'] == 0:
                continue

            data['nusers'] = len( list(set(map(lambda x:x.uid, my_jlist))) )

            l_rwtime  = map(lambda x:x.rwtime , my_jlist)
            l_cwtime  = map(lambda x:x.cwtime , my_jlist)
            l_cctime  = map(lambda x:x.cctime , my_jlist)
            l_rmem    = map(lambda x:x.rmem   , my_jlist)
            l_cmem    = map(lambda x:x.cmem   , my_jlist)
            l_cvmem   = map(lambda x:x.cvmem  , my_jlist)
            l_t_queue = map(lambda x:x.t_queue, my_jlist)
            l_t_start = map(lambda x:x.t_start, my_jlist)

            l_twait     = [ a-b for a,b in zip(l_t_start, l_t_queue) ]
            l_eff_wtime = [ 1.0*a/b if b else 0 for a,b in zip(l_cwtime, l_rwtime) ]
            l_eff_mem   = [ 1.0*a/b if b else 0 for a,b in zip(l_cmem  , l_rmem  ) ]
            l_cpu_util  = [ 1.0*a/b if b else 0 for a,b in zip(l_cctime, l_cwtime) ]

            data['rwtime'] = {'min'  : min( l_rwtime ),
                              'max'  : max( l_rwtime ),
                              'mean' : mean(l_rwtime ),
                              'std'  : std( l_rwtime )}
                              #'hist' : histogram(l_rwtime, hbins_time)[0]}

            data['cwtime'] = {'min'  : min( l_cwtime ),
                              'max'  : max( l_cwtime ),
                              'mean' : mean(l_cwtime ),
                              'std'  : std( l_cwtime )}
                              #'hist' : histogram(l_cwtime, hbins_time)[0]}

            data['cctime'] = {'min'  : min( l_cctime ),
                              'max'  : max( l_cctime ),
                              'mean' : mean(l_cctime ),
                              'std'  : std( l_cctime )}
                              #'hist' : histogram(l_cctime, hbins_time)[0]}

            data['twait']  = {'min'  : min( l_twait ),
                              'max'  : max( l_twait ),
                              'mean' : mean(l_twait ),
                              'std'  : std( l_twait )}
                              #'hist' : histogram(l_twait, hbins_time)[0]}

            data['rmem'] = {'min'  : min( l_rmem ),
                            'max'  : max( l_rmem ),
                            'mean' : mean( l_rmem ),
                            'std'  : std( l_rmem )}
                            #'hist' : histogram(l_rmem, hbins_mem)[0]}

            data['cmem'] = {'min'  : min( l_cmem ),
                            'max'  : max( l_cmem ),
                            'mean' : mean(l_cmem ),
                            'std'  : std( l_cmem )}
                            #'hist' : histogram(l_cmem, hbins_mem)[0]}

            data['cvmem'] = {'min'  : min( l_cvmem ),
                             'max'  : max( l_cvmem ),
                             'mean' : mean(l_cvmem ),
                             'std'  : std( l_cvmem )}
                             #'hist' : histogram(l_cvmem, hbins_mem)[0]}

            data['eff_wtime'] = {'min'   : min( l_eff_wtime ),
                                 'max'   : max( l_eff_wtime ),
                                 'mean'  : mean(l_eff_wtime ),
                                 'std'   : std( l_eff_wtime ),
                                 'var'   : var( l_eff_wtime ),
                                 'median': median(l_eff_wtime ),
                                 'bnd'   : __get_2sigma__(histogram(l_eff_wtime, hbins_eff, density=True))}

            data['eff_mem']   = {'min'   : min( l_eff_mem ),
                                 'max'   : max( l_eff_mem ),
                                 'mean'  : mean(l_eff_mem ),
                                 'std'   : std( l_eff_mem ),
                                 'var'   : var( l_eff_mem ),
                                 'median': median(l_eff_mem ),
                                 'bnd'   : __get_2sigma__(histogram(l_eff_mem, hbins_eff, density=True))}

            data['cpu_util']  = {'min'   : min( l_cpu_util ),
                                 'max'   : max( l_cpu_util ),
                                 'mean'  : mean(l_cpu_util ),
                                 'std'   : std( l_cpu_util ),
                                 'var'   : var( l_cpu_util ),
                                 'median': median(l_cpu_util ),
                                 'bnd'   : __get_2sigma__(histogram(l_cpu_util, hbins_eff, density=True))}

    for d in summeas:
        logger.debug('==== %s:%s ====' % (d['queue'],d['stat']))
        logger.debug(' njobs: %d by %d users' % (d['njobs'],d['nusers']))
        logger.debug(' stat. rwtime: %s' % repr(d['rwtime']))
        logger.debug(' stat. cwtime: %s' % repr(d['cwtime']))
        logger.debug(' stat. cctime: %s' % repr(d['cctime']))
        logger.debug(' stat. twait : %s' % repr(d['twait']))
        logger.debug(' stat. rmem  : %s' % repr(d['rmem']))
        logger.debug(' stat. cmem  : %s' % repr(d['cmem']))
        logger.debug(' stat. cvmem : %s' % repr(d['cvmem']))
        logger.debug(' eff. wtime  : %s' % repr(d['eff_wtime']))
        logger.debug(' eff. mem    : %s' % repr(d['eff_mem']))
        logger.debug(' cpu util.   : %s' % repr(d['cpu_util']))
        logger.debug('')

    if args.monitor:
        __sqlite_job_stat__( summeas )
