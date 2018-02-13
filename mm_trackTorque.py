#!/bin/env python

import subprocess
import datetime
import sys
import os
import glob
import tempfile
import pprint
import sqlite3 
from argparse import ArgumentParser

from numpy import mean
from numpy import histogram as hist
import cPickle as pickle

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from utils.Common  import *
from utils.Shell   import *
from utils.Cluster import *

sys.path.append(os.path.dirname(os.path.abspath(__file__))+'/external/lib/python')
from prettytable import PrettyTable

def __sqlite_cnode_status__(nodes):
    '''saving cluster node status into SQLite database'''

    conn    = sqlite3.connect(SQLite_DB_PATH)
    c       = conn.cursor()

    try:
        sql = '''CREATE TABLE rsrc (timestamp INTEGER, host TEXT, stat TEXT, cpu TEXT, net TEXT, ncores INTEGER, mem INTEGER, ncores_inter INTEGER, ncores_matlab INTEGER, ncores_vgl INTEGER, ncores_batch INTEGER, ncores_left INTEGER, mem_left INTEGER, is_interactive BOOLEAN, is_matlab BOOLEAN, is_vgl BOOLEAN, is_batch BOOLEAN, UNIQUE (timestamp,host))'''
        c.execute(sql)
    except sqlite3.OperationalError,e:
        logger.warning('SQL error: %s' % repr(e)) 

    sql  = '''INSERT INTO rsrc (timestamp,host,stat,cpu,net,ncores,mem,ncores_inter,ncores_matlab,ncores_vgl,ncores_batch,ncores_left,mem_left,is_interactive,is_matlab,is_vgl,is_batch) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
    data = []

    for n in nodes:
        data.append( (int(time.mktime(now.timetuple())), n.host, n.stat, n.cpu_type, n.net, n.ncores, n.mem, n.ncores_inter, n.ncores_matlab, n.ncores_vgl, n.ncores_batch, n.ncores_idle, n.memleft, n.interactive, n.matlab, n.vgl, n.batch ) )

    logger.debug(data)

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

def __sqlite_hnode_status__(nodes):
    '''saving head(mentat) node status into SQLite database'''

    conn    = sqlite3.connect(SQLite_DB_PATH)
    c       = conn.cursor()
    try:
        sql = '''CREATE TABLE hnode (timestamp INTEGER, host TEXT, ncores INTEGER, mem INTEGER, nxvnc INTEGER, load_1m REAL, load_5m REAL, load_10m REAL, total_ps INTEGER, top_ps TEXT, UNIQUE (timestamp,host))'''
        c.execute(sql)
    except sqlite3.OperationalError,e:
        logger.warning('SQL error: %s' % repr(e)) 

    sql  = '''INSERT INTO hnode (timestamp,host,ncores,mem,nxvnc,load_1m,load_5m,load_10m,total_ps,top_ps) VALUES (?,?,?,?,?,?,?,?,?,?)'''
    data = []

    for n in nodes:
        # it does not make sense if host's ncore is 0.
        # TODO: maybe mark the host with ncore=0 as 'down' 
        if n.ncores > 0:
            data.append( (int(time.mktime(now.timetuple())), n.host, n.ncores, n.mem, n.nxvnc, n.load_1m, n.load_5m, n.load_10m, n.total_ps, unicode('+|+'.join(n.top_ps), errors='ignore')) )

    logger.debug(data)

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

def __tab_cnode_status__(nodes):
    '''reporting current cluster node in table'''

    color_map = { 'blue'  : '\033[94m',
                  'yellow': '\033[93m',
                  'green' : '\033[92m',
                  'red'   : '\033[91m' }

    def __mkcolor__(value, color):
        if args.color:
             try:
                 value = color_map[color] + value + '\033[0m'
             except Exception,e:
                 pass
        return value
               
    t = PrettyTable()

    ## TODO: make the filed description/key mapping more flexible
    t.field_names = ['node','cpu (speedX)','net','ncores (t/u/f)','memory (t/u/f)','interact','matlab','vgl','batch']

#    t.align['node']           = 'l'
#    t.align['cpu (speedX)']   = 'c'
#    t.align['net']            = 'c'
#    t.align['ncores (t/u/l)'] = 'c'
#    t.align['memory (t/u/l)'] = 'c'
#    t.align['interact']       = 'c'
#    t.align['matlab']         = 'c'
#    t.align['vgl']            = 'c'
#    t.align['batch']          = 'c'

    ## sort nodes
    if args.sortby == 'net':
        nodes = sorted(nodes, key=lambda x:x.net, reverse = False)
    elif args.sortby == 'tcores':
        nodes = sorted(nodes, key=lambda x:x.ncores, reverse = True) 
    elif args.sortby == 'tmem':
        nodes = sorted(nodes, key=lambda x:x.mem, reverse = True) 
    elif args.sortby == 'lcores':
        nodes = sorted(nodes, key=lambda x:x.ncores_idle, reverse = True) 
    elif args.sortby == 'lmem':
        nodes = sorted(nodes, key=lambda x:x.memleft, reverse = True) 
    elif args.sortby == 'ucores':
        nodes = sorted(nodes, key=lambda x:x.ncores - x.ncores_idle, reverse = True) 
    elif args.sortby == 'umem':
        nodes = sorted(nodes, key=lambda x:x.mem - x.memleft, reverse = True)
    elif args.sortby == 'matlab':
        nodes = sorted(nodes, key=lambda x:str('matlab' in x.props) + '%03d' % x.ncores_matlab, reverse = True)
    elif args.sortby == 'vgl':
        nodes = sorted(nodes, key=lambda x:str('vgl' in x.props)    + '%03d' % x.ncores_vgl, reverse = True)
    elif args.sortby == 'batch':
        nodes = sorted(nodes, key=lambda x:str('batch' in x.props)  + '%03d' % x.ncores_batch, reverse = True)
    elif args.sortby == 'interact':
        nodes = sorted(nodes, key=lambda x:str(x.interactive)+'%03d' % x.ncores_inter, reverse = True) 
    else:
        nodes = sorted(nodes, key=lambda x:x.host, reverse = False) 

    c_bool = {'T': 'green', 'F': 'red'}
    c_stat = {'free': 'green', 'job-exclusive': 'yellow', 'down': 'red', 'offline': 'black'}
    for n in nodes:

        data = []
        data.append( '%s' % __mkcolor__( n.host, c_stat[ n.stat ] ) )
        data.append( '%-15s (%3.2f)'  % (n.cpu_type,n.cpu_speed) )
        data.append( '%2d Gb'      % int(n.net.replace('GigE','')) )

        data.append( __mkcolor__('%3d' % n.ncores                ,'blue' ) + '/' +
                     __mkcolor__('%3d' % (n.ncores-n.ncores_idle),'red'  ) + '/' +
                     __mkcolor__('%3d' % n.ncores_idle           ,'green') )

        data.append( __mkcolor__('%4d' % n.mem            ,'blue' ) + '/' +
                     __mkcolor__('%4d' % (n.mem-n.memleft),'red'  ) + '/' +
                     __mkcolor__('%4d' % n.memleft        ,'green') )

        data.append( '%s (%3d)' % (__mkcolor__(str(n.interactive)[0], c_bool[str(n.interactive)[0]]), n.ncores_inter ) )
        data.append( '%s (%3d)' % (__mkcolor__(str(n.matlab)[0]     , c_bool[str(n.matlab)[0]])     , n.ncores_matlab) )
        data.append( '%s (%3d)' % (__mkcolor__(str(n.vgl)[0]        , c_bool[str(n.vgl)[0]])        , n.ncores_vgl   ) )
        data.append( '%s (%3d)' % (__mkcolor__(str(n.batch)[0]      , c_bool[str(n.batch)[0]])      , n.ncores_batch ) )

        t.add_row( data )

    print t

def __tab_hnode_status__(nodes):
    '''reporting current head(mentat) node in table'''
    t = PrettyTable()

    ## TODO: make the filed description/key mapping more flexible
    t.field_names = ['node','ncores','memory','Xvnc sessions','10min. load','total procs.']

    for n in nodes:
        # it does not make sense if host's ncore is 0.
        # TODO: maybe mark the host with ncore=0 as 'down' 
        if n.ncores > 0:
            t.add_row( [n.host, n.ncores, n.mem, n.nxvnc, n.load_10m, n.total_ps] )

    print t

def report_node_status(hnodes, cnodes):
    '''report resource availability'''
    if args.rpt_rsrc:
        __tab_cnode_status__(cnodes)
        __tab_hnode_status__(hnodes)

    if args.monitor:

        # sending notification emails
        if args.sendmail:
            __sendmail_cnode_down__(map(lambda x: x.host, filter(lambda x: x.stat == 'down', cnodes)))

        __sqlite_cnode_status__(cnodes)
        __sqlite_hnode_status__(hnodes)


def __sendmail_cnode_down__(cnodes):
    """
    sends notification emails concerning the nodes are in status 'down'
    :param cnodes: the nodes that are down
    :return:
    """

    # retrieve those nodes already down in previous iteration
    conn = sqlite3.connect(SQLite_DB_PATH)
    c = conn.cursor()
    qry = 'SELECT host FROM rsrc WHERE stat = \'down\' AND timestamp in (SELECT max(timestamp) from rsrc)'

    cnodes_prev = []
    try:
        for r in c.execute(qry):
             cnodes_prev.append(r[0])
    except sqlite3.OperationalError,e:
         logger.warning('SQL error: %s' % repr(e))

    try:
        if conn:
            conn.close()
    except:
        pass

    # filter out nodes that are already down in previous iteration
    cnodes_notify = list(set(cnodes) - set(cnodes_prev))

    if len(cnodes_notify) > 0:
        subject = '[Torquemon] compute nodes DOWN!!'
        msg = '\n'.join(cnodes_notify)
        sendEmailNotification('admin@dccn-l018.dccn.nl', NOTIFICATION_EMAILS, subject, msg)

def __sqlite_summeas__( summeas ):
    '''report statistical measurement into SQLite database'''
    conn    = sqlite3.connect(SQLite_DB_PATH)
    c       = conn.cursor()
    try:
        sql = 'CREATE TABLE summeas ('       
        sql+= 'timestamp         INTEGER,'      # timestamp
        sql+= 'cores_total       INTEGER,'      # no. total cores
        sql+= 'cores_idle        INTEGER,'      # no. idling cores
        sql+= 'memleft_pcore_u33 REAL,'         # mean of memory left per core over nodes with <= 32 GB memory
        sql+= 'memleft_pcore_o32 REAL,'         # mean of memory left per core over nodes with  > 32 GB memory
        sql+= 'memleft_pcore_all REAL,'         # mean of memory left per core over all nodes
        sql+= 'memleft_pcore_inter    REAL,'    # mean of memory left per core over nodes with interactive jobs
        sql+= 'memleft_pcore_noninter REAL,'    # mean of memory left per core over nodes with batch jobs
        sql+= 'memreq_all      REAL,'           # mean of memory requirement over all jobs
        sql+= 'memreq_inter    REAL,'           # mean of memory requirement over interactive jobs
        sql+= 'memreq_noninter REAL,'           # mean of memory requirement over batch jobs
        sql+= 'wtreq_all       INTEGER,'        # mean of wallclock time requirement over all jobs
        sql+= 'wtreq_inter     INTEGER,'        # mean of wallclock time requirement over interactive jobs
        sql+= 'wtreq_noninter  INTEGER,'        # mean of wallclock time requirement over batch jobs
        sql+= 'njobs_inter     INTEGER,'        # number of interactive jobs
        sql+= 'njobs_noninter  INTEGER,'        # number of batch jobs
        sql+= 'njobs_running   INTEGER,'        # number of running jobs
        sql+= 'njobs_queue     INTEGER,'        # number of queued jobs
        sql+= 'njobs_exiting   INTEGER)'        # number of exiting jobs

        c.execute(sql)

    except sqlite3.OperationalError,e:
        logger.warning('SQL error: %s' % repr(e)) 

    sql  = '''INSERT INTO summeas VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
    data = []

    data.append( (int(time.mktime(now.timetuple())),
                  summeas['total_cores'], summeas['idling_cores'],
                  summeas['memleft_per_core_u33'], summeas['memleft_per_core_o32'],
                  summeas['memleft_per_core_all'],
                  summeas['memleft_per_core_oninter'], summeas['memleft_per_core_notoninter'],
                  summeas['memall'], summeas['meminter'], summeas['memnoninter'],
                  summeas['wallall'], summeas['wallinter'], summeas['wallnoninter'],
                  summeas['n_inter_jobs'], summeas['n_noninter_jobs'],
                  summeas['n_running_jobs'], summeas['n_queued_jobs'], summeas['n_exiting_jobs']) )

    logger.debug(data)

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

def __tab_jstat__(jlist):
    '''reporting job status'''
    t = PrettyTable()
    t.field_names = ['status','# jobs']
    t.align['status'] = 'c'
    t.align['# jobs'] = 'r'

#   the job state (according to qstat manual):
#     C  - Job is completed after having run
#     E  - Job is exiting after having run.
#     H  - Job is held.
#     Q  - Job is queued, eligible to run or routed.
#     R  - Job is running.
#     T  - Job is being moved to new location.
#     W  - Job is waiting for its execution time (-a option) to be reached.
#     S  - (Unicos only) job is suspended.

    for s in ['R','Q','H','W','S','T','E','C']:
        try:
            t.add_row( [s, len(jlist[s])] ) 
        except KeyError, e:
            pass
    print t

def report_jstat(jlist):
    '''report job status'''

    if args.rpt_jstat:
        __tab_jstat__(jlist)

def __tab_jqueued__(jlist):
    '''reporting queued jobs'''
    fs = get_fs(s_cmd=BIN_FSHARE_ALL, debug=libDebug)

    t = PrettyTable()
    t.field_names = ['job id','req. mem','req. wall time','queue','user fairshare','user running jobs']
    t.align['job id']            = 'l'
    t.align['req. mem']          = 'r'
    t.align['req. wall time']    = 'r'
    t.align['queue']             = 'l'
    t.align['user running jobs'] = 'r'
    t.align['user fairshare']    = 'r'

    t.reversesort = False
    if args.sortby == 'rmem':
        t.sortby = 'req. mem'
    elif args.sortby == 'rwtime':
        t.sortby = 'req. wall time'
    elif args.sortby == 'queue':
        t.sortby = 'queue'
    elif args.sortby == 'fs':
        t.sortby = 'user fairshare'
    else:
        t.sortby = 'job id'

    qlist = []

    for s in ['Q','H']:
        try:
            qlist += jlist[s]
        except KeyError,e:
            pass

    for j in qlist:
        u_fs = None 
        try:
            u_fs = fs['user'][j.uid]
        except KeyError,e:
            u_fs = 0.
        u_running = len( filter(lambda x:x.uid == j.uid, jlist['R']) )

        t.add_row( [j.jid, j.rmem, j.rtime, j.queue, u_fs, u_running] )

    print t

def report_jqueued(jlist):
    '''reporting queued jobs'''
    if args.rpt_jqueued:
        __tab_jqueued__(jlist)

## execute the main program
if __name__ == "__main__":

    parg = ArgumentParser(description='script for collecting resource utilization of running jobs', version="0.1")

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
                      help    = 'store data in SQLite database for monitoring purpose.')

    parg.add_argument('-r', '--rpt_rsrc',
                      action  = 'store_true',
                      dest    = 'rpt_rsrc',
                      default = False,
                      help    = 'report current cluster resource availability')

    parg.add_argument('-j', '--rpt_jstat',
                      action  = 'store_true',
                      dest    = 'rpt_jstat',
                      default = False,
                      help    = 'account jobs in different status')

    parg.add_argument('-q', '--rpt_jqueued',
                      action  = 'store_true',
                      dest    = 'rpt_jqueued',
                      default = False,
                      help    = 'summarize queued jobs')

    parg.add_argument('-o', '--order',
                      action  = 'store',
                      dest    = 'sortby',
                      choices = ['node', 'net', 'tcores', 'tmem','ucores','umem','lcores','lmem','interact','matlab','batch','vgl','rmem','rwtime','queue','fs'],  ## choices work only with str
                      default = 'node',
                      help    = 'specify the key to sort the table of the resource availability (i.e. -r) and the table of queued jobs (i.e. -q)')

    parg.add_argument('-n', '--no-color',
                      action  = 'store_false',
                      dest    = 'color',
                      default = True,
                      help    = 'disable table coloring')

    parg.add_argument('-s', '--sendmail',
                      action  = 'store_true',
                      dest    = 'sendmail',
                      default = False,
                      help    = 'enable sending notification emails at certain circumstances')

    global args 
#    global TORQUE_LOG_DIR
    global BIN_QSTAT_ALL
    global BIN_FSHARE_ALL
    global DB_DATA_DIR
    global libDebug
    global logger
    global now
    global SQLite_DB_PATH
    global TORQUE_BATCH_QUEUES
    global NOTIFICATION_EMAILS

    args = parg.parse_args()

    ## load config file and global settings
    c = getConfig(args.fconfig)
#    TORQUE_LOG_DIR   = c.get('TorqueTracker','TORQUE_LOG_DIR') 
    BIN_QSTAT_ALL       = c.get('TorqueTracker','BIN_QSTAT_ALL')
    BIN_FSHARE_ALL      = c.get('TorqueTracker','BIN_FSHARE_ALL')
    DB_DATA_DIR         = c.get('TorqueTracker','DB_DATA_DIR')
    TORQUE_BATCH_QUEUES = c.get('TorqueTracker','TORQUE_BATCH_QUEUES').split(',')
    NOTIFICATION_EMAILS = c.get('TorqueTracker','NOTIFICATION_EMAILS').split(',')

    SQLite_DB_PATH   = os.path.join( DB_DATA_DIR, '%s_%d.db' % (os.path.basename(__file__).replace('.py',''), datetime.datetime.now().year) )

    ## load logger and set the verbosity level
    logger = getMyLogger(os.path.basename(__file__))
    logger.setLevel(logging.WARNING)
    vlv = int(args.verbose)
    libDebug = False
    if vlv < 0:
        logger.setLevel(logging.ERROR)
    elif vlv == 1:
        logger.setLevel(logging.INFO)
    elif vlv >= 2:
        logger.setLevel(logging.DEBUG)
        libDebug = True

    ## create directories whenever necessary
    if args.monitor:
        try:
            os.makedirs(DB_DATA_DIR)
        except Exception, e:
            logger.warning('creating directory error: %s' % repr(e))
            pass
 
        if not os.path.isdir(DB_DATA_DIR):
            logger.error('directory not found: %s' % DB_DATA_DIR)
            sys.exit(1) 

    ## get current time
    now = datetime.datetime.now()

    jlist = get_qstat_jobs(s_cmd=BIN_QSTAT_ALL, debug=libDebug)

    report_jstat(jlist)

    report_jqueued(jlist)

    # initialize with an empty array when there are no running jobs
    if 'R' not in jlist.keys():
        jlist['R'] = []
        #sys.exit()

    # get list of active nodes on which the jobs are running
    #jnodes = set(map(lambda x:x.node, jlist['R']))
    jnodes = set(sum(map(lambda x:x.node, jlist['R']),[]))

    # get list of nodes with interactive jobs
    #interactivenodes = set( map(lambda x:x.node, filter(lambda x:x.queue=='interact', jlist['R'])) )
    interactivenodes = set(sum( map(lambda x:x.node, filter(lambda x:x.queue=='interact', jlist['R'])),[] ))
  
    # get list of nodes available on the cluster 
    now    = datetime.datetime.now()
    cnodes = get_cluster_node_properties(debug=libDebug)
    hnodes = get_mentat_node_properties(debug=libDebug)

    # loop through running nodes
    for node in sorted(jnodes):

        # get the node info
        cnode = None
        try:
            cnode = cnodes[ cnodes.index( Node(host=node) ) ]
        except ValueError, e:
            logger.warning('node %s not found in the cluster' % node) 
            continue
 
        # collect blocked memory per node

        blockedproc = sum(map(lambda x:x.node.count(node), jlist['R']))

        # TODO: for MPI jobs, the memory allocation per core may need to be divided to number of cores
        #blockedmem  = sum(map(lambda x:x.rmem, filter(lambda x:node in x.node, jlist['R'])))
        blockedmem  = sum(map(lambda x:(x.rmem*x.node.count(node)/len(x.node)), filter(lambda x:node in x.node, jlist['R'])))

        cnode.ncores_idle   = cnode.ncores_idle - blockedproc
        cnode.ncores_inter  = len( filter(lambda x:x.queue=='interact'            and cnode.host in x.node, jlist['R'] ) )
        cnode.ncores_matlab = len( filter(lambda x:x.queue=='matlab'              and cnode.host in x.node, jlist['R'] ) )
        cnode.ncores_vgl    = len( filter(lambda x:x.queue=='vgl'                 and cnode.host in x.node, jlist['R'] ) )
        cnode.ncores_batch  = len( filter(lambda x:x.queue in TORQUE_BATCH_QUEUES and cnode.host in x.node, jlist['R'] ) )

        # update supported job type according to running job types on the node
        cnode.interactive = cnode.ncores_inter  > 0 or cnode.interactive
        cnode.matlab      = cnode.ncores_matlab > 0 or cnode.matlab
        cnode.vgl         = cnode.ncores_vgl    > 0 or cnode.vgl
        cnode.batch       = cnode.ncores_batch  > 0 or cnode.batch

        # TODO: cluster-qstat rounds memory reservation in GB
        #       it can cause the rememing memory smaller than 0
        cnode.memleft     = max([0, cnode.memleft-blockedmem])

        if cnode.ncores_idle == 0: 
            cnode.memleft_c = None
        else:
            cnode.memleft_c = float( cnode.memleft / cnode.ncores_idle )

    report_node_status(hnodes, cnodes)

    # collect summary measures
    summeas = dict()
    
    # measurement time
    summeas['poll_time'] = now.strftime("%Y%m%d %H:%M:%S")

    # number of total cores
    summeas['total_cores'] = sum(map(lambda x:x.ncores, cnodes))
    
    # number of idling cores
    summeas['idling_cores'] = sum(map(lambda x:x.ncores_idle, cnodes))

    # number of interactive jobs
    summeas['n_inter_jobs']    = len(filter(lambda x:x.queue=='interact', jlist['R']))
    summeas['n_noninter_jobs'] = len(filter(lambda x:x.queue!='interact', jlist['R'])) 
    
    # memory left per idling core on machines with 32GB or less
    summeas['memleft_per_core_u33'] = mean( map( lambda x:x.memleft_c,
                                                 filter(lambda x:x.mem < 33 and x.memleft_c != None, cnodes) ) )
    
    # memory left per idling core on machines with more than 32GB of RAM
    summeas['memleft_per_core_o32'] = mean( map( lambda x:x.memleft_c,
                                                 filter(lambda x:x.mem > 33 and x.memleft_c != None, cnodes) ) )
    
    # memory left per idling core on all machines
    summeas['memleft_per_core_all'] = mean( map( lambda x:x.memleft_c,
                                                 filter(lambda x:x.memleft_c != None, cnodes) ) )
    
    # memory left per idling core on nodes that do not run interactive jobs
    summeas['memleft_per_core_notoninter'] = mean( map( lambda x:x.memleft_c,
                                                   filter(lambda x:x.memleft_c != None and x.ncores_inter == 0, cnodes) ) )
    
    # memory left per idling core on nodes that do run interactive jobs
    if summeas['n_inter_jobs'] == 0:
        summeas['memleft_per_core_oninter'] = None
    else:
        summeas['memleft_per_core_oninter'] = mean( map( lambda x:x.memleft_c,
                                                    filter(lambda x:x.memleft_c != None and x.ncores_inter > 0, cnodes) ) )
    
    # determine requested memory and walltime
    if len(jlist['R']) == 0:
        summeas['memall']  = None
        summeas['wallall'] = None
    else:
        summeas['memall']  = mean(map(lambda x:x.rmem , jlist['R']))
        summeas['wallall'] = mean(map(lambda x:x.rtime, jlist['R']))

    # set walltime and memory to None if there are no jobs of that nature.
    # non-interactive jobs
    if summeas['n_noninter_jobs'] == 0:
        summeas['memnoninter'] = None
        summeas['wallnoninter'] = None
    else:
        summeas['memnoninter']  = mean(map(lambda x:x.rmem , filter(lambda x:x.queue!='interact', jlist['R'])))
        summeas['wallnoninter'] = mean(map(lambda x:x.rtime, filter(lambda x:x.queue!='interact', jlist['R'])))

    # interactive jobs
    if summeas['n_inter_jobs'] == 0:
        summeas['meminter'] = None
        summeas['wallinter'] = None
    else:
        summeas['meminter']  = mean(map(lambda x:x.rmem , filter(lambda x:x.queue=='interact', jlist['R'])))
        summeas['wallinter'] = mean(map(lambda x:x.rtime, filter(lambda x:x.queue=='interact', jlist['R'])))
    
#    # determine distribution of memory requirements
#    memhist = hist(map(lambda x:x.rmem, jlist['R']), range(0, 51, 1))
#
#    # write histogram to file
#    with open( os.path.join(DB_DATA_DIR,'memdist.txt'), 'a' ) as text:
#        text.write('\t'.join([str(x) for x in memhist[0]]) + '\n')
#    
#    # determine distribution of walltime requirements
#    # 48 hours * 60 = 2880 minutes
#    wallhist = hist(map(lambda x:x.rtime, jlist['R']), range(0, 2881, 100))
#
#    # write histogram to file
#    with open( os.path.join(DB_DATA_DIR, 'walldist.txt'), 'a' ) as text:
#        text.write('\t'.join([str(x) for x in wallhist[0]]) + '\n')
#    
#    # count which nodes are running interactive jobs
#    # this first part was only needed to generate the pickle file
#    # if not os.path.exists('/home/mrstats/maamen/DCCN/Scripts/Torque/interactive_job_count.p'):
#    # 	ijc = dict()
#    # 	for node in torqueinfo.keys():
#    # 		ijc[node] = 0
#    # 	pickle.dump(ijc, open('/home/mrstats/maamen/DCCN/Scripts/Torque/interactive_job_count.p','w'))
#    # count which nodes are running interactive jobs
#    if summeas['n_inter_jobs'] > 0:
#        ijc = {}
#        try:
#            f = open( os.path.join( DB_DATA_DIR,'interactive_job_count.p'), 'r' )
#            ijc = pickle.load(f)
#            f.close()
#        except Exception,e:
#            pass
#
#        for n in map( lambda x:x.host, cnodes ):
#            if n not in ijc.keys():
#                ijc[n] = 0
#            ijc[n] += len( filter(lambda x:x.queue=='interact' and x.node==n, jlist['R'] ) )
#
#        f = open( os.path.join( DB_DATA_DIR,'interactive_job_count.p'), 'w' )
#        pickle.dump(ijc, f)
#        f.close()

    ## counting running,exiting and queued jobs
    summeas['n_running_jobs'] = len(jlist['R'])
    summeas['n_exiting_jobs'] = 0
    try:
         summeas['n_exiting_jobs'] = len(jlist['E'])
    except KeyError,e:
        pass

    summeas['n_queued_jobs']  = 0
    for s in ['Q','H']:
        try:
            summeas['n_queued_jobs'] += len(jlist[s])
        except KeyError,e:
            pass

    if args.monitor:
        __sqlite_summeas__( summeas )

#    ## write current measurement into pickle file
#    data = {}
#    try:
#        f = open( os.path.join( DB_DATA_DIR,'mm_trackTorque_summeas.p'), 'r' )
#        data = pickle.load(f)
#        f.close()
#    except Exception,e:
#        pass
#   
#    data[now] = summeas
#
#    logger.debug( pprint.pformat(summeas,indent=4) )
#
#    f = open( os.path.join( DB_DATA_DIR,'mm_trackTorque_summeas.p'), 'w' )
#    pickle.dump(data,f)
#    f.close()
 
#    ## write to MySQL ##
#    variables = [('poll_time', 'varchar(20)'),
#                 ('idling_cores', 'int(4)'),
#                 ('memleft_per_core_u33', 'float(4,2)'), ('memleft_per_core_o32', 'float(4,2)'),
#                 ('memleft_per_core_all', 'float(4,2)'),
#                 ('memleft_per_core_oninter', 'float(4,2)'), ('memleft_per_core_notoninter', 'float(4,2)'),
#                 ('memall', 'float(4,2)'), ('memnoninter', 'float(4,2)'), ('meminter', 'float(4,2)'),
#                 ('wallall', 'int(4)'), ('wallnoninter', 'int(4)'), ('wallinter', 'int(4)'),
#                 ('n_inter_jobs', 'int(4)'), ('n_noninter_jobs', 'int(4)'),
#                 ('n_running_jobs', 'int(4)'), ('n_queued_jobs', 'int(4)')]
#    listvars = list(variables)
#    listvars = [' '.join(x) for x in listvars]
#    
#    # add columns to the database, if they don't exist
#    db = mm_connectToDb.connect('image_private')
#    c = db.cursor()
#    
#    # add variables to table
#    # add_variables(db, listvars, 'Maamen_Torque_Tracking', c, 'id')
#    
#    # add values to table
#    mystring = [variable + '="' + str(summeas[variable]) + '"' for variable, value in variables]
#    mystring = ','.join(mystring)
#    sql = """INSERT INTO Maamen_Torque_Tracking SET %s """ % (mystring)
#    #print sql
#    c.execute(sql)
#    #print sql
#    
#    # close database
#    db.commit()
#    db.close()
