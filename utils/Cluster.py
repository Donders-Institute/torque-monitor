#!/usr/bin/env python
import glob
import os
import pprint
import logging 
import re 
import math 
from Common import getMyLogger
from Shell import *

class Job:
    '''data object containing job information'''
    def __init__(self, **kwargs):
        self.jid = None
        self.__dict__.update(kwargs)

    def __str__(self):
        return pprint.pformat(self.__dict__)

    def __repr__(self):
        return repr(self.__dict__)

    def __eq__(self, other):
        if not isinstance(other, Job):
            raise NotImplementedError
        return self.jid == other.jid

class Node:
    '''data object containing node information'''
    def __init__(self, **kwargs):
        self.host = None 
        self.__dict__.update(kwargs)

    def __str__(self):
        return pprint.pformat(self.__dict__)

    def __repr__(self):
        return repr(self.__dict__)

    def __eq__(self, other):
        if not isinstance(other, Node):
            raise NotImplementedError
        return self.host == other.host

def interpret_job_ec(ec):
    '''interpret job exit code in the torque logfile into major catagories'''
    
    cstat = ''

    if ec == 0:
        cstat = 'csuccess'
    elif ec > 0 and ec < 128:
        cstat = 'cfailed'
    elif ec > 128 or ec < 0:
        cstat = 'killed'
    else:
        pass

    return cstat

def get_complete_jobs(logdir, date, debug=False):
    '''gets all completed jobs on the given date expressed in format of %Y%m%d (i.e. 20140130)'''

    def __convert_memory__(mymem):
        '''check if memory type is specified else default to mb'''

        gb_mem = None

        scale = {'b': 1024**3, 'kb': 1024**2, 'mb':1024, 'gb': 1 }

        re_mem = re.compile('^([0-9]*)([m,k,g]{0,1}b{0,1})$')

        m = re_mem.match(mymem)

        if m:
            size   = float( m.group(1) )
            unit   = m.group(2)
            if not unit:
                unit = 'b'

            gb_mem = size / scale[unit]

        return gb_mem

    def __readfix_xml__(myfile):
        '''read and parse the torque log (XML) file'''
        import xmltodict
 
        # open xml file and do some fixing
        temp = open(myfile, 'r').read()
        # fix incorrect closing tag
        temp = temp.replace('JobId', 'Job_Id')
        # fix the fact that there is no overarching beginning and end tag.
        temp = '<data>\n' + temp + '\n</data>'
 
        # read xml string
        xmldoc = xmltodict.parse(temp)
 
        # return list containing subdict for each job
        cjobs = xmldoc['data']['Jobinfo']
 
        # if there is only one JobInfo block, the data is not put into a list
        # to make it consistent, we put data into a list
        if not isinstance( cjobs, list ):
            cjobs = [ cjobs ]

        return cjobs

    ## get list of XML files corresponding to the jobs from the given date 
    xmlfiles = glob.glob( os.path.join(logdir, date) + '*' )

    jlist = []

    logger = getMyLogger(os.path.basename(__file__))

    if debug:
        logger.setLevel(logging.DEBUG)

    for f in xmlfiles:

        logger.debug('parsing logfile: %s' % f)

        cjobs = __readfix_xml__(f)

        for j in cjobs:

            o = Job( jid      = j['Job_Id'],                  # torque job id
                     jname    = None,                         # torque job name
                     jstat    = None,                         # torque job status
                     jec      = None,                         # job exit code
                     cstat    = 'unknown',                    # category status interpreted from jec 
                     uid      = None,                         # job owner
                     gid      = None,                         # job owner's group id
                     queue    = None,                         # job queue
                     rmem     = 0,                            # requested memory in byte 
                     rwtime   = 0,                            # requested wall-clock time in second
                     htypes   = None,                         # the Job's Hold_Types 
                     jpath    = None,                         # the Job's Join_Path 
                     cmem     = None,                         # consumed physical memory in byte
                     cvmem    = None,                         # consumed virtual memory in byte
                     cwtime   = None,                         # consumed wall-clock time in second
                     cctime   = None,                         # consumed CPU time in second
                     node     = None,                         # compute node host
                     t_submit = None,                         # timestamp for job being submitted to Torque
                     t_queue  = None,                         # timestamp for job being scheduled in the queue 
                     t_start  = None,                         # timestamp for job being started on execution node 
                     t_finish = None                          # timestamp for job being completed 
                   )
           
            ## handles the retried jobs (seperate entry in log file with same job id)
            is_newjob = True
            try:
                o = jlist[ jlist.index(o) ]
                is_newjob = False
                logger.warning('job already presented in list: %s' % o.jid)
            except:
                pass
 
            ## attributes may not be available 
            ## - resource requirement
            try:
                o.jname  = j['Job_Name']
            except KeyError,e:
                logger.warning('cannot find "Job_Name" for job %s' % o.jid)

            ## - resource requirement
            try:
                o.rmem   = __convert_memory__( j['Resource_List']['mem'] )
                o.rwtime = int( j['Resource_List']['walltime'] )
            except KeyError,e:
                logger.warning('cannot find "Resource_List" for job %s' % o.jid)
            except TypeError,e:
                logger.warning('empty "Resource_List" for job %s' % o.jid)
            
            ## - resource consumption
            try:
                o.cmem   = __convert_memory__( j['resources_used']['mem']  )
                o.cvmem  = __convert_memory__( j['resources_used']['vmem'] )
                o.cwtime = int( j['resources_used']['walltime'] )
                o.cctime = int( j['resources_used']['cput'] )

                if o.cctime > o.cwtime:
                    logger.warning('Job %s: CPU time consumption (%d) > wallclock time consumption (%d)' % (o.jid, o.cctime, o.cwtime))

            except KeyError,e:
                logger.warning('cannot find "resources_used" for job %s' % o.jid)

            ## - job exit status 
            try:
                o.jec   = int( j['exit_status'] )
                o.cstat = interpret_job_ec( o.jec ) 
            except KeyError,e:
                logger.warning('cannot find "exit_status" for job %s' % o.jid)

            ## - job execution host 
            try:
                o.node = j['exec_host']
            except KeyError,e:
                logger.warning('cannot find "exec_host" for job %s' % o.jid)

            ## - job state 
            try:
                o.jstat = j['job_state']
            except KeyError,e:
                logger.warning('cannot find "job_state" for job %s' % o.jid)

            ## - job owner
            try:
               o.uid = j['Job_Owner'].split('@')[0]
            except KeyError,e:
                logger.warning('cannot find "Job_Owner" for job %s' % o.jid)

            ## - job owner's group
            try:
               o.gid = j['egroup']
            except KeyError,e:
                logger.warning('cannot find "egroup" for job %s' % o.jid)

            ## - job queue 
            try:
               o.queue = j['queue']
            except KeyError,e:
                logger.warning('cannot find "queue" for job %s' % o.jid)

            ## - job Hold_Types 
            try:
               o.htypes = j['Hold_Types']
            except KeyError,e:
                logger.warning('cannot find "Hold_Types" for job %s' % o.jid)

            ## - job Join_Path
            try:
               o.jpath = j['Join_Path']
            except KeyError,e:
                logger.warning('cannot find "Join_Path" for job %s' % o.jid)

            ## - job submission(creation?) time 
            try:
               o.t_submit = int(j['ctime'])
            except KeyError,e:
                logger.warning('cannot find "ctime" for job %s' % o.jid)
 
            ## - job queue time
            try:
               o.t_queue  = int(j['qtime'])
            except KeyError,e:
                logger.warning('cannot find "qtime" for job %s' % o.jid)

            ## - job start time 
            try:
                o.t_start = int( j['start_time'] )
            except KeyError,e:
                logger.warning('cannot find "start_time" for job %s' % o.jid)

            ## - job complete time 
            try:
                o.t_finish = int( j['comp_time'] )
            except KeyError,e:
                logger.warning('cannot find "comp_time" for job %s' % o.jid)

            if is_newjob:
                jlist.append( o )

    return jlist

def get_mentat_node_properties(debug=False):
    '''get memtat node properties (memory, ncores, network, no. active VNC sessions)'''
    logger = getMyLogger(os.path.basename(__file__))

    if debug:
        logger.setLevel(logging.DEBUG)

    conv_mem_gb = { 'kB': 1024**2, 'mB': 1024 }

    s = Shell(debug=False)

    ## get memtat node properties 
    ##  - node name
    ##  - number of cores
    ##  - total memory
    ##  - number of VNC sessions
    ##  - top 5 processes according to CPU utilization 
    cmd = 'cluster-ssh -m "grep processor /proc/cpuinfo | wc -l | xargs echo \'ncores: \'; grep MemTotal /proc/meminfo; ps aux | grep Xvnc | grep -v grep | wc -l | xargs echo \'VNC sessions: \'; cat /proc/loadavg | xargs echo \'Load average: \'; ps -eo pcpu,pmem,pid,user,etime,args | grep -v \'ps -eo pcpu,pmem,pid,user,etime,args\' | sort -n -k 1 -r | grep -v \'%CPU\' | head -5"'
    rc, output, m = s.cmd1(cmd, allowed_exit=[0,255], timeout=300)

    re_node_name = re.compile('^\-*\s+(\S+)\s+\-*$')
    re_ncores    = re.compile('^ncores:\s+(\d+)$')
    re_memory    = re.compile('^MemTotal:\s+((\d+)\s+(\S+))$')
    re_nxvnc     = re.compile('^VNC sessions:\s+(\d+)$')
    re_loadavg   = re.compile('^Load average:\s+([\d|\.]+)\s+([\d|\.]+)\s+([\d|\.]+)\s+([\d|/]+)\s+.*$')
    re_top_ps    = re.compile('^[\d|\.]+\s+[\d|\.]+\s+[\d]+.*$')
    nodes = []

    if rc not in [0,255]:
        logger.error('command \'%s\' return non-exit code: %d' % (cmd, rc))
    else:
        for l in output.split('\n'):

            logger.debug(l)

            l = l.strip()

            m = re_node_name.match(l)
            if m:
                n = Node(host     = m.group(1),   ## hostname
                         ncores   = 0,            ## number of CPU cores
                         mem      = 0.,           ## total physical memory
                         nxvnc    = 0,            ## number of Xvnc session
                         load_1m  = 0,            ## 1 min. load average 
                         load_5m  = 0,            ## 5 min. load average 
                         load_10m = 0,            ## 10 min. load average 
                         total_ps = 0,            ## total processes 
                         top_ps   = [])           ## top processes

                nodes.append( n )
                continue

            m = re_ncores.match(l)
            if m:
                nodes[-1].ncores = int( m.group(1) )
                continue

            m = re_memory.match(l)
            if m:
                nodes[-1].mem = math.ceil( float( m.group(2) ) / conv_mem_gb[ m.group(3) ] )
                continue

            m = re_nxvnc.match(l)
            if m:
                nodes[-1].nxvnc = int( m.group(1) )
                continue

            m = re_loadavg.match(l)
            if m:
                nodes[-1].load_1m  = float( m.group(1) ) 
                nodes[-1].load_5m  = float( m.group(2) ) 
                nodes[-1].load_10m = float( m.group(3) ) 
                nodes[-1].total_ps = int( m.group(4).split('/')[1] )
                continue

            m = re_top_ps.match(l)
            if m:
                nodes[-1].top_ps.append(l)
                continue

    return nodes

def get_cluster_node_properties(debug=False):
    '''parse pbsnodes -a to get node properties'''

    logger = getMyLogger(os.path.basename(__file__))

    if debug:
        logger.setLevel(logging.DEBUG)

    s = Shell(debug=False)

    ## get scale factor for node speed
    cmd = 'cluster-torqueconfig | grep NODECFG | grep SPEED | awk "{print $1 $2}"'
    rc, output, m = s.cmd1(cmd, allowed_exit=[0,255], timeout=300)

    speeds = {}
    re_node_speed = re.compile('^NODECFG\[(.*)\]\s+SPEED=(\S+)$')
    if rc == 0:
        for l in output.split('\n'):
            l = l.strip()
            m = re_node_speed.match(l)
            if m:
                speeds[m.group(1)] = float(m.group(2))

    ## get node information 
    nodes = []

    cmd = 'pbsnodes -a'
    s = Shell(debug=False)
    rc, output, m = s.cmd1(cmd, allowed_exit=[0,255], timeout=300)

    if rc != 0:
        logger.error('command %s return non-exit code: %d' % (cmd, rc))
    else:
        re_host = re.compile('^(\S+)$')
        re_stat = re.compile('^\s+state\s+\=\s+(\S+)$')
        re_np   = re.compile('^\s+np\s+\=\s+(\d+)$')
        re_prop = re.compile('^\s+properties\s+\=\s+(\S+)$')
        re_mem  = re.compile('^ram(\d+)gb$')
        re_net  = re.compile('^network(\S+)$')

        output = output.split('\n')

        n = None
        for l in output:

            l = l.rstrip()

            m = re_host.match(l)
            if m:
                n = Node(host          = m.group(1),  # hostname
                         stat          = 'free',      # state
                         ncores        = 1,           # ncores
                         ncores_idle   = 1,           # ncores idling
                         ncores_inter  = 0,           # ncores running interactive jobs 
                         ncores_matlab = 0,           # ncores running batch-mode matlab jobs 
                         ncores_vgl    = 0,           # ncores running vgl jobs
                         ncores_batch  = 0,           # ncores running batch jobs
                         cpu_type      = '',          # CPU type
                         cpu_speed     = 1.0,         # CPU speed scale 
                         mem           = 1,           # memory total
                         memleft       = 1,           # memory left
                         memleft_c     = 1,           # avg. memory left per core
                         net           = '',          # network connectivity
                         interactive   = False,       # node allowing interactive jobs 
                         matlab        = False,       # node allowing matlab batch jobs 
                         vgl           = False,       # node allowing VirtualGL jobs 
                         batch         = False,       # node allowing batch jobs 
                         props         = [])          # other queue properties
                continue
 
            m = re_stat.match(l)
            if m:
                n.stat = m.group(1)
                continue
 
            m = re_np.match(l)
            if m:
                n.ncores      = int(m.group(1))
                n.ncores_idle = n.ncores 
                continue
 
            m = re_prop.match(l)
            if m:
                data = m.group(1).split(',')

                ## TODO: find a better way to get CPU type
                n.cpu_type = ' '.join(data[0:2])

                ## try to get the CPU speed factor if available
                try:
                    n.cpu_speed = speeds[n.host]
                except KeyError, e:
                    pass

                for d in data[2:]:
                    mm = re_mem.match(d)
                    if mm:
                        n.mem     = int( mm.group(1) )
                        n.memleft = n.mem
                        continue
                    mm = re_net.match(d)
                    if mm:
                        n.net = mm.group(1)
                        continue

                    n.props.append(d)

                ## update job type support according to node properties
                n.interactive = 'interactive' in n.props
                n.matlab      = 'matlab'      in n.props
                n.vgl         = 'vgl'         in n.props
                n.batch       = 'batch'       in n.props

                continue

            if l == '':
                if n not in nodes: ## avoid duplicat node entry
                    n.memleft_c = float( n.mem / n.ncores )
                    nodes.append( n )
                continue

#        for n in nodes:
#            logger.debug(repr(n)) 

    return nodes

def get_fs(s_cmd, debug=False):
    '''run cluster-faireshare to get current fairshare per USER/GROUP/CLASS/QoS'''

    logger = getMyLogger(os.path.basename(__file__))

    if debug:
        logger.setLevel(logging.DEBUG)

    fs = {}

    s = Shell(debug=False)
    rc, output, m = s.cmd1(s_cmd, allowed_exit=[0,255], timeout=300)

    if rc != 0:
        logger.error('command %s return non-exit code: %d' % (cmd, rc))
    else:
        re_user_beg  = re.compile('^USER$')
        re_group_beg = re.compile('^GROUP$')
        re_class_beg = re.compile('^CLASS$')
 
        idx = {'user':[],'group':[],'class':[]}
        k   = None
        output = output.split('\n')
        for i in xrange(len(output)):
 
            if re_user_beg.match(output[i].strip()):
                k = 'user'
                idx[k].append(i+2)
                continue
 
            if re_group_beg.match(output[i].strip()):
                k = 'group'
                idx[k].append(i+2)
                continue
 
            if re_group_beg.match(output[i].strip()):
                k = 'class'
                idx[k].append(i+2)
                continue
 
            if k and output[i].strip() == '':
                idx[k].append(i)
                k = None 
                continue
 
        logger.debug('line indices on %s output for USER/GROUP/CLASS' % s_cmd )
        logger.debug(' |_ %s ' % repr(idx))

        re_skip = re.compile('^DEFAULT')
        for k,v in idx.iteritems():
            fs[k] = {}
            if v:
                for line in output[v[0]:v[1]]:
                    data  = line.strip().split()
                    if not re_skip.match( data[0] ):
                        ## remove the '*' at the tail of userid
                        fs[k][re.sub('\*$','',data[0])] = float(data[1])
    return fs

def get_qstat_jobs(s_cmd, node_domain_suffix='dccn.nl', debug=False):
    '''run cluster-qstat to get all job status and convert the output into job info dictionary'''

    def __proc_walltime__(mytime):
        '''convert walltime to sum of minutes'''
        minutes = int(mytime.split(':')[0]) * 60 + int(mytime.split(':')[1])
        return minutes

    logger = getMyLogger(os.path.basename(__file__))
    if debug:
        logger.setLevel(logging.DEBUG)

    jlist = {}

    s = Shell(debug=False)
    rc, output, m = s.cmd1(s_cmd, allowed_exit=[0,255], timeout=300)

    re_jinfo  = re.compile ( '^(\S+)\s+'                  +    # job id
                             '(\w+)\s+'                   +    # user id
                             '(\w+)\s+'                   +    # queue name
                             '(\S+)\s+'                   +    # job name
                             '([-,\d]+)\s+'               +    # session id
                             '([-,\d]+)\s+'               +    # NDS
                             '([-,\d,\S+]+)\s+'           +    # TSK
                             '(\d+)gb\s+'                 +    # requested physical memory 
                             '(\d+:\d+:\d+)\s+'           +    # requested wall-clock time
                             '([A-Z]+)\s+'                +    # job status (expecting Q,R,C,H,E only)
                             '(\d+:\d+:\d+|-{2,})\s+'     +    # wall-clock time consumption
                             '(.*)$' )                         # computer node and session
                             #'((((dccn-c\d+)/(\d+\+?))|-{2,}){1,})$' )   # computer node and session

    if rc != 0:
        logger.error('command %s return non-exit code: %d' % (s_cmd, rc))
    else:

        def __apply_domain_suffix__(node_hostname):
            host = node_hostname.split('/')[0]
            if host != '--':
                host += '.' + node_domain_suffix
            return host 

        for l in output.split('\n'):
            l = l.strip()
            m = re_jinfo.match(l)

            if m:
                nodelist = ['']
                if m.group(12) != '==':
                    nodelist = map( lambda x:__apply_domain_suffix__(x), m.group(12).split('+'))

                j = Job( jid   = m.group(1)               ,
                         uid   = m.group(2)               ,
                         queue = m.group(3)               ,
                         jname = m.group(4)               ,
                         sid   = m.group(5)               ,
                         nds   = m.group(6)               ,
                         tsk   = m.group(7)               ,
                         rmem  = int(m.group(8))          ,
                         rtime = __proc_walltime__(m.group(9)),
                         jstat = m.group(10)              ,
                         ctime = m.group(11)              ,
                         node  = nodelist                 )

                if j.jstat not in jlist.keys():
                    jlist[j.jstat] = []

                jlist[j.jstat].append(j)

    return jlist
