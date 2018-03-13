from prometheus_client import CollectorRegistry
from prometheus_client import Gauge, Counter, Summary, Histogram
from prometheus_client.core import GaugeMetricFamily
from prometheus_client import write_to_textfile, push_to_gateway
from prometheus_client.exposition import basic_auth_handler

import potsdb
import datetime

from utils.Cluster import *
from utils.Common  import *

class MetricData:
    """data object for metric data"""
    
    def __init__(self, tags, value):
        self.value = value
        self.tags = tags
        
    def __str__(self):
        return pprint.pformat(self.__dict__)

    def __repr__(self):
        return repr(self.__dict__)

    def __eq__(self, other):
        if not isinstance(other, MetricData):
            raise NotImplementedError
        return self.tags == other.tags  

class ClusterAccounting:
    """metrics collector for cluster utilisation accounting"""
    def __init__(self, config):
        
        self.logger = getMyLogger(self.__class__.__name__)
    
        ## load config file and global settings
        c = getConfig(config)
        self.TORQUE_LOG_DIR      = c.get('TorqueTracker','TORQUE_LOG_DIR')
        self.BIN_QSTAT_ALL       = c.get('TorqueTracker','BIN_QSTAT_ALL')
        self.BIN_FSHARE_ALL      = c.get('TorqueTracker','BIN_FSHARE_ALL')
        self.TORQUE_BATCH_QUEUES = c.get('TorqueTracker','TORQUE_BATCH_QUEUES').split(',')
        
        ## The registry contains metrics data in the following format
        ##
        ## key: metric name
        ## value: [ MetricData_1, MetricData_2, ... ]
        self.registry = {'hpc_wtime_asked': [],
                         'hpc_wtime_used' : [],
                         'hpc_mem_asked'  : [],
                         'hpc_mem_used'   : [],
                         'hpc_ctime_used' : []}
    
    def exportToFile(self, fpath):
        """export metrics in the registry to a file"""
        f = open(fpath, 'w')
        for m,d in self.registry:
            for x in d:
                f.write("%s %s %s\n" % (m, repr(x.tags), x.value))
        f.close()
        return

    def pushToGateway(self, host, **kwargs):
        port=4242
        qsize=100000
        host_tag=True
        mps=0
        check_host=True

        for k in kwargs.keys():
            if k == 'port':
                port = int(kwargs[k])
                continue
            if k == 'qsize':
                qsize = int(kwargs[k])
                continue
            if k == 'host_tag':
                host_tab = bool(kwargs[k])
                continue
            if k == 'mps':
                mps = int(kwargs[k])
                continue
            if k == 'check_host':
                check_host = bool(kwargs[k])
                continue

        metrics = potsdb.Client(host, port=port, qsize=qsize, host_tag=host_tag, mps=mps, check_host=check_host)
        
        # send data to openTSDB
        for m,d in self.registry.iteritems():
            for x in d:
                metrics.send(m, x.value, x.tags)

        # wait until data are being sent out
        metrics.wait()

    def collectMetrics(self, date=None):
        """collection metrics"""

        if not date:
            date = datetime.date.today() - datetime.timedelta(1)).strftime('%Y%m%d')

        self.logger.info('collecting data of jobs submitted on %s' % date)
        jobs = get_complete_jobs(self.TORQUE_LOG_DIR, date, debug=False)
        
        for j in jobs:
            t = [j.gid, j.uid, j.jstat, j.queue]
            
            # construct data points for this job
            data = {'hpc_wtime_asked': MetricData(tags=t, value=j.rwtime),
                    'hpc_mem_asked'  : MetricData(tags=t, value=j.rmem),
                    'hpc_wtime_used' : MetricData(tags=t, value=j.cwtime),
                    'hpc_mem_used'   : MetricData(tags=t, value=j.cmem),
                    'hpc_ctime_used' : MetricData(tags=t, value=j.cctime)}
            
            # update registry
            for m,d in data.iteritems():
                try:
                    idx = self.registry[m].index(d)
                    self.registry[m][idx].value += d.value
                except ValueError:
                    self.registry[m].append(d)

class ClusterStatistics:
    """metrics collector for cluster statistics"""
    def __init__(self, config):
        
        self.logger = getMyLogger(self.__class__.__name__)
        
        ## load config file and global settings
        c = getConfig(config)
        self.BIN_QSTAT_ALL       = c.get('TorqueTracker','BIN_QSTAT_ALL')
        self.BIN_FSHARE_ALL      = c.get('TorqueTracker','BIN_FSHARE_ALL')
        self.TORQUE_BATCH_QUEUES = c.get('TorqueTracker','TORQUE_BATCH_QUEUES').split(',')
        self.registry = CollectorRegistry()
    
    def exportToFile(self, fpath):
        """export metrics in the registry to a file"""
        write_to_textfile(fpath, self.registry)
        return

    def pushToGateway(self, endpoint, **kwargs):
        """push metrics in the registry to the prometheus gateway"""

        job = kwargs['job']
        instance = None
        try:
            instance = kwargs['instance']
        except:
            pass

        push_to_gateway(endpoint, job=job, instance=instance, registry=self.registry)
        return
    
    def collectMetrics(self):
        
        # metrics for core utilisation
        g_core_usage = Gauge('hpc_core_usage', 'number of used cores per node per queue', ['host', 'queue'], registry=self.registry)
        g_core_total = Gauge('hpc_core_total', 'number of total cores per node', ['host'], registry=self.registry)
        
        # metrics for memory utilisation
        g_mem_usage  = Gauge('hpc_mem_usage' , 'bytes of used memory per node per queue', ['host', 'queue'], registry=self.registry)
        g_mem_total  = Gauge('hpc_mem_total' , 'bytes of total memory per node', ['host'], registry=self.registry)
        
        # metrics for job count per queue, per state
        g_job_count  = Gauge('hpc_job_count' , 'number of jobs' , ['queue','status'], registry=self.registry)

        jobs = get_qstat_jobs(s_cmd=self.BIN_QSTAT_ALL)
        nodes = get_cluster_node_properties()

        # TODO: make it configurable
        q_cat = ['matlab','batch','vgl','interact','other']

        def _qcat(queue):
            cat = queue
            if cat in self.TORQUE_BATCH_QUEUES:
                cat = 'batch'
            elif q not in q_cat:
                cat = 'other'
            return cat 
 
        # static node information
        for n in nodes:
            g_core_total.labels(host=n.host).set( n.ncores )
            g_mem_total.labels(host=n.host).set( n.mem * 1000000000 )

            # set default usage metrics to zero
            for q in q_cat:
                g_core_usage.labels(queue=q, host=n.host).set( 0 )
                g_mem_usage.labels(queue=q , host=n.host).set( 0 )

        #   the job state (according to qstat manual):
        #     C  - Job is completed after having run
        #     E  - Job is exiting after having run.
        #     H  - Job is held.
        #     Q  - Job is queued, eligible to run or routed.
        #     R  - Job is running.
        #     T  - Job is being moved to new location.
        #     W  - Job is waiting for its execution time (-a option) to be reached.
        #     S  - (Unicos only) job is suspended.        
        
        ## get jobs in queue or waiting state
        _jobs = []
        map(_jobs.extend, jobs.values())

        q_jobs = filter(lambda j:j.jstat in ['Q','S'], _jobs)
        h_jobs = filter(lambda j:j.jstat in ['H'], _jobs)

        # set default job count metrics to zero
        for q in q_cat:
            for s in ['queued','held','running']:
                g_job_count.labels(queue=q, status=s).set(0)
             
        for j in q_jobs:
            g_job_count.labels(queue=_qcat(j.queue), status='queued').inc(1)
            
        for j in h_jobs:
            g_job_count.labels(queue=_qcat(j.queue), status='held').inc(1)

        ## get jobs in running state
        r_jobs = filter(lambda j:j.jstat in ['E','R'], _jobs)
        for j in r_jobs:
            g_job_count.labels(queue=_qcat(j.queue), status='running').inc(1)
            
            # update core and memory usage
            for n in j.node:
                g_core_usage.labels(queue=_qcat(j.queue), host=n).inc(1)
                g_mem_usage.labels(queue=_qcat(j.queue), host=n).inc(j.rmem * 1000000000)
                
        return
        
def testClusterMetrics(config):
    """Test function for MetricsRegistry"""
    m = ClusterStatistics(config)
    # collection metrics
    m.collectMetrics()
    # write out metrics to file
    m.exportToFile('statistics.prom')
    
    m = ClusterAccount(config)
    m.collectMetrics()
    m.exportToFile('accounting.txt')