from prometheus_client import CollectorRegistry
from prometheus_client import Gauge, Counter, Summary, Histogram
from prometheus_client.core import GaugeMetricFamily
from prometheus_client import write_to_textfile, push_to_gateway
from prometheus_client.exposition import basic_auth_handler

from utils.Cluster import *
from utils.Common  import *

class CollectorCluster:
    """metrics collector for cluster statistics"""
    def __init__(self, config):
        ## load config file and global settings
        c = getConfig(config)
        self.BIN_QSTAT_ALL       = c.get('TorqueTracker','BIN_QSTAT_ALL')
        self.BIN_FSHARE_ALL      = c.get('TorqueTracker','BIN_FSHARE_ALL')
        self.TORQUE_BATCH_QUEUES = c.get('TorqueTracker','TORQUE_BATCH_QUEUES').split(',')
    
    def collect(self):
        
        # metrics for core utilisation
        g_core_usage = GaugeMetricFamily('hpc_core_usage', 'number of used cores per node per queue', labels=['host', 'queue'])
        g_core_total = GaugeMetricFamily('hpc_core_total', 'number of total cores per node', labels=['host'])
        
        # metrics for memory utilisation
        g_mem_usage  = GaugeMetricFamily('hpc_mem_usage' , 'bytes of used memory per node per queue', labels=['host', 'queue'])
        g_mem_total  = GaugeMetricFamily('hpc_mem_total' , 'bytes of total memory per node', labels=['host'])
        
        # metrics for job count per queue, per state
        g_job_count  = GaugeMetricFamily('hpc_job_count' , 'number of jobs' , labels=['queue','status'])

        jobs = get_qstat_jobs(s_cmd=self.BIN_QSTAT_ALL)
        nodes = get_cluster_node_properties()
        
        # static node information
        for n in nodes:
            g_core_total.labels(host=n.host).set( n.cores )
            g_core_total.labels(host=n.host).set( n.mem * 1024 * 1024 * 1024 )

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
        q_jobs = filter( lambda j:j.jstate in ['Q', 'S'], jobs )
        h_jobs = filter( lambda j:j.jstate in ['H'], jobs )
        
        for j in q_jobs:
            g_job_count.labels(queue=j.queue, status='queued').inc(1)
            
        for j in h_jobs:
            g_job_count.labels(queue=j.queue, status='held').inc(1)

        ## get jobs in running state
        r_jobs = filter( lambda j:j.jstate in ['E','R'], jobs )
        
        for j in r_jobs:
            g_job_count.labels(queue=j.queue, status='running').inc(1)
            
            # set core usage
            for n in j.node:
                g_core_usage.labels(queue=j.queue, host=n).inc(1)
                g_mem_usage.labels(queue=j.queue, host=n).inc(rmem * 1024 * 1024 * 1024)
        
        # yield metrics
        yield g_core_total
        yield g_core_usage
        yield g_mem_total
        yield g_mem_usage
        yield g_job_count

class MetricsRegistry:
    '''data object containing metrics information'''
    __instance = None

    @staticmethod
    def getInstance():
        """ Static access method. """
        if MetricsRegistry.__instance == None:
            MetricsRegistry()
        return MetricsRegistry.__instance

    def __init__(self):
        if MetricsRegistry.__instance != None:
            raise Exception("This class is a singleton!")
        else:
            self.registry = CollectorRegistry()
            MetricsRegistry.__instance = self

    def registerMetrics(self, type, name, labels=[], desc=None):
        """register a new metric"""
        if not desc:
            desc = 'metric of %s' % name
        m = None
        if type == 'gauge':
            m = Gauge(name, desc, labels, registry=self.registry)
        elif type == 'counter':
            m = Counter(name, desc, labels, registry=self.registry)
        elif type == 'summary':
            m = Summary(name, desc, labels, registry=self.registry)
        elif type == 'histogram':
            m = Histogram(name, desc, labels, registry=self.registry)
        else:
            raise ValueError('unsupported metrics type %s' % type)
        return m

    def exportToFile(self, fpath):
        """export metrics in the registry to a file"""
        write_to_textfile(fpath, self.registry)
        return

    def pushToGateway(self, endpoint, job, instance):
        """push metrics in the registry to the prometheus gateway"""
        push_to_gateway(endpoint, job=job, instance=instance, registry=self.registry)
        return 
        
def testMetricsRegistry(config):
    """Test function for MetricsRegistry"""
    r = MetricsRegistry.getInstance()
    r.registry.register(CollectorCluster(config))
    
    # write out metrics to file
    r.exportToFile('test.prom')
