from prometheus_client import CollectorRegistry
from prometheus_client import Gauge, Counter, Summary, Histogram
from prometheus_client.core import GaugeMetricFamily
from prometheus_client import write_to_textfile, push_to_gateway
from prometheus_client.exposition import basic_auth_handler

from utils.Cluster import *
from utils.Common  import *

class ClusterMetrics:
    """metrics collector for cluster statistics"""
    def __init__(self, config):
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

    def pushToGateway(self, endpoint, job, instance):
        """push metrics in the registry to the prometheus gateway"""
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
                
        return
        
def testClusterMetrics(config):
    """Test function for MetricsRegistry"""
    m = ClusterMetrics(config)
    # collection metrics
    m.collectMetrics()
    # write out metrics to file
    m.exportToFile('test.prom')
