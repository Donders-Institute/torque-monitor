from __future__ import print_function
import socket
import threading
import time
import random
import string
import sys
try:
    import queue
except ImportError:
    import Queue as queue

MPS_LIMIT = 0  # Limit on metrics per second to send to OpenTSDB

_last_timestamp = None
_last_metrics = set()

_valid_metric_chars = set(string.ascii_letters + string.digits + '-_./')


def _mksocket(host, port, q, done, stop):
    """Returns a tcp socket to (host/port). Retries forever if connection fails"""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(2)
    attempt = 0
    while not stop.is_set():
        try:
            s.connect((host, port))
            return s
        except Exception as ex:
            # Simple exponential backoff: sleep for 1,2,4,8,16,30,30...
            time.sleep(min(30, 2 ** attempt))
            attempt += 1

def _push(host, port, q, done, mps, stop, test_mode):
    """Worker thread. Connect to host/port, pull data from q until done is set"""
    sock = None
    retry_line = None
    while not ( stop.is_set() or ( done.is_set() and retry_line == None and q.empty()) ):
        stime = time.time()

        if sock == None and not test_mode:
            sock = _mksocket(host, port, q, done, stop)
            if sock == None:
                break

        if retry_line:
            line = retry_line
            retry_line = None
        else:
            try:
                line = q.get(True, 1)  # blocking, with 1 second timeout
            except:
                if done.is_set():  # no items in queue, and parent finished
                    break
                else:  # no items in queue, but parent might send more
                    continue

        if not test_mode:
            try:
                sock.sendall(line.encode('utf-8'))
            except:
                sock = None  # notify that we need to make a new socket at start of loop
                retry_line = line  # can't really put back in q, so remember to retry this line
                continue

        etime = time.time() - stime  #time that actually elapsed

        #Expected value of wait_time is 1/MPS_LIMIT, ie. MPS_LIMIT per second.
        if mps > 0:
            wait_time = (2.0 * random.random()) / (mps)
            if wait_time > etime:  #if we should wait
                time.sleep(wait_time - etime)  #then wait

    if sock:
        sock.close()


class Client():
    def __init__(self, host, port=4242, qsize=100000, host_tag=True,
                 mps=MPS_LIMIT, check_host=True, test_mode=False):
        """Main tsdb client. Connect to host/port. Buffer up to qsize metrics"""

        self.q = queue.Queue(maxsize=qsize)
        self.done = threading.Event()
        self._stop = threading.Event()
        self.host = host
        self.port = int(port)
        self.queued = 0

        # Make initial check that the host is up, because once in the
        # background thread it will be silently ignored/retried
        if check_host == True:
            temp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            temp_sock.settimeout(3)
            temp_sock.connect((self.host, self.port))
            temp_sock.close()

        if host_tag == True:
            self.host_tag = socket.gethostname()
        elif isinstance(host_tag, str):
            self.host_tag = host_tag
        else:
            self.host_tag = None

        self.t = threading.Thread(target=_push,
                                  args=(host, self.port, self.q, self.done, mps, self._stop, test_mode))
        #self.t.daemon = daemon
        self.t.daemon = True
        self.t.start()

    def log(self, name, val, **tags):
        """Log metric name with value val. You must include at least one tag as a kwarg"""
        global _last_timestamp, _last_metrics

        # do not allow .log after closing
        assert not self.done.is_set(), "worker thread has been closed"
        # check if valid metric name
        assert all(c in _valid_metric_chars for c in name), "invalid metric name " + name

        val = float(val)  #Duck type to float/int, if possible.
        if int(val) == val:
            val = int(val)

        if self.host_tag and 'host' not in tags:
            tags['host'] = self.host_tag

        # get timestamp from system time, unless it's supplied as a tag
        timestamp = int(tags.pop('timestamp', time.time()))

        assert not self.done.is_set(), "tsdb object has been closed"
        assert tags != {}, "Need at least one tag"

        tagvals = ' '.join(['%s=%s' % (k, v) for k, v in tags.items()])

        # OpenTSDB has major problems if you insert a data point with the same
        # metric, timestamp and tags. So we keep a temporary set of what points
        # we have sent for the last timestamp value. If we encounter a duplicate,
        # it is dropped.
        unique_str = "%s, %s, %s, %s, %s" % (name, timestamp, tagvals, self.host, self.port)
        if timestamp == _last_timestamp or _last_timestamp == None:
            if unique_str in _last_metrics:
                return  # discard duplicate metrics
            else:
                _last_metrics.add(unique_str)
        else:
            _last_timestamp = timestamp
            _last_metrics.clear()

        line = "put %s %d %s %s\n" % (name, timestamp, val, tagvals)

        try:
            self.q.put(line, False)
            self.queued += 1
        except queue.Full:
            print("potsdb - Warning: dropping oldest metric because Queue is full. Size: %s" % self.q.qsize(), file=sys.stderr)
            self.q.get()  #Drop the oldest metric to make room
            self.q.put(line, False)
        return line # So we can get visibility on what was sent

    send = log  # Alias function name

    def close(self):
        """Close and clean up the connection"""
        self.done.set()

    def wait(self):
        """Close then block waiting for background thread to finish"""
        self.close()
        while self.t.is_alive():
            time.sleep(0.05)

    def stop(self):
        self._stop.set()

    def __del__(self):
        self.close()
