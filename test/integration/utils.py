from container_crawler.crawler import Crawler
import errno
import logging
import pystatsd
import Queue
import socket
import threading
import time
import types

import s3_sync.sync_container
import s3_sync.daemon_utils


class StatsdServer(pystatsd.server.Server):
    '''Thin wrapper around the pystatsd.server.Server.

    Can start the statsd server as a separate thread and stop it.'''

    def __init__(self, host='localhost', port=8125):
        self._queue = Queue.Queue()
        self._port = port
        self._host = host
        self._thread = None
        self._stopping = threading.Event()
        super(StatsdServer, self).__init__(
            transport='graphite_queue', queue=self._queue)

    def _setup_socket(self):
        '''Mostly copied from pystatsd.server.Server'''

        assert isinstance(self._port, types.IntType), \
            'port is not an integer: %s' % (self._port,)
        addr = (self._host, self._port)
        try:
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self._sock.bind(addr)
        except socket.gaierror:
            self._sock = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
            self._sock.bind(addr)

        if self.statsd_forward_address:
            if pystatsd.server.is_ipv6(self.statsd_forward_address[0]):
                self._sock_forward = socket.socket(socket.AF_INET6,
                                                   socket.SOCK_DGRAM)
            else:
                self._sock_forward = socket.socket(socket.AF_INET,
                                                   socket.SOCK_DGRAM)
        self._set_timer()
        self._stopping.clear()

    def _serve(self):
        # Use a non-blocking socket and poll for messages, so that
        # we can terminate the server
        self._sock.setblocking(0)

        while True and not self._stopping.is_set():
            try:
                data, addr = self._sock.recvfrom(self.buf)
            except socket.error as e:
                if e.errno == errno.EAGAIN:
                    time.sleep(0.01)
                    continue
                raise
            self.process(data)

    @pystatsd.server.close_on_exn
    def serve(self):
        self._setup_socket()
        self._thread = threading.Thread(target=self._serve)
        self._thread.start()

    def stop(self):
        self._stopping.set()
        if self._thread:
            self._thread.join()
        super(StatsdServer, self).stop()

    def clear(self):
        self.flush()
        while not self._queue.empty():
            self._queue.get()

    def get_messages(self):
        self.flush()
        while not self._queue.empty():
            yield self._queue.get()

    def unread_messages(self):
        return self._queue.qsize()


class TempSyncContainer(s3_sync.sync_container.SyncContainer):
    def __init__(self, status, *args, **kwargs):
        self._status = status
        super(TempSyncContainer, self).__init__(*args, **kwargs)

    def _save_status_row(self, row, field_id, db_id):
        policy = {}
        for field in self.POLICY_FIELDS:
            policy[field] = getattr(self, field)
        status = self._status.get(db_id)
        if not status:
            status = {self.PROCESSED_ROW_KEY: 0,
                      self.VERIFIED_ROW_KEY: 0,
                      'aws_bucket': self.aws_bucket,
                      'policy': policy}
            self._status[db_id] = status
        status[field_id] = row

    def _get_status_row(self, field_id, db_id):
        return self._status.get(db_id, {}).get(field_id, 0)


class TempSyncContainerFactory(s3_sync.sync_container.SyncContainerFactory):
    def __init__(self, *args, **kwargs):
        super(TempSyncContainerFactory, self).__init__(*args, **kwargs)
        self._handler_class = TempSyncContainer
        self._status = {}

    def instance(self, settings, per_account=True):
        # Copied from SyncContainerFactory, but added plumbing for the _status
        if 'statsd_host' in self.config:
            statsd_client = pystatsd.statsd.Client(
                self.config['statsd_host'],
                self.config.get('statsd_port', 8125),
                self.config.get('statsd_prefix'))
        else:
            statsd_client = None

        return self._handler_class(
            self._status,
            self.config['status_dir'],
            settings,
            per_account=per_account,
            statsd_client=statsd_client)


def get_container_crawler(profile, **kwargs):
    defaults = {
        'log_level': 'info',
        'log_file': '/var/log/1space-sync.log',
        'verification_slack': 60,
        'enumerator_workers': 10,
        'workers': 10,
        'items_chunk': 1000,
        'status_dir': '/foo/bar',
        'devices': '/swift/nodes/1/node',
        'statsd_host': 'localhost',
        'statsd_port': 21337}
    conf = {'containers': [profile]}
    for key in defaults:
        conf[key] = kwargs.get(key, defaults[key])

    logger = logging.getLogger(s3_sync.base_sync.LOGGER_NAME)
    if not logger.handlers:
        s3_sync.daemon_utils.initialize_loggers(conf)
        s3_sync.daemon_utils.load_swift(s3_sync.base_sync.LOGGER_NAME, True)

    logger.debug('Starting S3Sync')

    factory = TempSyncContainerFactory(conf)
    return Crawler(conf, factory, logger)
