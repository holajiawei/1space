import errno
import pystatsd
import Queue
import socket
import threading
import time
import types


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
                self.process(data)
            except socket.error as e:
                if e.errno == errno.EAGAIN:
                    time.sleep(0.01)

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
