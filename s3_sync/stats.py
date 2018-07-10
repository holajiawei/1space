import eventlet


class AtomicStats(object):
    def __init__(self):
        self._semaphore = eventlet.semaphore.Semaphore(1)

    def update(self, **kwargs):
        self._semaphore.acquire()
        self._update_stats(**kwargs)
        self._semaphore.release()


class MigratorPassStats(AtomicStats):
    def __init__(self):
        super(MigratorPassStats, self).__init__()
        self.copied = 0
        self.scanned = 0
        self.bytes_copied = 0

    def _update_stats(self, copied=0, scanned=0, bytes_copied=0):
        self.copied += copied
        self.scanned += scanned
        self.bytes_copied += bytes_copied
