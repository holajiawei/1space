# Copyright 2017 SwiftStack
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import eventlet


class AtomicStats(object):
    def __init__(self):
        self._semaphore = eventlet.semaphore.Semaphore(1)

    def update(self, **kwargs):
        self._semaphore.acquire()
        self._update_stats(**kwargs)
        self._semaphore.release()


class MigratorPassStats(AtomicStats):
    def __init__(self, statsd_increment):
        super(MigratorPassStats, self).__init__()
        self.statsd_increment = statsd_increment
        self.copied = 0
        self.scanned = 0
        self.bytes_copied = 0

    def _update_stats(self, copied=0, scanned=0, bytes_copied=0):
        if copied > 0:
            self.copied += copied
            self.statsd_increment('copied_objects', copied)
        if scanned > 0:
            self.scanned += scanned
            self.statsd_increment('scanned', scanned)
        if bytes_copied > 0:
            self.bytes_copied += bytes_copied
            self.statsd_increment('bytes', bytes_copied)
