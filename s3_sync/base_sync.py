"""
Copyright 2017 SwiftStack

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import eventlet
import logging


class ProviderResponse(object):
    def __init__(self, success, status, headers, body):
        self.success = success
        self.status = status
        self.headers = headers
        self.body = body

    def to_wsgi(self):
        return self.status, self.headers.items(), self.body


class BaseSync(object):
    """Generic base class that each provider must implement.

       These classes implement the actual data transfers, validation that
       objects have been propagated, and any other related operations to
       propagate Swift objects and metadata to a remote endpoint.
    """

    HTTP_CONN_POOL_SIZE = 1
    SLO_WORKERS = 10
    SLO_QUEUE_SIZE = 100
    MB = 1024 * 1024
    GB = 1024 * MB

    class HttpClientPoolEntry(object):
        def __init__(self, client, pool):
            self.semaphore = eventlet.semaphore.Semaphore(
                BaseSync.HTTP_CONN_POOL_SIZE)
            self.client = client
            self.pool = pool

        def acquire(self):
            return self.semaphore.acquire(blocking=False)

        def close(self):
            self.semaphore.release()
            self.pool.release()

        def __enter__(self):
            return self.client

        def __exit__(self, exc_type, exc_value, traceback):
            self.close()

    class HttpClientPool(object):
        def __init__(self, client_factory, max_conns):
            self.get_semaphore = eventlet.semaphore.Semaphore(max_conns)
            self.client_pool = self._create_pool(client_factory, max_conns)

        def _create_pool(self, client_factory, max_conns):
            clients = max_conns / BaseSync.HTTP_CONN_POOL_SIZE
            if max_conns % BaseSync.HTTP_CONN_POOL_SIZE:
                clients += 1
            self.pool_size = clients
            self.client_factory = client_factory
            # The pool is lazy-populated on every get request, up to the
            # calculated pool_size
            return []

        def get_client(self):
            # SLO uploads may exhaust the client pool and we will need to wait
            # for connections
            self.get_semaphore.acquire()
            # we are guaranteed that there is an open connection we can use
            # or we should create one
            for client in self.client_pool:
                if client.acquire():
                    return client
            if len(self.client_pool) < self.pool_size:
                new_entry = BaseSync.HttpClientPoolEntry(
                    self.client_factory(), self)
                new_entry.acquire()
                self.client_pool.append(new_entry)
                return new_entry
            raise RuntimeError('Pool was exhausted')  # should never happen

        def release(self):
            self.get_semaphore.release()

        def free_count(self):
            return self.get_semaphore.balance

    def __init__(self, settings, max_conns=10, per_account=False):
        """Base class that every Cloud Sync provider implementation should
        derive from. Sets up the client pool for the provider and the common
        settings.

        Arguments:
        settings -- all of the settings for the provider. Required keys are:
            account -- Swift account
            container -- Swift container
            Other required keys are provider-dependent.

        Keyword arguments:
        max_conns -- maximum number of connections the pool should support.
        per_account -- whether the sync is per-account, where all containers
                       are synced.
        """

        self.settings = settings
        self.account = settings['account']
        self.container = settings['container']
        self.logger = logging.getLogger('s3-sync')
        self._per_account = per_account
        if '/' in self.container:
            raise ValueError('Invalid container name %r' % self.container)

        # Due to the genesis of this project, the endpoint and bucket have the
        # "aws_" prefix, even though the endpoint may actually be a Swift
        # cluster and the "bucket" is a container.
        self.endpoint = settings.get('aws_endpoint', None)
        self.aws_bucket = settings['aws_bucket']

        self.client_pool = self.HttpClientPool(
            self._get_client_factory(), max_conns)

    def __repr__(self):
        return '<%s: %s/%s>' % (
            self.__class__.__name__,
            's3:/' if self.endpoint is None else self.endpoint.rstrip('/'),
            self.aws_bucket,
        )

    def upload_object(self, name, storage_policy_index):
        raise NotImplementedError()

    def update_metadata(self, swift_key, swift_meta):
        raise NotImplementedError()

    def delete_object(self, name):
        raise NotImplementedError()

    def shunt_object(self, request, name):
        raise NotImplementedError()

    def get_object(self, key, bucket=None, **options):
        raise NotImplementedError()

    def head_object(self, key, bucket=None, **options):
        raise NotImplementedError()

    def head_bucket(self, bucket, **options):
        raise NotImplementedError()

    def list_objects(self, marker, limit, prefix, delimiter=None):
        raise NotImplementedError()

    def list_buckets(self):
        raise NotImplementedError()

    def _get_client_factory(self):
        raise NotImplementedError()

    def _full_name(self, key):
        return u'%s/%s/%s' % (self.account, self.container,
                              key.decode('utf-8'))
