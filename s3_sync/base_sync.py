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

from s3_sync.utils import filter_hop_by_hop_headers

from swift.common import swob

LOGGER_NAME = 's3-sync'


def match_item(metadata, matchdict):
    if len(matchdict) == 0:  # No criteria matches all
        return True
    if len(matchdict) != 1:
        raise ValueError('Invalid Match dictionary: %s' % (matchdict,))
    key = matchdict.keys()[0]
    val = matchdict[key]
    if key == 'AND':
        return match_all(metadata, val)
    if key == 'OR':
        return match_any(metadata, val)
    if key == 'NOT':
        return not match_item(metadata, val)
    if key.encode('utf-8') in metadata:
        metadata_val = metadata.get(key.encode('utf-8'), '')
    elif key.lower().encode('utf-8') in metadata:
        metadata_val = metadata.get(key.lower().encode('utf-8'), '')
    else:
        return False
    if metadata_val is not None:
        metadata_val = metadata_val.lower()
    return metadata_val == val.lower().encode('utf-8')


def match_all(metadata, criteria):
    for matchdict in criteria:
        if not match_item(metadata, matchdict):
            return False
    return True


def match_any(metadata, criteria):
    for matchdict in criteria:
        if match_item(metadata, matchdict):
            return True
    return False


class ProviderResponse(object):
    def __init__(self, success, status, headers, body, exc_info=None):
        self.success = success
        self.status = status
        self.headers = headers
        self.body = body
        # optional result from calling sys.exc_info(); used to implement
        # reraise(), which allows a receiver of a ProviderResponse to reraise a
        # prior client library exception.
        self.exc_info = exc_info
        # WSGI expects an HTTP status + reason string
        self.wsgi_status = '%d %s' % (
            self.status, swob.RESPONSE_REASONS[self.status][0])

    def to_wsgi(self):
        return self.wsgi_status, self.headers.items(), self.body

    def to_swob_response(self, req=None):
        headers = dict(filter_hop_by_hop_headers(self.headers.items()))
        return swob.Response(app_iter=iter(self.body),
                             status=self.status,
                             headers=headers, request=req)

    def reraise(self):
        if self.exc_info:
            raise self.exc_info[0], self.exc_info[1], self.exc_info[2]
        header_str = '{' + ', '.join('%r: %r' % (k, self.headers[k])
                                     for k in sorted(self.headers)) + '}'
        body_str = ''.join(self.body) if self.body else ''
        if len(body_str) > 70:
            body_str = body_str[:70] + '...'
        me_as_a_str = '<%s: %s, %r, %s, %r>' % (
            self.__class__.__name__, self.success, self.status,
            header_str, body_str)
        raise ValueError('reraise had no prior exception for %s' % me_as_a_str)


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

    # Possible results of upload_object
    class UploadStatus(object):
        NOOP = 0  # object already exists
        PUT = 1
        POST = 2
        SKIPPED_METADATA = 3  # if metadata does not match
        INVALID_SLO = 4  # if the manifest cannot be validated
        NOT_FOUND = 5  # object does not appear to exist in Swift
        SKIPPED_NESTED_SLO = 6

    class HttpClientPoolEntry(object):
        def __init__(self, client, pool):
            self.semaphore = eventlet.semaphore.Semaphore(
                BaseSync.HTTP_CONN_POOL_SIZE)
            self.client = client
            self.pool = pool

        def acquire(self):
            return self.semaphore.acquire(blocking=False)

        def close(self):
            if self.semaphore.balance > BaseSync.HTTP_CONN_POOL_SIZE - 1:
                logging.getLogger(LOGGER_NAME).error(
                    'Detected double release of the semaphore')
                raise RuntimeError('Detected double release of the semaphore!')
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

    def __init__(self, settings, max_conns=10, per_account=False, logger=None,
                 extra_headers=None):
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
        logger -- anything that quacks like a Python logger; optional.
        extra_headers -- optional extra headers to send with every request; not
                         meant to be used outside of specific applications,
                         like cloud-connector's usage of providers.
        """

        self.settings = settings
        self.account = settings['account']
        self.container = settings['container']
        self.logger = logger or logging.getLogger(LOGGER_NAME)
        self._per_account = per_account
        if '/' in self.container:
            raise ValueError('Invalid container name %r' % self.container)
        self.extra_headers = extra_headers or {}

        # Due to the genesis of this project, the endpoint and bucket have the
        # "aws_" prefix, even though the endpoint may actually be a Swift
        # cluster and the "bucket" is a container.
        self.endpoint = settings.get('aws_endpoint', None)
        self.aws_bucket = settings['aws_bucket']

        self.selection_criteria = settings.get('selection_criteria', {})

        # custom prefix can potentially cause conflicts/data over write,
        # be VERY CAREFUL with this.
        self.custom_prefix = settings.get('custom_prefix', None)
        if self.custom_prefix is None:
            self.use_custom_prefix = False
        else:
            self.use_custom_prefix = True
            self.custom_prefix = self.custom_prefix.strip('/')
        self.client_pool = self.HttpClientPool(
            self._get_client_factory(), max_conns)

    def __repr__(self):
        return '<%s: %s/%s>' % (
            self.__class__.__name__,
            's3:/' if self.endpoint is None else self.endpoint.rstrip('/'),
            self.aws_bucket,
        )

    def post_object(self, swift_key, headers):
        raise NotImplementedError()

    def head_account(self):
        raise NotImplementedError()

    def put_object(self, swift_key, headers, body, query_string=None):
        """
        Uploads a single object to the provider's object store, as configured
        (container name, object name namespacing/prefixing, etc.).

        The headers provided are assumed to be in Swift parlance, and will be
        converted to S3-style headers if necessary.

        The `body` argument can be a string or unicode instance, a file-like
        object (like a wsgi.input stream wrapped by an InputProxy instance), or
        an iterable.

        The optional query_string is used only for the Swift provider and is
        sent on, verbatim, to the underlying swiftclient put_object() call.
        """
        raise NotImplementedError()

    def upload_object(self, row, internal_client):
        raise NotImplementedError()

    def update_metadata(self, swift_key, swift_meta):
        raise NotImplementedError()

    def delete_object(self, swift_key):
        raise NotImplementedError()

    def shunt_object(self, request, swift_key):
        raise NotImplementedError()

    def shunt_post(self, request, swift_key):
        raise NotImplementedError()

    def shunt_delete(self, request, swift_key):
        raise NotImplementedError()

    def head_object(self, swift_key, bucket=None, **options):
        raise NotImplementedError()

    def get_object(self, swift_key, bucket=None, **options):
        raise NotImplementedError()

    def head_bucket(self, bucket, **options):
        raise NotImplementedError()

    def list_objects(self, marker, limit, prefix, delimiter=None, bucket=None):
        raise NotImplementedError()

    def list_buckets(self, marker='', **kwargs):
        raise NotImplementedError()

    def close(self):
        for client in self.client_pool.client_pool:
            client.acquire()
            self._close_conn(client.client)
            client.close()

    def _get_client_factory(self):
        raise NotImplementedError()

    def _full_name(self, key):
        return u'%s/%s/%s' % (self.account, self.container,
                              key if isinstance(key, unicode)
                              else key.decode('utf-8'))
