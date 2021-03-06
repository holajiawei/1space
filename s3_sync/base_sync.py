# Copyright 2019 SwiftStack
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
import logging

from s3_sync.utils import filter_hop_by_hop_headers

from swift.common import swob
from swift.common.utils import Timestamp
from .utils import (get_dlo_prefix, check_slo, get_internal_manifest,
                    iter_internal_listing)

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
        ETAG_MISMATCH = 7  # PUT response ETag did not match Swift data

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
        self.relevant_container_metadata = set()

        # Due to the genesis of this project, the endpoint and bucket have the
        # "aws_" prefix, even though the endpoint may actually be a Swift
        # cluster and the "bucket" is a container.
        self.endpoint = settings.get('aws_endpoint', None)
        self.aws_bucket = settings['aws_bucket']

        self.selection_criteria = settings.get('selection_criteria', {})
        self.sync_container_metadata = settings.get(
            'sync_container_metadata', False)
        self.sync_container_acl = settings.get(
            'sync_container_acl', False)
        if self.sync_container_acl:
            self.relevant_container_metadata.add('X-Container-Write')
            self.relevant_container_metadata.add('X-Container-Read')

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
            self.aws_bucket.encode('utf-8') if self.aws_bucket else '',
        )

    def post_container(self, metadata):
        return ProviderResponse(False, 501, {}, '')

    def select_container_metadata(self, metadata):
        """Select container metadata to track

        This is used to select which metadata to care about with regards to
        sync'ing with the remote cluster. It also translates from the database
        representation to a header format.

        :param metadata: in the format from broker.metadata, that is a dict of
                         the form {<key>: (<value>, <timestamp>), ...)
        :returns: dict of the form {<key>: <value>, ...) containing only
                  relevant key value pairs
        """
        result = {}
        if not self.sync_container_metadata:
            return result
        for key in self.relevant_container_metadata:
            if key in metadata:
                result[key] = metadata[key][0]
        for key in metadata:
            if key.startswith('X-Container-Meta-'):
                result[key] = metadata[key][0]
        return result

    def post_object(self, key, headers, bucket=None):
        raise NotImplementedError()

    def head_account(self):
        raise NotImplementedError()

    def put_object(self, key, headers, body, bucket=None, **options):
        """
        Uploads a single object to the provider's object store, as configured
        (container name, object name namespacing/prefixing, etc.).

        :param key: object key.
        :param headers: request headers; assumed to be in Swift parlance,
         and converted to S3-style headers if necessary.
        :param body: a string or unicode instance, a file-like object (like a
         wsgi.input stream wrapped by an InputProxy instance), or an iterable.
        :param bucket: Bucket/container for the PUT call, if overriding the
         current default.
        :param \**options: See individual providers for additional options.
        :returns: an instance of ProviderResponse.
        """
        raise NotImplementedError()

    def upload_object(self, row, internal_client, upload_stats_cb=None):
        raise NotImplementedError()

    def update_metadata(self, key, metadata, remote_metadata={}, bucket=None):
        raise NotImplementedError()

    def delete_object(self, key, bucket=None):
        raise NotImplementedError()

    def shunt_object(self, request, key):
        raise NotImplementedError()

    def shunt_post(self, request, key):
        raise NotImplementedError()

    def shunt_delete(self, request, key):
        raise NotImplementedError()

    def head_object(self, key, bucket=None, **options):
        raise NotImplementedError()

    def get_object(self, key, bucket=None, **options):
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

    def _delete_objects(self, objects_list, swift_client):
        '''
        Initialize a task queue to delete objects in parallel
        '''
        work_queue = eventlet.queue.Queue(self.SLO_QUEUE_SIZE)
        worker_pool = eventlet.greenpool.GreenPool(self.SLO_WORKERS)
        workers = []
        for _ in range(self.SLO_WORKERS):
            workers.append(
                worker_pool.spawn(
                    self._delete_object_worker, work_queue, swift_client))

        # key must be a tuple (container, object)
        for key in objects_list:
            work_queue.put(key)
        work_queue.join()
        for _ in range(self.SLO_WORKERS):
            work_queue.put(None)
        errors = []
        for thread in workers:
            worker_errors = thread.wait()
            errors.extend(worker_errors)
        return errors

    def _delete_object_worker(self, work_queue, swift_client):
        failed_segs = []
        while True:
            work = work_queue.get()
            if not work:
                work_queue.task_done()
                return failed_segs

            container, obj = work
            try:
                swift_client.delete_object(self.account, container, obj,
                                           acceptable_statuses=(2, 404, 409))
            except Exception as e:
                failed_segs.append(work)
                self.logger.warning('Failed to delete segment %s/%s/%s: %s',
                                    self.account, container, obj, str(e))
            finally:
                work_queue.task_done()

    def _delete_dlo_segments(self, swift_client, obj, dlo_prefix):
        s_container, s_prefix = dlo_prefix.split('/', 1)
        s_prefix = s_prefix.rstrip('/')

        def _keys_generator():
            internal_iterator = iter_internal_listing(
                swift_client, self.account, container=s_container,
                prefix=s_prefix)
            for entry in internal_iterator:
                if not entry:
                    return
                yield (s_container, entry['name'])

        failed_segs = self._delete_objects(_keys_generator(), swift_client)

        return failed_segs

    def _delete_slo_segments(self, swift_client, obj):
        _, manifest = get_internal_manifest(
            self.account, self.container, obj, swift_client, {})
        self.logger.debug("JSON manifest: %s", str(manifest))
        failed_segs = self._delete_objects(
            (segment['name'].split('/', 2)[1:] for segment in manifest),
            swift_client)

        return failed_segs

    def _delete_object_segments(self, swift_client, obj):
        metadata = swift_client.get_object_metadata(
            self.account, self.container, obj,
            acceptable_statuses=(2, 404))

        dlo_prefix = get_dlo_prefix(metadata)
        if dlo_prefix:
            return self._delete_dlo_segments(swift_client, obj, dlo_prefix)
        elif check_slo(metadata):
            return self._delete_slo_segments(swift_client, obj)
        else:
            # not a large object, do nothing
            return None

    def delete_local_object(self, swift_client, row, meta_ts,
                            retain_local_segments):
        '''
        On archive policy, the local objects are deleted after being
        succesfuly uploaded to remote cluster.

        NOTE: We rely on the DELETE object X-Timestamp header to
        mitigate races where the object may be overwritten. We
        increment the offset to ensure that we never remove new
        customer data.
        '''

        # Delete large object segments before deleting manifest
        failed_segs = []
        if not retain_local_segments:
            failed_segs = self._delete_object_segments(swift_client,
                                                       row['name'])

        if failed_segs:
            # if failed to delete all segments, don't delete manifest
            # sync will attempt to delete again on a later iteration
            self.logger.error("Failed to delete %s segments of %s/%s",
                              len(failed_segs), self.container, row['name'])
        else:
            self.logger.debug(
                "Creating a new TS before deleting %s/%s: %f %f" % (
                    self.container.decode('utf-8'),
                    row['name'].decode('utf-8'),
                    meta_ts.offset, meta_ts.timestamp))
            delete_ts = Timestamp(meta_ts, offset=meta_ts.offset + 1)
            headers = {'X-Timestamp': delete_ts.internal}
            swift_client.delete_object(
                self.account, self.container, row['name'],
                acceptable_statuses=(2, 404, 409), headers=headers)
