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
import eventlet.pools
eventlet.patcher.monkey_patch(all=True)

from collections import namedtuple
import datetime
import errno
import hashlib
import json
import logging
import os
import sys
import time
import traceback

from container_crawler.utils import create_internal_client
from .daemon_utils import load_swift, setup_context, setup_logger
from .provider_factory import create_provider
from .utils import (convert_to_local_headers, convert_to_swift_headers,
                    get_container_headers, iter_listing, RemoteHTTPError,
                    SWIFT_TIME_FMT, diff_container_headers,
                    get_sys_migrator_header, MigrationContainerStates)
from swift.common.http import HTTP_NOT_FOUND, HTTP_CONFLICT
from swift.common import swob
from swift.common.internal_client import UnexpectedResponse
from swift.common.utils import FileLikeIter, Timestamp, quote


EQUAL = 0
ETAG_DIFF = 1
TIME_DIFF = 2
LAST_MODIFIED_FMT = '%a, %d %b %Y %H:%M:%S %Z'
EPOCH = datetime.datetime.utcfromtimestamp(0)

IGNORE_KEYS = set(('status', 'aws_secret', 'all_buckets', 'custom_prefix'))

MigrateObjectWork = namedtuple('MigrateObjectWork', 'aws_bucket container key')
UploadObjectWork = namedtuple('UploadObjectWork', 'container key object '
                              'headers aws_bucket')


class MigrationError(Exception):
    pass


class ContainerNotFound(Exception):
    def __init__(self, account, container, *args, **kwargs):
        self.account = account
        self.container = container
        super(ContainerNotFound, self).__init__(*args, **kwargs)

    def __unicode__(self):
        return u'Bucket/container "%s" does not exist for %s' % (
            self.container, self.account)


def equal_migration(left, right):
    for k in set(left.keys()) | set(right.keys()):
        if k in IGNORE_KEYS:
            continue
        if not (k in left and k in right):
            if k == 'container':
                continue
            return False
        if k in ('aws_bucket', 'container') and (
                left[k] == '/*' or right[k] == '/*'):
            continue
        if left[k] == right[k]:
            continue
        return False
    return True


def cmp_object_entries(left, right):
    local_time = datetime.datetime.strptime(
        left['last_modified'], SWIFT_TIME_FMT)
    remote_time = datetime.datetime.strptime(
        right['last_modified'], SWIFT_TIME_FMT)
    if local_time == remote_time:
        if left['hash'] == right['hash']:
            return 0
        raise MigrationError('Same time objects have different ETags!')
    return cmp(local_time, remote_time)


def cmp_meta(dest, source):
    if source['last-modified'] == dest['last-modified']:
        return EQUAL
    if source['etag'] != dest['etag']:
        return ETAG_DIFF
    return TIME_DIFF


def _update_status_counts(status, moved_count, scanned_count, reset):
    """
    Update counts and finished keys in status.  On reset copy existing counts
    to last_ counts if they've changed.
    """
    now = time.time()
    if reset:
        # the incoming counts are for the start of a new run, the counts in
        # status are from the previous run, the counts in last_ are from
        # *two* runs ago.  If the scan counts in status match up with the
        # scan counts in last_ and we haven't moved anything we don't
        # update last_
        overwrite_last = False
        if 'finished' in status:
            if 'last_finished' in status:
                was_something_moved = status['last_moved_count'] != 0
                scan_counts_match = \
                    status['scanned_count'] == status['last_scanned_count']
                overwrite_last = was_something_moved or not scan_counts_match
            else:
                overwrite_last = True
        if overwrite_last:
            status['last_moved_count'] = status['moved_count']
            status['last_scanned_count'] = status['scanned_count']
            status['last_finished'] = status['finished']
        status['moved_count'] = moved_count
        status['scanned_count'] = scanned_count
    else:
        status['moved_count'] = status.get('moved_count', 0) + moved_count
        status['scanned_count'] = status.get('scanned_count', 0) + \
            scanned_count
    # this is the end of this current pass
    status['finished'] = now


def _create_x_timestamp_from_hdrs(hdrs):
    if 'last-modified' in hdrs:
        ts = datetime.datetime.strptime(hdrs['last-modified'],
                                        LAST_MODIFIED_FMT)
        return (ts - EPOCH).total_seconds()
    return None


class Status(object):
    def __init__(self, status_location):
        self.status_location = status_location
        self.status_list = None

    def load_status_list(self):
        self.status_list = []
        try:
            with open(self.status_location) as fh:
                try:
                    self.status_list = json.load(fh)
                except ValueError as e:
                    if str(e) != 'No JSON object could be decoded':
                        raise
        except IOError as e:
            if e.errno != errno.ENOENT:
                raise

    def get_migration(self, migration):
        if not self.status_list:
            self.load_status_list()
        for entry in self.status_list:
            if equal_migration(entry, migration):
                return entry.get('status', {})
        return {}

    def save_status_list(self):
        try:
            with open(self.status_location, 'w') as fh:
                fh.truncate()
                json.dump(self.status_list, fh)
        except IOError as e:
            if e.errno == errno.ENOENT:
                os.mkdir(os.path.dirname(self.status_location), mode=0755)
                with open(self.status_location, 'w') as fh:
                    json.dump(self.status_list, fh)
            else:
                raise

    def save_migration(self, migration, marker, moved_count, scanned_count,
                       stats_reset=False):
        for entry in self.status_list:
            if equal_migration(entry, migration):
                if 'status' not in entry:
                    entry['status'] = {}
                if 'aws_secret' in entry:
                    entry.pop('aws_secret', None)
                status = entry['status']
                break
        else:
            entry = dict(migration)
            entry.pop('aws_secret', None)
            entry['status'] = {}
            self.status_list.append(entry)
            status = entry['status']

        status['marker'] = marker
        _update_status_counts(status, moved_count, scanned_count, stats_reset)
        self.save_status_list()

    def prune(self, migrations):
        self.load_status_list()
        keep_status_list = []
        for entry in self.status_list:
            for migration in migrations:
                if equal_migration(entry, migration):
                    keep_status_list.append(entry)
                    break
        self.status_list = keep_status_list
        self.save_status_list()


class Migrator(object):
    '''List and move objects from a remote store into the Swift cluster'''
    def __init__(self, config, status, work_chunk, swift_pool, logger,
                 node_id, nodes):
        self.config = dict(config)
        if 'container' not in self.config:
            # NOTE: in the future this may no longer be true, as we may allow
            # remapping buckets/containers during migrations.
            self.config['container'] = self.config['aws_bucket']
        # s3 side of migration is always like native (there is no way
        # to specify only take prefix from bucket at this time) and swift
        # cloud connector ignores the custom_prefix setting.
        self.config['custom_prefix'] = ''
        self.status = status
        self.work_chunk = work_chunk
        self.max_conns = swift_pool.max_size
        self.object_queue = eventlet.queue.Queue(self.max_conns * 2)
        self.ic_pool = swift_pool
        self.errors = eventlet.queue.Queue()
        self.workers = config.get('workers', 10)
        self.logger = logger
        self.node_id = node_id
        self.nodes = nodes
        self.provider = None

    def next_pass(self):
        if self.config['aws_bucket'] != '/*':
            self.provider = create_provider(
                self.config, self.max_conns, False)
            self._next_pass()
            return

        self.config['all_buckets'] = True
        self.config['container'] = '.'
        self.provider = create_provider(
            self.config, self.max_conns, False)
        try:
            self._reconcile_containers()
        except Exception:
            # Any exception raised will terminate the migrator process. As the
            # process should be going around in a loop through the configured
            # migrations, we log the error and continue. This requires us to
            # catch a bare exception.
            self.logger.error('Failed to list containers for "%s"' %
                              (self.config['account']))
            self.logger.error(''.join(traceback.format_exc()))

    def _reconcile_containers(self):
        resp, iterator = iter_listing(
            self.provider.list_buckets,
            self.logger, None, 10000, None)

        if not resp.success:
            self.logger.error(
                'Failed to list source buckets/containers: "%s"' %
                ''.join(resp.body))
            return

        # TODO: this is very similar to the code in _splice_listing() and
        # _find_missing_objects(). We might be able to provide a utility
        # function that accepts callables to operate on the streams.
        local_iterator = self._iterate_internal_listing()
        local_container = next(local_iterator)
        for index, entry in enumerate(iterator):
            remote_container, _ = entry
            if not remote_container:
                break
            remote_container = remote_container['name']

            while local_container and\
                    local_container['name'] < remote_container:
                self._maybe_delete_internal_container(local_container['name'])
                local_container = next(local_iterator)

            if index % self.nodes == self.node_id:
                # NOTE: we cannot remap container names when migrating the
                # entire account
                self.config['aws_bucket'] = remote_container
                self.config['container'] = remote_container
                self.provider.aws_bucket = remote_container
                self._next_pass()
            if local_container and local_container['name'] == remote_container:
                local_container = next(local_iterator)

        while local_container:
            self._maybe_delete_internal_container(local_container['name'])
            local_container = next(local_iterator)

    def _next_pass(self):
        worker_pool = eventlet.GreenPool(self.workers)
        for _ in xrange(self.workers):
            worker_pool.spawn_n(self._upload_worker)
        is_reset = False
        self._manifests = set()
        try:
            marker, scanned, copied = self._process_container()
            if scanned == 0 and marker:
                is_reset = True
                marker, scanned, copied = self._process_container(marker='')
        except ContainerNotFound as e:
            scanned = 0
            self.logger.error(unicode(e))
        except Exception:
            # We must catch any errors to make sure we stop our workers. This
            # might be better with a context manager.
            scanned = 0
            self.logger.error('Failed to migrate "%s"' %
                              self.config['aws_bucket'])
            self.logger.error(''.join(traceback.format_exc()))

        self.object_queue.join()
        self._stop_workers(self.object_queue)

        self.check_errors()
        if scanned == 0:
            return

        copied -= self.errors.qsize()
        # TODO: record the number of errors, as well
        self.status.save_migration(
            self.config, marker, copied, scanned, is_reset)

    def check_errors(self):
        while not self.errors.empty():
            container, key, err = self.errors.get()
            if type(err) == str:
                self.logger.error('Failed to migrate "%s/%s": %s' % (
                    container, key, err))
            else:
                self.logger.error('Failed to migrate "%s"/"%s": %s' % (
                    container, key, err[1]))
                self.logger.error(''.join(traceback.format_exception(*err)))

    def _stop_workers(self, q):
        for _ in range(self.workers):
            q.put(None)
        q.join()

    def _update_container_headers(self, container, internal_client, headers):
        # This explicitly does not use update_container_metadata because it
        # needs to be able to update the acls (swift_owner: True).
        req = swob.Request.blank(
            internal_client.make_path(self.config['account'], container),
            environ={'REQUEST_METHOD': 'POST',
                     'swift_owner': True},
            headers=headers)

        resp = req.get_response(internal_client.app)
        if resp.status_int // 100 != 2:
            raise UnexpectedResponse('Failed to update container headers for '
                                     '"%s": %d' % (container, resp.status_int),
                                     resp)

        self.logger.info('Updated headers for container "%s"' % container)

    def _create_container(self, container, internal_client, aws_bucket,
                          timeout=1):
        if self.config.get('protocol') == 'swift':
            try:
                headers = get_container_headers(self.provider, aws_bucket)
            except RemoteHTTPError as e:
                if e.resp.status == 404:
                    raise ContainerNotFound(
                        self.config['aws_identity'], aws_bucket)
                else:
                    raise
        else:
            headers = {}

        headers[get_sys_migrator_header('container')] =\
            MigrationContainerStates.MIGRATING

        req = swob.Request.blank(
            internal_client.make_path(self.config['account'], container),
            environ={'REQUEST_METHOD': 'PUT',
                     'swift_owner': True},
            headers=headers)

        resp = req.get_response(internal_client.app)
        if resp.status_int // 100 != 2:
            raise UnexpectedResponse('Failed to create container "%s": %d' % (
                container, resp.status_int), resp)

        start = time.time()
        while time.time() - start < timeout:
            if not internal_client.container_exists(
                    self.config['account'], container):
                time.sleep(0.1)
            else:
                self.logger.info('Created container "%s"' % container)
                return
        raise MigrationError('Timeout while creating container "%s"' %
                             container)

    def _iterate_internal_listing(
            self, container=None, marker='', prefix=None):
        '''Calls GET on the specified path to list items.

        Useful in case we cannot use the InternalClient.iter_{containers,
        objects}(). The InternalClient generators make multiple calls to the
        object store and require holding the client out of the InternalClient
        pool.
        '''
        while True:
            with self.ic_pool.item() as ic:
                path = ic.make_path(self.config['account'], container)
                query_string = 'format=json&marker=%s' % quote(marker)
                if prefix:
                    query_string += '&prefix=%s' % quote(prefix)
                resp = ic.make_request(
                    'GET', '%s?%s' % (path, query_string), {},
                    (2, HTTP_NOT_FOUND))
            if resp.status_int != 200:
                break
            if not resp.body:
                break

            listing = json.loads(resp.body)
            if not listing:
                break
            for entry in listing:
                yield entry
            marker = listing[-1]['name'].encode('utf-8')
        # Simplifies the bookkeeping
        yield None

    def _reconcile_deleted_objects(self, internal_client, container, key):
        # NOTE: to handle the case of objects being deleted from the source
        # cluster after they've been migrated, we have to HEAD the object to
        # check for the migration header.

        hdrs = internal_client.get_object_metadata(
            self.config['account'], container, key)
        if get_sys_migrator_header('object') in hdrs:
            self.logger.info(
                'Detected removed object %s. Removing from %s/%s' % (
                    key, self.config['account'], container))
            internal_client.delete_object(
                self.config['account'], container, key)

    def _maybe_delete_internal_container(self, container):
        '''Delete a specified internal container.

        Unfortunately, we cannot simply DELETE every object in the container,
        but have to issue a HEAD request to make sure the migrator header is
        not set. This makes clearing containers expensive and we hope that this
        is not a common operation.
        '''

        try:
            with self.ic_pool.item() as ic:
                headers = ic.get_container_metadata(
                    self.config['account'], container)
        except UnexpectedResponse as e:
            if e.resp.status_int == HTTP_NOT_FOUND:
                self.logger.info('Container %s/%s already removed' %
                                 (self.config['account'], container))
                return

            self.logger.error('Failed to delete container "%s/%s"' %
                              (self.config['account'], container))
            self.logger.error(''.join(traceback.format_exc()))
            return

        state = headers.get(get_sys_migrator_header('container'))
        if not state:
            self.logger.debug(
                'Not removing container %s/%s: created by a client.' %
                (self.config['account'], container))
            return

        if state == MigrationContainerStates.SRC_DELETED:
            return

        listing = self._iterate_internal_listing(container)
        for obj in listing:
            if not obj:
                break
            with self.ic_pool.item() as ic:
                self._reconcile_deleted_objects(ic, container, obj['name'])

        state_meta = {get_sys_migrator_header('container'):
                      MigrationContainerStates.SRC_DELETED}
        with self.ic_pool.item() as ic:
            if state == MigrationContainerStates.MIGRATING:
                try:
                    ic.delete_container(self.config['account'], container)
                except UnexpectedResponse as e:
                    if e.resp.status_int == HTTP_CONFLICT:
                        # NOTE: failing to DELETE the container is OK if there
                        # are objects in it. It means that there were write
                        # operations outside of the migrator.
                        ic.set_container_metadata(
                            self.config['account'], container, state_meta)
            else:
                ic.set_container_metadata(
                    self.config['account'], container, state_meta)

    def _iter_source_container(
            self, container, marker, prefix, list_all):
        next_marker = marker

        while True:
            resp = self.provider.list_objects(
                next_marker, self.work_chunk, prefix, bucket=container)
            if resp.status == 404:
                raise ContainerNotFound(
                    self.config['aws_identity'], container)
            if resp.status != 200:
                raise MigrationError(
                    'Failed to list source bucket/container "%s"' %
                    self.config['aws_bucket'])
            if not resp.body and marker and marker == next_marker:
                yield None
            for entry in resp.body:
                yield entry
            if not list_all or not resp.body:
                break
            next_marker = resp.body[-1]['name']
        yield None

    def _check_large_objects(self, aws_bucket, container, key, client):
        local_meta = client.get_object_metadata(
            self.config['account'], container, key)
        remote_resp = self.provider.head_object(key)

        if 'x-object-manifest' in remote_resp.headers and\
                'x-object-manifest' in local_meta:
            if remote_resp.headers['x-object-manifest'] !=\
                    local_meta['x-object-manifest']:
                self.errors.put((
                    container, key,
                    'Dynamic Large objects with differing manifests: '
                    '%s %s' % (remote_resp.headers['x-object-manifest'],
                               local_meta['x-object-manifest'])))
            # TODO: once swiftclient supports query_string on HEAD requests, we
            # would be able to compare the ETag of the manifest object itself.
            return

        if 'x-static-large-object' in remote_resp.headers and\
                'x-static-large-object' in local_meta:
            # We have to GET the manifests and cannot rely on the ETag, as
            # these are not guaranteed to be in stable order from Swift. Once
            # that issue is fixed in Swift, we can compare ETags.
            status, headers, local_manifest = client.get_object(
                self.config['account'], container, key, {})
            remote_manifest = self.provider.get_manifest(key,
                                                         bucket=aws_bucket)
            if json.load(FileLikeIter(local_manifest)) != remote_manifest:
                self.errors.put((
                    aws_bucket, key,
                    'Matching date, but differing SLO manifests'))
            return

        self.errors.put((
            aws_bucket, key,
            'Mismatching ETag for regular objects with the same date'))

    def _process_container(
            self, container=None, aws_bucket=None, marker=None, prefix=None,
            list_all=False):
        if aws_bucket is None:
            aws_bucket = self.config['aws_bucket']
        if container is None:
            container = self.config['container']
        if marker is None:
            state = self.status.get_migration(self.config)
            marker = state.get('marker', '')
        if prefix is None:
            prefix = self.config.get('prefix', '')
        # If a container has versioning enabled (either x-versions-location or
        # x-history-location is configured), we should migrate the versions
        # before migrating the container itself.
        if self.config.get('protocol') == 'swift':
            resp = self.provider.head_bucket(aws_bucket)
            if resp.status == 404:
                raise ContainerNotFound(
                    self.config['aws_identity'], aws_bucket)
            if resp.status != 200:
                raise MigrationError(
                    'Failed to HEAD bucket/container "%s"' % container)
            if 'x-versions-location' in resp.headers or\
                    'x-history-location' in resp.headers:
                versioned_container = resp.headers.get(
                    'x-versions-location')
                if not versioned_container:
                    versioned_container = resp.headers.get(
                        'x-history-location')
                if versioned_container:
                    # This diverts from our usual strategy of splitting up
                    # the work, but we cannot migrate the main container
                    # until the versions container is migrated.
                    self._process_container(
                        container=versioned_container,
                        aws_bucket=versioned_container, marker='',
                        list_all=True)

            local_headers = None
            with self.ic_pool.item() as ic:
                try:
                    local_headers = ic.get_container_metadata(
                        self.config['account'], container)
                except UnexpectedResponse as e:
                    if e.resp.status_int == HTTP_NOT_FOUND:
                        # TODO: this makes one more HEAD request to fetch
                        # headers. We should re-use resp.headers here
                        # (appropriately converted to handle versioning)
                        self._create_container(container, ic, aws_bucket)
                    else:
                        raise
                if resp.headers and local_headers:
                    local_ts = _create_x_timestamp_from_hdrs(local_headers)
                    remote_ts = _create_x_timestamp_from_hdrs(resp.headers)
                    header_changes = {}

                    if local_ts is not None and remote_ts is not None and \
                            local_ts < remote_ts:
                        header_changes = diff_container_headers(
                            resp.headers, local_headers)

                    migrator_header = get_sys_migrator_header('container')
                    if local_headers.get(migrator_header) ==\
                            MigrationContainerStates.SRC_DELETED:
                        header_changes[migrator_header] =\
                            MigrationContainerStates.MODIFIED

                    if header_changes:
                        self._update_container_headers(
                            container, ic, header_changes)
        else:  # Not swift
            with self.ic_pool.item() as ic:
                if not ic.container_exists(self.config['account'], container):
                    self._create_container(container, ic, aws_bucket)

        return self._find_missing_objects(container, aws_bucket, marker,
                                          prefix, list_all)

    def _old_enough(self, remote):
        older_than = self.config.get('older_than')
        if older_than is None:
            return True
        older_than = datetime.timedelta(seconds=older_than)
        now = datetime.datetime.utcnow()
        remote_time = datetime.datetime.strptime(
            remote['last_modified'], SWIFT_TIME_FMT)
        return remote_time < now - older_than

    def _find_missing_objects(
            self, container, aws_bucket, marker, prefix, list_all):

        try:
            source_iter = self._iter_source_container(
                aws_bucket, marker, prefix, list_all)
        except StopIteration:
            source_iter = iter([])

        copied = 0
        scanned = 0
        local_iter = self._iterate_internal_listing(container, marker, prefix)
        local = next(local_iter)
        remote = next(source_iter)
        if remote:
            marker = remote['name']
        while remote:
            # NOTE: the listing from the given marker may return fewer than
            # the number of items we should process. We will process all of
            # the keys that were returned in the listing and restart on the
            # following iteration.
            if not local or local['name'] > remote['name']:
                if self._old_enough(remote):
                    work = MigrateObjectWork(aws_bucket, container,
                                             remote['name'])
                    self.object_queue.put(work)
                    copied += 1
                scanned += 1
                remote = next(source_iter)
                if remote:
                    marker = remote['name']
            elif local['name'] < remote['name']:
                with self.ic_pool.item() as ic:
                    self._reconcile_deleted_objects(
                        ic, container, local['name'])
                local = next(local_iter)
            else:
                try:
                    cmp_ret = cmp_object_entries(local, remote)
                except MigrationError:
                    # This should only happen if we are comparing large
                    # objects: there will be an ETag mismatch.
                    with self.ic_pool.item() as ic:
                        self._check_large_objects(
                            aws_bucket, container, remote['name'], ic)
                else:
                    if cmp_ret < 0 and self._old_enough(remote):
                        work = MigrateObjectWork(aws_bucket, container,
                                                 remote['name'])
                        self.object_queue.put(work)
                        copied += 1
                remote = next(source_iter)
                local = next(local_iter)
                scanned += 1
                if remote:
                    marker = remote['name']

        while local and (not marker or local['name'] < marker or scanned == 0):
            # We may have objects left behind that need to be removed
            with self.ic_pool.item() as ic:
                self._reconcile_deleted_objects(ic, container, local['name'])
            local = next(local_iter)
        return marker, scanned, copied

    def _migrate_object(self, aws_bucket, container, key):
        args = {'bucket': aws_bucket}
        if self.config.get('protocol', 's3') == 'swift':
            args['resp_chunk_size'] = 65536

        if (container, key) in self._manifests:
            # Special handling for the DLO manifests
            args['query_string'] = 'multipart-manifest=get'
            resp = self.provider.get_object(key, **args)
            if 'x-object-manifest' not in resp.headers:
                self.logger.warning('DLO object changed before upload: %s/%s' %
                                    (aws_bucket, key))
                resp.body.close()
                return
            self._upload_object(UploadObjectWork(
                container, key, FileLikeIter(resp.body),
                convert_to_local_headers(resp.headers.items(),
                                         remove_timestamp=False),
                aws_bucket))
            return

        resp = self.provider.get_object(key, **args)
        if resp.status != 200:
            resp.body.close()
            raise MigrationError('Failed to GET "%s/%s": %s' % (
                aws_bucket, key, resp.body))
        put_headers = convert_to_local_headers(
            resp.headers.items(), remove_timestamp=False)
        if 'x-object-manifest' in resp.headers:
            self.logger.warning(
                'Migrating Dynamic Large Object "%s/%s" -- '
                'results may not be consistent' % (container, key))
            resp.body.close()
            self._migrate_dlo(aws_bucket, container, key, put_headers)
        elif 'x-static-large-object' in resp.headers:
            # We have to move the segments and then move the manifest file
            resp.body.close()
            self._migrate_slo(aws_bucket, container, key, put_headers)
        else:
            work = UploadObjectWork(
                container, key, FileLikeIter(resp.body), put_headers,
                aws_bucket)
            self._upload_object(work)

    def _migrate_dlo(self, aws_bucket, container, key, headers):
        dlo_container, prefix = headers['x-object-manifest'].split('/', 1)
        self._manifests.add((container, key))
        self._process_container(
            container=dlo_container, aws_bucket=dlo_container, marker='',
            prefix=prefix, list_all=True)
        if dlo_container != container or not key.startswith(prefix):
            # The DLO prefix can include the manifest object, which doesn't
            # have to be 0-sized. We have to be careful not to end up recursing
            # infinitely in that case.
            work = MigrateObjectWork(aws_bucket, container, key)
            self.object_queue.put(work)

    def _migrate_slo(self, aws_bucket, slo_container, key, headers):
        manifest = self.provider.get_manifest(key, aws_bucket)
        if not manifest:
            raise MigrationError('Failed to fetch the manifest for "%s/%s"' % (
                                 aws_bucket, key))
        for entry in manifest:
            container, segment_key = entry['name'][1:].split('/', 1)
            meta = None
            with self.ic_pool.item() as ic:
                try:
                    meta = ic.get_object_metadata(
                        self.config['account'], container, segment_key)
                except UnexpectedResponse as e:
                    if e.resp.status_int != 404:
                        self.errors.put((container, segment_key,
                                         sys.exc_info()))
                        continue
            if meta:
                resp = self.provider.head_object(
                    segment_key, container)
                if resp.status != 200:
                    raise MigrationError('Failed to HEAD "%s/%s"' % (
                        container, segment_key))
                src_meta = resp.headers
                if self.config.get('protocol', 's3') != 'swift':
                    src_meta = convert_to_swift_headers(src_meta)
                ret = cmp_meta(meta, src_meta)
                if ret == EQUAL:
                    continue
                if ret == TIME_DIFF:
                    # TODO: update metadata
                    self.logger.warning('Object metadata changed for "%s/%s"' %
                                        (container, segment_key))
                    continue
            work = MigrateObjectWork(container, container, segment_key)
            self.object_queue.put(work)
        manifest_blob = json.dumps(manifest)
        headers['Content-Length'] = str(len(manifest_blob))
        # The SLO middleware is not in the pipeline. The ETag we provide should
        # be for the manifest *JSON content*, rather than the hash of hashes
        # that the SLO middleware can validate.
        headers['etag'] = hashlib.md5(manifest_blob).hexdigest()
        work = UploadObjectWork(slo_container, key,
                                FileLikeIter(manifest_blob), headers,
                                slo_container)
        self.object_queue.put(work)

    def _upload_object(self, work):
        container, key, content, headers, aws_bucket = work
        headers['x-timestamp'] = Timestamp(
            _create_x_timestamp_from_hdrs(headers)).internal
        del headers['last-modified']
        headers[get_sys_migrator_header('object')] = headers['x-timestamp']
        with self.ic_pool.item() as ic:
            try:
                ic.upload_object(
                    content, self.config['account'], container, key,
                    headers)
                self.logger.debug('Copied "%s/%s"' % (container, key))
            except UnexpectedResponse as e:
                if e.resp.status_int != 404:
                    raise
                self._create_container(container, ic, aws_bucket)
                ic.upload_object(
                    content, self.config['account'], container, key,
                    headers)
                self.logger.debug('Copied "%s/%s"' % (container, key))

    def _upload_worker(self):
        while True:
            work = self.object_queue.get()
            try:
                if not work:
                    break
                aws_bucket = work.aws_bucket
                container = work.container
                key = work.key
                if isinstance(work, MigrateObjectWork):
                    self._migrate_object(aws_bucket, container, key)
                else:
                    self._upload_object(work)
            except Exception:
                # Avoid killing the worker, as it should only quit explicitly
                # when we initiate it. Otherwise, we might deadlock if all
                # workers quit, but the queue has not been drained.
                self.errors.put((aws_bucket, key, sys.exc_info()))
            finally:
                self.object_queue.task_done()

    def close(self):
        if not self.provider:
            return
        self.provider.close()
        self.provider = None


def process_migrations(migrations, migration_status, internal_pool, logger,
                       items_chunk, node_id, nodes):
    for index, migration in enumerate(migrations):
        if migration['aws_bucket'] == '/*' or index % nodes == node_id:
            if migration.get('remote_account'):
                src_account = migration.get('remote_account')
            else:
                src_account = migration['aws_identity']
            logger.info('Processing "%s"' % (
                ':'.join([migration.get('aws_endpoint', ''),
                          src_account, migration['aws_bucket']])))
            migrator = Migrator(migration, migration_status,
                                items_chunk,
                                internal_pool, logger,
                                node_id, nodes)
            migrator.next_pass()
            migrator.close()


def run(migrations, migration_status, internal_pool, logger, items_chunk,
        node_id, nodes, poll_interval, once):
    while True:
        cycle_start = time.time()
        process_migrations(migrations, migration_status, internal_pool, logger,
                           items_chunk, node_id, nodes)
        elapsed = time.time() - cycle_start
        naptime = max(0, poll_interval - elapsed)
        msg = 'Finished cycle in %0.2fs' % elapsed
        if once:
            logger.info(msg)
            return
        msg += ', sleeping for %0.2fs.' % naptime
        logger.info(msg)
        time.sleep(naptime)


def main():
    args, conf = setup_context(
        description='Daemon to migrate objects into Swift')
    if 'migrator_settings' not in conf:
        print 'Missing migrator settings section'
        exit(-1)

    migrator_conf = conf['migrator_settings']
    if 'status_file' not in migrator_conf:
        print 'Missing status file location!'
        exit(-1 * errno.ENOENT)

    logger_name = 'swift-s3-migrator'
    if args.log_level:
        migrator_conf['log_level'] = args.log_level
    migrator_conf['console'] = args.console

    setup_logger(logger_name, migrator_conf)
    load_swift(logger_name, args.once)

    logger = logging.getLogger(logger_name)

    pool_size = migrator_conf.get('workers', 10)
    swift_dir = conf.get('swift_dir', '/etc/swift')
    internal_pool = eventlet.pools.Pool(
        create=lambda: create_internal_client(conf, swift_dir),
        min_size=0,
        max_size=pool_size)

    if 'process' not in migrator_conf or 'processes' not in migrator_conf:
        print 'Missing "process" or "processes" settings in the config file'
        exit(-1)

    items_chunk = migrator_conf['items_chunk']
    node_id = int(migrator_conf['process'])
    nodes = int(migrator_conf['processes'])
    poll_interval = float(conf.get('poll_interval', 5))

    migrations = conf.get('migrations', [])
    migration_status = Status(migrator_conf['status_file'])
    migration_status.prune(migrations)

    run(migrations, migration_status, internal_pool, logger, items_chunk,
        node_id, nodes, poll_interval, args.once)


if __name__ == '__main__':
    main()
