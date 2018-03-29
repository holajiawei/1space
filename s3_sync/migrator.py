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
                    SWIFT_TIME_FMT)
from swift.common.internal_client import UnexpectedResponse
from swift.common.swob import Request
from swift.common.utils import FileLikeIter, Timestamp


EQUAL = 0
ETAG_DIFF = 1
TIME_DIFF = 2
LAST_MODIFIED_FMT = '%a, %d %b %Y %H:%M:%S %Z'
EPOCH = datetime.datetime.utcfromtimestamp(0)

IGNORE_KEYS = set(('status', 'aws_secret', 'all_buckets'))

MigrateObjectWork = namedtuple('MigrateObjectWork', 'aws_bucket container key')
UploadObjectWork = namedtuple('UploadObjectWork', 'container key object '
                              'headers')


class MigrationError(Exception):
    pass


class ContainerNotFound(Exception):
    def __init__(self, container, *args, **kwargs):
        self.container = container
        super(ContainerNotFound, self).__init__(*args, **kwargs)

    def __unicode__(self):
        return u'Bucket/container "%s" does not exist' % self.container


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
        if self.config.get('protocol') != 'swift':
            # s3 side of migration is always like native (there is no way
            # to specify only take prefix from bucket at this time
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
        resp = self.provider.list_buckets()
        if not resp.success:
            self.logger.error(
                'Failed to list source buckets/containers: "%s"' %
                ''.join(resp.body))
            return

        for index, container in enumerate(resp.body):
            if index % self.nodes == self.node_id:
                # NOTE: we cannot remap container names when migrating all
                # containers
                self.config['aws_bucket'] = container['name']
                self.config['container'] = container['name']
                self.provider.aws_bucket = container['name']
                self._next_pass()

    def _next_pass(self):
        worker_pool = eventlet.GreenPool(self.workers)
        for _ in xrange(self.workers):
            worker_pool.spawn_n(self._upload_worker)
        is_reset = False
        self._manifests = set()
        try:
            marker, scanned, copied = self._find_missing_objects()
            if scanned == 0 and marker:
                is_reset = True
                marker, scanned, copied = self._find_missing_objects(marker='')
        except ContainerNotFound as e:
            scanned = 0
            self.logger.error(unicode(e))
        except Exception:
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

    def _create_container(self, container, internal_client, timeout=1):
        if self.config.get('protocol') == 'swift':
            resp = self.provider.head_bucket(container)
            if resp.status == 404:
                raise ContainerNotFound(container)
            if resp.status != 200:
                raise MigrationError('Failed to HEAD bucket/container "%s"' %
                                     container)
            headers = {}
            acl_hdrs = ['x-container-read', 'x-container-write']
            for hdr in resp.headers:
                if hdr.startswith('x-container-meta-') or hdr in acl_hdrs:
                    # Dunno why, really, but the internal client app will 503
                    # with utf8-encoded header values IF the header key is a
                    # unicode instance (even if it's just low ascii chars in
                    # that unicode instance).  Go figure...
                    headers[hdr.encode('utf8')] = \
                        resp.headers[hdr].encode('utf8')
        else:
            headers = {}

        req = Request.blank(
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
                self.logger.debug('Created container "%s"' % container)
                return
        raise MigrationError('Timeout while creating container "%s"' %
                             container)

    def _iter_source_container(
            self, container, marker, prefix, list_all):
        next_marker = marker

        while True:
            status, keys = self.provider.list_objects(
                next_marker, self.work_chunk, prefix, bucket=container)
            if status == 404:
                raise ContainerNotFound(container)
            if status != 200:
                raise MigrationError(
                    'Failed to list source bucket/container "%s"' %
                    self.config['aws_bucket'])
            if not keys and marker and marker == next_marker:
                raise StopIteration
            for key in keys:
                yield key
            if not list_all or not keys:
                break
            next_marker = keys[-1]['name']
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

    def _find_missing_objects(
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

        try:
            source_iter = self._iter_source_container(
                aws_bucket, marker, prefix, list_all)
        except StopIteration:
            source_iter = iter([])

        copied = 0
        scanned = 0
        with self.ic_pool.item() as ic:
            local_iter = ic.iter_objects(self.config['account'],
                                         container,
                                         marker=marker,
                                         prefix=prefix)
            local = next(local_iter, None)
            if not local:
                if not ic.container_exists(self.config['account'],
                                           container):
                    self._create_container(container, ic)

            remote = next(source_iter, None)
            if remote:
                marker = remote['name']
            while remote:
                # NOTE: the listing from the given marker may return fewer than
                # the number of items we should process. We will process all of
                # the keys that were returned in the listing and restart on the
                # following iteration.
                if not local or local['name'] > remote['name']:
                    work = MigrateObjectWork(aws_bucket, container,
                                             remote['name'])
                    self.object_queue.put(work)
                    copied += 1
                    scanned += 1
                    remote = next(source_iter, None)
                    if remote:
                        marker = remote['name']
                elif local['name'] < remote['name']:
                    local = next(local_iter, None)
                else:
                    try:
                        cmp_ret = cmp_object_entries(local, remote)
                    except MigrationError:
                        # This should only happen if we are comparing large
                        # objects: there will be an ETag mismatch.
                        self._check_large_objects(
                            aws_bucket, container, remote['name'], ic)
                    else:
                        if cmp_ret < 0:
                            work = MigrateObjectWork(aws_bucket, container,
                                                     remote['name'])
                            self.object_queue.put(work)
                            copied += 1
                    remote = next(source_iter, None)
                    local = next(local_iter, None)
                    scanned += 1
                    if remote:
                        marker = remote['name']
        return marker, scanned, copied

    def _migrate_object(self, aws_bucket, container, key):
        args = {'bucket': aws_bucket}
        if self.config.get('protocol', 's3') == 'swift':
            args['resp_chunk_size'] = 65536
        resp = self.provider.get_object(key, **args)
        if resp.status != 200:
            resp.body.close()
            raise MigrationError('Failed to GET "%s/%s": %s' % (
                aws_bucket, key, resp.body))
        put_headers = convert_to_local_headers(
            resp.headers.items(), remove_timestamp=False)
        if 'x-object-manifest' in resp.headers and\
                (container, key) not in self._manifests:
            self.logger.warning(
                'Migrating Dynamic Large Object "%s/%s" -- results may not be '
                'consistent' % (container, key))
            resp.body.close()
            self._migrate_dlo(aws_bucket, container, key, put_headers)
        elif 'x-static-large-object' in resp.headers:
            # We have to move the segments and then move the manifest file
            resp.body.close()
            self._migrate_slo(aws_bucket, container, key, put_headers)
        else:
            self._upload_object(
                container, key, FileLikeIter(resp.body), put_headers)

    def _migrate_dlo(self, aws_bucket, container, key, headers):
        dlo_container, prefix = headers['x-object-manifest'].split('/', 1)
        self._manifests.add((container, key))
        self._find_missing_objects(
            dlo_container, dlo_container, '', prefix=prefix, list_all=True)
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
        headers['Content-Length'] = len(manifest_blob)
        work = UploadObjectWork(slo_container, key,
                                FileLikeIter(manifest_blob), headers)
        self.object_queue.put(work)

    def _upload_object(self, container, key, content, headers):
        if 'x-timestamp' in headers:
            headers['x-timestamp'] = Timestamp(
                float(headers['x-timestamp'])).internal
        else:
            ts = datetime.datetime.strptime(headers['last-modified'],
                                            LAST_MODIFIED_FMT)
            ts = Timestamp((ts - EPOCH).total_seconds()).internal
            headers['x-timestamp'] = ts
        del headers['last-modified']
        with self.ic_pool.item() as ic:
            try:
                ic.upload_object(
                    content, self.config['account'], container, key,
                    headers)
                self.logger.debug('Copied "%s/%s"' % (container, key))
            except UnexpectedResponse as e:
                if e.resp.status_int != 404:
                    raise
                self._create_container(container, ic)
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
                if isinstance(work, MigrateObjectWork):
                    aws_bucket = work.aws_bucket
                    container = work.container
                    key = work.key
                    self._migrate_object(aws_bucket, container, key)
                else:
                    self._upload_object(*work)
            except Exception:
                self.errors.put((container, key, sys.exc_info()))
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
            logger.debug('Processing "%s"' % (
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
            logger.debug(msg)
            return
        msg += ', sleeping for %0.2fs.' % naptime
        logger.debug(msg)
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
