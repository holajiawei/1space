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


def equal_migration(left, right):
    key_diff = set(left.keys()) ^ set(right.keys())
    if key_diff:
        if len(key_diff) > 1 or key_diff.pop() != 'status':
            return False
    for k in left.keys():
        if k == 'status':
            continue
        if left[k] != right[k]:
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
    if source['etag'] == dest['etag']:
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

    def get_migration(self, migration):
        if not self.status_list:
            try:
                with open(self.status_location) as fh:
                    self.status_list = json.load(fh)
            except IOError as e:
                if e.errno == errno.ENOENT:
                    self.status_list = []
                else:
                    raise
        for entry in self.status_list:
            if equal_migration(entry, migration):
                return entry.get('status', {})
        return {}

    def save_migration(self, migration, marker, moved_count, scanned_count,
                       stats_reset=False):
        for entry in self.status_list:
            if equal_migration(entry, migration):
                if 'status' not in entry:
                    entry['status'] = {}
                status = entry['status']
                break
        else:
            entry = dict(migration)
            entry['status'] = {}
            self.status_list.append(entry)
            status = entry['status']

        status['marker'] = marker
        _update_status_counts(status, moved_count, scanned_count, stats_reset)
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


class MigrationError(Exception):
    pass


class Migrator(object):
    '''List and move objects from a remote store into the Swift cluster'''
    def __init__(self, config, status, work_chunk, swift_pool, logger,
                 node_id, nodes):
        self.config = dict(config)
        if 'container' not in self.config:
            # NOTE: in the future this may no longer be true, as we may allow
            # remapping buckets/containers during migrations.
            self.config['container'] = self.config['aws_bucket']
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

    def next_pass(self):
        if self.config['aws_bucket'] != '/*':
            self.provider = create_provider(
                self.config, self.max_conns, False)
            self._next_pass()
            return

        self.config['container'] = '.'
        self.provider = create_provider(
            self.config, self.max_conns, False)
        resp = self.provider.list_buckets()
        if not resp.success:
            raise MigrationError(
                'Failed to list source buckets/containers: %s' %
                ''.join(resp.body))
        for index, container in enumerate(resp.body):
            if index % self.nodes == self.node_id:
                # NOTE: we cannot remap container names when migrating all
                # containers
                self.config['aws_bucket'] = container['name']
                self.config['container'] = container['name']
                self.provider.aws_bucket = container['name']
                try:
                    self._next_pass()
                except Exception as e:
                    self.logger.log_error('Failed to migrate %s: %s' % (
                                          container['name'], e))
                    self.logger.log_error(''.join(traceback.format_exc()))

    def _next_pass(self):
        is_reset, keys, local_marker = self._list_source_objects()

        worker_pool = eventlet.GreenPool(self.workers)
        for _ in xrange(self.workers):
            worker_pool.spawn_n(self._upload_worker)
        moved = self._find_missing_objects(keys, local_marker)
        self.object_queue.join()
        self._stop_workers(self.object_queue)

        self.check_errors()
        if not keys:
            return

        marker = keys[-1]['name']
        moved -= self.errors.qsize()
        # TODO: record the number of errors, as well
        self.status.save_migration(
            self.config, marker, moved, len(keys), is_reset)

    def check_errors(self):
        while not self.errors.empty():
            container, key, err = self.errors.get()
            self.logger.error('Failed to migrate %s/%s' % (
                container, key))
            self.logger.error(''.join(traceback.format_exception(*err)))

    def _stop_workers(self, q):
        for _ in range(self.workers):
            q.put(None)
        q.join()

    def _list_source_objects(self):
        state = self.status.get_migration(self.config)
        reset = False
        list_args = {}
        if self.config.get('protocol', 's3') == 's3':
            list_args['native'] = True
        status, keys = self.provider.list_objects(
            state.get('marker', None),
            self.work_chunk,
            self.config.get('prefix'),
            **list_args)
        if status != 200:
            raise MigrationError(
                'Failed to list source bucket/container "%s"' %
                self.config['aws_bucket'])
        if not keys and state.get('marker'):
            reset = True
            status, keys = self.provider.list_objects(
                None, self.work_chunk, self.config.get('prefix'),
                **list_args)
            if status != 200:
                raise MigrationError(
                    'Failed to list source bucket/container "%s"' %
                    self.config['aws_bucket'])
        if not reset:
            local_marker = state.get('marker', '')
        else:
            local_marker = ''
        return reset, keys, local_marker

    def _create_container(self, container, internal_client, timeout=1):
        if self.config.get('protocol') == 'swift':
            resp = self.provider.head_bucket(container)
            if resp.status != 200:
                raise MigrationError('Failed to HEAD bucket/container %s' %
                                     container)
            headers = {}
            acl_hdrs = ['x-container-read', 'x-container-write']
            for hdr in resp.headers:
                if hdr.startswith('x-container-meta-') or hdr in acl_hdrs:
                    headers[hdr] = resp.headers[hdr]
        else:
            headers = {}

        req = Request.blank(
            internal_client.make_path(self.config['account'], container),
            environ={'REQUEST_METHOD': 'PUT',
                     'swift_owner': True},
            headers=headers)

        resp = req.get_response(internal_client.app)
        if resp.status_int // 100 != 2:
            raise UnexpectedResponse('Failed to create container %s: %d' % (
                container, resp.status_int), resp)

        start = time.time()
        while time.time() - start < timeout:
            if not internal_client.container_exists(
                    self.config['account'], container):
                time.sleep(0.1)
            else:
                self.logger.debug('Created container %s' % container)
                return
        raise MigrationError('Timeout while creating container %s' % container)

    def _find_missing_objects(self, source_list, marker):
        moved = 0
        with self.ic_pool.item() as ic:
            try:
                local_iter = ic.iter_objects(self.config['account'],
                                             self.config['container'],
                                             marker=marker)
                local = next(local_iter, None)
                if not local:
                    if not ic.container_exists(self.config['account'],
                                               self.config['container']):
                        self._create_container(self.config['container'], ic)
            except UnexpectedResponse as e:
                if e.resp.status_int != 404:
                    raise
                self._create_container(self.config['container'], ic)

            index = 0
            while index < len(source_list):
                # NOTE: the listing from the given marker may return fewer than
                # the number of items we should process. We will process all of
                # the keys that were returned in the listing and restart on the
                # following iteration.
                if not local or local['name'] > source_list[index]['name']:
                    self.object_queue.put((
                        self.config['aws_bucket'], source_list[index]['name']))
                    index += 1
                    moved += 1
                elif local['name'] < source_list[index]:
                    local = next(local_iter, None)
                else:
                    cmp_ret = cmp_object_entries(local, source_list[index])
                    if cmp_ret < 0:
                        self.object_queue.put((self.config['aws_bucket'],
                                               source_list[index]['name']))
                        moved += 1
                    local = next(local_iter, None)
                    index += 1
        return moved

    def _migrate_object(self, container, key):
        args = {'bucket': container, 'native': True}
        if self.config.get('protocol', 's3') == 'swift':
            args['resp_chunk_size'] = 65536
        resp = self.provider.get_object(key, **args)
        if resp.status != 200:
            raise MigrationError('Failed to GET %s/%s: %s' % (
                container, key, resp.body))
        put_headers = convert_to_local_headers(
            resp.headers.items(), remove_timestamp=False)
        if 'x-object-manifest' in resp.headers:
            self.logger.warning('Skipping Dynamic Large Object %s/%s' % (
                container, key))
            resp.body.close()
            return
        if 'x-static-large-object' in resp.headers:
            # We have to move the segments and then move the manifest file
            resp.body.close()
            self._migrate_slo(container, key, put_headers)
        else:
            self._upload_object(
                container, key, FileLikeIter(resp.body), put_headers)

    def _migrate_slo(self, slo_container, key, headers):
        manifest = self.provider.get_manifest(key, slo_container)
        if not manifest:
            raise MigrationError('Failed to fetch the manifest for %s/%s' % (
                                 slo_container, key))
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
                    segment_key, container, native=True)
                if resp.status != 200:
                    raise MigrationError('Failed to HEAD %s/%s' % (
                        container, segment_key))
                src_meta = resp.headers
                if self.config.get('protocol', 's3') != 'swift':
                    src_meta = convert_to_swift_headers(src_meta)
                ret = cmp_meta(meta, src_meta)
                if ret == EQUAL:
                    continue
                if ret == TIME_DIFF:
                    # TODO: update metadata
                    self.logger.warning('Object metadata changed for %s/%s' % (
                        container, segment_key))
                    continue
            self.object_queue.put((container, segment_key))
        manifest_blob = json.dumps(manifest)
        headers['Content-Length'] = len(manifest_blob)
        self.object_queue.put((
            slo_container, key, FileLikeIter(manifest_blob), headers))

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
                self.logger.debug('Copied %s/%s' % (container, key))
            except UnexpectedResponse as e:
                if e.resp.status_int != 404:
                    raise
                self._create_container(container, ic)
                ic.upload_object(
                    content, self.config['account'], container, key,
                    headers)
                self.logger.debug('Copied %s/%s' % (container, key))

    def _upload_worker(self):
        while True:
            work = self.object_queue.get()
            try:
                if not work:
                    break
                if len(work) == 2:
                    container, key = work
                    self._migrate_object(container, key)
                else:
                    self._upload_object(*work)
            except Exception:
                self.errors.put((container, key, sys.exc_info()))
            finally:
                self.object_queue.task_done()


def process_migrations(migrations, migration_status, internal_pool, logger,
                       items_chunk, node_id, nodes):
    for index, migration in enumerate(migrations):
        if migration['aws_bucket'] == '/*' or index % nodes == node_id:
            try:
                if migration.get('remote_account'):
                    src_account = migration.get('remote_account')
                else:
                    src_account = migration['aws_identity']
                logger.debug('Processing %s' % (
                    ':'.join([migration.get('aws_endpoint', ''),
                              src_account, migration['aws_bucket']])))
                migrator = Migrator(migration, migration_status,
                                    items_chunk,
                                    internal_pool, logger,
                                    node_id, nodes)
                migrator.next_pass()
            except Exception as e:
                logger.error('Migration error: %r\n%s' % (
                    e, traceback.format_exc(e)))


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
    migration_status = Status(migrator_conf['status_file'])

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

    run(migrations, migration_status, internal_pool, logger, items_chunk,
        node_id, nodes, poll_interval, args.once)


if __name__ == '__main__':
    main()
