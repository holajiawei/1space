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
import time
import traceback
import elasticsearch
import elasticsearch.helpers
import email.utils
import hashlib

from distutils.version import StrictVersion
from .daemon_utils import load_swift, setup_context, setup_logger
from .utils import (convert_to_local_headers, SWIFT_TIME_FMT,
                    get_container_headers, RemoteHTTPError,
                    get_sys_migrator_header,
                    MigrationContainerStates,
                    get_local_versioning_headers,
                    _propagated_hdr,
                    SYSMETA_VERSIONS_LOC, SYSMETA_VERSIONS_MODE)

from swift.common.http import HTTP_NOT_FOUND, HTTP_CONFLICT
from swift.common.internal_client import UnexpectedResponse
from swift.common.utils import Timestamp
from .migrator import (UploadObjectWork, MigrationError,
                       ContainerNotFound, _create_x_timestamp_from_hdrs,
                       Status, Migrator)
from swift_metadata_sync.metadata_sync import MetadataSync
from .sync_s3 import SyncS3

LOGGER_NAME = 'swift-metadata-migrator'


def diff_container_headers(remote_headers, local_headers):
    # remote_headers are unicode, local are str returns str
    rem_headers = dict([(k.encode('utf8'), v.encode('utf8')) for k, v in
                        remote_headers.items() if _propagated_hdr(k)])
    rem_headers.update(get_local_versioning_headers(remote_headers))

    matching_keys = set(rem_headers.keys()).intersection(local_headers.keys())
    missing_remote_keys = set(
        local_headers.keys()).difference(rem_headers.keys())

    missing_local_keys = set(
        rem_headers.keys()).difference(local_headers.keys())

    # TODO: we can probably sink some of these checks into the
    # _propagated_hdr() function.
    versioning_headers = [SYSMETA_VERSIONS_LOC, SYSMETA_VERSIONS_MODE]
    return dict([(k, rem_headers[k])
                 for k in matching_keys if
                 rem_headers[k] != local_headers.get(k)] +
                [(k, rem_headers[k]) for k in missing_local_keys] +
                [(k, None) for k in missing_remote_keys
                 if _propagated_hdr(k) or k in versioning_headers])


class MetadataMigratorConfigError(Exception):
    pass


class MetadataMigrator(Migrator):
    '''List and index object metadata from a remote store into an Elasticsearch
    cluster'''

    def __init__(self, *args, **kwargs):
        super(MetadataMigrator, self).__init__(*args, **kwargs)

    def _head_internal_account(self, internal_client):
        return

    def _process_account_metadata(self):
        return

    def _update_container_headers(self, container, internal_client, headers):
        req = internal_client.set_container_metadata(
            self.config, self.config['account'], container, headers)

        resp = req.get_response()
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

        req = internal_client.create_container(self.config,
                                               self.config['account'],
                                               container, headers)
        resp = req.get_response()
        if resp.status_int // 100 != 2:
            raise UnexpectedResponse('Failed to create container "%s": %d' % (
                container, resp.status_int), resp)

        start = time.time()
        while time.time() - start < timeout:
            if not internal_client.container_exists(
                    self.config,
                    self.config['account'], container):
                time.sleep(0.1)
            else:
                self.logger.info('Created container "%s"' % container)
                return
        raise MigrationError('Timeout while creating container "%s"' %
                             container)

    def _iterate_internal_listing(
            self, container=None, marker='', prefix=None):
        '''Iterate Elasticsearch indices as if they were Swift containers, or
        iterate Elasticsearch documents as if they were objects'''

        while True:
            with self.ic_pool.item() as ic:
                return ic.es_iter_items(self.config, self.config['account'],
                                        container, marker, prefix)

    def _reconcile_deleted_objects(self, container, key):
        # NOTE: to handle the case of objects being deleted from the source
        # cluster after they've been migrated, we have to HEAD the object to
        # check for the migration header.
        with self.ic_pool.item() as ic:
            try:
                hdrs = ic.get_object_metadata(self.config,
                                              self.config['account'],
                                              container, key)
            except UnexpectedResponse as e:
                # This may arise if there an eventual consistency issue between
                # the container database and the object server state.
                if e.resp.status_int == HTTP_NOT_FOUND:
                    return
                raise
            if get_sys_migrator_header('object') in hdrs:
                headers = {}
                xts = hdrs.get('x-backend-durable-timestamp') or \
                    hdrs.get('x-backend-timestamp') or hdrs.get('x-timestamp')
                if xts:
                    ts = Timestamp(xts)
                    headers['x-timestamp'] = Timestamp(
                        ts.timestamp, ts.offset + 1).internal
                else:
                    xts = _create_x_timestamp_from_hdrs(hdrs)
                    if xts:
                        headers['x-timestamp'] = Timestamp(xts, 1)
                try:
                    ic.delete_object(self.config, self.config['account'],
                                     container, key, headers)
                    self.logger.info(
                        'Detected removed object %s. Removing from %s/%s' % (
                            key, self.config['account'], container))
                except UnexpectedResponse as e:
                    if e.resp.status_int == HTTP_CONFLICT:
                        self.logger.info(
                            'Conflict removing object %s from %s/%s' % (
                                key, self.config['account'], container))
                        return
                    raise

    def _maybe_delete_internal_container(self, container):
        '''Delete a specified internal container.

        Unfortunately, we cannot simply DELETE every object in the container,
        but have to issue a HEAD request to make sure the migrator header is
        not set. This makes clearing containers expensive and we hope that this
        is not a common operation.
        '''
        try:
            with self.ic_pool.item() as ic:
                headers = ic.get_container_metadata(self.config,
                                                    self.config['account'],
                                                    container)
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
            self._reconcile_deleted_objects(container, obj['name'])

        state_meta = {get_sys_migrator_header('container'):
                      MigrationContainerStates.SRC_DELETED}
        with self.ic_pool.item() as ic:
            if state == MigrationContainerStates.MIGRATING:
                try:
                    ic.delete_container(self.config,
                                        self.config['account'], container)
                except UnexpectedResponse as e:
                    if e.resp.status_int == HTTP_CONFLICT:
                        # NOTE: failing to DELETE the container is OK if there
                        # are objects in it. It means that there were write
                        # operations outside of the migrator.
                        ic.set_container_metadata(self.config,
                                                  self.config['account'],
                                                  container, state_meta)
            else:
                ic.set_container_metadata(self.config,
                                          self.config['account'],
                                          container, state_meta)

    # We only propagate metadata for large object manifests, not object
    # segments.
    def _check_large_objects(self, aws_bucket, container, key, client):
        return

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
        # The metadata migrator makes no special allowances for history
        # (be it x-versions-location or x-history-location).
        if self.config.get('protocol') == 'swift':
            resp = self.provider.head_bucket(aws_bucket)
            if resp.status == 404:
                raise ContainerNotFound(
                    self.config['aws_identity'], aws_bucket)
            if resp.status != 200:
                raise MigrationError(
                    'Failed to HEAD bucket/container "%s"' % container)
            local_headers = None
            with self.ic_pool.item() as ic:
                try:
                    local_headers = ic.get_container_metadata(
                        self.config, self.config['account'], container)
                except UnexpectedResponse as e:
                    if e.resp.status_int == HTTP_NOT_FOUND:
                        # TODO: this makes one more HEAD request to fetch
                        # headers. We should re-use resp.headers here
                        # (appropriately converted to handle versioning)
                        self._create_container(container, ic, aws_bucket)
                    else:
                        raise
                if resp.headers and local_headers:
                    local_ts = _create_x_timestamp_from_hdrs(
                        local_headers, use_x_timestamp=False)
                    remote_ts = _create_x_timestamp_from_hdrs(
                        resp.headers, use_x_timestamp=False)
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
                if not ic.container_exists(self.config,
                                           self.config['account'], container):
                    self._create_container(container, ic, aws_bucket)

        return self._find_missing_objects(container, aws_bucket, marker,
                                          prefix, list_all)

    def _migrate_object(self, aws_bucket, container, key):
        args = {'bucket': aws_bucket}
        resp = self.provider.head_object(key, **args)
        if resp.status != 200:
            resp.body.close()
            raise MigrationError('Failed to HEAD "%s/%s": %s %s' % (
                aws_bucket, key, resp.body))

        put_headers = convert_to_local_headers(
            resp.headers.items(), remove_timestamp=False)
        work = UploadObjectWork(
            container, key, None, put_headers,
            aws_bucket)
        self._upload_object(work)

    def _upload_object(self, work):
        container, key, content, headers, aws_bucket = work
        headers['x-timestamp'] = Timestamp(
            _create_x_timestamp_from_hdrs(headers)).internal
        headers[get_sys_migrator_header('object')] = headers['x-timestamp']
        with self.ic_pool.item() as ic:
            try:
                ic.upload_object(self.config,
                                 content, self.config['account'],
                                 container, key, headers)
                self.logger.debug('Copied "%s/%s"' % (container, key))
            except UnexpectedResponse as e:
                if e.resp.status_int != 404:
                    raise
                self._create_container(container, ic, aws_bucket)
                ic.upload_object(self.config,
                                 content, self.config['account'],
                                 container, key, headers)
                self.logger.debug('Copied "%s/%s"' % (container, key))
        self.gthread_local.uploaded_objects += 1
        self.gthread_local.bytes_copied += 0


def process_migrations(migrations, migration_status, internal_pool, logger,
                       items_chunk, workers, node_id, nodes):
    handled_containers = []
    for index, migration in enumerate(migrations):
        if migration['aws_bucket'] == '/*' or index % nodes == node_id:
            # If 'all buckets' is specified, we handle index creation ourselves
            # and only allow the user to specify an index prefix.
            if 'index_prefix' not in migration:
                raise MetadataMigratorConfigError(
                    "Config error: index_prefix" +
                    "not provided. When indexing all remote buckets, you " +
                    "must provide an index prefix to use when creating " +
                    "Elasticsearch indices.")
            if migration.get('remote_account'):
                src_account = migration.get('remote_account')
            else:
                src_account = migration['aws_identity']
            logger.info('Processing "%s"' % (
                ':'.join([migration.get('aws_endpoint', ''),
                          src_account, migration['aws_bucket']])))
            migrator = MetadataMigrator(migration, migration_status,
                                        items_chunk, workers,
                                        internal_pool, logger,
                                        node_id, nodes)
            pass_containers = migrator.next_pass()
            if pass_containers is None:
                # Happens if there is an error listing containers.
                # Inserting the migration we attempted to process will ensure
                # we don't prune it (or the related containers).
                handled_containers.append(migration)
            else:
                handled_containers += pass_containers
            migrator.close()
    migration_status.prune(handled_containers)


def run(metadata_migrations, migration_status, internal_pool, logger,
        items_chunk, workers, node_id, nodes, poll_interval, once):
    while True:
        cycle_start = time.time()
        process_migrations(metadata_migrations, migration_status,
                           internal_pool, logger,
                           items_chunk, workers, node_id, nodes)
        elapsed = time.time() - cycle_start
        naptime = max(0, poll_interval - elapsed)
        msg = 'Finished cycle in %0.2fs' % elapsed
        if once:
            logger.info(msg)
            return
        msg += ', sleeping for %0.2fs.' % naptime
        logger.info(msg)
        time.sleep(naptime)


def create_ic_pool(config, swift_dir, workers, logger):
    return eventlet.pools.Pool(
        create=lambda: create_es_client(config, swift_dir, logger),
        min_size=0,
        max_size=workers + 1)  # Our enumerating thread uses a client as well.


def create_es_client(config, swift_dir, logger):
    return ESInternalClient(config, swift_dir, logger)


class ESUnexpectedResponse(UnexpectedResponse):
    def __init__(self, response):
        self.response = self.resp = response

    def __repr__(self):
        return repr(self.response)

    def __str__(self):
        return str(self.response)


class ESInternalClientResponse(object):
    def __init__(self, status_int, headers={}, body={}):
        self.status_int = status_int
        self.headers = headers
        self.body = body

    def __repr__(self):
        return str(self)

    def __str__(self):
        return "ESInternalClientResponse: status %s, headers %s, body %s" % (
            self.status_int, self.headers, self.body)


class ESInternalClientRequest(object):
    def __init__(self, response_status_int, response_headers={},
                 response_body={}):
        self.response = ESInternalClientResponse(response_status_int,
                                                 response_headers,
                                                 response_body)

    def __repr__(self):
        return str(self)

    def __str__(self):
        return "ESInternalClientRequest: Response:  %s" % str(self.response)

    def get_response(self):
        return self.response


class ESInternalClient(object):
    '''Provide a swift internal-client-like Elasticsearch connection'''

    DOC_TYPE = MetadataSync.DOC_TYPE
    DOC_MAPPING = MetadataSync.DOC_MAPPING
    USER_META_PREFIX = MetadataSync.USER_META_PREFIX
    INDEX_MAX_LENGTH = 255
    INDEX_INVALID_CHARS = ['#', "\\" '/', '*', '?', '"', '<', '>', '|']
    SWIFT_PAGINATION_LIMIT = 10000
    _es_conns = {}

    def __init__(self, config, swift_dir, logger):
        self.config = dict(config)
        self._verified_indices = {}
        self.logger = logger

    def __str__(self):
        return str(self.config)

    def _verifies_mapping(fn):
        def wrapper(self, config, account, container, *args, **kwargs):
            index = self._get_es_index(config, account, container)
            index_pattern = "%s:%s" % (config['es_hosts'], index)
            try:
                self._verified_indices[index_pattern]
            except KeyError:
                self._verify_mapping(config, index)
                self._verified_indices[index_pattern] = True
            return fn(self, config, account, container, *args, **kwargs)
        return wrapper

    def make_request(
            self, method, path, headers, acceptable_statuses, body_file=None,
            params=None):
        raise NotImplementedError

    def get_object_metadata(self, config, account, container, key):
        _id = self._get_document_id(account, container, key)

        try:
            resp = self._get_es_conn(config).get(
                index=self._get_es_index(config, account, container),
                doc_type=self.DOC_TYPE,
                id=_id
            )
        except Exception as e:
            raise ESUnexpectedResponse(self._es_exception_as_response(e))

        self.logger.debug("ESInternalClient: Retrieved ES document " +
                          "metadata %s for %s/%s" % (
                              resp, self._get_es_index(
                                  config, account, container), key))
        return self._es_to_object_store(resp)

    def delete_object(self, config, account, container, key, headers={}):
        _id = self._get_document_id(account, container, key)

        try:
            self._get_es_conn(config).delete(
                index=self._get_es_index(config, account, container),
                doc_type=self.DOC_TYPE,
                id=_id)
        except Exception as e:
            raise ESUnexpectedResponse(self._es_exception_as_response(e))

        self.logger.debug("ESInternalClient: Deleted ES document %s/%s" % (
            self._get_es_index(config, account, container), key))
        return True

    def upload_object(self, config, content, account, container, key, headers):
        try:
            self._create_index_op(config, account, container, key, headers)
        except Exception as e:
            raise ESUnexpectedResponse(self._es_exception_as_response(e))
        self.logger.debug("ESInternalClient: Indexed ES document %s/%s" % (
            self._get_es_index(config, account, container), key))
        return True

    def create_container(self, config, account, container, headers={}):
        properties = dict([(k, v) for k, v in
                          ESInternalClient.DOC_MAPPING.items()])
        if self._get_server_version(config) >= StrictVersion('5.0'):
            properties = dict([(k, self._update_string_mapping(v))
                              for k, v in properties.items()])
        new_mapping = {
            'mappings': {
                self.DOC_TYPE: {
                    'properties': properties,
                    '_meta': headers
                }
            }
        }

        try:
            self._get_es_conn(config).indices.create(
                index=self._get_es_index(config, account, container),
                body=new_mapping)
        except Exception as e:
            return self._es_exception_as_request(e)

        self.logger.debug("ESInternalClient: Created ES index %s" % (
            self._get_es_index(config, account, container)))
        return ESInternalClientRequest(200)

    def container_exists(self, config, account, container):
        try:
            return bool(self._get_es_conn(config).indices.exists(
                index=self._get_es_index(config, account, container)))
        except Exception as e:
            raise ESUnexpectedResponse(self._es_exception_as_response(e))

    def get_container_metadata(self, config, account, container):
        try:
            mappings = self._get_es_conn(config).indices.get_mapping(
                index=self._get_es_index(config, account, container))
            self.logger.debug("ESInternalClient: Retrieved metadata mapping " +
                              " %s for ES index %s" % (
                                  mappings, self._get_es_index(
                                      config, account, container)))
            return (mappings[self._get_es_index(config, account, container)]
                            ['mappings'][self.DOC_TYPE].get('_meta', {}))
        except Exception as e:
            raise ESUnexpectedResponse(self._es_exception_as_response(e))

    def delete_container(self, config, account, container):
        try:
            self._get_es_conn(config).indices.delete(
                index=self._get_es_index(config, account, container))
            self.logger.debug("ESInternalClient: Deleted ES index %s" % (
                self._get_es_index(config, account, container)))
            return ESInternalClientRequest(200)
        except Exception as e:
            raise ESUnexpectedResponse(self._es_exception_as_response(e))

    # Replaces, rather than updates, the _meta properties for a given index
    def set_container_metadata(self, config, account, container, headers):
        try:
            mappings = self._get_es_conn(config).indices.get_mapping(
                index=self._get_es_index(config, account, container),
                doc_type=self.DOC_TYPE)
        except Exception as e:
            return self._es_exception_as_request(e)

        cleaned_headers = {k: v for (k, v) in headers.items()
                           if v is not None}
        mapping = (mappings[self._get_es_index(config, account, container)]
                   ['mappings'][self.DOC_TYPE])
        mapping['_meta'] = cleaned_headers

        try:
            self._get_es_conn(config).indices.put_mapping(
                index=self._get_es_index(config, account, container),
                doc_type=self.DOC_TYPE,
                body=mapping)
            self.logger.debug("ESInternalClient: Updated meta for index " +
                              "%s with %s" % (
                                  self._get_es_index(
                                      config, account, container), headers))
            return ESInternalClientRequest(200)
        except Exception as e:
            return self._es_exception_as_request(e)

    def set_account_metadata(self, account, headers):
        return ESInternalClientRequest(200)

    def _get_es_conn(self, config, item='conn'):
        try:
            return ESInternalClient._es_conns[config['es_hosts']][item]
        except KeyError:
            verify_certs = config.get('verify_certs', True)
            ca_certs = config.get('ca_certs')
            es_conn = elasticsearch.Elasticsearch(
                config['es_hosts'],
                verify_certs=verify_certs,
                ca_certs=ca_certs
            )
            server_version = StrictVersion(
                es_conn.info()['version']['number'])
            ESInternalClient._es_conns[config['es_hosts']] = {
                'conn': es_conn,
                'server_version': server_version}

            self.logger.debug("ESInternalClient: Connected to ES at " +
                              " %s (server version %s" %
                              (config['es_hosts'], server_version))

            return es_conn

    def _get_server_version(self, config):
        return self._get_es_conn(config, item='server_version')

    def _get_es_index(self, config, account, container):
        return self._get_es_index_prefix(config, account, container=container)

    def _get_es_index_prefix(self, config, account, container=''):
        try:
            index_prefix = config['index_prefix']
        except KeyError:
            return self._maybe_decode(config['index'])

        index = "%s%s_%s" % (
            self._maybe_decode(index_prefix),
            self._maybe_decode(account),
            self._maybe_decode(container))
        index = index.lower()
        for i in self.INDEX_INVALID_CHARS:
            index = index.replace(i, '_')
        if len(index) > self.INDEX_MAX_LENGTH:
            index = (index[:self.INDEX_MAX_LENGTH])

        return self._maybe_decode(index)

    def _es_exception_as_response(self, exception):
        return self._es_exception_as_request(exception).response

    def _es_exception_as_request(self, exception):
        if type(exception) is elasticsearch.ImproperlyConfigured:
            return ESInternalClientRequest(
                400, {}, "Elasticsearch client is improperly configured")
        elif type(exception) is elasticsearch.SerializationError:
            return ESInternalClientRequest(
                422, {}, "Client passed improperly serialized data")
        elif type(exception) is elasticsearch.TransportError:
            if exception.status_code is not 'N/A':
                return ESInternalClientRequest(
                    exception.status_code, {}, exception.error)
            else:
                return ESInternalClientRequest(
                    502, {}, "Could not connect to Elasticsearch")
        elif type(exception) is elasticsearch.ConnectionError:
            return ESInternalClientRequest(
                502, {}, "Connection to Elasticsearch failed: %s" % (
                    str(exception)))
        elif type(exception) is elasticsearch.ConnectionTimeout:
            return ESInternalClientRequest(
                502, {}, "Connection to Elasticsearch timed out: %s" % (
                    str(exception)))
        elif type(exception) is elasticsearch.SSLError:
            return ESInternalClientRequest(
                502, {}, "TLS Error communicating with Elasticsearch: %s" % (
                    str(exception)))
        elif type(exception) is elasticsearch.NotFoundError:
            return ESInternalClientRequest(
                404, {}, "Entity not found in Elasticsearch")
        elif type(exception) is elasticsearch.ConflictError:
            return ESInternalClientRequest(
                409, {}, "Elasticsearch replied with 409 conflict")
        elif type(exception) is elasticsearch.RequestError:
            return ESInternalClientRequest(
                400, {}, "Elasticsearch replied with 400 bad request")
        else:
            return ESInternalClientRequest(
                502, {}, "Unknown Elasticsearch error: %s" % (
                    str(exception)))

    def es_iter_items(self, config, account, container=None, marker=None,
                      prefix=None):
        if container is None:
            return self._es_iter_containers(config, account, marker, prefix)
        else:
            return self._es_iter_objects(config, account, container,
                                         marker, prefix)

    def _es_iter_containers(self, config, account, marker=None, _prefix=''):
        if _prefix is None:
            prefix = ''

        indices = None
        while True:
            index_prefix = "%s%s*" % (self._get_es_index_prefix(
                                      config, account), prefix)
            if not indices:
                try:
                    indices = sorted(self._get_es_conn(config)
                                         .indices.get("%s" % (
                                             index_prefix)).keys())
                except Exception as e:
                    raise ESUnexpectedResponse(
                        self._es_exception_as_response(e))

            if not indices:
                break

            index = 0
            if marker:
                try:
                    index = indices.index(marker) + 1
                except ValueError:
                    pass
                try:
                    indices[index]
                except IndexError:
                    break

            for entry in indices[index:index + self.SWIFT_PAGINATION_LIMIT]:
                marker = entry
                yield {'name': entry[len(index_prefix) - 1:]}

        yield None

    def _es_iter_objects(self, config, account, container=None, marker=None,
                         prefix=None):
        while True:
            body = {
                "sort": [{"x-swift-object.keyword": {"order": "asc"}}],
                "query": {
                    "bool": {
                        "filter": [
                            {"match": {"x-swift-account":
                                       account.encode('utf-8')}},
                            {"match": {"x-swift-container":
                                       container.encode('utf-8')}},
                            {"exists": {"field": "x-swift-object"}}
                        ]
                    }
                }
            }

            if marker:
                body['search_after'] = [marker]
            self.logger.debug("Searching, search_after marker is %s" % marker)

            try:
                resp = self._get_es_conn(config).search(
                    index=self._get_es_index(config, account, container),
                    doc_type=self.DOC_TYPE,
                    size=self.SWIFT_PAGINATION_LIMIT,
                    body=body)
            except Exception as e:
                self.logger.error("ES document iteration for index %s " %
                                  self._get_es_index(
                                      config, account, container) +
                                  " raised exception %s" % e)
                break

            if not resp:
                break

            if 'hits' not in resp:
                break

            if 'hits' not in resp['hits']:
                break

            if not resp['hits']['hits']:
                break

            for entry in resp['hits']['hits']:
                yield self._es_to_object_store(entry)

            marker = resp['hits']['hits'][-1]['sort'][0].encode('utf-8')
        yield None

    def _es_to_object_store(self, item):
        n = {
            'hash': item['_source']['etag'],
            'name': item['_source']['x-swift-object'],
            'content_type': item['_source']['content-type'],
            'bytes': item['_source']['content-length']}

        if 'last-modified' in item['_source']:
            n['last_modified'] = datetime.datetime.utcfromtimestamp(
                int(int(item['_source']['last-modified']) / 1000)
            ).strftime(SWIFT_TIME_FMT)

        if get_sys_migrator_header('object') in item['_source']:
            n[get_sys_migrator_header('object')] = \
                item['_source'][get_sys_migrator_header('object')]

        if 'content-location' in item['_source']:
            n['content-location'] = item['_source']['content-location']

        return n

    def _maybe_decode(self, string):
        _string = string if isinstance(
            string, unicode) else string.decode('utf-8')
        return _string

    def _maybe_encode(self, string):
        _string = string if not isinstance(
            string, unicode) else string.encode('utf-8')
        return _string

    def _make_content_location(self, config):
        if config.get('protocol', '') == 's3':
            location_prefix = 'AWS S3'
            if config.get('aws_endpoint', '') == SyncS3.GOOGLE_API:
                location_prefix = 'Google Cloud Storage'
            location_parts = [location_prefix, config.get('aws_bucket', '')]
            location_parts.append(config.get('prefix', ''))
            content_location = ';'.join(location_parts)
        else:
            u_ident = config['aws_identity'] if isinstance(
                config['aws_identity'], unicode) else \
                config['aws_identity'].decode('utf8')
            endpoint = config.get('endpoint', '')
            if not endpoint:
                endpoint = 'swift'
            content_location = '%s;%s;%s' % (
                endpoint,
                u_ident, config.get('aws_bucket', ''))
        return content_location

    @_verifies_mapping
    def _create_index_op(self, config, account, container, key, headers):
        meta = headers
        meta['content-location'] = self._make_content_location(config)

        op = self._get_es_conn(config).index(
            index=self._get_es_index(config, account, container),
            doc_type=self.DOC_TYPE,
            body=self._create_es_doc(meta, account,
                                     container,
                                     self._maybe_decode(key),
                                     config.get('parse_json', False)),
            id=self._get_document_id(account, container, key),
            pipeline=config.get('pipeline', None))

        self.logger.debug("ESInternalClient: Created ES document %s/%s" % (
            self._get_es_index(config, account, container), key))

        return op

    def _get_document_id(self, account, container, key):
        ret = hashlib.sha256(
            '/'.join(
                [account.encode('utf-8'),
                 container.encode('utf-8'),
                 key.encode('utf-8')])).hexdigest()
        return ret

    """
        Verify document mapping for the elastic search index. Does not include
        any user-defined fields.
    """
    def _verify_mapping(self, config, index):
        index_client = self._get_es_conn(config).indices

        try:
            mapping = index_client.get_mapping(
                index=index,
                doc_type=self.DOC_TYPE)
        except elasticsearch.TransportError as e:
            if e.status_code != 404:
                raise
            if e.error != 'type_missing_exception':
                raise
            mapping = {}
        if not mapping.get(index, None) or \
                self.DOC_TYPE not in (
                    mapping[index]['mappings']):
            missing_fields = self.DOC_MAPPING.keys()
        else:
            current_mapping = (mapping[index]
                               ['mappings']
                               [self.DOC_TYPE]['properties'])
            # We are not going to force re-indexing, so won't be checking the
            # mapping format
            missing_fields = [key for key in self.DOC_MAPPING.keys()
                              if key not in current_mapping]
        if missing_fields:
            new_mapping = dict([(k, v) for k, v in self.DOC_MAPPING.items()
                                if k in missing_fields])
            # Elasticsearch 5.x deprecated the "string" type. We convert the
            # string fields into the appropriate 5.x types.
            # TODO: Once we remove  support for the 2.x clusters, we should
            # remove this code and create the new mappings for each field.
            if self._get_server_version(config) >= StrictVersion('5.0'):
                new_mapping = dict([(k, self._update_string_mapping(v))
                                    for k, v in new_mapping.items()])
            index_client.put_mapping(index=index, doc_type=self.DOC_TYPE,
                                     body={'properties': new_mapping})
        self.logger.debug("ESInternalClient: Verified ES mapping for " +
                          "index %s" % index)

    @staticmethod
    def _update_string_mapping(mapping):
        if mapping['type'] != 'string':
            return mapping
        if 'index' in mapping and mapping['index'] == 'not_analyzed':
            return {'type': 'keyword'}
        # This creates a mapping that is both searchable as a text and keyword
        # (the default  behavior in Elasticsearch for 2.x string types).
        return {
            'type': 'text',
            'fields': {
                'keyword': {
                    'type': 'keyword'}
            }
        }

    @staticmethod
    def _create_es_doc(_meta, account, container, key, parse_json=False):
        def _parse_document(value):
            try:
                return json.loads(value.decode('utf-8'))
            except ValueError:
                return value.decode('utf-8')

        # Content-Length ends up capitalised
        meta = {k.lower(): v for k, v in _meta.items()}

        es_doc = {}
        # ElasticSearch only supports millisecond resolution;
        # we also add the migrator header to our index
        es_doc[get_sys_migrator_header('object')] = \
            es_doc['x-timestamp'] = \
            int(float(meta['x-timestamp']) * 1000)

        # Convert Last-Modified header into a millis since epoch date
        ts = email.utils.mktime_tz(
            email.utils.parsedate_tz(meta['last-modified'])) * 1000
        es_doc['last-modified'] = ts
        es_doc['x-swift-object'] = key
        es_doc['x-swift-account'] = account
        es_doc['x-swift-container'] = container

        if 'content-location' in meta:
            es_doc['content-location'] = meta['content-location']

        user_meta_keys = dict(
            [(k.split(ESInternalClient.USER_META_PREFIX, 1)[1].decode('utf-8'),
              _parse_document(v) if parse_json else v.decode('utf-8'))
             for k, v in meta.items()
             if k.startswith(ESInternalClient.USER_META_PREFIX)])
        es_doc.update(user_meta_keys)
        for field in ESInternalClient.DOC_MAPPING.keys():
            if field in es_doc:
                continue
            if field not in meta:
                continue
            if ESInternalClient.DOC_MAPPING[field]['type'] == 'boolean':
                es_doc[field] = str(meta[field]).lower()
                continue
            es_doc[field] = meta[field]
        return es_doc


def main():
    args, conf = setup_context(
        description='Daemon to migrate object metadata into Elasticsearch')
    if 'metadata_migrator_settings' not in conf:
        print 'Missing metadata migrator settings section'
        exit(-1)

    migrator_conf = conf['metadata_migrator_settings']
    if 'status_file' not in migrator_conf:
        print 'Missing status file location!'
        exit(-1 * errno.ENOENT)

    if args.log_level:
        migrator_conf['log_level'] = args.log_level
    migrator_conf['console'] = args.console

    handler = setup_logger(LOGGER_NAME, migrator_conf)
    logger = logging.getLogger('s3_sync')
    logger.addHandler(handler)

    load_swift(LOGGER_NAME, args.once)

    logger = logging.getLogger(LOGGER_NAME)

    workers = migrator_conf.get('workers', 10)
    swift_dir = conf.get('swift_dir', '/etc/swift')
    internal_pool = create_ic_pool(conf, swift_dir, workers, logger)

    if 'process' not in migrator_conf or 'processes' not in migrator_conf:
        print 'Missing "process" or "processes" settings in the config file'
        exit(-1)

    items_chunk = migrator_conf['items_chunk']
    node_id = int(migrator_conf['process'])
    nodes = int(migrator_conf['processes'])
    poll_interval = float(migrator_conf.get('poll_interval', 5))

    migrations = conf.get('metadata_migrations', [])
    migration_status = Status(migrator_conf['status_file'])
    run(migrations, migration_status, internal_pool, logger, items_chunk,
        workers, node_id, nodes, poll_interval, args.once)


if __name__ == '__main__':
    main()
