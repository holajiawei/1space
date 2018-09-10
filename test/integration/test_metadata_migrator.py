"""
Copyright 2018 SwiftStack

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

import boto3
import botocore
import botocore.exceptions
from contextlib import contextmanager
from functools import partial
import hashlib
import json
import StringIO
import swiftclient
from swiftclient.exceptions import ClientException
import time
import urllib
from . import (
    clear_swift_container, wait_for_condition,
    clear_s3_bucket, clear_swift_account, kill_a_pid)
import s3_sync.daemon_utils
import s3_sync.migrator
import s3_sync.metadata_migrator
from s3_sync.migrator import get_sys_migrator_header
from s3_sync.utils import SWIFT_TIME_FMT
from swift_metadata_sync.metadata_sync import MetadataSync
import elasticsearch
import test_migrator
import logging
import psutil
import subprocess
import datetime
import re
import mock


LAST_MODIFIED_FMT = '%a, %d %b %Y %H:%M:%S %Z'


def is_metadata_migrator_running():
    """
    Returns the PID of a running metadata migrator, or a false value otherwise.
    """
    for proc in psutil.process_iter():
        try:
            if '/usr/local/bin/swift-metadata-migrator' in proc.cmdline():
                return proc.pid
        except Exception:
            pass


class TempMetadataMigratorStatus(object):
    def __init__(self, config):
        self.config = config
        self.status = {}

    def save_migration(self, config, marker, copied, scanned, bytes_count,
                       is_reset):
        self.status['marker'] = marker
        s3_sync.migrator._update_status_counts(
            self.status, copied, scanned, bytes_count, is_reset)

    def get_migration(self, config):
        # Currently, we only support a single migration configuration
        if not s3_sync.migrator.equal_migration(self.config, config):
            raise NotImplementedError
        return self.status


class MetadataMigratorFactory(object):
    CONFIG = '/swift-s3-sync/containers/swift-s3-sync/swift-s3-sync.conf'
    SWIFT_DIR = '/etc/swift'

    def __init__(self, conf_path=CONFIG):
        with open(conf_path) as conf_fh:
            self.config = json.load(conf_fh)
        self.migrator_config = self.config['metadata_migrator_settings']
        s3_sync.daemon_utils.setup_logger(
            s3_sync.metadata_migrator.LOGGER_NAME, self.migrator_config)
        self.logger = logging.getLogger(s3_sync.metadata_migrator.LOGGER_NAME)

    def get_migrator(self, migration, status):
        ic_pool = s3_sync.metadata_migrator.create_ic_pool(
            self.config, self.SWIFT_DIR, self.migrator_config['workers'] + 1,
            self.logger)
        return s3_sync.metadata_migrator.MetadataMigrator(
            migration, status, self.migrator_config['items_chunk'],
            self.migrator_config['workers'], ic_pool, self.logger, 0, 1)


class ESClientException(ClientException):
    def __init__(self, status_code, headers, message):
        self.status_code = status_code
        self.headers = status_code
        self.message = message
        super(ESClientException, self).__init__(
            message,
            http_status=status_code,
            http_response_headers=headers,
            http_response_content=message)

    @classmethod
    def from_response(*args, **kwargs):
        raise NotImplementedError

    def __str__(self):
        return "Elasticsearch response: %s: %s" % (
            self.status_code, self.message)


class SwiftLikeESConnection(object):

    DOC_TYPE = MetadataSync.DOC_TYPE
    SWIFT_PAGINATION_LIMIT = 10000
    DOC_MAPPING = MetadataSync.DOC_MAPPING
    USER_META_PREFIX = MetadataSync.USER_META_PREFIX
    INDEX_MAX_LENGTH = 255
    INDEX_INVALID_CHARS = ['#', "\\" '/', '*', '?', '"', '<', '>', '|']

    def __init__(self, account, mapping_config, **kwargs):
        self.logger = logging.getLogger()
        self._config = mapping_config
        self.account = account
        self._es_conn = elasticsearch.Elasticsearch(
            mapping_config['es_hosts'])

    def _maybe_decode(self, string):
        s = string.decode('utf8') if not isinstance(string, unicode)\
            else string
        return s

    def _maybe_encode(self, string):
        s = string.encode('utf8') if isinstance(string, unicode)\
            else string
        return s

    def _get_es_index(self, container):
        return self._get_es_index_prefix(container=container)

    def _get_es_index_prefix(self, container=''):
        try:
            index_prefix = self._config['index_prefix']
        except KeyError:
            return self._maybe_decode(self._config['index'])

        index = "%s%s_%s" % (
            self._maybe_decode(index_prefix),
            self._maybe_decode(self.account),
            self._maybe_decode(container))
        index = index.lower()
        for i in self.INDEX_INVALID_CHARS:
            index = index.replace(i, '_')
        if len(index) > self.INDEX_MAX_LENGTH:
            index = (index[:self.INDEX_MAX_LENGTH])
        return index

    def _es_exception_as_clientexception(self, exception):
        if type(exception) is elasticsearch.ImproperlyConfigured:
            return ESClientException(
                400, {}, "Elasticsearch client is improperly configured")
        elif type(exception) is elasticsearch.SerializationError:
            return ESClientException(
                422, {}, "Client passed improperly serialized data")
        elif type(exception) is elasticsearch.TransportError:
            if exception.status_code is not 'N/A':
                return ESClientException(
                    exception.status_code, {}, exception.error)
            else:
                return ESClientException(
                    502, {}, "Could not connect to Elasticsearch")
        elif type(exception) is elasticsearch.ConnectionError:
            return ESClientException(
                502, {}, "Connection to Elasticsearch failed: %s" % (
                    str(exception)))
        elif type(exception) is elasticsearch.ConnectionTimeout:
            return ESClientException(
                502, {}, "Connection to Elasticsearch timed out: %s" % (
                    str(exception)))
        elif type(exception) is elasticsearch.SSLError:
            return ESClientException(
                502, {}, "TLS Error communicating with Elasticsearch: %s" % (
                    str(exception)))
        elif type(exception) is elasticsearch.NotFoundError:
            return ESClientException(
                404, {}, "Entity not found in Elasticsearch")
        elif type(exception) is elasticsearch.ConflictError:
            return ESClientException(
                409, {}, "Elasticsearch replied with 409 conflict")
        elif type(exception) is elasticsearch.RequestError:
            return ESClientException(
                400, {}, "Elasticsearch replied with 400 bad request")
        else:
            return ESClientException(
                502, {}, "Unknown Elasticsearch error: %s" % (
                    str(exception)))

    def _es_to_object_store(self, item):
        n = {
            'hash': item['_source'].get('etag', ''),
            'name': item['_source']['x-swift-object'],
            'content_type': item['_source'].get('content-type', ''),
            'bytes': item['_source'].get('content-length', '')}

        if 'last-modified' in item['_source']:
            n['last_modified'] = datetime.datetime.utcfromtimestamp(
                int(int(item['_source']['last-modified']) / 1000)
            ).strftime(SWIFT_TIME_FMT)

        if get_sys_migrator_header('object') in item['_source']:
            n[get_sys_migrator_header('object')] = \
                item['_source'][get_sys_migrator_header('object')]

        if 'content-location' in item['_source']:
            n['content_location'] = item['_source']['content-location']

        return n

    def _es_to_object_store_headers(self, item):
        n = {
            'etag': item['_source'].get('etag', ''),
            'name': item['_source']['x-swift-object'],
            'content-type': item['_source'].get('content-type', ''),
            'content-length': item['_source'].get('content-length', '')}

        if 'last-modified' in item['_source']:
            n['last-modified'] = datetime.datetime.utcfromtimestamp(
                int(int(item['_source']['last-modified']) / 1000)
            ).strftime(SWIFT_TIME_FMT)

        if get_sys_migrator_header('object') in item['_source']:
            n[get_sys_migrator_header('object')] = \
                item['_source'][get_sys_migrator_header('object')]

        if 'content-location' in item['_source']:
            n['content-location'] = item['_source']['content-location']

        non_user_meta = ['etag', 'x-swift-object', 'x-swift-account',
                         'x-swift-container', 'content-type', 'content-length',
                         'last-modified', 'content-location']
        for k, v in item['_source'].items():
            if k not in non_user_meta:
                n["x-object-meta-%s" % k] = v

        return n

    def _get_document_id(self, container, key):
        return hashlib.sha256(
            '/'.join([self._maybe_encode(self.account),
                      self._maybe_encode(container),
                      self._maybe_encode(key)])
        ).hexdigest()

    def _create_es_doc(self, _meta, container, key, parse_json=False):
        def _parse_document(value):
            try:
                return json.loads(value.decode('utf-8'))
            except ValueError:
                return value.decode('utf-8')

        meta = {k.lower(): v for k, v in _meta.items()}
        es_doc = {}

        ts = int(time.time() * 1000)
        es_doc['last-modified'] = ts
        es_doc['x-swift-object'] = key
        es_doc['x-swift-account'] = self.account
        es_doc['x-swift-container'] = container

        if 'content-location' in meta:
            es_doc['content-location'] = meta['content-location']

        user_meta_keys = dict(
            [(k.split(SwiftLikeESConnection.USER_META_PREFIX, 1)[1].
              decode('utf-8'),
             _parse_document(v) if parse_json else v.decode('utf-8'))
             for k, v in meta.items()
             if k.startswith(SwiftLikeESConnection.USER_META_PREFIX)])
        es_doc.update(user_meta_keys)
        for field in SwiftLikeESConnection.DOC_MAPPING.keys():
            if field in es_doc:
                continue
            if field not in meta:
                continue
            if SwiftLikeESConnection.DOC_MAPPING[field]['type'] == 'boolean':
                es_doc[field] = str(meta[field]).lower()
                continue
            es_doc[field] = meta[field]
        return es_doc

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

    def delete_container(self, container):
        try:
            self._es_conn.indices.delete(
                index=self._get_es_index(container))
        except Exception as e:
            if hasattr(e, 'status_code') and e.status_code == 404:
                pass
            else:
                raise self._es_exception_as_clientexception(e)

    def delete_object(self, container, obj):
        _id = self._get_document_id(container, obj)

        try:
            self._es_conn.delete(
                index=self._get_es_index(container),
                doc_type=self.DOC_TYPE,
                id=_id)
        except Exception as e:
            raise self._es_exception_as_clientexception(e)

    def get_account(self):
        index_prefix = "%s*" % self._get_es_index_prefix()

        try:
            indices = sorted(self._es_conn
                                 .indices.get("%s" % (
                                     index_prefix)).keys())
        except Exception as e:
            raise self._es_exception_as_clientexception(e)

        def lreplace(s, old, new):
            return re.sub(r'^(?:%s)+' % re.escape(old),
                          lambda m: new * (m.end() / len(old)),
                          s)
        return (
            {},
            [{'name': lreplace(index,
             self._get_es_index_prefix(), '')}
             for index in indices])

    def get_container(self, container):
        body = {
            "sort": [{"x-swift-object.keyword": {"order": "asc"}}],
            "query": {
                "bool": {
                    "filter": [
                        {"match":
                            {"x-swift-account":
                                self._maybe_encode(self.account)}},
                        {"match":
                            {"x-swift-container":
                                self._maybe_encode(container)}},
                        {"exists": {"field": "x-swift-object"}}
                    ]
                }
            }
        }

        headers = self.head_container(container)

        try:
            resp = self._es_conn.search(
                index=self._get_es_index(container),
                doc_type=self.DOC_TYPE,
                size=self.SWIFT_PAGINATION_LIMIT,
                body=body)
        except Exception as e:
            raise self._es_exception_as_clientexception(e)

        if not resp:
            return (headers, [])

        hits = resp.get('hits', {}).get('hits', [])
        entries = [self._es_to_object_store(entry) for entry in hits]
        result = (headers, entries)
        return result

    def get_object(self, container, key):
        hdrs = self.head_object(container, key)
        # The metadata migrator only respects some sysmeta
        # headers, none of which are content-disposition,
        # content-encoding, or x-delete-at.
        # To shim some migrator tests we need to mock these out.
        hdrs['content-disposition'] = mock.ANY
        hdrs['content-encoding'] = mock.ANY
        hdrs['x-delete-at'] = mock.ANY
        # Also mock the body
        return (hdrs, mock.ANY)

    # No equivalent in ES.
    def head_account(self):
        raise NotImplementedError

    def head_container(self, container):
        try:
            mappings = self._es_conn.indices.get_mapping(
                index=self._get_es_index(container))
            return (mappings[self._get_es_index(container)]
                            ['mappings'][self.DOC_TYPE].get(
                                '_meta', {}))
        except Exception as e:
            raise self._es_exception_as_clientexception(e)

    def head_object(self, container, key):
        _id = self._get_document_id(container, key)

        try:
            resp = self._es_conn.get(
                index=self._get_es_index(container),
                doc_type=self.DOC_TYPE,
                id=_id
            )
        except Exception as e:
            raise self._es_exception_as_clientexception(e)

        return self._es_to_object_store_headers(resp)

    def post_account(self, headers):
        raise NotImplementedError

    def post_container(self, container, headers):
        try:
            mappings = self._es_conn.indices.get_mapping(
                index=self._get_es_index(container),
                doc_type=self.DOC_TYPE)
        except Exception as e:
            raise self._es_exception_as_clientexception(e)

        mapping = mappings[
            self._get_es_index(container)
        ]['mappings'][self.DOC_TYPE]

        try:
            mapping['_meta'].update(headers)
        except KeyError:
            mapping['_meta'] = headers

        mapping['_meta']['last-modified'] =\
            time.strftime(LAST_MODIFIED_FMT)

        (mappings[self._get_es_index(container)]
            ['mappings'][self.DOC_TYPE]
            ['_meta']).update(headers)
        try:
            self._es_conn.indices.put_mapping(
                index=self._get_es_index(container),
                doc_type=self.DOC_TYPE,
                body=mapping)
        except Exception as e:
            raise self._es_exception_as_clientexception(e)

    def post_object(self, container, key, headers):
        headers['etag'] = 'deadbeef'
        headers['content-length'] = 0
        headers['content-type'] = 'application/foo'
        try:
            self._create_es_doc(headers,
                                container,
                                self._maybe_decode(key),
                                self._config.get('parse_json',
                                                 False))
            # Force-refresh the index to avoid consistency
            # issues with "X newer than Y" type tests.
            self._es_conn.index(
                index=self._get_es_index(container),
                doc_type=self.DOC_TYPE,
                body=self._create_es_doc(headers,
                                         container,
                                         self._maybe_decode(key),
                                         self._config.get(
                                             'parse_json', False)),
                id=self._get_document_id(container, key),
                pipeline=self._config.get('pipeline', None),
                refresh=True)
        except Exception as e:
            raise self._es_exception_as_clientexception(e)

    def put_container(self, container, headers=None):
        properties = dict([(k, v) for k, v in
                          SwiftLikeESConnection.DOC_MAPPING.items()])
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

        new_mapping['mappings'][self.DOC_TYPE]['_meta']['last-modified'] =\
            time.strftime(LAST_MODIFIED_FMT)

        try:
            self._es_conn.indices.create(
                index=self._get_es_index(container),
                body=new_mapping)
        except Exception as e:
            try:
                if not e.info['error']['type'] ==\
                        'index_already_exists_exception':
                    raise self._es_exception_as_clientexception(e)
            except (KeyError, AttributeError, ValueError) as e:
                raise self._es_exception_as_clientexception(e)

    def put_object(self, container, key, body, headers={}):
        return self.post_object(container, key, headers)


class TestMetadataMigrator(test_migrator.TestMigrator):

    conn_by_acct_es = {}  # Connections by account, for elasticsearch

    # We can reuse many, but not all migrator tests by just
    # swapping out our 'local' and / or 'noshunt' swift connections
    # with a swift-like elasticsearch connection.
    # In some cases we need to retain the 'local', shunted connection
    # so we can test the shunt.
    _es_shims = {'conn_for_acct': [], 'conn_for_acct_noshunt': []}

    @classmethod
    def conn_for_acct(klass, acct):
        if acct in TestMetadataMigrator._es_shims['conn_for_acct']:
            return TestMetadataMigrator.conn_for_acct_es(acct)
        else:
            return test_migrator.TestMigrator.conn_for_acct(acct)

    @classmethod
    def conn_for_acct_noshunt(klass, acct):
        if acct in TestMetadataMigrator._es_shims['conn_for_acct_noshunt']:
            return TestMetadataMigrator.conn_for_acct_es(acct)
        else:
            return test_migrator.TestMigrator.conn_for_acct_noshunt(acct)

    def local_swift(self, method, *args, **kwargs):
        if TestMetadataMigrator._es_shims.get('local_swift', False):
            return self.local_es(method, *args, **kwargs)
        else:
            return getattr(self.swift_src, method)(*args, **kwargs)

    @classmethod
    def _add_conn_for_elastic_acct(klass, mapping, acct_utf8, mapping_config):
        if acct_utf8 not in mapping:
            mapping[acct_utf8] =\
                SwiftLikeESConnection(acct_utf8, mapping_config)

    @classmethod
    def _add_conns_for_elastic_acct(klass, acct_utf8, mapping_config):
        klass._add_conn_for_elastic_acct(
            klass.conn_by_acct_es, acct_utf8, mapping_config)

    @classmethod
    def _get_s3_sync_conf(klass):
        with open(klass.CLOUD_SYNC_CONF) as conf_handle:
            conf = json.load(conf_handle)
            conf['migrations'] = conf['metadata_migrations']
            conf['migrator_settings'] = conf['metadata_migrator_settings']
            return conf

    @classmethod
    def _find_migration(klass, matcher):
        for migration in klass.test_conf['metadata_migrations']:
            if matcher(migration):
                return migration
        raise RuntimeError('No matching metadata migration')

    @classmethod
    def conn_for_acct_es(klass, acct):
        acct_utf8 = acct.encode('utf-8') if isinstance(acct, unicode) else acct
        try:
            return klass.conn_by_acct_es[acct_utf8]
        except KeyError:
            raise

    def local_es(self, method, *args, **kwargs):
        return getattr(self.es_src, method)(*args, **kwargs)

    @classmethod
    def setUpClass(klass):
        TestMetadataMigrator._es_shims = \
            {'conn_for_acct': [], 'conn_for_acct_noshunt': []}
        klass.test_conf = klass._get_s3_sync_conf()

        # Get a reseller_admin token so we don't have to mess with any auth
        # silliness.
        klass.admin_conn = swiftclient.client.Connection(
            klass.SWIFT_CREDS['authurl'],
            klass.SWIFT_CREDS['admin']['user'],
            klass.SWIFT_CREDS['admin']['key'],
            retries=0)
        _, admin_token = klass.admin_conn.get_auth()

        # Get our s3 client biz
        s3 = [container for container in klass.test_conf['containers']
              if container.get('protocol', 's3') == 's3'][0]
        klass.S3_CREDS.update({
            'endpoint': 'http://localhost:%d' % klass.PORTS['s3'],
            'user': s3['aws_identity'],
            'key': s3['aws_secret'],
        })
        session = boto3.session.Session(
            aws_access_key_id=s3['aws_identity'],
            aws_secret_access_key=s3['aws_secret'])
        conf = boto3.session.Config(s3={'addressing_style': 'path'})
        klass.s3_client = session.client(
            's3', config=conf,
            endpoint_url='http://localhost:%d' % klass.PORTS['s3'])

        url_user_key_to_acct = {}  # temporary for deduping account lookups
        for mapping in klass.test_conf['migrations']:
            # Make sure we have a connection for any Swift account on either
            # "end".  For remote ends, we have to auth once to find the account
            # name; we use url_user_key_to_acct to only do that once per input
            # tuple that could give us different answers.
            acct_utf8 = mapping['account'].encode('utf8')
            klass._add_conns_for_elastic_acct(acct_utf8, mapping)
            klass._add_conns_for_swift_acct(acct_utf8, admin_token)

            if mapping['protocol'] == 'swift':
                # Get conns for the other side, too
                if mapping.get('remote_account'):
                    acct_utf8 = mapping['remote_account'].encode('utf8')
                else:
                    conn_key = (mapping['aws_endpoint'],
                                mapping['aws_identity'],
                                mapping['aws_secret'])
                    acct_utf8 = url_user_key_to_acct.get(conn_key)
                if not acct_utf8:
                    connection_kwargs = {
                        'authurl': mapping['aws_endpoint'],
                        'user': mapping['aws_identity'],
                        'key': mapping['aws_secret'],
                        'retries': 0
                    }

                    if mapping.get('auth_type') == 'keystone_v3':
                        connection_kwargs['os_options'] = {
                            'project_name': mapping.get('project_name'),
                            'user_domain_name': mapping.get(
                                'user_domain_name'),
                            'project_domain_name':
                            mapping.get('project_domain_name')
                        }
                        connection_kwargs['auth_version'] = '3'

                    # Need to auth to get acct name, then add it to the cache
                    conn = swiftclient.client.Connection(**connection_kwargs)
                    url, _ = conn.get_auth()
                    acct_utf8 = urllib.unquote(url.rsplit('/')[-1])
                    url_user_key_to_acct[conn_key] = acct_utf8
                klass._add_conns_for_swift_acct(acct_utf8, admin_token)
                klass._add_conns_for_elastic_acct(acct_utf8, mapping)
                # As a convenience for ourselves, we'll stick the resolved
                # remote Swift account name in the mapping as "aws_account"
                # (stored as Unicode string for consistency)
                # TODO(darrell): maybe use `remote_account` since the
                # SwiftSync() class can eat that already, so there's precedent
                # for the storage key name.
                mapping['aws_account'] = acct_utf8.decode('utf8')

            # Now maybe auto-create some containers
            if mapping.get('aws_bucket') == '/*':
                continue
            if container['aws_bucket'].startswith('no-auto-'):
                # Remove any no-auto create containers for swift
                if mapping['protocol'] == 'swift':
                    try:
                        conn = klass.conn_for_acct_noshunt(
                            mapping['aws_account'])
                        conn.delete_container(mapping['aws_bucket'])
                    except swiftclient.exceptions.ClientException as e:
                        if e.http_status == 404:
                            continue
                        raise
            else:
                if mapping['protocol'] == 'swift' and \
                        mapping.get('aws_bucket'):
                    # For now, the aws_bucket is just a prefix, not a container
                    # name for a swift destination that has a source container
                    # of /*.  So don't create a container of that name.
                    if mapping.get('container') != '/*':
                        conn = klass.conn_for_acct_noshunt(
                            mapping['aws_account'])
                        conn.put_container(mapping['aws_bucket'])
                else:
                    try:
                        klass.s3_client.create_bucket(
                            Bucket=mapping['aws_bucket'])
                    except botocore.exceptions.ClientError as e:
                        if e.response['Error']['Code'] == 409:
                            pass
            if mapping.get('container') and mapping.get('container') != '/*':
                conn = klass.conn_for_acct_noshunt(mapping['account'])
                if mapping['container'].startswith('no-auto-'):
                    try:
                        clear_swift_container(conn, mapping['container'])
                        conn.delete_container(mapping['container'])
                    except swiftclient.exceptions.ClientException as e:
                        if e.http_status != 404:
                            raise
                else:
                    conn.put_container(mapping['container'])

        klass.swift_src = klass.conn_for_acct('AUTH_test')
        klass.es_src = klass.conn_for_acct_es('AUTH_test')
        klass.swift_dst = klass.conn_for_acct(
            u"AUTH_\u062aacct2".encode('utf8'))
        klass.swift_nuser = klass.conn_for_acct('AUTH_nacct')
        klass.es_nuser = klass.conn_for_acct('AUTH_nacct')
        klass.swift_nuser2 = klass.conn_for_acct('AUTH_nacct2')
        klass.es_nuser2 = klass.conn_for_acct('AUTH_nacct2')
        # We actually test auth through this connection, so give it real creds:
        klass.cloud_connector_client = swiftclient.Connection(
            'http://cloud-connector:%d/auth/v1.0' %
            klass.PORTS['cloud_connector'],
            klass.SWIFT_CREDS['cloud-connector']['user'],
            klass.SWIFT_CREDS['cloud-connector']['key'],
            retries=0)

        klass.es_src = klass.conn_for_acct_es('AUTH_test')
        klass.es_dst = klass.conn_for_acct_es(
            u"AUTH_\u062aacct2".encode('utf8'))
        klass.es_nuser = klass.conn_for_acct_es('AUTH_nacct')
        klass.es_nuser2 = klass.conn_for_acct_es('AUTH_nacct2')

    @contextmanager
    def migrator_running(self):
        """
        Context manager that starts the migrator and then ensures it gets
        stopped when the context is exited.
        """
        old_migrator_pid = is_metadata_migrator_running()
        if old_migrator_pid:
            kill_a_pid(old_migrator_pid)

        devnull = open('/dev/null', 'wb')
        proc = subprocess.Popen(
            ['/usr/bin/python', '/usr/local/bin/swift-metadata-migrator',
             '--config',
             '/swift-s3-sync/containers/swift-s3-sync/swift-s3-sync.conf'],
            close_fds=True,
            cwd='/',
            stdout=devnull, stderr=devnull)
        try:
            yield
        finally:
            devnull.close()
            if proc.poll() is None:
                kill_a_pid(proc.pid)

    def tearDown(self):
        TestMetadataMigrator._es_shims = \
            {'conn_for_acct': [], 'conn_for_acct_noshunt': []}

        def _clear_and_maybe_delete_container(account, container):
            for conn in [self.conn_for_acct_noshunt(account),
                         self.conn_for_acct_es(account)]:
                clear_swift_container(conn, container)
                clear_swift_container(conn, container + '_segments')
                if container.startswith('no-auto-'):
                    try:
                        conn.delete_container(container)
                        conn.delete_container(container + '_segments')
                    except swiftclient.exceptions.ClientException as e:
                        if e.http_status != 404:
                            raise

        # Make sure all migration-related containers are cleared
        for container in self.test_conf['migrations']:
            if container.get('container'):
                _clear_and_maybe_delete_container(container['account'],
                                                  container['container'])
            if container['aws_bucket'] == '/*':
                continue
            if container['protocol'] == 'swift':
                _clear_and_maybe_delete_container(container['aws_account'],
                                                  container['aws_bucket'])
            else:
                try:
                    clear_s3_bucket(self.s3_client, container['aws_bucket'])
                except botocore.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == 'NoSuchBucket':
                        continue

        # Clean out all container accounts
        clear_swift_account(self.swift_nuser)
        clear_swift_account(self.swift_nuser2)
        clear_swift_account(self.es_nuser)
        clear_swift_account(self.es_nuser2)

    def test_s3_migration(self):
        TestMetadataMigrator._es_shims = {
            'conn_for_acct': [],
            'conn_for_acct_noshunt': [],
            'local_swift': True}
        return super(TestMetadataMigrator, self).\
            test_s3_migration()

    def test_swift_migration(self):
        migration = self.swift_migration()
        TestMetadataMigrator._es_shims = {
            'conn_for_acct': [],
            'conn_for_acct_noshunt': [migration['account']]}
        return super(TestMetadataMigrator, self).\
            test_swift_migration()

    def test_swift_migration_unicode_acct(self):
        migration = self._find_migration(
            lambda m: m.get('container') == 'flotty')
        TestMetadataMigrator._es_shims = {
            'conn_for_acct': [],
            'conn_for_acct_noshunt': [migration['account']]}
        return super(TestMetadataMigrator, self).\
            test_swift_migration_unicode_acct()

    def test_swift_large_objects(self):
        migration = self.swift_migration()
        conn_es = self.conn_for_acct_es(migration['account'])
        status = TempMetadataMigratorStatus(migration)
        migrator = MetadataMigratorFactory().get_migrator(
            migration, status)

        segments_container = migration['aws_bucket'] + '_segments'

        self.remote_swift(
            'put_container', segments_container)
        for i in range(10):
            self.remote_swift('put_object', segments_container,
                              'slo-part-%d' % i, chr(97 + i) * 2**20,
                              headers={'x-object-meta-part': i})
            self.remote_swift('put_object', segments_container,
                              'dlo-part-%d' % i, chr(97 + i) * 2**20,
                              headers={'x-object-meta-part': i})
        # Upload the manifests
        self.remote_swift(
            'put_object', migration['aws_bucket'], 'dlo', '',
            headers={'x-object-manifest': segments_container + '/dlo-part',
                     'x-object-meta-dlo': 'dlo-meta'})

        slo_manifest = [
            {'path': '/%s/slo-part-%d' % (segments_container, i)}
            for i in range(10)]
        self.remote_swift(
            'put_object', migration['aws_bucket'], 'slo',
            json.dumps(slo_manifest),
            headers={'x-object-meta-slo': 'slo-meta'},
            query_string='multipart-manifest=put')

        migrator.next_pass()

        # Allow ES just enough time to refresh the index.
        # The metadata migrator won't try to do anything with the segments.
        time.sleep(2)
        _, listing = conn_es.get_container(migration['container'])
        swift_names = [obj['name'] for obj in listing]
        objects = ['dlo', 'slo']
        self.assertEqual(sorted(objects), swift_names)

    def test_swift_self_contained_dlo(self):
        # Metadata migrator doesn't care about large object segments.
        pass

    def test_migrate_new_container_location(self):
        migration = self._find_migration(
            lambda cont: cont['container'] == 'no-auto-migration-swift-reloc')
        TestMetadataMigrator._es_shims = {
            'conn_for_acct': [],
            'conn_for_acct_noshunt': [migration['account']]}
        return super(TestMetadataMigrator, self).\
            test_migrate_new_container_location()

    def test_container_meta(self):
        migration = self._find_migration(
            lambda cont: cont['container'] == 'no-auto-acl')

        acl = 'AUTH_' + migration['aws_identity'].split(':')[0]
        self.remote_swift('put_container', migration['aws_bucket'],
                          headers={'x-container-read': acl,
                                   'x-container-write': acl,
                                   'x-container-meta-test': 'test metadata'})

        conn_local = self.conn_for_acct(migration['account'])
        conn_es = self.conn_for_acct_es(migration['account'])

        def _check_container_created(conn):
            try:
                ret = conn.get_container(migration['container'])
                if ret:
                    return ret
            except swiftclient.exceptions.ClientException as e:
                if e.http_status == 404:
                    return False
                raise

        # verify get_head works through shunt
        hdrs = conn_local.head_container(migration['container'])
        self.assertIn('x-container-meta-test', hdrs)
        self.assertEqual('test metadata', hdrs['x-container-meta-test'])

        # Verify get_container works through shunt
        res = _check_container_created(conn_local)
        self.assertTrue(res)
        hdrs, listing = res
        self.assertIn('x-container-meta-test', hdrs)
        self.assertEqual('test metadata', hdrs['x-container-meta-test'])

        # verify container not really there
        self.assertFalse(_check_container_created(conn_es))

        with self.migrator_running():
            hdrs, listing = wait_for_condition(5, partial(
                _check_container_created, conn_es))

        self.assertIn('x-container-meta-test', hdrs)
        self.assertEqual('test metadata', hdrs['x-container-meta-test'])

    def test_migrate_all_containers(self):
        migration = self._find_migration(
            lambda cont: cont['aws_bucket'] == '/*')
        test_objects = [
            ('swift-blobBBB2', 'blob content', {}),
            (u'swift-2unicod\u00e9', '\xde\xad\xbe\xef', {}),
            ('swift-2with-headers',
             'header-blob',
             {'x-object-meta-custom-header': 'value',
              u'x-object-meta-unicod\u00e9': u'\u262f',
              u'x-object-meta-parse-json': '["foo", "bar"]',
              u'x-object-meta-parse-json-unicod\u00e9':
                  u'["foo\u262f", "\u262f"]',
              'content-type': 'migrator/test'})]

        test_containers = ['container1', 'container2', 'container3',
                           u'container-\u062a']

        conn_es = self.conn_for_acct_es(migration['account'])
        conn_local = self.conn_for_acct(migration['account'])
        conn_remote = self.conn_for_acct(migration['aws_account'])

        def _check_objects_copied():
            test_names = set([obj[0] for obj in test_objects])
            for cont in test_containers:
                try:
                    hdrs, listing = conn_es.get_container(cont)
                except swiftclient.exceptions.ClientException as ce:
                    if '404' in str(ce):
                        return False
                    else:
                        raise
                swift_names = set([obj['name'] for obj in listing])
                if test_names != swift_names:
                    return False
            return True

        for i, container in enumerate(test_containers):
            container_meta = {
                u'x-container-meta-tes\u00e9t': u'test\u262f metadata%d' % i}
            conn_remote.put_container(
                # We create a new dictionary, because SwiftClient mutates the
                # header dictionary that we pass it, unfortunately.
                test_containers[i], headers=dict(container_meta))
            # verify get_head works through shunt
            hdrs = conn_local.head_container(test_containers[i])
            for key, value in container_meta.items():
                self.assertIn(key, hdrs)
                self.assertEqual(value, hdrs[key])

            for name, body, headers in test_objects:
                conn_remote.put_object(
                    container, name, StringIO.StringIO(body), headers=headers)

        with self.migrator_running():
            wait_for_condition(5, _check_objects_copied)

        for name, expected_body, user_meta in test_objects:
            for cont in test_containers:
                hdrs = conn_es.head_object(cont, name)
                for k, v in user_meta.items():
                    self.assertIn(k, hdrs)
                    if 'parse-json' in k:
                        self.assertEqual(
                            json.loads(v.encode('utf-8')),
                            hdrs[k])
                    else:
                        self.assertEqual(v, hdrs[k])

    def test_propagate_delete(self):
        # We don't propagate deletes.
        pass

    def test_propagate_container_meta(self):
        # Nor do we propagate container meta.
        pass

    def test_migrate_account_metadata(self):
        # ...and we don't migrate account metadata as we don't have
        # an equivalent structure to an "account" in ES, only a container
        # (which is represented as an entire index).
        pass

    def test_propagate_container_meta_changes(self):
        migration = self._find_migration(
            lambda cont: cont['aws_bucket'] == '/*')

        acl = 'AUTH_' + self.SWIFT_CREDS['dst']['user'].split(':')[0]

        key1 = u'x-container-meta-migrated-\u062a'
        key2 = u'x-container-meta-migrated-2-\u062a'
        key3 = u'x-container-meta-migrated-3-\u062a'
        val1 = u'new-meta \u062a'
        val2 = u'changed-meta \u062a'
        val3 = u'new-meta2 \u062a'

        init_local_headers = {
            key2: val2,
            key3: val1,
        }
        init_remote_headers = {
            'x-container-write': acl,
            'x-container-read': acl,
            key1: val1,
            key3: val3,
        }

        conn_es = self.conn_for_acct_es(migration['account'])
        conn_remote = self.conn_for_acct(migration['aws_account'])

        conn_es.put_container('metadata_test', headers=init_local_headers)
        # Note: container last_modified dates are whole seconds, so sleep
        # is needed to ensure remote is newer
        time.sleep(2)
        conn_remote.put_container('metadata_test',
                                  headers=init_remote_headers)

        # validate the acl exists on remote (sanity check)
        scheme, rest = self.SWIFT_CREDS['authurl'].split(':', 1)
        swift_host, _ = urllib.splithost(rest)

        remote_swift = swiftclient.client.Connection(
            authurl=self.SWIFT_CREDS['authurl'],
            user=self.SWIFT_CREDS['dst']['user'],
            key=self.SWIFT_CREDS['dst']['key'],
            os_options={'object_storage_url': '%s://%s/v1/%s' % (
                scheme, swift_host, migration['aws_account'].split(':')[0])})
        etag = remote_swift.put_object('metadata_test', 'test', 'test')
        self.assertEqual(hashlib.md5('test').hexdigest(), etag)

        def key_val_copied(k, v):
            test_hdrs = conn_es.head_container('metadata_test')
            if k not in test_hdrs:
                return False
            if test_hdrs[k] == v:
                return True
            return False

        key1_copied = partial(key_val_copied, key1, val1)
        val3_copied = partial(key_val_copied, key3, val3)

        def key_gone(key):
            test_hdrs = conn_es.head_container('metadata_test')
            if key not in test_hdrs:
                return True
            return False

        # sanity check
        self.assertTrue(key_val_copied(key2, val2))
        self.assertTrue(key_gone(key1))

        with self.migrator_running():
            # verify really copied
            wait_for_condition(5, key1_copied)
            self.assertTrue(key_gone(key2))
            self.assertTrue(val3_copied)

    def test_container_metadata_copied_only_when_newer(self):
        migration = self._find_migration(
            lambda cont: cont['aws_bucket'] == '/*')
        TestMetadataMigrator._es_shims = {
            'conn_for_acct': [migration['account']],
            'conn_for_acct_noshunt': [migration['account']]}
        return super(TestMetadataMigrator, self).\
            test_container_metadata_copied_only_when_newer()

    def test_object_metadata_copied_only_when_newer(self):
        migration = self.swift_migration()
        TestMetadataMigrator._es_shims = {
            'conn_for_acct': [migration['account']],
            'conn_for_acct_noshunt': [migration['account']]}
        return super(TestMetadataMigrator, self).\
            test_object_metadata_copied_only_when_newer()

    def test_propagate_object_meta(self):
        # Not applicable here.
        pass

    def test_swift_versions_location(self):
        # We don't do anything special on encoutering a versioned object.
        pass

    def test_swift_history_location(self):
        # Not applicable.
        pass

    def test_put_on_unmigrated_container(self):
        migration = self._find_migration(
            lambda cont: cont['aws_bucket'] == '/*')
        TestMetadataMigrator._es_shims = {
            'conn_for_acct': [migration['account']],
            'conn_for_acct_noshunt': []}
        return super(TestMetadataMigrator, self).\
            test_put_on_unmigrated_container()

    def test_deleted_source_objects(self):
        migration = self.swift_migration()
        TestMetadataMigrator._es_shims = {
            'conn_for_acct': [migration['account']],
            'conn_for_acct_noshunt': []}
        return super(TestMetadataMigrator, self).\
            test_deleted_source_objects()

    def test_post_deleted_source_objects(self):
        migration = self.swift_migration()
        TestMetadataMigrator._es_shims = {
            'conn_for_acct': [migration['account']],
            'conn_for_acct_noshunt': []}
        return super(TestMetadataMigrator, self).\
            test_post_deleted_source_objects()

    def _setup_deleted_container_test(self):
        '''Common code for the deleted origin container tests'''
        migration = self._find_migration(
            lambda cont: cont['aws_bucket'] == '/*')

        conn_source = self.conn_for_acct(migration['aws_account'])
        conn_destination = self.conn_for_acct_es(migration['account'])

        conn_source.put_container('test-delete')
        conn_source.put_object('test-delete', 'source_object', 'body')

        def _check_object_copied():
            try:
                dog = conn_destination.get_container('test-delete')
                hdrs, listing = dog
                if not listing:
                    return False
                if listing[0]['name'] != 'source_object':
                    return False
                if 'swift' not in listing[0]['content_location']:
                    return False
                return True
            except swiftclient.exceptions.ClientException as e:
                if e.http_status == 404:
                    return False

        with self.migrator_running():
            wait_for_condition(5, _check_object_copied)

    def test_deleted_source_container(self):
        migration = self._find_migration(
            lambda cont: cont['aws_bucket'] == '/*')
        TestMetadataMigrator._es_shims = {
            'conn_for_acct': [migration['account']],
            'conn_for_acct_noshunt': [migration['account']]}
        return super(TestMetadataMigrator, self).\
            test_deleted_source_container()

    def test_deleted_container_after_new_object(self):
        '''Destination container removed even if it has extra objects'''
        migration = self._find_migration(
            lambda cont: cont['aws_bucket'] == '/*')

        conn_source = self.conn_for_acct(migration['aws_account'])
        conn_destination = self.conn_for_acct_es(migration['account'])

        self._setup_deleted_container_test()
        conn_destination.put_object('test-delete', 'new-object', 'new')
        conn_source.delete_object('test-delete', 'source_object')
        conn_source.delete_container('test-delete')

        def _check_deleted_migrated_objects():
            try:
                conn_destination.head_container('test-delete')
            except swiftclient.exceptions.ClientException as e:
                if e.http_status == 404:
                    return True

        with self.assertRaises(swiftclient.exceptions.ClientException) as cm:
            conn_source.head_container('test-delete')
        self.assertEqual(404, cm.exception.http_status)

        with self.migrator_running():
            wait_for_condition(5, _check_deleted_migrated_objects)

    def test_deleted_container_after_post(self):
        '''Destination container removed even if it has been modified'''
        migration = self._find_migration(
            lambda cont: cont['aws_bucket'] == '/*')

        conn_source = self.conn_for_acct(migration['aws_account'])
        conn_destination = self.conn_for_acct_es(migration['account'])

        self._setup_deleted_container_test()
        conn_destination.post_container(
            'test-delete',
            {'x-container-meta-new-meta': 'value'})
        conn_source.delete_object('test-delete', 'source_object')
        conn_source.delete_container('test-delete')

        def _check_deleted_migrated_objects():
            try:
                conn_destination.head_container('test-delete')
            except swiftclient.exceptions.ClientException as e:
                if e.http_status == 404:
                    return True

        with self.assertRaises(swiftclient.exceptions.ClientException) as cm:
            conn_source.head_container('test-delete')
        self.assertEqual(404, cm.exception.http_status)

        with self.migrator_running():
            wait_for_condition(5, _check_deleted_migrated_objects)

    def test_deleted_object_pagination(self):
        migration = self.swift_migration()
        TestMetadataMigrator._es_shims = {
            'conn_for_acct': [migration['account']],
            'conn_for_acct_noshunt': [migration['account']]}
        return super(TestMetadataMigrator, self).\
            test_deleted_object_pagination()

    def test_migrate_many_slos(self):
        # The metadata migrator doesn't do anything special
        # with large objects; it just adds the metadata from the manifest.
        pass

    def test_migrate_many_dlos(self):
        # The metadata migrator doesn't do anything special
        # with dlos either.
        pass
