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
from contextlib import contextmanager
import datetime
import hashlib
import json
import logging
import math
import mock
from s3_sync.base_sync import ProviderResponse
import s3_sync.migrator
import s3_sync.metadata_migrator
from StringIO import StringIO
from swift.common.internal_client import UnexpectedResponse
from swift.common.utils import Timestamp
import time
import unittest
from tempfile import mkdtemp
import shutil
import os
import test_migrator
from test_migrator import create_timestamp, create_list_timestamp
import elasticsearch
from s3_sync.utils import SWIFT_TIME_FMT


class TestMetadataMigrator(test_migrator.TestMigrator):
    def setUp(self):
        config = {'aws_bucket': 'bucket',
                  'account': 'AUTH_test',
                  'aws_identity': 'source-account'}

        self.swift_client = mock.Mock()

        def _iter_yield_none(iterable):
            for x in iterable:
                yield x
            while True:
                yield None
        self._iter_yield_none = _iter_yield_none

        self.swift_client.es_iter_items.return_value =\
            self._iter_yield_none([])

        class FakeESResponse:
            def __init__(self, status_int):
                self.status_int = status_int

        class FakeESRequest:
            def __init__(self, status_int):
                self.status_int = status_int

            def get_response(self):
                return FakeESResponse(self.status_int)
        self.swift_client.create_container.return_value = FakeESRequest(200)

        pool = mock.Mock()
        pool.item.return_value.__enter__ = lambda *args: self.swift_client
        pool.item.return_value.__exit__ = lambda *args: None
        pool.max_size = 11
        self.logger = logging.getLogger()
        self.stream = StringIO()
        self.logger.addHandler(logging.StreamHandler(self.stream))
        self.migrator = s3_sync.metadata_migrator.MetadataMigrator(
            config, None, 1000, 5, pool, self.logger, 0, 1)
        self.migrator.status = mock.Mock()

    @mock.patch('s3_sync.migrator.create_provider')
    def test_all_buckets_next_pass_fails(self, create_provider_mock):
        self.migrator.config['aws_bucket'] = '/*'
        self.migrator._process_container = mock.Mock(
            side_effect=Exception('kaboom'))
        create_provider_mock.return_value.list_buckets.side_effect = \
            [ProviderResponse(True, 200, [], [
                {'name': 'bucket',
                 'content_location': 'some_provider'}]),
             ProviderResponse(True, 200, [], [])]
        self.migrator.next_pass()
        self.assertEqual('Failed to migrate "bucket"',
                         self.stream.getvalue().splitlines()[0])

    @mock.patch('s3_sync.metadata_migrator.time')
    @mock.patch('s3_sync.migrator.create_provider')
    def test_create_container_timeout(self, create_provider_mock, time_mock):
        provider = create_provider_mock.return_value
        provider.list_objects.return_value = ProviderResponse(
            True, 200, {}, [{'name': 'test'}])

        resp = mock.Mock()
        resp.status = 200
        resp.headers = {}
        provider.head_bucket.return_value = resp
        self.swift_client.es_iter_objects.return_value = iter([])
        self.swift_client.container_exists.return_value = False

        def fake_app(env, func):
                return func(200, [])

        self.swift_client.app.side_effect = fake_app

        time_mock.time.side_effect = (0, 0, 1)
        self.migrator.status.get_migration.return_value = {}

        self.migrator.next_pass()
        self.assertEqual(
            'MigrationError: Timeout while creating container "bucket"',
            self.get_log_lines()[-1])
        self.swift_client.create_container.assert_called_once_with(
            self.migrator.config, self.migrator.config['account'],
            self.migrator.config['container'],
            {'x-container-sysmeta-multi-cloud-internal-migrator': 'migrating'})

        self.swift_client.container_exists.assert_has_calls(
            [mock.call(self.migrator.config,
                       self.migrator.config['account'],
                       self.migrator.config['container'])] * 2)

    @mock.patch('s3_sync.migrator.create_provider')
    def test_etag_mismatch(self, create_provider_mock):
        self.migrator.config['protocol'] = 'swift'
        provider = create_provider_mock.return_value
        self.migrator.status.get_migration.return_value = {}

        objects = {
            'dlo': {
                'list_entry': {
                    'last-modified': create_list_timestamp(1.5e9),
                    'hash': 'deadbeef'},
                'headers': {
                    'x-object-manifest': 'segments/',
                    'last-modified': create_timestamp(1.5e9)
                }
            },
            'slo': {
                'list_entry': {
                    'last-modified': create_list_timestamp(1.4e9),
                    'hash': 'feedbead'},
                'headers': {
                    'x-static-large-object': True,
                    'last-modified': create_timestamp(1.4e9)
                }
            }
        }

        swift_objects = {
            'dlo': {
                'name': 'dlo',
                'last_nodified': create_list_timestamp(1.5e9),
                'etag': 'other'
            },
            'slo': {
                'name': 'slo',
                'last_nodified': create_list_timestamp(1.4e9),
                'etag': 'other-still'
            }
        }

        def _head_object(key, **kwargs):
            resp = mock.Mock()
            resp.headers = objects[key]['headers']
            resp.headers['etag'] = objects[key]['list_entry']['hash']
            resp.status = 200
            resp.body = 'nothing'
            return resp

        def _get_object_metadata(config, _account, _container, key):
            return swift_objects[key]

        self.swift_client.container_exists.return_value = True
        self.swift_client.get_object_metadata.return_value =\
            _get_object_metadata
        self.migrator._read_account_headers = mock.Mock(return_value={})
        provider.list_objects.return_value = ProviderResponse(
            True, 200, {},
            [dict([('name', k)] + objects[k]['list_entry'].items())
             for k in sorted(objects.keys())])
        provider.head_object.side_effect = _head_object
        provider.head_bucket.return_value = mock.Mock(status=200, headers={})
        provider.get_manifest.return_value = {}
        provider.head_account.return_value = {}

        self.migrator.next_pass()
        # The metadata migrator ignores large object content completely,
        # so will migrate the objects - unlike the vanilla migrator
        self.assertEqual("Copied \"bucket/dlo\"\nCopied \"bucket/slo\"\n",
                         self.stream.getvalue())

    @mock.patch('s3_sync.migrator.create_provider')
    def test_migrate_all_containers_next_pass(self, create_provider_mock):
        provider_mock = mock.Mock()
        buckets = [{'name': 'bucket1', 'content_location': 'other'},
                   {'name': 'bucket2', 'content_location': 'other'}]
        provider_mock.list_buckets.side_effect = [
            ProviderResponse(True, 200, [], buckets),
            ProviderResponse(True, 200, [], [])]
        provider_mock.list_objects.return_value = ProviderResponse(
            True, 200, {}, [{'name': 'obj'}])
        provider_mock.head_object.return_value = ProviderResponse(
            True, 200, {'last-modified': create_timestamp(1.5e9),
                        'etag': 'deadbeef',
                        'Content-Length': '1337'},
            StringIO(''))

        self.swift_client.es_iter_items.return_value =\
            self._iter_yield_none([None])
        create_provider_mock.return_value = provider_mock
        self.migrator.config = {
            'account': 'AUTH_dev',
            'aws_bucket': '/*',
        }
        temp_dir = mkdtemp()
        self.addCleanup(lambda: shutil.rmtree(temp_dir))
        status_file = os.path.join(temp_dir, 'migrator.status')
        self.migrator.status = s3_sync.migrator.Status(status_file)
        with mock.patch('time.time', return_value=100.0):
            handled_containers = self.migrator.next_pass()

        self.assertEqual(
            [{'aws_bucket': 'bucket1',
              'account': 'AUTH_dev',
              'container': 'bucket1',
              'all_buckets': True},
             {'aws_bucket': 'bucket2',
              'account': 'AUTH_dev',
              'container': 'bucket2',
              'all_buckets': True}],
            handled_containers)

        self.assertEqual(['Copied "bucket1/obj"',
                          'Copied "bucket2/obj"'],
                         self.stream.getvalue().splitlines())
        with open(status_file) as f:
            status = json.load(f)
        self.assertEqual(status, [{
            'status': {
                'marker': 'obj',
                'moved_count': 1,
                'finished': 100.0,
                'scanned_count': 1,
                'bytes_count': 0,
            },
            'account': 'AUTH_dev',
            'container': 'bucket1',
            'all_buckets': True,
            'aws_bucket': 'bucket1',
        }, {
            'status': {
                'marker': 'obj',
                'moved_count': 1,
                'finished': 100.0,
                'scanned_count': 1,
                'bytes_count': 0,
            },
            'account': 'AUTH_dev',
            'container': 'bucket2',
            'all_buckets': True,
            'aws_bucket': 'bucket2',
        }])
        self.migrator.config = {
            'account': 'AUTH_dev',
            'aws_bucket': 'bucket3',
            'container': 'bucket3',
        }
        with mock.patch('time.time', return_value=100.0):
            self.migrator.next_pass()
        with open(status_file) as f:
            status = json.load(f)
        self.assertEqual(status, [{
            'status': {
                'marker': 'obj',
                'moved_count': 1,
                'finished': 100.0,
                'scanned_count': 1,
                'bytes_count': 0,
            },
            'account': 'AUTH_dev',
            'container': 'bucket1',
            'all_buckets': True,
            'aws_bucket': 'bucket1',
        }, {
            'status': {
                'marker': 'obj',
                'moved_count': 1,
                'finished': 100.0,
                'scanned_count': 1,
                'bytes_count': 0,
            },
            'account': 'AUTH_dev',
            'container': 'bucket2',
            'all_buckets': True,
            'aws_bucket': 'bucket2',
        }, {
            'status': {
                'marker': 'obj',
                'moved_count': 1,
                'finished': 100.0,
                'scanned_count': 1,
                'bytes_count': 0,
            },
            'account': 'AUTH_dev',
            'container': 'bucket3',
            'aws_bucket': 'bucket3',
        }])

    @mock.patch('s3_sync.migrator.create_provider')
    def test_migrate_dlo(self, create_provider_mock):
        self.migrator.config['protocol'] = 'swift'
        provider = create_provider_mock.return_value
        self.migrator.status.get_migration.return_value = {}
        segments_container = 'dlo-segments'

        objects = {
            'dlo': {
                'remote_headers': {
                    'x-object-meta-custom': 'dlo-meta',
                    'last-modified': create_timestamp(1.5e9),
                    'x-object-manifest': '%s/' % segments_container,
                    'etag': 'd10',
                    'Content-Length': '10'},
                'expected_headers': {
                    'x-object-meta-custom': 'dlo-meta',
                    'x-timestamp': Timestamp(1.5e9).internal,
                    'x-object-manifest': '%s/' % segments_container,
                    'etag': 'd10',
                    'Content-Length': '10',
                    'last-modified': mock.ANY,
                    s3_sync.utils.get_sys_migrator_header('object'): str(
                        Timestamp(1.5e9).internal)}
            },
            '1': {
                'remote_headers': {
                    'x-object-meta-part': 'part-1',
                    'last-modified': create_timestamp(1.4e9),
                    'etag': '3e41',
                    'Content-Length': '1'},
                'expected_headers': {
                    'x-object-meta-part': 'part-1',
                    'etag': '3e41',
                    'x-timestamp': Timestamp(1.4e9).internal,
                    'Content-Length': '1',
                    'last-modified': mock.ANY,
                    s3_sync.utils.get_sys_migrator_header('object'): str(
                        Timestamp(1.4e9).internal)}
            },
            '2': {
                'remote_headers': {
                    'x-object-meta-part': 'part-2',
                    'last-modified': create_timestamp(1.1e9),
                    'etag': '3e42',
                    'Content-Length': '2'},
                'expected_headers': {
                    'x-object-meta-part': 'part-2',
                    'etag': '3e42',
                    'x-timestamp': Timestamp(1.1e9).internal,
                    'Content-Length': '2',
                    'last-modified': mock.ANY,
                    s3_sync.utils.get_sys_migrator_header('object'): str(
                        Timestamp(1.1e9).internal)}
            },
            '3': {
                'remote_headers': {
                    'x-object-meta-part': 'part-3',
                    'last-modified': create_timestamp(1.2e9),
                    'etag': '3e43',
                    'Content-Length': '3'},
                'expected_headers': {
                    'x-object-meta-part': 'part-3',
                    'etag': '3e43',
                    'x-timestamp': Timestamp(1.2e9).internal,
                    'Content-Length': '3',
                    'last-modified': mock.ANY,
                    s3_sync.utils.get_sys_migrator_header('object'): str(
                        Timestamp(1.2e9).internal)}
            }
        }

        containers = {segments_container: False,
                      self.migrator.config['container']: False}

        swift_404_resp = mock.Mock()
        swift_404_resp.status_int = 404

        class FakeESResponse:
            def __init__(self, status_int):
                self.status_int = status_int

        class FakeESRequest:
            def __init__(self, status_int):
                self.status_int = status_int

            def get_response(self):
                return FakeESResponse(self.status_int)

        def create_container(config, account, container, headers={}):
            containers[container] = True
            return FakeESRequest(200)

        def container_exists(config, account, container):
            return containers[container]

        def fake_app(env, func):
            containers[env['PATH_INFO'].split('/')[3]] = True
            return func(200, [])

        def get_container_metadata(config, account, container):
            if not containers[container]:
                raise UnexpectedResponse('', swift_404_resp)

        def _make_path(account, container):
            return '/'.join(['http://test/v1', account, container])

        def head_object(name, **args):
            if name not in objects.keys():
                raise RuntimeError('Unknown object: %s' % name)
            if name == 'dlo':
                return ProviderResponse(
                    True, 200, objects[name]['remote_headers'], StringIO(''))
            return ProviderResponse(
                True, 200, objects[name]['remote_headers'],
                StringIO('object body'))

        def upload_object(config, body, account, container, key, headers):
            if not containers[container]:
                raise UnexpectedResponse('', swift_404_resp)

        def list_objects(marker, chunk, prefix, bucket=None):
            if bucket is None or bucket == self.migrator.config['container']:
                return ProviderResponse(True, 200, {}, [{'name': 'dlo'}])
            elif bucket == segments_container:
                if marker == '':
                    return ProviderResponse(
                        True, 200, {},
                        [{'name': '1'}, {'name': '2'}, {'name': '3'}])
                if marker == '3':
                    return ProviderResponse(True, 200, {}, [])
            raise RuntimeError('Unknown container')

        self.swift_client.container_exists.side_effect = container_exists
        self.swift_client.get_container_metadata.side_effect = \
            get_container_metadata
        self.swift_client.app.side_effect = fake_app
        self.swift_client.make_path.side_effect = _make_path
        self.swift_client.upload_object.side_effect = upload_object
        self.swift_client.create_container.side_effect = create_container
        self.migrator._read_account_headers = mock.Mock(return_value={})

        bucket_resp = mock.Mock()
        bucket_resp.status = 200
        bucket_resp.headers = {}

        provider.head_account.return_value = {}
        provider.head_bucket.return_value = bucket_resp
        provider.list_objects.side_effect = list_objects
        provider.head_object.side_effect = head_object
        self.swift_client.es_iter_objects.return_value = iter([])

        self.migrator.next_pass()

        self.swift_client.upload_object.assert_has_calls(
            [mock.call(self.migrator.config, mock.ANY,
                       self.migrator.config['account'],
                       'bucket', 'dlo',
                       objects['dlo']['expected_headers'])])

        class NotAnAssertionError(Exception):
            pass

        try:
            self.swift_client.upload_object.assert_has_calls(
                [mock.call(self.migrator.config, mock.ANY,
                           self.migrator.config['account'],
                           'dlo-segments', '2',
                           objects['2']['expected_headers']),
                 mock.call(self.migrator.config, mock.ANY,
                           self.migrator.config['account'],
                           'dlo-segments', '3',
                           objects['3']['expected_headers']),
                 mock.call(self.migrator.config, mock.ANY,
                           self.migrator.config['account'],
                           self.migrator.config['container'], 'dlo',
                           objects['dlo']['expected_headers'])])
            raise NotAnAssertionError()
        except AssertionError:
            pass
        except NotAnAssertionError:
            raise AssertionError(
                "Metadata migrator should not migrate DLO segments")

        parts = {'dlo': False}
        for call in self.swift_client.upload_object.mock_calls:
            config, body, acct, cont, obj, headers = call[1]
            if parts[obj]:
                continue
            self.assertEqual(self.migrator.config['container'], cont)
            parts[obj] = True

    @mock.patch('s3_sync.migrator.create_provider')
    def test_migrate_objects(self, create_provider_mock):
        provider = create_provider_mock.return_value
        self.migrator.status.get_migration.return_value = {}

        utcnow = datetime.datetime.utcnow()
        now = (utcnow - s3_sync.migrator.EPOCH).total_seconds()

        tests = [{
            'objects': {
                'foo': {
                    'remote_headers': {
                        'x-object-meta-custom': 'custom',
                        'last-modified': create_timestamp(1.5e9),
                        'x-timestamp': 1499999999.66,
                        'etag': 'f001a4',
                        'Content-Length': '1024'},
                    'expected_headers': {
                        'x-object-meta-custom': 'custom',
                        'x-timestamp': Timestamp(1499999999.66).internal,
                        'etag': 'f001a4',
                        'last-modified': mock.ANY,
                        s3_sync.utils.get_sys_migrator_header('object'): str(
                            Timestamp(1499999999.66).internal),
                        'Content-Length': '1024'},
                    'list-time': create_list_timestamp(1.5e9),
                    'hash': 'etag'
                },
                'bar': {
                    'remote_headers': {
                        'x-object-meta-custom': 'custom',
                        'last-modified': create_timestamp(1.4e9),
                        'etag': 'ba3',
                        'Content-Length': '1024'},
                    'expected_headers': {
                        'x-object-meta-custom': 'custom',
                        'x-timestamp': Timestamp(1.4e9).internal,
                        'etag': 'ba3',
                        'last-modified': mock.ANY,
                        s3_sync.utils.get_sys_migrator_header('object'): str(
                            Timestamp(1.4e9).internal),
                        'Content-Length': '1024'},
                    'list-time': create_list_timestamp(1.4e9),
                    'hash': 'etag'
                }
            },
        }, {
            'objects': {
                'foo': {
                    'remote_headers': {
                        'x-object-meta-custom': 'custom',
                        'last-modified': create_timestamp(1.5e9),
                        'etag': 'f001a4',
                        'Content-Length': '1024'},
                    'expected_headers': {
                        'x-object-meta-custom': 'custom',
                        'x-timestamp': Timestamp(1.5e9).internal,
                        'etag': 'f001a4',
                        'last-modified': mock.ANY,
                        s3_sync.utils.get_sys_migrator_header('object'): str(
                            Timestamp(1.5e9).internal),
                        'Content-Length': '1024'},
                    'list-time': create_list_timestamp(1.5e9),
                    'hash': 'etag'
                },
                'bar': {
                    'remote_headers': {
                        'x-object-meta-custom': 'custom',
                        'last-modified': create_timestamp(1.4e9),
                        'etag': 'ba3',
                        'Content-Length': '1024'},
                    'expected_headers': {
                        'x-object-meta-custom': 'custom',
                        'x-timestamp': Timestamp(1.4e9).internal,
                        'etag': 'ba3',
                        'last-modified': mock.ANY,
                        s3_sync.utils.get_sys_migrator_header('object'): str(
                            Timestamp(1.4e9).internal),
                        'Content-Length': '1024'},
                    'list-time': create_list_timestamp(1.4e9),
                    'hash': 'etag'
                }
            },
            'config': {
                'aws_bucket': 'container',
                'protocol': 'swift'
            }
        }, {
            'objects': {
                'foo': {
                    'remote_headers': {
                        'x-object-meta-custom': 'custom',
                        'last-modified': create_timestamp(1.5e9),
                        'etag': 'f001a4',
                        'Content-Length': '1024'},
                    'expected_headers': {
                        'x-object-meta-custom': 'custom',
                        'x-timestamp': Timestamp(1.5e9).internal,
                        'etag': 'f001a4',
                        'last-modified': mock.ANY,
                        s3_sync.utils.get_sys_migrator_header('object'): str(
                            Timestamp(1.5e9).internal),
                        'Content-Length': '1024'},
                    'list-time': create_list_timestamp(1.5e9),
                    'hash': 'etag'
                },
                'bar': {
                    'remote_headers': {
                        'x-object-meta-custom': 'custom',
                        'last-modified': create_timestamp(1.4e9),
                        'etag': 'ba3',
                        'Content-Length': '1024'},
                    'expected_headers': {
                        'x-object-meta-custom': 'custom',
                        'x-timestamp': Timestamp(1.4e9).internal,
                        'etag': 'ba3',
                        'last-modified': mock.ANY,
                        'Content-Length': '1024',
                        s3_sync.utils.get_sys_migrator_header('object'): str(
                            Timestamp(1.4e9).internal)},
                    'list-time': create_list_timestamp(1.4e9),
                    'hash': 'etag'
                }
            },
            'config': {
                'aws_bucket': 'container',
                'protocol': 'swift'
            },
            'local_objects': [
                {'name': 'foo',
                 'last_modified': create_list_timestamp(1.5e9),
                 'hash': 'etag'},
                {'name': 'bar',
                 'last_modified': create_list_timestamp(1.4e9),
                 'hash': 'etag'}
            ],
            'migrated': []
        }, {
            'objects': {
                'foo': {
                    'remote_headers': {
                        'x-object-meta-custom': 'custom',
                        'last-modified': create_timestamp(1.5e9),
                        'etag': 'f001a4',
                        'Content-Length': '1024'},
                    'expected_headers': {
                        'x-object-meta-custom': 'custom',
                        'x-timestamp': Timestamp(1.5e9).internal,
                        'etag': 'f001a4',
                        'last-modified': mock.ANY,
                        'Content-Length': '1024',
                        s3_sync.utils.get_sys_migrator_header('object'): str(
                            Timestamp(1.5e9).internal)},
                    'list-time': create_list_timestamp(1.5e9),
                    'hash': 'etag'
                },
                'bar': {
                    'remote_headers': {
                        'x-object-meta-custom': 'custom',
                        'last-modified': create_timestamp(1.4e9),
                        'etag': 'ba3',
                        'Content-Length': '1024'},
                    'expected_headers': {
                        'x-object-meta-custom': 'custom',
                        'x-timestamp': Timestamp(1.4e9).internal,
                        'etag': 'ba3',
                        'last-modified': mock.ANY,
                        'Content-Length': '1024',
                        s3_sync.utils.get_sys_migrator_header('object'): str(
                            Timestamp(1.4e9).internal)},
                    'list-time': create_list_timestamp(1.4e9),
                    'hash': 'etag'
                }
            },
            'config': {
                'aws_bucket': 'container',
                'protocol': 'swift'
            },
            'local_objects': [
                {'name': 'foo',
                 'last_modified': create_list_timestamp(1.4e9),
                 'hash': 'old-etag'},
                {'name': 'bar',
                 'last_modified': create_list_timestamp(1.3e9),
                 'hash': 'old-etag'}
            ],
            'migrated': ['foo', 'bar']
        }, {
            'objects': {
                'foo': {
                    'remote_headers': {
                        'x-object-meta-custom': 'custom',
                        'last-modified': create_timestamp(1.5e9),
                        'etag': 'f001a4',
                        'Content-Length': '1024'},
                    'expected_headers': {
                        'x-object-meta-custom': 'custom',
                        'x-timestamp': Timestamp(1.5e9).internal,
                        'etag': 'f001a4',
                        'last-modified': mock.ANY,
                        'Content-Length': '1024',
                        s3_sync.utils.get_sys_migrator_header('object'): str(
                            Timestamp(1.5e9).internal)},
                    'list-time': create_list_timestamp(1.5e9),
                    'hash': 'etag'
                },
                'bar': {
                    'remote_headers': {
                        'x-object-meta-custom': 'custom',
                        'last-modified': create_timestamp(now - 35.0),
                        'etag': 'ba3',
                        'Content-Length': '1024'},
                    'expected_headers': {
                        'x-object-meta-custom': 'custom',
                        'etag': 'ba3',
                        'last-modified': mock.ANY,
                        'x-timestamp':
                            Timestamp(math.floor(now - 35.0)).internal,
                        'Content-Length': '1024',
                        s3_sync.utils.get_sys_migrator_header('object'): str(
                            Timestamp(math.floor(now - 35.0)).internal)},
                    'list-time': create_list_timestamp(now - 35.0),
                    'hash': 'etag'
                },
                'baz': {
                    'remote_headers': {
                        'x-object-meta-custom': 'custom',
                        'last-modified': create_timestamp(now),
                        'etag': 'ba33',
                        'Content-Length': '1024',
                    },
                    'expected_headers': {
                        'x-object-meta-custom': 'custom',
                        'etag': 'ba33',
                        'last-modified': mock.ANY,
                        'x-timestamp': Timestamp(math.floor(now)).internal,
                        'Content-Length': '1024',
                        s3_sync.utils.get_sys_migrator_header('object'): str(
                            Timestamp(math.floor(now)).internal)},
                    'list-time': create_list_timestamp(math.floor(now)),
                    'hash': 'etag'
                }
            },
            'config': {
                'aws_bucket': 'container',
                'protocol': 'swift',
                'older_than': 30,
            },
            'local_objects': [
                {'name': 'foo',
                 'last_modified': create_list_timestamp(1.4e9),
                 'hash': 'old-etag'},
                {'name': 'bar',
                 'last_modified': create_list_timestamp(1.3e9),
                 'hash': 'old-etag'}
            ],
            'migrated': ['foo', 'bar']
        }]
        config = self.migrator.config
        self.migrator._read_account_headers = mock.Mock(return_value={})

        for test in tests:
            objects = test['objects']
            if 'migrated' not in test:
                migrated = objects.keys()
            else:
                migrated = test['migrated']
            test_config = dict(config)
            for k, v in test.get('config', {}).items():
                test_config[k] = v
                if k == 'aws_bucket':
                    test_config['container'] = v
            self.migrator.config = test_config

            provider.reset_mock()
            self.swift_client.reset_mock()
            self.swift_client.container_exists.return_value = True
            provider.head_account.return_value = {}

            local_objects = test.get('local_objects', [])

            def head_object(name, **args):
                if name not in objects.keys():
                    raise RuntimeError('Unknown object: %s' % name)
                if name not in migrated:
                    raise RuntimeError('Object should not be moved %s' % name)
                return ProviderResponse(
                    True, 200, objects[name]['remote_headers'],
                    StringIO('object body'))

            provider.list_objects.return_value = ProviderResponse(
                True, 200, {},
                [{'name': name,
                  'last_modified': objects[name]['list-time'],
                  'hash': objects[name]['hash']}
                 for name in objects.keys()])
            provider.head_bucket.return_value = mock.Mock(
                status=200, headers={})
            provider.head_object.side_effect = head_object
            self.swift_client.es_iter_objects.return_value =\
                self._iter_yield_none(local_objects)

            self.migrator.next_pass()

            self.swift_client.upload_object.assert_has_calls(
                [mock.call(self.migrator.config, mock.ANY,
                           self.migrator.config['account'],
                           self.migrator.config['aws_bucket'], name,
                           objects[name]['expected_headers'])
                 for name in migrated])

    @mock.patch('s3_sync.migrator.create_provider')
    def test_migrate_slo(self, create_provider_mock):
        self.migrator.config['protocol'] = 'swift'
        provider = create_provider_mock.return_value
        self.migrator.status.get_migration.return_value = {}
        segments_container = '/slo-segments'

        manifest = [{'name': '/'.join([segments_container, 'part1'])},
                    {'name': '/'.join([segments_container, 'part2'])}]
        manifest_etag = hashlib.md5(json.dumps(manifest)).hexdigest()
        objects = {
            'slo': {
                'remote_headers': {
                    'x-object-meta-custom': 'slo-meta',
                    'last-modified': create_timestamp(1.5e9),
                    'x-static-large-object': 'True',
                    'Content-Length': str(len(json.dumps(manifest))),
                    'etag': manifest_etag},
                'expected_headers': {
                    'x-object-meta-custom': 'slo-meta',
                    'x-timestamp': Timestamp(1.5e9).internal,
                    'x-static-large-object': 'True',
                    'Content-Length': str(len(json.dumps(manifest))),
                    'etag': manifest_etag,
                    'last-modified': mock.ANY,
                    s3_sync.utils.get_sys_migrator_header('object'): str(
                        Timestamp(1.5e9).internal)}
            },
            'part1': {
                'remote_headers': {
                    'x-object-meta-part': 'part-1',
                    'last-modified': create_timestamp(1.4e9),
                    'etag': 'part1',
                    'Content-Length': '1024'},
                'expected_headers': {
                    'x-object-meta-part': 'part-1',
                    'x-timestamp': Timestamp(1.4e9).internal,
                    'etag': 'part1',
                    'last-modified': mock.ANY,
                    s3_sync.utils.get_sys_migrator_header('object'): str(
                        Timestamp(1.4e9).internal),
                    'Content-Length': '1024'}
            },
            'part2': {
                'remote_headers': {
                    'x-object-meta-part': 'part-2',
                    'last-modified': create_timestamp(1.1e9),
                    'etag': 'part2',
                    'Content-Length': '1024'},
                'expected_headers': {
                    'x-object-meta-part': 'part-2',
                    'x-timestamp': Timestamp(1.1e9).internal,
                    'etag': 'part2',
                    'last-modified': mock.ANY,
                    s3_sync.utils.get_sys_migrator_header('object'): str(
                        Timestamp(1.1e9).internal),
                    'Content-Length': '1024'}
            }
        }

        containers = {segments_container[1:]: False,
                      self.migrator.config['container']: False}

        swift_404_resp = mock.Mock()
        swift_404_resp.status_int = 404

        class FakeESResponse:
            def __init__(self, status_int):
                self.status_int = status_int

        class FakeESRequest:
            def __init__(self, status_int):
                self.status_int = status_int

            def get_response(self):
                return FakeESResponse(self.status_int)

        def create_container(config, account, container, headers={}):
            containers[container] = True
            return FakeESRequest(200)

        def container_exists(config, account, container):
            return containers[container]

        def get_container_metadata(config, account, container):
            if not containers[container]:
                raise UnexpectedResponse('', swift_404_resp)

        def head_object(name, bucket=None):
            if bucket != 'bucket':
                raise RuntimeError('wrong container: %s' % bucket)
            if name is not 'slo':
                raise RuntimeError(
                    'attempt to migrate segment object: %s' % name)
            resp = mock.Mock()
            resp.status = 200
            resp.headers = objects[name]['remote_headers']
            return resp

        def upload_object(config, body, account, container, key, headers):
            if not containers[container]:
                raise UnexpectedResponse('', swift_404_resp)

        self.swift_client.create_container.side_effect = create_container
        self.swift_client.container_exists.side_effect = container_exists
        self.swift_client.get_container_metadata.side_effect = \
            get_container_metadata
        self.swift_client.get_object_metadata.side_effect = UnexpectedResponse(
            '', swift_404_resp)
        self.swift_client.upload_object.side_effect = upload_object
        self.migrator._read_account_headers = mock.Mock(return_value={})

        bucket_resp = mock.Mock()
        bucket_resp.status = 200
        bucket_resp.headers = {}

        provider.head_account.return_value = {}
        provider.head_bucket.return_value = bucket_resp
        provider.list_objects.return_value = ProviderResponse(
            True, 200, {}, [{'name': 'slo'}])
        provider.head_object.side_effect = head_object
        self.swift_client.es_iter_objects.return_value =\
            self._iter_yield_none([])

        self.migrator.next_pass()

        self.swift_client.upload_object.assert_has_calls(
            [mock.call(mock.ANY, mock.ANY, self.migrator.config['account'],
                       self.migrator.config['container'],
                       'slo',
                       objects['slo']['expected_headers'])])

        class NotAnAssertionError(Exception):
            pass

        try:
            self.swift_client.upload_object.assert_has_calls(
                [mock.call(mock.ANY, mock.ANY, self.migrator.config['account'],
                           'slo-segments',
                           'part1',
                           objects['part1']['expected_headers']),
                 mock.call(mock.ANY, mock.ANY, self.migrator.config['account'],
                           'slo-segments',
                           'part2',
                           objects['part2']['expected_headers'])])
            raise NotAnAssertionError()
        except AssertionError:
            pass
        except NotAnAssertionError:
            raise AssertionError(
                "Metadata migrator should not migrate SLO segments")

        parts = {'slo': False}
        for call in self.swift_client.upload_object.mock_calls:
            config, body, acct, cont, obj, headers = call[1]
            if parts[obj]:
                continue
            self.assertEqual(self.migrator.config['container'], cont)
            parts[obj] = True

    @mock.patch('s3_sync.migrator.create_provider')
    def test_missing_container(self, create_provider_mock):
        tests = [
            ({'protocol': 'swift'}, {'x-container-meta-foo': 'foo'}),
            ({}, {}),
            ({'protocol': 'swift'}, {'x-container-read': '.r*',
                                     'x-container-write': 'AUTH_bob'})
        ]
        provider = create_provider_mock.return_value
        config = self.migrator.config
        self.migrator._read_account_headers = mock.Mock(return_value={})

        for test_config, container_headers in tests:
            self.migrator.config = dict(config, **test_config)

            provider.reset_mock()
            self.swift_client.reset_mock()
            provider.head_account.return_value = {}
            provider.list_objects.return_value = ProviderResponse(
                True, 200, {}, [{'name': 'test'}])
            provider.get_object.return_value = ProviderResponse(
                True, 200, {'last-modified': create_timestamp(1.5e9)},
                StringIO(''))

            if self.migrator.config.get('protocol') == 'swift':
                resp = mock.Mock()
                resp.status = 200
                resp.headers = container_headers
                provider.head_bucket.return_value = resp
                swift_404_resp = mock.Mock()
                swift_404_resp.status_int = 404

                def fake_get_metadata(config, account, container):
                    raise UnexpectedResponse('', swift_404_resp)

                self.swift_client.get_container_metadata.side_effect = \
                    fake_get_metadata
                self.swift_client.container_exists.side_effect = (True,)
            else:
                self.swift_client.container_exists.side_effect = (False, True)

            self.swift_client.es_iter_objects.return_value = iter([])
            self.migrator.status.get_migration.return_value = {}

            self.migrator.next_pass()
            if test_config.get('protocol') == 'swift':
                provider.list_objects.assert_called_once_with(
                    '', self.migrator.work_chunk, '', bucket='bucket')
                provider.head_bucket.assert_has_calls(
                    [mock.call(self.migrator.config['container'])] * 2)
            else:
                provider.list_objects.assert_called_once_with(
                    '', self.migrator.work_chunk, '', bucket='bucket')

    @mock.patch('s3_sync.migrator.create_provider')
    def test_paginate_migration_listings(self, create_provider_mock):
        self.migrator.status.get_migration.return_value = {
            'marker': 'bar'}
        provider = create_provider_mock.return_value
        provider.list_objects.return_value = mock.Mock(status=200, body=[])
        self.swift_client.es_iter_items.return_value =\
            self._iter_yield_none([])

        self.migrator.next_pass()

        self.swift_client.es_iter_items.assert_has_calls(
            [mock.call(
                self.migrator.config,
                self.migrator.config['account'],
                self.migrator.config['container'],
                'bar',
                mock.ANY),
             mock.call(
                self.migrator.config,
                self.migrator.config['account'],
                self.migrator.config['container'],
                '',
                mock.ANY)])
        provider.list_objects.assert_has_calls(
            [mock.call(
                'bar', 1000, '', bucket=self.migrator.config['aws_bucket']),
             mock.call(
                '', 1000, '', bucket=self.migrator.config['aws_bucket'])])

    @mock.patch('s3_sync.migrator.create_provider')
    def test_reconcile_deleted_object(self, create_provider_mock):
        provider_mock = create_provider_mock.return_value
        provider_mock.list_objects.side_effect = [
            ProviderResponse(True, 200, {}, [{'name': 'qux'}]),
            ProviderResponse(True, 200, {}, [])]
        provider_mock.get_object.return_value = ProviderResponse(
            True, 200,
            {'last-modified': create_timestamp(1.5e9),
             'etag': 'deadbeef',
             'Content-Length': str(2**10)},
            [])

        def _head_object(key, **kwargs):
            resp = mock.Mock()
            resp.headers = {
                'etag': 'deadbeef',
                'last-modified': create_timestamp(1.5e9)}
            resp.status = 200
            resp.body = 'nothing'
            return resp
        provider_mock.head_object = _head_object

        swift_404_resp = mock.Mock()
        swift_404_resp.status_int = 404
        self.migrator.status.get_migration.return_value = {}

        self.swift_client.es_iter_items.return_value = self._iter_yield_none(
            [{"name": "foo"}, None])

        self.swift_client.get_object_metadata.side_effect = UnexpectedResponse(
            '', swift_404_resp)

        self.migrator.next_pass()

        internal_header = s3_sync.utils.get_sys_migrator_header('object')

        self.swift_client.upload_object.assert_called_once_with(
            self.migrator.config,
            mock.ANY,
            self.migrator.config['account'],
            self.migrator.config['aws_bucket'],
            'qux',
            {internal_header: '1500000000.00000',
             'x-timestamp': '1500000000.00000',
             'etag': 'deadbeef',
             'last-modified': mock.ANY})

    def test_reconcile_deleted_timestamps(self):
        internal_header = s3_sync.utils.get_sys_migrator_header('object')
        tests = [
            ({
                'x-timestamp': '1500000000.00000',
                'x-backend-timestamp': '1500000001.00000_0000000000000001',
                'x-backend-durable-timestamp':
                    '1500000002.00000_0000000000000001',
                'last-modified': 'Fri, 14 Jul 2017 02:40:10 GMT',
                internal_header: '1500000000.00000',
            },
                '1500000002.00000_0000000000000002'),
            ({
                'x-timestamp': '1500000000.00000',
                'x-backend-timestamp': '1500000001.00000_0000000000000001',
                'last-modified': 'Fri, 14 Jul 2017 02:40:10 GMT',
                internal_header: '1500000000.00000',
            },
                '1500000001.00000_0000000000000002'),
            ({
                'x-timestamp': '1500000000.00000',
                'last-modified': 'Fri, 14 Jul 2017 02:40:10 GMT',
                internal_header: '1500000000.00000',
            },
                '1500000000.00000_0000000000000001'),
            ({
                'last-modified': 'Fri, 14 Jul 2017 02:40:10 GMT',
                internal_header: '1500000000.00000',
            },
                '1500000010.00000_0000000000000001'),
            ({
                internal_header: '1500000000.00000',
            },
                None),
        ]
        self.swift_client.delete_object.return_value = \
            ProviderResponse(True, 204, {}, [])

        for obj_headers, want in tests:

            self.swift_client.get_object_metadata.return_value = obj_headers
            self.migrator._reconcile_deleted_objects('foo', 'bar')

            hdrs = {}
            if want:
                hdrs = {'x-timestamp': want}
            self.assertEqual(
                self.swift_client.delete_object.call_args,
                mock.call(self.migrator.config,
                          self.migrator.config['account'],
                          'foo',
                          'bar',
                          hdrs))

    @mock.patch('s3_sync.migrator.create_provider')
    def test_reconcile_deleted_container(self, create_provider_mock):
        self.migrator.config['aws_bucket'] = '/*'
        create_provider_mock.list_buckets.return_value = \
            iter([ProviderResponse(True, 200, [], [])])

        self.migrator.status.get_migration.return_value = {}

        self.swift_client.es_iter_items.return_value = self._iter_yield_none(
            [{"name": "bucket"}, None])
        self.swift_client.get_container_metadata.return_value = {
            s3_sync.utils.get_sys_migrator_header('container'):
                s3_sync.utils.MigrationContainerStates.MIGRATING}

        self.migrator.next_pass()
        self.swift_client.delete_container.assert_called_once_with(
            self.migrator.config,
            self.migrator.config['account'],
            "bucket")

    @mock.patch('s3_sync.migrator.create_provider')
    def test_reconcile_already_deleted_container(self, create_provider_mock):
        self.migrator.config['aws_bucket'] = '/*'
        create_provider_mock.list_buckets.return_value = \
            iter([ProviderResponse(True, 200, [], [])])

        self.migrator.status.get_migration.return_value = {}

        self.swift_client.es_iter_items.return_value = self._iter_yield_none(
            [{"name": "bucket"}, None])

        swift_404_resp = mock.Mock()
        swift_404_resp.status_int = 404

        def _get_container_metadata(config, account, container):
            raise UnexpectedResponse('', swift_404_resp)
        self.swift_client.get_container_metadata.side_effect =\
            _get_container_metadata

        self.migrator.next_pass()
        self.assertEqual('Container %s/%s already removed' % (
            self.migrator.config['account'], "bucket"),
            self.stream.getvalue().splitlines()[1])

    @mock.patch('s3_sync.migrator.create_provider')
    def test_reconcile_deleted_container_error(self, create_provider_mock):
        self.migrator.config['aws_bucket'] = '/*'
        create_provider_mock.list_buckets.return_value = \
            iter([ProviderResponse(True, 200, [], [])])

        self.migrator.status.get_migration.return_value = {}

        self.swift_client.es_iter_items.return_value = self._iter_yield_none(
            [{"name": "bucket"}, None])

        swift_500_resp = mock.Mock()
        swift_500_resp.status_int = 500

        def _get_container_metadata(config, account, container):
            raise UnexpectedResponse('', swift_500_resp)
        self.swift_client.get_container_metadata.side_effect =\
            _get_container_metadata

        self.migrator.next_pass()
        self.assertEqual('Failed to delete container "%s/%s"' % (
            self.migrator.config['account'], "bucket"),
            self.stream.getvalue().splitlines()[1])

    @mock.patch('s3_sync.migrator.create_provider')
    def test_reconcile_client_created_container(self, create_provider_mock):
        self.migrator.config['aws_bucket'] = '/*'
        create_provider_mock.list_buckets.return_value = \
            iter([ProviderResponse(True, 200, [], [])])

        self.migrator.status.get_migration.return_value = {}

        self.swift_client.es_iter_items.return_value = self._iter_yield_none(
            [{"name": "bucket"}, None])

        self.swift_client.get_container_metadata.return_value = {}

        self.migrator.next_pass()
        self.assertEqual('Not removing container %s/%s: ' % (
            self.migrator.config['account'], "bucket") +
            'created by a client.',
            self.stream.getvalue().splitlines()[1])


class TestMetadataMigratorStatus(test_migrator.TestStatus):
    pass


class TestMain(unittest.TestCase):

    def setUp(self):
        self.logger = logging.getLogger()
        self.stream = StringIO()
        self.logger.addHandler(logging.StreamHandler(self.stream))
        self.conf = {
            'metadata_migrations': [],
            'migration_status': mock.Mock(),
            'internal_pool': None,
            'logger': self.logger,
            'items_chunk': None,
            'workers': 5,
            'node_id': 0,
            'nodes': 1,
            'poll_interval': 30,
            'once': True,
        }

    @contextmanager
    def patch(self, name, **kwargs):
        with mock.patch('s3_sync.metadata_migrator.' + name,
                        **kwargs) as mocked:
            yield mocked

    def pop_log_lines(self):
        lines = self.stream.getvalue()
        self.stream.seek(0)
        self.stream.truncate()
        return lines

    def test_run_once(self):
        start = time.time()
        with self.patch('time') as mocktime:
            mocktime.time.side_effect = [start, start + 1]
            s3_sync.metadata_migrator.run(**self.conf)
            # with once = True we don't sleep
            self.assertEqual(mocktime.sleep.call_args_list, [])
            self.assertEqual('Finished cycle in 1.00s\n',
                             self.pop_log_lines())

    def test_run_forever(self):
        start = time.time()
        self.conf['once'] = False

        class StopDeamon(Exception):
            pass

        with self.patch('process_migrations') as mock_process, \
                self.patch('time') as mocktime:
            mock_process.side_effect = [None, None, StopDeamon()]
            mocktime.time.side_effect = [start + i for i in range(5)]
            with self.assertRaises(StopDeamon):
                s3_sync.metadata_migrator.run(**self.conf)
            self.assertEqual(mocktime.sleep.call_args_list,
                             [mock.call(29)] * 2)
            self.assertEqual([
                'Finished cycle in 1.00s, sleeping for 29.00s.',
                'Finished cycle in 1.00s, sleeping for 29.00s.',
            ], self.pop_log_lines().splitlines())

    def test_conf_parsing(self):
        config = {
            'metadata_migrator_settings': {
                'workers': 1337,
                'items_chunk': 42,
                'status_file': '/test/status',
                'poll_interval': 60,
                'process': 0,
                'processes': 15,
            },
            'metadata_migrations': [
                {'aws_bucket': 'test_bucket',
                 'account': 'AUTH_test',
                 'aws_identity': 'identity',
                 'aws_secret': 'secret',
                 'container': 'dst-container',
                 'es_hosts': 'http://es.swift.lab:9200',
                 'index_prefix': 'metadata_migrator_'}
            ],
            'swift_dir': '/foo/bar/swift',
        }

        old_run = s3_sync.metadata_migrator.run
        with self.patch('setup_context') as mock_setup_context,\
                self.patch('MetadataMigrator') as mock_migrator,\
                self.patch('Status') as mock_status,\
                self.patch(
                    'run',
                    new_callable=lambda: mock.Mock(side_effect=old_run))\
                as mock_run:
            mock_setup_context.return_value = (
                mock.Mock(log_level='warn', console=True, once=True),
                config)

            s3_sync.metadata_migrator.main()
            mock_status.assert_called_once_with('/test/status')
            mock_migrator.assert_called_once_with(
                config['metadata_migrations'][0],
                mock_status.return_value, 42, 1337,
                mock.ANY, mock.ANY, 0, 15)
            mock_run.assert_called_once_with(
                config['metadata_migrations'],
                mock_status.return_value, mock.ANY,
                mock.ANY, 42, 1337, 0, 15, 60, True)

    @mock.patch('s3_sync.migrator.create_provider')
    def test_migrate_all_containers_error(self, create_provider_mock):
        provider_mock = mock.Mock()
        provider_mock.list_buckets.side_effect = RuntimeError('Failed to list')
        create_provider_mock.return_value = provider_mock
        migrations = [
            {'account': 'AUTH_dev',
             'aws_bucket': '/*',
             'aws_identity': 'identity',
             'aws_secret': 'secret',
             'es_hosts': 'http://null:9200',
             'index_prefix': 'index_'}
        ]

        status = s3_sync.metadata_migrator.Status('status-path')
        old_list = [
            {'account': 'AUTH_dev',
             'es_hosts': 'http://null:9200',
             'index_prefix': 'index_',
             'aws_bucket': 'bucket1',
             'container': 'bucket1',
             'aws_identity': 'identity'},
            {'account': 'AUTH_dev',
             'es_hosts': 'http://null:9200',
             'index_prefix': 'index_',
             'aws_bucket': 'bucket2',
             'container': 'bucket2',
             'aws_identity': 'identity'}]

        def fake_load_list():
            status.status_list = [dict(entry) for entry in old_list]

        status.save_status_list = mock.Mock()
        status.load_status_list = mock.Mock(side_effect=fake_load_list)

        s3_sync.metadata_migrator.run(
            migrations, status, mock.Mock(max_size=10), logging.getLogger(),
            1000, 10, 0, 1, 10, True)
        self.assertEqual(old_list, status.status_list)


class TestESInternalClient(unittest.TestCase):
    def setUp(self):
        self.config = {
            'account': 'AUTH_swiftaccount',
            'aws_bucket': 'bucket',
            'aws_identity': 'identity',
            'aws_secret': 'secret',
            'container': 'swift-container',
            'index_prefix': 'index_prefix_',
            'es_hosts': 'http://elastic:9200',
            'parse_json': True,
            'older_than': 0,
            'prefix': "",
            'propagate_account_metadata': False,
            'protocol': 's3',
            'remote_account': ""}
        self.elasticsearch = mock.Mock()
        self.logger = logging.getLogger()
        self.stream = StringIO()
        self.logger.addHandler(logging.StreamHandler(self.stream))
        self.swift_dir = '/etc/swift'
        self.es_client =\
            s3_sync.metadata_migrator.ESInternalClient(
                self.config,
                self.swift_dir, self.logger)
        self.es_client._verified_indices = {}

        self.elasticsearch = mock.Mock()
        self.es_object = {'_source': {
                          'x-swift-object': 'a-swift-object',
                          'x-swift-container': 'a-swift-container',
                          'x-swift-account': 'a-swift-account',
                          'etag': 'etag',
                          'content-type': 'application/foo',
                          'foo': 'bar',
                          'content-length': '100',
                          'last-modified': '1534426198000',
                          'content-location': 'Storage;bucket;prefix'}}
        self.es_converted_object = {
            'hash': 'etag',
            'name': 'a-swift-object',
            'bytes': '100',
            'last_modified': '2018-08-16T13:29:58.000000',
            'content_type': 'application/foo',
            'content-location': 'Storage;bucket;prefix'}
        self.es_remote_object = {
            'hash': 'etag',
            'name': 'a-remote-object',
            'bytes': '100',
            'last_modified': '2018-08-16T13:29:58.000000',
            'content_type': 'application/foo',
            'foo': 'bar',
            'content-location': 'Storage;bucket;prefix'}
        self.es_remote_headers = {
            'etag': 'etag',
            'Content-Length': '100',
            'last-modified': 'Mon, 20 Nov 1995 19:12:08 -0500',
            'x-timestamp': '1534426198',
            'foo': 'bar'}

        self.elasticsearch.get.return_value = self.es_object
        self.elasticsearch.delete.return_value = self.es_object
        self.elasticsearch.create.return_value = self.es_object
        self.elasticsearch.index.return_value = self.es_object
        self.elasticsearch.indices.exists.return_value = True
        self.elasticsearch.indices.delete.return_value = 'dog'
        self.elasticsearch.indices.put_mapping.return_value = 'dog'
        self.elasticsearch.indices.get_mapping.return_value = {
            'index_prefix_account_container': {
                'mappings': {
                    'object': {
                        '_meta': {
                            'foo': 'bar'
                        },
                        'properties': {
                            'foo': 'text'
                        }
                    }
                }
            }
        }

        self.es_client.__get_es_conn = self.es_client._get_es_conn
        self.es_client._get_es_conn =\
            mock.Mock(return_value=self.elasticsearch)

    def test_get_es_conn(self):
        class mock_es:
            def __init__(self):
                pass

            def info(self):
                return {'version': {'number': '6.0.0'}}

        with mock.patch('elasticsearch.Elasticsearch',
                        return_value=mock_es()) as es_mock:
                self.es_client.__get_es_conn(self.config)
                es_mock.assert_called_once_with(
                    'http://elastic:9200',
                    ca_certs=None,
                    verify_certs=True)
                self.es_client.__get_es_conn(self.config)
                es_mock.assert_called_once()

    def test_get_object_metadata(self):
        resp = self.es_client.get_object_metadata(
            self.config, 'account', 'container', 'key')
        self.elasticsearch.get.assert_called_once_with(
            doc_type='object',
            id=mock.ANY,
            index="%saccount_container" % self.config['index_prefix']
        )
        self.assertEqual(resp, self.es_converted_object)

    def test_get_object_metadata_error(self):
        self.elasticsearch.get.side_effect = (Exception('barfs'))
        with self.assertRaises(s3_sync.metadata_migrator.ESUnexpectedResponse):
            self.es_client.get_object_metadata(self.config,
                                               'account',
                                               'container', 'key')
            self.elasticsearch.get.assert_called_once_with(
                doc_type='object',
                id=mock.ANY,
                index="%saccount_container" % self.config['index_prefix']
            )

    def test_delete_object(self):
        self.es_client.delete_object(self.config, 'account', 'container',
                                     'key', {})
        self.elasticsearch.delete.assert_called_once_with(
            doc_type='object',
            id=mock.ANY,
            index="%saccount_container" % self.config['index_prefix']
        )

    def test_delete_object_errors(self):
        self.elasticsearch.delete.side_effect = (Exception('barfs'))
        with self.assertRaises(s3_sync.metadata_migrator.ESUnexpectedResponse):
            self.es_client.delete_object(self.config,
                                         'account',
                                         'container', 'key', {})

    def test_upload_object(self):
        self.es_client._verified_mappings = {self.config['es_hosts']: True}
        self.es_client.upload_object(self.config,
                                     'content',
                                     'account',
                                     'container', 'key',
                                     self.es_remote_headers)
        _id = '2eb2d1ac01fa01f8e2d5c36ea26e148a' +\
              'fa8da234170c9c28bd8044beacd4ce67'
        self.elasticsearch.index.assert_called_once_with(
            body={'x-swift-container': 'container',
                  'content-length': '100',
                  'x-swift-account': 'account',
                  'content-location': 'AWS S3;bucket;',
                  'last-modified': 816912728000,
                  'etag': 'etag',
                  'x-timestamp': 1534426198000,
                  'x-object-transient-sysmeta-multi-cloud-internal-migrator':
                      1534426198000,
                  'x-swift-object': u'key'},
            doc_type='object',
            id=_id,
            index=u'index_prefix_account_container',
            pipeline=None)

    def test_upload_object_verifies_mapping(self):
        with mock.patch.object(self.es_client,
                               '_verify_mapping',
                               return_value=True) as vm_mock:
                self.es_client.upload_object(self.config,
                                             'content',
                                             'account',
                                             'container', 'key',
                                             self.es_remote_headers)
                vm_mock.assert_called_once_with(
                    self.config,
                    'index_prefix_account_container')

    def test_upload_object_error(self):
        self.elasticsearch.create.side_effect = (Exception('barfs'))
        with self.assertRaises(s3_sync.metadata_migrator.ESUnexpectedResponse):
            self.es_client.upload_object(self.config,
                                         'content',
                                         'account',
                                         'container', 'key', {})
            self.elasticsearch.create.assert_called_once_with(
                doc_type='object',
                id=mock.ANY,
                index="%saccount_container" % self.config['index_prefix']
            )

    def test_create_container(self):
        self.es_client.create_container(self.config,
                                        'account',
                                        'container', {})
        self.elasticsearch.indices.create.assert_called_once_with(
            body={
                'mappings': {
                    'object': {
                        'properties': {
                            'x-swift-container': {'type': 'string'},
                            'content-length': {'type': 'long'},
                            'x-swift-account': {'type': 'string'},
                            'last-modified': {'type': 'date'},
                            'x-object-manifest': {'type': 'string'},
                            'x-timestamp': {'type': 'date'},
                            'etag': {'index':
                                     'not_analyzed', 'type': 'string'},
                            'x-trans-id': {'index': 'not_analyzed',
                                           'type': 'string'},
                            'x-swift-object': {'type': 'string'},
                            'x-static-large-object': {'type': 'boolean'},
                            'content-type': {'type': 'string'}},
                        '_meta': {}}}},
            index=u'index_prefix_account_container')

    def test_create_container_error(self):
        self.elasticsearch.indices.create.side_effect = (Exception('barfs'))
        resp = self.es_client.create_container(self.config,
                                               'account',
                                               'container', {})
        self.assertIsInstance(
            resp,
            s3_sync.metadata_migrator.ESInternalClientRequest)

    def test_container_exists(self):
        resp = self.es_client.container_exists(self.config,
                                               'account',
                                               'container')
        self.elasticsearch.indices.exists.assert_called_once_with(
            index=u'index_prefix_account_container')

        self.assertEqual(resp, True)

    def test_container_exists_generic_error(self):
        self.elasticsearch.indices.exists.side_effect = (Exception('barfs'))
        with self.assertRaises(s3_sync.metadata_migrator.ESUnexpectedResponse):
            self.es_client.container_exists(self.config,
                                            'account',
                                            'container')

    def test_container_exists_404(self):
        self.elasticsearch.indices.exists.return_value = False
        resp = self.es_client.container_exists(self.config,
                                               'account',
                                               'container')
        self.assertEqual(resp, False)

    def test_get_container_metadata(self):
        resp = self.es_client.get_container_metadata(self.config,
                                                     'account',
                                                     'container')
        self.elasticsearch.indices.get_mapping.assert_called_once_with(
            index=u'index_prefix_account_container')
        self.assertEqual(resp, {'foo': 'bar'})

    def test_get_container_metadata_error(self):
        self.elasticsearch.indices.get_mapping.side_effect =\
            (Exception('barfs'))
        with self.assertRaises(s3_sync.metadata_migrator.ESUnexpectedResponse):
            self.es_client.get_container_metadata(self.config,
                                                  'account',
                                                  'container')

    def test_delete_container(self):
        req = self.es_client.delete_container(self.config,
                                              'account',
                                              'container')
        self.elasticsearch.indices.delete.assert_called_once_with(
            index=u'index_prefix_account_container')
        self.assertIsInstance(
            req,
            s3_sync.metadata_migrator.ESInternalClientRequest)
        self.assertEqual(req.response.status_int, 200)

    def test_delete_container_error(self):
        self.elasticsearch.indices.delete.side_effect = (Exception('barfs'))
        with self.assertRaises(s3_sync.metadata_migrator.ESUnexpectedResponse):
            self.es_client.delete_container(self.config,
                                            'account',
                                            'container')

    def test_set_container_metadata(self):
        req = self.es_client.set_container_metadata(self.config,
                                                    'account',
                                                    'container',
                                                    {'foo': 'bar'})
        self.elasticsearch.indices.get_mapping.assert_called_once_with(
            doc_type='object', index='index_prefix_account_container')
        self.elasticsearch.indices.put_mapping.assert_called_once_with(
            body={'_meta': {'foo': 'bar'}, 'properties': {'foo': 'text'}},
            doc_type='object',
            index=u'index_prefix_account_container')
        self.assertIsInstance(
            req, s3_sync.metadata_migrator.ESInternalClientRequest)
        self.assertEqual(req.response.status_int, 200)

    def test_set_container_metadata_missing_mapping_meta(self):
        with mock.patch.object(
            self.elasticsearch.indices,
            'get_mapping',
            return_value={
                'index_prefix_account_container': {
                    'mappings': {
                        'object': {
                            'properties': {'foo': 'text'}}}}}):
            self.es_client.set_container_metadata(self.config,
                                                  'account',
                                                  'container', {'foo': 'bar'})
            self.elasticsearch.indices.put_mapping.assert_called_once_with(
                body={'_meta': {'foo': 'bar'}, 'properties': {'foo': 'text'}},
                doc_type='object',
                index=u'index_prefix_account_container')

    def test_set_container_metadata_get_mapping_error(self):
        self.elasticsearch.indices.get_mapping.side_effect =\
            (Exception('barfs'))
        req = self.es_client.set_container_metadata(self.config,
                                                    'account',
                                                    'container',
                                                    {'foo': 'bar'})
        self.assertIsInstance(
            req, s3_sync.metadata_migrator.ESInternalClientRequest)
        self.assertEqual(req.response.status_int, 502)

    def test_set_container_metadata_put_mapping_error(self):
        self.elasticsearch.indices.put_mapping.side_effect =\
            (Exception('barfs'))
        req = self.es_client.set_container_metadata(self.config,
                                                    'account',
                                                    'container',
                                                    {'foo': 'bar'})
        self.assertIsInstance(
            req, s3_sync.metadata_migrator.ESInternalClientRequest)
        self.assertEqual(req.response.status_int, 502)

    def test_set_account_meta(self):
        req = self.es_client.set_account_metadata('account', {'foo': 'bar'})
        self.assertIsInstance(
            req, s3_sync.metadata_migrator.ESInternalClientRequest)
        self.assertEqual(req.response.status_int, 200)

    def test_str(self):
        self.assertIsInstance(str(self.es_client), basestring)

    def test_make_request(self):
        with self.assertRaises(NotImplementedError):
            self.es_client.make_request('GET', '/', {}, {}, None, None)

    def test_es_exception_as_request(self):
        expected_requests = {
            elasticsearch.ImproperlyConfigured(): 400,
            elasticsearch.SerializationError(): 422,
            elasticsearch.TransportError('N/A', 'barfed'): None,
            elasticsearch.TransportError(418, 'barfed', {
                'error': {
                    'root_cause': [
                        {'reason': ''}
                    ]
                }
            }): None,
            elasticsearch.ConnectionError('N/A', 'barfed', {
                'error': {
                    'root_cause': [
                        {'reason': ''}
                    ]
                }
            }): 502,
            elasticsearch.ConnectionTimeout('N/A', 'barfed', {
                'error': {
                    'root_cause': [
                        {'reason': ''}
                    ]
                }
            }): 502,
            elasticsearch.SSLError('N/A', 'barfed', {
                'error': {
                    'root_cause': [
                        {'reason': ''}
                    ]
                }
            }): 502,
            elasticsearch.NotFoundError('N/A', 'barfed', {
                'error': {
                    'root_cause': [
                        {'reason': ''}
                    ]
                }
            }): 404,
            elasticsearch.ConflictError('N/A', 'barfed', {
                'error': {
                    'root_cause': [
                        {'reason': ''}
                    ]
                }
            }): 409,
            elasticsearch.RequestError(): 400,
            TypeError(): 502}

        for exception, status_int in expected_requests.items():
            r = self.es_client._es_exception_as_request(exception)
            self.assertIsInstance(
                r, s3_sync.metadata_migrator.ESInternalClientRequest)
            if status_int:
                self.assertEqual(r.response.status_int, status_int)
                self.assertEqual(r.get_response().status_int, status_int)
            else:
                self.assertEqual(r.response.status_int, exception.status_code)
                self.assertEqual(r.get_response().status_int,
                                 exception.status_code)

    def test_es_iteration_selector(self):
        self.es_client.set_account_metadata('account', {'foo': 'bar'})
        with mock.patch.object(self.es_client,
                               '_es_iter_containers', return_value=None) as c:
            with mock.patch.object(self.es_client, '_es_iter_objects',
                                   return_value=None) as o:
                self.es_client.es_iter_items(
                    self.es_client.config, 'AUTH_test', 'bucket', '', '')
                self.es_client.es_iter_items(
                    self.es_client.config, 'AUTH_test', '', '', '')
                self.es_client.es_iter_items(
                    self.es_client.config, 'AUTH_test', None, '', '')
                o.assert_has_calls(
                    [mock.call(self.es_client.config,
                     'AUTH_test', 'bucket', '', '')],
                    [mock.call(self.es_client.config,
                     'AUTH_test', '', '', '')])
                c.assert_has_calls([
                    mock.call(self.es_client.config,
                              'AUTH_test', '', '')])

    def test_es_iter_objects(self):
        self.es_client.SWIFT_PAGINATION_LIMIT = 10

        def _set_es_iter_objects_response(count):
            def _fake_search(index, doc_type, size, body):
                try:
                    start = int(body['search_after'][0])
                except KeyError:
                    start = 0

                es_response = {'hits': {'hits': []}}
                if (start >= count):
                    return es_response
                if (start + self.es_client.SWIFT_PAGINATION_LIMIT) >= count:
                    end = count
                else:
                    end = start + self.es_client.SWIFT_PAGINATION_LIMIT

                internal_header = s3_sync.utils.get_sys_migrator_header(
                    'object')
                for i in range(start + 1, end + 1):
                    resp = {
                        "_id": "id%s" % i,
                        "_index": "index",
                        "_score": 1.0,
                        "_source": {
                            "content-length": i,
                            "content-location": "location%s" % i,
                            "content-type": "content-type",
                            "etag": "etag%s" % i,
                            "last-modified": int(i * 100),
                            internal_header: int(i * 2),
                            "x-swift-account": "account",
                            "x-swift-container": "container",
                            "x-swift-object": "object%s" % i,
                            "x-timestamp": 1533560136000
                        },
                        "_type": "object",
                        "sort": ["%s" % i]
                    }
                    es_response['hits']['hits'].append(resp)

                return es_response

            self.elasticsearch.search.side_effect = _fake_search

        def _get_iter_objects_expected_responses(count):

            def _last_modified(ts):
                return datetime.datetime.utcfromtimestamp(
                    int(int(ts) / 1000)).strftime(SWIFT_TIME_FMT)

            iter_response = []
            for x in range(count + 1):
                i = x + 1
                internal_header = s3_sync.utils.get_sys_migrator_header(
                    'object')
                iter_response.append({
                    "hash": "etag%s" % i,
                    "name": "object%s" % i,
                    "content_type": "content-type",
                    "bytes": i,
                    "last_modified": _last_modified(int(i * 100)),
                    internal_header: int(i * 2),
                    "content-location": "location%s" % i})

            return iter(iter_response)

        def _get_iter_objects_expected_calls(count, marker=None, prefix=None):
            calls = []
            _count = count / self.es_client.SWIFT_PAGINATION_LIMIT
            if (count % self.es_client.SWIFT_PAGINATION_LIMIT):
                _count += 1

            callcount = 0
            for x in range(_count):
                i = x + 1
                body = {
                    "sort": [{"x-swift-object.keyword": {"order": "asc"}}],
                    "query": {
                        "bool": {
                            "filter": [
                                {"match": {"x-swift-account": u'account'}},
                                {"match": {"x-swift-container": u'container'}},
                                {"exists": {"field": "x-swift-object"}}
                            ]
                        }
                    }
                }

                if i > 1:
                    body['search_after'] =\
                        ["%s" % int((i - 1) *
                         self.es_client.SWIFT_PAGINATION_LIMIT)]

                calls.append(
                    mock.call(
                        index=u'index_prefix_account_container',
                        doc_type='object',
                        size=self.es_client.SWIFT_PAGINATION_LIMIT,
                        body=body))
                callcount += 1

            return calls

        for count in [0, 1, 5, 9, 10, 11, 19, 20, 21, 100]:
            _set_es_iter_objects_response(count)
            exp_results = _get_iter_objects_expected_responses(count)

            iterator = self.es_client._es_iter_objects(
                self.es_client.config,
                'account',
                'container',
                None,
                None)

            for i in range(1, count + 1):
                real = iterator.next()
                expected = exp_results.next()
                self.assertEqual(real, expected)

            self.elasticsearch.search.assert_has_calls(
                _get_iter_objects_expected_calls(count))

    def test_es_iter_containers(self):
        self.es_client.SWIFT_PAGINATION_LIMIT = 10

        def _set_es_iter_containers_response(count):

            def _fake_get(index_prefix):
                indices = {}
                for i in range(count + 1):
                    indices["%s%s_%s" % (self.config['index_prefix'],
                            'account', i)] = True
                return indices

            self.elasticsearch.indices.get.side_effect = _fake_get

        def _get_iter_containers_expected_responses(count):
            iter_response = []
            entries = []
            for i in range(count + 1):
                entries.append(str(i))
            for i in sorted(entries):
                iter_response.append({'name': str(i).encode('utf-8')})
            return iter(iter_response)

        def _get_iter_containers_expected_calls(count, marker=None,
                                                prefix=None):
            calls = []
            arg = "%s%s_*" % (self.config['index_prefix'],
                              'account')
            calls.append(mock.call(arg.encode('utf-8')))
            return calls

        for count in [0, 1, 5, 9, 10, 11, 19, 20, 21, 100]:
            _set_es_iter_containers_response(count)
            exp_results = _get_iter_containers_expected_responses(count)

            iterator = self.es_client._es_iter_containers(
                self.es_client.config,
                'account',
                None,
                None)

            for i in range(0, count + 1):
                real = iterator.next()
                expected = exp_results.next()
                self.assertEqual(real, expected)

            self.elasticsearch.indices.get.assert_has_calls(
                _get_iter_containers_expected_calls(count))
