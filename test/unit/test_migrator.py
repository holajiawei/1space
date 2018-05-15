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
import errno
import hashlib
import itertools
import json
import logging
import math
import mock
from s3_sync.base_sync import ProviderResponse
import s3_sync.migrator
from StringIO import StringIO
from swift.common.internal_client import UnexpectedResponse
from swift.common.utils import Timestamp
import time
import unittest
from tempfile import NamedTemporaryFile, mkdtemp
import shutil
import os


def create_timestamp(epoch_ts):
    dt = datetime.datetime.utcfromtimestamp(epoch_ts)
    return dt.strftime(s3_sync.migrator.LAST_MODIFIED_FMT) + ' UTC'


def create_list_timestamp(epoch_ts):
    dt = datetime.datetime.utcfromtimestamp(epoch_ts)
    return dt.strftime(s3_sync.utils.SWIFT_TIME_FMT)


class TestMigratorUtils(unittest.TestCase):
    maxDiff = None

    def test_migration_comparison(self):
        test_cases = [
            ({'account': 'AUTH_account1',
              'aws_bucket': 'bucket',
              'aws_identity': 'id',
              'aws_secret': 'secret'},
             {'account': 'AUTH_account1',
              'aws_bucket': 'bucket',
              'aws_identity': 'id',
              'aws_secret': 'secret',
              'status': {'moved': 100,
                         'scanned': 200}},
             True),
            ({'account': 'AUTH_account2',
              'aws_bucket': 'bucket',
              'aws_identity': 'id',
              'aws_secret': 'secret'},
             {'account': 'AUTH_account2',
              'aws_bucket': 'other_bucket',
              'aws_identity': 'id',
              'aws_secret': 'secret',
              'status': {'moved': 100,
                         'scanned': 200}},
             False),
            ({'account': 'AUTH_account3',
              'aws_bucket': 'bucket',
              'aws_identity': 'id',
              'aws_secret': 'secret'},
             {'account': 'AUTH_account3',
              'aws_bucket': 'bucket',
              'aws_identity': 'id',
              'aws_secret': 'secret',
              'aws_endpoint': 'http://s3-clone',
              'status': {'moved': 100,
                         'scanned': 200}},
             False),
            ({'account': 'swift1',
              'aws_bucket': 'bucket',
              'aws_identity': 'aws access key',
              'aws_secret': 'the secret'},
             {'account': 'swift1',
              'aws_bucket': 'bucket',
              'aws_identity': 'aws access key',
              'status': {'moved': 100,
                         'scanned': 200}},
             True),
            ({'account': 'swift2',
              'aws_bucket': 'bucket',
              'aws_identity': 'aws access key',
              'aws_secret': 'old secret'},
             {'account': 'swift2',
              'aws_bucket': 'bucket',
              'aws_identity': 'aws access key',
              'aws_secret': 'new secret'},
             True),
            ({'account': 'swift3',
              'aws_bucket': 'bucket',
              'aws_identity': 'aws access key',
              'status': {'moved': 100,
                         'scanned': 200}},
             {'account': 'swift3',
              'aws_bucket': '/*',
              'aws_identity': 'aws access key',
              'aws_secret': 'new secret'},
             True),
            ({'account': 'swift4',
              'aws_bucket': 'bucket',
              'container': 'bucket',
              'aws_identity': 'aws access key',
              'aws_secret': 'old secret'},
             {'account': 'swift4',
              'aws_bucket': 'bucket',
              'aws_identity': 'aws access key',
              'status': {'moved': 100,
                         'scanned': 200}},
             True),
        ]
        failures = []
        for left, right, expected in test_cases:
            try:
                self.assertEqual(
                    expected, s3_sync.migrator.equal_migration(left, right))
            except AssertionError:
                failures.append('%s %s %s' % (
                    left, '!=' if expected else '==', right))
            left, right = right, left
            try:
                self.assertEqual(
                    expected, s3_sync.migrator.equal_migration(left, right))
            except AssertionError:
                failures.append('%s %s %s' % (
                    left, '!=' if expected else '==', right))
        if failures:
            self.fail('Unexpected results:\n' + '\n'.join(failures))

    def test_listing_comparison(self):
        test_cases = [
            ({'last_modified': '2000-01-01T00:00:00.00000',
              'hash': 'deadbeef'},
             {'last_modified': '2000-01-01T00:00:00.00000',
              'hash': 'deadbeef'},
             0),
            ({'last_modified': '2000-01-01T00:00:00.00000',
              'hash': 'deadbeef'},
             {'last_modified': '1999-12-31T11:59:59.99999',
              'hash': 'deadbeef'},
             1),
            ({'last_modified': '2000-01-01T00:00:00.00000',
              'hash': 'deadbeef'},
             {'last_modified': '2000-01-01T00:00:00.00000',
              'hash': 'beefdead'},
             s3_sync.migrator.MigrationError),
            ({'last_modified': '2000-01-01T00:00:00.00000',
              'hash': 'deadbeef'},
             {'last_modified': '2000-01-01T00:00:00.00001',
              'hash': 'deadbeef'},
             -1),
        ]
        for left, right, expected in test_cases:
            if type(expected) == int:
                self.assertEqual(
                    expected, s3_sync.migrator.cmp_object_entries(left, right))
            else:
                with self.assertRaises(expected):
                    s3_sync.migrator.cmp_object_entries(left, right)

    @mock.patch('__builtin__.open')
    def test_status_get(self, mock_open):
        mock_file = mock.Mock()
        mock_file.__enter__ = lambda *args: mock_file
        mock_file.__exit__ = lambda *args: None
        mock_open.return_value = mock_file

        test_cases = [
            [{'aws_bucket': 'testbucket',
              'aws_endpoint': '',
              'aws_identity': 'identity',
              'aws_secret': 'secret',
              'account': 'AUTH_test',
              'protocol': 's3',
              'status': {
                  'moved_count': 10,
                  'scanned_count': 20}}],
            [{'aws_bucket': 'testbucket',
              'aws_endpoint': 'http://swift',
              'aws_identity': 'identity',
              'aws_secret': 'secret',
              'account': 'AUTH_test',
              'protocol': 'swift',
              'status': {
                  'moved_count': 123,
                  'scanned_count': 100}}],
        ]
        for test in test_cases:
            mock_file.reset_mock()
            mock_file.read.return_value = json.dumps(test)

            status = s3_sync.migrator.Status('/fake/location')
            self.assertEqual({}, status.get_migration(
                {'aws_identity': '',
                 'aws_secret': ''}))
            for migration in test:
                query = dict(migration)
                del query['status']
                self.assertEqual(
                    migration['status'], status.get_migration(query))

    @mock.patch('__builtin__.open')
    def test_status_get_missing(self, mock_open):
        mock_open.side_effect = IOError(errno.ENOENT, 'missing file')

        status = s3_sync.migrator.Status('/fake/location')
        self.assertEqual({}, status.get_migration({}))

    @mock.patch('__builtin__.open')
    def test_status_get_errors(self, mock_open):
        mock_open.side_effect = IOError(errno.EPERM, 'denied')

        status = s3_sync.migrator.Status('/fake/location')
        with self.assertRaises(IOError) as e:
            status.get_migration({})
        self.assertEqual(errno.EPERM, e.exception.errno)

    @mock.patch('__builtin__.open')
    def test_status_save(self, mock_open):
        mock_file = mock.Mock()
        mock_file.__enter__ = lambda *args: mock_file
        mock_file.__exit__ = lambda *args: None
        mock_open.return_value = mock_file
        start = int(time.time()) + 1

        test_cases = [
            ([{'aws_bucket': 'testbucket',
               'aws_endpoint': '',
               'aws_identity': 'identity',
               'aws_secret': 'secret',
               'account': 'AUTH_test',
               'protocol': 's3',
               'status': {
                   'finished': start - 1,
                   'moved_count': 10,
                   'scanned_count': 20}}],
             {'marker': 'marker', 'moved_count': 1000, 'scanned_count': 1000,
              'reset_stats': False},
             {'finished': start, 'moved_count': 1010, 'scanned_count': 1020}),
            ([{'aws_bucket': 'testbucket',
               'aws_endpoint': '',
               'aws_identity': 'identity',
               'aws_secret': 'secret',
               'account': 'AUTH_test',
               'protocol': 's3',
               'status': {
                   'finished': start - 1,
                   'moved_count': 10,
                   'scanned_count': 20}}],
             {'marker': 'marker', 'moved_count': 1000,
              'scanned_count': 1000, 'reset_stats': True},
             {'finished': start, 'moved_count': 1000, 'scanned_count': 1000,
              'last_finished': start - 1, 'last_moved_count': 10,
              'last_scanned_count': 20}),
            ([],
             {'marker': 'marker', 'finished': start, 'moved_count': 1000,
              'scanned_count': 1000, 'reset_stats': False},
             {'finished': start, 'moved_count': 1000, 'scanned_count': 1000}),
        ]
        for status_list, test_params, write_status in test_cases:
            mock_file.reset_mock()
            status = s3_sync.migrator.Status('/fake/location')
            status.status_list = status_list

            if not status_list:
                migration = {'aws_identity': 'aws id', 'aws_secret': 'secret'}
            else:
                migration = dict(status_list[0])

            with mock.patch('time.time') as mock_time:
                mock_time.return_value = start
                status.save_migration(
                    migration, test_params['marker'],
                    test_params['moved_count'],
                    test_params['scanned_count'],
                    test_params['reset_stats'])
            write_status['marker'] = test_params['marker']
            migration['status'] = write_status
            # gets the 1st argument in the call argument list
            written = ''.join([call[1][0] for call in
                               mock_file.write.mock_calls])
            written_conf = json.loads(written)
            for entry in written_conf:
                self.assertTrue('aws_secret' not in entry)
            del migration['aws_secret']
            self.assertEqual(written_conf, [migration])

    @mock.patch('s3_sync.migrator.os.mkdir')
    @mock.patch('__builtin__.open')
    def test_status_save_create(self, mock_open, mock_mkdir):
        start = int(time.time()) + 1
        mock_file = mock.Mock()
        mock_file.__enter__ = lambda *args: mock_file
        mock_file.__exit__ = lambda *args: None
        mock_open.side_effect = [IOError(errno.ENOENT, 'not found'), mock_file]

        status = s3_sync.migrator.Status('/fake/location')
        status.status_list = []
        with mock.patch('time.time') as mock_time:
            mock_time.return_value = start
            status.save_migration(
                {'aws_identity': 'aws id', 'aws_secret': 'secret'},
                'marker', 100, 100, False)
        mock_mkdir.assert_called_once_with('/fake', mode=0755)
        written = ''.join([call[1][0] for call in mock_file.write.mock_calls])
        self.assertEqual(json.loads(written), [
            {'aws_identity': 'aws id',
             'status': {'marker': 'marker', 'moved_count': 100,
                        'scanned_count': 100, 'finished': start}}])

    @mock.patch('s3_sync.migrator.os.mkdir')
    @mock.patch('__builtin__.open')
    def test_status_save_create_raises(self, mock_open, mock_mkdir):
        mock_file = mock.Mock()
        mock_file.__enter__ = lambda *args: mock_file
        mock_file.__exit__ = lambda *args: None
        mock_open.side_effect = IOError(errno.ENOENT, 'not found')
        mock_mkdir.side_effect = IOError(errno.EPERM, 'denied')

        status = s3_sync.migrator.Status('/fake/location')
        status.status_list = []
        with self.assertRaises(IOError) as cm:
            status.save_migration(
                {'aws_identity': 'aws id', 'aws_secret': 'secret'},
                'marker', 100, 100, False)
        mock_mkdir.assert_called_once_with('/fake', mode=0755)
        self.assertEqual(errno.EPERM, cm.exception.errno)

    @mock.patch('__builtin__.open')
    def test_status_save_raises(self, mock_open):
        mock_open.side_effect = IOError(errno.EPERM, 'denied')
        status = s3_sync.migrator.Status('/fake/location')
        status.status_list = []
        with self.assertRaises(IOError) as err:
            status.save_migration(
                {'aws_identity': 'aws id', 'aws_secret': 'secret'},
                'marker', 100, 100, False)
        self.assertEqual(errno.EPERM, err.exception.errno)

    def setup_status_file_path(self):
        self.temp_file = NamedTemporaryFile()
        self.temp_file.__enter__()
        self.status_file_path = self.temp_file.name
        self.addCleanup(lambda: self.temp_file.__exit__(None, None, None))

    def test_status_prune(self):
        self.setup_status_file_path()
        with open(self.status_file_path, 'w') as wf:
            json.dump([
                {
                    "account": "AUTH_dev",
                    "aws_bucket": "bucket.example.com",
                    "aws_identity": "identity",
                    "aws_secret": "secret",
                    "container": "bucket.example.com",
                    "all_buckets": True,
                    "prefix": "",
                    "protocol": "s3",
                    "remote_account": "",
                    "status": {
                        "finished": 1519177565.416393,
                        "last_finished": 1519177190.048912,
                        "last_moved_count": 0,
                        "last_scanned_count": 1,
                        "marker": "blah",
                        "moved_count": 0,
                        "scanned_count": 1
                    }
                }, {
                    "account": "AUTH_dev2",
                    "aws_bucket": "bucket2.example.com",
                    "aws_identity": "identity",
                    "aws_secret": "secret",
                    "container": "bucket2.example.com",
                    "all_buckets": True,
                    "prefix": "",
                    "protocol": "s3",
                    "remote_account": "",
                    "status": {
                        "finished": 1519177178.41313,
                        "last_finished": 1519177173.780246,
                        "last_moved_count": 1,
                        "last_scanned_count": 1,
                        "marker": "blah",
                        "moved_count": 1,
                        "scanned_count": 1
                    }
                },
            ], wf)
        status = s3_sync.migrator.Status(self.status_file_path)
        status.prune([{
            "account": "AUTH_dev2",
            "aws_bucket": "bucket2.example.com",
            "aws_identity": "identity",
            "aws_secret": "secret",
            "prefix": "",
            "protocol": "s3",
            "remote_account": "",
        }])
        with open(self.status_file_path) as rf:
            self.assertEqual(json.load(rf), json.loads(json.dumps([{
                "account": "AUTH_dev2",
                "aws_bucket": "bucket2.example.com",
                "aws_identity": "identity",
                "aws_secret": "secret",
                "container": "bucket2.example.com",
                "all_buckets": True,
                "prefix": "",
                "protocol": "s3",
                "remote_account": "",
                "status": {
                    "finished": 1519177178.41313,
                    "last_finished": 1519177173.780246,
                    "last_moved_count": 1,
                    "last_scanned_count": 1,
                    "marker": "blah",
                    "moved_count": 1,
                    "scanned_count": 1
                }
            }])))

    def test_prune_all_buckets_migration(self):
        existing_status = json.loads(json.dumps([
            {
                "account": "AUTH_dev",
                "aws_bucket": "bucket.example.com",
                "aws_identity": "identity",
                "aws_secret": "secret",
                "container": "bucket.example.com",
                "prefix": "",
                "protocol": "s3",
                "remote_account": "",
                "status": {
                    "finished": 1519177565.416393,
                    "last_finished": 1519177190.048912,
                    "last_moved_count": 0,
                    "last_scanned_count": 1,
                    "marker": "blah",
                    "moved_count": 0,
                    "scanned_count": 1
                }
            }, {
                "account": "AUTH_dev",
                "aws_bucket": "bucket2.example.com",
                "aws_identity": "identity",
                "aws_secret": "secret",
                "container": "bucket2.example.com",
                "prefix": "",
                "protocol": "s3",
                "remote_account": "",
                "status": {
                    "finished": 1519177178.41313,
                    "last_finished": 1519177173.780246,
                    "last_moved_count": 1,
                    "last_scanned_count": 1,
                    "marker": "blah",
                    "moved_count": 1,
                    "scanned_count": 1
                }
            },
        ]))
        self.setup_status_file_path()
        with open(self.status_file_path, 'w') as wf:
            json.dump(existing_status, wf)
        status = s3_sync.migrator.Status(self.status_file_path)
        status.prune([{
            "account": "AUTH_dev",
            "aws_bucket": "/*",
            "aws_identity": "identity",
            "aws_secret": "secret",
            "prefix": "",
            "protocol": "s3",
            "remote_account": "",
        }])
        with open(self.status_file_path) as rf:
            self.assertEqual(json.load(rf), existing_status)

    @mock.patch('s3_sync.migrator.time')
    def test_status_prune_saved(self, time_mock):
        time_mock.time.return_value = 10000
        self.setup_status_file_path()
        config = {
            "account": "AUTH_dev",
            "aws_bucket": "bucket.example.com",
            "aws_identity": "identity",
            "aws_secret": "secret",
            "container": "bucket.example.com",
            "all_buckets": True,
            "prefix": "",
            "protocol": "s3",
            "remote_account": "",
        }
        status_list = [dict(config)]
        status_list[0]['status'] = {
            "finished": 1000,
            "last_finished": 900,
            "last_moved_count": 0,
            "last_scanned_count": 1,
            "marker": "blah",
            "moved_count": 0,
            "scanned_count": 1
        }
        with open(self.status_file_path, 'w') as wf:
            json.dump(status_list, wf)
        status = s3_sync.migrator.Status(self.status_file_path)
        status.load_status_list()
        migrator = s3_sync.migrator.Migrator(
            config, status, 10, 5, mock.Mock(max_size=1), None, 0, 1)
        self.assertIn('custom_prefix', migrator.config)
        self.assertEqual('', migrator.config['custom_prefix'])
        status.save_migration(migrator.config, 'new-marker', 10, 100, False)
        status.prune([config])
        with open(self.status_file_path) as rf:
            self.assertEqual(json.load(rf), json.loads(json.dumps([{
                "account": "AUTH_dev",
                "aws_bucket": "bucket.example.com",
                "aws_identity": "identity",
                "container": "bucket.example.com",
                "all_buckets": True,
                "prefix": "",
                "protocol": "s3",
                "remote_account": "",
                "status": {
                    "finished": 10000,
                    "last_finished": 900,
                    "last_moved_count": 0,
                    "last_scanned_count": 1,
                    "marker": "new-marker",
                    "moved_count": 10,
                    "scanned_count": 101
                }
            }])))

    def test_status_get_migration(self):
        config = {
            'aws_secret': 'admin',
            'account': 'AUTH_dev',
            'protocol': 'swift',
            'aws_identity': 'dev',
            'prefix': '',
            'container': 'd12738cf0aa74bb1bdb4136f6ca76794_6',
            'aws_endpoint': 'http://192.168.22.101/auth/v1.0',
            'remote_account': '',
            'aws_bucket': 'd12738cf0aa74bb1bdb4136f6ca76794_6',
        }
        self.setup_status_file_path()
        status = s3_sync.migrator.Status(self.status_file_path)
        status.load_status_list()
        status.save_migration(config, 'end', 0, 1000, stats_reset=True)
        status.save_migration(config, 'end', 0, 1000, stats_reset=True)
        with open(self.status_file_path) as rf:
            self.assertEqual(1, len(json.load(rf)))

    @mock.patch('s3_sync.migrator.json.load')
    def test_load_corrupt_json(self, mock_json_load):
        mock_json_load.side_effect = ValueError(
            'No JSON object could be decoded')

        self.setup_status_file_path()
        status = s3_sync.migrator.Status(self.status_file_path)
        status.load_status_list()
        self.assertEqual([], status.status_list)

    def test_load_status(self):
        status_list = [{
            'aws_identity': 'swift1',
            'aws_bucket': 'container1',
            'status': {
                'marker': 'foo',
                'moved_count': 100,
                'scanned_count': 200,
                'finished': 1519177178.41313,
                'last_finished': 1519177173.780246,
                'last_moved_count': 1,
                'last_scanned_count': 1
            }}]
        self.setup_status_file_path()
        with open(self.status_file_path, 'w') as status_file:
            json.dump(status_list, status_file)
        status = s3_sync.migrator.Status(self.status_file_path)
        status.load_status_list()
        self.assertEqual(status_list, status.status_list)

    def test_cmp_meta(self):
        test_cases = [
            ({'last-modified': create_timestamp(1.5e9),
              'etag': 'foo'},
             {'last-modified': create_timestamp(1.5e9),
              'etag': 'bar'},
             s3_sync.migrator.EQUAL),
            ({'last-modified': create_timestamp(1.5e9),
              'etag': 'foo'},
             {'last-modified': create_timestamp(1.4e9),
              'etag': 'bar'},
             s3_sync.migrator.ETAG_DIFF),
            ({'last-modified': create_timestamp(1.5e9),
              'etag': 'foo'},
             {'last-modified': create_timestamp(1.4e9),
              'etag': 'foo'},
             s3_sync.migrator.TIME_DIFF)
        ]
        for left, right, expected in test_cases:
            self.assertEqual(expected, s3_sync.migrator.cmp_meta(left, right))


class TestMigrator(unittest.TestCase):
    def setUp(self):
        config = {'aws_bucket': 'bucket',
                  'account': 'AUTH_test',
                  'aws_identity': 'source-account'}
        self.swift_client = mock.Mock()
        pool = mock.Mock()
        pool.item.return_value.__enter__ = lambda *args: self.swift_client
        pool.item.return_value.__exit__ = lambda *args: None
        pool.max_size = 11
        self.logger = logging.getLogger()
        self.stream = StringIO()
        self.logger.addHandler(logging.StreamHandler(self.stream))
        self.migrator = s3_sync.migrator.Migrator(
            config, None, 1000, 5, pool, self.logger, 0, 1)
        self.migrator.status = mock.Mock()

    def get_log_lines(self):
        lines = ''.join(self.stream.getvalue()).split('\n')
        return filter(lambda line: line != '', lines)

    @mock.patch('s3_sync.migrator.create_provider')
    def test_single_container(self, create_provider_mock):
        self.migrator._next_pass = mock.Mock()
        self.migrator.next_pass()
        create_provider_mock.assert_called_once_with(
            {'aws_bucket': 'bucket', 'container': 'bucket',
             'account': 'AUTH_test', 'custom_prefix': '',
             'aws_identity': 'source-account'},
            self.migrator.ic_pool.max_size, False)
        self.migrator._next_pass.assert_called_once_with()

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

    @mock.patch('s3_sync.migrator.create_provider')
    def test_all_containers(self, create_provider_mock):
        provider_mock = mock.Mock()
        buckets = [{'name': 'bucket', 'content_location': 'other_swift'}]
        provider_mock.list_buckets.side_effect = [
            ProviderResponse(True, 200, [], buckets),
            ProviderResponse(True, 200, [], [])]

        def check_provider(config, conns, per_account):
            # We have to check the arguments this way, as otherwise the
            # dictionary gets mutated and assert_called_once_with check will
            # fail.
            self.assertEqual('/*', config['aws_bucket'])
            self.assertEqual('.', config['container'])
            self.assertEqual(self.migrator.ic_pool.max_size, conns)
            return provider_mock

        create_provider_mock.side_effect = check_provider
        self.migrator.config = {'aws_bucket': '/*', 'account': 'AUTH_migrator'}
        self.migrator._next_pass = mock.Mock()
        self.migrator.next_pass()
        self.assertEqual(buckets[0]['name'], self.migrator.config['container'])
        self.assertEqual(buckets[0]['name'],
                         self.migrator.config['aws_bucket'])
        self.assertEqual(buckets[0]['name'], provider_mock.aws_bucket)
        self.assertTrue(self.migrator.config['all_buckets'])

    @mock.patch('s3_sync.migrator.create_provider')
    def test_all_containers_paginated(self, create_provider_mock):
        provider_mock = mock.Mock()
        buckets = [
            {'name': 'bucket', 'content_location': 'other_swift'},
            {'name': 'next-bucket', 'content_location': 'other_swift'}
        ]
        provider_mock.list_buckets.side_effect = [
            ProviderResponse(True, 200, [], [buckets[0]]),
            ProviderResponse(True, 200, [], [buckets[1]]),
            ProviderResponse(True, 200, [], [])]
        next_pass_call = [0]

        def check_provider(config, conns, per_account):
            # We have to check the arguments this way, as otherwise the
            # dictionary gets mutated and assert_called_once_with check will
            # fail.
            self.assertEqual('/*', config['aws_bucket'])
            self.assertEqual('.', config['container'])
            self.assertEqual(self.migrator.ic_pool.max_size, conns)
            return provider_mock

        def check_pass_provider():
            bucket = buckets[next_pass_call[0]]
            self.assertEqual(bucket['name'], self.migrator.config['container'])
            self.assertEqual(
                bucket['name'], self.migrator.config['aws_bucket'])
            self.assertEqual(bucket['name'], provider_mock.aws_bucket)
            next_pass_call[0] += 1

        create_provider_mock.side_effect = check_provider
        self.migrator.config = {'aws_bucket': '/*', 'account': 'AUTH_migrator'}
        self.migrator._next_pass = mock.Mock(side_effect=check_pass_provider)
        self.migrator.next_pass()
        self.assertTrue(self.migrator.config['all_buckets'])
        self.migrator._next_pass.assert_has_calls(
            [mock.call(), mock.call()])
        provider_mock.list_buckets.assert_has_calls(
            [mock.call(None, 10000, None),
             mock.call(buckets[0]['name'], 10000, None)])

    @mock.patch('s3_sync.migrator.create_provider')
    def test_list_buckets_error(self, create_provider_mock):
        create_provider_mock.return_value.list_buckets.return_value = \
            ProviderResponse(False, 404, [], 'Not Found')
        self.migrator.config = {'aws_bucket': '/*'}
        self.migrator.next_pass()
        expected_conf = {'aws_bucket': '/*', 'container': '.',
                         'all_buckets': True}
        self.assertEqual(expected_conf, self.migrator.config)
        create_provider_mock.assert_called_once_with(
            expected_conf, self.migrator.ic_pool.max_size, False)
        self.assertEqual(
            'Failed to list source buckets/containers: "Not Found"',
            self.stream.getvalue().splitlines()[0])

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
                        'last-modified': create_timestamp(1.5e9)},
                    'expected_headers': {
                        'x-object-meta-custom': 'custom',
                        'x-timestamp': Timestamp(1.5e9).internal,
                        s3_sync.utils.get_sys_migrator_header('object'): str(
                            Timestamp(1.5e9).internal)},
                    'list-time': create_list_timestamp(1.5e9),
                    'hash': 'etag'
                },
                'bar': {
                    'remote_headers': {
                        'x-object-meta-custom': 'custom',
                        'last-modified': create_timestamp(1.4e9)},
                    'expected_headers': {
                        'x-object-meta-custom': 'custom',
                        'x-timestamp': Timestamp(1.4e9).internal,
                        s3_sync.utils.get_sys_migrator_header('object'): str(
                            Timestamp(1.4e9).internal)},
                    'list-time': create_list_timestamp(1.4e9),
                    'hash': 'etag'
                }
            },
        }, {
            'objects': {
                'foo': {
                    'remote_headers': {
                        'x-object-meta-custom': 'custom',
                        'last-modified': create_timestamp(1.5e9)},
                    'expected_headers': {
                        'x-object-meta-custom': 'custom',
                        'x-timestamp': Timestamp(1.5e9).internal,
                        s3_sync.utils.get_sys_migrator_header('object'): str(
                            Timestamp(1.5e9).internal)},
                    'list-time': create_list_timestamp(1.5e9),
                    'hash': 'etag'
                },
                'bar': {
                    'remote_headers': {
                        'x-object-meta-custom': 'custom',
                        'last-modified': create_timestamp(1.4e9)},
                    'expected_headers': {
                        'x-object-meta-custom': 'custom',
                        'x-timestamp': Timestamp(1.4e9).internal,
                        s3_sync.utils.get_sys_migrator_header('object'): str(
                            Timestamp(1.4e9).internal)},
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
                        'last-modified': create_timestamp(1.5e9)},
                    'expected_headers': {
                        'x-object-meta-custom': 'custom',
                        'x-timestamp': Timestamp(1.5e9).internal,
                        s3_sync.utils.get_sys_migrator_header('object'): str(
                            Timestamp(1.5e9).internal)},
                    'list-time': create_list_timestamp(1.5e9),
                    'hash': 'etag'
                },
                'bar': {
                    'remote_headers': {
                        'x-object-meta-custom': 'custom',
                        'last-modified': create_timestamp(1.4e9)},
                    'expected_headers': {
                        'x-object-meta-custom': 'custom',
                        'x-timestamp': Timestamp(1.4e9).internal,
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
                        'last-modified': create_timestamp(1.5e9)},
                    'expected_headers': {
                        'x-object-meta-custom': 'custom',
                        'x-timestamp': Timestamp(1.5e9).internal,
                        s3_sync.utils.get_sys_migrator_header('object'): str(
                            Timestamp(1.5e9).internal)},
                    'list-time': create_list_timestamp(1.5e9),
                    'hash': 'etag'
                },
                'bar': {
                    'remote_headers': {
                        'x-object-meta-custom': 'custom',
                        'last-modified': create_timestamp(1.4e9)},
                    'expected_headers': {
                        'x-object-meta-custom': 'custom',
                        'x-timestamp': Timestamp(1.4e9).internal,
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
                        'last-modified': create_timestamp(1.5e9)},
                    'expected_headers': {
                        'x-object-meta-custom': 'custom',
                        'x-timestamp': Timestamp(1.5e9).internal,
                        s3_sync.utils.get_sys_migrator_header('object'): str(
                            Timestamp(1.5e9).internal)},
                    'list-time': create_list_timestamp(1.5e9),
                    'hash': 'etag'
                },
                'bar': {
                    'remote_headers': {
                        'x-object-meta-custom': 'custom',
                        'last-modified': create_timestamp(now - 35.0)},
                    'expected_headers': {
                        'x-object-meta-custom': 'custom',
                        'x-timestamp':
                            Timestamp(math.floor(now - 35.0)).internal,
                        s3_sync.utils.get_sys_migrator_header('object'): str(
                            Timestamp(math.floor(now - 35.0)).internal)},
                    'list-time': create_list_timestamp(now - 35.0),
                    'hash': 'etag'
                },
                'baz': {
                    'remote_headers': {
                        'x-object-meta-custom': 'custom',
                        'last-modified': create_timestamp(now)
                    },
                    'expected_headers': {
                        'x-object-meta-custom': 'custom',
                        'x-timestamp': Timestamp(math.floor(now)).internal,
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

            local_objects = test.get('local_objects', [])

            def get_object(name, **args):
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
            provider.get_object.side_effect = get_object
            self.swift_client.iter_objects.return_value = iter(local_objects)

            self.migrator.next_pass()

            self.swift_client.upload_object.assert_has_calls(
                [mock.call(mock.ANY, self.migrator.config['account'],
                           self.migrator.config['aws_bucket'], name,
                           objects[name]['expected_headers'])
                 for name in migrated])

            for call in self.swift_client.upload_object.mock_calls:
                self.assertEqual('object body', ''.join(call[1][0]))

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
        provider_mock.get_object.return_value = ProviderResponse(
            True, 200, {'last-modified': create_timestamp(1.5e9)},
            StringIO(''))
        self.swift_client.iter_objects.return_value = iter([])
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
            self.migrator.next_pass()
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
            },
            'account': 'AUTH_dev',
            'container': 'bucket3',
            'aws_bucket': 'bucket3',
        }])

    @mock.patch('s3_sync.migrator.create_provider')
    def test_migrate_objects_reset(self, create_provider_mock):
        provider = create_provider_mock.return_value

        self.migrator.status = mock.Mock()
        self.migrator.status.get_migration.return_value = {'marker': 'zzz'}
        provider.list_objects.return_value = ProviderResponse(
            True, 200, {}, [])
        self.swift_client.make_request.return_value = mock.Mock(
            body=json.dumps([]), status_int=200)

        self.migrator.next_pass()
        provider.list_objects.assert_has_calls(
            [mock.call('zzz', self.migrator.work_chunk, '', bucket='bucket'),
             mock.call('', self.migrator.work_chunk, '', bucket='bucket')])

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

        for test_config, container_headers in tests:
            self.migrator.config = dict(config, **test_config)

            provider.reset_mock()
            self.swift_client.reset_mock()
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
                headers = resp.headers
                swift_404_resp = mock.Mock()
                swift_404_resp.status_int = 404

                def fake_get_metadata(account, container):
                    raise UnexpectedResponse('', swift_404_resp)

                self.swift_client.get_container_metadata.side_effect = \
                    fake_get_metadata
                self.swift_client.container_exists.side_effect = (True,)
            else:
                self.swift_client.container_exists.side_effect = (False, True)
                headers = {}

            self.swift_client.iter_objects.return_value = iter([])
            self.swift_client.make_path.return_value = '/'.join(
                ['http://test/v1', self.migrator.config['account'],
                 self.migrator.config['container']])

            def fake_app(env, func):
                return func(200, [])

            self.swift_client.app.side_effect = fake_app

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
            self.swift_client.make_path.has_calls(
                [mock.call(self.migrator.config['account'],
                           self.migrator.config['container'])] * 2)
            called_env = self.swift_client.app.mock_calls[0][1][0]
            for k, v in headers.items():
                self.assertEqual(
                    v, called_env['HTTP_' + k.replace('-', '_').upper()])
            self.assertEqual(self.migrator.config['account'],
                             called_env['PATH_INFO'].split('/')[2])
            self.assertEqual(self.migrator.config['container'],
                             called_env['PATH_INFO'].split('/')[3])

    @mock.patch('s3_sync.migrator.create_provider')
    def test_head_container_error(self, create_provider_mock):
        self.migrator.config['protocol'] = 'swift'

        provider = create_provider_mock.return_value
        provider.list_objects.return_value = ProviderResponse(
            True, 200, {}, [{'name': 'test'}])

        resp = mock.Mock()
        resp.status = 404
        resp.headers = {}
        provider.head_bucket.return_value = resp
        self.swift_client.iter_objects.return_value = iter([])
        self.swift_client.container_exists.return_value = False

        self.migrator.status.get_migration.return_value = {}

        self.migrator.next_pass()
        self.assertEqual(
            'Bucket/container "bucket" does not exist for source-account',
            self.get_log_lines()[0])
        provider.head_bucket.assert_called_once_with(
            self.migrator.config['container'])

    @mock.patch('s3_sync.migrator.time')
    @mock.patch('s3_sync.migrator.create_provider')
    def test_create_container_timeout(self, create_provider_mock, time_mock):
        provider = create_provider_mock.return_value
        provider.list_objects.return_value = ProviderResponse(
            True, 200, {}, [{'name': 'test'}])

        resp = mock.Mock()
        resp.status = 200
        resp.headers = {}
        provider.head_bucket.return_value = resp
        self.swift_client.iter_objects.return_value = iter([])
        self.swift_client.container_exists.return_value = False

        def fake_app(env, func):
                return func(200, [])

        self.swift_client.app.side_effect = fake_app
        self.swift_client.make_path.return_value = '/'.join(
            ['http://test/v1', self.migrator.config['account'],
             self.migrator.config['container']])

        time_mock.time.side_effect = (0, 0, 1)
        self.migrator.status.get_migration.return_value = {}

        self.migrator.next_pass()
        self.assertEqual(
            'MigrationError: Timeout while creating container "bucket"',
            self.get_log_lines()[-1])
        self.swift_client.make_path.assert_called_once_with(
            self.migrator.config['account'], self.migrator.config['container'])
        self.swift_client.container_exists.assert_has_calls(
            [mock.call(self.migrator.config['account'],
                       self.migrator.config['container'])] * 2)

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
                    'x-static-large-object': 'True'},
                'expected_headers': {
                    'x-object-meta-custom': 'slo-meta',
                    'x-timestamp': Timestamp(1.5e9).internal,
                    'x-static-large-object': 'True',
                    'Content-Length': str(len(json.dumps(manifest))),
                    'etag': manifest_etag,
                    s3_sync.utils.get_sys_migrator_header('object'): str(
                        Timestamp(1.5e9).internal)}
            },
            'part1': {
                'remote_headers': {
                    'x-object-meta-part': 'part-1',
                    'last-modified': create_timestamp(1.4e9)},
                'expected_headers': {
                    'x-object-meta-part': 'part-1',
                    'x-timestamp': Timestamp(1.4e9).internal,
                    s3_sync.utils.get_sys_migrator_header('object'): str(
                        Timestamp(1.4e9).internal)}
            },
            'part2': {
                'remote_headers': {
                    'x-object-meta-part': 'part-2',
                    'last-modified': create_timestamp(1.1e9)},
                'expected_headers': {
                    'x-object-meta-part': 'part-2',
                    'x-timestamp': Timestamp(1.1e9).internal,
                    s3_sync.utils.get_sys_migrator_header('object'): str(
                        Timestamp(1.1e9).internal)}
            }
        }

        containers = {segments_container[1:]: False,
                      self.migrator.config['container']: False}

        swift_404_resp = mock.Mock()
        swift_404_resp.status_int = 404

        def container_exists(_, container):
            return containers[container]

        def get_container_metadata(_, container):
            if not containers[container]:
                raise UnexpectedResponse('', swift_404_resp)

        def fake_app(env, func):
            containers[env['PATH_INFO'].split('/')[3]] = True
            return func(200, [])

        def _make_path(account, container):
            return '/'.join(['http://test/v1', account, container])

        def get_object(name, **args):
            if name not in objects.keys():
                raise RuntimeError('Unknown object: %s' % name)
            return ProviderResponse(
                True, 200, objects[name]['remote_headers'],
                StringIO('object body'))

        def head_object(name, container):
            if container != segments_container[1:]:
                raise RuntimeError('wrong container: %s' % container)
            if name not in objects.keys():
                raise RuntimeError('unknown object: %s' % name)
            resp = mock.Mock()
            resp.status = 200
            resp.headers = objects[name]['remote_headers']
            return resp

        def upload_object(body, account, container, key, headers):
            if not containers[container]:
                raise UnexpectedResponse('', swift_404_resp)

        self.swift_client.container_exists.side_effect = container_exists
        self.swift_client.get_container_metadata.side_effect = \
            get_container_metadata
        self.swift_client.app.side_effect = fake_app
        self.swift_client.make_path.side_effect = _make_path
        self.swift_client.get_object_metadata.side_effect = UnexpectedResponse(
            '', swift_404_resp)
        self.swift_client.upload_object.side_effect = upload_object

        bucket_resp = mock.Mock()
        bucket_resp.status = 200
        bucket_resp.headers = {}

        provider.head_bucket.return_value = bucket_resp
        provider.list_objects.return_value = ProviderResponse(
            True, 200, {}, [{'name': 'slo'}])
        provider.get_object.side_effect = get_object
        provider.head_object.side_effect = head_object
        provider.get_manifest.return_value = manifest
        self.swift_client.iter_objects.return_value = iter([])

        self.migrator.next_pass()

        self.swift_client.upload_object.assert_has_calls(
            [mock.call(mock.ANY, self.migrator.config['account'],
                       'slo-segments',
                       'part1',
                       objects['part1']['expected_headers']),
             mock.call(mock.ANY, self.migrator.config['account'],
                       'slo-segments',
                       'part2',
                       objects['part2']['expected_headers']),
             mock.call(mock.ANY, self.migrator.config['account'],
                       self.migrator.config['container'],
                       'slo',
                       objects['slo']['expected_headers'])])

        called_env = self.swift_client.app.mock_calls[0][1][0]
        self.assertEqual(self.migrator.config['account'],
                         called_env['PATH_INFO'].split('/')[2])
        self.assertEqual(self.migrator.config['container'],
                         called_env['PATH_INFO'].split('/')[3])
        called_env = self.swift_client.app.mock_calls[1][1][0]
        self.assertEqual(self.migrator.config['account'],
                         called_env['PATH_INFO'].split('/')[2])
        self.assertEqual(segments_container[1:],
                         called_env['PATH_INFO'].split('/')[3])

        parts = {'part1': False, 'part2': False, 'slo': False}
        for call in self.swift_client.upload_object.mock_calls:
            body, acct, cont, obj, headers = call[1]
            if parts[obj]:
                continue
            if obj.startswith('part'):
                self.assertEqual(segments_container[1:], cont)
                self.assertEqual('object body', ''.join(body))
            else:
                self.assertEqual(self.migrator.config['container'], cont)
                self.assertEqual(manifest, json.loads(''.join(body)))
            parts[obj] = True

    @mock.patch('s3_sync.sync_s3.SyncS3._get_client_factory')
    @mock.patch('s3_sync.migrator.create_provider')
    def test_closes_s3_connections(
            self, create_provider_mock, client_factory_mock):
        conn_mock = mock.Mock()
        fake_factory = mock.Mock()
        fake_factory.return_value = conn_mock
        client_factory_mock.return_value = lambda: conn_mock
        fake_provider = s3_sync.sync_s3.SyncS3(self.migrator.config)
        create_provider_mock.return_value = fake_provider
        conn_mock.list_objects.return_value = {'Contents': []}
        self.swift_client.make_request.return_value = mock.Mock(
            status_int=200, body='[]')
        self.migrator.status.get_migration.return_value = {}
        self.migrator.config['container'] = 'container'

        self.migrator.next_pass()
        self.assertEqual(1, len(fake_provider.client_pool.client_pool))
        self.migrator.close()
        conn_mock._endpoint.http_session.close.assert_called_once_with()

    @mock.patch('s3_sync.sync_swift.SyncSwift._get_client_factory')
    @mock.patch('s3_sync.migrator.create_provider')
    def test_closes_swift_connections(
            self, create_provider_mock, client_factory_mock):
        conn_mock = mock.Mock()
        fake_factory = mock.Mock()
        fake_factory.return_value = conn_mock
        client_factory_mock.return_value = lambda: conn_mock
        fake_provider = s3_sync.sync_swift.SyncSwift(self.migrator.config)
        create_provider_mock.return_value = fake_provider
        conn_mock.get_container.return_value = ({}, [])
        self.swift_client.make_request.return_value = mock.Mock(
            status_int=200, body='[]')
        conn_mock.http_conn = [None, mock.Mock()]
        self.migrator.status.get_migration.return_value = {}
        self.migrator.config['container'] = 'container'

        self.migrator.next_pass()
        self.assertEqual(1, len(fake_provider.client_pool.client_pool))
        self.migrator.close()
        conn_mock.http_conn[1].request_session.close.assert_called_once_with()

    @mock.patch('s3_sync.migrator.create_provider')
    def test_paginate_migration_listings(self, create_provider_mock):
        self.migrator.status.get_migration.return_value = {
            'marker': 'bar'}
        provider = create_provider_mock.return_value
        provider.list_objects.return_value = mock.Mock(status=200, body=[])
        self.swift_client.make_request.return_value = mock.Mock(
            status_int=200, body='[]')

        def _make_path(account, container):
            return '/'.join([account, container])

        self.swift_client.make_path.side_effect = _make_path

        self.migrator.next_pass()
        self.swift_client.make_request.assert_has_calls(
            [mock.call(
                'GET', '%s/%s?format=json&marker=bar' % (
                    self.migrator.config['account'],
                    self.migrator.config['container']),
                {}, (2, 404)),
             mock.call(
                'GET', '%s/%s?format=json&marker=' % (
                    self.migrator.config['account'],
                    self.migrator.config['container']),
                {}, (2, 404))])
        provider.list_objects.assert_has_calls(
            [mock.call(
                'bar', 1000, '', bucket=self.migrator.config['aws_bucket']),
             mock.call(
                '', 1000, '', bucket=self.migrator.config['aws_bucket'])])

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
                    'x-object-manifest': '%s/' % segments_container},
                'expected_headers': {
                    'x-object-meta-custom': 'dlo-meta',
                    'x-timestamp': Timestamp(1.5e9).internal,
                    'x-object-manifest': '%s/' % segments_container,
                    s3_sync.utils.get_sys_migrator_header('object'): str(
                        Timestamp(1.5e9).internal)}
            },
            '1': {
                'remote_headers': {
                    'x-object-meta-part': 'part-1',
                    'last-modified': create_timestamp(1.4e9)},
                'expected_headers': {
                    'x-object-meta-part': 'part-1',
                    'x-timestamp': Timestamp(1.4e9).internal,
                    s3_sync.utils.get_sys_migrator_header('object'): str(
                        Timestamp(1.4e9).internal)}
            },
            '2': {
                'remote_headers': {
                    'x-object-meta-part': 'part-2',
                    'last-modified': create_timestamp(1.1e9)},
                'expected_headers': {
                    'x-object-meta-part': 'part-2',
                    'x-timestamp': Timestamp(1.1e9).internal,
                    s3_sync.utils.get_sys_migrator_header('object'): str(
                        Timestamp(1.1e9).internal)}
            },
            '3': {
                'remote_headers': {
                    'x-object-meta-part': 'part-3',
                    'last-modified': create_timestamp(1.2e9)},
                'expected_headers': {
                    'x-object-meta-part': 'part-3',
                    'x-timestamp': Timestamp(1.2e9).internal,
                    s3_sync.utils.get_sys_migrator_header('object'): str(
                        Timestamp(1.2e9).internal)}
            }
        }

        containers = {segments_container: False,
                      self.migrator.config['container']: False}

        swift_404_resp = mock.Mock()
        swift_404_resp.status_int = 404

        def container_exists(_, container):
            return containers[container]

        def fake_app(env, func):
            containers[env['PATH_INFO'].split('/')[3]] = True
            return func(200, [])

        def get_container_metadata(_, container):
            if not containers[container]:
                raise UnexpectedResponse('', swift_404_resp)

        def _make_path(account, container):
            return '/'.join(['http://test/v1', account, container])

        def get_object(name, **args):
            if name not in objects.keys():
                raise RuntimeError('Unknown object: %s' % name)
            if name == 'dlo':
                return ProviderResponse(
                    True, 200, objects[name]['remote_headers'], StringIO(''))
            return ProviderResponse(
                True, 200, objects[name]['remote_headers'],
                StringIO('object body'))

        def upload_object(body, account, container, key, headers):
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

        bucket_resp = mock.Mock()
        bucket_resp.status = 200
        bucket_resp.headers = {}

        provider.head_bucket.return_value = bucket_resp
        provider.list_objects.side_effect = list_objects
        provider.get_object.side_effect = get_object
        self.swift_client.iter_objects.return_value = iter([])

        self.migrator.next_pass()

        self.swift_client.upload_object.assert_has_calls(
            [mock.call(mock.ANY, self.migrator.config['account'],
                       'dlo-segments', '1',
                       objects['1']['expected_headers']),
             mock.call(mock.ANY, self.migrator.config['account'],
                       'dlo-segments', '2',
                       objects['2']['expected_headers']),
             mock.call(mock.ANY, self.migrator.config['account'],
                       'dlo-segments', '3',
                       objects['3']['expected_headers']),
             mock.call(mock.ANY, self.migrator.config['account'],
                       self.migrator.config['container'], 'dlo',
                       objects['dlo']['expected_headers'])])

        called_env = self.swift_client.app.mock_calls[0][1][0]
        self.assertEqual(self.migrator.config['account'],
                         called_env['PATH_INFO'].split('/')[2])
        self.assertEqual(self.migrator.config['container'],
                         called_env['PATH_INFO'].split('/')[3])
        called_env = self.swift_client.app.mock_calls[1][1][0]
        self.assertEqual(self.migrator.config['account'],
                         called_env['PATH_INFO'].split('/')[2])
        self.assertEqual(segments_container,
                         called_env['PATH_INFO'].split('/')[3])

        parts = {'1': False, '2': False, '3': False, 'dlo': False}
        for call in self.swift_client.upload_object.mock_calls:
            body, acct, cont, obj, headers = call[1]
            if parts[obj]:
                continue
            if obj != 'dlo':
                self.assertEqual(segments_container, cont)
                self.assertEqual('object body', ''.join(body))
            else:
                self.assertEqual(self.migrator.config['container'], cont)
                self.assertEqual('', ''.join(body))
            parts[obj] = True

    @mock.patch('s3_sync.migrator.create_provider')
    def test_etag_mismatch(self, create_provider_mock):
        self.migrator.config['protocol'] = 'swift'
        provider = create_provider_mock.return_value
        self.migrator.status.get_migration.return_value = {}

        objects = {
            'dlo': {
                'list_entry': {
                    'last_modified': create_list_timestamp(1.5e9),
                    'hash': 'deadbeef'},
                'headers': {
                    'x-object-manifest': 'segments/',
                    'last_modified': create_timestamp(1.5e9)
                }
            },
            'slo': {
                'list_entry': {
                    'last_modified': create_list_timestamp(1.4e9),
                    'hash': 'feedbead'},
                'headers': {
                    'x-static-large-object': True,
                    'last_modified': create_timestamp(1.4e9)
                }
            }
        }

        self.swift_client.make_request.side_effect = (
            mock.Mock(body=json.dumps([
                {'name': 'dlo',
                 'last_modified': create_list_timestamp(1.5e9),
                 'hash': 'other'},
                {'name': 'slo',
                 'last_modified': create_list_timestamp(1.4e9),
                 'hash': 'other-still'}]),
                status_int=200),
            mock.Mock(body=json.dumps([]), status_int=200))

        def _head_object(key):
            resp = mock.Mock()
            resp.headers = objects[key]['headers']
            return resp

        def _get_object_metadata(_account, _container, key):
            return objects[key]['headers']

        def _get_object(_account, _container, key, _headers):
            return 200, {}, '{}'

        self.swift_client.container_exists.return_value = True
        self.swift_client.get_object_metadata.side_effect =\
            _get_object_metadata
        self.swift_client.get_object.side_effect = _get_object
        provider.list_objects.return_value = ProviderResponse(
            True, 200, {},
            [dict([('name', k)] + objects[k]['list_entry'].items())
             for k in sorted(objects.keys())])
        provider.head_object.side_effect = _head_object
        provider.head_bucket.return_value = mock.Mock(status=200, headers={})
        provider.get_manifest.return_value = {}

        self.migrator.next_pass()
        self.assertEqual('', self.stream.getvalue())


class TestStatus(unittest.TestCase):

    def setUp(self):
        self.start = int(time.time()) + 1
        patcher = mock.patch('time.time')
        self.addCleanup(patcher.stop)
        self.mock_time = patcher.start()
        self.mock_time.side_effect = itertools.count(self.start)

    def test_update_status_fresh(self):
        status = {}
        # initial pass
        s3_sync.migrator._update_status_counts(status, 10, 10, True)
        self.assertEqual({
            'finished': self.start,
            'moved_count': 10,
            'scanned_count': 10,
        }, status)

    def test_update_status_update(self):
        # second pass finishes
        status = {
            'finished': self.start - 1,
            'moved_count': 10,
            'scanned_count': 10,
        }
        s3_sync.migrator._update_status_counts(status, 8, 8, False)
        self.assertEqual({
            'finished': self.start,
            'moved_count': 18,
            'scanned_count': 18,
        }, status)

    def test_update_status_set_last(self):
        # next pass has nothing to move
        status = {
            'finished': self.start - 1,
            'moved_count': 18,
            'scanned_count': 18,
        }
        s3_sync.migrator._update_status_counts(status, 0, 10, True)
        self.assertEqual({
            'finished': self.start,
            'moved_count': 0,
            'scanned_count': 10,
            'last_finished': self.start - 1,
            'last_moved_count': 18,
            'last_scanned_count': 18,
        }, status)

    def test_update_status_update_current_maintains_last(self):
        # still nothing
        status = {
            'finished': self.start - 1,
            'moved_count': 0,
            'scanned_count': 10,
            'last_finished': self.start - 2,
            'last_moved_count': 18,
            'last_scanned_count': 18,
        }
        s3_sync.migrator._update_status_counts(status, 0, 8, False)
        self.assertEqual({
            'finished': self.start,
            'moved_count': 0,
            'scanned_count': 18,
            'last_finished': self.start - 2,
            'last_moved_count': 18,
            'last_scanned_count': 18,
        }, status)

    def test_update_status_finished_resets_last(self):
        # fresh run, but nothing moved and scanned matches!
        status = {
            'finished': self.start - 1,
            'moved_count': 0,
            'scanned_count': 18,
            'last_finished': self.start - 3,
            'last_moved_count': 18,
            'last_scanned_count': 18,
        }
        s3_sync.migrator._update_status_counts(status, 0, 10, True)
        self.assertEqual({
            'finished': self.start,
            'moved_count': 0,
            'scanned_count': 10,
            'last_finished': self.start - 1,
            'last_moved_count': 0,
            'last_scanned_count': 18,
        }, status)

    def test_update_status_update_with_new_move(self):
        # oh weird, something new showed up!?
        status = {
            'finished': self.start - 3,
            'moved_count': 0,
            'scanned_count': 10,
            'last_finished': self.start - 4,
            'last_moved_count': 0,
            'last_scanned_count': 18,
        }
        s3_sync.migrator._update_status_counts(status, 1, 9, False)
        self.assertEqual({
            'finished': self.start,
            'moved_count': 1,
            'scanned_count': 19,
            'last_finished': self.start - 4,
            'last_moved_count': 0,
            'last_scanned_count': 18,
        }, status)

    def test_update_status_finished_new_move_resets_last(self):
        # ok, back to boring nothing
        status = {
            'finished': self.start - 3,
            'moved_count': 1,
            'scanned_count': 19,
            'last_finished': self.start - 5,
            'last_moved_count': 0,
            'last_scanned_count': 18,
        }
        s3_sync.migrator._update_status_counts(status, 0, 10, True)
        self.assertEqual({
            'finished': self.start,
            'moved_count': 0,
            'scanned_count': 10,
            'last_finished': self.start - 3,
            'last_moved_count': 1,
            'last_scanned_count': 19,
        }, status)

    def test_update_status_finished_no_moves_resets_last(self):
        # and we're done here...
        status = {
            'finished': self.start - 5,
            'moved_count': 0,
            'scanned_count': 19,
            'last_finished': self.start - 7,
            'last_moved_count': 1,
            'last_scanned_count': 19,
        }
        s3_sync.migrator._update_status_counts(status, 0, 10, True)
        self.assertEqual({
            'finished': self.start,
            'moved_count': 0,
            'scanned_count': 10,
            'last_finished': self.start - 5,
            'last_moved_count': 0,
            'last_scanned_count': 19,
        }, status)

    def test_update_status_clean_finish_does_not_reset_last(self):
        # and we'll stay this way, indefinitely...
        status = {
            'finished': self.start - 2,
            'moved_count': 0,
            'scanned_count': 19,
            'last_finished': self.start - 7,
            'last_moved_count': 0,
            'last_scanned_count': 19,
        }
        s3_sync.migrator._update_status_counts(status, 0, 10, True)
        self.assertEqual({
            'finished': self.start,
            'moved_count': 0,
            'scanned_count': 10,
            'last_finished': self.start - 7,
            'last_moved_count': 0,
            'last_scanned_count': 19,
        }, status)

    def test_update_legacy(self):
        # we start with less info
        status = {
            'moved_count': 0,
            'scanned_count': 8,
        }
        s3_sync.migrator._update_status_counts(status, 0, 8, True)
        self.assertEqual({
            'finished': self.start,
            'moved_count': 0,
            'scanned_count': 8,
        }, status)
        s3_sync.migrator._update_status_counts(status, 0, 8, True)
        # ... but we get there eventually
        self.assertEqual({
            'finished': self.start + 1,
            'moved_count': 0,
            'scanned_count': 8,
            'last_finished': self.start,
            'last_moved_count': 0,
            'last_scanned_count': 8,
        }, status)


class TestMain(unittest.TestCase):

    def setUp(self):
        self.logger = logging.getLogger()
        self.stream = StringIO()
        self.logger.addHandler(logging.StreamHandler(self.stream))
        self.conf = {
            'migrations': [],
            'migration_status': None,
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
    def patch(self, name):
        with mock.patch('s3_sync.migrator.' + name) as mocked:
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
            s3_sync.migrator.run(**self.conf)
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
                s3_sync.migrator.run(**self.conf)
            self.assertEqual(mocktime.sleep.call_args_list,
                             [mock.call(29)] * 2)
            self.assertEqual([
                'Finished cycle in 1.00s, sleeping for 29.00s.',
                'Finished cycle in 1.00s, sleeping for 29.00s.',
            ], self.pop_log_lines().splitlines())

    def test_conf_parsing(self):
        config = {
            'migrator_settings': {
                'workers': 1337,
                'items_chunk': 42,
                'status_file': '/test/status',
                'poll_interval': 5,
                'process': 0,
                'processes': 15,
            },
            'migrations': [
                {'aws_bucket': 'test_bucket',
                 'account': 'AUTH_test',
                 'aws_identity': 'identity',
                 'aws_secret': 'secret',
                 'container': 'dst-container'}
            ],
            'swift_dir': '/foo/bar/swift',
        }
        with self.patch('setup_context') as mock_setup_context,\
                self.patch('Migrator') as mock_migrator,\
                self.patch('Status') as mock_status:
            mock_setup_context.return_value = (
                mock.Mock(log_level='warn', console=True, once=True),
                config)

            s3_sync.migrator.main()
            mock_status.assert_called_once_with('/test/status')
            mock_migrator.assert_called_once_with(
                config['migrations'][0], mock_status.return_value, 42, 1337,
                mock.ANY, mock.ANY, 0, 15)
