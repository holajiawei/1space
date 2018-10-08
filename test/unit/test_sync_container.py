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

import json
import mock
import time
import unittest

from container_crawler import RetryError
from s3_sync.sync_container import SyncContainer
from s3_sync.sync_s3 import SyncS3
from s3_sync.sync_swift import SyncSwift
from swift.common.utils import decode_timestamps, Timestamp


class TestSyncContainer(unittest.TestCase):
    class MockMetaConf(object):
        def __init__(self, fake_status):
            self.fake_status = fake_status
            self.write_buf = ''

        def read(self, size=-1):
            if size != -1:
                raise RuntimeError()
            return json.dumps(self.fake_status)

        def write(self, data):
            # Only support write at the beginning
            self.write_buf += data

        def truncate(self, size=None):
            if size:
                raise RuntimeError('Not supported')
            self.fake_status = json.loads(self.write_buf)
            self.write_buf = ''

        def __exit__(self, *args):
            if self.write_buf:
                self.fake_status = json.loads(self.write_buf)
                self.write_buf = ''

        def __enter__(self):
            return self

        def seek(self, offset, flags=None):
            if offset != 0:
                raise RuntimeError

    @mock.patch('s3_sync.sync_s3.boto3.session.Session')
    def setUp(self, mock_boto3):
        self.mock_boto3_session = mock.Mock()
        self.mock_boto3_client = mock.Mock()

        mock_boto3.return_value = self.mock_boto3_session
        self.mock_boto3_session.client.return_value = self.mock_boto3_client

        self.aws_bucket = 'bucket'
        self.scratch_space = 'scratch'
        self.sync_container = SyncContainer(self.scratch_space,
                                            {'aws_bucket': self.aws_bucket,
                                             'aws_identity': 'identity',
                                             'aws_secret': 'credential',
                                             'account': 'account',
                                             'container': 'container'})

    def test_load_non_existent_meta(self):
        ret = self.sync_container.get_last_processed_row('db-id')
        self.assertEqual(0, ret)

    @mock.patch('__builtin__.open')
    @mock.patch('s3_sync.sync_container.os.path.exists')
    def test_load_upgrade_status(self, mock_exists, mock_open):
        mock_exists.return_value = True
        fake_status = dict(last_row=42)
        mock_open.return_value = self.MockMetaConf(fake_status)

        status = self.sync_container.get_last_processed_row('db-id')
        self.assertEqual(fake_status['last_row'], status)

        mock_exists.assert_called_with('%s/%s/%s' % (
            self.scratch_space, self.sync_container._account,
            self.sync_container._container))

    @mock.patch('__builtin__.open')
    @mock.patch('s3_sync.sync_container.os.path.exists')
    def test_last_row_new_bucket(self, mock_exists, mock_open):
        db_id = 'db-id-test'
        new_bucket = 'new-bucket'
        self.sync_container.aws_bucket = 'bucket'
        fake_status = {db_id: dict(last_row=42, aws_bucket=new_bucket)}

        mock_exists.return_value = True
        mock_open.return_value = self.MockMetaConf(fake_status)

        status = self.sync_container.get_last_processed_row(db_id)
        self.assertEqual(0, status)

        mock_exists.assert_called_with('%s/%s/%s' % (
            self.scratch_space, self.sync_container._account,
            self.sync_container._container))

    @mock.patch('__builtin__.open')
    @mock.patch('s3_sync.sync_container.os.path.exists')
    def test_last_row_new_db_id(self, mock_exists, mock_open):
        db_id = 'db-id-test'
        self.sync_container.aws_bucket = 'bucket'
        fake_status = {db_id: dict(last_row=42, aws_bucket='bucket')}

        mock_exists.return_value = True
        mock_open.return_value = self.MockMetaConf(fake_status)

        status = self.sync_container.get_last_processed_row('other-db-id')
        self.assertEqual(0, status)

        mock_exists.assert_called_with('%s/%s/%s' % (
            self.scratch_space, self.sync_container._account,
            self.sync_container._container))

    @mock.patch('__builtin__.open')
    @mock.patch('s3_sync.sync_container.os.path.exists')
    def test_last_row_new_policy(self, mock_exists, mock_open):
        db_id = 'db-id-test'
        for field in SyncContainer.POLICY_FIELDS:
            if field != 'copy_after':
                setattr(self.sync_container, field, False)
            else:
                setattr(self.sync_container, field, 42)
        fake_status = {db_id: dict(last_row=42,
                                   aws_bucket='bucket',
                                   policy=dict(retain_local=True,
                                               propagate_delete=True,
                                               copy_after=0))}

        mock_exists.return_value = True
        mock_open.return_value = self.MockMetaConf(fake_status)

        status = self.sync_container.get_last_processed_row(db_id)
        self.assertEqual(0, status)

        mock_exists.assert_called_with('%s/%s/%s' % (
            self.scratch_space, self.sync_container._account,
            self.sync_container._container))

    @mock.patch('__builtin__.open')
    @mock.patch('s3_sync.sync_container.os.path.exists')
    def test_last_row_old_policy(self, mock_exists, mock_open):
        db_id = 'db-id-test'
        fake_status = {db_id: dict(last_row=42,
                                   aws_bucket='bucket',
                                   policy=dict(retain_local=True,
                                               propagate_delete=True,
                                               copy_after=0))}

        mock_exists.return_value = True
        mock_open.return_value = self.MockMetaConf(fake_status)

        status = self.sync_container.get_last_processed_row(db_id)
        self.assertEqual(42, status)

        mock_exists.assert_called_with('%s/%s/%s' % (
            self.scratch_space, self.sync_container._account,
            self.sync_container._container))

    @mock.patch('__builtin__.open')
    @mock.patch('s3_sync.sync_container.os.path.exists')
    def test_last_row(self, mock_exists, mock_open):
        db_entries = [{'id': 'db-id-1', 'aws_bucket': 'bucket', 'last_row': 5},
                      {'id': 'db-id-2', 'aws_bucket': 'bucket', 'last_row': 7}]
        for entry in db_entries:
            self.sync_container.aws_bucket = entry['aws_bucket']
            fake_status = {entry['id']: dict(last_row=entry['last_row'],
                                             aws_bucket=entry['aws_bucket'])}

            mock_exists.return_value = True
            mock_open.return_value = self.MockMetaConf(fake_status)

            status = self.sync_container.get_last_processed_row(entry['id'])
            self.assertEqual(entry['last_row'], status)

            mock_exists.assert_called_with('%s/%s/%s' % (
                self.scratch_space, self.sync_container._account,
                self.sync_container._container))

    @mock.patch('__builtin__.open')
    @mock.patch('s3_sync.sync_container.os.path.exists')
    def test_get_last_verified_row(self, mock_exists, mock_open):
        db_entries = [{'id': 'db-id-1',
                       'aws_bucket': 'bucket',
                       'last_row': 5},
                      {'id': 'db-id-2',
                       'aws_bucket': 'bucket',
                       'last_verified_row': 7},
                      {'id': 'db-id-3',
                       'aws_bucket': 'bucket',
                       'last_row': 100,
                       'last_verified_row': 10}]
        for entry in db_entries:
            self.sync_container.aws_bucket = entry['aws_bucket']
            fake_status = {entry['id']: dict(aws_bucket=entry['aws_bucket'])}
            if 'last_row' in entry:
                fake_status[entry['id']]['last_row'] = entry['last_row']
            if 'last_verified_row' in entry:
                fake_status[entry['id']]['last_verified_row'] =\
                    entry['last_verified_row']

            mock_exists.return_value = True
            mock_open.return_value = self.MockMetaConf(fake_status)

            status = self.sync_container.get_last_verified_row(entry['id'])
            if 'last_verified_row' in entry:
                self.assertEqual(entry['last_verified_row'], status)
            else:
                self.assertEqual(entry['last_row'], status)

            mock_exists.assert_called_with('%s/%s/%s' % (
                self.scratch_space, self.sync_container._account,
                self.sync_container._container))

    @mock.patch('__builtin__.open')
    def test_save_last_processed_row(self, mock_open):
        db_entries = {'db-id-1': {'aws_bucket': 'bucket', 'last_row': 5},
                      'db-id-2': {'aws_bucket': 'bucket', 'last_row': 7}}
        new_row = 42
        for db_id, entry in db_entries.items():
            self.sync_container.aws_bucket = entry['aws_bucket']
            fake_conf_file = self.MockMetaConf(db_entries)
            mock_open.return_value = fake_conf_file

            with mock.patch('s3_sync.sync_container.os.path.exists')\
                    as mock_exists:
                mock_exists.return_value = True

                self.sync_container.save_last_processed_row(new_row, db_id)
                file_entries = fake_conf_file.fake_status
                for file_db_id, status in file_entries.items():
                    if file_db_id == db_id:
                        self.assertEqual(new_row, status['last_row'])
                        self.assertEqual(
                            entry['last_row'], status['last_verified_row'])
                    else:
                        self.assertEqual(db_entries[file_db_id]['last_row'],
                                         status['last_row'])
                    if db_id != file_db_id:
                        continue
                    else:
                        self.assertIn('policy', status)
                    for field in SyncContainer.POLICY_FIELDS:
                        self.assertEqual(status['policy'][field],
                                         getattr(self.sync_container, field))

                self.assertEqual(
                    [mock.call('%s/%s' % (self.scratch_space,
                                          self.sync_container._account)),
                     mock.call('%s/%s/%s' % (self.scratch_space,
                                             self.sync_container._account,
                                             self.sync_container._container))],
                    mock_exists.call_args_list)

    @mock.patch('__builtin__.open')
    def test_save_last_verified_row(self, mock_open):
        db_entries = {'db-id-1': {'aws_bucket': 'bucket',
                                  'last_row': 100,
                                  'last_verified_row': 10},
                      'db-id-2': {'aws_bucket': 'bucket',
                                  'last_row': 200,
                                  'last_verified_row': 50},
                      'db-id-3': {'aws_bucket': 'bucket',
                                  'last_row': 1000}}
        new_row = 99
        for db_id, entry in db_entries.items():
            self.sync_container.aws_bucket = entry['aws_bucket']
            fake_conf_file = self.MockMetaConf(db_entries)
            mock_open.return_value = fake_conf_file

            with mock.patch('s3_sync.sync_container.os.path.exists')\
                    as mock_exists:
                mock_exists.return_value = True

                self.sync_container.save_last_verified_row(new_row, db_id)
                file_entries = fake_conf_file.fake_status
                for file_db_id, status in file_entries.items():
                    if file_db_id == db_id:
                        self.assertEqual(new_row, status['last_verified_row'])
                    elif 'last_verified_row' in db_entries[file_db_id]:
                        self.assertEqual(
                            db_entries[file_db_id]['last_verified_row'],
                            status['last_verified_row'])
                    self.assertEqual(
                        db_entries[file_db_id]['last_row'],
                        status['last_row'])
                    if db_id != file_db_id:
                        continue
                    else:
                        self.assertIn('policy', status)
                    for field in SyncContainer.POLICY_FIELDS:
                        self.assertEqual(status['policy'][field],
                                         getattr(self.sync_container, field))

                self.assertEqual(
                    [mock.call('%s/%s' % (self.scratch_space,
                                          self.sync_container._account)),
                     mock.call('%s/%s/%s' % (self.scratch_space,
                                             self.sync_container._account,
                                             self.sync_container._container))],
                    mock_exists.call_args_list)

    @mock.patch('__builtin__.open')
    @mock.patch('s3_sync.sync_container.os.path.exists')
    def test_save_no_prior_status(self, mock_exists, mock_open):
        def existence_check(path):
            if path == '%s/%s' % (self.scratch_space,
                                  self.sync_container._account):
                return True
            elif path == '%s/%s/%s' % (self.scratch_space,
                                       self.sync_container._account,
                                       self.sync_container._container):
                return False
            else:
                raise RuntimeError('Invalid path')

        self.sync_container.aws_bucket = 'bucket'
        fake_conf_file = self.MockMetaConf({})
        mock_exists.side_effect = existence_check
        mock_open.return_value = fake_conf_file

        self.sync_container.save_last_processed_row(42, 'db-id')
        self.assertEqual(42, fake_conf_file.fake_status['db-id']['last_row'])
        self.assertEqual(
            0, fake_conf_file.fake_status['db-id']['last_verified_row'])
        self.assertEqual('bucket',
                         fake_conf_file.fake_status['db-id']['aws_bucket'])

        self.assertEqual(
            [mock.call('%s/%s' % (self.scratch_space,
                                  self.sync_container._account)),
             mock.call('%s/%s/%s' % (self.scratch_space,
                                     self.sync_container._account,
                                     self.sync_container._container))],
            mock_exists.call_args_list)

    @mock.patch('__builtin__.open')
    @mock.patch('s3_sync.sync_container.os.path.exists')
    def test_save_last_processed_row_new_bucket(self, mock_exists, mock_open):
        db_entries = {'db-id-1': {'aws_bucket': 'bucket', 'last_row': 5},
                      'db-id-2': {'aws_bucket': 'old-bucket', 'last_row': 7}}
        new_row = 42
        for db_id, entry in db_entries.items():
            self.sync_container.aws_bucket = 'bucket'
            fake_conf_file = self.MockMetaConf(db_entries)
            mock_open.return_value = fake_conf_file

            with mock.patch('s3_sync.sync_container.os.path.exists')\
                    as mock_exists:
                mock_exists.return_value = True
                self.sync_container.save_last_processed_row(new_row, db_id)
                file_entries = fake_conf_file.fake_status
                for file_db_id, status in file_entries.items():
                    if file_db_id == db_id:
                        self.assertEqual(new_row, status['last_row'])
                        self.assertEqual('bucket', status['aws_bucket'])
                    else:
                        self.assertEqual(db_entries[file_db_id]['last_row'],
                                         status['last_row'])
                        self.assertEqual(db_entries[file_db_id]['aws_bucket'],
                                         status['aws_bucket'])

                self.assertEqual(
                    [mock.call('%s/%s' % (self.scratch_space,
                                          self.sync_container._account)),
                     mock.call('%s/%s/%s' % (self.scratch_space,
                                             self.sync_container._account,
                                             self.sync_container._container))],
                    mock_exists.call_args_list)

    def test_s3_provider(self):
        defaults = {'aws_bucket': self.aws_bucket,
                    'aws_identity': 'identity',
                    'aws_secret': 'credential',
                    'account': 'account',
                    'container': 'container'}
        test_settings = [defaults,
                         dict(defaults.items() + [('protocol', 's3')])]

        for settings in test_settings:
            sync = SyncContainer(self.scratch_space, settings, max_conns=1)
            self.assertIsInstance(sync.provider, SyncS3)
            self.assertEqual(sync.provider.settings, settings)
            self.assertEqual(len(sync.provider.client_pool.client_pool), 0)
            self.assertEqual(sync.provider.client_pool.pool_size, 1)

    def test_swift_provider(self):
        settings = {'aws_bucket': self.aws_bucket,
                    'aws_identity': 'identity',
                    'aws_secret': 'credential',
                    'aws_endpoint': 'http://swift.example.com:8080/auth/v1.0',
                    'account': 'account',
                    'container': 'container',
                    'protocol': 'swift'}
        sync = SyncContainer(self.scratch_space, settings, max_conns=1)
        self.assertIsInstance(sync.provider, SyncSwift)
        self.assertEqual(sync.provider.settings, settings)
        self.assertEqual(len(sync.provider.client_pool.client_pool), 0)
        self.assertEqual(sync.provider.client_pool.pool_size, 1)

    def test_unknown_provider(self):
        settings = {'aws_bucket': self.aws_bucket,
                    'aws_identity': 'identity',
                    'aws_secret': 'credential',
                    'account': 'account',
                    'container': 'container',
                    'protocol': 'foo'}
        with self.assertRaises(NotImplementedError):
            SyncContainer(self.scratch_space, settings, 1)

    @mock.patch('s3_sync.sync_s3.boto3.session.Session')
    def test_retry_copy_after(self, session_mock):
        settings = {
            'aws_bucket': self.aws_bucket,
            'aws_identity': 'identity',
            'aws_secret': 'credential',
            'account': 'account',
            'container': 'container',
            'copy_after': 3600}
        with self.assertRaises(RetryError):
            sync = SyncContainer(self.scratch_space, settings)
            sync.handle({'deleted': 0, 'created_at': str(time.time())}, None)

        current = time.time()
        with mock.patch('s3_sync.sync_container.time') as time_mock:
            time_mock.time.return_value = current + settings['copy_after'] + 1
            sync = SyncContainer(self.scratch_space, settings)
            sync.provider = mock.Mock()
            sync.handle({'deleted': 0,
                         'created_at': str(time.time()),
                         'name': 'foo',
                         'storage_policy_index': 99}, None)
            sync.provider.upload_object.assert_called_once_with(
                'foo', 99, None)

    @mock.patch('s3_sync.sync_s3.boto3.session.Session')
    def test_retain_copy(self, session_mock):
        settings = {
            'aws_bucket': self.aws_bucket,
            'aws_identity': 'identity',
            'aws_secret': 'credential',
            'account': 'account',
            'container': 'container',
            'retain_local': False}

        sync = SyncContainer(self.scratch_space, settings)
        sync.provider = mock.Mock()
        swift_client = mock.Mock()
        row = {'deleted': 0,
               'created_at': str(time.time() - 5),
               'name': 'foo',
               'storage_policy_index': 99}
        sync.handle(row, swift_client)

        _, _, swift_ts = decode_timestamps(row['created_at'])
        swift_ts.offset += 1

        sync.provider.upload_object.assert_called_once_with(
            row['name'], 99, swift_client)
        swift_client.delete_object.assert_called_once_with(
            settings['account'], settings['container'], row['name'],
            headers={'X-Timestamp': Timestamp(swift_ts).internal})

    @mock.patch('s3_sync.sync_s3.boto3.session.Session')
    def test_no_propagate_delete(self, session_mock):
        settings = {
            'aws_bucket': self.aws_bucket,
            'aws_identity': 'identity',
            'aws_secret': 'credential',
            'account': 'account',
            'container': 'container',
            'propagate_delete': False}

        sync = SyncContainer(self.scratch_space, settings)
        sync.provider = mock.Mock()
        row = {'deleted': 1, 'name': 'tombstone'}
        sync.handle(row, None)

        # Make sure we do nothing with this row
        self.assertEqual([], sync.provider.mock_calls)

    @mock.patch('s3_sync.sync_s3.boto3.session.Session')
    def test_propagate_delete(self, session_mock):
        settings = {
            'aws_bucket': self.aws_bucket,
            'aws_identity': 'identity',
            'aws_secret': 'credential',
            'account': 'account',
            'container': 'container',
            'propagate_delete': True}

        sync = SyncContainer(self.scratch_space, settings)
        sync.provider = mock.Mock()
        row = {'deleted': 1, 'name': 'tombstone'}
        sync.handle(row, None)

        # Make sure that we do not make any additional calls
        self.assertEqual([mock.call.delete_object(row['name'])],
                         sync.provider.mock_calls)
