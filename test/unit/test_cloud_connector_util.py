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

from contextlib import contextmanager
import mock
import os
import pwd

from s3_sync.cloud_connector import util as cc_util

from .test_cloud_connector_app import TestCloudConnectorBase


@contextmanager
def env_changed(new_kvs):
    # Save keys & values
    old_kvs = {}
    for k, v in new_kvs.items():
        if k not in os.environ:
            # None means not present
            old_kvs[k] = None
        else:
            old_kvs[k] = os.environ[k]

    try:
        # Set new k/v pairs (a None means delete)
        for k, v in new_kvs.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

        yield
    finally:
        # Restore old keys & values
        for k, v in old_kvs.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


class TestCloudConnectorUtil(TestCloudConnectorBase):
    def setUp(self):
        super(TestCloudConnectorUtil, self).setUp()

        patcher = mock.patch('s3_sync.cloud_connector.util.requests')
        self.mock_requests = patcher.start()
        self.addCleanup(patcher.stop)

    @mock.patch('s3_sync.cloud_connector.util.os.geteuid')
    @mock.patch('s3_sync.cloud_connector.util.SyncS3')
    def test_get_and_write_bad_geteuid(self, mock_syncs3, mock_geteuid):
        target_path = os.path.join(self.tempdir, 'dlkfjke')
        resp = mock_syncs3().get_object.return_value
        resp.success = True
        resp.body = ['a', 'b', 'c']
        mock_geteuid.side_effect = Exception('JAMMY')

        mock_syncs3.reset_mock()
        with self.assertRaises(cc_util.GetAndWriteFileException) as cm:
            cc_util.get_and_write_conf_file_from_s3('jojo', target_path, {
                'AWS_ACCESS_KEY_ID': "ASIAIYLSOW5USUQCZAAQ",
                'AWS_SECRET_ACCESS_KEY': 'swell',
                'CONF_BUCKET': 'abc',
                'CONF_ENDPOINT': '',  # always set to at least this
            }, 'mr_man')

        self.assertTrue(cm.exception.message.startswith(
            'Writing jojo: '))
        self.assertIn('JAMMY', cm.exception.message)
        self.assertEqual([
            mock.call({
                'aws_identity': 'ASIAIYLSOW5USUQCZAAQ',
                'aws_secret': 'swell',
                'encryption': False,
                'custom_prefix': '',
                'account': 'notused',
                'container': 'notused',
                'aws_bucket': 'abc',
            }),
            mock.call().get_object('jojo'),
        ], mock_syncs3.mock_calls)
        self.assertFalse(os.path.exists(target_path))

    @mock.patch('s3_sync.cloud_connector.util.SyncS3')
    def test_get_and_write_bad_get(self, mock_syncs3):
        target_path = os.path.join(self.tempdir, 'dlkfjke')
        resp = mock_syncs3().get_object.return_value
        resp.success = False
        resp.status = 'NOT GREAT, BRO.'
        resp.body = ['a', 'b', 'c']

        mock_syncs3.reset_mock()
        with self.assertRaises(cc_util.GetAndWriteFileException) as cm:
            cc_util.get_and_write_conf_file_from_s3('jojo', target_path, {
                'AWS_ACCESS_KEY_ID': "ASIAIYLSOW5USUQCZAAQ",
                'AWS_SECRET_ACCESS_KEY': 'swell',
                'CONF_BUCKET': 'abc',
                'CONF_ENDPOINT': '',  # always set to at least this
            })

        self.assertEqual('GET for jojo: NOT GREAT, BRO. abc',
                         cm.exception.message)
        self.assertEqual([
            mock.call({
                'aws_identity': 'ASIAIYLSOW5USUQCZAAQ',
                'aws_secret': 'swell',
                'encryption': False,
                'custom_prefix': '',
                'account': 'notused',
                'container': 'notused',
                'aws_bucket': 'abc',
            }),
            mock.call().get_object('jojo'),
        ], mock_syncs3.mock_calls)
        self.assertFalse(os.path.exists(target_path))

    @mock.patch('s3_sync.cloud_connector.util.os.fchmod')
    @mock.patch('s3_sync.cloud_connector.util.os.geteuid')
    @mock.patch('s3_sync.cloud_connector.util.os.fchown')
    @mock.patch('s3_sync.cloud_connector.util.SyncS3')
    def test_get_and_write(self, mock_syncs3, mock_fchown, mock_geteuid,
                           mock_fchmod):
        target_path = os.path.join(self.tempdir, 'dlkfjke')
        resp = mock_syncs3().get_object.return_value
        resp.success = True
        resp.body = ['a', 'b', 'c']

        mock_syncs3.reset_mock()
        cc_util.get_and_write_conf_file_from_s3('jojo', target_path, {
            'AWS_ACCESS_KEY_ID': "ASIAIYLSOW5USUQCZAAQ",
            'AWS_SECRET_ACCESS_KEY': 'swell',
            'CONF_BUCKET': 'abc',
            'CONF_ENDPOINT': '',  # always set to at least this
        })

        self.assertEqual([], mock_fchown.mock_calls)
        self.assertEqual([], mock_geteuid.mock_calls)
        self.assertEqual([
            mock.call({
                'aws_identity': 'ASIAIYLSOW5USUQCZAAQ',
                'aws_secret': 'swell',
                'encryption': False,
                'custom_prefix': '',
                'account': 'notused',
                'container': 'notused',
                'aws_bucket': 'abc',
            }),
            mock.call().get_object('jojo'),
        ], mock_syncs3.mock_calls)
        self.assertEqual('abc', open(target_path).read())
        self.assertEqual([mock.call(mock.ANY, 0o0640)], mock_fchmod.mock_calls)

    @mock.patch('s3_sync.cloud_connector.util.pwd.getpwnam')
    @mock.patch('s3_sync.cloud_connector.util.os.geteuid')
    @mock.patch('s3_sync.cloud_connector.util.os.fchown')
    @mock.patch('s3_sync.cloud_connector.util.SyncS3')
    def test_get_and_write_with_ownership(self, mock_syncs3, mock_fchown,
                                          mock_geteuid, mock_getpwnam):
        target_path = os.path.join(self.tempdir, 'dlkfjke')
        resp = mock_syncs3().get_object.return_value
        resp.success = True
        resp.body = ['a', 'b', 'c']
        mock_geteuid.return_value = 0
        mock_getpwnam.return_value = pwd.struct_passwd(
            ('name', 'pass', 42, 43, 'gecos', 'dir', 'shell'))

        mock_syncs3.reset_mock()
        cc_util.get_and_write_conf_file_from_s3('jojo', target_path, {
            'AWS_ACCESS_KEY_ID': "ASIAIYLSOW5USUQCZAAQ",
            'AWS_SECRET_ACCESS_KEY': 'swell',
            'CONF_BUCKET': 'abc',
            'CONF_ENDPOINT': 'llii',
            'AWS_SECURITY_TOKEN_STRING': 'difi',
        }, user='teapot')

        self.assertEqual([mock.call(mock.ANY, 42, 43)], mock_fchown.mock_calls)
        self.assertEqual([mock.call()], mock_geteuid.mock_calls)
        self.assertEqual([
            mock.call({
                'aws_endpoint': 'llii',
                'aws_identity': 'ASIAIYLSOW5USUQCZAAQ',
                'aws_secret': 'swell',
                'aws_session_token': 'difi',
                'encryption': False,
                'custom_prefix': '',
                'account': 'notused',
                'container': 'notused',
                'aws_bucket': 'abc',
            }),
            mock.call().get_object('jojo'),
        ], mock_syncs3.mock_calls)
        self.assertEqual('abc', open(target_path).read())
        self.assertEqual([mock.call('teapot')], mock_getpwnam.mock_calls)

    @mock.patch('s3_sync.cloud_connector.util.os.geteuid')
    @mock.patch('s3_sync.cloud_connector.util.os.fchown')
    @mock.patch('s3_sync.cloud_connector.util.SyncS3')
    def test_get_and_write_with_extras(self, mock_syncs3, mock_fchown,
                                       mock_geteuid):
        target_path = os.path.join(self.tempdir, 'dlkfjke')
        resp = mock_syncs3().get_object.return_value
        resp.success = True
        resp.body = ['a', 'b', 'c']
        mock_geteuid.return_value = 1

        mock_syncs3.reset_mock()
        cc_util.get_and_write_conf_file_from_s3('jojo', target_path, {
            'AWS_ACCESS_KEY_ID': "ASIAIYLSOW5USUQCZAAQ",
            'AWS_SECRET_ACCESS_KEY': 'swell',
            'CONF_BUCKET': 'abc',
            'CONF_ENDPOINT': 'llii',
            'AWS_SECURITY_TOKEN_STRING': 'difi',
        }, user='teapot')

        self.assertEqual([], mock_fchown.mock_calls)
        self.assertEqual([mock.call()], mock_geteuid.mock_calls)
        self.assertEqual([
            mock.call({
                'aws_endpoint': 'llii',
                'aws_identity': 'ASIAIYLSOW5USUQCZAAQ',
                'aws_secret': 'swell',
                'aws_session_token': 'difi',
                'encryption': False,
                'custom_prefix': '',
                'account': 'notused',
                'container': 'notused',
                'aws_bucket': 'abc',
            }),
            mock.call().get_object('jojo'),
        ], mock_syncs3.mock_calls)
        self.assertEqual('abc', open(target_path).read())

    def test_get_aws_ecs_creds_no_uri(self):
        with env_changed({'AWS_CONTAINER_CREDENTIALS_RELATIVE_URI': None}):
            self.assertEqual({}, cc_util.get_aws_ecs_creds())

    def test_get_aws_ecs_creds_bad_status(self):
        with env_changed({'AWS_CONTAINER_CREDENTIALS_RELATIVE_URI':
                          'stub_uri'}):
            with self.assertRaises(Exception) as cm:
                get_resp = self.mock_requests.get.return_value
                get_resp.raise_for_status.side_effect = Exception('gah!')
                self.mock_requests.reset_mock()
                cc_util.get_aws_ecs_creds()
        self.assertEqual('gah!', cm.exception.message)
        self.assertEqual([
            mock.call.get('http://169.254.170.2%s' % ('stub_uri',)),
            mock.call.get().raise_for_status(),
        ], self.mock_requests.mock_calls)

    def test_get_aws_ecs_creds_success(self):
        with env_changed({'AWS_CONTAINER_CREDENTIALS_RELATIVE_URI':
                          'stub_uri'}):
            get_resp = self.mock_requests.get.return_value
            get_resp.json.return_value = {
                "RoleArn": "arn:aws:iam::111111111111:role/test-service",
                "AccessKeyId": "ASIAIYLSOW5USUQCZAAQ",
                "SecretAccessKey": "swell",
                "Token": "swimmingly",
                "Expiration": "2017-08-10T02:01:43Z",
            }
            self.mock_requests.reset_mock()
            got_creds = cc_util.get_aws_ecs_creds()
        self.assertEqual({
            'AWS_ACCESS_KEY_ID': "ASIAIYLSOW5USUQCZAAQ",
            'AWS_SECRET_ACCESS_KEY': 'swell',
            'AWS_SECURITY_TOKEN_STRING': 'swimmingly',
        }, got_creds)
        self.assertEqual([
            mock.call.get('http://169.254.170.2%s' % ('stub_uri',)),
            mock.call.get().raise_for_status(),
            mock.call.get().json(),
        ], self.mock_requests.mock_calls)

    @mock.patch('s3_sync.cloud_connector.util.get_aws_ecs_creds')
    def test_get_env_options_no_creds(self, mock_get_aws_ecs_creds):
        for env_set in [
                {'AWS_ACCESS_KEY_ID': None, 'AWS_SECRET_ACCESS_KEY': 'a'},
                {'AWS_ACCESS_KEY_ID': 'a', 'AWS_SECRET_ACCESS_KEY': None}]:
            with env_changed(env_set):
                with self.assertRaises(SystemExit):
                    cc_util.get_env_options()

        with env_changed({'AWS_ACCESS_KEY_ID': None,
                          'AWS_SECRET_ACCESS_KEY': None}):
            mock_get_aws_ecs_creds.return_value = {
                'AWS_ACCESS_KEY_ID': 'a',
            }
            with self.assertRaises(SystemExit):
                cc_util.get_env_options()

            mock_get_aws_ecs_creds.return_value = {
                'AWS_SECRET_ACCESS_KEY': 'a',
            }
            with self.assertRaises(SystemExit):
                cc_util.get_env_options()

    def test_get_env_options_no_conf_bucket(self):
        with env_changed({'AWS_ACCESS_KEY_ID': 'a',
                          'AWS_SECRET_ACCESS_KEY': 'b'}):
            with self.assertRaises(SystemExit):
                cc_util.get_env_options()

    @mock.patch('s3_sync.cloud_connector.util.get_aws_ecs_creds')
    def test_get_env_options_specific_creds_win(self, mock_get_aws_ecs_creds):
        with env_changed({'AWS_ACCESS_KEY_ID': 'a',
                          'AWS_SECRET_ACCESS_KEY': 'b',
                          'CONF_BUCKET': 'jojo'}):
            mock_get_aws_ecs_creds.return_value = {
                'AWS_ACCESS_KEY_ID': 'c',
                'AWS_SECRET_ACCESS_KEY': 'd',
            }
            self.assertEqual({
                'AWS_ACCESS_KEY_ID': 'a',
                'AWS_SECRET_ACCESS_KEY': 'b',
                'CONF_BUCKET': 'jojo',
                'CONF_ENDPOINT': '',  # default
                'CONF_NAME': 'cloud-connector.conf',  # default
            }, cc_util.get_env_options())

    @mock.patch('s3_sync.cloud_connector.util.get_aws_ecs_creds')
    def test_get_env_options_with_aws_ecs_creds(self, mock_get_aws_ecs_creds):
        with env_changed({'AWS_ACCESS_KEY_ID': None,
                          'AWS_SECRET_ACCESS_KEY': None,
                          'CONF_BUCKET': 'jojo',
                          'CONF_ENDPOINT': 'dldl',
                          'CONF_NAME': 'ioio'}):
            mock_get_aws_ecs_creds.return_value = {
                'AWS_ACCESS_KEY_ID': "ASIAIYLSOW5USUQCZAAQ",
                'AWS_SECRET_ACCESS_KEY': 'swell',
                'AWS_SECURITY_TOKEN_STRING': 'swimmingly',
            }
            self.assertEqual({
                'AWS_ACCESS_KEY_ID': "ASIAIYLSOW5USUQCZAAQ",
                'AWS_SECRET_ACCESS_KEY': 'swell',
                'AWS_SECURITY_TOKEN_STRING': 'swimmingly',
                'CONF_BUCKET': 'jojo',
                'CONF_ENDPOINT': 'dldl',
                'CONF_NAME': 'ioio',
            }, cc_util.get_env_options())
