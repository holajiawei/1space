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

import datetime
from io import BytesIO
import mock
import StringIO
import sys
import unittest

from s3_sync.base_sync import ProviderResponse
from s3_sync.verify import main


@mock.patch('s3_sync.verify.validate_bucket', return_value=object())
class TestMainTrackProvider(unittest.TestCase):
    def test_account_requires_swift(self, mock_validate):
        msg = 'Invalid argument: account is only valid with swift protocol'
        self.assertEqual(msg, main([
            '--protocol', 's3',
            '--endpoint', 'https://s3.amazonaws.com',
            '--username', 'access id',
            '--password', 'secret key',
            '--account', 'AUTH_account',
            '--bucket', 'some-bucket',
        ]))
        self.assertEqual(mock_validate.mock_calls, [])

    def test_bucket_cant_have_slash(self, mock_validate):
        msg = 'Invalid argument: slash is not allowed in container name'
        self.assertEqual(msg, main([
            '--protocol', 's3',
            '--endpoint', 'https://s3.amazonaws.com',
            '--username', 'access id',
            '--password', 'secret key',
            '--bucket', 'some/bucket',
        ]))
        self.assertEqual(mock_validate.mock_calls, [])

    def _stderr_of_sysexit(self, args):
        with self.assertRaises(SystemExit), \
                mock.patch('sys.stderr', new_callable=BytesIO) as err:
            got = main(args)
            # This little adapter lets us also test error conditions where
            # the message is returned, for the surrounding "exit(...)" to
            # turn into a SystemExit.
            if got:
                print >>sys.stderr, got
                raise SystemExit()
        return err.getvalue()

    def test_missing_args(self, mock_validate):
        def do_test(args, missing_arg):
            stderr_stuff = self._stderr_of_sysexit(args)
            self.assertIn('argument %s is required' % missing_arg,
                          stderr_stuff)

        do_test([
            '--endpoint', 'https://s3.amazonaws.com',
            '--username', 'access id',
            '--password', 'secret key',
        ], '--protocol')
        do_test([
            '--protocol', 's3',
            '--username', 'access id',
            '--password', 'secret key',
        ], '--endpoint')
        do_test([
            '--protocol', 's3',
            '--endpoint', 'https://s3.amazonaws.com',
            '--password', 'secret key',
        ], '--username')
        do_test([
            '--protocol', 's3',
            '--endpoint', 'https://s3.amazonaws.com',
            '--username', 'access id',
        ], '--password')
        do_test([
            '--protocol', 'swift',
            '--endpoint', 'http://1space-keystone:5000/v3',
            '--username', 'access id',
            '--password', 'secret key',
            "--auth-type", "keystone_v2",
        ], "--tenant-name")
        do_test([
            '--protocol', 'swift',
            '--endpoint', 'http://1space-keystone:5000/v3',
            '--username', 'access id',
            '--password', 'secret key',
            "--auth-type", "keystone_v3",
            "--project-name", "test",
            "--user-domain-name", "default",
        ], "--project-domain-name")
        do_test([
            '--protocol', 'swift',
            '--endpoint', 'http://1space-keystone:5000/v3',
            '--username', 'access id',
            '--password', 'secret key',
            "--auth-type", "keystone_v3",
            "--project-name", "test",
            "--project-domain-name", "default",
        ], "--user-domain-name")
        do_test([
            '--protocol', 'swift',
            '--endpoint', 'http://1space-keystone:5000/v3',
            '--username', 'access id',
            '--password', 'secret key',
            "--auth-type", "keystone_v3",
            "--user-domain-name", "default",
            "--project-domain-name", "default",
        ], "--project-name")

    def test_keystone_requires_swift_proto(self, mock_validate):
        exit_arg = main([
            '--protocol', 's3',
            '--endpoint', 'https://s3.amazonaws.com',
            '--username', 'access id',
            '--password', 'secret key',
            "--auth-type", "keystone_v3",
            "--project-name", "test",
            "--user-domain-name", "default",
            "--project-domain-name", "default",
        ])
        self.assertIn('Keystone auth requires swift protocol',
                      exit_arg)
        exit_arg = main([
            '--protocol', 's3',
            '--endpoint', 'https://s3.amazonaws.com',
            '--username', 'access id',
            '--password', 'secret key',
            "--auth-type", "keystone_v2",
            "--tenant-name", "test",
        ])
        self.assertIn('Keystone auth requires swift protocol',
                      exit_arg)

    def test_auth_type_choices(self, mock_validate):
        got = self._stderr_of_sysexit([
            '--protocol', 'swift',
            '--endpoint', 'http://1space-keystone:5000/v3',
            '--username', 'access id',
            '--password', 'secret key',
            "--auth-type", "flimflam",
        ])
        self.assertIn('invalid choice: ', got)
        self.assertIn("choose from 'keystone_v2', 'keystone_v3'", got)

    def test_aws_adjusts_endpoint(self, mock_validate):
        exit_arg = main([
            '--protocol', 's3',
            '--endpoint', 'https://s3.amazonaws.com',
            '--username', 'access id',
            '--password', 'secret key',
            '--bucket', 'some-bucket',
        ])
        self.assertIs(exit_arg, mock_validate.return_value)
        self.assertEqual([mock.ANY], mock_validate.mock_calls)
        provider, swift_key, create_bucket = mock_validate.mock_calls[0][1]
        self.assertEqual({
            k: v for k, v in provider.settings.items() if k != 'container'
        }, {
            'protocol': 's3',
            'aws_endpoint': None,
            'aws_identity': 'access id',
            'aws_secret': 'secret key',
            'aws_bucket': 'some-bucket',
            'account': 'verify-auth',
            'custom_prefix': None,
            'remote_account': None,
        })
        self.assertEqual(swift_key, 'fabcab/cloud_sync_test')
        self.assertFalse(create_bucket)

    def test_aws_with_prefix(self, mock_validate):
        exit_arg = main([
            '--protocol', 's3',
            '--endpoint', 'https://s3.amazonaws.com',
            '--username', 'access id',
            '--password', 'secret key',
            '--bucket', 'some-bucket',
            '--prefix', 'jojo/hoho/',
        ])
        self.assertIs(exit_arg, mock_validate.return_value)
        self.assertEqual([mock.ANY], mock_validate.mock_calls)
        provider, swift_key, create_bucket = mock_validate.mock_calls[0][1]
        self.assertEqual({
            'protocol': 's3',
            'aws_endpoint': None,
            'aws_identity': 'access id',
            'aws_secret': 'secret key',
            'aws_bucket': 'some-bucket',
            'account': 'verify-auth',
            'custom_prefix': 'jojo/hoho/',
            'remote_account': None,
        }, {k: v for k, v in provider.settings.items() if k != 'container'})
        self.assertEqual(swift_key, 'cloud_sync_test')
        self.assertFalse(create_bucket)

    def test_google_leaves_endpoint_alone(self, mock_validate):
        exit_arg = main([
            '--protocol', 's3',
            '--endpoint', 'https://storage.googleapis.com',
            '--username', 'access id',
            '--password', 'secret key',
            '--bucket', 'some-bucket',
        ])
        self.assertIs(exit_arg, mock_validate.return_value)
        self.assertEqual([mock.ANY], mock_validate.mock_calls)
        provider, swift_key, create_bucket = mock_validate.mock_calls[0][1]
        self.assertEqual({
            k: v for k, v in provider.settings.items()
            if k.startswith('aws_') or k in ('protocol',)
        }, {
            'protocol': 's3',
            'aws_endpoint': 'https://storage.googleapis.com',
            'aws_identity': 'access id',
            'aws_secret': 'secret key',
            'aws_bucket': 'some-bucket',
        })
        self.assertEqual(swift_key, 'fabcab/cloud_sync_test')
        self.assertFalse(create_bucket)

    def test_swift_one_bucket(self, mock_validate):
        exit_arg = main([
            '--protocol', 'swift',
            '--endpoint', 'https://saio:8080/auth/v1.0',
            '--username', 'access id',
            '--password', 'secret key',
            '--bucket', 'some-bucket',
        ])
        self.assertIs(exit_arg, mock_validate.return_value)
        self.assertEqual([mock.ANY], mock_validate.mock_calls)
        provider, swift_key, create_bucket = mock_validate.mock_calls[0][1]
        self.assertEqual({
            k: v for k, v in provider.settings.items()
            if k.startswith('aws_') or k in ('protocol',)
        }, {
            'protocol': 'swift',
            'aws_endpoint': 'https://saio:8080/auth/v1.0',
            'aws_identity': 'access id',
            'aws_secret': 'secret key',
            'aws_bucket': 'some-bucket',
        })
        self.assertEqual(swift_key, 'cloud_sync_test_object')
        self.assertFalse(create_bucket)

    def test_swift_all_buckets(self, mock_validate):
        exit_arg = main([
            '--protocol', 'swift',
            '--endpoint', 'https://saio:8080/auth/v1.0',
            '--username', 'access id',
            '--password', 'secret key',
            '--bucket', '/*',
        ])
        self.assertIs(exit_arg, mock_validate.return_value)
        self.assertEqual([mock.ANY], mock_validate.mock_calls)
        provider, swift_key, create_bucket = mock_validate.mock_calls[0][1]
        self.assertEqual({
            k: v for k, v in provider.settings.items()
            if k.startswith('aws_') or k in ('protocol',)
        }, {
            'protocol': 'swift',
            'aws_endpoint': 'https://saio:8080/auth/v1.0',
            'aws_identity': 'access id',
            'aws_secret': 'secret key',
            'aws_bucket': u'.cloudsync_test_container-\U0001f44d',
        })
        self.assertEqual(swift_key, 'cloud_sync_test_object')
        self.assertTrue(create_bucket)

    def test_swift_keystone_v2(self, mock_validate):
        exit_arg = main([
            '--protocol', 'swift',
            '--endpoint', 'http://1space-keystone:5000/v3',
            '--username', 'access id',
            '--password', 'secret key',
            "--auth-type", "keystone_v2",
            "--tenant-name", "flipper-flapp",
            '--bucket', '/*',
        ])
        self.assertIs(exit_arg, mock_validate.return_value)
        self.assertEqual([mock.ANY], mock_validate.mock_calls)
        provider, swift_key, create_bucket = mock_validate.mock_calls[0][1]
        self.assertEqual({
            'protocol': 'swift',
            'aws_endpoint': 'http://1space-keystone:5000/v3',
            'aws_identity': 'access id',
            'aws_secret': 'secret key',
            'aws_bucket': u'.cloudsync_test_container-\U0001f44d',
            'account': 'verify-auth',
            'container': u'testing-\U0001f44d',
            'custom_prefix': None,
            'remote_account': None,
            'auth_type': 'keystone_v2',
            'tenant_name': 'flipper-flapp',
        }, provider.settings)
        self.assertEqual(swift_key, 'cloud_sync_test_object')
        self.assertTrue(create_bucket)

    def test_swift_keystone_v3(self, mock_validate):
        exit_arg = main([
            '--protocol', 'swift',
            '--endpoint', 'http://1space-keystone:5000/v3',
            '--username', 'access id',
            '--password', 'secret key',
            "--auth-type", "keystone_v3",
            "--project-name", "test",
            "--project-domain-name", "wat-wat",
            "--user-domain-name", 'floo-boo',
            '--bucket', '/*',
        ])
        self.assertIs(exit_arg, mock_validate.return_value)
        self.assertEqual([mock.ANY], mock_validate.mock_calls)
        provider, swift_key, create_bucket = mock_validate.mock_calls[0][1]
        self.assertEqual({
            'protocol': 'swift',
            'aws_endpoint': 'http://1space-keystone:5000/v3',
            'aws_identity': 'access id',
            'aws_secret': 'secret key',
            'aws_bucket': u'.cloudsync_test_container-\U0001f44d',
            'account': 'verify-auth',
            'container': u'testing-\U0001f44d',
            'custom_prefix': None,
            'remote_account': None,
            'auth_type': 'keystone_v3',
            'user_domain_name': 'floo-boo',
            'project_name': 'test',
            'project_domain_name': 'wat-wat',
        }, provider.settings)
        self.assertEqual(swift_key, 'cloud_sync_test_object')
        self.assertTrue(create_bucket)


@mock.patch('s3_sync.base_sync.BaseSync.HttpClientPool.get_client')
class TestMainTrackClientCalls(unittest.TestCase):
    def assert_calls(self, mock_obj, calls):
        actual_calls = iter(mock_obj.mock_calls)
        for i, expected in enumerate(calls):
            for actual in actual_calls:
                if actual == expected:
                    break
            else:
                self.fail('Never found %r after %r in %r' % (
                    expected, calls[:i], mock_obj.mock_calls))

    def test_aws_no_bucket(self, mock_get_client):
        mock_client = \
            mock_get_client.return_value.__enter__.return_value
        mock_client.list_buckets.return_value = {
            'Buckets': [{'CreationDate': datetime.datetime.now(),
                         'Name': 'test-bucket'}],
            'ResponseMetadata': {
                'HTTPStatusCode': 200,
                'HTTPHeaders': {}}}
        exit_arg = main([
            '--protocol', 's3',
            '--endpoint', 'https://s3.amazonaws.com',
            '--username', 'access id',
            '--password', 'secret key',
        ])
        self.assertEqual(exit_arg, 0)
        self.assertEqual(mock_client.mock_calls, [
            mock.call.list_buckets(),
        ])

    def test_aws_with_bucket(self, mock_get_client):
        mock_client = \
            mock_get_client.return_value.__enter__.return_value

        mock_client.put_object.return_value = {
            'Body': [''],
            'ResponseMetadata': {
                'HTTPStatusCode': 201,
                'HTTPHeaders': {}
            }
        }
        mock_client.head_object.return_value = {
            'Body': [''],
            'ResponseMetadata': {
                'HTTPStatusCode': 200,
                'HTTPHeaders': {
                    'x-amz-meta-cloud-sync': 'fabcab',
                },
            },
        }
        mock_client.copy_object.return_value = {
            'CopyObjectResult': {}
        }
        mock_client.list_objects.return_value = {
            'Contents': [],
            'ResponseMetadata': {
                'HTTPStatusCode': 200,
                'HTTPHeaders': {
                    'x-amz-meta-cloud-sync': 'fabcab',
                },
            },
        }
        mock_client.delete_object.return_value = {
            'DeleteMarker': False,
            'VersionId': '',
        }
        exit_arg = main([
            '--protocol', 's3',
            '--endpoint', 'https://s3.amazonaws.com',
            '--username', 'access id',
            '--password', 'secret key',
            '--bucket', 'some-bucket',
        ])
        self.assertEqual(exit_arg, 0)
        key = u'9f9835/verify-auth/testing-\U0001f44d/fabcab/cloud_sync_test'
        self.assert_calls(mock_client, [
            mock.call.put_object(
                Body='1space-test',
                Bucket='some-bucket',
                ContentLength=11,
                ContentType='text/plain',
                Key=key,
                Metadata={},
                ServerSideEncryption='AES256'),
            mock.call.copy_object(
                Bucket='some-bucket',
                ContentType='text/plain',
                CopySource={'Bucket': 'some-bucket', 'Key': key},
                Key=key,
                Metadata={'cloud-sync': 'fabcab'},
                MetadataDirective='REPLACE',
                ServerSideEncryption='AES256'),
            mock.call.head_object(
                Bucket='some-bucket',
                Key=key),
            mock.call.list_objects(
                Bucket='some-bucket',
                MaxKeys=1,
                Prefix=u'9f9835/verify-auth/testing-\U0001f44d/'),
            mock.call.delete_object(
                Bucket='some-bucket',
                Key=key),
        ])

    def test_aws_with_bucket_and_prefix(self, mock_get_client):
        mock_client = \
            mock_get_client.return_value.__enter__.return_value
        mock_client.put_object.return_value = {
            'Body': [''],
            'ResponseMetadata': {
                'HTTPStatusCode': 201,
                'HTTPHeaders': {}
            }
        }
        mock_client.head_object.return_value = {
            'Body': [''],
            'ResponseMetadata': {
                'HTTPStatusCode': 200,
                'HTTPHeaders': {
                    'x-amz-meta-cloud-sync': 'fabcab',
                },
            },
        }
        mock_client.copy_object.return_value = {'CopyObjectResult': {}}
        mock_client.list_objects.return_value = {
            'Contents': [],
            'ResponseMetadata': {
                'HTTPStatusCode': 200,
                'HTTPHeaders': {
                    'x-amz-meta-cloud-sync': 'fabcab',
                },
            },
        }
        mock_client.delete_object.return_value = {
            'DeleteMarker': False,
            'VersionId': '',
        }
        exit_arg = main([
            '--protocol', 's3',
            '--endpoint', 'https://s3.amazonaws.com',
            '--username', 'access id',
            '--password', 'secret key',
            '--bucket', 'some-bucket',
            '--prefix', 'heehee/hawhaw/',
        ])
        self.assertEqual(exit_arg, 0)
        key = u'heehee/hawhaw/cloud_sync_test'
        self.assert_calls(mock_client, [
            mock.call.put_object(
                Body='1space-test',
                Bucket='some-bucket',
                ContentLength=11,
                ContentType='text/plain',
                Key=key,
                Metadata={},
                ServerSideEncryption='AES256'),
            mock.call.copy_object(
                Bucket='some-bucket',
                ContentType='text/plain',
                CopySource={'Bucket': 'some-bucket', 'Key': key},
                Key=key,
                Metadata={'cloud-sync': 'fabcab'},
                MetadataDirective='REPLACE',
                ServerSideEncryption='AES256'),
            mock.call.head_object(
                Bucket='some-bucket',
                Key=key),
            mock.call.list_objects(
                Bucket='some-bucket',
                MaxKeys=1,
                Prefix=u'heehee/hawhaw/'),
            mock.call.delete_object(
                Bucket='some-bucket',
                Key=key),
        ])

    def test_google_no_bucket(self, mock_get_client):
        mock_client = \
            mock_get_client.return_value.__enter__.return_value
        mock_client.list_buckets.return_value = {
            'Buckets': [{'CreationDate': datetime.datetime.now(),
                         'Name': 'google-bucket'}],
            'ResponseMetadata': {
                'HTTPStatusCode': 200,
                'HTTPHeaders': {}}}
        exit_arg = main([
            '--protocol', 's3',
            '--endpoint', 'https://storage.googleapis.com',
            '--username', 'access id',
            '--password', 'secret key',
        ])
        self.assertEqual(exit_arg, 0)
        self.assertEqual(mock_client.mock_calls, [
            mock.call.list_buckets(),
        ])

    def test_google_with_bucket(self, mock_get_client):
        mock_client = \
            mock_get_client.return_value.__enter__.return_value
        mock_client.put_object.return_value = {
            'Body': [''],
            'ResponseMetadata': {
                'HTTPStatusCode': 201,
                'HTTPHeaders': {}
            }
        }
        mock_client.head_object.return_value = {
            'Body': [''],
            'ResponseMetadata': {
                'HTTPStatusCode': 200,
                'HTTPHeaders': {
                    'x-amz-meta-cloud-sync': 'fabcab',
                },
            },
        }

        mock_client.copy_object.return_value = {
            'Body': [''],
            'ResponseMetadata': {
                'HTTPStatusCode': 200,
            },
            'CopyObjectResult': {}
        }

        mock_client.list_objects.return_value = {
            'Contents': [],
            'ResponseMetadata': {
                'HTTPStatusCode': 200,
                'HTTPHeaders': {
                    'x-amz-meta-cloud-sync': 'fabcab',
                },
            },
        }
        exit_arg = main([
            '--protocol', 's3',
            '--endpoint', 'https://storage.googleapis.com',
            '--username', 'access id',
            '--password', 'secret key',
            '--bucket', 'some-bucket',
        ])
        self.assertEqual(exit_arg, 0)
        key = u'9f9835/verify-auth/testing-\U0001f44d/fabcab/cloud_sync_test'
        self.assert_calls(mock_client, [
            mock.call.put_object(
                Body='1space-test',
                Bucket='some-bucket',
                ContentLength=11,
                ContentType='text/plain',
                Key=key,
                Metadata={}),
            mock.call.copy_object(
                Bucket='some-bucket',
                ContentType='text/plain',
                CopySource={'Bucket': 'some-bucket', 'Key': key},
                Key=key,
                Metadata={'cloud-sync': 'fabcab'},
                MetadataDirective='REPLACE'),
            mock.call.head_object(
                Bucket='some-bucket',
                Key=key),
            mock.call.list_objects(
                Bucket='some-bucket',
                MaxKeys=1,
                Prefix=u'9f9835/verify-auth/testing-\U0001f44d/'),
            mock.call.delete_object(
                Bucket='some-bucket',
                Key=key),
        ])

    def test_swift_no_bucket(self, mock_get_client):
        mock_client = \
            mock_get_client.return_value.__enter__.return_value
        mock_client.get_account.return_value = (
            {}, [{'name': 'swift-container'}])
        exit_arg = main([
            '--protocol', 'swift',
            '--endpoint', 'https://saio:8080/auth/v1.0',
            '--username', 'access id',
            '--password', 'secret key',
        ])
        self.assertEqual(exit_arg, 0)
        self.assertEqual(mock_client.mock_calls, [
            mock.call.get_account(
                headers={}, prefix='', limit=1, marker='')
        ])

    def test_swift_with_bucket(self, mock_get_client):
        def _fake_put_object(*args, **kwargs):
            kwargs['response_dict']['headers'] = {}
            kwargs['response_dict']['status'] = 201
            return 'deadbeef'

        mock_client = \
            mock_get_client.return_value.__enter__.return_value
        mock_client.put_object.side_effect = _fake_put_object
        mock_client.head_object.side_effect = [
            {'x-object-meta-cloud-sync': 'fabcab'},
            {'x-object-meta-cloud-sync': 'fabcab'},  # One extra for the DELETE
        ]
        mock_client.get_container.return_value = ({}, [])
        mock_client.post_object.return_value = None
        mock_client.delete_object.return_value = None
        exit_arg = main([
            '--protocol', 'swift',
            '--endpoint', 'https://saio:8080/auth/v1.0',
            '--username', 'access id',
            '--password', 'secret key',
            '--bucket', 'some-bucket',
        ])
        self.assertEqual(exit_arg, 0)
        self.assertEqual(mock_client.mock_calls, [
            mock.call.put_object(
                'some-bucket', 'cloud_sync_test_object',
                contents='1space-test',
                headers={'content-type': 'text/plain'},
                response_dict=mock.ANY, query_string=None),
            mock.call.post_object(
                'some-bucket', 'cloud_sync_test_object', headers={
                    'content-type': 'text/plain',
                    'X-Object-Meta-Cloud-Sync': 'fabcab'}),
            mock.call.head_object('some-bucket', 'cloud_sync_test_object',
                                  headers={}),
            mock.call.get_container('some-bucket', delimiter='', limit=1,
                                    marker='', prefix='', headers={}),
            mock.call.head_object('some-bucket', 'cloud_sync_test_object',
                                  headers={}),
            mock.call.delete_object('some-bucket', 'cloud_sync_test_object',
                                    headers={}),
        ])

    def test_swift_with_bucket_and_prefix(self, mock_get_client):
        def _fake_put_object(*args, **kwargs):
            kwargs['response_dict']['headers'] = {}
            kwargs['response_dict']['status'] = 201
            return 'deadbeef'

        # custom_prefix doesn't do much (anything?) for Swift provider
        mock_client = \
            mock_get_client.return_value.__enter__.return_value
        mock_client.put_object.side_effect = _fake_put_object
        mock_client.head_object.side_effect = [
            {'x-object-meta-cloud-sync': 'fabcab'},
            {'x-object-meta-cloud-sync': 'fabcab'},  # One extra for the DELETE
        ]
        mock_client.get_container.return_value = ({}, [])
        mock_client.post_object.return_value = None
        mock_client.delete_object.return_value = None
        exit_arg = main([
            '--protocol', 'swift',
            '--endpoint', 'https://saio:8080/auth/v1.0',
            '--username', 'access id',
            '--password', 'secret key',
            '--bucket', 'some-bucket',
            '--prefix', 'floo/gloo/',
        ])
        self.assertEqual(exit_arg, 0)
        self.assertEqual(mock_client.mock_calls, [
            mock.call.put_object(
                'some-bucket', 'cloud_sync_test_object',
                contents='1space-test',
                headers={'content-type': 'text/plain'},
                response_dict=mock.ANY,
                query_string=None),
            mock.call.post_object(
                'some-bucket', 'cloud_sync_test_object', headers={
                    'content-type': 'text/plain',
                    'X-Object-Meta-Cloud-Sync': 'fabcab'}),
            mock.call.head_object('some-bucket', 'cloud_sync_test_object',
                                  headers={}),
            mock.call.get_container('some-bucket', delimiter='', limit=1,
                                    marker='', prefix='', headers={}),
            mock.call.head_object('some-bucket', 'cloud_sync_test_object',
                                  headers={}),
            mock.call.delete_object('some-bucket', 'cloud_sync_test_object',
                                    headers={}),
        ])


class TestVerify(unittest.TestCase):
    class TrackingStringIO(StringIO.StringIO, object):
        def close(self):
            self.last_pos = self.tell()
            super(TestVerify.TrackingStringIO, self).close()

    @mock.patch('s3_sync.verify.create_provider')
    def test_read_only_with_bucket_s3(self, mock_provider_factory):
        mock_provider = mock_provider_factory.return_value
        args = ['--protocol', 's3',
                '--username', 'id',
                '--password', 'key',
                '--read-only',
                '--endpoint', 'https://s3.amazonaws.com',
                '--bucket', 'some-bucket']

        body = self.TrackingStringIO('response')
        mock_provider.list_objects.return_value = ProviderResponse(
            True, 200, {}, [{'name': 'foo'}])
        mock_provider.get_object.return_value = ProviderResponse(
            True, 200, {}, body)
        mock_provider.head_object.return_value = ProviderResponse(
            True, 204, {}, '')
        exit_arg = main(args)

        self.assertEqual(0, exit_arg)
        mock_provider.list_objects.assert_called_once_with(
            None, 1, None, bucket=None)
        mock_provider.head_object.assert_called_once_with(
            'foo', bucket=None)
        mock_provider.get_object.assert_called_once_with(
            'foo', bucket=None)
        self.assertEqual(0, body.last_pos)
        self.assertTrue(body.closed)

    @mock.patch('s3_sync.verify.create_provider')
    def test_read_only_with_bucket_swift(self, mock_provider_factory):
        mock_provider = mock.Mock()

        def _fake_create(conf, max_conns=1024):
            mock_provider.settings = conf
            return mock_provider

        mock_provider_factory.side_effect = _fake_create

        args = ['--protocol', 'swift',
                '--username', 'id',
                '--password', 'key',
                '--read-only',
                '--endpoint', 'https://some.swift.com/auth/v1.0',
                '--bucket', 'some-bucket']

        body = self.TrackingStringIO('response')
        mock_provider.list_objects.return_value = ProviderResponse(
            True, 200, {}, [{'name': 'foo'}])
        mock_provider.get_object.return_value = ProviderResponse(
            True, 200, {}, body)
        mock_provider.head_object.return_value = ProviderResponse(
            True, 204, {}, '')
        exit_arg = main(args)

        self.assertEqual(0, exit_arg)
        mock_provider.list_objects.assert_called_once_with(
            None, 1, None, bucket=None)
        mock_provider.head_object.assert_called_once_with(
            'foo', bucket=None)
        mock_provider.get_object.assert_called_once_with(
            'foo', bucket=None, resp_chunk_size=1)
        self.assertEqual(0, body.last_pos)
        self.assertTrue(body.closed)

    @mock.patch('s3_sync.verify.create_provider')
    def test_read_only_all_buckets(self, mock_provider_factory):
        args = [
            '--protocol', 'swift',
            '--username', 'id',
            '--password', 'key',
            '--read-only',
            '--endpoint', 'https://saio:8080/auth/v1.0',
            '--bucket', '/*']
        body = self.TrackingStringIO('response')
        mock_provider = mock_provider_factory.return_value
        mock_provider.list_buckets.return_value = ProviderResponse(
            True, 200, {}, [{'name': 'container'}])
        mock_provider.list_objects.return_value = ProviderResponse(
            True, 200, {}, [{'name': 'foo'}])
        mock_provider.get_object.return_value = ProviderResponse(
            True, 200, {}, body)
        mock_provider.head_object.return_value = ProviderResponse(
            True, 204, {}, '')
        exit_arg = main(args)

        self.assertEqual(0, exit_arg)
        mock_provider.list_buckets.assert_called_once_with(limit=1)
        mock_provider.list_objects.assert_called_once_with(
            None, 1, None, bucket='container')
        mock_provider.head_object.assert_called_once_with(
            'foo', bucket='container')
        mock_provider.get_object.assert_called_once_with(
            'foo', bucket='container')
        self.assertEqual(0, body.last_pos)
        self.assertTrue(body.closed)

    @mock.patch('s3_sync.verify.create_provider')
    def test_read_only_bucket_prefix(self, mock_provider_factory):
        args = ['--protocol', 's3',
                '--username', 'id',
                '--password', 'key',
                '--read-only',
                '--endpoint', 'https://s3.amazonaws.com',
                '--bucket', 'some-bucket',
                '--prefix', 'some/nested/path']
        mock_provider = mock_provider_factory.return_value
        body = self.TrackingStringIO('response')

        mock_provider.list_objects.return_value = ProviderResponse(
            True, 200, {}, [{'name': 'some/nested/path/foo'}])
        mock_provider.get_object.return_value = ProviderResponse(
            True, 200, {}, body)
        mock_provider.head_object.return_value = ProviderResponse(
            True, 204, {}, '')
        exit_arg = main(args)

        self.assertEqual(0, exit_arg)
        mock_provider.list_objects.assert_called_once_with(
            None, 1, 'some/nested/path', bucket=None)
        mock_provider.head_object.assert_called_once_with(
            'some/nested/path/foo', bucket=None)
        mock_provider.get_object.assert_called_once_with(
            'some/nested/path/foo', bucket=None)
        self.assertEqual(0, body.last_pos)
        self.assertTrue(body.closed)

    @mock.patch('s3_sync.verify.create_provider')
    def test_read_only_no_objects(self, mock_provider_factory):
        mock_provider = mock_provider_factory.return_value
        args = ['--protocol', 's3',
                '--username', 'id',
                '--password', 'key',
                '--read-only',
                '--endpoint', 'https://s3.amazonaws.com',
                '--bucket', 'some-bucket']
        mock_provider = mock_provider_factory.return_value
        mock_provider.list_objects.return_value = ProviderResponse(
            True, 200, {}, [])
        self.assertEqual(
            'There are no objects in the bucket to validate GET/HEAD access',
            main(args))
        mock_provider.list_objects.assert_called_once_with(
            None, 1, None, bucket=None)

    @mock.patch('s3_sync.verify.create_provider')
    def test_read_only_no_buckets(self, mock_provider_factory):
        mock_provider = mock_provider_factory.return_value
        args = ['--protocol', 's3',
                '--username', 'id',
                '--password', 'key',
                '--read-only',
                '--endpoint', 'https://s3.amazonaws.com',
                '--bucket', '/*']
        mock_provider = mock_provider_factory.return_value
        mock_provider.list_buckets.return_value = ProviderResponse(
            True, 200, {}, [])
        self.assertEqual('No buckets/containers found', main(args))
        mock_provider.list_buckets.assert_called_once_with(limit=1)

    @mock.patch('s3_sync.verify.create_provider')
    def test_list_buckets_error(self, mock_provider_factory):
        mock_provider = mock_provider_factory.return_value
        args = ['--protocol', 's3',
                '--username', 'id',
                '--password', 'key',
                '--read-only',
                '--endpoint', 'https://s3.amazonaws.com',
                '--bucket', '/*']
        mock_provider = mock_provider_factory.return_value
        mock_provider.list_buckets.return_value = ProviderResponse(
            False, 401, {}, [])
        self.assertTrue(main(args).endswith(
            mock_provider.list_buckets.return_value.wsgi_status))
        mock_provider.list_buckets.assert_called_once_with(limit=1)

    @mock.patch('s3_sync.verify.create_provider')
    def test_list_objects_error(self, mock_provider_factory):
        mock_provider = mock_provider_factory.return_value
        args = ['--protocol', 's3',
                '--username', 'id',
                '--password', 'key',
                '--read-only',
                '--endpoint', 'https://s3.amazonaws.com',
                '--bucket', 'bucket']
        mock_provider = mock_provider_factory.return_value
        mock_provider.list_objects.return_value = ProviderResponse(
            False, 401, {}, [])
        self.assertTrue(main(args).endswith(
            mock_provider.list_objects.return_value.wsgi_status))
        mock_provider.list_objects.assert_called_once_with(
            None, 1, None, bucket=None)

    @mock.patch('s3_sync.verify.create_provider')
    def test_head_object_error(self, mock_provider_factory):
        mock_provider = mock_provider_factory.return_value
        args = ['--protocol', 's3',
                '--username', 'id',
                '--password', 'key',
                '--read-only',
                '--endpoint', 'https://s3.amazonaws.com',
                '--bucket', 'some-bucket']

        mock_provider.list_objects.return_value = ProviderResponse(
            True, 200, {}, [{'name': 'foo'}])
        mock_provider.head_object.return_value = ProviderResponse(
            False, 500, {}, '')
        exit_arg = main(args)

        self.assertTrue(exit_arg.endswith(
            mock_provider.head_object.return_value.wsgi_status))
        mock_provider.list_objects.assert_called_once_with(
            None, 1, None, bucket=None)
        mock_provider.head_object.assert_called_once_with(
            'foo', bucket=None)
        mock_provider.get_object.assert_not_called()

    @mock.patch('s3_sync.verify.create_provider')
    def test_get_object_error(self, mock_provider_factory):
        mock_provider = mock_provider_factory.return_value
        args = ['--protocol', 's3',
                '--username', 'id',
                '--password', 'key',
                '--read-only',
                '--endpoint', 'https://s3.amazonaws.com',
                '--bucket', 'some-bucket']

        body = self.TrackingStringIO('response')
        mock_provider.list_objects.return_value = ProviderResponse(
            True, 200, {}, [{'name': 'foo'}])
        mock_provider.get_object.return_value = ProviderResponse(
            False, 500, {}, body)
        mock_provider.head_object.return_value = ProviderResponse(
            True, 200, {}, '')
        exit_arg = main(args)

        self.assertTrue(exit_arg.endswith(
            mock_provider.get_object.return_value.wsgi_status))
        mock_provider.list_objects.assert_called_once_with(
            None, 1, None, bucket=None)
        mock_provider.head_object.assert_called_once_with(
            'foo', bucket=None)
        mock_provider.get_object.assert_called_once_with(
            'foo', bucket=None)
        self.assertEqual(0, body.last_pos)
        self.assertTrue(body.closed)

    @mock.patch('s3_sync.verify.create_provider')
    def test_no_bucket_bad_creds(self, mock_provider_factory):
        mock_provider = mock_provider_factory.return_value
        args = ['--protocol', 's3',
                '--username', 'id',
                '--password', 'key',
                '--endpoint', 'https://s3.amazonaws.com']

        mock_provider.list_buckets.return_value = ProviderResponse(
            False, 500, {}, 'error')
        exit_arg = main(args)

        self.assertTrue(exit_arg.endswith(
            mock_provider.list_buckets.return_value.wsgi_status))
