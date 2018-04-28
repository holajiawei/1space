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

from io import BytesIO
import mock
import unittest

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

    def test_missing_args(self, mock_validate):
        def do_test(args, missing_arg):
            with self.assertRaises(SystemExit), \
                    mock.patch('sys.stderr', new_callable=BytesIO) as err:
                main(args)
            self.assertIn('argument %s is required' % missing_arg,
                          err.getvalue())

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
            k: v for k, v in provider.settings.items()
            if k.startswith('aws_') or k in ('protocol',)
        }, {
            'protocol': 's3',
            'aws_endpoint': None,
            'aws_identity': 'access id',
            'aws_secret': 'secret key',
            'aws_bucket': 'some-bucket',
        })
        self.assertEqual(swift_key, 'fabcab/cloud_sync_test')
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
        exit_arg = main([
            '--protocol', 's3',
            '--endpoint', 'https://s3.amazonaws.com',
            '--username', 'access id',
            '--password', 'secret key',
        ])
        self.assertEqual(exit_arg, 0)
        mock_client = \
            mock_get_client.return_value.__enter__.return_value
        self.assertEqual(mock_client.mock_calls, [
            mock.call.list_buckets(),
        ])

    def test_aws_with_bucket(self, mock_get_client):
        mock_client = \
            mock_get_client.return_value.__enter__.return_value
        mock_client.head_object.side_effect = [
            None,
            {  # HEAD after PUT
                'Body': [''],
                'ResponseMetadata': {
                    'HTTPStatusCode': 200,
                    'HTTPHeaders': {
                        'x-amz-meta-cloud-sync': 'fabcab',
                    },
                },
            },
        ]
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
            '--endpoint', 'https://s3.amazonaws.com',
            '--username', 'access id',
            '--password', 'secret key',
            '--bucket', 'some-bucket',
        ])
        self.assertEqual(exit_arg, 0)
        key = u'9f9835/verify-auth/testing-\U0001f44d/fabcab/cloud_sync_test'
        self.assert_calls(mock_client, [
            mock.call.head_object(
                Bucket='some-bucket',
                Key=key),
            mock.call.put_object(
                Body=mock.ANY,
                Bucket='some-bucket',
                ContentLength=15,
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

    def test_google_no_bucket(self, mock_get_client):
        exit_arg = main([
            '--protocol', 's3',
            '--endpoint', 'https://storage.googleapis.com',
            '--username', 'access id',
            '--password', 'secret key',
        ])
        self.assertEqual(exit_arg, 0)
        mock_client = \
            mock_get_client.return_value.__enter__.return_value
        self.assertEqual(mock_client.mock_calls, [
            mock.call.list_buckets(),
        ])

    def test_google_with_bucket(self, mock_get_client):
        mock_client = \
            mock_get_client.return_value.__enter__.return_value
        mock_client.head_object.side_effect = [
            None,
            {  # HEAD after PUT
                'Body': [''],
                'ResponseMetadata': {
                    'HTTPStatusCode': 200,
                    'HTTPHeaders': {
                        'x-amz-meta-cloud-sync': 'fabcab',
                    },
                },
            },
        ]
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
            mock.call.head_object(
                Bucket='some-bucket',
                Key=key),
            mock.call.put_object(
                Body=mock.ANY,
                Bucket='some-bucket',
                ContentLength=15,
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
        exit_arg = main([
            '--protocol', 'swift',
            '--endpoint', 'https://saio:8080/auth/v1.0',
            '--username', 'access id',
            '--password', 'secret key',
        ])
        self.assertEqual(exit_arg, 0)
        mock_client = \
            mock_get_client.return_value.__enter__.return_value
        self.assertEqual(mock_client.mock_calls, [
            mock.call.get_account(),
        ])

    def test_swift_with_bucket(self, mock_get_client):
        mock_client = \
            mock_get_client.return_value.__enter__.return_value
        mock_client.head_object.side_effect = [
            None,
            {'x-object-meta-cloud-sync': 'fabcab'},
            {'x-object-meta-cloud-sync': 'fabcab'},  # One extra for the DELETE
        ]
        mock_client.get_container.return_value = ({}, [])
        exit_arg = main([
            '--protocol', 'swift',
            '--endpoint', 'https://saio:8080/auth/v1.0',
            '--username', 'access id',
            '--password', 'secret key',
            '--bucket', 'some-bucket',
        ])
        self.assertEqual(exit_arg, 0)
        self.assertEqual(mock_client.mock_calls, [
            mock.call.head_object('some-bucket', 'cloud_sync_test_object',
                                  headers={}),
            mock.call.put_object(
                'some-bucket', 'cloud_sync_test_object', mock.ANY,
                content_length=15, etag=mock.ANY,
                headers={'content-type': 'text/plain'}),
            mock.call.post_object(
                'some-bucket', 'cloud_sync_test_object',
                {'content-type': 'text/plain',
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
