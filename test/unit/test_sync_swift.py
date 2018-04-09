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
from s3_sync.sync_swift import SyncSwift
from s3_sync import utils
import swiftclient
from swiftclient.exceptions import ClientException
from swift.common import swob
from swift.common.internal_client import UnexpectedResponse
import unittest
from utils import FakeStream


class FakeBody(object):
    def __init__(self, content, chunk=1 << 16):
        self.resp = mock.Mock()
        self.length = len(content)
        self.content = content
        self.offset = 0
        self.chunk = chunk

    def next(self):
        # Simulate swiftclient's _ObjectBody. Note that this requires that
        # we supply a resp_chunk_size argument to get_body.
        if self.offset == self.length:
            raise StopIteration
        self.offset += self.chunk
        return self.content[self.offset - self.chunk:self.offset]

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()


class TestSyncSwift(unittest.TestCase):
    def setUp(self):
        self.aws_bucket = 'bucket'
        self.scratch_space = 'scratch'
        self.max_conns = 10
        self.sync_swift = SyncSwift(
            {'aws_bucket': self.aws_bucket,
             'aws_identity': 'identity',
             'aws_secret': 'credential',
             'account': 'account',
             'container': 'container',
             'aws_endpoint': 'http://swift.url/auth/v1.0'},
            max_conns=self.max_conns)

    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    @mock.patch('s3_sync.sync_swift.check_slo')
    @mock.patch('s3_sync.sync_swift.FileWrapper')
    def test_upload_new_object(
            self, mock_file_wrapper, mock_check_slo, mock_swift):
        key = 'key'
        storage_policy = 42
        swift_req_headers = {'X-Backend-Storage-Policy-Index': storage_policy,
                             'X-Newest': True}
        swift_client = mock.Mock()
        mock_swift.return_value = swift_client

        wrapper = mock.Mock()
        wrapper.__len__ = lambda s: 0
        wrapper.get_headers.return_value = {
            'etag': 'deadbeef',
            'Content-Type': 'application/testing'}
        mock_file_wrapper.return_value = wrapper
        not_found = swiftclient.exceptions.ClientException('not found',
                                                           http_status=404)
        swift_client.head_object.side_effect = not_found

        mock_check_slo.return_value = False
        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.return_value = {}

        self.sync_swift.upload_object(key, storage_policy, mock_ic)
        mock_file_wrapper.assert_called_with(mock_ic,
                                             self.sync_swift.account,
                                             self.sync_swift.container,
                                             key, swift_req_headers)

        swift_client.put_object.assert_called_with(
            self.aws_bucket, key, wrapper,
            headers={'Content-Type': 'application/testing'},
            etag='deadbeef',
            content_length=0)

    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    @mock.patch('s3_sync.sync_swift.check_slo')
    @mock.patch('s3_sync.sync_swift.FileWrapper')
    def test_upload_unicode_object(
            self, mock_file_wrapper, mock_check_slo, mock_swift):
        key = 'monkey-\xf0\x9f\x90\xb5'
        storage_policy = 42
        swift_req_headers = {'X-Backend-Storage-Policy-Index': storage_policy,
                             'X-Newest': True}
        swift_client = mock.Mock()
        mock_swift.return_value = swift_client

        wrapper = mock.Mock()
        wrapper.__len__ = lambda s: 0
        wrapper.get_headers.return_value = {'etag': 'deadbeef'}
        mock_file_wrapper.return_value = wrapper
        not_found = swiftclient.exceptions.ClientException('not found',
                                                           http_status=404)
        swift_client.head_object.side_effect = not_found

        mock_check_slo.return_value = False
        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.return_value = {}

        self.sync_swift.upload_object(key, storage_policy, mock_ic)
        mock_file_wrapper.assert_called_with(mock_ic,
                                             self.sync_swift.account,
                                             self.sync_swift.container,
                                             key, swift_req_headers)

        swift_client.put_object.assert_called_with(
            self.aws_bucket, key, wrapper, headers={},
            etag='deadbeef', content_length=0)

    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    def test_upload_changed_meta(self, mock_swift):
        key = 'key'
        storage_policy = 42
        etag = '1234'
        swift_object_meta = {'x-object-meta-new': 'new',
                             'x-object-meta-old': 'updated',
                             'Content-Type': 'application/bar',
                             'etag': etag}
        swift_client = mock.Mock()
        mock_swift.return_value = swift_client

        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.return_value = swift_object_meta
        swift_client.head_object.return_value = {
            'x-object-meta-old': 'old',
            'etag': '%s' % etag,
            'Content-Type': 'application/foo'}

        self.sync_swift.upload_object(key, storage_policy, mock_ic)

        swift_client.post_object.assert_called_with(
            self.aws_bucket, key,
            {'x-object-meta-new': 'new',
             'x-object-meta-old': 'updated',
             'Content-Type': 'application/bar'})

    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    def test_meta_unicode(self, mock_swift):
        key = 'key'
        storage_policy = 42
        etag = '1234'
        swift_object_meta = {'x-object-meta-new': '\xf0\x9f\x91\x8d',
                             'x-object-meta-old': 'updated',
                             'etag': etag}
        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.return_value = swift_object_meta
        swift_client = mock.Mock()
        mock_swift.return_value = swift_client
        swift_client.head_object.return_value = {
            'x-object-meta-old': 'old', 'etag': '%s' % etag}

        self.sync_swift.upload_object(key, storage_policy, mock_ic)

        swift_client.post_object.assert_called_with(
            self.aws_bucket,
            key,
            {'x-object-meta-new': '\xf0\x9f\x91\x8d',
             'x-object-meta-old': 'updated'})

    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    @mock.patch('s3_sync.sync_swift.FileWrapper')
    def test_upload_replace_object(self, mock_file_wrapper, mock_swift):
        key = 'key'
        storage_policy = 42
        swift_object_meta = {'x-object-meta-new': 'new',
                             'x-object-meta-old': 'updated',
                             'etag': '2'}
        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.return_value = swift_object_meta
        swift_client = mock.Mock()
        mock_swift.return_value = swift_client
        swift_client.head_object.return_value = {
            'x-object-meta-old': 'old', 'etag': '1'}

        wrapper = mock.Mock()
        wrapper.get_headers.return_value = swift_object_meta
        wrapper.__len__ = lambda s: 42
        mock_file_wrapper.return_value = wrapper

        self.sync_swift.upload_object(key, storage_policy, mock_ic)

        swift_client.put_object.assert_called_with(
            self.aws_bucket,
            key,
            wrapper,
            headers={'x-object-meta-new': 'new',
                     'x-object-meta-old': 'updated'},
            etag='2',
            content_length=42)

    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    def test_upload_same_object(self, mock_swift):
        key = 'key'
        storage_policy = 42
        etag = '1234'
        swift_object_meta = {'x-object-meta-foo': 'foo',
                             'etag': etag}
        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.return_value = swift_object_meta
        swift_client = mock.Mock()
        mock_swift.return_value = swift_client
        swift_client.head_object.return_value = {
            'x-object-meta-foo': 'foo', 'etag': '%s' % etag}

        self.sync_swift.upload_object(key, storage_policy, mock_ic)

        swift_client.post_object.assert_not_called()
        swift_client.put_object.assert_not_called()

    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    def test_upload_slo(self, mock_swift):
        slo_key = 'slo-object'
        storage_policy = 42
        swift_req_headers = {'X-Backend-Storage-Policy-Index': storage_policy,
                             'X-Newest': True}
        manifest = [{'name': '/segment_container/slo-object/part1',
                     'hash': 'deadbeef',
                     'bytes': 1024},
                    {'name': '/segment_container/slo-object/part2',
                     'hash': 'beefdead',
                     'bytes': 1024}]

        not_found = swiftclient.exceptions.ClientException('not found',
                                                           http_status=404)
        swift_client = mock.Mock()
        mock_swift.return_value = swift_client
        swift_client.head_object.side_effect = not_found
        swift_client.get_object.side_effect = not_found

        def get_metadata(account, container, key, headers):
            if key == slo_key:
                return {utils.SLO_HEADER: 'True',
                        'Content-Type': 'application/slo'}
            raise RuntimeError('Unknown key')

        def get_object(account, container, key, headers):
            if key == slo_key:
                return (200, {utils.SLO_HEADER: 'True',
                              'Content-Type': 'application/slo'},
                        FakeStream(content=json.dumps(manifest)))
            if container == 'segment_container':
                if key == 'slo-object/part1':
                    return (200, {'Content-Length': 1024}, FakeStream(1024))
                elif key == 'slo-object/part2':
                    return (200, {'Content-Length': 1024}, FakeStream(1024))
            raise RuntimeError('Unknown key!')

        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.side_effect = get_metadata
        mock_ic.get_object.side_effect = get_object

        self.sync_swift.upload_object(slo_key, storage_policy, mock_ic)

        swift_client.head_object.assert_called_once_with(
            self.aws_bucket, slo_key)
        segment_container = self.aws_bucket + '_segments'
        swift_client.put_object.assert_has_calls([
            mock.call(segment_container,
                      'slo-object/part1', mock.ANY, etag='deadbeef',
                      content_length=1024),
            mock.call(self.aws_bucket + '_segments',
                      'slo-object/part2', mock.ANY, etag='beefdead',
                      content_length=1024),
            mock.call(self.aws_bucket, slo_key,
                      mock.ANY,
                      headers={'Content-Type': 'application/slo'},
                      query_string='multipart-manifest=put')
        ])

        expected_manifest = [
            {'path': '/%s/%s' % (segment_container,
                                 'slo-object/part1'),
             'size_bytes': 1024,
             'etag': 'deadbeef'},
            {'path': '/%s/%s' % (segment_container,
                                 'slo-object/part2'),
             'size_bytes': 1024,
             'etag': 'beefdead'}]

        called_manifest = json.loads(
            swift_client.put_object.mock_calls[-1][1][2])
        self.assertEqual(len(expected_manifest), len(called_manifest))
        for index, segment in enumerate(expected_manifest):
            called_segment = called_manifest[index]
            self.assertEqual(set(segment.keys()), set(called_segment.keys()))
            for k in segment.keys():
                self.assertEqual(segment[k], called_segment[k])

        mock_ic.get_object_metadata.assert_called_once_with(
            'account', 'container', slo_key, headers=swift_req_headers)
        mock_ic.get_object.assert_has_calls([
            mock.call('account', 'container', slo_key,
                      headers=swift_req_headers),
            mock.call('account', 'segment_container', 'slo-object/part1',
                      headers=swift_req_headers),
            mock.call('account', 'segment_container', 'slo-object/part2',
                      headers=swift_req_headers)])

    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    def test_slo_metadata_update(self, mock_swift):
        key = 'key'
        storage_policy = 42
        etag = '1234'
        swift_object_meta = {'x-object-meta-new': 'new',
                             'x-object-meta-old': 'updated',
                             'x-static-large-object': 'True',
                             'etag': etag}
        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.return_value = swift_object_meta
        swift_client = mock.Mock()
        mock_swift.return_value = swift_client
        swift_client.head_object.return_value = {
            'x-object-meta-old': 'old',
            'x-static-large-object': 'True',
            'etag': '%s' % etag}
        swift_client.get_object.return_value = ({
            'etag': etag
        }, '')

        self.sync_swift.upload_object(key, storage_policy, mock_ic)

        swift_client.post_object.assert_called_with(
            self.aws_bucket, key,
            {'x-object-meta-new': 'new',
             'x-object-meta-old': 'updated'})

    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    def test_slo_no_changes(self, mock_swift):
        key = 'key'
        storage_policy = 42
        etag = '1234'
        meta = {'x-object-meta-new': 'new',
                'x-object-meta-old': 'updated',
                'x-static-large-object': 'True',
                'etag': etag}
        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.return_value = meta
        swift_client = mock.Mock()
        mock_swift.return_value = swift_client
        swift_client.head_object.return_value = meta
        swift_client.get_object.return_value = (meta, '')

        self.sync_swift.upload_object(key, storage_policy, mock_ic)

        swift_client.post_object.assert_not_called()

    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    def test_delete_object(self, mock_swift):
        key = 'key'

        swift_client = mock.Mock()
        mock_swift.return_value = swift_client
        # When deleting in Swift, we have to do a HEAD in case it's an SLO
        swift_client.head_object.return_value = {}
        self.sync_swift.delete_object(key)
        swift_client.delete_object.assert_called_with(
            self.aws_bucket, key)

    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    def test_delete_non_existent_object(self, mock_swift):
        swift_client = mock.Mock()
        mock_swift.return_value = swift_client

        key = 'key'
        not_found = swiftclient.exceptions.ClientException(
            'not found', http_status=404)
        swift_client.head_object.side_effect = not_found
        self.sync_swift.delete_object(key)
        swift_client.delete_object.assert_not_called()

    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    def test_delete_slo(self, mock_swift):
        slo_key = 'slo-object'
        manifest = [{'name': '/segment_container/slo-object/part1',
                     'hash': 'deadbeef'},
                    {'name': '/segment_container/slo-object/part2',
                     'hash': 'beefdead'}]

        swift_client = mock.Mock()
        mock_swift.return_value = swift_client

        swift_client.head_object.return_value = {
            'x-static-large-object': 'True',
            'etag': 'deadbeef'
        }
        swift_client.get_object.return_value = (
            {}, json.dumps(manifest))

        self.sync_swift.delete_object(slo_key)

        swift_client.delete_object.assert_called_once_with(
            self.aws_bucket, slo_key, query_string='multipart-manifest=delete')

        swift_client.head_object.assert_called_once_with(
            self.aws_bucket, slo_key)

    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    def test_shunt_object(self, mock_swift):
        key = 'key'

        common_headers = {
            'content-type': 'application/unknown',
            'date': 'Thu, 15 Jun 2017 00:09:25 GMT',
            'last-modified': 'Wed, 14 Jun 2017 23:11:34 GMT',
            'x-trans-id': 'some trans id',
            'x-openstack-request-id': 'also some trans id',
            'x-object-meta-mtime': '1497315527.000000'}

        tests = [
            dict(content='some fairly large content' * (1 << 16),
                 method='GET',
                 headers={'etag': 'e06dd4228b3a7ab66aae5fbc9e4b905e'},
                 conns_start=self.max_conns - 1,
                 calls=[mock.call(
                        self.aws_bucket, key,
                        headers={'X-Trans-Id-Extra': 'local transaction id'},
                        resp_chunk_size=1 << 16)]),
            dict(content='',
                 method='GET',
                 headers={'etag': 'd41d8cd98f00b204e9800998ecf8427e'},
                 conns_start=self.max_conns - 1,
                 calls=[mock.call(
                        self.aws_bucket, key,
                        headers={'X-Trans-Id-Extra': 'local transaction id'},
                        resp_chunk_size=1 << 16)]),
            dict(method='HEAD',
                 headers={'etag': 'e06dd4228b3a7ab66aae5fbc9e4b905e'},
                 conns_start=self.max_conns,
                 calls=[mock.call(
                        self.aws_bucket, key,
                        headers={'X-Trans-Id-Extra': 'local transaction id'})])
        ]

        swift_client = mock.Mock()
        mock_swift.return_value = swift_client

        for test in tests:
            content = test.get('content', '')
            body = FakeBody(content)
            headers = dict(common_headers)
            headers['content-length'] = str(len(content))
            headers.update(test['headers'])

            swift_client.reset_mock()
            mocked = getattr(
                swift_client, '_'.join([test['method'].lower(), 'object']))
            if test['method'] == 'GET':
                mocked.return_value = (headers, body)
            else:
                mocked.return_value = headers

            expected_headers = {}
            for k, v in common_headers.items():
                if k == 'x-trans-id' or k == 'x-openstack-request-id':
                    expected_headers['Remote-' + k] = v
                else:
                    expected_headers[k] = v
            expected_headers['Content-Length'] = str(len(content))
            expected_headers['etag'] = test['headers']['etag']

            req = swob.Request.blank(
                '/v1/AUTH_a/c/key', method=test['method'],
                environ={'swift.trans_id': 'local transaction id'})
            status, headers, body_iter = self.sync_swift.shunt_object(req, key)
            self.assertEqual(
                test['conns_start'], self.sync_swift.client_pool.free_count())
            self.assertEqual(status.split()[0], str(200))
            self.assertEqual(sorted(headers), sorted(expected_headers.items()))
            self.assertEqual(b''.join(body_iter), content)
            self.assertEquals(mocked.mock_calls, test['calls'])
            self.assertEqual(
                self.max_conns, self.sync_swift.client_pool.free_count())

    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    def test_shunt_range_request(self, mock_swift):
        key = 'key'
        body = 'some fairly large content' * (1 << 16)
        headers = {
            'content-length': str(len(body)),
            'content-range': 'bytes 10-20/1000'}

        resp_body = FakeBody(body)

        swift_client = mock.Mock()
        mock_swift.return_value = swift_client

        swift_client.get_object.return_value = (headers, resp_body)
        swift_client.head_object.return_value = headers

        expected_headers = [
            # Content-Length must be properly capitalized,
            # or eventlet will try to be "helpful"
            ('Content-Length', str(len(body))),
            ('content-range', 'bytes 10-20/1000'),
        ]

        req = swob.Request.blank('/v1/AUTH_a/c/key', method='GET', environ={
            'swift.trans_id': 'local transaction id',
        }, headers={'Range': 'bytes=10-20'})
        status, headers, body_iter = self.sync_swift.shunt_object(req, key)
        self.assertEqual(status.split()[0], str(206))
        self.assertEqual(sorted(headers), expected_headers)
        self.assertEqual(b''.join(body_iter), body)
        self.assertEqual(swift_client.get_object.mock_calls, [
            mock.call(self.aws_bucket, key, headers={
                'X-Trans-Id-Extra': 'local transaction id',
                'Range': 'bytes=10-20',
            }, resp_chunk_size=1 << 16)])

        req.method = 'HEAD'
        status, headers, body_iter = self.sync_swift.shunt_object(req, key)
        # This test doesn't exactly match Swift's behavior: on HEAD with Range
        # Swift will respond 200, but with no Content-Range
        self.assertEqual(status.split()[0], str(206))
        self.assertEqual(sorted(headers), expected_headers)
        self.assertEqual(b''.join(body_iter), '')
        self.assertEqual(swift_client.head_object.mock_calls, [
            mock.call(self.aws_bucket, key, headers={
                'X-Trans-Id-Extra': 'local transaction id',
                'Range': 'bytes=10-20',
            })])

    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    def test_shunt_object_network_error(self, mock_swift):
        key = 'key'
        req = swob.Request.blank('/v1/AUTH_a/c/key', method='GET', environ={
            'swift.trans_id': 'local transaction id',
        })
        swift_client = mock.Mock()
        mock_swift.return_value = swift_client

        tests = [
            {'method': 'GET',
             'exception': Exception,
             'status': 502,
             'headers': [],
             'message': 'Bad Gateway',
             'mock_call': mock.call(
                 self.aws_bucket, key,
                 headers={'X-Trans-Id-Extra': 'local transaction id'},
                 resp_chunk_size=1 << 16),
             'conns_start': self.max_conns - 1,
             'conns_end': self.max_conns},
            {'method': 'GET',
             'exception': ClientException(
                 msg='failure occurred', http_status=500,
                 http_response_content='failure occurred',
                 http_response_headers={}),
             'status': 500,
             'headers': [],
             'message': 'failure occurred',
             'mock_call': mock.call(
                 self.aws_bucket, key,
                 headers={'X-Trans-Id-Extra': 'local transaction id'},
                 resp_chunk_size=1 << 16),
             'conns_start': self.max_conns - 1,
             'conns_end': self.max_conns},
            {'method': 'HEAD',
             'exception': Exception,
             'status': 502,
             'headers': [],
             'message': '',
             'mock_call': mock.call(
                 self.aws_bucket, key,
                 headers={'X-Trans-Id-Extra': 'local transaction id'}),
             'conns_start': self.max_conns,
             'conns_end': self.max_conns}]

        for test in tests:
            swift_client.reset_mock()
            client_method = '_'.join([test['method'].lower(), 'object'])
            mocked_method = getattr(swift_client, client_method)
            mocked_method.side_effect = test['exception']
            req.method = test['method']

            status, headers, body_iter = self.sync_swift.shunt_object(req, key)
            self.assertEqual(
                test['conns_start'], self.sync_swift.client_pool.free_count())
            self.assertEqual(status.split()[0], str(test['status']))
            self.assertEqual(headers, test['headers'])
            self.assertEqual(b''.join(body_iter), test['message'])
            mocked_method.assert_has_calls([test['mock_call']])
            self.assertEqual(
                test['conns_end'], self.sync_swift.client_pool.free_count())

    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    def test_per_account_bucket(self, mock_swift):
        mock_swift.return_value = mock.Mock()

        # in this case, the "bucket" is actually the prefix
        aws_bucket = 'sync_'
        sync_swift = SyncSwift(
            {'aws_bucket': aws_bucket,
             'aws_identity': 'identity',
             'aws_secret': 'credential',
             'account': 'account',
             'container': 'container',
             'aws_endpoint': 'http://swift.url/auth/v1.0'},
            per_account=True)

        self.assertEqual('sync_container', sync_swift.remote_container)

    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    def test_per_account_container_create(self, mock_swift):
        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.side_effect = UnexpectedResponse(
            '404 Not Found', None)
        swift_client = mock.Mock()
        mock_swift.return_value = swift_client
        swift_client.head_container.side_effect = ClientException(
            'not found', http_status=404, http_reason='Not Found')
        self.sync_swift._per_account = True
        self.assertFalse(self.sync_swift.verified_container)
        self.sync_swift.upload_object('foo', 'policy', mock_ic)
        swift_client.put_container.assert_called_once_with(
            'bucketcontainer')
        self.assertTrue(self.sync_swift.verified_container)

        swift_client.reset_mock()
        self.sync_swift.upload_object('foo', 'policy', mock_ic)
        self.assertEqual([mock.call.head_object('bucketcontainer', 'foo')],
                         swift_client.mock_calls)
