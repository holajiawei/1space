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

from container_crawler.exceptions import RetryError
import hashlib
import json
import mock
from s3_sync.sync_swift import SyncSwift
from s3_sync import utils
import StringIO
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
    not_found = swiftclient.exceptions.ClientException(
        'not found', http_status=404)

    def setUp(self):
        self.aws_bucket = 'container'
        self.scratch_space = 'scratch'
        self.max_conns = 10
        self.mapping = {
            'aws_bucket': self.aws_bucket,
            'aws_identity': 'identity',
            'aws_secret': 'credential',
            'account': 'account',
            'container': 'container',
            'aws_endpoint': 'http://swift.url/auth/v1.0',
        }
        self.sync_swift = SyncSwift(self.mapping, max_conns=self.max_conns)
        self.logger = mock.Mock()
        self.sync_swift.logger = self.logger

    def tearDown(self):
        checked_levels = ['error', 'exception']
        for level in checked_levels:
            for call in getattr(self.logger, level).mock_calls:
                print call
            getattr(self.logger, level).assert_not_called()

    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    def test_put_object(self, mock_swift):
        key = 'key'
        swift_client = mock_swift.return_value

        response_dict_holder = []

        def _stub_put_object(*args, **kwargs):
            if 'response_dict' in kwargs:
                response_dict_holder.append(kwargs['response_dict'])
                kwargs['response_dict']['status'] = 200
                kwargs['response_dict']['reason'] = 'OK'
                kwargs['response_dict']['headers'] = {
                    # swiftclent's `parse_headers()` gets used and that can
                    # totally return unicode stuff
                    u'\u062afun': u'times\u062a'
                }
            return 'an_etag'
        swift_client.put_object.side_effect = _stub_put_object

        body_iter = ['a', 'bb', 'c']
        resp = self.sync_swift.put_object(key, {
            'x-object-meta-jammy': 'hammy',
        }, body_iter, query_string=u'q1=v%201&q2=v2')
        self.assertTrue(resp.success)
        self.assertEqual({u'\u062afun': u'times\u062a'}, resp.headers)

        self.assertEqual([
            mock.call(
                authurl='http://swift.url/auth/v1.0', key='credential',
                os_options={}, retries=3, user='identity'),
            mock.call().put_object(self.mapping['container'], key,
                                   contents=body_iter,
                                   headers={'x-object-meta-jammy': 'hammy'},
                                   query_string='q1=v%201&q2=v2',
                                   response_dict=response_dict_holder[0]),
        ], mock_swift.mock_calls)

    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    def test_put_object_extra_headers(self, mock_swift):
        self.sync_swift = SyncSwift(self.mapping, max_conns=self.max_conns,
                                    extra_headers={'a': 'b', 'c': 'd'})

        key = 'key'
        swift_client = mock_swift.return_value

        response_dict_holder = []

        def _stub_put_object(*args, **kwargs):
            if 'response_dict' in kwargs:
                response_dict_holder.append(kwargs['response_dict'])
                kwargs['response_dict']['status'] = 200
                kwargs['response_dict']['reason'] = 'OK'
                kwargs['response_dict']['headers'] = {
                    # swiftclent's `parse_headers()` gets used and that can
                    # totally return unicode stuff
                    u'\u062afun': u'times\u062a'
                }
            return 'an_etag'
        swift_client.put_object.side_effect = _stub_put_object

        body_iter = ['a', 'bb', 'c']
        resp = self.sync_swift.put_object(key, {
            'x-object-meta-jammy': 'hammy',
        }, body_iter, query_string=u'q1=v%201&q2=v2')
        self.assertTrue(resp.success)
        self.assertEqual({u'\u062afun': u'times\u062a'}, resp.headers)

        self.assertEqual([
            mock.call(
                authurl='http://swift.url/auth/v1.0', key='credential',
                os_options={}, retries=3, user='identity'),
            mock.call().put_object(self.mapping['container'], key,
                                   contents=body_iter,
                                   headers={'x-object-meta-jammy': 'hammy',
                                            'a': 'b', 'c': 'd'},
                                   query_string='q1=v%201&q2=v2',
                                   response_dict=response_dict_holder[0]),
        ], mock_swift.mock_calls)

    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    @mock.patch('s3_sync.sync_swift.check_slo')
    @mock.patch('s3_sync.sync_swift.FileWrapper')
    def test_upload_new_object(
            self, mock_file_wrapper, mock_check_slo, mock_swift):
        key = 'key'
        storage_policy = 42
        swift_req_headers = {'X-Backend-Storage-Policy-Index': storage_policy}
        swift_client = mock.Mock()
        mock_swift.return_value = swift_client

        wrapper = mock.Mock()
        wrapper.__len__ = lambda s: 0
        wrapper.get_headers.return_value = {
            'etag': 'deadbeef',
            'Content-Type': 'application/testing'}
        mock_file_wrapper.return_value = wrapper
        swift_client.head_object.side_effect = self.not_found

        mock_check_slo.return_value = False
        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.return_value = {
            'x-timestamp': str(1e9)}

        self.sync_swift.upload_object(
            {'name': key,
             'storage_policy_index': storage_policy,
             'created_at': str(1e9)}, mock_ic)
        mock_file_wrapper.assert_called_with(mock_ic,
                                             self.sync_swift.account,
                                             self.sync_swift.container,
                                             key, swift_req_headers,
                                             stats_cb=None)

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
        swift_req_headers = {'X-Backend-Storage-Policy-Index': storage_policy}
        swift_client = mock.Mock()
        mock_swift.return_value = swift_client

        wrapper = mock.Mock()
        wrapper.__len__ = lambda s: 0
        wrapper.get_headers.return_value = {'etag': 'deadbeef'}
        mock_file_wrapper.return_value = wrapper
        swift_client.head_object.side_effect = self.not_found

        mock_check_slo.return_value = False
        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.return_value = {
            'x-timestamp': str(1e9)}

        self.sync_swift.upload_object(
            {'name': key,
             'storage_policy_index': storage_policy,
             'created_at': str(1e9)}, mock_ic)
        mock_file_wrapper.assert_called_with(mock_ic,
                                             self.sync_swift.account,
                                             self.sync_swift.container,
                                             key, swift_req_headers,
                                             stats_cb=None)

        swift_client.put_object.assert_called_with(
            self.aws_bucket, key, wrapper, headers={},
            etag='deadbeef', content_length=0)

    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    @mock.patch('s3_sync.sync_swift.check_slo')
    def test_upload_object_failure(self, mock_check_slo, mock_swift):
        key = 'monkey-\xf0\x9f\x90\xb5'
        storage_policy = 42
        body = FakeStream(1024)
        swift_client = mock.Mock()
        mock_swift.return_value = swift_client

        swift_client.head_object.side_effect = self.not_found
        swift_client.put_object.side_effect = RuntimeError('Failed to PUT')

        mock_check_slo.return_value = False
        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.return_value = {
            'content-type': 'application/test',
            'x-timestamp': str(1e9)}
        mock_ic.get_object.return_value = (
            200, {'Content-Length': len(body),
                  'etag': 'deadbeef'}, body)

        with self.assertRaises(RuntimeError):
            self.sync_swift.upload_object(
                {'name': key,
                 'storage_policy_index': storage_policy,
                 'created_at': str(1e9)}, mock_ic)

        swift_client.put_object.assert_called_with(
            self.aws_bucket, key, mock.ANY, headers={},
            etag='deadbeef', content_length=len(body))
        self.assertTrue(body.closed)

    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    def test_upload_changed_meta(self, mock_swift):
        key = 'key'
        storage_policy = 42
        etag = '1234'
        swift_object_meta = {'x-object-meta-new': 'new',
                             'x-object-meta-old': 'updated',
                             'Content-Type': 'application/bar',
                             'etag': etag,
                             'x-timestamp': str(1e9)}
        swift_client = mock.Mock()
        mock_swift.return_value = swift_client

        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.return_value = swift_object_meta
        swift_client.head_object.return_value = {
            'x-object-meta-old': 'old',
            'etag': '%s' % etag,
            'Content-Type': 'application/foo'}
        swift_client.post_object.return_value = None
        swift_client.delete_object.return_value = None

        self.sync_swift.upload_object(
            {'name': key,
             'storage_policy_index': storage_policy,
             'created_at': str(1e9)}, mock_ic)

        swift_client.post_object.assert_called_with(
            self.aws_bucket, key, headers={
                'x-object-meta-new': 'new',
                'x-object-meta-old': 'updated',
                'Content-Type': 'application/bar'})

    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    def test_meta_unicode(self, mock_swift):
        key = 'key'
        storage_policy = 42
        etag = '1234'
        swift_object_meta = {'x-object-meta-new': '\xf0\x9f\x91\x8d',
                             'x-object-meta-old': 'updated',
                             'x-delete-at': '12345',
                             'etag': etag,
                             'x-timestamp': str(1e9)}
        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.return_value = swift_object_meta
        swift_client = mock.Mock()
        mock_swift.return_value = swift_client
        swift_client.head_object.return_value = {
            'x-object-meta-old': 'old', 'etag': '%s' % etag}
        swift_client.post_object.return_value = None
        swift_client.delete_object.return_value = None

        self.sync_swift.upload_object(
            {'name': key,
             'storage_policy_index': storage_policy,
             'created_at': str(1e9)}, mock_ic)

        swift_client.post_object.assert_called_with(
            self.aws_bucket, key, headers={
                'x-object-meta-new': '\xf0\x9f\x91\x8d',
                'x-object-meta-old': 'updated'})

    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    def test_propagate_expiration(self, mock_swift):
        self.mapping['propagate_expiration'] = True
        self.sync_swift = SyncSwift(self.mapping, max_conns=self.max_conns)
        key = 'key'
        storage_policy = 42
        etag = '1234'
        swift_object_meta = {'x-object-meta-foo': 'bla',
                             'x-delete-at': '12345',
                             'content-type': 'text/plain',
                             'etag': etag,
                             'x-timestamp': str(1e9)}
        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.return_value = swift_object_meta
        swift_client = mock.Mock()
        mock_swift.return_value = swift_client
        swift_client.head_object.return_value = {
            'x-object-meta-old': 'old', 'etag': '%s' % etag}
        swift_client.post_object.return_value = None
        swift_client.delete_object.return_value = None

        self.sync_swift.upload_object(
            {'name': key,
             'storage_policy_index': storage_policy,
             'created_at': str(1e9)}, mock_ic)

        swift_client.post_object.assert_called_with(
            self.aws_bucket, key, headers={
                'x-object-meta-foo': 'bla',
                'content-type': 'text/plain',
                'x-delete-at': '12345'})

    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    @mock.patch('s3_sync.sync_swift.FileWrapper')
    def test_upload_replace_object(self, mock_file_wrapper, mock_swift):
        key = 'key'
        storage_policy = 42
        swift_object_meta = {'x-object-meta-new': 'new',
                             'x-object-meta-old': 'updated',
                             'etag': '2',
                             'x-timestamp': str(1e9)}
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

        self.sync_swift.upload_object(
            {'name': key,
             'storage_policy_index': storage_policy,
             'created_at': str(1e9)}, mock_ic)

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
                             'etag': etag,
                             'x-timestamp': str(1e9)}
        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.return_value = swift_object_meta
        swift_client = mock.Mock()
        mock_swift.return_value = swift_client
        swift_client.head_object.return_value = {
            'x-object-meta-foo': 'foo', 'etag': '%s' % etag}

        self.sync_swift.upload_object(
            {'name': key,
             'storage_policy_index': storage_policy,
             'created_at': str(1e9)}, mock_ic)

        swift_client.post_object.assert_not_called()
        swift_client.put_object.assert_not_called()

    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    def test_upload_slo(self, mock_swift):
        slo_key = 'slo-object'
        storage_policy = 42
        swift_req_headers = {'X-Backend-Storage-Policy-Index': storage_policy}
        manifest = [{'name': '/segment_container/slo-object/part1',
                     'hash': 'deadbeef',
                     'bytes': 1024},
                    {'name': '/segment_container/slo-object/part2',
                     'hash': 'beefdead',
                     'bytes': 1024}]

        swift_client = mock.Mock()
        mock_swift.return_value = swift_client
        swift_client.head_object.side_effect = self.not_found
        swift_client.get_object.side_effect = self.not_found

        def get_metadata(account, container, key, headers):
            if key == slo_key:
                return {utils.SLO_HEADER: 'True',
                        'Content-Type': 'application/slo',
                        'x-timestamp': str(1e9)}
            if key == 'slo-object/part1' or key == 'slo-object/part2':
                return {}
            raise RuntimeError('Unknown key: %s' % key)

        def get_object(account, container, key, headers):
            if key == slo_key:
                return (200, {utils.SLO_HEADER: 'True',
                              'Content-Type': 'application/slo',
                              'x-timestamp': str(1e9)},
                        FakeStream(content=json.dumps(manifest)))
            if container == 'segment_container':
                if key == 'slo-object/part1':
                    return (200, {'Content-Length': 1024, 'etag': 'deadbeef'},
                            FakeStream(1024))
                elif key == 'slo-object/part2':
                    return (200, {'Content-Length': 1024, 'etag': 'beefdead'},
                            FakeStream(1024))
            raise RuntimeError('Unknown key: %s' % key)

        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.side_effect = get_metadata
        mock_ic.get_object.side_effect = get_object

        self.sync_swift.upload_object(
            {'name': slo_key,
             'storage_policy_index': storage_policy,
             'created_at': str(1e9)}, mock_ic)

        swift_client.head_object.assert_has_calls([
            mock.call(self.aws_bucket, slo_key, headers={}),
            mock.call(self.aws_bucket + '_segments', 'slo-object/part1',
                      headers={}),
            mock.call(self.aws_bucket + '_segments', 'slo-object/part2',
                      headers={})])
        segment_container = self.aws_bucket + '_segments'
        swift_client.put_object.assert_has_calls([
            mock.call(segment_container,
                      'slo-object/part1', mock.ANY, etag='deadbeef',
                      content_length=1024, headers={}),
            mock.call(self.aws_bucket + '_segments',
                      'slo-object/part2', mock.ANY, etag='beefdead',
                      content_length=1024, headers={}),
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

        mock_ic.get_object_metadata.assert_has_calls(
            [mock.call('account', 'container', slo_key,
                       headers=swift_req_headers),
             mock.call('account', 'segment_container', 'slo-object/part1',
                       headers={}),
             mock.call('account', 'segment_container', 'slo-object/part2',
                       headers={})])
        mock_ic.get_object.assert_has_calls([
            mock.call('account', 'container', slo_key,
                      headers=swift_req_headers),
            mock.call('account', 'segment_container', 'slo-object/part1',
                      headers={}),
            mock.call('account', 'segment_container', 'slo-object/part2',
                      headers={})])

    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    def test_upload_slo_extra_headers(self, mock_swift):
        self.sync_swift = SyncSwift(self.mapping, max_conns=self.max_conns,
                                    extra_headers={'a': 'b', 'c': 'd'})

        slo_key = 'slo-object'
        storage_policy = 42
        swift_req_headers = {'X-Backend-Storage-Policy-Index': storage_policy}
        manifest = [{'name': '/segment_container/slo-object/part1',
                     'hash': 'deadbeef',
                     'bytes': 1024},
                    {'name': '/segment_container/slo-object/part2',
                     'hash': 'beefdead',
                     'bytes': 1024}]

        swift_client = mock.Mock()
        mock_swift.return_value = swift_client
        swift_client.head_object.side_effect = self.not_found
        swift_client.get_object.side_effect = self.not_found

        def get_metadata(account, container, key, headers):
            if key == slo_key:
                return {utils.SLO_HEADER: 'True',
                        'Content-Type': 'application/slo',
                        'x-timestamp': str(1e9)}
            if key.startswith('slo-object/'):
                return {}
            raise RuntimeError('Unknown key')

        def get_object(account, container, key, headers):
            if key == slo_key:
                return (200, {utils.SLO_HEADER: 'True',
                              'Content-Type': 'application/slo',
                              'x-timestamp': str(1e9)},
                        FakeStream(content=json.dumps(manifest)))
            if container == 'segment_container':
                if key == 'slo-object/part1':
                    return (200, {'Content-Length': 1024, 'etag': 'deadbeef'},
                            FakeStream(1024))
                elif key == 'slo-object/part2':
                    return (200, {'Content-Length': 1024, 'etag': 'beefdead'},
                            FakeStream(1024))
            raise RuntimeError('Unknown key!')

        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.side_effect = get_metadata
        mock_ic.get_object.side_effect = get_object

        self.sync_swift.upload_object(
            {'name': slo_key,
             'storage_policy_index': storage_policy,
             'created_at': str(1e9)}, mock_ic)

        swift_client.head_object.assert_has_calls(
            [mock.call(self.aws_bucket, slo_key,
                       headers={'a': 'b', 'c': 'd'}),
             mock.call(self.aws_bucket + '_segments', 'slo-object/part1',
                       headers={'a': 'b', 'c': 'd'}),
             mock.call(self.aws_bucket + '_segments', 'slo-object/part2',
                       headers={'a': 'b', 'c': 'd'})])

        segment_container = self.aws_bucket + '_segments'
        swift_client.put_object.assert_has_calls([
            mock.call(segment_container,
                      'slo-object/part1', mock.ANY, etag='deadbeef',
                      content_length=1024, headers={'a': 'b', 'c': 'd'}),
            mock.call(self.aws_bucket + '_segments',
                      'slo-object/part2', mock.ANY, etag='beefdead',
                      content_length=1024, headers={'a': 'b', 'c': 'd'}),
            mock.call(self.aws_bucket, slo_key,
                      mock.ANY,
                      headers={'Content-Type': 'application/slo',
                               'a': 'b', 'c': 'd'},
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

        mock_ic.get_object_metadata.assert_has_calls(
            [mock.call('account', 'container', slo_key,
                       headers=swift_req_headers),
             mock.call('account', 'segment_container', 'slo-object/part1',
                       headers={}),
             mock.call('account', 'segment_container', 'slo-object/part2',
                       headers={})])

        mock_ic.get_object.assert_has_calls([
            mock.call('account', 'container', slo_key,
                      headers=swift_req_headers),
            mock.call('account', 'segment_container', 'slo-object/part1',
                      headers={}),
            mock.call('account', 'segment_container', 'slo-object/part2',
                      headers={})])

    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    def test_slo_upload_per_account(self, mock_swift):
        slo_key = 'slo-object'
        storage_policy = 42
        segment_container = 'segment_container'
        swift_req_headers = {'X-Backend-Storage-Policy-Index': storage_policy}
        manifest = [{'name': '/%s/slo-object/part1' % segment_container,
                     'hash': 'deadbeef',
                     'bytes': 1024},
                    {'name': '/%s/slo-object/part2' % segment_container,
                     'hash': 'beefdead',
                     'bytes': 1024}]

        swift_client = mock.Mock()
        mock_swift.return_value = swift_client
        swift_client.head_object.side_effect = self.not_found
        swift_client.get_object.side_effect = self.not_found

        def get_metadata(account, container, key, headers):
            if key == slo_key:
                return {utils.SLO_HEADER: 'True',
                        'Content-Type': 'application/slo',
                        'x-timestamp': str(1e9)}
            if key.startswith('slo-object/part'):
                return {}
            raise RuntimeError('Unknown key')

        def get_object(account, container, key, headers):
            if key == slo_key:
                return (200, {utils.SLO_HEADER: 'True',
                              'Content-Type': 'application/slo',
                              'x-timestamp': str(1e9)},
                        FakeStream(content=json.dumps(manifest)))
            if container == 'segment_container':
                if key == 'slo-object/part1':
                    return (200, {'Content-Length': 1024, 'etag': 'deadbeef'},
                            FakeStream(1024))
                elif key == 'slo-object/part2':
                    return (200, {'Content-Length': 1024, 'etag': 'beefdead'},
                            FakeStream(1024))
            raise RuntimeError('Unknown key!')

        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.side_effect = get_metadata
        mock_ic.get_object.side_effect = get_object

        mapping = dict(self.mapping)
        mapping['aws_bucket'] = 'prefix-'
        sync_swift = SyncSwift(
            mapping, max_conns=self.max_conns,
            per_account=True)
        sync_swift.upload_object(
            {'name': slo_key,
             'storage_policy_index': storage_policy,
             'created_at': str(1e9)}, mock_ic)

        swift_client.head_object.assert_has_calls(
            [mock.call(mapping['aws_bucket'] + mapping['container'], slo_key,
                       headers={}),
             mock.call(mapping['aws_bucket'] + 'segment_container',
                       'slo-object/part1', headers={}),
             mock.call(mapping['aws_bucket'] + 'segment_container',
                       'slo-object/part2', headers={})])

        swift_client.put_object.assert_has_calls([
            mock.call(mapping['aws_bucket'] + segment_container,
                      'slo-object/part1', mock.ANY, etag='deadbeef',
                      content_length=1024, headers={}),
            mock.call(mapping['aws_bucket'] + segment_container,
                      'slo-object/part2', mock.ANY, etag='beefdead',
                      content_length=1024, headers={}),
            mock.call(mapping['aws_bucket'] + mapping['container'], slo_key,
                      mock.ANY,
                      headers={'Content-Type': 'application/slo'},
                      query_string='multipart-manifest=put')
        ])

        expected_manifest = [
            {'path': '/%s/%s' % (mapping['aws_bucket'] + segment_container,
                                 'slo-object/part1'),
             'size_bytes': 1024,
             'etag': 'deadbeef'},
            {'path': '/%s/%s' % (mapping['aws_bucket'] + segment_container,
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

        mock_ic.get_object_metadata.assert_has_calls(
            [mock.call('account', 'container', slo_key,
                       headers=swift_req_headers),
             mock.call('account', 'segment_container', 'slo-object/part1',
                       headers={}),
             mock.call('account', 'segment_container', 'slo-object/part2',
                       headers={})])
        mock_ic.get_object.assert_has_calls([
            mock.call('account', 'container', slo_key,
                      headers=swift_req_headers),
            mock.call('account', segment_container, 'slo-object/part1',
                      headers={}),
            mock.call('account', segment_container, 'slo-object/part2',
                      headers={})])

    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    def test_slo_metadata_update(self, mock_swift):
        key = 'key'
        storage_policy = 42
        slo_etag = hashlib.md5('deadbeef').hexdigest()
        swift_object_meta = {'x-object-meta-new': 'new',
                             'x-object-meta-old': 'updated',
                             'x-static-large-object': 'True',
                             'etag': 'foobar',
                             'x-timestamp': str(1e9)}

        manifest = [{'path': '/segments/part1', 'hash': 'deadbeef'}]

        def _get_object(account, container, key, **kwargs):
            return 200, {}, StringIO.StringIO(json.dumps(manifest))

        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.return_value = swift_object_meta
        mock_ic.get_object.side_effect = _get_object

        swift_client = mock.Mock()
        mock_swift.return_value = swift_client
        swift_client.head_object.return_value = {
            'x-object-meta-old': 'old',
            'x-static-large-object': 'True',
            'etag': '"%s"' % slo_etag}
        swift_client.post_object.return_value = None

        self.sync_swift.upload_object(
            {'name': key,
             'storage_policy_index': storage_policy,
             'created_at': str(1e9)}, mock_ic)

        mock_ic.get_object.assert_called_once_with(
            self.sync_swift.account, self.sync_swift.container, key,
            headers=mock.ANY)
        swift_client.post_object.assert_called_once_with(
            self.aws_bucket, key, headers={
                'x-object-meta-new': 'new',
                'x-object-meta-old': 'updated'})

    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    def test_slo_no_changes(self, mock_swift):
        key = 'key'
        storage_policy = 42
        slo_etag = hashlib.md5('deadbeef').hexdigest()
        meta = {'x-object-meta-new': 'new',
                'x-object-meta-old': 'updated',
                'x-static-large-object': 'True',
                'etag': 'foobar',
                'x-timestamp': str(1e9)}

        remote_meta = {
            'x-object-meta-new': 'new',
            'x-object-meta-old': 'updated',
            'x-static-large-object': 'True',
            'etag': '"%s"' % slo_etag}

        manifest = [{'path': '/segments/part1', 'hash': 'deadbeef'}]

        def _get_object(account, container, key, **kwargs):
            return 200, {}, StringIO.StringIO(json.dumps(manifest))

        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.return_value = meta
        mock_ic.get_object.side_effect = _get_object

        swift_client = mock.Mock()
        mock_swift.return_value = swift_client
        swift_client.head_object.return_value = remote_meta

        self.sync_swift.upload_object(
            {'name': key,
             'storage_policy_index': storage_policy,
             'created_at': str(1e9)}, mock_ic)

        mock_ic.get_object.assert_called_once_with(
            self.sync_swift.account, self.sync_swift.container, key,
            headers=mock.ANY)
        swift_client.post_object.assert_not_called()

    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    def test_slo_no_changes_different_container(self, mock_swift):
        self.sync_swift.aws_bucket = 'other-container'

        key = 'key'
        storage_policy = 42
        slo_etag = hashlib.md5('deadbeef').hexdigest()
        meta = {'x-object-meta-new': 'new',
                'x-object-meta-old': 'updated',
                'x-static-large-object': 'True',
                'etag': 'foobar',
                'x-timestamp': str(1e9)}

        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.return_value = meta

        manifest = [
            {'name': '/%s_segments/part1' % (self.sync_swift.container),
             'hash': 'deadbeef'}]

        remote_meta = dict(meta.items())
        remote_meta['etag'] = '"%s"' % slo_etag

        swift_client = mock.Mock()
        mock_swift.return_value = swift_client
        swift_client.head_object.return_value = remote_meta

        def _get_object(account, container, key, **kwargs):
            if container != 'container' or key != 'key':
                raise NotImplementedError

            body = json.dumps(manifest)
            headers = {
                'etag': hashlib.md5(body).hexdigest()}
            return 200, headers, StringIO.StringIO(body)

        mock_ic.get_object.side_effect = _get_object

        self.sync_swift.upload_object(
            {'name': key,
             'storage_policy_index': storage_policy,
             'created_at': str(1e9)}, mock_ic)

        swift_client.post_object.assert_not_called()

    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    def test_slo_update_meta_different_container(self, mock_swift):
        self.sync_swift.aws_bucket = 'other-container'

        key = 'key'
        storage_policy = 42
        slo_etag = hashlib.md5('deadbeef').hexdigest()
        meta = {'x-object-meta-new': 'new',
                'x-static-large-object': 'True',
                'etag': 'foobar',
                'x-timestamp': str(1e9)}

        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.return_value = meta

        manifest = [
            {'name': '/%s_segments/part1' % (self.sync_swift.container),
             'hash': 'deadbeef'}]
        remote_meta = dict(meta.items())
        del remote_meta['x-object-meta-new']
        remote_meta['etag'] = '"%s"' % slo_etag

        swift_client = mock.Mock()
        mock_swift.return_value = swift_client
        swift_client.head_object.return_value = remote_meta
        swift_client.post_object.return_value = None

        def _get_object(account, container, key, **kwargs):
            if container != 'container' or key != 'key':
                raise NotImplementedError

            body = json.dumps(manifest)
            headers = {
                'etag': hashlib.md5(body).hexdigest()}
            return 200, headers, StringIO.StringIO(body)

        mock_ic.get_object.side_effect = _get_object

        self.sync_swift.upload_object(
            {'name': key,
             'storage_policy_index': storage_policy,
             'created_at': str(1e9)}, mock_ic)

        swift_client.post_object.assert_called_once_with(
            self.sync_swift.remote_container, key,
            headers={'x-object-meta-new': 'new'})

    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    def test_nested_slo_upload(self, mock_swift):
        slo_key = 'key'
        storage_policy = 42
        meta = {'x-static-large-object': 'True',
                'etag': 'foobar',
                'x-timestamp': str(1e9)}

        manifest = [
            {'name': '/%s_segments/part1' % (self.sync_swift.container),
             'hash': 'deadbeef',
             'bytes': 1024}]

        def fake_internal_head(_, __, key, headers={}):
            if key == slo_key:
                return meta
            if key == manifest[0]['name'].split('/', 2)[-1]:
                return {'x-static-large-object': 'True'}
            raise NotImplementedError

        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.side_effect = fake_internal_head

        mock_swift.return_value.head_object.side_effect = self.not_found

        mock_ic.get_object.return_value = (
            200,
            {'etag': hashlib.md5(json.dumps(manifest)).hexdigest()},
            StringIO.StringIO(json.dumps(manifest)))

        self.assertEqual(
            SyncSwift.UploadStatus.SKIPPED_NESTED_SLO,
            self.sync_swift.upload_object(
                {'name': slo_key,
                 'storage_policy_index': storage_policy,
                 'created_at': str(1e9)}, mock_ic))

    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    def test_upload_dlo(self, mock_swift):
        dlo_key = 'dlo'
        storage_policy = 42
        meta = {'x-object-manifest': 'segments_container/segments-',
                'etag': 'foobar',
                'x-timestamp': str(1e9),
                'Content-Length': 0}

        segments = [chr(ord('A') + i) * 1024 for i in range(1, 4)]

        def fake_internal_head(_, container, key, headers={}):
            if key == dlo_key and container == self.mapping['container']:
                return meta
            if container == 'segments_container' and\
                    key.startswith('segments-'):
                return {}
            raise NotImplementedError

        def fake_internal_get(_, container, key, headers={}):
            if container == 'segments_container' and\
                    key.startswith('segments-'):
                body = segments[int(key.split('-')[1]) - 1]
                etag = hashlib.md5(body).hexdigest()
                return (200, {'etag': etag, 'Content-Length': len(body)},
                        StringIO.StringIO(body))
            if container == 'container' and key == dlo_key:
                return 200, meta, ''
            raise NotImplementedError

        mock_ic = mock.NonCallableMock()
        mock_ic.get_object_metadata.side_effect = fake_internal_head
        mock_ic.make_request.side_effect = (
            mock.Mock(
                body=json.dumps([
                    {'name': 'segments-%d' % (i + 1),
                     'hash': hashlib.md5(segments[i]).hexdigest()}
                    for i in range(len(segments))]),
                status_int=200),
            mock.Mock(body='[]', status_int=200))
        mock_ic.get_object.side_effect = fake_internal_get
        mock_swift.return_value.head_object.side_effect = self.not_found

        self.assertEqual(
            SyncSwift.UploadStatus.PUT,
            self.sync_swift.upload_object(
                {'name': dlo_key,
                 'storage_policy_index': storage_policy,
                 'created_at': str(1e9)}, mock_ic))

        dlo_etag = hashlib.md5(''.join([hashlib.md5(segments[i]).hexdigest()
                                        for i in range(len(segments))]))\
            .hexdigest()
        self.assertEqual(
            [mock.call('%s_segments' % self.aws_bucket, 'segments-%d' % i,
                       mock.ANY, content_length=len(segments[i - 1]),
                       etag=hashlib.md5(segments[i - 1]).hexdigest(),
                       headers={})
             for i in range(1, 4)] +
            [mock.call(
                self.aws_bucket, dlo_key, mock.ANY, content_length=0,
                etag='foobar',
                headers={
                    'x-object-manifest': '%s_segments/segments-' %
                    self.aws_bucket,
                    'x-object-meta-swift-source-dlo-etag': dlo_etag
                },
                query_string=None)],
            mock_swift.return_value.put_object.mock_calls)

    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    def test_update_dlo_metadata(self, mock_swift):
        dlo_key = 'dlo'
        storage_policy = 42
        meta = {'x-object-manifest': 'segments_container/segments-',
                'etag': 'foobar',
                'x-timestamp': str(1e9),
                'Content-Length': 0,
                'x-object-meta-new-key': 'value'}

        segments = [chr(ord('A') + i) * 1024 for i in range(1, 4)]
        dlo_etag = hashlib.md5(''.join([hashlib.md5(segments[i]).hexdigest()
                                        for i in range(len(segments))]))\
            .hexdigest()

        def fake_internal_head(_, container, key, headers={}):
            if key == dlo_key and container == self.mapping['container']:
                return meta
            raise NotImplementedError

        def fake_swift_head(container, key, **kwargs):
            if container == self.aws_bucket and key == dlo_key:
                return {
                    'etag': 'foobar',
                    'x-object-meta-swift-source-dlo-etag': dlo_etag,
                    'x-object-manifest': '%s_segments/segments-' %
                    self.aws_bucket}
            raise NotImplementedError

        mock_ic = mock.NonCallableMock()
        mock_ic.get_object_metadata.side_effect = fake_internal_head
        mock_ic.make_request.side_effect = (
            mock.Mock(
                body=json.dumps([
                    {'name': 'segments-%d' % (i + 1),
                     'hash': hashlib.md5(segments[i]).hexdigest()}
                    for i in range(len(segments))]),
                status_int=200),
            mock.Mock(body='[]', status_int=200))
        mock_swift.return_value.head_object.side_effect = fake_swift_head

        self.assertEqual(
            SyncSwift.UploadStatus.POST,
            self.sync_swift.upload_object(
                {'name': dlo_key,
                 'storage_policy_index': storage_policy,
                 'created_at': str(1e9)}, mock_ic))

        mock_swift.return_value.post_object.assert_called_once_with(
            self.aws_bucket, dlo_key,
            headers={
                'x-object-manifest': '%s_segments/segments-' % self.aws_bucket,
                'x-object-meta-swift-source-dlo-etag': dlo_etag,
                'x-object-meta-new-key': 'value'})

    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    def test_upload_same_dlo(self, mock_swift):
        dlo_key = 'dlo'
        storage_policy = 42
        meta = {'x-object-manifest': 'segments_container/segments-',
                'etag': 'foobar',
                'x-timestamp': str(1e9),
                'Content-Length': 0}

        segments = [chr(ord('A') + i) * 1024 for i in range(1, 4)]
        dlo_etag = hashlib.md5(''.join([hashlib.md5(segments[i]).hexdigest()
                                        for i in range(len(segments))]))\
            .hexdigest()

        def fake_internal_head(_, container, key, headers={}):
            if key == dlo_key and container == self.mapping['container']:
                return meta
            raise NotImplementedError

        def fake_swift_head(container, key, **kwargs):
            if container == self.aws_bucket and key == dlo_key:
                return {
                    'etag': 'foobar',
                    'x-object-meta-swift-source-dlo-etag': dlo_etag,
                    'x-object-manifest': '%s_segments/segments-' %
                    self.aws_bucket}
            raise NotImplementedError

        mock_ic = mock.NonCallableMock()
        mock_ic.get_object_metadata.side_effect = fake_internal_head
        mock_ic.make_request.side_effect = (
            mock.Mock(
                body=json.dumps([
                    {'name': 'segments-%d' % (i + 1),
                     'hash': hashlib.md5(segments[i]).hexdigest()}
                    for i in range(len(segments))]),
                status_int=200),
            mock.Mock(body='[]', status_int=200))
        mock_swift.return_value.head_object.side_effect = fake_swift_head

        self.assertEqual(
            SyncSwift.UploadStatus.NOOP,
            self.sync_swift.upload_object(
                {'name': dlo_key,
                 'storage_policy_index': storage_policy,
                 'created_at': str(1e9)}, mock_ic))

        self.assertEqual(
            [mock.call.head_object(self.mapping['container'], dlo_key,
                                   headers={})],
            mock_swift.return_value.mock_calls)

    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    def test_changed_dlo(self, mock_swift):
        dlo_key = 'dlo'
        storage_policy = 42
        meta = {'x-object-manifest': 'segments_container/segments-',
                'etag': 'foobar',
                'x-timestamp': str(1e9),
                'Content-Length': 0}

        segments = [chr(ord('A') + i) * 1024 for i in range(1, 4)]
        dlo_etag = hashlib.md5(''.join([hashlib.md5(segments[i]).hexdigest()
                                        for i in range(len(segments))]))\
            .hexdigest()
        local_segments = [segment for segment in segments]
        local_segments.append('X' * 1024)
        new_dlo_etag = hashlib.md5(''.join(
            [hashlib.md5(local_segments[i]).hexdigest()
             for i in range(len(local_segments))])).hexdigest()

        def fake_internal_head(_, container, key, headers={}):
            if key == dlo_key and container == self.mapping['container']:
                return meta
            if container == 'segments_container':
                index = int(key.split('-')[1]) - 1
                body = local_segments[index]
                return {'etag': hashlib.md5(body).hexdigest(),
                        'Content-Length': len(body)}
            raise NotImplementedError

        def fake_internal_get(_, container, key, headers={}):
            if container == 'segments_container' and\
                    key == 'segments-%d' % len(local_segments):
                body = local_segments[int(key.split('-')[1]) - 1]
                etag = hashlib.md5(body).hexdigest()
                return (200, {'etag': etag, 'Content-Length': len(body)},
                        StringIO.StringIO(body))
            if key == dlo_key:
                return (200, meta, '')
            raise NotImplementedError

        def fake_swift_head(container, key, **kwargs):
            if container == self.aws_bucket and key == dlo_key:
                return {
                    'etag': 'foobar',
                    'x-object-meta-swift-source-dlo-etag': dlo_etag,
                    'x-object-manifest': '%s_segments/segments-' %
                    self.aws_bucket,
                    'Content-Length': 0}
            if container == '%s_segments' % self.aws_bucket:
                index = int(key.split('-')[1]) - 1
                if index >= len(segments):
                    raise self.not_found
                body = segments[index]
                return {'etag': hashlib.md5(body).hexdigest(),
                        'Content-Length': len(body)}
            raise NotImplementedError

        mock_ic = mock.NonCallableMock()
        mock_ic.get_object_metadata.side_effect = fake_internal_head
        listing = json.dumps([
            {'name': 'segments-%d' % (i + 1),
             'hash': hashlib.md5(local_segments[i]).hexdigest()}
            for i in range(len(local_segments))])
        mock_ic.make_request.side_effect = (
            mock.Mock(body=listing, status_int=200),
            mock.Mock(body='[]', status_int=200),
            # we list twice -- to check the remote DLO ETag and then again to
            # upload
            mock.Mock(body=listing, status_int=200),
            mock.Mock(body='[]', status_int=200))
        mock_ic.get_object.side_effect = fake_internal_get
        mock_swift.return_value.head_object.side_effect = fake_swift_head

        self.assertEqual(
            SyncSwift.UploadStatus.PUT,
            self.sync_swift.upload_object(
                {'name': dlo_key,
                 'storage_policy_index': storage_policy,
                 'created_at': str(1e9)}, mock_ic))

        self.assertEqual([
            mock.call('%s_segments' % self.aws_bucket, 'segments-4', mock.ANY,
                      etag=hashlib.md5(local_segments[-1]).hexdigest(),
                      content_length=len(local_segments[-1]), headers={}),
            mock.call(
                self.aws_bucket, dlo_key, mock.ANY, etag='foobar',
                content_length=0,
                headers={'x-object-manifest': '%s_segments/segments-' %
                         self.aws_bucket,
                         'x-object-meta-swift-source-dlo-etag': new_dlo_etag},
                query_string=None)
        ], mock_swift.return_value.put_object.mock_calls)

    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    def test_upload_dlo_to_slo(self, mock_swift):
        dlo_key = 'dlo'
        storage_policy = 42
        meta = {'x-object-manifest': 'segments_container/segments-',
                'etag': 'foobar',
                'x-timestamp': str(1e9),
                'Content-Length': 0}
        mapping = dict(self.mapping)
        mapping['convert_dlo'] = True
        sync_swift = SyncSwift(mapping)

        segments = [chr(ord('A') + i) * 1024 for i in range(1, 4)]

        def fake_internal_head(_, container, key, headers={}):
            if key == dlo_key and container == self.mapping['container']:
                return meta
            if container == 'segments_container' and\
                    key.startswith('segments-'):
                return {}
            raise NotImplementedError

        def fake_internal_get(_, container, key, headers={}):
            if container == 'segments_container' and\
                    key.startswith('segments-'):
                body = segments[int(key.split('-')[1]) - 1]
                etag = hashlib.md5(body).hexdigest()
                return (200, {'etag': etag, 'Content-Length': len(body)},
                        StringIO.StringIO(body))
            if container == 'container' and key == dlo_key:
                return 200, meta, ''
            raise NotImplementedError

        mock_ic = mock.NonCallableMock()
        mock_ic.get_object_metadata.side_effect = fake_internal_head
        mock_ic.make_request.side_effect = (
            mock.Mock(
                body=json.dumps([
                    {'name': 'segments-%d' % (i + 1),
                     'hash': hashlib.md5(segments[i]).hexdigest(),
                     'bytes': len(segments[i])}
                    for i in range(len(segments))]),
                status_int=200),
            mock.Mock(body='[]', status_int=200))
        mock_ic.get_object.side_effect = fake_internal_get
        mock_swift.return_value.head_object.side_effect = self.not_found

        self.assertEqual(
            SyncSwift.UploadStatus.PUT,
            sync_swift.upload_object(
                {'name': dlo_key,
                 'storage_policy_index': storage_policy,
                 'created_at': str(1e9)}, mock_ic))

        manifest = [{'path': '/%s_segments/segments-%d' % (self.aws_bucket, i),
                     'etag': hashlib.md5(segments[i - 1]).hexdigest(),
                     'size_bytes': len(segments[i - 1])}
                    for i in range(1, len(segments) + 1)]
        dlo_etag = hashlib.md5(''.join([hashlib.md5(segments[i]).hexdigest()
                                        for i in range(len(segments))]))\
            .hexdigest()
        self.assertEqual(
            [mock.call('%s_segments' % self.aws_bucket, 'segments-%d' % i,
                       mock.ANY, content_length=len(segments[i - 1]),
                       etag=hashlib.md5(segments[i - 1]).hexdigest(),
                       headers={})
             for i in range(1, 4)] +
            [mock.call(
                self.aws_bucket, dlo_key, json.dumps(manifest),
                content_length=len(json.dumps(manifest)),
                etag=None,
                headers={
                    'x-object-meta-swift-source-dlo-etag': dlo_etag
                },
                query_string='multipart-manifest=put')],
            mock_swift.return_value.put_object.mock_calls)

    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    def test_upload_dlo_to_slo_metadata(self, mock_swift):
        dlo_key = 'dlo'
        storage_policy = 42
        meta = {'x-object-manifest': 'segments_container/segments-',
                'etag': 'foobar',
                'x-timestamp': str(1e9),
                'Content-Length': 0,
                'x-object-meta-new-key': 'value'}
        mapping = dict(self.mapping)
        mapping['convert_dlo'] = True
        sync_swift = SyncSwift(mapping)

        segments = [chr(ord('A') + i) * 1024 for i in range(1, 4)]
        dlo_etag = hashlib.md5(''.join([hashlib.md5(segments[i]).hexdigest()
                                        for i in range(len(segments))]))\
            .hexdigest()

        def fake_internal_head(_, container, key, headers={}):
            if key == dlo_key and container == self.mapping['container']:
                return meta
            raise NotImplementedError

        def fake_internal_get(_, container, key, headers={}):
            if container == 'container' and key == dlo_key:
                return 200, meta, ''
            raise NotImplementedError

        mock_ic = mock.NonCallableMock()
        mock_ic.get_object_metadata.side_effect = fake_internal_head
        mock_ic.make_request.side_effect = (
            mock.Mock(
                body=json.dumps([
                    {'name': 'segments-%d' % (i + 1),
                     'hash': hashlib.md5(segments[i]).hexdigest(),
                     'bytes': len(segments[i])}
                    for i in range(len(segments))]),
                status_int=200),
            mock.Mock(body='[]', status_int=200))
        mock_ic.get_object.side_effect = fake_internal_get

        def fake_swift_head(container, key, **kwargs):
            if container == self.aws_bucket and key == dlo_key:
                return {'etag': '"%s"' % dlo_etag,
                        'x-object-meta-swift-source-dlo-etag': dlo_etag,
                        'x-static-large-object': True}
            raise NotImplementedError
        mock_swift.return_value.head_object.side_effect = fake_swift_head
        mock_swift.return_value.post_object.return_value = None

        self.assertEqual(
            SyncSwift.UploadStatus.POST,
            sync_swift.upload_object(
                {'name': dlo_key,
                 'storage_policy_index': storage_policy,
                 'created_at': str(1e9)}, mock_ic))

        self.assertEqual(
            [mock.call(
                self.aws_bucket, dlo_key,
                headers={
                    'x-object-meta-swift-source-dlo-etag': dlo_etag,
                    'x-object-meta-new-key': 'value',
                })],
            mock_swift.return_value.post_object.mock_calls)

    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    def test_upload_changed_dlo_to_slo(self, mock_swift):
        '''Test an updated DLO already uploaded to SLO is propagated.'''
        dlo_key = 'dlo'
        storage_policy = 42
        meta = {'x-object-manifest': 'segments_container/segments-',
                'etag': 'foobar',
                'x-timestamp': str(1e9),
                'Content-Length': 0}
        mapping = dict(self.mapping)
        mapping['convert_dlo'] = True
        sync_swift = SyncSwift(mapping)

        # Prior uploaded segments
        old_segments = [chr(ord('A') + i) * 1024 for i in range(1, 4)]
        # New list of DLO segments
        new_segments = old_segments + [chr(ord('A') + 4) * 1024]
        dlo_etag = hashlib.md5(
            ''.join([hashlib.md5(old_segments[i]).hexdigest()
                     for i in range(len(old_segments))])).hexdigest()
        new_dlo_etag = hashlib.md5(
            ''.join([hashlib.md5(new_segments[i]).hexdigest()
                     for i in range(len(new_segments))])).hexdigest()

        # Use the new segments when doing internal HEAD requests
        def fake_internal_head(_, container, key, headers={}):
            if key == dlo_key and container == self.mapping['container']:
                return meta
            if container == 'segments_container' and\
                    key.startswith('segments-'):
                index = int(key.split('-')[1]) - 1
                return {'etag': hashlib.md5(new_segments[index]).hexdigest()}
            raise NotImplementedError

        # Use the new segments when doing internal GETs
        def fake_internal_get(_, container, key, headers={}):
            if container == 'segments_container' and key == 'segments-4':
                body = new_segments[-1]
                etag = hashlib.md5(body).hexdigest()
                return (200, {'etag': etag, 'Content-Length': len(body)},
                        StringIO.StringIO(body))
            raise NotImplementedError

        mock_ic = mock.NonCallableMock()
        mock_ic.get_object_metadata.side_effect = fake_internal_head
        mock_ic.make_request.side_effect = (
            # We iterate twice in this case: to compute the DLO ETag for
            # checking if it's uploaded, and then again to re-upload segments
            mock.Mock(
                body=json.dumps([
                    {'name': 'segments-%d' % (i + 1),
                     'hash': hashlib.md5(new_segments[i]).hexdigest(),
                     'bytes': len(new_segments[i])}
                    for i in range(len(new_segments))]),
                status_int=200),
            mock.Mock(body='[]', status_int=200)) * 2
        mock_ic.get_object.side_effect = fake_internal_get

        # Return the old upload metadata and segments
        def fake_swift_head(container, key, **kwargs):
            if container == '%s_segments' % self.aws_bucket:
                index = int(key.split('-')[1]) - 1
                if index >= len(old_segments):
                    raise self.not_found
                body = old_segments[index]
                return {'etag': hashlib.md5(body).hexdigest(),
                        'Content-Length': len(body)}
            if container == self.aws_bucket and key == dlo_key:
                return {
                    'etag': 'foobar',
                    'x-static-large-object': True,
                    'x-object-meta-swift-source-dlo-etag': dlo_etag,
                    'Content-Length': 1234
                }
            raise NotImplementedError

        mock_swift.return_value.head_object.side_effect = fake_swift_head

        self.assertEqual(
            SyncSwift.UploadStatus.PUT,
            sync_swift.upload_object(
                {'name': dlo_key,
                 'storage_policy_index': storage_policy,
                 'created_at': str(1e9)}, mock_ic))

        manifest = [{'path': '/%s_segments/segments-%d' % (self.aws_bucket, i),
                     'etag': hashlib.md5(new_segments[i - 1]).hexdigest(),
                     'size_bytes': len(new_segments[i - 1])}
                    for i in range(1, len(new_segments) + 1)]
        # Make sure only the new segment is uploaded and the manifest
        self.assertEqual(
            [mock.call('%s_segments' % self.aws_bucket, 'segments-4',
                       mock.ANY, content_length=len(new_segments[-1]),
                       etag=hashlib.md5(new_segments[-1]).hexdigest(),
                       headers={}),
             mock.call(
                self.aws_bucket, dlo_key, json.dumps(manifest),
                content_length=len(json.dumps(manifest)),
                etag=None,
                headers={
                    'x-object-meta-swift-source-dlo-etag': new_dlo_etag},
                query_string='multipart-manifest=put')],
            mock_swift.return_value.put_object.mock_calls)

    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    def test_upload_dlo_to_slo_same(self, mock_swift):
        dlo_key = 'dlo'
        storage_policy = 42
        meta = {'x-object-manifest': 'segments_container/segments-',
                'etag': 'foobar',
                'x-timestamp': str(1e9),
                'Content-Length': 0}
        mapping = dict(self.mapping)
        mapping['convert_dlo'] = True
        sync_swift = SyncSwift(mapping)

        segments = [chr(ord('A') + i) * 1024 for i in range(1, 4)]
        dlo_etag = hashlib.md5(''.join([hashlib.md5(segments[i]).hexdigest()
                                        for i in range(len(segments))]))\
            .hexdigest()

        def fake_internal_head(_, container, key, headers={}):
            if key == dlo_key and container == self.mapping['container']:
                return meta
            raise NotImplementedError

        mock_ic = mock.NonCallableMock()
        mock_ic.get_object_metadata.side_effect = fake_internal_head
        mock_ic.make_request.side_effect = (
            mock.Mock(
                body=json.dumps([
                    {'name': 'segments-%d' % (i + 1),
                     'hash': hashlib.md5(segments[i]).hexdigest(),
                     'bytes': len(segments[i])}
                    for i in range(len(segments))]),
                status_int=200),
            mock.Mock(body='[]', status_int=200))

        def fake_swift_head(container, key, **kwargs):
            if container == self.aws_bucket and key == dlo_key:
                return {
                    'etag': 'deadbeef',
                    'x-static-large-object': True,
                    'x-object-meta-swift-source-dlo-etag': dlo_etag,
                    'Content-Length': 1234
                }
            raise NotImplementedError

        mock_swift.return_value.head_object.side_effect = fake_swift_head

        self.assertEqual(
            SyncSwift.UploadStatus.NOOP,
            sync_swift.upload_object(
                {'name': dlo_key,
                 'storage_policy_index': storage_policy,
                 'created_at': str(1e9)}, mock_ic))
        mock_swift.return_value.put_object.assert_not_called()

    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    def test_delete_object(self, mock_swift):
        key = 'key'

        swift_client = mock.Mock()
        mock_swift.return_value = swift_client
        # When deleting in Swift, we have to do a HEAD in case it's an SLO
        swift_client.head_object.return_value = {}
        swift_client.delete_object.return_value = None

        self.sync_swift.delete_object(key)
        swift_client.delete_object.assert_called_with(
            self.aws_bucket, key, headers={})

    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    def test_delete_non_existent_object(self, mock_swift):
        swift_client = mock.Mock()
        mock_swift.return_value = swift_client

        key = 'key'
        swift_client.head_object.side_effect = self.not_found
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
        swift_client.delete_object.return_value = None

        self.sync_swift.delete_object(slo_key)

        swift_client.delete_object.assert_called_once_with(
            self.aws_bucket, slo_key, query_string='multipart-manifest=delete',
            headers={})

        swift_client.head_object.assert_called_once_with(
            self.aws_bucket, slo_key, headers={})

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
            if not isinstance(test['exception'], ClientException):
                self.logger.exception.assert_called_once_with(
                    'Error contacting remote swift cluster')
                self.logger.exception.reset_mock()

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
        self.sync_swift.upload_object(
            {'name': 'foo',
             'storage_policy_index': 'policy'}, mock_ic)
        swift_client.put_container.assert_called_once_with(
            self.aws_bucket + 'container', headers={})
        self.assertTrue(self.sync_swift.verified_container)

        swift_client.reset_mock()
        self.sync_swift.upload_object(
            {'name': 'foo',
             'storage_policy_index': 'policy'}, mock_ic)
        swift_client.head_object.assert_called_once_with(
            self.aws_bucket + self.mapping['aws_bucket'], 'foo', headers={})

    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    def test_list_buckets(self, mock_swift):
        containers = [
            {'name': 'a', 'last_modified': '2018-01-01T22:22:22.000000',
             'count': 1000, 'bytes': 2**20},
            {'name': 'b', 'last_modified': '2018-01-02T23:23:23.123456',
             'count': 10000, 'bytes': 3**20},
            {'name': u'\u062a', 'last_modified': '2000-10-10T01:01:01.867530',
             'count': 1, 'bytes': 1**20}]

        swift_client = mock.Mock()
        mock_swift.return_value = swift_client
        swift_client.get_account.return_value = (
            {'x-account-meta-header': 'value'}, containers)

        resp = self.sync_swift.list_buckets(None, 1000, None)

        self.assertEqual(200, resp.status)
        self.assertEqual({'x-account-meta-header': 'value'}, resp.headers)
        self.assertEqual(containers, resp.body)

    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    def test_list_buckets_no_last_modified(self, mock_swift):
        containers = [
            {'name': 'a', 'count': 1000, 'bytes': 2**20},
            {'name': 'b', 'count': 10000, 'bytes': 3**20},
            {'name': u'\u062a', 'count': 1, 'bytes': 1**20}]

        swift_client = mock.Mock()
        mock_swift.return_value = swift_client
        swift_client.get_account.return_value = (
            {'x-account-meta-header': 'value'}, containers)

        resp = self.sync_swift.list_buckets(None, 1000, None)

        self.assertEqual(200, resp.status)
        self.assertEqual({'x-account-meta-header': 'value'}, resp.headers)
        self.assertEqual(containers, resp.body)

    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    def test_keystone_v2_auth_missing_tenant_name(self, mock_swift):
        mock_swift.return_value = mock.Mock()
        # in this case, the "bucket" is actually the prefix
        aws_bucket = 'sync_'
        settings = {
            'aws_bucket': aws_bucket,
            'aws_identity': 'identity',
            'aws_secret': 'credential',
            'account': 'account',
            'container': 'container',
            'auth_type': 'keystone_v2',
            'aws_endpoint': 'http://swift.url/auth/v1.0'}
        with self.assertRaises(ValueError) as context:
            SyncSwift(settings)
        self.assertTrue(
            'tenant_name' in str(context.exception))

    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    def test_keystone_v2_auth(self, mock_swift):
        mock_swift.return_value = mock.Mock()
        sync_swift = SyncSwift(
            {'aws_bucket': 'container',
             'aws_identity': 'identity',
             'aws_secret': 'credential',
             'account': 'account',
             'container': 'container',
             'auth_type': 'keystone_v2',
             'tenant_name': 'tenantname',
             'aws_endpoint': 'http://swift.url/auth/v1.0'})
        sync_swift._get_client_factory()()
        mock_swift.assert_called_once_with(
            authurl='http://swift.url/auth/v1.0', user='identity',
            key='credential', auth_version='2',
            tenant_name='tenantname', retries=3, os_options={})

    def test_keystone_v3_auth_missing_arguments(self):
        common_args = {
            'aws_bucket': 'container',
            'aws_identity': 'identity',
            'aws_secret': 'credential',
            'account': 'account',
            'container': 'container',
            'auth_type': 'keystone_v3',
            'aws_endpoint': 'http://swift.url/auth/v1.0'}
        tests = [
            ({}, 'project_name, project_domain_name, user_domain_name'),
            ({'project_name': 'project'},
             'project_domain_name, user_domain_name'),
            ({'project_name': 'project',
              'project_domain_name': 'project domain'},
             'user_domain_name'),
            ({'user_domain_name': 'user domain'},
             'project_name, project_domain_name')]
        for args, error_content in tests:
            with mock.patch(
                    's3_sync.sync_swift.swiftclient.client.Connection'),\
                    self.assertRaises(ValueError) as context:
                SyncSwift(dict(common_args.items() + args.items()))
            self.assertTrue(
                error_content in context.exception.message)

    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    def test_keystone_v3_auth(self, mock_swift):
        mock_swift.return_value = mock.Mock()
        aws_bucket = 'sync_'
        settings = {
            'aws_bucket': aws_bucket,
            'aws_identity': 'identity',
            'aws_secret': 'credential',
            'account': 'account',
            'container': 'container',
            'auth_type': 'keystone_v3',
            'user_domain_name': 'userdomainname',
            'project_name': 'projectname',
            'project_domain_name': 'projectdomainname',
            'aws_endpoint': 'http://swift.url/auth/v1.0'}
        sync_swift = SyncSwift(settings)
        sync_swift._get_client_factory()()
        mock_swift.assert_called_once_with(
            authurl=settings['aws_endpoint'],
            user=settings['aws_identity'],
            key=settings['aws_secret'],
            auth_version='3',
            retries=3,
            os_options=dict(
                project_name=settings['project_name'],
                project_domain_name=settings['project_domain_name'],
                user_domain_name=settings['user_domain_name']))

    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    def test_remote_account(self, mock_swift):
        def mock_auth():
            mock_swift.return_value.os_options = dict()
            mock_swift.return_value.url = returned_storage_url

        returned_storage_url = 'http://foobar:1234/v1/AUTH_account'
        mock_swift.return_value = mock.Mock(
            get_auth=mock.Mock(side_effect=mock_auth))

        aws_bucket = 'sync_'
        settings = {
            'aws_bucket': aws_bucket,
            'aws_identity': 'identity',
            'aws_secret': 'credential',
            'account': 'account',
            'container': 'container',
            'aws_endpoint': 'http://swift.url/auth/v1.0',
            'remote_account': 'remote'}
        sync_swift = SyncSwift(settings)
        conn = sync_swift._get_client_factory()()
        mock_swift.assert_called_once_with(
            authurl=settings['aws_endpoint'],
            user=settings['aws_identity'],
            key=settings['aws_secret'],
            retries=3,
            os_options={})
        conn.get_auth.assert_called_once_with()
        self.assertEqual('http://foobar:1234/v1/remote',
                         conn.os_options['object_storage_url'])
        self.assertEqual('http://foobar:1234/v1/remote', conn.url)

    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    def test_retry_error_stale_object(self, mock_swift):
        swift_object_meta = {'x-timestamp': str(1e9)}
        mock_swift.return_value.head_object.return_value = {}
        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.return_value = swift_object_meta

        with self.assertRaises(RetryError):
            self.sync_swift.upload_object(
                {'name': 'key',
                 'storage_policy_index': 0,
                 'created_at': str(2e9)}, mock_ic)

    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    def test_lifecycle_skip_selection_criteria(self, mock_swift):
        '''Should not lifecycle objects if metadata does not match.'''
        mock_swift.return_value.head_object.side_effect = self.not_found
        mock_ic = mock.Mock(get_object_metadata=mock.Mock(
            return_value={'x-object-meta-foo': 'False',
                          'x-timestamp': 1e9}))
        self.sync_swift.selection_criteria = {
            'AND': [{'x-object-meta-foo': 'True'},
                    {'x-object-meta-bar': 'False'}]}

        self.assertEqual(SyncSwift.UploadStatus.SKIPPED_METADATA,
                         self.sync_swift.upload_object(
                             {'name': 'key',
                              'storage_policy_index': 0,
                              'created_at': str(1e9)}, mock_ic))

    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    def test_lifecycle_match_selection_criteria(self, mock_swift):
        '''Should lifecycle objects with matching metadata.'''
        mock_swift.return_value.head_object.side_effect = self.not_found
        object_meta = {u'x-object-meta-fo\u00f4'.encode('utf-8'): 'True',
                       u'x-object-meta-b\u00e4r'.encode('utf-8'): 'False',
                       'x-timestamp': 1e9,
                       'Content-Length': '1024',
                       'etag': 'deadbeef',
                       'content-type': 'applcation/unknown'}
        mock_ic = mock.Mock(
            get_object_metadata=mock.Mock(return_value=object_meta),
            get_object=mock.Mock(
                return_value=(200, object_meta, FakeStream())))
        self.sync_swift.selection_criteria = {
            'AND': [{u'x-object-meta-fo\u00d4': 'True'},
                    {u'x-object-meta-b\u00c4r': 'False'}]}

        self.assertEqual(SyncSwift.UploadStatus.PUT,
                         self.sync_swift.upload_object(
                             {'name': 'key',
                              'storage_policy_index': 0,
                              'created_at': str(1e9)}, mock_ic))
