"""
Copyright 2019 SwiftStack

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

import hashlib
import json
import mock
import StringIO
import sys
import time
import unittest

from s3_sync import base_sync
from swift.common import swob
from swift.common.internal_client import UnexpectedResponse
from swift.common.utils import decode_timestamps, Timestamp


class TestMatchers(unittest.TestCase):
    def test_matchall(self):
        metadata = {'k1': 'v1', 'k2': 'v2', 'k3': 'v3'}
        testcases = [
            ([{'k1': 'v1'}, {'k2': 'v2'}, {'k3': 'v3'}], True),
            ([{'k1': 'v1'}, {'k2': 'v2'}, {'k3': 'v2'}], False),
            ([{'k1': 'v1'}, {'k4': 'v2'}, {'k3': 'v3'}], False),
            ([{'k1': 'v1'}, {'k2': 'v5'}, {'k3': 'v5'}], False),
            ([{'k1': 'v5'}, {'k2': 'v2'}, {'k5': 'v3'}], False),
            ([{'k1': 'v5'}, {'k2': 'v5'}, {'k3': 'v3'}], False),
            ([{'k5': 'v1'}, {'k4': 'v2'}, {'k3': 'v3'}], False),
            ([{'k5': 'v1'}, {'k2': 'v5'}, {'k3': 'v5'}], False),
        ]
        for i, (test_dict, expected_res) in enumerate(testcases):
            self.assertEqual(
                base_sync.match_all(metadata, test_dict),
                expected_res, "Failed test %d" % i)

    def test_matchany(self):
        metadata = {'k1': 'v1', 'k2': 'v2', 'k3': 'v3'}
        testcases = [
            ([{'k1': 'v1'}, {'k2': 'v2'}, {'k3': 'v3'}], True),
            ([{'k1': 'v1'}, {'k2': 'v2'}, {'k3': 'v2'}], True),
            ([{'k1': 'v1'}, {'k4': 'v2'}, {'k3': 'v3'}], True),
            ([{'k1': 'v1'}, {'k2': 'v5'}, {'k3': 'v5'}], True),
            ([{'k1': 'v5'}, {'k2': 'v2'}, {'k5': 'v3'}], True),
            ([{'k1': 'v5'}, {'k2': 'v5'}, {'k3': 'v3'}], True),
            ([{'k5': 'v1'}, {'k4': 'v2'}, {'k3': 'v3'}], True),
            ([{'k5': 'v1'}, {'k2': 'v5'}, {'k3': 'v5'}], False),
        ]
        for i, (test_dict, expected_res) in enumerate(testcases):
            self.assertEqual(
                base_sync.match_any(metadata, test_dict),
                expected_res, "Failed test %d" % i)

    def test_match_item(self):
        default_metadata = {'k1': 'v1', 'k2': 'v2', 'k3': 'v3'}
        testcases = [
            ({'OR': [
                {'AND': [
                    {'NOT': {'k1': 'v1'}},
                    {'k2': 'v2'}
                ]},
                {'AND': [
                    {'k1': 'v1'},
                    {'NOT': {'k3': 'v4'}}
                ]}
            ]}, True),
            ({'OR': [
                {'AND': [
                    {'NOT': {'k1': 'v4'}},
                    {'k2': 'v2'},
                ]},
                {'AND': [
                    {'k1': 'v1'},
                    {'NOT': {'k3': 'v3'}},
                ]}
            ]}, True),
            ({'OR': [
                {'AND': [
                    {'NOT': {'k1': 'v1'}},
                    {'k2': 'v2'},
                ]},
                {'AND': [
                    {'k1': 'v1'},
                    {'k3': 'v4'}
                ]},
            ]}, False),
            ({'AND': [
                {'AND': [
                    {'NOT': {'k1': 'v1'}},
                    {'k2': 'v2'},
                ]},
                {'AND': [
                    {'k1': 'v1'},
                    {'NOT': {'k3': 'v4'}}
                ]}
            ]}, False),
            ({'k1': 'v1'},
             {'k2': 'v2', 'k3': 'v3', 'k1': 'v1'},
             True),
            ({'k1': 'v1'},
             {},
             False),
            ({'k1': 'v1'},
             {'k2': 'v2', 'k3': 'v3'},
             False),
            ({'k1': 'v1'},
             {'k1': None},
             False),
            ({'k1': 'v1'},
             {'k1': 'v3'},
             False),
            ({'k1': 'v1'},
             {'k2': 'v2', 'k3': 'v3', 'k1': 'v3'},
             False),
            ({'AND': [{'k1': 'v1'}, {'k2': 'v2'}]},
             {'k2': 'v2', 'k3': 'v3', 'k1': 'v1'},
             True),
            ({'AND': [{'k1': 'v1'}, {'k2': 'v2'}]},
             {},
             False),
            ({'AND': [{'k1': 'v1'}, {'k2': 'v2'}]},
             {'k2': 'v2', 'k3': 'v3'},
             False),
            ({'AND': [{'k1': 'v1'}, {'k2': 'v2'}]},
             {'k1': None},
             False),
            ({'AND': [{'k1': 'v1'}, {'k2': 'v2'}]},
             {'k1': 'v3'},
             False),
            ({'AND': [{'k1': 'v1'}, {'k2': 'v2'}]},
             {'k2': 'v2', 'k3': 'v3', 'k1': 'v3'},
             False),
            ({'OR': [{'k1': 'v1'}, {'k2': 'v2'}]},
             {'k2': 'v2', 'k3': 'v3', 'k1': 'v1'},
             True),
            ({'OR': [{'k1': 'v1'}, {'k2': 'v2'}]},
             {'k2': 'v2', 'k3': 'v3'},
             True),
            ({'OR': [{'k1': 'v1'}, {'k2': 'v2'}]},
             {'k2': 'v2', 'k3': 'v3', 'k1': 'v3'},
             True),
            ({'OR': [{'k1': 'v1'}, {'k2': 'v2'}]},
             {},
             False),
            ({'OR': [{'k1': 'v1'}, {'k2': 'v2'}]},
             {'k1': None},
             False),
            ({'OR': [{'k1': 'v1'}, {'k2': 'v2'}]},
             {'k1': 'v3'},
             False),
            ({u'FO\u00d3': u'B\u00c1R'},
             {u'fo\u00f3'.encode('utf-8'): u'b\u00e1r'.encode('utf-8')},
             True),
            ({'Foo': 'Bar'},
             {'Foo': 'Bar'},
             True)
        ]
        for i, testcase in enumerate(testcases):
            if len(testcase) < 3:
                test_meta = default_metadata
            else:
                test_meta = testcase[1]
            test_dict = testcase[0]
            expected = testcase[-1]
            self.assertEqual(
                base_sync.match_item(test_meta, test_dict),
                expected, "Failed test %s" % str(testcase))


class TestBaseSync(unittest.TestCase):
    def setUp(self):
        self.settings = {
            'account': 'account',
            'container': 'container',
            'aws_bucket': 'bucket'
        }

    @mock.patch('s3_sync.base_sync.BaseSync._get_client_factory')
    def test_http_pool_locking(self, factory_mock):
        factory_mock.return_value = mock.Mock()

        base = base_sync.BaseSync(self.settings, max_conns=1)
        with base.client_pool.get_client():
            self.assertEqual(0, base.client_pool.get_semaphore.balance)
            self.assertEqual(
                0, base.client_pool.client_pool[0].semaphore.balance)
        self.assertEqual(1, base.client_pool.get_semaphore.balance)

    @mock.patch('s3_sync.base_sync.BaseSync._get_client_factory')
    def test_double_release(self, factory_mock):
        factory_mock.return_value = mock.Mock()

        base = base_sync.BaseSync(self.settings, max_conns=1)
        client = base.client_pool.get_client()
        self.assertEqual(0, base.client_pool.get_semaphore.balance)
        self.assertEqual(
            0, base.client_pool.client_pool[0].semaphore.balance)
        client.close()
        self.assertEqual(1, base.client_pool.get_semaphore.balance)
        self.assertEqual(1, client.semaphore.balance)

        with self.assertRaises(RuntimeError):
            client.close()
        self.assertEqual(1, client.semaphore.balance)

    @mock.patch('s3_sync.base_sync.BaseSync._get_client_factory')
    def test_select_container_metadata(self, factory_mock):
        factory_mock.return_value = mock.Mock()
        test_headers = {
            'X-Container-Meta-foo': ('foo', '1545179217.201453'),
            'x-delete-at': ('1645179217', '1545179217.201453'),
            'someotherkey': ('boo', '1545179217.201453'),
            'X-Versions-Location': (
                'test_versions_location', '1545179217.201453'),
            'X-Container-Meta-bar': ('bar', '1545179217.201453'),
            'X-Container-Read': ('test2:tester2', '1545179217.201453'),
        }
        expected_result = {
            'X-Container-Meta-foo': 'foo',
            'X-Container-Meta-bar': 'bar',
            'X-Container-Read': 'test2:tester2',
        }
        base = base_sync.BaseSync(self.settings, max_conns=1)
        self.assertFalse(base.sync_container_metadata)
        self.assertEqual({}, base.select_container_metadata(test_headers))
        self.settings['sync_container_metadata'] = True
        self.settings['sync_container_acl'] = True
        base = base_sync.BaseSync(self.settings, max_conns=1)
        self.assertTrue(base.sync_container_metadata)
        self.assertEqual(expected_result,
                         base.select_container_metadata(test_headers))

    def test_provider_response_reraise(self):
        def blammo():
            raise Exception('boom?')

        try:
            blammo()
        except Exception:
            r = base_sync.ProviderResponse(False, 404, {}, iter(['']),
                                           exc_info=sys.exc_info())

        with self.assertRaises(Exception) as cm:
            r.reraise()

        self.assertEqual('boom?', cm.exception.message)

    def test_provider_response_reraise_no_exc_info(self):
        headers = swob.HeaderEnvironProxy({
            'HTTP_FOO': 'bar',
            'CONTENT_LENGTH': '88',
        })
        r = base_sync.ProviderResponse(True, 204, headers, iter(['ab', 'cd']))
        with self.assertRaises(ValueError) as cm:
            r.reraise()
        self.assertEqual(
            'reraise had no prior exception for '
            '<%s: %s, %r, %s, %r>' % (
                'ProviderResponse', 'True', 204,
                "{'Content-Length': '88', 'Foo': 'bar'}",
                "abcd"),
            cm.exception.message)

    def test_provider_response_reraise_no_exc_info_long_body(self):
        headers = swob.HeaderEnvironProxy({
            'HTTP_FOO': 'bar',
            'CONTENT_LENGTH': '88',
        })
        r = base_sync.ProviderResponse(True, 204, headers, iter(['A'] * 100))
        with self.assertRaises(ValueError) as cm:
            r.reraise()
        self.assertEqual(
            'reraise had no prior exception for '
            '<%s: %s, %r, %s, %r>' % (
                'ProviderResponse', 'True', 204,
                "{'Content-Length': '88', 'Foo': 'bar'}",
                ("A" * 70) + '...'),
            cm.exception.message)

    @mock.patch('s3_sync.base_sync.BaseSync._get_client_factory')
    def test_retain_copy(self, factory_mock):
        factory_mock.return_value = mock.Mock()
        base = base_sync.BaseSync(self.settings, max_conns=1)
        swift_client = mock.Mock()
        swift_client.get_object_metadata.return_value = {}

        row = {'deleted': 0,
               'created_at': str(time.time() - 5),
               'name': 'foo',
               'storage_policy_index': 99}

        _, _, swift_ts = decode_timestamps(row['created_at'])
        base.delete_local_object(swift_client, row, swift_ts, False)
        swift_ts.offset += 1

        swift_client.delete_object.assert_called_once_with(
            self.settings['account'], self.settings['container'],
            row['name'], acceptable_statuses=(2, 404, 409),
            headers={'X-Timestamp': Timestamp(swift_ts).internal})

    @mock.patch('s3_sync.base_sync.BaseSync._get_client_factory')
    def test_retain_copy_slo(self, factory_mock):
        factory_mock.return_value = mock.Mock()
        base = base_sync.BaseSync(self.settings, max_conns=1)
        swift_client = mock.Mock()
        swift_client.get_object_metadata.return_value = {
            'x-static-large-object': 'true'}

        def _get_object(account, container, key, **kwargs):
            manifest = [
                {'name': '/container_segments/part1',
                 'hash': 'deadbeef'},
                {'name': '/container_segments/part2',
                 'hash': 'deadbeef2'}]
            body = json.dumps(manifest)
            headers = {
                'etag': hashlib.md5(body).hexdigest()}
            return 200, headers, StringIO.StringIO(body)

        swift_client.get_object.side_effect = _get_object
        row = {'deleted': 0,
               'created_at': str(time.time() - 5),
               'name': 'foo',
               'storage_policy_index': 99}

        _, _, swift_ts = decode_timestamps(row['created_at'])
        base.delete_local_object(swift_client, row, swift_ts, False)
        swift_ts.offset += 1

        swift_client.delete_object.assert_has_calls([
            mock.call(self.settings['account'], 'container_segments', 'part1',
                      acceptable_statuses=(2, 404, 409)),
            mock.call(self.settings['account'], 'container_segments', 'part2',
                      acceptable_statuses=(2, 404, 409)),
            mock.call(self.settings['account'], self.settings['container'],
                      row['name'], acceptable_statuses=(2, 404, 409),
                      headers={'X-Timestamp': Timestamp(swift_ts).internal})])

    @mock.patch('s3_sync.base_sync.BaseSync._get_client_factory')
    def test_retain_copy_dlo(self, factory_mock):
        factory_mock.return_value = mock.Mock()
        base = base_sync.BaseSync(self.settings, max_conns=1)
        swift_client = mock.NonCallableMock()
        swift_client.get_object_metadata.return_value = {
            'x-object-manifest': 'container_segments/segment_'}

        swift_client.make_request.side_effect = (
            mock.Mock(
                body=json.dumps([
                    {'name': 'segments_%d' % (i + 1),
                     'hash': 'deadbeef'}
                    for i in range(2)]),
                status_int=200),
            mock.Mock(body='[]', status_int=200))

        row = {'deleted': 0,
               'created_at': str(time.time() - 5),
               'name': 'foo',
               'storage_policy_index': 99}

        _, _, swift_ts = decode_timestamps(row['created_at'])
        base.delete_local_object(swift_client, row, swift_ts, False)
        swift_ts.offset += 1

        swift_client.delete_object.assert_has_calls([
            mock.call(self.settings['account'], 'container_segments',
                      'segments_1', acceptable_statuses=(2, 404, 409)),
            mock.call(self.settings['account'], 'container_segments',
                      'segments_2', acceptable_statuses=(2, 404, 409)),
            mock.call(self.settings['account'], self.settings['container'],
                      row['name'], acceptable_statuses=(2, 404, 409),
                      headers={'X-Timestamp': Timestamp(swift_ts).internal})])

    @mock.patch('s3_sync.base_sync.BaseSync._get_client_factory')
    def test_fail_upload_segment(self, factory_mock):
        factory_mock.return_value = mock.Mock()
        base = base_sync.BaseSync(self.settings, max_conns=1)
        base.logger = mock.Mock()
        swift_client = mock.Mock()
        swift_client.get_object_metadata.return_value = {
            'x-static-large-object': 'true'}

        def _get_object(account, container, key, **kwargs):
            manifest = [
                {'name': '/container_segments/part1',
                 'hash': 'deadbeef'},
                {'name': '/container_segments/part2',
                 'hash': 'deadbeef2'}]
            body = json.dumps(manifest)
            headers = {
                'etag': hashlib.md5(body).hexdigest()}
            return 200, headers, StringIO.StringIO(body)

        def _delete_object(acc, cont, obj, acceptable_statuses):
            if obj == 'part1':
                raise UnexpectedResponse('foo', None)

        swift_client.get_object.side_effect = _get_object
        swift_client.delete_object.side_effect = _delete_object

        row = {'deleted': 0,
               'created_at': str(time.time() - 5),
               'name': 'foo',
               'storage_policy_index': 99}

        _, _, swift_ts = decode_timestamps(row['created_at'])
        base.delete_local_object(swift_client, row, swift_ts, False)

        # manifest should not be deleted
        swift_client.delete_object.assert_has_calls([
            mock.call(self.settings['account'], 'container_segments', 'part1',
                      acceptable_statuses=(2, 404, 409)),
            mock.call(self.settings['account'], 'container_segments', 'part2',
                      acceptable_statuses=(2, 404, 409))])

        base.logger.warning.assert_called_once_with(
            'Failed to delete segment %s/%s/%s: %s', 'account',
            'container_segments', 'part1', 'foo')
        base.logger.error.assert_called_once_with(
            'Failed to delete %s segments of %s/%s', 1, 'container', 'foo')
