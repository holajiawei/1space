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

import mock
from s3_sync.providers import base_provider
from swift.common import swob
import sys
import unittest


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
                base_provider.match_all(metadata, test_dict),
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
                base_provider.match_any(metadata, test_dict),
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
                base_provider.match_item(test_meta, test_dict),
                expected, "Failed test %s" % str(testcase))


class TestBaseSync(unittest.TestCase):
    def setUp(self):
        self.settings = {
            'account': 'account',
            'container': 'container',
            'aws_bucket': 'bucket'
        }

    @mock.patch(
        's3_sync.providers.base_provider.BaseProvider._get_client_factory')
    def test_http_pool_locking(self, factory_mock):
        factory_mock.return_value = mock.Mock()

        base = base_provider.BaseProvider(self.settings, max_conns=1)
        with base.client_pool.get_client():
            self.assertEqual(0, base.client_pool.get_semaphore.balance)
            self.assertEqual(
                0, base.client_pool.client_pool[0].semaphore.balance)
        self.assertEqual(1, base.client_pool.get_semaphore.balance)

    @mock.patch(
        's3_sync.providers.base_provider.BaseProvider._get_client_factory')
    def test_double_release(self, factory_mock):
        factory_mock.return_value = mock.Mock()

        base = base_provider.BaseProvider(self.settings, max_conns=1)
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

    def test_provider_response_reraise(self):
        def blammo():
            raise Exception('boom?')

        try:
            blammo()
        except Exception:
            r = base_provider.ProviderResponse(False, 404, {}, iter(['']),
                                               exc_info=sys.exc_info())

        with self.assertRaises(Exception) as cm:
            r.reraise()

        self.assertEqual('boom?', cm.exception.message)

    def test_provider_response_reraise_no_exc_info(self):
        headers = swob.HeaderEnvironProxy({
            'HTTP_FOO': 'bar',
            'CONTENT_LENGTH': '88',
        })
        r = base_provider.ProviderResponse(
            True, 204, headers, iter(['ab', 'cd']))
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
        r = base_provider.ProviderResponse(
            True, 204, headers, iter(['A'] * 100))
        with self.assertRaises(ValueError) as cm:
            r.reraise()
        self.assertEqual(
            'reraise had no prior exception for '
            '<%s: %s, %r, %s, %r>' % (
                'ProviderResponse', 'True', 204,
                "{'Content-Length': '88', 'Foo': 'bar'}",
                ("A" * 70) + '...'),
            cm.exception.message)
