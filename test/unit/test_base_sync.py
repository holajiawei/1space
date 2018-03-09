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
from s3_sync.base_sync import BaseSync
import unittest


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

        base = BaseSync(self.settings, max_conns=1)
        with base.client_pool.get_client():
            self.assertEqual(0, base.client_pool.get_semaphore.balance)
            self.assertEqual(
                0, base.client_pool.client_pool[0].semaphore.balance)
        self.assertEqual(1, base.client_pool.get_semaphore.balance)

    @mock.patch('s3_sync.base_sync.BaseSync._get_client_factory')
    def test_double_release(self, factory_mock):
        factory_mock.return_value = mock.Mock()

        base = BaseSync(self.settings, max_conns=1)
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
