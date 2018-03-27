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

from . import TestCloudSyncBase

from swift.common.memcached import MemcacheRing
from swiftclient.client import get_auth
from swiftclient.exceptions import ClientException
import urllib

from s3_sync.cloud_connector.auth import MEMCACHE_TOKEN_KEY_FMT
from s3_sync.provider_factory import create_provider


class TestCloudConnector(TestCloudSyncBase):
    def setUp(self):
        super(TestCloudConnector, self).setUp()

    def test_auth(self):
        # A successful auth through cloud-connect should result in a cached
        # token => Swift Account mapping, as well as a token valid inside the
        # onprem Swift cluster.
        url, token = self.cloud_connector('get_auth')

        auth_acct = url.rsplit('/')[-1]
        exp_acct = u"AUTH_\u062aacct".encode('utf8')
        self.assertEqual(exp_acct, urllib.unquote(auth_acct))

        # The token should match what the real Swift cluster hands out
        swift_url, swift_token = get_auth(
            self.SWIFT_CREDS['authurl'],
            self.SWIFT_CREDS['cloud-connector']['user'],
            self.SWIFT_CREDS['cloud-connector']['key'])
        self.assertEqual(swift_token, token)

        # whitebox check for cached token => acct mapping
        memcache_client = MemcacheRing(['127.0.0.1:11211'])
        tempauth_key = '%s/token/%s' % ('AUTH_', token)
        tempauth_expires, _ = memcache_client.get(tempauth_key)
        cache_key = MEMCACHE_TOKEN_KEY_FMT % token
        got_expires, got_acct = memcache_client.get(cache_key)

        self.assertEqual(exp_acct, got_acct.encode('utf8'))
        self.assertAlmostEqual(got_expires, tempauth_expires, delta=1)

    def test_obj_head_and_get_not_in_s3(self):
        mapping = self.s3_sync_cc_mapping()

        # A GET for an obj not in S3 should check in the Swift cluster
        with self.assertRaises(ClientException) as cm:
            self.cloud_connector('get_object', mapping['container'], 'foobie')
        self.assertEqual(404, cm.exception.http_status)

        # put the obj in swift
        with self.admin_conn_for(mapping['account']) as admin_conn:
            admin_conn.put_object(mapping['container'], 'foobie', 'abc',
                                  headers={'x-object-meta-crazy': 'madness'})

        rheaders, body = self.cloud_connector('get_object',
                                              mapping['container'], 'foobie')
        self.assertEqual('abc', body)
        self.assertEqual('madness', rheaders['x-object-meta-crazy'])

        rheaders = self.cloud_connector('head_object', mapping['container'],
                                        'foobie')
        self.assertEqual('madness', rheaders['x-object-meta-crazy'])
        self.assertEqual('3', rheaders['content-length'])

    def test_obj_head_and_get_in_s3(self):
        mapping = self.s3_sync_cc_mapping()

        # A GET for an obj in S3 should return the S3 obj (even in preference
        # to a copy also in Swift)
        with self.assertRaises(ClientException) as cm:
            self.cloud_connector('get_object', mapping['container'], 'barbie')
        self.assertEqual(404, cm.exception.http_status)

        # put the obj in S3
        provider = create_provider(mapping, 1)
        s3_key = provider.get_s3_name('barbie')
        self.s3('put_object', Bucket=mapping['aws_bucket'], Key=s3_key,
                Body='def', Metadata={'jojo': 'foofoo'})

        rheaders, body = self.cloud_connector('get_object',
                                              mapping['container'], 'barbie')
        self.assertEqual('def', body)
        self.assertEqual('foofoo', rheaders['x-object-meta-jojo'])

        rheaders = self.cloud_connector('head_object', mapping['container'],
                                        'barbie')
        self.assertEqual('foofoo', rheaders['x-object-meta-jojo'])
        self.assertEqual('3', rheaders['content-length'])

        # put a diff obj in real swift, should still get the S3 one back
        with self.admin_conn_for(mapping['account']) as admin_conn:
            admin_conn.put_object(mapping['container'], 'barbie', 'abcd',
                                  headers={'x-object-meta-crazy': 'madness'})

        rheaders, body = self.cloud_connector('get_object',
                                              mapping['container'], 'barbie')
        self.assertEqual('def', body)
        self.assertEqual('foofoo', rheaders['x-object-meta-jojo'])

        rheaders = self.cloud_connector('head_object', mapping['container'],
                                        'barbie')
        self.assertEqual('foofoo', rheaders['x-object-meta-jojo'])
        self.assertEqual('3', rheaders['content-length'])
