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
import urllib

from s3_sync.cloud_connector.auth import MEMCACHE_TOKEN_KEY_FMT


class TestCloudConnector(TestCloudSyncBase):
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
