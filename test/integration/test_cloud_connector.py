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

from s3_sync.provider_factory import create_provider


class TestCloudConnector(TestCloudSyncBase):
    def setUp(self):
        super(TestCloudConnector, self).setUp()

        self.mapping = self._find_mapping(
            # NOTE: Swift container name must be a valid Amazon S3 bucket name.
            # Also, this mapping has a `copy_after` of 3600 meaning that
            # background daemons won't be mucking about with our objects while
            # these tests are trying to do _their_ job.
            lambda m: m.get('container') == u"s3-restore")
        self.conn_noshunt = self.conn_for_acct_noshunt(u'AUTH_test')
        self.local_to_me_provider = create_provider(self.mapping, 1, False)
        self.cc_provider = create_provider({
            "account": u"AUTH_test",
            "container": "s3-restore",
            "aws_bucket": "s3-restore",
            "aws_endpoint": "http://localhost:8081",
            "aws_identity": u"test:tester",
            "aws_secret": u"testing",
            "protocol": "s3",
            "custom_prefix": '',
        }, 1, False)

    def test_obj_head_and_get(self):
        # Not there yet...
        obj_name = u'shimmy\u062ajimmy'

        resp = self.cc_provider.get_object(obj_name)

        self.assertEqual(404, resp.status)
        self.assertEqual('The specified key does not exist.',
                         ''.join(resp.body))

        # Put an obj into swift
        self.conn_noshunt.put_object(
            self.mapping['container'], obj_name,
            'abc', headers={'x-object-meta-slim': 'slam'})
        # Sanity-check
        headers = self.conn_noshunt.head_object(self.mapping['container'],
                                                obj_name)
        self.assertEqual('3', headers['content-length'])

        resp = self.cc_provider.get_object(obj_name)

        self.assertEqual(200, resp.status)
        self.assertEqual('abc', ''.join(resp.body))
        self.assertEqual('slam', resp.headers['x-object-meta-slim'])

        # Sanity-check
        headers = self.conn_noshunt.head_object(self.mapping['container'],
                                                obj_name)
        self.assertEqual('3', headers['content-length'])

        # Stick a different obj in S3 and make sure that's what we get
        self.local_to_me_provider.put_object(obj_name,
                                             {'x-object-meta-jamm': 'bamm'},
                                             'def')

        resp = self.cc_provider.get_object(obj_name)

        self.assertEqual(200, resp.status)
        self.assertEqual('def', ''.join(resp.body))
        self.assertEqual('bamm', resp.headers['x-object-meta-jamm'])

        # Sanity-check
        headers = self.conn_noshunt.head_object(self.mapping['container'],
                                                obj_name)
        self.assertEqual('3', headers['content-length'])

        # And same if we delete the obj in swift
        self.conn_noshunt.delete_object(self.mapping['container'], obj_name)

        resp = self.cc_provider.get_object(obj_name)

        self.assertEqual(200, resp.status)
        self.assertEqual('def', ''.join(resp.body))
        self.assertEqual('bamm', resp.headers['x-object-meta-jamm'])
