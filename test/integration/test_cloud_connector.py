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

import boto3
import hashlib
from swiftclient import ClientException

from . import TestCloudSyncBase, clear_swift_container, clear_s3_bucket

from s3_sync.provider_factory import create_provider


class TestCloudConnector(TestCloudSyncBase):
    maxDiff = None

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
        self.cc_endpoint = "http://cloud-connector:%d" % (
            self.PORTS['cloud_connector'],)
        self.cc_mapping = {
            "account": u"AUTH_test",
            "container": "s3-restore",
            "aws_bucket": "s3-restore",
            "aws_endpoint": "http://cloud-connector:8081",
            "aws_identity": u"test:tester",
            "aws_secret": u"testing",
            "protocol": "s3",
            "custom_prefix": '',
        }
        self.cc_provider = create_provider(self.cc_mapping, 1, False)

        session = boto3.session.Session(
            aws_access_key_id=self.cc_mapping['aws_identity'],
            aws_secret_access_key=self.cc_mapping['aws_secret'])
        conf = boto3.session.Config(s3={'addressing_style': 'path'})
        self._orig_s3_client = self.s3_client
        self.s3_client = session.client('s3', config=conf,
                                        endpoint_url=self.cc_endpoint)

        # make sure our container & bucket are clear
        self.tearDown()

    def tearDown(self):
        clear_swift_container(self.conn_noshunt, self.mapping['container'])
        clear_s3_bucket(self._orig_s3_client, self.mapping['aws_bucket'])

    def test_container_get_just_s3_objs(self):
        obj_names = [u'floo\u062agloo', u'2nd obj', u'i/am/groot']

        # Upload a few objs into local-to-us S3
        exp_etags = {}
        for obj_name in obj_names:
            body_iter = ['a'] * (len(obj_name) + 1)
            resp = self.cc_provider.put_object(
                obj_name, {'content-length': str(len(obj_name) + 1),
                           'content-type': 'application/party-time'},
                body_iter)
            exp_etags[obj_name] = '"' + \
                hashlib.md5(''.join(body_iter)).hexdigest() + '"'
            self.assertEqual(200, resp.status)  # S3 sez 200
            self.assertEqual('', ''.join(resp.body))

        list_resp = self.s3_client.list_objects_v2(
            Bucket=self.cc_mapping['aws_bucket'],
        )
        self.assertEqual([{
            'ETag': exp_etags[u'2nd obj'], u'StorageClass': 'STANDARD',
            u'Key': '2nd obj', u'Size': 8,
        }, {
            'ETag': exp_etags[u'floo\u062agloo'], u'StorageClass': 'STANDARD',
            u'Key': u'floo\u062agloo', u'Size': 10,
        }, {
            'ETag': exp_etags[u'i/am/groot'], u'StorageClass': 'STANDARD',
            u'Key': 'i/am/groot', u'Size': 11,
        }], [{k: v for k, v in l.items() if k != 'LastModified'}
             for l in list_resp['Contents']])

    def test_container_get_local_remote_local(self):
        loc_obj_names = [u'2nd obj', u'i/am/groot']
        rem_obj_names = [u'floo\u062agloo']

        exp_etags = {}
        # locals
        for obj_name in loc_obj_names:
            body_iter = ['a'] * (len(obj_name) + 1)
            resp = self.cc_provider.put_object(
                obj_name, {'content-length': str(len(obj_name) + 1),
                           'content-type': 'application/party-time'},
                body_iter)
            exp_etags[obj_name] = '"' + \
                hashlib.md5(''.join(body_iter)).hexdigest() + '"'
            self.assertEqual(200, resp.status)  # S3 sez 200
            self.assertEqual('', ''.join(resp.body))
        # remotes
        for obj_name in rem_obj_names:
            body = 'a' * (len(obj_name) + 1)
            got_etag = self.conn_noshunt.put_object(
                self.cc_mapping['container'], obj_name,
                content_length=str(len(obj_name) + 1),
                content_type='application/party-time',
                contents=body)
            self.assertEqual(got_etag,
                             hashlib.md5(body).hexdigest())
            exp_etags[obj_name] = '"' + got_etag + '"'

        list_resp = self.s3_client.list_objects_v2(
            Bucket=self.cc_mapping['aws_bucket'],
        )
        self.assertEqual([{
            'ETag': exp_etags[u'2nd obj'], u'StorageClass': 'STANDARD',
            u'Key': '2nd obj', u'Size': 8,
        }, {
            'ETag': exp_etags[u'floo\u062agloo'], u'StorageClass': 'STANDARD',
            u'Key': u'floo\u062agloo', u'Size': 10,
        }, {
            'ETag': exp_etags[u'i/am/groot'], u'StorageClass': 'STANDARD',
            u'Key': 'i/am/groot', u'Size': 11,
        }], [{k: v for k, v in l.items() if k != 'LastModified'}
             for l in list_resp['Contents']])

    def test_container_get_remote_local_remote(self):
        loc_obj_names = [u'floo\u062agloo']
        rem_obj_names = [u'2nd obj', u'i/am/groot']

        exp_etags = {}
        # locals
        for obj_name in loc_obj_names:
            body_iter = ['a'] * (len(obj_name) + 1)
            resp = self.cc_provider.put_object(
                obj_name, {'content-length': str(len(obj_name) + 1),
                           'content-type': 'application/party-time'},
                body_iter)
            exp_etags[obj_name] = '"' + \
                hashlib.md5(''.join(body_iter)).hexdigest() + '"'
            self.assertEqual(200, resp.status)  # S3 sez 200
            self.assertEqual('', ''.join(resp.body))
        # remotes
        for obj_name in rem_obj_names:
            body = 'a' * (len(obj_name) + 1)
            got_etag = self.conn_noshunt.put_object(
                self.cc_mapping['container'], obj_name,
                content_length=str(len(obj_name) + 1),
                content_type='application/party-time',
                contents=body)
            self.assertEqual(got_etag,
                             hashlib.md5(body).hexdigest())
            exp_etags[obj_name] = '"' + got_etag + '"'

        list_resp = self.s3_client.list_objects_v2(
            Bucket=self.cc_mapping['aws_bucket'],
        )
        self.assertEqual([{
            'ETag': exp_etags[u'2nd obj'], u'StorageClass': 'STANDARD',
            u'Key': '2nd obj', u'Size': 8,
        }, {
            'ETag': exp_etags[u'floo\u062agloo'], u'StorageClass': 'STANDARD',
            u'Key': u'floo\u062agloo', u'Size': 10,
        }, {
            'ETag': exp_etags[u'i/am/groot'], u'StorageClass': 'STANDARD',
            u'Key': 'i/am/groot', u'Size': 11,
        }], [{k: v for k, v in l.items() if k != 'LastModified'}
             for l in list_resp['Contents']])

    def test_container_get_just_swift_objs(self):
        obj_names = [u'floo\u062agloo', u'2nd obj', u'i/am/groot']

        exp_etags = {}
        for obj_name in obj_names:
            body = 'a' * (len(obj_name) + 1)
            got_etag = self.conn_noshunt.put_object(
                self.cc_mapping['container'], obj_name,
                content_length=str(len(obj_name) + 1),
                content_type='application/party-time',
                contents=body)
            self.assertEqual(got_etag,
                             hashlib.md5(body).hexdigest())
            exp_etags[obj_name] = '"' + got_etag + '"'

        list_resp = self.s3_client.list_objects_v2(
            Bucket=self.cc_mapping['aws_bucket'],
        )
        self.assertEqual([{
            'ETag': exp_etags[u'2nd obj'], u'StorageClass': 'STANDARD',
            u'Key': '2nd obj', u'Size': 8,
        }, {
            'ETag': exp_etags[u'floo\u062agloo'], u'StorageClass': 'STANDARD',
            u'Key': u'floo\u062agloo', u'Size': 10,
        }, {
            'ETag': exp_etags[u'i/am/groot'], u'StorageClass': 'STANDARD',
            u'Key': 'i/am/groot', u'Size': 11,
        }], [{k: v for k, v in l.items() if k != 'LastModified'}
             for l in list_resp['Contents']])

    def test_container_get_filters_against_s3_dirs(self):
        obj_names = [
            u'\u0622|',
            u'\u0622|1',
            u'\u062a|2',
            u'\u062a|a|',
            u'\u062a|a|b',
            u'\u062a|c|d',
            u'\u062a|c|e',
        ]

        exp_etags = {}
        for obj_name in obj_names:
            body_iter = ['a'] * (len(obj_name) + 1)
            resp = self.cc_provider.put_object(
                obj_name, {'content-length': str(len(obj_name) + 1),
                           'content-type': 'application/party-time'},
                body_iter)
            exp_etags[obj_name] = '"' + \
                hashlib.md5(''.join(body_iter)).hexdigest() + '"'
            self.assertEqual(200, resp.status)  # S3 sez 200
            self.assertEqual('', ''.join(resp.body))

        list_resp = self.s3_client.list_objects_v2(
            Bucket=self.cc_mapping['aws_bucket'],
        )
        self.assertEqual([
            {'ETag': exp_etags[o], u'StorageClass': 'STANDARD',
             u'Key': o, u'Size': len(o) + 1}
            for o in obj_names
        ], [{k: v for k, v in l.items() if k != 'LastModified'}
            for l in list_resp['Contents']])
        self.assertNotIn('CommonPrefixes', list_resp, repr(list_resp))

        # With StartAfter
        list_resp = self.s3_client.list_objects_v2(
            Bucket=self.cc_mapping['aws_bucket'],
            StartAfter=obj_names[2],
        )
        self.assertEqual([
            {'ETag': exp_etags[o], u'StorageClass': 'STANDARD',
             u'Key': o, u'Size': len(o) + 1}
            for o in obj_names[3:]
        ], [{k: v for k, v in l.items() if k != 'LastModified'}
            for l in list_resp['Contents']])
        self.assertNotIn('CommonPrefixes', list_resp, repr(list_resp))
        self.assertFalse(list_resp['IsTruncated'])

        # With delimiter
        list_resp = self.s3_client.list_objects_v2(
            Bucket=self.cc_mapping['aws_bucket'],
            Delimiter='|',
        )
        self.assertEqual([
            {'Prefix': u'\u0622|'}, {'Prefix': u'\u062a|'},
        ], list_resp['CommonPrefixes'])
        self.assertNotIn('Contents', list_resp, repr(list_resp))

        # With delimiter and prefix
        list_resp = self.s3_client.list_objects_v2(
            Bucket=self.cc_mapping['aws_bucket'],
            Delimiter='|',
            Prefix=u'\u062a|',
        )
        self.assertEqual([
            {'Prefix': u'\u062a|a|'}, {'Prefix': u'\u062a|c|'},
        ], list_resp['CommonPrefixes'])
        self.assertEqual([
            {'ETag': exp_etags[o], u'StorageClass': 'STANDARD',
             u'Key': o, u'Size': len(o) + 1}
            for o in (u'\u062a|2',)
        ], [{k: v for k, v in l.items() if k != 'LastModified'}
            for l in list_resp['Contents']])
        self.assertFalse(list_resp['IsTruncated'])

        # With delimiter and prefix and limit 1
        list_resp = self.s3_client.list_objects_v2(
            Bucket=self.cc_mapping['aws_bucket'],
            Delimiter='|',
            Prefix=u'\u062a|',
            MaxKeys=1,
        )
        self.assertEqual([
            {'ETag': exp_etags[o], u'StorageClass': 'STANDARD',
             u'Key': o, u'Size': len(o) + 1}
            for o in (u'\u062a|2',)
        ], [{k: v for k, v in l.items() if k != 'LastModified'}
            for l in list_resp['Contents']])
        # I guess the 1 limit is satisfied with objects before dirs?
        self.assertNotIn('CommonPrefixes', list_resp, repr(list_resp))
        self.assertTrue(list_resp['IsTruncated'])

        # With delimiter and prefix and limit 1 and continuation token
        list_resp = self.s3_client.list_objects_v2(
            Bucket=self.cc_mapping['aws_bucket'],
            Delimiter='|',
            Prefix=u'\u062a|',
            MaxKeys=1,
            ContinuationToken=list_resp['NextContinuationToken'],
        )
        self.assertNotIn('Contents', list_resp)
        self.assertEqual([
            {'Prefix': u'\u062a|a|'},
        ], list_resp['CommonPrefixes'])
        self.assertTrue(list_resp['IsTruncated'])

        # With delimiter and prefix and limit 2
        list_resp = self.s3_client.list_objects_v2(
            Bucket=self.cc_mapping['aws_bucket'],
            Delimiter='|',
            Prefix=u'\u062a|',
            MaxKeys=2,
        )
        self.assertEqual([
            {'ETag': exp_etags[o], u'StorageClass': 'STANDARD',
             u'Key': o, u'Size': len(o) + 1}
            for o in (u'\u062a|2',)
        ], [{k: v for k, v in l.items() if k != 'LastModified'}
            for l in list_resp['Contents']])
        self.assertEqual([
            {'Prefix': u'\u062a|a|'},
        ], list_resp['CommonPrefixes'])
        self.assertTrue(list_resp['IsTruncated'])

        # With delimiter and prefix and limit 3
        list_resp = self.s3_client.list_objects_v2(
            Bucket=self.cc_mapping['aws_bucket'],
            Delimiter='|',
            Prefix=u'\u062a|',
            MaxKeys=3,
        )
        self.assertEqual([
            {'ETag': exp_etags[o], u'StorageClass': 'STANDARD',
             u'Key': o, u'Size': len(o) + 1}
            for o in (u'\u062a|2',)
        ], [{k: v for k, v in l.items() if k != 'LastModified'}
            for l in list_resp['Contents']])
        self.assertEqual([
            {'Prefix': u'\u062a|a|'}, {'Prefix': u'\u062a|c|'},
        ], list_resp['CommonPrefixes'])
        self.assertFalse(list_resp['IsTruncated'])

    def test_container_get_filters_against_mixed_obj_stores(self):
        obj_names = [
            u'\u0622|',
            u'\u0622|1',
            u'\u062a|2',
            u'\u062a|a|',
            u'\u062a|a|b',
            u'\u062a|c|d',
            u'\u062a|c|e',
        ]

        exp_etags = {}
        for i, obj_name in enumerate(obj_names):
            if i % 2 == 0:
                body_iter = ['a'] * (len(obj_name) + 1)
                resp = self.cc_provider.put_object(
                    obj_name, {'content-length': str(len(obj_name) + 1),
                               'content-type': 'application/party-time'},
                    body_iter)
                exp_etags[obj_name] = '"' + \
                    hashlib.md5(''.join(body_iter)).hexdigest() + '"'
                self.assertEqual(200, resp.status)  # S3 sez 200
                self.assertEqual('', ''.join(resp.body))
            else:
                body = 'a' * (len(obj_name) + 1)
                got_etag = self.conn_noshunt.put_object(
                    self.cc_mapping['container'], obj_name,
                    content_length=str(len(obj_name) + 1),
                    content_type='application/party-time',
                    contents=body)
                self.assertEqual(got_etag,
                                 hashlib.md5(body).hexdigest())
                exp_etags[obj_name] = '"' + got_etag + '"'

        list_resp = self.s3_client.list_objects_v2(
            Bucket=self.cc_mapping['aws_bucket'],
        )
        self.assertEqual([
            {'ETag': exp_etags[o], u'StorageClass': 'STANDARD',
             u'Key': o, u'Size': len(o) + 1}
            for o in obj_names
        ], [{k: v for k, v in l.items() if k != 'LastModified'}
            for l in list_resp['Contents']])
        self.assertNotIn('CommonPrefixes', list_resp, repr(list_resp))

        # With StartAfter
        list_resp = self.s3_client.list_objects_v2(
            Bucket=self.cc_mapping['aws_bucket'],
            StartAfter=obj_names[2],
        )
        self.assertEqual([
            {'ETag': exp_etags[o], u'StorageClass': 'STANDARD',
             u'Key': o, u'Size': len(o) + 1}
            for o in obj_names[3:]
        ], [{k: v for k, v in l.items() if k != 'LastModified'}
            for l in list_resp['Contents']])
        self.assertNotIn('CommonPrefixes', list_resp, repr(list_resp))
        self.assertFalse(list_resp['IsTruncated'])

        # With delimiter
        list_resp = self.s3_client.list_objects_v2(
            Bucket=self.cc_mapping['aws_bucket'],
            Delimiter='|',
        )
        self.assertEqual([
            {'Prefix': u'\u0622|'}, {'Prefix': u'\u062a|'},
        ], list_resp['CommonPrefixes'])
        self.assertNotIn('Contents', list_resp, repr(list_resp))

        # With delimiter and prefix
        list_resp = self.s3_client.list_objects_v2(
            Bucket=self.cc_mapping['aws_bucket'],
            Delimiter='|',
            Prefix=u'\u062a|',
        )
        self.assertEqual([
            {'Prefix': u'\u062a|a|'}, {'Prefix': u'\u062a|c|'},
        ], list_resp['CommonPrefixes'])
        self.assertEqual([
            {'ETag': exp_etags[o], u'StorageClass': 'STANDARD',
             u'Key': o, u'Size': len(o) + 1}
            for o in (u'\u062a|2',)
        ], [{k: v for k, v in l.items() if k != 'LastModified'}
            for l in list_resp['Contents']])
        self.assertFalse(list_resp['IsTruncated'])

        # With delimiter and prefix and limit 1
        list_resp = self.s3_client.list_objects_v2(
            Bucket=self.cc_mapping['aws_bucket'],
            Delimiter='|',
            Prefix=u'\u062a|',
            MaxKeys=1,
        )
        self.assertEqual([
            {'ETag': exp_etags[o], u'StorageClass': 'STANDARD',
             u'Key': o, u'Size': len(o) + 1}
            for o in (u'\u062a|2',)
        ], [{k: v for k, v in l.items() if k != 'LastModified'}
            for l in list_resp['Contents']])
        # I guess the 1 limit is satisfied with objects before dirs?
        self.assertNotIn('CommonPrefixes', list_resp, repr(list_resp))
        self.assertTrue(list_resp['IsTruncated'])

        # With delimiter and prefix and limit 1 and continuation token
        list_resp = self.s3_client.list_objects_v2(
            Bucket=self.cc_mapping['aws_bucket'],
            Delimiter='|',
            Prefix=u'\u062a|',
            MaxKeys=1,
            ContinuationToken=list_resp['NextContinuationToken'],
        )
        self.assertNotIn('Contents', list_resp)
        self.assertEqual([
            {'Prefix': u'\u062a|a|'},
        ], list_resp['CommonPrefixes'])
        self.assertTrue(list_resp['IsTruncated'])

        # With delimiter and prefix and limit 2
        list_resp = self.s3_client.list_objects_v2(
            Bucket=self.cc_mapping['aws_bucket'],
            Delimiter='|',
            Prefix=u'\u062a|',
            MaxKeys=2,
        )
        self.assertEqual([
            {'ETag': exp_etags[o], u'StorageClass': 'STANDARD',
             u'Key': o, u'Size': len(o) + 1}
            for o in (u'\u062a|2',)
        ], [{k: v for k, v in l.items() if k != 'LastModified'}
            for l in list_resp['Contents']])
        self.assertEqual([
            {'Prefix': u'\u062a|a|'},
        ], list_resp['CommonPrefixes'])
        self.assertTrue(list_resp['IsTruncated'])

        # With delimiter and prefix and limit 3
        list_resp = self.s3_client.list_objects_v2(
            Bucket=self.cc_mapping['aws_bucket'],
            Delimiter='|',
            Prefix=u'\u062a|',
            MaxKeys=3,
        )
        self.assertEqual([
            {'ETag': exp_etags[o], u'StorageClass': 'STANDARD',
             u'Key': o, u'Size': len(o) + 1}
            for o in (u'\u062a|2',)
        ], [{k: v for k, v in l.items() if k != 'LastModified'}
            for l in list_resp['Contents']])
        self.assertEqual([
            {'Prefix': u'\u062a|a|'}, {'Prefix': u'\u062a|c|'},
        ], list_resp['CommonPrefixes'])
        self.assertFalse(list_resp['IsTruncated'])

    def test_container_get_filters_against_swift_dirs(self):
        obj_names = [
            u'\u0622/',
            u'\u0622/1',
            u'\u062a/2',
            u'\u062a/a/',
            u'\u062a/a/b',
            u'\u062a/c/d',
            u'\u062a/c/e',
        ]

        exp_etags = {}
        for obj_name in obj_names:
            body = 'a' * (len(obj_name) + 1)
            got_etag = self.conn_noshunt.put_object(
                self.cc_mapping['container'], obj_name,
                content_length=str(len(obj_name) + 1),
                content_type='application/party-time',
                contents=body)
            self.assertEqual(got_etag,
                             hashlib.md5(body).hexdigest())
            exp_etags[obj_name] = '"' + got_etag + '"'

        list_resp = self.s3_client.list_objects_v2(
            Bucket=self.cc_mapping['aws_bucket'],
        )
        self.assertEqual([
            {'ETag': exp_etags[o], u'StorageClass': 'STANDARD',
             u'Key': o, u'Size': len(o) + 1}
            for o in obj_names
        ], [{k: v for k, v in l.items() if k != 'LastModified'}
            for l in list_resp['Contents']])
        self.assertNotIn('CommonPrefixes', list_resp, repr(list_resp))

        # With StartAfter
        list_resp = self.s3_client.list_objects_v2(
            Bucket=self.cc_mapping['aws_bucket'],
            StartAfter=obj_names[2],
        )
        self.assertEqual([
            {'ETag': exp_etags[o], u'StorageClass': 'STANDARD',
             u'Key': o, u'Size': len(o) + 1}
            for o in obj_names[3:]
        ], [{k: v for k, v in l.items() if k != 'LastModified'}
            for l in list_resp['Contents']])
        self.assertNotIn('CommonPrefixes', list_resp, repr(list_resp))
        self.assertFalse(list_resp['IsTruncated'])

        # With delimiter
        list_resp = self.s3_client.list_objects_v2(
            Bucket=self.cc_mapping['aws_bucket'],
            Delimiter='/',
        )
        self.assertEqual([
            {'Prefix': u'\u0622/'}, {'Prefix': u'\u062a/'},
        ], list_resp['CommonPrefixes'])
        self.assertNotIn('Contents', list_resp, repr(list_resp))

        # With delimiter and prefix
        list_resp = self.s3_client.list_objects_v2(
            Bucket=self.cc_mapping['aws_bucket'],
            Delimiter='/',
            Prefix=u'\u062a/',
        )
        self.assertEqual([
            {'Prefix': u'\u062a/a/'}, {'Prefix': u'\u062a/c/'},
        ], list_resp['CommonPrefixes'])
        self.assertEqual([
            {'ETag': exp_etags[o], u'StorageClass': 'STANDARD',
             u'Key': o, u'Size': len(o) + 1}
            for o in (u'\u062a/2',)
        ], [{k: v for k, v in l.items() if k != 'LastModified'}
            for l in list_resp['Contents']])
        self.assertFalse(list_resp['IsTruncated'])

        # With delimiter and prefix and limit 1
        list_resp = self.s3_client.list_objects_v2(
            Bucket=self.cc_mapping['aws_bucket'],
            Delimiter='/',
            Prefix=u'\u062a/',
            MaxKeys=1,
        )
        self.assertEqual([
            {'ETag': exp_etags[o], u'StorageClass': 'STANDARD',
             u'Key': o, u'Size': len(o) + 1}
            for o in (u'\u062a/2',)
        ], [{k: v for k, v in l.items() if k != 'LastModified'}
            for l in list_resp['Contents']])
        # I guess the 1 limit is satisfied with objects before dirs?
        self.assertNotIn('CommonPrefixes', list_resp, repr(list_resp))
        self.assertTrue(list_resp['IsTruncated'])

        # With delimiter and prefix and limit 1 and continuation token
        list_resp = self.s3_client.list_objects_v2(
            Bucket=self.cc_mapping['aws_bucket'],
            Delimiter='/',
            Prefix=u'\u062a/',
            MaxKeys=1,
            ContinuationToken=list_resp['NextContinuationToken'],
        )
        self.assertNotIn('Contents', list_resp)
        self.assertEqual([
            {'Prefix': u'\u062a/a/'},
        ], list_resp['CommonPrefixes'])
        self.assertTrue(list_resp['IsTruncated'])

        # With delimiter and prefix and limit 2
        list_resp = self.s3_client.list_objects_v2(
            Bucket=self.cc_mapping['aws_bucket'],
            Delimiter='/',
            Prefix=u'\u062a/',
            MaxKeys=2,
        )
        self.assertEqual([
            {'ETag': exp_etags[o], u'StorageClass': 'STANDARD',
             u'Key': o, u'Size': len(o) + 1}
            for o in (u'\u062a/2',)
        ], [{k: v for k, v in l.items() if k != 'LastModified'}
            for l in list_resp['Contents']])
        self.assertEqual([
            {'Prefix': u'\u062a/a/'},
        ], list_resp['CommonPrefixes'])
        self.assertTrue(list_resp['IsTruncated'])

        # With delimiter and prefix and limit 3
        list_resp = self.s3_client.list_objects_v2(
            Bucket=self.cc_mapping['aws_bucket'],
            Delimiter='/',
            Prefix=u'\u062a/',
            MaxKeys=3,
        )
        self.assertEqual([
            {'ETag': exp_etags[o], u'StorageClass': 'STANDARD',
             u'Key': o, u'Size': len(o) + 1}
            for o in (u'\u062a/2',)
        ], [{k: v for k, v in l.items() if k != 'LastModified'}
            for l in list_resp['Contents']])
        self.assertEqual([
            {'Prefix': u'\u062a/a/'}, {'Prefix': u'\u062a/c/'},
        ], list_resp['CommonPrefixes'])
        self.assertFalse(list_resp['IsTruncated'])

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

    def test_obj_put(self):
        # Not there yet...
        obj_name = u'flimm\u062aflamm'

        resp = self.cc_provider.get_object(obj_name)

        self.assertEqual(404, resp.status)
        self.assertEqual('The specified key does not exist.',
                         ''.join(resp.body))

        # PUT it through the cloud-connector
        resp = self.cc_provider.put_object(
            obj_name, {'x-object-meta-jam': 'bamm',
                       'content-length': '3'}, ['a', 'bc'])
        self.assertEqual(200, resp.status)  # S3 sez 200
        self.assertEqual('', ''.join(resp.body))

        resp = self.cc_provider.get_object(obj_name)

        self.assertEqual(200, resp.status)
        self.assertEqual('abc', ''.join(resp.body))
        self.assertEqual('bamm', resp.headers['x-object-meta-jam'])

        # It doesn't actually get streamed into the Swift cluster
        with self.assertRaises(ClientException) as cm:
            self.conn_noshunt.head_object(self.mapping['container'], obj_name)
        self.assertEqual(404, cm.exception.http_status)
