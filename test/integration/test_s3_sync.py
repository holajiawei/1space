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

import hashlib
import json
import swiftclient
import time
import unittest
import utils

from itertools import repeat
from swiftclient.client import ClientException
from s3_sync.provider_factory import create_provider
from s3_sync.utils import DLO_ETAG_FIELD, SWIFT_USER_META_PREFIX,\
    SLO_ETAG_FIELD

from . import TestCloudSyncBase, clear_swift_container, \
    swift_content_location, s3_key_name, clear_s3_bucket


DAY = 60 * 60 * 24
DAY_F = DAY * 1.0


class TestCloudSync(TestCloudSyncBase):
    def setUp(self):
        self.statsd_server.clear()

    def tearDown(self):
        self.statsd_server.clear()

    def _test_archive(
            self, key, content, mapping, get_etag, expected_location,
            headers={}):
        crawler = utils.get_container_crawler(mapping)
        etag = self.local_swift(
            'put_object', mapping['container'], key,
            content.encode('utf-8'), headers=headers)
        self.assertEqual(
            hashlib.md5(content.encode('utf-8')).hexdigest(), etag)

        crawler.run_once()
        swift_hdrs, listing = self.local_swift(
            'get_container', mapping['container'])
        for entry in listing:
            if entry['name'] == key:
                break

        self.assertEqual(0, int(swift_hdrs['x-container-object-count']))
        self.assertEqual(etag, entry['hash'])
        self.assertEqual(
            [expected_location],
            entry['content_location'])
        self.assertEqual(etag, get_etag(key))

        hdrs = self.local_swift(
            'head_object', mapping['container'], key)
        for hdr in headers:
            self.assertIn(hdr, hdrs)
            self.assertEqual(headers[hdr], hdrs[hdr])

    def test_swift_sync_slash_star(self):
        mapping = self._find_mapping(
            lambda m: m['aws_bucket'] == 'crazy-target:')
        crawler = utils.get_container_crawler(mapping)
        target_conn = self.conn_for_acct_noshunt(mapping['aws_account'])

        conn = self.conn_for_acct(mapping['account'])
        puts = [  # (container, obj, body)
            ('slashc1', 'c1.o1', 'the c1 o1'),
            ('slashc1', 'c1.o2', 'the c1 o2'),
            ('slashc2', 'c2.o1', 'the c2 o2')]
        for cont, obj, body in puts:
            conn.put_container(cont)
            got_etag = conn.put_object(cont, obj, body)
            self.assertEqual(hashlib.md5(body).hexdigest(), got_etag)

        crawler.run_once()

        # The tree should have 2 containers and 3 objs
        self.assertEqual(5, len(self.get_swift_tree(target_conn)))
        self.assertEqual(['crazy-target:slashc1', 'crazy-target:slashc2',
                          'crazy-target:slashc1/c1.o1',
                          'crazy-target:slashc1/c1.o2',
                          'crazy-target:slashc2/c2.o1'],
                         self.get_swift_tree(target_conn))

        clear_swift_container(target_conn, 'crazy-target:slashc1')
        clear_swift_container(target_conn, 'crazy-target:slashc2')
        clear_swift_container(conn, 'slashc1')
        clear_swift_container(conn, 'slashc2')
        target_conn.delete_container('crazy-target:slashc1')
        target_conn.delete_container('crazy-target:slashc2')
        conn.delete_container('slashc1')
        conn.delete_container('slashc2')

    def test_swift_sync_account_slo(self):
        mapping = self._find_mapping(
            lambda m: m['aws_bucket'] == 'crazy-target:')
        crawler = utils.get_container_crawler(mapping)
        target_conn = self.conn_for_acct_noshunt(mapping['aws_account'])

        container = 'test-slo'
        key = 'slo'
        content = 'A' * 1024 + 'B' * 1024
        manifest = [
            {'size_bytes': 1024, 'path': '/segments/part1'},
            {'size_bytes': 1024, 'path': '/segments/part2'}]
        conn = self.conn_for_acct(mapping['account'])
        conn.put_container('segments')
        conn.put_container(container)
        conn.put_object('segments', 'part1', content[:1024])
        conn.put_object('segments', 'part2', content[1024:])
        conn.put_object(container, key,
                        json.dumps(manifest),
                        query_string='multipart-manifest=put')

        crawler.run_once()

        hdrs = target_conn.head_object(
            mapping['aws_bucket'] + 'segments', 'part1')
        self.assertEqual(1024, int(hdrs['content-length']))
        self.assertEqual(hashlib.md5(content[:1024]).hexdigest(),
                         hdrs['etag'])
        hdrs = target_conn.head_object(
            mapping['aws_bucket'] + 'segments', 'part2')
        self.assertEqual(1024, int(hdrs['content-length']))
        self.assertEqual(hashlib.md5(content[1024:]).hexdigest(),
                         hdrs['etag'])

        slo_etag = hashlib.md5(''.join([
            hashlib.md5(content[:1024]).hexdigest(),
            hashlib.md5(content[1024:]).hexdigest()])).hexdigest()
        hdrs = target_conn.head_object(
            mapping['aws_bucket'] + container, key)
        self.assertEqual(2048, int(hdrs['content-length']))
        self.assertEqual('"%s"' % slo_etag, hdrs['etag'])

        clear_swift_container(
            target_conn, mapping['aws_bucket'] + container)
        clear_swift_container(
            target_conn, mapping['aws_bucket'] + 'segments')
        clear_swift_container(conn, container)
        clear_swift_container(conn, 'segments')
        target_conn.delete_container(mapping['aws_bucket'] + container)
        target_conn.delete_container(mapping['aws_bucket'] + 'segments')
        conn.delete_container(container)
        conn.delete_container('segments')

    def test_s3_sync(self):
        s3_mapping = self.s3_sync_mapping()
        crawler = utils.get_container_crawler(s3_mapping)

        test_args = [
            (u'test_sync', u'testing archive put'),
            (u'unicod\u00e9', u'unicod\u00e9 blob')]
        for key, content in test_args:
            etag = self.local_swift(
                'put_object', s3_mapping['container'], key,
                content.encode('utf-8'))
            self.assertEqual(hashlib.md5(content.encode('utf-8')).hexdigest(),
                             etag)
            s3_key = s3_key_name(s3_mapping, key)

            crawler.run_once()
            head_resp = self.s3('head_object',
                                Bucket=s3_mapping['aws_bucket'], Key=s3_key)
            self.assertEqual('"%s"' % etag, head_resp['ETag'])

        self._assert_stats(
            s3_mapping,
            {'copied_objects': len(test_args),
             'bytes': sum([len(arg[1].encode('utf-8'))
                           for arg in test_args])})

    def test_s3_archive(self):
        s3_mapping = self.s3_archive_mapping()

        test_args = [
            (u'test_archive', u'testing archive put', {}),
            (u'unicod\u00e9', u'unicod\u00e9 blob', {}),
            ('funky-meta', 'blob', {'x-object-meta-key': u'un\xedcod\xeb'})]
        for key, content, headers in test_args:
            s3_key = s3_key_name(s3_mapping, key)
            expected_location = '%s;%s;%s/' % (
                s3_mapping['aws_endpoint'],
                s3_mapping['aws_identity'],
                s3_key[:-1 * (len(key) + 1)])

            def etag_func(key):
                hdrs = self.s3(
                    'head_object', Bucket=s3_mapping['aws_bucket'], Key=s3_key)
                return hdrs['ETag'][1:-1]

            self._test_archive(key, content, s3_mapping, etag_func,
                               expected_location, headers=headers)

        self._assert_stats(
            s3_mapping,
            {'copied_objects': len(test_args),
             'bytes': sum([len(arg[1].encode('utf-8')) for arg in test_args])})
        clear_s3_bucket(self.s3_client, s3_mapping['aws_bucket'])
        clear_swift_container(self.swift_src, s3_mapping['container'])

    def test_s3_archive_by_metadata(self):
        s3_mapping = self._find_mapping(
            lambda cont:
                cont.get('selection_criteria') is not None and
                cont.get('protocol') == 's3')
        crawler = utils.get_container_crawler(s3_mapping)
        test_data = [
            (u'test_archive', u'testing archive put',
             {'x-object-meta-msd': 'foo'}, False),
            (u'test_\u00ear', u'testing \u00eae put', {}, False),
            (u'west_archive', u'testing archive put',
             {'x-object-meta-msd': 'boo'}, True),
            (u'zest_archive', u'testing archive put',
             {'x-object-meta-msd': u'b\u00e1r'}, True),
            (u'jest_archive', u'testing archive put',
             {'x-object-meta-msd': 'baz'}, False),
        ]
        count_not_archived = 0
        for key, content, headers, sb_archived in test_data:
            etag = self.local_swift(
                'put_object', s3_mapping['container'], key,
                content.encode('utf-8'), headers=headers)
            self.assertEqual(
                hashlib.md5(content.encode('utf-8')).hexdigest(), etag)
            if not sb_archived:
                count_not_archived += 1

        crawler.run_once()
        swift_hdrs, listing = self.local_swift(
            'get_container', s3_mapping['container'])
        for key, content, headers, sb_archived in test_data:
            for entry in listing:
                if entry['name'] == key:
                    break
            if sb_archived:
                s3_key = s3_key_name(s3_mapping, key)
                expected_location = '%s;%s;%s/' % (
                    s3_mapping['aws_endpoint'],
                    s3_mapping['aws_bucket'],
                    s3_key[:-1 * (len(key) + 1)])
                self.assertEqual(
                    [expected_location],
                    entry['content_location'])
            else:
                self.assertEqual(None, entry.get('expected_location'))

        self._assert_stats(
            s3_mapping,
            {'copied_objects': len(test_data) - count_not_archived,
             'bytes': sum([len(arg[1].encode('utf-8'))
                           for arg in test_data if arg[3]])})
        clear_s3_bucket(self.s3_client, s3_mapping['aws_bucket'])
        clear_swift_container(self.swift_src, s3_mapping['container'])

    def test_swift_archive_by_metadata(self):
        mapping = self._find_mapping(
            lambda cont:
                cont.get('selection_criteria') is not None and
                cont.get('protocol') == 'swift')
        crawler = utils.get_container_crawler(mapping)
        test_data = [
            (u'test_archive', u'testing archive put',
             {'x-object-meta-msd': 'foo'}, False),
            (u'test_\u00ear', u'testing \u00eae put', {}, False),
            (u'west_archive', u'testing archive put',
             {'x-object-meta-msd': 'boo'}, True),
            (u'zest_archive', u'testing archive put',
             {'x-object-meta-msd': u'b\u00e1r'}, True),
            (u'jest_archive', u'testing archive put',
             {'x-object-meta-msd': 'baz'}, False),
            ('unicode meta archive', 'put content',
             {u'x-object-meta-un\u00edcode': u'v\u00e1l'}, True)
        ]
        count_not_archived = 0
        for key, content, headers, sb_archived in test_data:
            etag = self.local_swift(
                'put_object', mapping['container'], key,
                content.encode('utf-8'), headers=headers)
            self.assertEqual(
                hashlib.md5(content.encode('utf-8')).hexdigest(), etag)
            if not sb_archived:
                count_not_archived += 1

        crawler.run_once()
        swift_hdrs, listing = self.local_swift(
            'get_container', mapping['container'])
        for key, content, headers, sb_archived in test_data:
            for entry in listing:
                if entry['name'] == key:
                    break
            if sb_archived:
                expected_location = '%s;%s;%s' % (
                    mapping['aws_endpoint'],
                    mapping['aws_identity'],
                    mapping['aws_bucket'])
                self.assertEqual(
                    [expected_location],
                    entry['content_location'])
            else:
                self.assertEqual(None, entry.get('expected_location'))

        self._assert_stats(
            mapping,
            {'copied_objects': len(test_data) - count_not_archived,
             'bytes': sum([len(arg[1].encode('utf-8'))
                           for arg in test_data if arg[3]])})
        clear_swift_container(self.swift_dst, mapping['aws_bucket'])
        clear_swift_container(self.swift_src, mapping['container'])

    def test_swift_sync(self):
        mapping = self.swift_sync_mapping()
        crawler = utils.get_container_crawler(mapping)

        test_args = [
            (u'test_swift_sync', u'testing sync put'),
            (u'unicod\u00e9', u'unicod\u00e9 blob')]
        for key, content in test_args:
            etag = self.local_swift(
                'put_object', mapping['container'], key,
                content.encode('utf-8'))
            self.assertEqual(hashlib.md5(content.encode('utf-8')).hexdigest(),
                             etag)

            crawler.run_once()
            head_resp = self.remote_swift(
                'head_object', mapping['aws_bucket'], key)
            self.assertEqual(etag, head_resp['etag'])
        self._assert_stats(
            mapping,
            {'copied_objects': len(test_args),
             'bytes': sum([len(arg[1].encode('utf-8')) for arg in test_args])})
        clear_swift_container(self.swift_src, mapping['container'])
        clear_swift_container(self.swift_dst, mapping['aws_bucket'])

    def test_swift_propagate_expiration(self):
        mapping = dict(self.swift_sync_mapping())
        mapping['propagate_expiration'] = True
        obj = 'obj2expire'
        crawler = utils.get_container_crawler(mapping)
        etag = self.local_swift('put_object', mapping['container'],
                                obj, 'body of obj',
                                headers={'x-delete-after': '30'})
        self.assertEqual(hashlib.md5('body of obj').hexdigest(), etag)
        hdrs = self.local_swift('head_object', mapping['container'], obj)
        self.assertIn('x-delete-at', hdrs.keys())
        expected_exp = hdrs['x-delete-at']
        crawler.run_once()
        remote_hdrs = self.remote_swift('head_object', mapping['aws_bucket'],
                                        obj)
        self.assertIn('x-delete-at', remote_hdrs.keys())
        self.assertEqual(expected_exp, remote_hdrs['x-delete-at'])
        clear_swift_container(self.swift_src, mapping['container'])
        clear_swift_container(self.swift_dst, mapping['aws_bucket'])

    def test_keystone_sync_v3(self):
        mapping = self._find_mapping(
            lambda m: m['aws_endpoint'].endswith('v3'))
        crawler = utils.get_container_crawler(mapping)
        local_conn = self.conn_for_acct(mapping['account'])
        obj_name = 'sync-test-keystone-v3'
        body = 'party-hardy' * 512
        etag = local_conn.put_object(mapping['container'], obj_name, body)
        self.assertEqual(hashlib.md5(body).hexdigest(), etag)

        self.assertIn('KEY_', mapping['aws_account'])
        remote_conn = self.conn_for_acct_noshunt(mapping['aws_account'])
        self.assertIn('KEY_', remote_conn.url)

        crawler.run_once()
        _, listing = local_conn.get_container(mapping['container'])
        self.assertIn('swift', listing[0].get('content_location', []))
        got_headers, got_body = remote_conn.get_object(
            mapping['container'], obj_name)
        self.assertEqual(body, got_body)
        self._assert_stats(mapping, {'copied_objects': 1, 'bytes': len(body)})

    def test_keystone_sync_v2(self):
        mapping = self._find_mapping(
            lambda m: m['aws_bucket'] == 'keystone2')
        crawler = utils.get_container_crawler(mapping)
        local_conn = self.conn_for_acct(mapping['account'])
        obj_name = 'sync-test-keystone-v2'
        body = 'party-animl' * 512
        etag = local_conn.put_object(mapping['container'], obj_name, body)
        self.assertEqual(hashlib.md5(body).hexdigest(), etag)

        self.assertIn('KEY_', mapping['aws_account'])
        remote_conn = self.conn_for_acct_noshunt(mapping['aws_account'])
        self.assertIn('KEY_', remote_conn.url)

        crawler.run_once()
        _, listing = local_conn.get_container(mapping['container'])
        self.assertIn('swift', listing[0].get('content_location', []))
        _, got_body = remote_conn.get_object(
            mapping['container'], obj_name)
        self.assertEqual(body, got_body)
        self._assert_stats(mapping, {'copied_objects': 1, 'bytes': len(body)})

    def test_keystone_cross_account_v2(self):
        mapping = self._find_mapping(
            lambda m: m['aws_bucket'] == 'keystone2a')
        crawler = utils.get_container_crawler(mapping)
        local_conn = self.conn_for_acct(mapping['account'])
        obj_name = 'sync-test-keystone-v2-cross-acct'
        body = 'party-animl' * 512
        etag = local_conn.put_object(mapping['container'], obj_name, body)
        self.assertEqual(hashlib.md5(body).hexdigest(), etag)

        remote_conn = self.conn_for_acct_noshunt(mapping['remote_account'])
        crawler.run_once()

        _, got_body = remote_conn.get_object(mapping['container'], obj_name)
        self.assertEqual(body, got_body)

        # Make sure it actually got lifecycled
        hdrs, listing = local_conn.get_container(mapping['container'])
        list_item = [i for i in listing if i['name'] == obj_name][0]
        self.assertIn(';'.join((mapping['aws_endpoint'],
                                mapping['aws_identity'],
                                mapping['aws_bucket'])),
                      list_item['content_location'])
        self._assert_stats(
            mapping, {'copied_objects': 1, 'bytes': len(body)})

    def test_swift_archive(self):
        mapping = self.swift_archive_mapping()
        expected_location = swift_content_location(mapping)

        test_args = [
            (u'test_archive', u'testing archive put'),
            (u'unicod\u00e9', u'unicod\u00e9 blob')]

        def get_etag(key):
            hdrs = self.remote_swift('head_object', mapping['aws_bucket'], key)
            return hdrs['etag']

        for key, content in test_args:
            self._test_archive(key, content, mapping, get_etag,
                               expected_location)
        self._assert_stats(
            mapping,
            {'copied_objects': len(test_args),
             'bytes': sum([len(arg[1].encode('utf-8')) for arg in test_args])})

    def test_swift_archive_x_delete_after(self):
        mapping = self._find_mapping(
            lambda m: m['aws_bucket'] == 'bit-bucket')
        crawler = utils.get_container_crawler(mapping)
        conn = self.conn_for_acct(mapping['account'])
        conn.put_object(mapping['container'], 'del_after_test', 'B' * 1024)

        crawler.run_once()
        hdrs = self.remote_swift(
            'head_object', mapping['aws_bucket'], 'del_after_test')
        self.assertIn('x-delete-at', hdrs.keys())
        target_time = int(time.time() + mapping['remote_delete_after'])
        self.assertTrue(int(hdrs['x-delete-at']) <= target_time)

        self._assert_stats(mapping, {'copied_objects': 1, 'bytes': 1024})

    def test_swift_sync_container_metadata(self):
        mapping = dict(self._find_mapping(
            lambda m: m.get('sync_container_metadata')))
        mapping['sync_container_acl'] = True
        crawler = utils.get_container_crawler(mapping)
        conn = self.conn_for_acct(mapping['account'])
        test_metadata = {
            'x-container-write': 'test2:tester2',
            'x-container-read': 'test2',
            'x-container-meta-test1': 'test1value',
            'x-container-meta-test2': 'test2value',
            u'x-container-meta-tes\u00e9t': u'test\u262f metadata',
        }
        conn.put_container(
            # Note: copying dict because SwiftClient mutates headers
            mapping['container'], headers=dict(test_metadata))
        crawler.run_once()
        hdrs = self.remote_swift('head_container', mapping['aws_bucket'])
        for key in test_metadata:
            self.assertEqual(hdrs.get(key), test_metadata[key])

    def test_provider_s3_put_object_defaults(self):
        mapping = self.s3_sync_mapping()
        provider = create_provider(mapping, 1)

        # NOTE: as long as the response includes a Content-Length, the
        # SeekableFileLikeIter will bound reads to the content-length, even if
        # the iterable goes further
        swift_key = u'a-\u062a-b-c'
        content = 'this str has a length'
        resp = provider.put_object(swift_key, {
            'x-object-meta-foobie-bar': 'blam',
        }, content)
        self.assertTrue(resp.success)

        s3_key = s3_key_name(mapping, swift_key)
        resp = self.s3('get_object', Bucket=mapping['aws_bucket'], Key=s3_key)
        self.assertEqual(len(content), resp['ContentLength'])
        self.assertEqual('application/octet-stream', resp['ContentType'])
        self.assertEqual(content, resp['Body'].read())
        self.assertEqual('blam', resp['Metadata']['foobie-bar'])

    def test_provider_s3_put_object(self):
        mapping = self.s3_sync_mapping()
        provider = create_provider(mapping, 1)

        # NOTE: as long as the response includes a Content-Length, the
        # SeekableFileLikeIter will bound reads to the content-length, even if
        # the iterable goes further
        swift_key = u'a-\u062a-b-c'
        resp = provider.put_object(swift_key, {
            'content-length': 88,  # take an int for convenience
            'content-type': 'text/plain',
            'x-object-meta-foobie-bar': 'bam',
            'x-object-meta-unreadable-prefix': u'\x04w'.encode('utf8'),
            'x-object-meta-unreadable-suffix': u'h\x04'.encode('utf8'),
            'x-object-meta-lots-of-unprintable': 5 * '\x04'.encode('utf8'),
        }, repeat('a'))
        self.assertTrue(resp.success)

        s3_key = s3_key_name(mapping, swift_key)
        resp = self.s3('get_object', Bucket=mapping['aws_bucket'], Key=s3_key)
        self.assertEqual(88, resp['ContentLength'])
        self.assertEqual('text/plain', resp['ContentType'])
        self.assertEqual('a' * 88, resp['Body'].read())
        self.assertEqual('bam', resp['Metadata']['foobie-bar'])
        self.assertEqual('=?UTF-8?Q?=04w?=',
                         resp['Metadata']['unreadable-prefix'])
        self.assertEqual('=?UTF-8?Q?h=04?=',
                         resp['Metadata']['unreadable-suffix'])
        self.assertEqual('=?UTF-8?B?BAQEBAQ=?=',
                         resp['Metadata']['lots-of-unprintable'])

    def test_provider_s3_put_object_no_prefix(self):
        mapping = dict(self.s3_sync_mapping())
        mapping['custom_prefix'] = ''
        provider = create_provider(mapping, 1)

        # NOTE: as long as the response includes a Content-Length, the
        # SeekableFileLikeIter will bound reads to the content-length, even if
        # the iterable goes further
        swift_key = u'a-\u062a-b-c'
        resp = provider.put_object(swift_key, {
            'content-length': 87,  # take an int for convenience
            'content-type': 'text/plain',
            'x-object-meta-foobie-bar': 'bam',
        }, repeat('b'))
        self.assertTrue(resp.success)

        resp = self.s3('get_object', Bucket=mapping['aws_bucket'],
                       Key=swift_key)
        self.assertEqual(87, resp['ContentLength'])
        self.assertEqual('text/plain', resp['ContentType'])
        self.assertEqual('b' * 87, resp['Body'].read())
        self.assertEqual('bam', resp['Metadata']['foobie-bar'])

    def test_s3_archive_get(self):
        tests = [{'content': 's3 archive and get',
                  'key': 'test_s3_archive'},
                 {'content': '',
                  'key': 'test-empty'}]

        s3_mapping = self.s3_restore_mapping()
        provider = create_provider(s3_mapping, 1)
        for test in tests:
            content = test['content']
            key = test['key']
            provider.put_object(key, {}, content)

            hdrs = self.local_swift(
                'head_object', s3_mapping['container'], key)
            self.assertIn('server', hdrs)
            self.assertTrue(hdrs['server'].startswith('Jetty'))

            hdrs, body = self.local_swift(
                'get_object', s3_mapping['container'], key, content)
            self.assertEqual(hashlib.md5(content).hexdigest(), hdrs['etag'])
            swift_content = ''.join([chunk for chunk in body])
            self.assertEqual(content, swift_content)
            # There should be a "server" header, set to Jetty for S3Proxy
            self.assertEqual('Jetty(9.2.z-SNAPSHOT)', hdrs['server'])

            # the subsequent request should come back from Swift
            hdrs, body = self.local_swift(
                'get_object', s3_mapping['container'], key)
            swift_content = ''.join([chunk for chunk in body])
            self.assertEqual(content, swift_content)
            self.assertEqual(False, 'server' in hdrs)
            clear_s3_bucket(self.s3_client, s3_mapping['aws_bucket'])
            clear_swift_container(self.swift_src, s3_mapping['container'])

    def test_s3_archive_range_get(self):
        content = 's3 archive and get'
        key = 'test_s3_archive'
        s3_mapping = self.s3_restore_mapping()
        provider = create_provider(s3_mapping, 1)
        provider.put_object(key, {}, content)

        hdrs = self.local_swift(
            'head_object', s3_mapping['container'], key)
        self.assertIn('server', hdrs)
        self.assertTrue(hdrs['server'].startswith('Jetty'))

        hdrs, body = self.local_swift(
            'get_object', s3_mapping['container'], key, content,
            headers={'Range': 'bytes=0-5'})
        self.assertEqual(hashlib.md5(content).hexdigest(), hdrs['etag'])
        swift_content = ''.join([chunk for chunk in body])
        self.assertEqual(content[:6], swift_content)
        # There should be a "server" header, set to Jetty for S3Proxy
        self.assertEqual('Jetty(9.2.z-SNAPSHOT)', hdrs['server'])

        # the object should not be restored
        hdrs = self.local_swift(
            'head_object', s3_mapping['container'], key)
        self.assertTrue('server' in hdrs)
        clear_s3_bucket(self.s3_client, s3_mapping['aws_bucket'])
        clear_swift_container(self.swift_src, s3_mapping['container'])

    def test_s3_slo_sync(self):
        s3_mapping = self.s3_sync_mapping()
        crawler = utils.get_container_crawler(s3_mapping)
        segments_container = 's3_slo_segments'
        # TODO: use a unicode key here, but S3Proxy has a bug with unicode
        # characters.
        key = u'slo-object'
        s3_key = s3_key_name(s3_mapping, key)

        segments = [
            'A' * (5 * 2**20),
            'B' * (5 * 2**20),
            'C' * 1024
        ]
        manifest = [{'size_bytes': len(segments[i]),
                     'path': u'/%s/p\u00e1rt%d' % (segments_container, i)}
                    for i in range(len(segments))]

        self.local_swift('put_container', segments_container)
        for i in range(len(segments)):
            self.local_swift('put_object', segments_container,
                             u'p\u00e1rt%d' % i, segments[i])
        self.local_swift('put_object', s3_mapping['container'], key,
                         json.dumps(manifest),
                         query_string='multipart-manifest=put')
        crawler.run_once()

        head_resp = self.s3(
            'head_object', Bucket=s3_mapping['aws_bucket'], Key=s3_key)
        self.assertEqual(sum(map(len, segments)), head_resp['ContentLength'])
        expected_etag = '"%s-%d"' % (
            hashlib.md5(reduce(lambda x, y: x + hashlib.md5(y).digest(),
                               segments, '')).hexdigest(), len(segments))
        self.assertEqual(expected_etag, head_resp['ETag'])

        self._assert_stats(
            s3_mapping,
            {'copied_objects': 1,
             'bytes': sum([len(segment) for segment in segments])})
        clear_s3_bucket(self.s3_client, s3_mapping['aws_bucket'])
        clear_swift_container(self.swift_src, s3_mapping['container'])
        clear_swift_container(self.swift_src, segments_container)

    def test_s3_archive_slo_restore(self):
        # Satisfy the 5MB minimum MPU part size
        content = 'A' * (6 * 1024 * 1024)
        key = 'test_swift_archive'
        mapping = self.s3_restore_mapping()
        provider = create_provider(mapping, 1)
        provider.put_object(key, {}, content)
        s3_key = s3_key_name(mapping, key)
        manifest_key = s3_key_name
        prefix, account, container, _ = s3_key.split('/', 3)
        key_hash = hashlib.sha256(key).hexdigest()
        manifest_key = '/'.join([
            prefix, '.manifests', account, container,
            '%s.swift_slo_manifest' % (key_hash)])
        manifest = [
            {'bytes': 5 * 1024 * 1024,
             'name': '/segments/part1',
             'hash': hashlib.md5('A' * 5 * 2**20).hexdigest()},
            {'bytes': 1024 * 1024,
             'name': '/segments/part2',
             'hash': hashlib.md5('A' * 2**20).hexdigest()}]
        self.s3('put_object',
                Bucket=mapping['aws_bucket'],
                Key=manifest_key,
                Body=json.dumps(manifest))
        resp = self.s3('create_multipart_upload',
                       Bucket=mapping['aws_bucket'],
                       Key=s3_key,
                       Metadata={'x-static-large-object': 'True'})
        self.s3('upload_part',
                Bucket=mapping['aws_bucket'],
                Key=s3_key,
                PartNumber=1,
                UploadId=resp['UploadId'],
                Body=content[:(5 * 1024 * 1024)])
        self.s3('upload_part',
                Bucket=mapping['aws_bucket'],
                Key=s3_key,
                PartNumber=2,
                UploadId=resp['UploadId'],
                Body=content[(5 * 1024 * 1024):])
        self.s3('complete_multipart_upload',
                Bucket=mapping['aws_bucket'],
                Key=s3_key,
                UploadId=resp['UploadId'],
                MultipartUpload={
                    'Parts': [
                        {'PartNumber': 1,
                         'ETag': hashlib.md5(
                             content[:(5 * 1024 * 1024)]).hexdigest()},
                        {'PartNumber': 2,
                         'ETag': hashlib.md5(
                             content[(5 * 1024 * 1024):]).hexdigest()}]})

        hdrs, listing = self.local_swift('get_container', mapping['container'])
        self.assertEqual(0, int(hdrs['x-container-object-count']))
        for entry in listing:
            self.assertIn('content_location', entry)

        hdrs, body = self.local_swift(
            'get_object', mapping['container'], key)
        expected_etag = '%s-2' % (hashlib.md5(
            hashlib.md5(content[:(5 * 1024 * 1024)]).digest() +
            hashlib.md5(content[(5 * 1024 * 1024):]).digest()).hexdigest())
        self.assertEqual(expected_etag, hdrs['etag'])
        swift_content = ''.join(body)
        self.assertEqual(content, swift_content)
        self.assertEqual('True', hdrs['x-static-large-object'])

        # the subsequent request should come back from Swift
        hdrs, listing = self.local_swift('get_container', mapping['container'])
        self.assertEqual(1, int(hdrs['x-container-object-count']))
        # We get back an entry for the remote and the local object
        self.assertEqual(1, len(listing))
        self.assertEqual(2, len(listing[0]['content_location']))
        hdrs, body = self.local_swift('get_object', mapping['container'], key)
        swift_content = ''.join([chunk for chunk in body])
        self.assertEqual(content, swift_content)

        for k in hdrs.keys():
            self.assertFalse(k.startswith('Remote-'))
        clear_s3_bucket(self.s3_client, mapping['aws_bucket'])
        clear_swift_container(self.swift_src, mapping['container'])
        clear_swift_container(self.swift_src, 'segments')

    def test_swift_archive_get(self):
        content = 'swift archive and get'
        key = 'test_swift_archive'
        mapping = self.swift_restore_mapping()
        self.remote_swift('put_object', mapping['aws_bucket'], key, content)

        hdrs, listing = self.local_swift('get_container', mapping['container'])
        self.assertEqual(0, int(hdrs['x-container-object-count']))
        for entry in listing:
            self.assertIn('content_location', entry)
            self.assertEqual([swift_content_location(mapping)],
                             entry['content_location'])

        hdrs, body = self.local_swift(
            'get_object', mapping['container'], key, content)
        self.assertEqual(hashlib.md5(content).hexdigest(), hdrs['etag'])
        swift_content = ''.join([chunk for chunk in body])
        self.assertEqual(content, swift_content)

        # the subsequent request should come back from Swift
        hdrs, listing = self.local_swift('get_container', mapping['container'])
        self.assertEqual(1, int(hdrs['x-container-object-count']))
        # We get back an entry for the remote and the local object
        self.assertIn('content_location', listing[0])
        self.assertEqual([swift_content_location(mapping), 'swift'],
                         listing[0]['content_location'])
        hdrs, body = self.local_swift('get_object', mapping['container'], key)
        swift_content = ''.join([chunk for chunk in body])
        self.assertEqual(content, swift_content)
        clear_swift_container(self.swift_dst, mapping['aws_bucket'])
        clear_swift_container(self.swift_src, mapping['container'])

    def test_swift_archive_range_get(self):
        content = 'swift archive and get'
        key = 'test_swift_archive'
        mapping = self.swift_restore_mapping()
        self.remote_swift('put_object', mapping['aws_bucket'], key, content)

        hdrs, listing = self.local_swift('get_container', mapping['container'])
        self.assertEqual(0, int(hdrs['x-container-object-count']))
        for entry in listing:
            self.assertIn('content_location', entry)
            self.assertEqual([swift_content_location(mapping)],
                             entry['content_location'])

        hdrs, body = self.local_swift(
            'get_object', mapping['container'], key, content,
            headers={'Range': 'bytes=0-5'})
        self.assertEqual(hashlib.md5(content).hexdigest(), hdrs['etag'])
        swift_content = ''.join([chunk for chunk in body])
        self.assertEqual(content[:6], swift_content)

        # the object should not be restored
        hdrs, listing = self.local_swift('get_container', mapping['container'])
        self.assertEqual(0, int(hdrs['x-container-object-count']))

        clear_swift_container(self.swift_dst, mapping['aws_bucket'])
        clear_swift_container(self.swift_src, mapping['container'])

    def test_swift_archive_slo_w_remote_delete_at(self):
        content = 'A' * 2048
        key = 'test_swift_slo_del_after'
        mapping = self._find_mapping(
            lambda m: m['aws_bucket'] == 'bit-bucket')
        crawler = utils.get_container_crawler(mapping)
        manifest = [
            {'size_bytes': 1024, 'path': '/segments/part1'},
            {'size_bytes': 1024, 'path': '/segments/part2'}]
        early_target_time = int(time.time() + mapping['remote_delete_after'])
        early_seg_target_time = early_target_time + \
            mapping['remote_delete_after_addition']
        conn = self.conn_for_acct(mapping['account'])
        conn.put_container('segments')
        conn.put_object('segments', 'part1', content[:1024])
        conn.put_object('segments', 'part2', content[1024:])
        conn.put_object(mapping['container'], key,
                        json.dumps(manifest),
                        query_string='multipart-manifest=put')
        self.remote_swift(
            'put_container', '%s_segments' % mapping['aws_bucket'])

        crawler.run_once()

        hdrs = self.remote_swift('head_object', mapping['aws_bucket'], key)
        self.assertIn('x-delete-at', hdrs.keys())
        target_time = int(time.time() + mapping['remote_delete_after'])
        seg_target_time = target_time + mapping['remote_delete_after_addition']
        self.assertTrue(int(hdrs['x-delete-at']) <= target_time)
        self.assertTrue(int(hdrs['x-delete-at']) >= early_target_time)
        seg_container = mapping['aws_bucket'] + '_segments'
        hdrs = self.remote_swift('head_object', seg_container, 'part1')
        self.assertIn('x-delete-at', hdrs.keys())
        self.assertTrue(int(hdrs['x-delete-at']) <= seg_target_time)
        self.assertTrue(int(hdrs['x-delete-at']) >= early_seg_target_time)
        hdrs = self.remote_swift('head_object', seg_container, 'part2')
        self.assertIn('x-delete-at', hdrs.keys())
        self.assertTrue(int(hdrs['x-delete-at']) <= seg_target_time)
        self.assertTrue(int(hdrs['x-delete-at']) >= early_seg_target_time)
        # NOTE: we only record the SLO as being uploaded, not the segments
        self._assert_stats(mapping, {'copied_objects': 1, 'bytes': 2048})

    def test_s3_archive_slo_w_delete_segments(self):
        s3_mapping = dict(self.s3_archive_mapping())
        s3_mapping['retain_local_segments'] = False
        crawler = utils.get_container_crawler(s3_mapping)
        segments_container = 's3_slo_segments'
        key = 'test_s3_slo_del_segments'
        s3_key = s3_key_name(s3_mapping, key)

        segments = [
            'A' * (5 * 2 ** 20),
            'B' * (5 * 2 ** 20),
            'C' * 1024
        ]
        manifest = [{'size_bytes': len(segments[i]),
                     'path': u'/%s/p\u00e1rt%d' % (segments_container, i)}
                    for i in range(len(segments))]

        self.local_swift('put_container', segments_container)
        for i in range(len(segments)):
            self.local_swift('put_object', segments_container,
                             u'p\u00e1rt%d' % i, segments[i])
        self.local_swift('put_object', s3_mapping['container'], key,
                         json.dumps(manifest),
                         query_string='multipart-manifest=put')
        crawler.run_once()

        # sanity, object should be synced to S3
        head_resp = self.s3(
            'head_object', Bucket=s3_mapping['aws_bucket'], Key=s3_key)
        self.assertEqual(sum(map(len, segments)), head_resp['ContentLength'])
        expected_etag = '"%s-%d"' % (
            hashlib.md5(reduce(lambda x, y: x + hashlib.md5(y).digest(),
                               segments, '')).hexdigest(), len(segments))
        self.assertEqual(expected_etag, head_resp['ETag'])

        # manifest and segments should have been deleted locally
        conn_noshunt = self.conn_for_acct_noshunt(s3_mapping['account'])
        with self.assertRaises(swiftclient.exceptions.ClientException) as ctx:
            conn_noshunt.head_object(s3_mapping['container'], key)
        self.assertEqual(404, ctx.exception.http_status)

        for i in range(len(segments)):
            with self.assertRaises(
                    swiftclient.exceptions.ClientException) as ctx:
                conn_noshunt.head_object(segments_container,
                                         u'p\u00e1rt%d' % i)
            self.assertEqual(404, ctx.exception.http_status)

        self._assert_stats(
            s3_mapping,
            {'copied_objects': 1,
             'bytes': sum([len(segment) for segment in segments])})
        clear_s3_bucket(self.s3_client, s3_mapping['aws_bucket'])
        clear_swift_container(self.swift_src, s3_mapping['container'])
        clear_swift_container(self.swift_src, segments_container)

    def test_swift_archive_slo_w_delete_segments(self):
        content = 'A' * 2048
        key = 'test_swift_slo_del_segments'
        mapping = dict(self._find_mapping(
            lambda m: m['aws_bucket'] == 'bit-bucket'))
        mapping['retain_local_segments'] = False

        crawler = utils.get_container_crawler(mapping)
        manifest = [
            {'size_bytes': 1024, 'path': '/segments/part1'},
            {'size_bytes': 1024, 'path': '/segments/part2'}]

        conn = self.conn_for_acct(mapping['account'])
        conn.put_container('segments')
        conn.put_object('segments', 'part1', content[:1024])
        conn.put_object('segments', 'part2', content[1024:])
        conn.put_object(mapping['container'], key,
                        json.dumps(manifest),
                        query_string='multipart-manifest=put')
        self.remote_swift(
            'put_container', '%s_segments' % mapping['aws_bucket'])

        crawler.run_once()

        # sanity, manifest + segments should be synced to remote
        self.remote_swift('head_object', mapping['aws_bucket'], key)
        seg_container = mapping['aws_bucket'] + '_segments'
        self.remote_swift('head_object', seg_container, 'part1')
        self.remote_swift('head_object', seg_container, 'part2')

        # manifest and segments should have been deleted locally
        conn_noshunt = self.conn_for_acct_noshunt(mapping['account'])
        with self.assertRaises(swiftclient.exceptions.ClientException) as ctx:
            conn_noshunt.head_object(mapping['container'], key)
        self.assertEqual(404, ctx.exception.http_status)

        with self.assertRaises(swiftclient.exceptions.ClientException) as ctx:
            conn_noshunt.head_object('segments', 'part1')
        self.assertEqual(404, ctx.exception.http_status)

        with self.assertRaises(swiftclient.exceptions.ClientException) as ctx:
            conn_noshunt.head_object('segments', 'part2')
        self.assertEqual(404, ctx.exception.http_status)

        # NOTE: we only record the SLO as being uploaded, not the segments
        self._assert_stats(mapping, {'copied_objects': 1, 'bytes': 2048})
        mapping['retain_local_segments'] = True
        clear_swift_container(self.swift_src, mapping['container'])
        clear_swift_container(self.swift_dst, mapping['aws_bucket'])
        clear_swift_container(self.swift_src, 'segments')
        clear_swift_container(self.swift_dst,
                              '%s_segments' % mapping['aws_bucket'])

    def test_swift_archive_dlo_w_delete_segments(self):
        content = [chr(ord('A') + i) * 1024 for i in range(10)]
        key = 'test_swift_dlo_del_segments'
        mapping = dict(self._find_mapping(
            lambda m: m['aws_bucket'] == 'bit-bucket'))
        mapping['retain_local_segments'] = False
        crawler = utils.get_container_crawler(mapping)
        manifest = 'segments/test-dlo-parts-'
        self.local_swift('put_container', 'segments')
        for i in range(len(content)):
            self.local_swift(
                'put_object', 'segments', 'test-dlo-parts-%d' % i, content[i])
        self.local_swift(
            'put_object', mapping['container'], key, '',
            headers={'x-object-manifest': manifest})
        self.remote_swift('put_container',
                          '%s_segments' % mapping['aws_bucket'])

        crawler.run_once()

        headers, body = self.remote_swift(
            'get_object', mapping['aws_bucket'], key)
        self.assertEqual(''.join(content), body)
        dlo_etag = hashlib.md5(''.join(
            [hashlib.md5(content[i]).hexdigest() for i in range(len(content))]
        )).hexdigest()
        self.assertEqual('"%s"' % dlo_etag, headers['etag'])
        self.assertEqual(dlo_etag,
                         headers.get('x-object-meta-swift-source-dlo-etag'))

        self._assert_stats(mapping, {'copied_objects': 1, 'bytes': 10240})

        # manifest and segments should have been deleted locally
        conn_noshunt = self.conn_for_acct_noshunt(mapping['account'])
        with self.assertRaises(swiftclient.exceptions.ClientException) as ctx:
            conn_noshunt.head_object(mapping['container'], key)
        self.assertEqual(404, ctx.exception.http_status)

        for i in range(len(content)):
            with self.assertRaises(
                    swiftclient.exceptions.ClientException) as ctx:
                conn_noshunt.head_object('segments', 'test-dlo-parts-%d' % i)
            self.assertEqual(404, ctx.exception.http_status)

        mapping['retain_local_segments'] = True
        clear_swift_container(self.swift_src, mapping['container'])
        clear_swift_container(self.swift_dst, mapping['aws_bucket'])
        clear_swift_container(self.swift_src, 'segments')
        clear_swift_container(self.swift_dst,
                              '%s_segments' % mapping['aws_bucket'])

    def test_swift_archive_slo_restore(self):
        content = 'A' * 2048
        key = 'test_swift_archive'
        mapping = self.swift_restore_mapping()
        manifest = [
            {'size_bytes': 1024, 'path': '/segments/part1'},
            {'size_bytes': 1024, 'path': '/segments/part2'}]
        self.remote_swift('put_container', 'segments')
        self.remote_swift('put_object', 'segments', 'part1', content[:1024])
        self.remote_swift('put_object', 'segments', 'part2', content[1024:])
        self.remote_swift('put_object', mapping['aws_bucket'], key,
                          json.dumps(manifest),
                          query_string='multipart-manifest=put')

        hdrs, listing = self.local_swift('get_container', mapping['container'])
        self.assertEqual(0, int(hdrs['x-container-object-count']))
        for entry in listing:
            self.assertIn('content_location', entry)
            self.assertEqual([swift_content_location(mapping)],
                             entry['content_location'])

        slo_etag = hashlib.md5(''.join([
            hashlib.md5(content[:1024]).hexdigest(),
            hashlib.md5(content[1024:]).hexdigest()])).hexdigest()
        hdrs, body = self.local_swift(
            'get_object', mapping['container'], key, content)
        self.assertEqual('"%s"' % slo_etag, hdrs['etag'])
        swift_content = ''.join([chunk for chunk in body])
        self.assertEqual(content, swift_content)
        self.assertEqual('True', hdrs['x-static-large-object'])

        # the subsequent request should come back from Swift
        hdrs, listing = self.local_swift('get_container', mapping['container'])
        self.assertEqual(1, int(hdrs['x-container-object-count']))
        # We get back an entry for the remote and the local object
        self.assertIn('content_location', listing[0])
        self.assertEqual([swift_content_location(mapping), 'swift'],
                         listing[0]['content_location'])
        hdrs, body = self.local_swift('get_object', mapping['container'], key)
        swift_content = ''.join([chunk for chunk in body])
        self.assertEqual(content, swift_content)

        for k in hdrs.keys():
            self.assertFalse(k.startswith('Remote-'))
        clear_swift_container(self.swift_dst, mapping['aws_bucket'])
        clear_swift_container(self.swift_dst, 'segments')
        clear_swift_container(self.swift_src, mapping['container'])
        clear_swift_container(self.swift_src, 'segments')

    def test_swift_slo_sync(self):
        content = 'A' * 2048
        key = 'test_slo'
        mapping = self.swift_sync_mapping()
        crawler = utils.get_container_crawler(mapping)
        manifest = [
            {'size_bytes': 1024, 'path': '/segments/part1'},
            {'size_bytes': 1024, 'path': '/segments/part2'}]
        self.local_swift('put_container', 'segments')
        self.local_swift('put_object', 'segments', 'part1', content[:1024])
        self.local_swift('put_object', 'segments', 'part2', content[1024:])
        self.local_swift('put_object', mapping['container'], key,
                         json.dumps(manifest),
                         query_string='multipart-manifest=put')
        self.remote_swift('put_container',
                          '%s_segments' % mapping['aws_bucket'])

        crawler.run_once()
        headers, body = self.remote_swift(
            'get_object', mapping['aws_bucket'], key)
        self.assertEqual(body, content)
        self.assertEqual(
            '"%s"' % hashlib.md5('%s%s' % (
                hashlib.md5(content[:1024]).hexdigest(),
                hashlib.md5(content[1024:]).hexdigest())).hexdigest(),
            headers['etag'])
        # NOTE: we only record the SLO as being uploaded, not the segments
        self._assert_stats(
            mapping, {'copied_objects': 1, 'bytes': len(content)})
        for conn, container in [(self.swift_src, mapping['container']),
                                (self.swift_dst, mapping['aws_bucket'])]:
            clear_swift_container(conn, container)
            clear_swift_container(conn, container + '_segments')

    def test_swift_slo_sync_resize_segments(self):
        content = 'A' * 1024 + 'B' * 1024 + 'C' * 512
        key = 'test_slo'
        mapping = dict(self.swift_sync_mapping())
        mapping['min_segment_size'] = 2000
        mapping['convert_dlo'] = True
        crawler = utils.get_container_crawler(mapping)
        manifest = [
            {'size_bytes': 1024, 'path': '/segments/part1'},
            {'size_bytes': 1024, 'path': '/segments/part2'},
            {'size_bytes': 512, 'path': '/segments/part3'}]
        self.local_swift('put_container', 'segments')
        self.local_swift('put_object', 'segments', 'part1', content[:1024])
        self.local_swift('put_object', 'segments', 'part2', content[1024:2048])
        self.local_swift('put_object', 'segments', 'part3', content[2048:])
        self.local_swift('put_object', mapping['container'], key,
                         json.dumps(manifest),
                         query_string='multipart-manifest=put')
        self.remote_swift('put_container',
                          '%s_segments' % mapping['aws_bucket'])

        crawler.run_once()

        headers, body = self.remote_swift(
            'get_object', mapping['aws_bucket'], key)
        self.assertEqual(len(content), int(headers['content-length']))
        self.assertEqual(body, content)
        self.assertEqual(
            '"%s"' % hashlib.md5('%s%s' % (
                hashlib.md5(content[:2048]).hexdigest(),
                hashlib.md5(content[2048:]).hexdigest())).hexdigest(),
            headers['etag'])
        local_headers = self.local_swift(
            'head_object', mapping['container'], key,
            query_string='multipart-manifest=get')
        self.assertEqual(
            headers[SWIFT_USER_META_PREFIX + SLO_ETAG_FIELD],
            local_headers['etag'])
        # NOTE: we only record the SLO as being uploaded, not the segments
        self._assert_stats(mapping, {'copied_objects': 1, 'bytes': 2560})

        # Reupload should not result in any uploads
        crawler = utils.get_container_crawler(mapping)
        crawler.run_once()

        self._assert_stats(mapping, {'copied_objects': 0})

        clear_swift_container(self.swift_src, mapping['container'])
        clear_swift_container(self.swift_dst, mapping['aws_bucket'])
        clear_swift_container(self.swift_src, 'segments')
        clear_swift_container(
            self.swift_dst, '%s_segments' % mapping['aws_bucket'])

    def test_swift_slo_sync_resize_segments_new_meta(self):
        content = 'A' * 1024 + 'B' * 1024 + 'C' * 512
        key = u'test_sl\xef'
        mapping = dict(self.swift_sync_mapping())
        mapping['min_segment_size'] = 2000
        mapping['convert_dlo'] = True
        crawler = utils.get_container_crawler(mapping)
        manifest = [
            {'size_bytes': 1024, 'path': u'/segments/p\xe2rt1'},
            {'size_bytes': 1024, 'path': u'/segments/p\xe2rt2'},
            {'size_bytes': 512, 'path': u'/segments/p\xe2rt3'}]
        self.local_swift('put_container', 'segments')
        self.local_swift('put_object', 'segments', u'p\xe2rt1', content[:1024])
        self.local_swift(
            'put_object', 'segments', u'p\xe2rt2', content[1024:2048])
        self.local_swift('put_object', 'segments', u'p\xe2rt3', content[2048:])
        self.local_swift('put_object', mapping['container'], key,
                         json.dumps(manifest),
                         query_string='multipart-manifest=put')
        self.remote_swift('put_container',
                          '%s_segments' % mapping['aws_bucket'])

        crawler.run_once()
        remote_headers = self.remote_swift(
            'head_object', mapping['aws_bucket'], key,
            query_string='multipart-manifest=get')
        local_headers = self.local_swift(
            'head_object', mapping['container'], key,
            query_string='multipart-manifest=get')
        self.assertEqual(
            remote_headers[SWIFT_USER_META_PREFIX + SLO_ETAG_FIELD],
            local_headers['etag'])

        # NOTE: we only record the SLO as being uploaded, not the segments
        self._assert_stats(mapping, {'copied_objects': 1, 'bytes': 2560})

        source_conn = self.conn_for_acct_noshunt(mapping['account'])
        source_conn.post_object(mapping['container'], key,
                                {u'x-object-meta-fo\xef': u'v\xe2lue'})

        crawler = utils.get_container_crawler(mapping)
        crawler.run_once()

        headers = self.remote_swift('head_object', mapping['aws_bucket'], key)
        self.assertEqual(u'v\xe2lue', headers.get(u'x-object-meta-fo\xef'))

        self._assert_stats(mapping, {'copied_objects': 0})

        clear_swift_container(self.swift_src, mapping['container'])
        clear_swift_container(self.swift_dst, mapping['aws_bucket'])
        clear_swift_container(self.swift_src, 'segments')
        clear_swift_container(
            self.swift_dst, '%s_segments' % mapping['aws_bucket'])

    def test_swift_slo_sync_new_meta(self):
        content = 'A' * 1024 + 'B' * 1024
        key = 'test_slo'
        mapping = self.swift_sync_mapping()
        crawler = utils.get_container_crawler(mapping)
        manifest = [
            {'size_bytes': 1024, 'path': '/segments/part1'},
            {'size_bytes': 1024, 'path': '/segments/part2'}]
        self.local_swift('put_container', 'segments')
        self.local_swift('put_object', 'segments', 'part1', content[:1024])
        self.local_swift('put_object', 'segments', 'part2', content[1024:])
        self.local_swift('put_object', mapping['container'], key,
                         json.dumps(manifest),
                         query_string='multipart-manifest=put')
        self.remote_swift('put_container',
                          '%s_segments' % mapping['aws_bucket'])

        crawler.run_once()
        # record the last-modified date, so we can check that segments are not
        # re-uploaded
        remote_objects = [
            (mapping['aws_bucket'], key),
            ('%s_segments' % mapping['aws_bucket'], 'part1'),
            ('%s_segments' % mapping['aws_bucket'], 'part2')]
        old_headers = {}
        for cont, obj in remote_objects:
            old_headers[(cont, obj)] = self.remote_swift(
                'head_object', cont, obj,
                query_string='multipart-manifest=get')

        # NOTE: segments are currently excluded from the total count
        self._assert_stats(
            mapping, {'copied_objects': 1, 'bytes': len(content)})

        source_conn = self.conn_for_acct_noshunt(mapping['account'])
        source_conn.post_object(
            mapping['container'], key,
            headers={'x-object-meta-test-hdr': 'new header'})
        crawler.run_once()

        headers = self.remote_swift(
            'head_object', mapping['aws_bucket'], key)
        self.assertEqual('new header', headers['x-object-meta-test-hdr'])
        for cont, obj in remote_objects:
            hdrs = self.remote_swift(
                'head_object', cont, obj,
                query_string='multipart-manifest=get')
            if cont == mapping['aws_bucket']:
                self.assertTrue(
                    float(old_headers[(cont, obj)]['x-timestamp']) <
                    float(hdrs['x-timestamp']))
            else:
                self.assertEqual(
                    old_headers[(cont, obj)]['x-timestamp'],
                    hdrs['x-timestamp'])

        self._assert_stats(mapping, {'copied_objects': 0, 'bytes': 0})
        clear_swift_container(self.swift_src, mapping['container'])
        clear_swift_container(self.swift_dst, mapping['aws_bucket'])
        clear_swift_container(self.swift_src, 'segments')
        clear_swift_container(
            self.swift_dst, '%s_segments' % mapping['aws_bucket'])

    def test_swift_sync_same_segments_slo(self):
        '''Uploading the same SLO manifest should not duplicate segments'''
        content = 'A' * 1024 + 'B' * 1024
        key = 'test_slo'
        mapping = self.swift_sync_mapping()
        crawler = utils.get_container_crawler(mapping)
        manifest = [
            {'size_bytes': 1024, 'path': '/segments/part1'},
            {'size_bytes': 1024, 'path': '/segments/part2'}]
        self.local_swift('put_container', 'segments')
        self.local_swift('put_object', 'segments', 'part1', content[:1024])
        self.local_swift('put_object', 'segments', 'part2', content[1024:])
        self.local_swift('put_object', mapping['container'], key,
                         json.dumps(manifest),
                         query_string='multipart-manifest=put')
        self.remote_swift('put_container',
                          '%s_segments' % mapping['aws_bucket'])

        crawler.run_once()
        # record the last-modified date, so we can check that segments are not
        # re-uploaded
        remote_objects = [
            ('%s_segments' % mapping['aws_bucket'], 'part1'),
            ('%s_segments' % mapping['aws_bucket'], 'part2')]
        old_headers = {}
        for cont, obj in remote_objects:
            old_headers[(cont, obj)] = self.remote_swift(
                'head_object', cont, obj)

        new_slo = 'test_slo2'
        self.local_swift(
            'put_object', mapping['container'], new_slo, json.dumps(manifest),
            query_string='multipart-manifest=put')

        crawler.run_once()
        for cont, obj in remote_objects:
            hdrs = self.remote_swift('head_object', cont, obj)
            self.assertEqual(
                old_headers[(cont, obj)]['x-timestamp'], hdrs['x-timestamp'])
        clear_swift_container(self.swift_src, mapping['container'])
        clear_swift_container(self.swift_dst, mapping['aws_bucket'])
        clear_swift_container(self.swift_src, 'segments')
        clear_swift_container(
            self.swift_dst, '%s_segments' % mapping['aws_bucket'])

    def test_swift_slo_same_container_segments(self):
        segment_regex = '\w+/\d+.\d+/\d+$'
        content = 'A' * 1024 + 'B' * 1024
        key = 'test_slo'
        mapping = dict(self.swift_sync_mapping())
        mapping['exclude_pattern'] = segment_regex
        crawler = utils.get_container_crawler(mapping, log_level='debug')
        manifest = [
            {'size_bytes': 1024, 'path': '/%s/%s/123456.67/00001' %
                (mapping['container'], key)},
            {'size_bytes': 1024, 'path': '/%s/%s/123456.789/00002' %
                (mapping['container'], key)}]
        self.local_swift('put_object', mapping['container'],
                         manifest[0]['path'].split('/', 2)[2],
                         content[:1024])
        self.local_swift('put_object', mapping['container'],
                         manifest[1]['path'].split('/', 2)[2],
                         content[1024:])
        self.local_swift('put_object', mapping['container'], key,
                         json.dumps(manifest),
                         query_string='multipart-manifest=put')
        self.remote_swift('put_container',
                          '%s_segments' % mapping['aws_bucket'])

        crawler.run_once()
        headers, body = self.remote_swift(
            'get_object', mapping['aws_bucket'], key)
        self.assertEqual(body, content)
        self.assertEqual(
            '"%s"' % hashlib.md5('%s%s' % (
                hashlib.md5(content[:1024]).hexdigest(),
                hashlib.md5(content[1024:]).hexdigest())).hexdigest(),
            headers['etag'])

        _, listing = self.remote_swift('get_container', mapping['aws_bucket'])
        self.assertEqual(1, len(listing))

        # NOTE: we only record the SLO as being uploaded, not the segments. The
        # segments should be ignored in this case.
        self._assert_stats(mapping, {'copied_objects': 1, 'bytes': 2048})
        clear_swift_container(self.swift_src, mapping['container'])
        clear_swift_container(self.swift_dst, mapping['aws_bucket'])

    def test_swift_dlo_sync(self):
        content = [chr(ord('A') + i) * 1024 for i in range(10)]
        key = 'test_dlo'
        mapping = self.swift_sync_mapping()
        crawler = utils.get_container_crawler(mapping)
        manifest = 'segments/test-dlo-parts-'
        self.local_swift('put_container', 'segments')
        for i in range(len(content)):
            self.local_swift(
                'put_object', 'segments', 'test-dlo-parts-%d' % i, content[i])
        self.local_swift(
            'put_object', mapping['container'], key, '',
            headers={'x-object-manifest': manifest})
        self.remote_swift('put_container',
                          '%s_segments' % mapping['aws_bucket'])

        crawler.run_once()

        headers, body = self.remote_swift(
            'get_object', mapping['aws_bucket'], key)
        self.assertEqual(''.join(content), body)
        dlo_etag = hashlib.md5(''.join(
            [hashlib.md5(content[i]).hexdigest() for i in range(len(content))]
        )).hexdigest()
        self.assertEqual('"%s"' % dlo_etag, headers['etag'])
        self.assertEqual(dlo_etag,
                         headers.get('x-object-meta-swift-source-dlo-etag'))

        self._assert_stats(
            mapping,
            {'copied_objects': 1,
             'bytes': sum(map(len, content))})

        # Re-processing the object should be a NOOP
        crawler = utils.get_container_crawler(mapping)
        crawler.run_once()
        self._assert_stats(mapping, {'copied_objects': 0})

        clear_swift_container(self.swift_src, mapping['container'])
        clear_swift_container(self.swift_dst, mapping['aws_bucket'])
        clear_swift_container(self.swift_src, 'segments')
        clear_swift_container(
            self.swift_dst, '%s_segments' % mapping['aws_bucket'])

    def test_swift_dlo_sync_resize_segments(self):
        content = [chr(ord('A') + i) * 1024 for i in range(10)]
        key = u'test_dl\xf4'
        mapping = dict(self.swift_sync_mapping())
        # This should result in 2048 byte segments
        mapping['min_segment_size'] = 2000
        mapping['convert_dlo'] = True
        crawler = utils.get_container_crawler(mapping)
        prefix = u'test-dlo-p\xe3rts-'
        manifest = u'segments/%s' % prefix
        self.local_swift('put_container', 'segments')
        for i in range(len(content)):
            self.local_swift(
                'put_object', 'segments', u'%s-%d' % (prefix, i), content[i])
        self.local_swift(
            'put_object', mapping['container'], key, '',
            headers={'x-object-manifest': manifest})
        self.remote_swift('put_container',
                          '%s_segments' % mapping['aws_bucket'])

        crawler.run_once()

        headers, body = self.remote_swift(
            'get_object', mapping['aws_bucket'], key)
        self.assertEqual(''.join(content), body)
        dlo_etag = hashlib.md5(''.join(
            [hashlib.md5(content[i]).hexdigest() for i in range(len(content))]
        )).hexdigest()
        self.assertEqual(dlo_etag,
                         headers.get(SWIFT_USER_META_PREFIX + DLO_ETAG_FIELD))
        slo_etag = hashlib.md5(''.join(
            [hashlib.md5(content[i] + content[i + 1]).hexdigest()
             for i in range(0, len(content), 2)])).hexdigest()
        self.assertEqual('"%s"' % slo_etag, headers['etag'])

        _, manifest_blob = self.remote_swift(
            'get_object', mapping['aws_bucket'], key,
            query_string='multipart-manifest=get')
        manifest = json.loads(manifest_blob)
        self.assertEqual(len(content) / 2, len(manifest))
        for index, segment in enumerate(manifest):
            content_etag = hashlib.md5(content[index * 2] +
                                       content[index * 2 + 1]).hexdigest()
            self.assertEqual(content_etag, segment['hash'])

        self._assert_stats(mapping, {'copied_objects': 1,
                                     'bytes': sum(map(len, content))})

        # Re-processing the object should be a NOOP
        crawler = utils.get_container_crawler(mapping)
        crawler.run_once()
        self._assert_stats(mapping, {'copied_objects': 0})

        clear_swift_container(self.swift_src, mapping['container'])
        clear_swift_container(self.swift_dst, mapping['aws_bucket'])
        clear_swift_container(self.swift_src, 'segments')
        clear_swift_container(
            self.swift_dst, '%s_segments' % mapping['aws_bucket'])

    def test_swift_dlo_sync_resize_segments_new_meta(self):
        content = [chr(ord('A') + i) * 1024 for i in range(10)]
        key = u'test_dl\xf4'
        mapping = dict(self.swift_sync_mapping())
        # This should result in 2048 byte segments
        mapping['min_segment_size'] = 2000
        mapping['convert_dlo'] = True
        prefix = u'test-dlo-p\xe3rts-'
        segments_container = u's\xe3gments'
        manifest = u'%s/%s' % (segments_container, prefix)
        self.local_swift('put_container', segments_container)
        for i in range(len(content)):
            self.local_swift(
                'put_object', segments_container,
                u'%s-%d' % (prefix, i), content[i])
        self.local_swift(
            'put_object', mapping['container'], key, '',
            headers={'x-object-manifest': manifest})
        self.remote_swift('put_container',
                          '%s_segments' % mapping['aws_bucket'])

        crawler = utils.get_container_crawler(mapping)
        crawler.run_once()

        headers, body = self.remote_swift(
            'get_object', mapping['aws_bucket'], key)
        self.assertEqual(''.join(content), body)
        dlo_etag = hashlib.md5(''.join(
            [hashlib.md5(content[i]).hexdigest() for i in range(len(content))]
        )).hexdigest()
        self.assertEqual(dlo_etag,
                         headers.get(SWIFT_USER_META_PREFIX + DLO_ETAG_FIELD))
        self._assert_stats(mapping, {'copied_objects': 1,
                                     'bytes': sum(map(len, content))})

        # Updating the metadata on the object should be a NOOP
        source_conn = self.conn_for_acct_noshunt(mapping['account'])
        source_conn.post_object(mapping['container'], key,
                                {u'x-object-meta-fo\xef': u'v\xe2lue',
                                 'x-object-manifest': manifest})

        crawler = utils.get_container_crawler(mapping, log_level='debug')
        crawler.run_once()

        headers = self.remote_swift('head_object', mapping['aws_bucket'], key)
        self.assertEqual(u'v\xe2lue', headers.get(u'x-object-meta-fo\xef'))

        self._assert_stats(mapping, {'copied_objects': 0})

        clear_swift_container(self.swift_src, mapping['container'])
        clear_swift_container(self.swift_dst, mapping['aws_bucket'])
        clear_swift_container(self.swift_src, segments_container)
        clear_swift_container(
            self.swift_dst, '%s_segments' % mapping['aws_bucket'])

    def test_swift_dlo_metadata_update(self):
        content = [chr(ord('A') + i) * 1024 for i in range(10)]
        key = 'test_dlo'
        mapping = self.swift_sync_mapping()
        crawler = utils.get_container_crawler(mapping)
        segments_container = u's\xe4gments'
        prefix = u'test-dl\xf3-parts-'
        manifest = u'%s/%s' % (segments_container, prefix)
        self.local_swift('put_container', segments_container)
        for i in range(len(content)):
            self.local_swift(
                'put_object', segments_container, u'%s%d' % (prefix, i),
                content[i])
        self.local_swift(
            'put_object', mapping['container'], key, '',
            headers={'x-object-manifest': manifest})
        self.remote_swift('put_container',
                          '%s_segments' % mapping['aws_bucket'])

        crawler.run_once()

        _, listing = self.remote_swift('get_container',
                                       '%s_segments' % mapping['aws_bucket'],
                                       query_string=u'prefix=%s' % prefix)

        source_conn = self.conn_for_acct_noshunt(mapping['account'])
        source_conn.post_object(mapping['container'], key,
                                headers={u'x-object-meta-fo\xf3': u'fo\xf3',
                                         'x-object-manifest': manifest})
        headers = self.local_swift('head_object', mapping['container'], key)
        self.assertEqual(u'fo\xf3', headers.get(u'x-object-meta-fo\xf3'))
        self.assertEqual(manifest, headers.get('x-object-manifest'))

        self._assert_stats(mapping, {'copied_objects': 1,
                                     'bytes': sum(map(len, content))})

        crawler.run_once()

        _, new_listing = self.remote_swift(
            'get_container', '%s_segments' % mapping['aws_bucket'],
            query_string=u'prefix=%s' % prefix)
        for i in range(len(new_listing)):
            self.assertEqual(listing[i]['last_modified'],
                             new_listing[i]['last_modified'])

        headers = self.remote_swift('head_object', mapping['aws_bucket'], key)
        dlo_etag = hashlib.md5(''.join(
            [hashlib.md5(content[i]).hexdigest() for i in range(len(content))]
        )).hexdigest()
        self.assertEqual('"%s"' % dlo_etag, headers['etag'])
        self.assertEqual(dlo_etag,
                         headers.get('x-object-meta-swift-source-dlo-etag'))
        self.assertEqual(u'fo\xf3', headers.get(u'x-object-meta-fo\xf3'))

        self._assert_stats(mapping, {'copied_objects': 0,
                                     'bytes': 0})
        clear_swift_container(self.swift_src, mapping['container'])
        clear_swift_container(self.swift_dst, mapping['aws_bucket'])
        clear_swift_container(self.swift_src, segments_container)
        clear_swift_container(
            self.swift_dst, '%s_segments' % mapping['aws_bucket'])

    def test_dlo_slo_conversion(self):
        content = [chr(ord('A') + i) * 1024 for i in range(10)]
        key = u'test_dl\xf3'
        mapping = dict(self.swift_sync_mapping())
        mapping['convert_dlo'] = True
        crawler = utils.get_container_crawler(mapping)
        prefix = u'test-dlo-slo-p\xe0rts-'
        manifest = u'segments/%s' % prefix
        segments_container = 'segments'
        self.local_swift('put_container', segments_container)
        for i in range(len(content)):
            self.local_swift(
                'put_object', segments_container,
                u'%s-%d' % (prefix, i), content[i])
        self.local_swift(
            'put_object', mapping['container'], key, '',
            headers={'x-object-manifest': manifest})
        self.remote_swift('put_container',
                          '%s_segments' % mapping['aws_bucket'])

        crawler.run_once()

        headers, body = self.remote_swift(
            'get_object', mapping['aws_bucket'], key)
        self.assertEqual(''.join(content), body)
        dlo_etag = hashlib.md5(''.join(
            [hashlib.md5(content[i]).hexdigest() for i in range(len(content))]
        )).hexdigest()
        self.assertEqual('"%s"' % dlo_etag, headers['etag'])
        self.assertEqual(dlo_etag,
                         headers.get('x-object-meta-swift-source-dlo-etag'))
        self.assertTrue(headers.get('x-static-large-object'))

        self._assert_stats(mapping, {'copied_objects': 1,
                                     'bytes': sum(map(len, content))})

        clear_swift_container(self.swift_src, mapping['container'])
        clear_swift_container(self.swift_dst, mapping['aws_bucket'])
        clear_swift_container(self.swift_src, segments_container)
        clear_swift_container(self.swift_dst,
                              '%s_segments' % mapping['aws_bucket'])

    def test_dlo_slo_conversion_meta_update(self):
        content = [chr(ord('A') + i) * 1024 for i in range(10)]
        key = u'test_dl\xf3-to-sl\xf3'
        mapping = dict(self.swift_sync_mapping())
        mapping['convert_dlo'] = True
        crawler = utils.get_container_crawler(mapping)
        prefix = u'test-dlo-slo-p\xe0rts-'
        manifest = u'segments/%s' % prefix
        segments_container = 'segments'
        self.local_swift('put_container', segments_container)
        for i in range(len(content)):
            self.local_swift(
                'put_object', segments_container,
                u'%s-%d' % (prefix, i), content[i])
        self.local_swift(
            'put_object', mapping['container'], key, '',
            headers={'x-object-manifest': manifest})
        self.remote_swift('put_container',
                          '%s_segments' % mapping['aws_bucket'])

        crawler.run_once()

        self._assert_stats(mapping, {'copied_objects': 1,
                                     'bytes': sum(map(len, content))})
        source_conn = self.conn_for_acct_noshunt(mapping['account'])
        source_conn.post_object(mapping['container'], key,
                                {u'x-object-meta-new-valu\xe3': u'valu\xe3',
                                 u'x-object-manifest': manifest})

        # Re-processing the object should be a POST (which does not count as
        # copied).
        crawler.run_once()

        headers = self.remote_swift('head_object', mapping['aws_bucket'], key)
        self.assertEqual(u'valu\xe3',
                         headers.get(u'x-object-meta-new-valu\xe3'))
        self.assertTrue(headers.get('x-static-large-object'))

        self._assert_stats(mapping, {'copied_objects': 0})

        clear_swift_container(self.swift_src, mapping['container'])
        clear_swift_container(self.swift_dst, mapping['aws_bucket'])
        clear_swift_container(self.swift_src, segments_container)
        clear_swift_container(self.swift_dst,
                              '%s_segments' % mapping['aws_bucket'])

    def test_dlo_slo_conversion_reupload(self):
        content = [chr(ord('A') + i) * 1024 for i in range(10)]
        key = u'test_dl\xf3'
        mapping = dict(self.swift_sync_mapping())
        mapping['convert_dlo'] = True
        crawler = utils.get_container_crawler(mapping)
        prefix = u'test-dlo-slo-p\xe0rts-'
        manifest = u'segments/%s' % prefix
        segments_container = 'segments'
        self.local_swift('put_container', segments_container)
        for i in range(len(content)):
            self.local_swift(
                'put_object', segments_container,
                u'%s-%d' % (prefix, i), content[i])
        self.local_swift(
            'put_object', mapping['container'], key, '',
            headers={'x-object-manifest': manifest})
        self.remote_swift('put_container',
                          '%s_segments' % mapping['aws_bucket'])

        crawler.run_once()

        headers, body = self.remote_swift(
            'get_object', mapping['aws_bucket'], key)
        self.assertEqual(''.join(content), body)
        dlo_etag = hashlib.md5(''.join(
            [hashlib.md5(content[i]).hexdigest() for i in range(len(content))]
        )).hexdigest()
        self.assertEqual('"%s"' % dlo_etag, headers['etag'])
        self.assertEqual(dlo_etag,
                         headers.get('x-object-meta-swift-source-dlo-etag'))
        self.assertTrue(headers.get('x-static-large-object'))

        self._assert_stats(mapping, {'copied_objects': 1,
                                     'bytes': sum(map(len, content))})

        # Re-processing the object should be a NOOP
        # NOTE: crawler is recreated so that we can reset the status and the
        # object gets re-processed.
        crawler = utils.get_container_crawler(mapping)
        crawler.run_once()
        self._assert_stats(mapping, {'copied_objects': 0})

        clear_swift_container(self.swift_src, mapping['container'])
        clear_swift_container(self.swift_dst, mapping['aws_bucket'])
        clear_swift_container(self.swift_src, segments_container)
        clear_swift_container(self.swift_dst,
                              '%s_segments' % mapping['aws_bucket'])

    def test_swift_sync_delete_dlo(self):
        content = [chr(ord('A') + i) * 1024 for i in range(10)]
        key = 'test_dlo'
        mapping = self.swift_sync_mapping()
        crawler = utils.get_container_crawler(mapping)
        segments_container = 'segments'
        manifest = '%s/test-dlo-parts-' % segments_container
        self.local_swift('put_container', segments_container)
        for i in range(len(content)):
            self.local_swift(
                'put_object', 'segments', 'test-dlo-parts-%d' % i, content[i])
        self.local_swift(
            'put_object', mapping['container'], key, '',
            headers={'x-object-manifest': manifest})
        self.remote_swift('put_container',
                          '%s_segments' % mapping['aws_bucket'])

        crawler.run_once()

        headers = self.remote_swift('head_object', mapping['aws_bucket'], key)
        dlo_etag = hashlib.md5(''.join(
            [hashlib.md5(content[i]).hexdigest() for i in range(len(content))]
        )).hexdigest()
        self.assertEqual('"%s"' % dlo_etag, headers['etag'])
        self.assertEqual(dlo_etag,
                         headers.get('x-object-meta-swift-source-dlo-etag'))

        self._assert_stats(
            mapping,
            {'copied_objects': 1,
             'bytes': sum(map(len, content))})

        # Deleting the object should remove the remote segments 0a0s0 well
        self.local_swift('delete_object', mapping['container'], key)
        crawler.run_once()
        _, listing = self.remote_swift(
            'get_container', '%s_segments' % mapping['aws_bucket'])
        self.assertEqual(0, len(listing))

        with self.assertRaises(ClientException) as ctx:
            self.remote_swift('head_object', mapping['aws_bucket'], key)
        self.assertEqual(404, ctx.exception.http_status)

        clear_swift_container(self.swift_src, mapping['container'])
        clear_swift_container(self.swift_src, segments_container)
        clear_swift_container(self.swift_dst, mapping['aws_bucket'])
        clear_swift_container(
            self.swift_dst, '%s_segments ' % mapping['aws_bucket'])

    def test_swift_dont_shunt_post(self):
        # shunt middleware only shunts POST request for migration profiles
        content = 'better not shunt post'
        key = 'test_swift_dont_shunt_post'

        mapping = self.swift_restore_mapping()
        self.remote_swift('put_object', mapping['aws_bucket'], key, content,
                          headers={'x-object-meta-test-header': 'remote'})

        def ensure_remote():
            hdrs, listing = self.local_swift(
                'get_container', mapping['container'])
            self.assertEqual(0, int(hdrs['x-container-object-count']))
            for entry in listing:
                self.assertIn('content_location', entry)
                self.assertEqual([swift_content_location(mapping)],
                                 entry['content_location'])
            obj_hdrs = self.local_swift(
                'head_object', mapping['container'], key)
            self.assertIn('x-object-meta-test-header', obj_hdrs)
            self.assertEqual('remote', obj_hdrs['x-object-meta-test-header'])

        ensure_remote()
        with self.assertRaises(ClientException):
            self.local_swift(
                'post_object', mapping['container'], key,
                {'x-object-meta-test-header': 'local'})
        ensure_remote()

        clear_swift_container(self.swift_dst, mapping['aws_bucket'])
        clear_swift_container(self.swift_src, mapping['container'])

    def test_s3_set_expire_after(self):
        if not self.has_aws:
            raise unittest.SkipTest('AWS Credentials not defined.')

        container = 's3_expire_after_test'
        mapping = dict(
            aws_identity=self.aws_identity,
            aws_secret=self.aws_secret,
            account='AUTH_test',
            aws_bucket=self.aws_bucket,
            container=container,
            copy_after=0,
            propagate_delete=False,
            protocol='s3',
            retain_local=True,
            remote_delete_after=2000 * DAY,
        )
        conn = self.conn_for_acct(mapping['account'])
        conn.put_container(mapping['container'])
        provider = create_provider(mapping, 1)
        orig_rules = provider._get_lifecycle_configuration_for_bucket()
        prefix = provider.get_s3_name('')

        match = next((r.get('Filter', {}).get('Prefix') ==
                      prefix for r in orig_rules), None)
        if match:
            old_del_after = match.get('Expiration', {}).get('Days')
            if old_del_after:
                mapping['remote_delete_after'] = \
                    (old_del_after + 1) * DAY

        del_after = mapping['remote_delete_after']
        exp_days = int(del_after / DAY_F + 0.5)
        crawler = utils.get_container_crawler(mapping)
        crawler.run_once()

        rules = provider._get_lifecycle_configuration_for_bucket()

        # clean up before checking if it worked (in case it didn't)
        conn.delete_container(mapping['container'])

        provider._put_lifecycle_configuration(orig_rules)

        def is_match(r):
            if r.get('Filter', {}).get('Prefix') != prefix:
                return False
            if r.get('Status') != 'Enabled':
                return False
            if r.get('Expiration', {}).get('Days') != exp_days:
                return False
            return True

        self.assertTrue(any(is_match(r) for r in rules), 'no rules match: %s'
                        % (rules,))
