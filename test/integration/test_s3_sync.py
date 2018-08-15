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
from itertools import repeat
import json

from s3_sync.provider_factory import create_provider

from . import TestCloudSyncBase, clear_swift_container, wait_for_condition, \
    swift_content_location, s3_key_name, clear_s3_bucket, WaitTimedOut


class TestCloudSync(TestCloudSyncBase):
    def _test_archive(
            self, key, content, mapping, get_etag, expected_location):
        etag = self.local_swift(
            'put_object', mapping['container'], key,
            content.encode('utf-8'))
        self.assertEqual(
            hashlib.md5(content.encode('utf-8')).hexdigest(), etag)

        def _check_expired():
            # wait for the shunt to return the results for the object
            hdrs, listing = self.local_swift(
                'get_container', mapping['container'])
            if int(hdrs['x-container-object-count']) != 0:
                return False
            if any(map(lambda entry: 'content_location' not in entry,
                       listing)):
                return False
            return (hdrs, listing)

        swift_hdrs, listing = wait_for_condition(5, _check_expired)
        for entry in listing:
            if entry['name'] == key:
                break

        self.assertEqual(0, int(swift_hdrs['x-container-object-count']))
        self.assertEqual(etag, entry['hash'])
        self.assertEqual(
            [expected_location],
            entry['content_location'])
        self.assertEqual(etag, get_etag(key))

    def test_swift_sync_slash_star(self):
        mapping = self._find_mapping(
            lambda m: m['aws_bucket'] == 'crazy-target:')
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

        def _check_objs():
            # The tree should have 2 containers and 3 objs
            return len(self.get_swift_tree(target_conn)) == 5

        try:
            wait_for_condition(5, _check_objs)
        except WaitTimedOut:
            pass

        self.assertEqual(['crazy-target:slashc1', 'crazy-target:slashc2',
                          'crazy-target:slashc1/c1.o1',
                          'crazy-target:slashc1/c1.o2',
                          'crazy-target:slashc2/c2.o1'],
                         self.get_swift_tree(target_conn))

    def test_s3_sync(self):
        s3_mapping = self.s3_sync_mapping()

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

            def _check_sync():
                try:
                    return self.s3('head_object',
                                   Bucket=s3_mapping['aws_bucket'], Key=s3_key)
                except Exception:
                    return False

            head_resp = wait_for_condition(5, _check_sync)
            self.assertEqual('"%s"' % etag, head_resp['ETag'])

    def test_s3_archive(self):
        s3_mapping = self.s3_archive_mapping()

        test_args = [
            (u'test_archive', u'testing archive put'),
            (u'unicod\u00e9', u'unicod\u00e9 blob')]
        for key, content in test_args:
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
                               expected_location)

    def test_s3_archive_by_metadata(self):
        s3_mapping = self._find_mapping(
            lambda cont:
                cont.get('selection_criteria') is not None and
                cont.get('protocol') == 's3')
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

        def _check_expired():
            hdrs, listing = self.local_swift(
                'get_container', s3_mapping['container'])
            if int(hdrs['x-container-object-count']) != count_not_archived:
                return False
            return (hdrs, listing)

        swift_hdrs, listing = wait_for_condition(5, _check_expired)
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

    def test_swift_archive_by_metadata(self):
        mapping = self._find_mapping(
            lambda cont:
                cont.get('selection_criteria') is not None and
                cont.get('protocol') == 'swift')
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

        def _check_expired():
            hdrs, listing = self.local_swift(
                'get_container', mapping['container'])
            if int(hdrs['x-container-object-count']) != count_not_archived:
                return False
            return (hdrs, listing)

        swift_hdrs, listing = wait_for_condition(5, _check_expired)
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

    def test_swift_sync(self):
        mapping = self.swift_sync_mapping()

        test_args = [
            (u'test_archive', u'testing archive put'),
            (u'unicod\u00e9', u'unicod\u00e9 blob')]
        for key, content in test_args:
            etag = self.local_swift(
                'put_object', mapping['container'], key,
                content.encode('utf-8'))
            self.assertEqual(hashlib.md5(content.encode('utf-8')).hexdigest(),
                             etag)

            def _check_sync():
                try:
                    return self.remote_swift(
                        'head_object', mapping['aws_bucket'], key)
                except Exception:
                    return False

            head_resp = wait_for_condition(5, _check_sync)
            self.assertEqual(etag, head_resp['etag'])

    def test_keystone_sync_v3(self):
        mapping = self._find_mapping(
            lambda m: m['aws_endpoint'].endswith('v3'))
        local_conn = self.conn_for_acct(mapping['account'])
        obj_name = 'sync-test-keystone-v3'
        body = 'party-hardy' * 512
        etag = local_conn.put_object(mapping['container'], obj_name, body)
        self.assertEqual(hashlib.md5(body).hexdigest(), etag)

        self.assertIn('KEY_', mapping['aws_account'])
        remote_conn = self.conn_for_acct_noshunt(mapping['aws_account'])
        self.assertIn('KEY_', remote_conn.url)

        def _check_synced():
            hdrs, listing = local_conn.get_container(mapping['container'])
            if 'swift' not in listing[0].get('content_location', []):
                return False
            got_headers, got_body = remote_conn.get_object(
                mapping['container'], obj_name)
            self.assertEqual(body, got_body)
            return True

        wait_for_condition(5, _check_synced)

    def test_keystone_sync_v2(self):
        mapping = self._find_mapping(
            lambda m: m['aws_bucket'] == 'keystone2')
        local_conn = self.conn_for_acct(mapping['account'])
        obj_name = 'sync-test-keystone-v2'
        body = 'party-animl' * 512
        etag = local_conn.put_object(mapping['container'], obj_name, body)
        self.assertEqual(hashlib.md5(body).hexdigest(), etag)

        self.assertIn('KEY_', mapping['aws_account'])
        remote_conn = self.conn_for_acct_noshunt(mapping['aws_account'])
        self.assertIn('KEY_', remote_conn.url)

        def _check_synced():
            hdrs, listing = local_conn.get_container(mapping['container'])
            if 'swift' not in listing[0].get('content_location', []):
                return False
            got_headers, got_body = remote_conn.get_object(
                mapping['container'], obj_name)
            self.assertEqual(body, got_body)
            return True

        wait_for_condition(5, _check_synced)

    def test_keystone_cross_account_v2(self):
        mapping = self._find_mapping(
            lambda m: m['aws_bucket'] == 'keystone2a')
        local_conn = self.conn_for_acct(mapping['account'])
        obj_name = 'sync-test-keystone-v2-cross-acct'
        body = 'party-animl' * 512
        etag = local_conn.put_object(mapping['container'], obj_name, body)
        self.assertEqual(hashlib.md5(body).hexdigest(), etag)

        remote_conn = self.conn_for_acct_noshunt(mapping['remote_account'])

        def _check_synced():
            try:
                got_headers, got_body = remote_conn.get_object(
                    mapping['container'], obj_name)
                self.assertEqual(body, got_body)
                return True
            except Exception:
                return False

        wait_for_condition(5, _check_synced)

        # Make sure it actually got lifecycled
        hdrs, listing = local_conn.get_container(mapping['container'])
        list_item = [i for i in listing if i['name'] == obj_name][0]
        self.assertEqual([';'.join((mapping['aws_endpoint'],
                                    mapping['aws_identity'],
                                    mapping['aws_bucket']))],
                         list_item['content_location'])

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
        }, repeat('a'))
        self.assertTrue(resp.success)

        s3_key = s3_key_name(mapping, swift_key)
        resp = self.s3('get_object', Bucket=mapping['aws_bucket'], Key=s3_key)
        self.assertEqual(88, resp['ContentLength'])
        self.assertEqual('text/plain', resp['ContentType'])
        self.assertEqual('a' * 88, resp['Body'].read())
        self.assertEqual('bam', resp['Metadata']['foobie-bar'])

    def test_provider_s3_put_object_no_prefix(self):
        mapping = self.s3_sync_mapping()
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
        # NOTE: this is different from real S3 as all of the parts are merged
        # and this is the content ETag
        self.assertEqual(hashlib.md5(content).hexdigest(), hdrs['etag'])
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

    def test_swift_shunt_post(self):
        content = 'shunt post'
        key = 'test_swift_shunt_post'
        mapping = self.swift_restore_mapping()
        self.remote_swift('put_object', mapping['aws_bucket'], key, content)

        def ensure_remote():
            hdrs, listing = self.local_swift(
                'get_container', mapping['container'])
            self.assertEqual(0, int(hdrs['x-container-object-count']))
            for entry in listing:
                self.assertIn('content_location', entry)
                self.assertEqual([swift_content_location(mapping)],
                                 entry['content_location'])

        ensure_remote()
        self.local_swift(
            'post_object', mapping['container'], key,
            {'x-object-meta-test-header': 'foo'})
        hdrs = self.local_swift('head_object', mapping['container'], key)
        self.assertEqual('foo', hdrs.get('x-object-meta-test-header'))
        ensure_remote()

        clear_swift_container(self.swift_dst, mapping['aws_bucket'])
        clear_swift_container(self.swift_src, mapping['container'])
