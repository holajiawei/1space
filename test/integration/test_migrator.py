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


import botocore
import hashlib
import json
import StringIO
import swiftclient
import time
import urllib
from . import (
    TestCloudSyncBase, clear_swift_container, wait_for_condition,
    clear_s3_bucket)


class TestMigrator(TestCloudSyncBase):
    def tearDown(self):
        # Make sure all migration-related containers are cleared
        for container in self.test_conf['migrations']:
            if container['protocol'] == 'swift':
                clear_swift_container(self.swift_dst,
                                      container['aws_bucket'])
                clear_swift_container(self.swift_dst,
                                      container['aws_bucket'] + '_segments')
            else:
                try:
                    clear_s3_bucket(self.s3_client, container['aws_bucket'])
                except botocore.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == 'NoSuchBucket':
                        continue

        for container in self.test_conf['migrations']:
            with self.admin_conn_for(container['account']) as conn:
                clear_swift_container(conn, container['container'])
                clear_swift_container(conn,
                                      container['container'] + '_segments')

    def test_s3_migration(self):
        migration = self.s3_migration()

        test_objects = [
            ('s3-blob', 's3 content', {}, {}),
            ('s3-unicod\u00e9', '\xde\xad\xbe\xef', {}, {}),
            ('s3-with-headers', 'header-blob',
             {'custom-header': 'value',
              'unicod\u00e9': '\u262f'},
             {'content-type': 'migrator/test',
              'content-disposition': "attachment; filename='test-blob.jpg'",
              'content-encoding': 'identity'})]

        def _check_objects_copied():
            hdrs, listing = self.local_swift(
                'get_container', migration['container'])
            swift_names = [obj['name'] for obj in listing]
            return set([obj[0] for obj in test_objects]) == set(swift_names)

        for name, body, headers, req_headers in test_objects:
            kwargs = dict([('Content' + key.split('-')[1].capitalize(), value)
                           for key, value in req_headers.items()])
            self.s3('put_object', Bucket=migration['aws_bucket'], Key=name,
                    Body=StringIO.StringIO(body), Metadata=headers,
                    **kwargs)

        wait_for_condition(5, _check_objects_copied)

        for name, expected_body, user_meta, req_headers in test_objects:
            hdrs, body = self.local_swift(
                'get_object', migration['container'], name)
            self.assertEqual(expected_body, body)
            for k, v in user_meta.items():
                self.assertIn('x-object-meta-' + k, hdrs)
                self.assertEqual(v, hdrs['x-object-meta-' + k])
            for k, v in req_headers.items():
                self.assertIn(k, hdrs)
                self.assertEqual(v, hdrs[k])

        for name, expected_body, user_meta, req_headers in test_objects:
            resp = self.s3('get_object', Bucket=migration['aws_bucket'],
                           Key=name)
            self.assertEqual(user_meta, resp['Metadata'])
            self.assertEqual(expected_body, resp['Body'].read())
            for k, v in req_headers.items():
                self.assertIn(k, resp['ResponseMetadata']['HTTPHeaders'])
                self.assertEqual(v, resp['ResponseMetadata']['HTTPHeaders'][k])

    def test_swift_migration(self):
        migration = self.swift_migration()

        test_objects = [
            ('swift-blob', 'blob content', {}),
            ('swift-unicod\u00e9', '\xde\xad\xbe\xef', {}),
            ('swift-with-headers',
             'header-blob',
             {'x-object-meta-custom-header': 'value',
              'x-object-meta-unicod\u00e9': '\u262f',
              'content-type': 'migrator/test',
              'content-disposition': "attachment; filename='test-blob.jpg'",
              'content-encoding': 'identity',
              'x-delete-at': str(int(time.time() + 7200))})]

        def _check_objects_copied():
            hdrs, listing = self.local_swift(
                'get_container', migration['container'])
            swift_names = [obj['name'] for obj in listing]
            return set([obj[0] for obj in test_objects]) == set(swift_names)

        for name, body, headers in test_objects:
            self.remote_swift('put_object', migration['aws_bucket'], name,
                              StringIO.StringIO(body), headers=headers)

        wait_for_condition(5, _check_objects_copied)

        for name, expected_body, user_meta in test_objects:
            for swift in [self.local_swift, self.remote_swift]:
                hdrs, body = swift('get_object', migration['container'], name)
                self.assertEqual(expected_body, body)
                for k, v in user_meta.items():
                    self.assertIn(k, hdrs)
                    self.assertEqual(v, hdrs[k])

    def test_swift_large_objects(self):
        migration = self.swift_migration()

        segments_container = migration['aws_bucket'] + '_segments'
        content = ''.join([chr(97 + i) * 2**20 for i in range(10)])

        self.remote_swift(
            'put_container', segments_container)
        for i in range(10):
            self.remote_swift('put_object', segments_container,
                              'slo-part-%d' % i, chr(97 + i) * 2**20,
                              headers={'x-object-meta-part': i})
            self.remote_swift('put_object', segments_container,
                              'dlo-part-%d' % i, chr(97 + i) * 2**20,
                              headers={'x-object-meta-part': i})
        # Upload the manifests
        self.remote_swift(
            'put_object', migration['aws_bucket'], 'dlo', '',
            headers={'x-object-manifest': segments_container + '/dlo-part',
                     'x-object-meta-dlo': 'dlo-meta'})

        slo_manifest = [
            {'path': '/%s/slo-part-%d' % (segments_container, i)}
            for i in range(10)]
        self.remote_swift(
            'put_object', migration['aws_bucket'], 'slo',
            json.dumps(slo_manifest),
            headers={'x-object-meta-slo': 'slo-meta'},
            query_string='multipart-manifest=put')

        # Verify migration
        def _check_objects_copied():
            try:
                self.local_swift('get_container', migration['container'])
                self.local_swift('get_container', segments_container)
            except swiftclient.exceptions.ClientException as e:
                if e.http_status == 404:
                    return False
                else:
                    raise

            _, listing = self.local_swift(
                'get_container', migration['container'])
            swift_names = [obj['name'] for obj in listing]
            if set(['dlo', 'slo']) != set(swift_names):
                print "Swift names in %s: %s" % (migration['container'],
                                                 swift_names)
                return False
            _, listing = self.local_swift(
                'get_container', segments_container)
            segments = [obj['name'] for obj in listing]
            expected = set(
                ['slo-part-%d' % i for i in range(10)] +
                ['dlo-part-%d' % i for i in range(10)])
            return expected == set(segments)

        wait_for_condition(5, _check_objects_copied)

        mismatched = []

        def _check_segments(prefix):
            for i in range(10):
                part_name = prefix + '%d' % i
                hdrs = self.local_swift(
                    'head_object', segments_container, part_name)
                if hdrs.get('x-object-meta-part') != str(i):
                    mismatched.append('mismatched segment: %s != %s' % (
                        hdrs.get('x-object-meta-part'), i))

        slo_part_prefix = 'slo-part-'
        dlo_part_prefix = 'dlo-part-'
        _check_segments(slo_part_prefix)
        _check_segments(dlo_part_prefix)
        if mismatched:
            self.fail('Found segment mismatches: %s' % '; '.join(mismatched))
        slo_hdrs, slo_body = self.local_swift(
            'get_object', migration['container'], 'slo')
        self.assertEqual('slo-meta', slo_hdrs.get('x-object-meta-slo'))
        self.assertTrue(content == slo_body)

        dlo_hdrs, dlo_body = self.local_swift(
            'get_object', migration['container'], 'dlo')
        self.assertEqual('dlo-meta', dlo_hdrs.get('x-object-meta-dlo'))
        self.assertTrue(content == dlo_body)

    def test_migrate_new_container_location(self):
        migration = self._find_migration(
            lambda cont: cont['container'] == 'migration-swift-reloc')

        test_objects = [
            ('swift-blobBBBB', 'blob content', {}),
            ('swift-unicod\u00e9', '\xde\xad\xbe\xef', {}),
            ('swift-with-headers',
             'header-blob',
             {'x-object-meta-custom-header': 'value',
              'x-object-meta-unicod\u00e9': '\u262f',
              'content-type': 'migrator/test',
              'content-disposition': "attachment; filename='test-blob.jpg'",
              'content-encoding': 'identity',
              'x-delete-at': str(int(time.time() + 7200))})]

        def _check_objects_copied():
            hdrs, listing = self.local_swift(
                'get_container', migration['container'])
            swift_names = [obj['name'] for obj in listing]
            return set([obj[0] for obj in test_objects]) == set(swift_names)

        for name, body, headers in test_objects:
            self.remote_swift('put_object', migration['aws_bucket'], name,
                              StringIO.StringIO(body), headers=headers)

        wait_for_condition(5, _check_objects_copied)

        for name, expected_body, user_meta in test_objects:
            for swift, bkey in [(self.local_swift, 'container'),
                                (self.remote_swift, 'aws_bucket')]:
                hdrs, body = swift('get_object', migration[bkey], name)
                self.assertEqual(expected_body, body)
                for k, v in user_meta.items():
                    self.assertIn(k, hdrs)
                    self.assertEqual(v, hdrs[k])

    def test_container_meta(self):
        migration = self._find_migration(
            lambda cont: cont['container'] == 'no-auto-acl')

        acl = 'AUTH_' + migration['aws_identity'].split(':')[0]
        self.remote_swift('put_container', migration['aws_bucket'],
                          headers={'x-container-read': acl,
                                   'x-container-write': acl,
                                   'x-container-meta-test': 'test metadata'})

        def _check_container_created():
            try:
                return self.local_swift(
                    'get_container', migration['container'])
            except swiftclient.exceptions.ClientException as e:
                if e.http_status == 404:
                    return False
                raise

        hdrs, listing = wait_for_condition(5, _check_container_created)
        self.assertIn('x-container-meta-test', hdrs)
        self.assertEqual('test metadata', hdrs['x-container-meta-test'])

        scheme, rest = self.SWIFT_CREDS['authurl'].split(':', 1)
        swift_host, _ = urllib.splithost(rest)

        remote_swift = swiftclient.client.Connection(
            authurl=self.SWIFT_CREDS['authurl'],
            user=self.SWIFT_CREDS['dst']['user'],
            key=self.SWIFT_CREDS['dst']['key'],
            os_options={'object_storage_url': '%s://%s/v1/AUTH_test' % (
                scheme, swift_host)})
        remote_swift.put_object(migration['container'], 'test', 'test')
        hdrs, body = remote_swift.get_object(migration['container'], 'test')
        self.assertEqual(body, 'test')

    def test_migrate_all_containers(self):
        migration = self._find_migration(
            lambda cont: cont['aws_bucket'] == '/*')

        test_objects = [
            ('swift-blobBBBB', 'blob content', {}),
            ('swift-unicod\u00e9', '\xde\xad\xbe\xef', {}),
            ('swift-with-headers',
             'header-blob',
             {'x-object-meta-custom-header': 'value',
              'x-object-meta-unicod\u00e9': '\u262f',
              'content-type': 'migrator/test',
              'content-disposition': "attachment; filename='test-blob.jpg'",
              'content-encoding': 'identity',
              'x-delete-at': str(int(time.time() + 7200))})]

        test_containers = ['container1', 'container2', 'container3']

        def _check_objects_copied():
            done = True
            test_names = set([obj[0] for obj in test_objects])
            for cont in test_containers:
                hdrs, listing = self.local_swift(
                    'get_container', cont)
                swift_names = set([obj['name'] for obj in listing])
                done = done and (test_names == swift_names)
                if not done:
                    return False
            return True

        for cont in test_containers:
            self.clear_containers_nuser.append(cont)
            self.clear_containers_local.append(cont)

        for name, body, headers in test_objects:
            self.nuser_swift('put_object', migration['aws_bucket'], name,
                             StringIO.StringIO(body), headers=headers)

        wait_for_condition(5, _check_objects_copied)

        for name, expected_body, user_meta in test_objects:
            for cont in test_containers:
                hdrs, body = self.local_swift('get_object', cont, name)
                self.assertEqual(expected_body, body)
                for k, v in user_meta.items():
                    self.assertIn(k, hdrs)
                    self.assertEqual(v, hdrs[k])

    def test_propagate_delete(self):
        migration = self.swift_migration()
        key = 'test_delete_object'
        content = 'D' * 2**10

        def _check_object_copied():
            hdrs, listing = self.local_swift(
                'get_container', migration['container'])
            names = [obj['name'] for obj in listing]
            return names[0] == key

        def _check_removed():
            _, listing = self.local_swift(
                'get_container', migration['container'])
            return listing == []

        self.remote_swift('put_object', migration['aws_bucket'], key,
                          StringIO.StringIO(content))

        wait_for_condition(5, _check_object_copied)

        for swift in [self.local_swift, self.remote_swift]:
            hdrs, body = swift('get_object', migration['container'], key)
            self.assertEqual(content, body)

        self.local_swift('delete_object', migration['container'], key)
        wait_for_condition(5, _check_removed)

        _, listing = self.local_swift('get_container', migration['container'])
        self.assertEqual([], listing)

        clear_swift_container(self.swift_dst, migration['aws_bucket'])
        clear_swift_container(self.swift_src, migration['container'])

    def test_swift_versions_location(self):
        migration = self._find_migration(
            lambda cont: cont['container'] == 'no-auto-versioning')

        versions_container = migration['aws_bucket'] + '_versions'
        self.remote_swift(
            'put_container', versions_container)
        self.remote_swift('put_container', migration['aws_bucket'],
                          headers={'X-Versions-Location': versions_container})
        self.remote_swift(
            'put_object', migration['aws_bucket'], 'object',
            'A' * 2**20)
        self.remote_swift(
            'put_object', migration['aws_bucket'], 'object',
            'B' * 2**20)

        hdrs, listing = self.remote_swift(
            'get_container', migration['aws_bucket'])
        self.assertEqual(versions_container, hdrs['x-versions-location'])
        self.assertEqual(1, len(listing))
        self.assertEqual('object', listing[0]['name'])
        self.assertEqual(hashlib.md5('B' * 2**20).hexdigest(),
                         listing[0]['hash'])

        _, listing = self.remote_swift(
            'get_container', versions_container)
        self.assertEqual(1, len(listing))
        self.assertEqual(hashlib.md5('A' * 2**20).hexdigest(),
                         listing[0]['hash'])

        def _check_objects_copied():
            try:
                hdrs, listing = self.local_swift(
                    'get_container', migration['container'])
                if hdrs.get('x-versions-location') != versions_container:
                    return False
                if len(listing) == 0:
                    return False
                if 'swift' not in listing[0].get('content_location', []):
                    return False
                return True
            except swiftclient.exceptions.ClientException as e:
                if e.http_status == 404:
                    return False
                raise

        wait_for_condition(5, _check_objects_copied)

        hdrs, listing = self.local_swift(
            'get_container', migration['container'])
        self.assertEqual(versions_container, hdrs['x-versions-location'])
        self.assertEqual(1, len(listing))
        self.assertEqual('object', listing[0]['name'])
        self.assertEqual(hashlib.md5('B' * 2**20).hexdigest(),
                         listing[0]['hash'])
        hdrs, listing = self.local_swift(
            'get_container', versions_container)
        self.assertEqual(hashlib.md5('A' * 2**20).hexdigest(),
                         listing[0]['hash'])

        clear_swift_container(self.swift_dst, versions_container)
        clear_swift_container(self.swift_dst, migration['container'])
        clear_swift_container(self.swift_src, versions_container)
        clear_swift_container(self.swift_src, migration['container'])

    def test_swift_history_location(self):
        migration = self._find_migration(
            lambda cont: cont['container'] == 'no-auto-history')

        history_container = migration['aws_bucket'] + '_history'
        self.remote_swift(
            'put_container', history_container)
        clear_swift_container(self.swift_dst, history_container)
        self.remote_swift('put_container', migration['aws_bucket'],
                          headers={'X-History-Location': history_container})
        self.remote_swift(
            'put_object', migration['aws_bucket'], 'object',
            'A' * 2**20)
        self.remote_swift(
            'put_object', migration['aws_bucket'], 'object',
            'B' * 2**20)

        hdrs, listing = self.remote_swift(
            'get_container', migration['aws_bucket'])
        self.assertEqual(history_container, hdrs['x-history-location'])
        self.assertEqual(1, len(listing))
        self.assertEqual('object', listing[0]['name'])
        self.assertEqual(hashlib.md5('B' * 2**20).hexdigest(),
                         listing[0]['hash'])

        _, listing = self.remote_swift(
            'get_container', history_container)
        self.assertEqual(1, len(listing))
        self.assertEqual(hashlib.md5('A' * 2**20).hexdigest(),
                         listing[0]['hash'])

        def _check_objects_copied():
            try:
                hdrs, listing = self.local_swift(
                    'get_container', migration['container'])
                if hdrs.get('x-history-location') != history_container:
                    return False
                if len(listing) == 0:
                    return False
                if 'swift' not in listing[0].get('content_location', []):
                    return False
                return True
            except swiftclient.exceptions.ClientException as e:
                if e.http_status == 404:
                    return False
                raise

        wait_for_condition(5, _check_objects_copied)

        hdrs, listing = self.local_swift(
            'get_container', migration['container'])
        self.assertEqual(history_container, hdrs['x-history-location'])
        self.assertEqual(1, len(listing))
        self.assertEqual('object', listing[0]['name'])
        self.assertEqual(hashlib.md5('B' * 2**20).hexdigest(),
                         listing[0]['hash'])
        hdrs, listing = self.local_swift(
            'get_container', history_container)
        self.assertEqual(hashlib.md5('A' * 2**20).hexdigest(),
                         listing[0]['hash'])

        clear_swift_container(self.swift_dst, migration['container'])
        clear_swift_container(self.swift_dst, history_container)
        clear_swift_container(self.swift_src, migration['container'])
        clear_swift_container(self.swift_src, history_container)
