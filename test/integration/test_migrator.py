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
from functools import partial
import hashlib
import json
import StringIO
import swiftclient
import time
import urllib
from . import (
    TestCloudSyncBase, clear_swift_container, wait_for_condition,
    clear_s3_bucket, clear_swift_account, WaitTimedOut)


class TestMigrator(TestCloudSyncBase):
    def tearDown(self):
        def _clear_and_maybe_delete_container(account, container):
            conn = self.conn_for_acct_noshunt(account)
            clear_swift_container(conn, container)
            clear_swift_container(conn, container + '_segments')
            if container.startswith('no-auto-'):
                try:
                    conn.delete_container(container)
                    conn.delete_container(container + '_segments')
                except swiftclient.exceptions.ClientException as e:
                    if e.http_status != 404:
                        raise

        # Make sure all migration-related containers are cleared
        for container in self.test_conf['migrations']:
            if container.get('container'):
                _clear_and_maybe_delete_container(container['account'],
                                                  container['container'])
            if container['aws_bucket'] == '/*':
                continue
            if container['protocol'] == 'swift':
                _clear_and_maybe_delete_container(container['aws_account'],
                                                  container['aws_bucket'])
            else:
                try:
                    clear_s3_bucket(self.s3_client, container['aws_bucket'])
                except botocore.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == 'NoSuchBucket':
                        continue

        # Clean out all container accounts
        clear_swift_account(self.swift_nuser)
        clear_swift_account(self.swift_nuser2)

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

        with self.migrator_running():
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
            (u'swift-unicod\u00e9'.encode('utf8'), '\xde\xad\xbe\xef', {}),
            ('swift-with-headers',
             'header-blob',
             {'x-object-meta-custom-header': 'value',
              u'x-object-meta-unicod\u00e9'.encode('utf8'):
              u'\u262f'.encode('utf8'),
              'content-type': 'migrator/test',
              'content-disposition': "attachment; filename='test-blob.jpg'",
              'content-encoding': 'identity',
              'x-delete-at': str(int(time.time() + 7200))})]

        conn_remote = self.conn_for_acct(migration['aws_account'])
        conn_noshunt = self.conn_for_acct_noshunt(migration['account'])
        conn_local = self.conn_for_acct(migration['account'])

        def _check_objects_copied(conn):
            hdrs, listing = conn.get_container(migration['container'])
            swift_names = [obj['name'].encode('utf8') for obj in listing]
            return set([obj[0] for obj in test_objects]) == set(swift_names)

        for name, body, headers in test_objects:
            conn_remote.put_object(migration['aws_bucket'], name,
                                   StringIO.StringIO(body),
                                   headers=headers)

        # Sanity-check (not actually migrated yet)
        self.assertFalse(_check_objects_copied(conn_noshunt))

        # But they are visible through the shunt
        self.assertTrue(_check_objects_copied(conn_local))

        with self.migrator_running():
            wait_for_condition(5, partial(_check_objects_copied, conn_noshunt))

        for name, expected_body, user_meta in test_objects:
            for conn in (conn_remote, conn_noshunt, conn_local):
                hdrs, body = conn.get_object(migration['container'], name)
                self.assertEqual(expected_body, body)
                for k, v in user_meta.items():
                    self.assertIn(k.decode('utf8'), hdrs)
                    self.assertEqual(v.decode('utf8'), hdrs[k.decode('utf8')])

    def test_swift_migration_unicode_acct(self):
        migration = self._find_migration(
            lambda m: m.get('container') == 'flotty')

        test_objects = [
            ('swift-blog', 'blog content', {}),
            (u'swift-unicog\u00e9'.encode('utf8'), 'g\xde\xad\xbe\xef', {}),
            ('swift-with-headerg',
             'header-blog',
             {'x-object-meta-custom-headeg': 'valug',
              u'x-object-meta-unicog\u00e9'.encode('utf8'):
              u'\u262fg'.encode('utf8'),
              'content-type': 'migrator/tesg',
              'content-disposition': "attachment; filename='test-blog.jpg'",
              'content-encoding': 'identitg',
              'x-delete-at': str(int(time.time() + 7200))})]

        conn_remote = self.conn_for_acct(migration['remote_account'])
        conn_noshunt = self.conn_for_acct_noshunt(migration['account'])
        conn_local = self.conn_for_acct(migration['account'])

        def _check_objects_copied(conn):
            hdrs, listing = conn.get_container(migration['container'])
            swift_names = [obj['name'].encode('utf8') for obj in listing]
            return set([obj[0] for obj in test_objects]) == set(swift_names)

        for name, body, headers in test_objects:
            conn_remote.put_object(migration['aws_bucket'], name,
                                   StringIO.StringIO(body), headers=headers)

        # Sanity-check (not actually migrated yet)
        self.assertFalse(_check_objects_copied(conn_noshunt))

        # But they are visible through the shunt
        self.assertTrue(_check_objects_copied(conn_local))

        with self.migrator_running():
            wait_for_condition(5, partial(_check_objects_copied, conn_noshunt))

        for name, expected_body, user_meta in test_objects:
            for conn in (conn_remote, conn_noshunt, conn_local):
                hdrs, body = conn.get_object(migration['container'], name)
                self.assertEqual(expected_body, body)
                for k, v in user_meta.items():
                    self.assertIn(k.decode('utf8'), hdrs)
                    self.assertEqual(v.decode('utf8'), hdrs[k.decode('utf8')])

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

        with self.migrator_running():
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
            lambda cont: cont['container'] == 'no-auto-migration-swift-reloc')

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

        conn_local = self.conn_for_acct(migration['account'])
        conn_remote = self.conn_for_acct(migration['aws_account'])
        conn_noshunt = self.conn_for_acct_noshunt(migration['account'])

        test_object_set = set([obj[0] for obj in test_objects])

        def _check_objects_copied(conn, container):
            try:
                hdrs, listing = conn.get_container(container)
                swift_names = [obj['name'] for obj in listing]
                return test_object_set == set(swift_names)
            except swiftclient.exceptions.ClientException as e:
                if e.http_status == 404:
                    return False
                raise

        def _verify_objects(conn, container):
            for name, expected_body, user_meta in test_objects:
                hdrs, body = conn.get_object(container, name)
                self.assertEqual(expected_body, body)
                for k, v in user_meta.items():
                    self.assertIn(k, hdrs)
                    self.assertEqual(v, hdrs[k])

        for name, body, headers in test_objects:
            conn_remote.put_object(migration['aws_bucket'], name,
                                   StringIO.StringIO(body), headers=headers)

        # verify that objects visible through shunt
        self.assertTrue(_check_objects_copied(conn_local,
                                              migration['container']))
        _verify_objects(conn_local, migration['container'])

        # verify that objects are where they were put (sanity)
        self.assertTrue(_check_objects_copied(conn_remote,
                                              migration['aws_bucket']))
        _verify_objects(conn_remote, migration['aws_bucket'])

        # verify objects are not really there
        self.assertFalse(_check_objects_copied(conn_noshunt,
                                               migration['container']))

        with self.migrator_running():
            try:
                wait_for_condition(5, partial(_check_objects_copied,
                                              conn_noshunt,
                                              migration['container']))
            except WaitTimedOut:
                hdrs, listing = conn_noshunt.get_container(
                    migration['container'])
                swift_names = [obj['name'] for obj in listing]
                self.assertEqual(test_object_set, set(swift_names))

        # verify objects are really there
        _verify_objects(conn_noshunt, migration['container'])

    def test_container_meta(self):
        migration = self._find_migration(
            lambda cont: cont['container'] == 'no-auto-acl')

        acl = 'AUTH_' + migration['aws_identity'].split(':')[0]
        self.remote_swift('put_container', migration['aws_bucket'],
                          headers={'x-container-read': acl,
                                   'x-container-write': acl,
                                   'x-container-meta-test': 'test metadata'})

        conn_noshunt = self.conn_for_acct_noshunt(migration['account'])

        def _check_container_created():
            try:
                return conn_noshunt.get_container(migration['container'])
            except swiftclient.exceptions.ClientException as e:
                if e.http_status == 404:
                    return False
                raise

        with self.migrator_running():
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

        test_objects = [
            ('swift-blobBBB2', 'blob content', {}),
            ('swift-2unicod\u00e9', '\xde\xad\xbe\xef', {}),
            ('swift-2with-headers',
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
                try:
                    hdrs, listing = self.nuser2_swift(
                        'get_container', cont)
                except swiftclient.exceptions.ClientException as ce:
                    if '404' in str(ce):
                        return False
                    else:
                        raise
                swift_names = set([obj['name'] for obj in listing])
                done = done and (test_names == swift_names)
                if not done:
                    return False
            return True

        for cont in test_containers:
            self.swift_nuser.put_container(cont)
            for name, body, headers in test_objects:
                self.nuser_swift('put_object', cont, name,
                                 StringIO.StringIO(body), headers=headers)

        with self.migrator_running():
            wait_for_condition(5, _check_objects_copied)

        for name, expected_body, user_meta in test_objects:
            for cont in test_containers:
                hdrs, body = self.nuser2_swift('get_object', cont, name)
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
            if not listing:
                return False
            if listing[0]['name'] != key:
                return False
            if 'swift' not in listing[0]['content_location']:
                return False
            return True

        def _check_removed():
            _, listing = self.local_swift(
                'get_container', migration['container'])
            return listing == []

        def _container_exists(client, container):
            try:
                client('get_container', container)
                return True
            except swiftclient.exceptions.ClientException as e:
                if e.http_status == 404:
                    return False
                raise

        def _check_removed_container():
            if _container_exists(self.local_swift, migration['container']):
                return False
            if _container_exists(self.remote_swift, migration['aws_bucket']):
                return False
            return True

        self.remote_swift('put_object', migration['aws_bucket'], key,
                          StringIO.StringIO(content))

        with self.migrator_running():
            wait_for_condition(5, _check_object_copied)

            for swift in [self.local_swift, self.remote_swift]:
                hdrs, body = swift('get_object', migration['container'], key)
                self.assertEqual(content, body)

            self.local_swift('delete_object', migration['container'], key)
            wait_for_condition(5, _check_removed)
            self.local_swift('delete_container', migration['container'])
            wait_for_condition(5, _check_removed_container)

        # recreate the removed container for future tests
        self.local_swift('put_container', migration['container'])
        self.remote_swift('put_container', migration['aws_bucket'])

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
                # I had the listing of the versions container assert, down
                # below, fail (empty listing), which seems crazy, but we might
                # as well synchronize on that as well, here.
                hdrs, listing = self.local_swift(
                    'get_container', versions_container)
                if len(listing) == 0:
                    return False
                return True
            except swiftclient.exceptions.ClientException as e:
                if e.http_status == 404:
                    return False
                raise

        with self.migrator_running():
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
                # TravisCI had the listing of the versions container assert,
                # down below, fail (empty listing), which seems crazy, but we
                # might as well synchronize on that as well, here.
                hdrs, listing = self.local_swift(
                    'get_container', history_container)
                if len(listing) == 0:
                    return False
                return True
            except swiftclient.exceptions.ClientException as e:
                if e.http_status == 404:
                    return False
                raise

        with self.migrator_running():
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
