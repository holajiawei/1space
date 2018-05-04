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
            (u's3-unicod\u00e9', '\xde\xad\xbe\xef', {}, {}),
            ('s3-with-headers', 'header-blob',
             {'custom-header': 'value'},
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
            self.s3('put_object', Bucket=migration['aws_bucket'],
                    Key=name,
                    Body=StringIO.StringIO(body),
                    Metadata=headers,
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
        conn_noshunt = self.conn_for_acct_noshunt(migration['account'])

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

            _, listing = conn_noshunt.get_container(migration['container'])
            swift_names = [obj['name'] for obj in listing]
            objects = ['dlo', 'slo']
            if sorted(objects) != swift_names:
                return False
            _, listing = conn_noshunt.get_container(segments_container)
            segments = [obj['name'] for obj in listing]
            expected = sorted(
                ['slo-part-%d' % i for i in range(10)] +
                ['dlo-part-%d' % i for i in range(10)])
            return expected == segments

        with self.migrator_running():
            wait_for_condition(5, _check_objects_copied)

        mismatched = []

        def _check_segments(prefix):
            for i in range(10):
                part_name = prefix + '%d' % i
                hdrs = conn_noshunt.head_object(
                    segments_container, part_name)
                if hdrs.get('x-object-meta-part') != str(i):
                    mismatched.append('mismatched segment: %s != %s' % (
                        hdrs.get('x-object-meta-part'), i))

        slo_part_prefix = 'slo-part-'
        dlo_part_prefix = 'dlo-part-'
        _check_segments(slo_part_prefix)
        _check_segments(dlo_part_prefix)
        if mismatched:
            self.fail('Found segment mismatches: %s' % '; '.join(mismatched))
        slo_hdrs, slo_body = conn_noshunt.get_object(
            migration['container'], 'slo')
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
            (u'swift-unicod\u00e9', '\xde\xad\xbe\xef', {}),
            ('swift-with-headers',
             'header-blob',
             {'x-object-meta-custom-header': 'value',
              u'x-object-meta-unicod\u00e9': u'\u262f',
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
        conn_local = self.conn_for_acct(migration['account'])

        def _check_container_created(conn):
            try:
                return conn.get_container(migration['container'])
            except swiftclient.exceptions.ClientException as e:
                if e.http_status == 404:
                    return False
                raise

        # verify get_head works through shunt
        hdrs = conn_local.head_container(migration['container'])
        self.assertIn('x-container-meta-test', hdrs)
        self.assertEqual('test metadata', hdrs['x-container-meta-test'])

        # Verify get_container works through shunt
        res = _check_container_created(conn_local)
        self.assertTrue(res)
        hdrs, listing = res
        self.assertIn('x-container-meta-test', hdrs)
        self.assertEqual('test metadata', hdrs['x-container-meta-test'])

        # verify container not really there
        self.assertFalse(_check_container_created(conn_noshunt))

        with self.migrator_running():
            hdrs, listing = wait_for_condition(5, partial(
                _check_container_created, conn_noshunt))

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
            ('swift-blobBBB2', 'blob content', {}),
            (u'swift-2unicod\u00e9', '\xde\xad\xbe\xef', {}),
            ('swift-2with-headers',
             'header-blob',
             {'x-object-meta-custom-header': 'value',
              u'x-object-meta-unicod\u00e9': u'\u262f',
              'content-type': 'migrator/test',
              'content-disposition': "attachment; filename='test-blob.jpg'",
              'content-encoding': 'identity',
              'x-delete-at': str(int(time.time() + 7200))})]

        test_containers = ['container1', 'container2', 'container3',
                           u'container-\u062a']

        conn_local = self.conn_for_acct(migration['account'])
        conn_remote = self.conn_for_acct(migration['aws_account'])

        def _check_objects_copied():
            test_names = set([obj[0] for obj in test_objects])
            for cont in test_containers:
                try:
                    hdrs, listing = conn_local.get_container(cont)
                except swiftclient.exceptions.ClientException as ce:
                    if '404' in str(ce):
                        return False
                    else:
                        raise
                swift_names = set([obj['name'] for obj in listing])
                if test_names != swift_names:
                    return False
            return True

        for i, container in enumerate(test_containers):
            container_meta = {
                u'x-container-meta-tes\u00e9t': u'test\u262f metadata%d' % i}
            conn_remote.put_container(
                # We create a new dictionary, because SwiftClient mutates the
                # header dictionary that we pass it, unfortunately.
                test_containers[i], headers=dict(container_meta))
            # verify get_head works through shunt
            hdrs = conn_local.head_container(test_containers[i])
            for key, value in container_meta.items():
                self.assertIn(key, hdrs)
                self.assertEqual(value, hdrs[key])

            for name, body, headers in test_objects:
                conn_remote.put_object(
                    container, name, StringIO.StringIO(body), headers=headers)

        with self.migrator_running():
            wait_for_condition(5, _check_objects_copied)

        for name, expected_body, user_meta in test_objects:
            for cont in test_containers:
                hdrs, body = conn_local.get_object(cont, name)
                self.assertEqual(expected_body, body)
                for k, v in user_meta.items():
                    self.assertIn(k, hdrs)
                    self.assertEqual(v, hdrs[k])

    def test_shunt_all_containers(self):
        test_objects = [
            ('swift-blobBBB2', 'blob content', {}),
            (u'swift-2unicod\u00e9', '\xde\xad\xbe\xef', {}),
            ('swift-2with-headers',
             'header-blob',
             {'x-object-meta-custom-header': 'value',
              u'x-object-meta-unicod\u00e9': u'\u262f',
              'content-type': 'migrator/test',
              'content-disposition': "attachment; filename='test-blob.jpg'",
              'content-encoding': 'identity',
              'x-delete-at': str(int(time.time() + 7200))})]

        self.swift_nuser.put_container('container')
        for name, body, headers in test_objects:
            self.nuser_swift('put_object', 'container', name,
                             StringIO.StringIO(body), headers=headers)
        _, listing = self.nuser2_swift('get_container', 'container')
        self.assertEqual(
            sorted([obj[0] for obj in test_objects]),
            [obj['name'] for obj in listing])

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

    def test_propagate_container_meta(self):
        migration = self.swift_migration()
        expected_meta = {
            u'x-container-meta-migrated-\u062a': u'new-meta \u062a'}

        # Since the shunt will propagate a container delete, we use the
        # no-shunt proxy
        conn_noshunt = self.conn_for_acct_noshunt(migration['account'])
        conn_noshunt.delete_container(migration['container'])
        self.local_swift('post_container', migration['container'],
                         headers=expected_meta)

        hdrs = self.remote_swift(
            'head_container', migration['aws_bucket'])
        self.assertEqual(
            u'new-meta \u062a', hdrs.get(u'x-container-meta-migrated-\u062a'))

        self.local_swift('put_container', migration['container'])
        # The headers should no longer be visible on this container, as the
        # local container should be authoritative and the shunt should not
        # include any headers from the remote container.
        new_hdrs = self.local_swift(
            'head_container', migration['container'])
        self.assertNotIn(u'x-container-meta-migrated-\u062a', new_hdrs)

    def test_migrate_correct_metadata_changes(self):
        migration = self._find_migration(
            lambda cont: cont['aws_bucket'] == '/*')

        acl = 'AUTH_' + self.SWIFT_CREDS['dst']['user'].split(':')[0]

        key1 = u'x-container-meta-migrated-\u062a'
        key2 = u'x-container-meta-migrated-2-\u062a'
        key3 = u'x-container-meta-migrated-3-\u062a'
        val1 = u'new-meta \u062a'
        val2 = u'changed-meta \u062a'
        val3 = u'new-meta2 \u062a'

        init_local_headers = {
            key2: val2,
            key3: val1,
        }
        init_remote_headers = {
            'x-container-write': acl,
            'x-container-read': acl,
            key1: val1,
            key3: val3,
        }

        conn_local = self.conn_for_acct(migration['account'])
        conn_remote = self.conn_for_acct(migration['aws_account'])
        conn_noshunt = self.conn_for_acct_noshunt(migration['account'])

        conn_local.put_container('metadata_test', headers=init_local_headers)
        # Note: container last_modified dates are whole seconds, so sleep
        # is needed to ensure remote is newer
        time.sleep(2)
        conn_remote.put_container('metadata_test',
                                  headers=init_remote_headers)

        # validate the acl exists on remote (sanity check)
        scheme, rest = self.SWIFT_CREDS['authurl'].split(':', 1)
        swift_host, _ = urllib.splithost(rest)

        remote_swift = swiftclient.client.Connection(
            authurl=self.SWIFT_CREDS['authurl'],
            user=self.SWIFT_CREDS['dst']['user'],
            key=self.SWIFT_CREDS['dst']['key'],
            os_options={'object_storage_url': '%s://%s/v1/%s' % (
                scheme, swift_host, migration['aws_account'].split(':')[0])})
        remote_swift.put_object('metadata_test', 'test', 'test')
        hdrs, body = remote_swift.get_object('metadata_test', 'test')
        self.assertEqual(body, 'test')

        def key_val_copied(k, v):
            test_hdrs = conn_noshunt.head_container('metadata_test')
            if k not in test_hdrs:
                return False
            if test_hdrs[k] == v:
                return True
            return False

        key1_copied = partial(key_val_copied, key1, val1)
        val3_copied = partial(key_val_copied, key3, val3)

        def key_gone(key):
            test_hdrs = conn_noshunt.head_container('metadata_test')
            if key not in test_hdrs:
                return True
            return False

        # sanity check
        self.assertTrue(key_val_copied(key2, val2))
        self.assertTrue(key_gone(key1))

        with self.migrator_running():
            # verify really copied
            wait_for_condition(5, key1_copied)
            self.assertTrue(key_gone(key2))
            self.assertTrue(val3_copied)

        # validate the acl was copied
        remote_swift = swiftclient.client.Connection(
            authurl=self.SWIFT_CREDS['authurl'],
            user=self.SWIFT_CREDS['dst']['user'],
            key=self.SWIFT_CREDS['dst']['key'],
            os_options={'object_storage_url': '%s://%s/v1/%s' % (
                scheme, swift_host, migration['account'])})
        remote_swift.put_object('metadata_test', 'test', 'test')
        hdrs, body = remote_swift.get_object('metadata_test', 'test')
        self.assertEqual(body, 'test')

    def test_container_metadata_copied_only_when_newer(self):
        migration = self._find_migration(
            lambda cont: cont['aws_bucket'] == '/*')
        conn_local = self.conn_for_acct(migration['account'])
        conn_remote = self.conn_for_acct(migration['aws_account'])
        conn_noshunt = self.conn_for_acct_noshunt(migration['account'])

        mykey = 'x-container-meta-where'

        def is_where(val):
            test_hdrs = conn_noshunt.head_container('prop_metadata_test')
            if test_hdrs[mykey] == val:
                return True
            return False

        conn_remote.put_container(
            'prop_metadata_test', headers={mykey: 'remote'})
        conn_local.put_container(
            'prop_metadata_test', headers={mykey: 'local'})
        time.sleep(1)
        conn_remote.post_container(
            'prop_metadata_test', headers={mykey: 'remote'})
        time.sleep(1)
        conn_local.post_container(
            'prop_metadata_test', headers={mykey: 'local'})
        with self.migrator_running():
            # TODO (MSD): This should be replaced with a mechanism for
            # executing the migrator once
            with self.assertRaises(WaitTimedOut):
                wait_for_condition(3, partial(is_where, 'remote'))
            self.assertTrue(is_where('local'))

    def test_propagate_object_meta(self):
        migration = self.swift_migration()
        key = u'test_object-\u062a'
        content = 'test object'
        expected_meta = {
            u'x-object-meta-migration-\u062a': u'new object meta \u062a'}

        self.remote_swift('put_object', migration['aws_bucket'], key,
                          StringIO.StringIO(content))
        self.local_swift('post_object', migration['container'], key,
                         headers=expected_meta)
        hdrs = self.remote_swift(
            'head_object', migration['aws_bucket'], key)
        self.assertEqual(u'new object meta \u062a',
                         hdrs.get(u'x-object-meta-migration-\u062a'))

        clear_swift_container(self.swift_dst, migration['aws_bucket'])

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

    def test_put_on_unmigrated_container(self):
        migration = self._find_migration(
            lambda cont: cont['aws_bucket'] == '/*')

        test_objects = [
            (u'swift-2unicod\u00e9', '\xde\xad\xbe\xef', {}),
            (u'swift-3unicod\u00e9', '2\xde\xad\xbe\xef', {}),
        ]

        test_container = u'test_put-container-\u062a'

        conn_remote = self.conn_for_acct(migration['aws_account'])
        conn_local = self.conn_for_acct(migration['account'])

        conn_remote.put_container(test_container)
        for name, body, headers in test_objects:
            conn_local.put_object(test_container, name,
                                  StringIO.StringIO(body), headers=headers)

        _, listing = conn_local.get_container(test_container)
        self.assertEqual(
            [obj[0] for obj in test_objects],
            [obj['name'] for obj in listing])
        for entry in listing:
            self.assertNotIn('content_location', entry)

    def test_deleted_source_objects(self):
        migration = self.swift_migration()

        objects = [
            (u'test-blob-\u062a-1', 'blob content',
             {u'x-object-meta-\u062a': u'\u062a'}),
            (u'test-blob-\u062a-2', 'blob content',
             {u'x-object-meta-\u062a': u'\u062a'})
        ]
        tests = [[u'test-blob-\u062a-1',
                  u'test-blob-\u062a-2'],
                 [u'test-blob-\u062a-2']]

        conn_remote = self.conn_for_acct(migration['aws_account'])
        conn_local = self.conn_for_acct(migration['account'])

        def _check_objects_copied():
            hdrs, listing = conn_local.get_container(migration['container'])
            swift_names = [obj['name'] for obj in listing
                           if 'swift' in obj.get('content_location', '')]
            return set([obj[0] for obj in objects]) == set(swift_names)

        def _check_unmodified_objects_removed(expected):
            _, listing = conn_local.get_container(migration['container'])
            local_objects = [obj['name'] for obj in listing
                             if 'swift' in obj.get('content_location', '')]
            return expected == local_objects

        for test in tests:
            for name, body, headers in objects:
                conn_remote.put_object(migration['aws_bucket'], name,
                                       StringIO.StringIO(body),
                                       headers=headers)
            self.assertFalse(_check_objects_copied())
            with self.migrator_running():
                wait_for_condition(5, _check_objects_copied)

            for obj in test:
                conn_remote.delete_object(migration['aws_bucket'], obj)

            expected = [obj[0] for obj in objects if obj[0] not in test]

            with self.migrator_running():
                wait_for_condition(
                    5, partial(_check_unmodified_objects_removed, expected))
            clear_swift_container(conn_local, migration['container'])
            clear_swift_container(conn_remote, migration['aws_bucket'])

    def test_post_deleted_source_objects(self):
        migration = self.swift_migration()

        test_objects = [
            (u'post-blob-\u062a', 'blob content',
             {u'x-object-meta-\u062a': u'\u062a'}),
            (u'removed-blob', 'deleted content',
             {u'x-object-meta-\u062a-removed': u'\u062a'})]

        conn_remote = self.conn_for_acct(migration['aws_account'])
        conn_local = self.conn_for_acct(migration['account'])

        def _check_objects_copied():
            hdrs, listing = conn_local.get_container(migration['container'])
            swift_names = [obj['name'] for obj in listing
                           if 'swift' in obj.get('content_location', '')]
            return set([obj[0] for obj in test_objects]) == set(swift_names)

        for name, body, headers in test_objects:
            conn_remote.put_object(migration['aws_bucket'], name,
                                   StringIO.StringIO(body),
                                   headers=headers)

        self.assertFalse(_check_objects_copied())

        with self.migrator_running():
            wait_for_condition(5, _check_objects_copied)

        conn_local.post_object(migration['container'], u'post-blob-\u062a',
                               {'x-object-meta-foo': 'foo'})
        for obj in test_objects:
            conn_remote.delete_object(migration['aws_bucket'], obj[0])

        def _check_unmodified_objects_removed():
            _, listing = conn_local.get_container(migration['container'])
            if len(listing) != 1:
                return False
            if 'content_location' in listing[0]:
                return False
            return listing[0]['name'] == u'post-blob-\u062a'

        with self.migrator_running():
            wait_for_condition(5, _check_unmodified_objects_removed)

    def _setup_deleted_container_test(self):
        '''Common code for the deleted origin container tests'''
        migration = self._find_migration(
            lambda cont: cont['aws_bucket'] == '/*')

        conn_source = self.conn_for_acct(migration['aws_account'])
        conn_destination = self.conn_for_acct(migration['account'])

        conn_source.put_container('test-delete')
        conn_source.put_object('test-delete', 'source_object', 'body')

        def _check_object_copied():
            hdrs, listing = conn_destination.get_container('test-delete')
            if not listing:
                return False
            if listing[0]['name'] != 'source_object':
                return False
            if 'swift' not in listing[0]['content_location']:
                return False
            return True

        with self.migrator_running():
            wait_for_condition(5, _check_object_copied)

    def test_deleted_source_container(self):
        '''Delete the migrated container after it is erased from the source.'''
        self._setup_deleted_container_test()

        migration = self._find_migration(
            lambda cont: cont['aws_bucket'] == '/*')

        conn_source = self.conn_for_acct(migration['aws_account'])
        conn_destination = self.conn_for_acct(migration['account'])

        def _check_container_removed():
            hdrs, listing = conn_destination.get_account()
            if listing:
                return False
            return True

        conn_source.delete_object('test-delete', 'source_object')
        conn_source.delete_container('test-delete')
        with self.migrator_running():
            wait_for_condition(5, _check_container_removed)

    def test_deleted_container_after_new_object(self):
        '''Destination container remains if it has extra objects'''
        migration = self._find_migration(
            lambda cont: cont['aws_bucket'] == '/*')

        conn_source = self.conn_for_acct(migration['aws_account'])
        conn_destination = self.conn_for_acct(migration['account'])

        self._setup_deleted_container_test()
        conn_destination.put_object('test-delete', 'new-object', 'new')
        conn_source.delete_object('test-delete', 'source_object')
        conn_source.delete_container('test-delete')

        def _check_deleted_migrated_objects():
            hdrs, listing = conn_destination.get_container('test-delete')
            if len(listing) != 1:
                return False
            if listing[0]['name'] != 'new-object':
                return False
            return True

        with self.assertRaises(swiftclient.exceptions.ClientException) as cm:
            conn_source.head_container('test-delete')
        self.assertEqual(404, cm.exception.http_status)

        with self.migrator_running():
            wait_for_condition(5, _check_deleted_migrated_objects)

    def test_deleted_container_after_post(self):
        '''Destination container remains if it has been modified'''
        migration = self._find_migration(
            lambda cont: cont['aws_bucket'] == '/*')

        conn_source = self.conn_for_acct(migration['aws_account'])
        conn_destination = self.conn_for_acct(migration['account'])

        self._setup_deleted_container_test()
        conn_destination.post_container(
            'test-delete',
            {'x-container-meta-new-meta': 'value'})
        conn_source.delete_object('test-delete', 'source_object')
        conn_source.delete_container('test-delete')

        def _check_deleted_migrated_objects():
            hdrs, listing = conn_destination.get_container('test-delete')
            self.assertIn('x-container-meta-new-meta', hdrs)
            if len(listing) == 0:
                return True
            return False

        with self.assertRaises(swiftclient.exceptions.ClientException) as cm:
            conn_source.head_container('test-delete')
        self.assertEqual(404, cm.exception.http_status)

        with self.migrator_running():
            wait_for_condition(5, _check_deleted_migrated_objects)

    def test_deleted_object_pagination(self):
        '''Make sure objects are not removed if they're not yet listed.'''
        migration = self.swift_migration()

        conn_source = self.conn_for_acct(migration['aws_account'])
        conn_destination = self.conn_for_acct(migration['account'])

        container = migration['aws_bucket']
        test_objects = [(str(i), 'test-%s' % i, {}) for i in range(10)]

        # Upload the second half of the objects first and migrate them.
        conn_source.put_container(container)
        for i in range(5, 10):
            conn_source.put_object(container, *test_objects[i])

        def _check_migrated_objects(expected_count):
            _, listing = conn_destination.get_container(container)
            local = [entry for entry in listing
                     if 'swift' in entry.get('content_location', [])]
            if len(local) != expected_count:
                return False
            return local

        with self.migrator_running():
            first_migrated = wait_for_condition(
                5, partial(_check_migrated_objects, 5))

        # Upload the first 5 objects. The migrator runs with a 5 object limit
        # and we rely on that in this test.
        for i in range(0, 5):
            conn_source.put_object(container, *test_objects[i])

        with self.migrator_running():
            new_migrated = wait_for_condition(
                5, partial(_check_migrated_objects, 10))

        # The older migrated objects should not have been changed by the
        # migrator
        for i in range(0, 5):
            self.assertEqual(first_migrated[i], new_migrated[i + 5])
