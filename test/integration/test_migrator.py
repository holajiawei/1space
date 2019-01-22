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
import botocore
import hashlib
import json
import mock
import StringIO
from swift.common.middleware.acl import format_acl
import swiftclient
import tempfile
import time
import unittest
import urllib
import uuid
from . import (
    TestCloudSyncBase, clear_swift_container,
    clear_s3_bucket, clear_swift_account)
import migrator_utils
from s3_sync.utils import ACCOUNT_ACL_KEY


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
        status = migrator_utils.TempMigratorStatus(migration)
        migrator = migrator_utils.MigratorFactory().get_migrator(
            migration, status)

        test_objects = [
            ('s3-blob', 's3 content', {}, {}),
            (u's3-unicod\u00e9', '\xde\xad\xbe\xef', {}, {}),
            ('s3-with-headers', 'header-blob',
             {'custom-header': 'value',
              'unreadable-prefix': '=?UTF-8?Q?=04w?=',
              'unreadable-suffix': '=?UTF-8?Q?h=04?=',
              'lots-of-unprintable': '=?UTF-8?B?BAQEBAQ=?='},
             {'content-type': 'migrator/test',
              'content-disposition': "attachment; filename='test-blob.jpg'",
              'content-encoding': 'identity'})]

        expected_objects = [
            ('s3-blob', 's3 content', {}, {}),
            (u's3-unicod\u00e9', '\xde\xad\xbe\xef', {}, {}),
            ('s3-with-headers', 'header-blob',
             {'x-object-meta-custom-header': 'value',
              'x-object-meta-unreadable-prefix': '\x04w',
              'x-object-meta-unreadable-suffix': 'h\x04',
              'x-object-meta-lots-of-unprintable': 5 * '\x04'},
             {'content-type': 'migrator/test',
              'content-disposition': "attachment; filename='test-blob.jpg'",
              'content-encoding': 'identity'})]

        conn_noshunt = self.conn_for_acct_noshunt(migration['account'])
        conn_local = self.conn_for_acct(migration['account'])

        def _check_objects_copied(conn):
            hdrs, listing = conn.get_container(migration['container'])
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

        # Sanity-check (not actually migrated yet)
        self.assertFalse(_check_objects_copied(conn_noshunt))

        # But they are visible through the shunt
        self.assertTrue(_check_objects_copied(conn_local))

        migrator.next_pass()
        self.assertTrue(_check_objects_copied(conn_noshunt))

        for name, expected_body, user_meta, req_headers in expected_objects:
            for conn in (conn_noshunt, conn_local):
                hdrs, body = conn.get_object(migration['container'], name)
                self.assertEqual(expected_body, body)
                for k, v in user_meta.items():
                    self.assertIn(k, hdrs)
                    self.assertEqual(v, hdrs[k])
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
        status = migrator_utils.TempMigratorStatus(migration)
        migrator = migrator_utils.MigratorFactory().get_migrator(
            migration, status)

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

        migrator.next_pass()
        self.assertTrue(_check_objects_copied(conn_noshunt))

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
        status = migrator_utils.TempMigratorStatus(migration)
        migrator = migrator_utils.MigratorFactory().get_migrator(
            migration, status)

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

        migrator.next_pass()
        self.assertTrue(_check_objects_copied(conn_noshunt))

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
        status = migrator_utils.TempMigratorStatus(migration)
        migrator = migrator_utils.MigratorFactory().get_migrator(
            migration, status)

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

        migrator.next_pass()

        _, listing = conn_noshunt.get_container(migration['container'])
        swift_names = [obj['name'] for obj in listing]
        objects = ['dlo', 'slo']
        self.assertEqual(sorted(objects), swift_names)

        _, listing = conn_noshunt.get_container(segments_container)
        segments = [obj['name'] for obj in listing]
        expected = sorted(
            ['slo-part-%d' % i for i in range(10)] +
            ['dlo-part-%d' % i for i in range(10)])
        self.assertEqual(expected, segments)

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

    def test_swift_self_contained_dlo(self):
        migration = self.swift_migration()
        conn_noshunt = self.conn_for_acct_noshunt(migration['account'])
        status = migrator_utils.TempMigratorStatus(migration)
        migrator = migrator_utils.MigratorFactory().get_migrator(
            migration, status)
        migrator.work_chunk = 1

        content = ''.join([chr(97 + i) * 2**20 for i in range(10)])

        for i in range(10):
            self.remote_swift('put_object', migration['aws_bucket'],
                              'dlo/part-%d' % i, chr(97 + i) * 2**20,
                              headers={'x-object-meta-part': i})
        # Upload the manifest
        self.remote_swift(
            'put_object', migration['aws_bucket'], 'dlo', '',
            headers={'x-object-manifest': migration['aws_bucket'] + '/dlo',
                     'x-object-meta-dlo': 'dlo-meta'})

        migrator.next_pass()

        _, listing = conn_noshunt.get_container(migration['container'])
        swift_names = [obj['name'] for obj in listing]
        objects = ['dlo'] + ['dlo/part-%d' % part for part in range(10)]
        self.assertEqual(objects, swift_names)

        dlo_hdrs, dlo_body = self.local_swift(
            'get_object', migration['container'], 'dlo')
        self.assertEqual('dlo-meta', dlo_hdrs.get('x-object-meta-dlo'))
        self.assertTrue(content == dlo_body)

    def test_swift_migrate_new_container_location(self):
        migration = self._find_migration(
            lambda cont: cont['container'] == 'no-auto-migration-swift-reloc')
        status = migrator_utils.TempMigratorStatus(migration)
        migrator = migrator_utils.MigratorFactory().get_migrator(
            migration, status)

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
                hdrs = conn.head_object(container, name)
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

        migrator.next_pass()
        # verify objects are really there
        _verify_objects(conn_noshunt, migration['container'])

    def test_s3_migrate_new_container_location(self):
        migration = self._find_migration(
            lambda cont: cont['container'] == 'migration-s3-target')
        status = migrator_utils.TempMigratorStatus(migration)
        migrator = migrator_utils.MigratorFactory().get_migrator(
            migration, status)

        test_objects = [
            ('s3-blob', 's3 content', {}, {}),
            (u's3-unicod\u00e9', '\xde\xad\xbe\xef', {}, {}),
            ('s3-with-headers', 'header-blob',
             {'custom-header': 'value'},
             {'content-type': 'migrator/test',
              'content-disposition': "attachment; filename='test-blob.jpg'",
              'content-encoding': 'identity'})]

        conn_local = self.conn_for_acct(migration['account'])
        conn_noshunt = self.conn_for_acct_noshunt(migration['account'])

        def _check_objects_copied(conn, container):
            hdrs, listing = conn.get_container(container)
            swift_names = [obj['name'] for obj in listing]
            return set([obj[0] for obj in test_objects]) == set(swift_names)

        def _verify_local_s3_objects(conn, container):
            # verify original s3 objects that have been shunted or migrated
            # depending on conn to swift cluster. These s3 objects should
            # return with swift style custom headers (i.e., 'x-object-meta-')
            for name, expected_body, user_meta, req_headers in test_objects:
                hdrs, body = conn.get_object(migration['container'], name)
                self.assertEqual(expected_body, body)
                for k, v in user_meta.items():
                    self.assertIn('x-object-meta-' + k, hdrs)
                    self.assertEqual(v, hdrs['x-object-meta-' + k])
                for k, v in req_headers.items():
                    self.assertIn(k, hdrs)
                    self.assertEqual(v, hdrs[k])

        def _verify_remote_s3_objects(bucket):
            for name, expected_body, user_meta, req_headers in test_objects:
                resp = self.s3('get_object', Bucket=bucket, Key=name)
                self.assertEqual(user_meta, resp['Metadata'])
                self.assertEqual(expected_body, resp['Body'].read())
                for k, v in req_headers.items():
                    self.assertIn(k, resp['ResponseMetadata']['HTTPHeaders'])
                    self.assertEqual(
                        v, resp['ResponseMetadata']['HTTPHeaders'][k])

        # PUT objects in remote location
        for name, body, headers, req_headers in test_objects:
            kwargs = dict([('Content' + key.split('-')[1].capitalize(), value)
                           for key, value in req_headers.items()])
            self.s3('put_object', Bucket=migration['aws_bucket'],
                    Key=name,
                    Body=StringIO.StringIO(body),
                    Metadata=headers,
                    **kwargs)

        # verify objects have not migrated yet
        self.assertFalse(_check_objects_copied(conn_noshunt,
                                               migration['container']))

        # verify that objects visible through shunt
        _verify_local_s3_objects(conn_local, migration['container'])

        # verify that objects are where they were put (sanity)
        _verify_remote_s3_objects(migration['aws_bucket'])

        # run migration
        migrator.next_pass()
        # verify objects have migrated over
        _verify_local_s3_objects(conn_noshunt, migration['container'])

    def test_container_meta(self):
        migration = self._find_migration(
            lambda cont: cont['container'] == 'no-auto-acl')
        status = migrator_utils.TempMigratorStatus(migration)
        migrator = migrator_utils.MigratorFactory().get_migrator(
            migration, status)

        acl = 'AUTH_' + migration['aws_identity'].split(':')[0]
        self.remote_swift('put_container', migration['aws_bucket'],
                          headers={'x-container-read': acl,
                                   'x-container-write': acl,
                                   'x-container-meta-test': 'test metadata'})

        conn_noshunt = self.conn_for_acct_noshunt(migration['account'])

        # verify container not really there
        with self.assertRaises(swiftclient.exceptions.ClientException) as ctx:
            conn_noshunt.get_container(migration['container'])
        self.assertEqual(404, ctx.exception.http_status)

        migrator.next_pass()

        hdrs = conn_noshunt.head_container(migration['container'])
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
        status = migrator_utils.TempMigratorStatus(migration)
        migrator = migrator_utils.MigratorFactory().get_migrator(
            migration, status)

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

        migrator.next_pass()

        test_names = set([obj[0] for obj in test_objects])
        for cont in test_containers:
            hdrs, listing = conn_local.get_container(cont)
            swift_names = set([obj['name'] for obj in listing])
            self.assertEqual(test_names, swift_names)

        for name, expected_body, user_meta in test_objects:
            for cont in test_containers:
                hdrs, body = conn_local.get_object(cont, name)
                self.assertEqual(expected_body, body)
                for k, v in user_meta.items():
                    self.assertIn(k, hdrs)
                    self.assertEqual(v, hdrs[k])

    def test_shunt_list_container_all_containers(self):
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

        container = 'container'
        container_headers = {
            'x-container-meta-test': 'test-header',
            u'x-container-meta-unicod\u00e9': u'\u262f'}
        self.swift_nuser.put_container(container, headers=container_headers)
        for name, body, headers in test_objects:
            self.nuser_swift('put_object', container, name,
                             StringIO.StringIO(body), headers=headers)
        # Ensure the container does not yet exist
        conn_noshunt = self.conn_for_acct_noshunt('AUTH_nacct2')
        with self.assertRaises(swiftclient.exceptions.ClientException) as ctx:
            conn_noshunt.head_container(container)
        self.assertEqual(404, ctx.exception.http_status)

        _, listing = self.nuser2_swift('get_container', container)
        self.assertEqual(
            sorted([obj[0] for obj in test_objects]),
            [obj['name'] for obj in listing])
        # The container should now exist
        new_container_headers = conn_noshunt.head_container(container)
        self.assertFalse(
            any(filter(lambda k: k.startswith('Remote'),
                       new_container_headers.keys())))
        for hdr in container_headers:
            self.assertEqual(
                container_headers[hdr], new_container_headers[hdr])

    def test_shunt_get_object_all_containers(self):
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
        container = 'shunt-get-object'
        container_headers = {
            'x-container-meta-test': 'test-header',
            u'x-container-meta-unicod\u00e9': u'\u262f'}
        self.swift_nuser.put_container(container, headers=container_headers)
        for name, body, headers in test_objects:
            self.nuser_swift('put_object', container, name,
                             StringIO.StringIO(body), headers=headers)
        # Ensure the container does not yet exist
        conn_noshunt = self.conn_for_acct_noshunt('AUTH_nacct2')
        with self.assertRaises(swiftclient.exceptions.ClientException) as ctx:
            conn_noshunt.head_container(container)
        self.assertEqual(404, ctx.exception.http_status)

        # We should be able to restore the objects even if the container does
        # not yet exist
        for name, body, headers in test_objects:
            restored_headers, restored_body = self.nuser2_swift(
                'get_object', container, name)
            self.assertEqual(body, restored_body)
            for hdr in headers:
                self.assertEqual(headers[hdr], restored_headers[hdr])

        # The container should now exist
        new_container_headers = conn_noshunt.head_container(container)
        self.assertFalse(
            any(filter(lambda k: k.startswith('Remote'),
                       new_container_headers.keys())))
        for hdr in container_headers:
            self.assertEqual(
                container_headers[hdr], new_container_headers[hdr])

    def test_shunt_head_container(self):
        '''HEAD on the container should create it'''
        acl = 'AUTH_' + self.SWIFT_CREDS['dst']['user'].split(':')[0]
        container_headers = {
            'x-container-meta-test': 'test-header',
            u'x-container-meta-unicod\u00e9': u'\u262f',
            'x-container-read': acl}
        container = 'shunt-head-container'
        self.swift_nuser.put_container(container, headers=container_headers)

        # Ensure the container does not yet exist
        conn_noshunt = self.conn_for_acct_noshunt('AUTH_nacct2')
        with self.assertRaises(swiftclient.exceptions.ClientException) as ctx:
            conn_noshunt.head_container(container)
        self.assertEqual(404, ctx.exception.http_status)

        self.swift_nuser2.head_container(container)

        # The container should now exist
        new_container_headers = conn_noshunt.head_container(container)
        self.assertFalse(
            any(filter(lambda k: k.startswith('Remote'),
                       new_container_headers.keys())))
        for hdr in container_headers:
            if hdr == 'x-container-read':
                continue
            self.assertEqual(
                container_headers[hdr], new_container_headers[hdr])

        # Try to read using the ACL
        scheme, rest = self.SWIFT_CREDS['authurl'].split(':', 1)
        swift_host, _ = urllib.splithost(rest)
        remote_swift = swiftclient.client.Connection(
            authurl=self.SWIFT_CREDS['authurl'],
            user=self.SWIFT_CREDS['dst']['user'],
            key=self.SWIFT_CREDS['dst']['key'],
            os_options={'object_storage_url': '%s://%s/v1/AUTH_nacct2' % (
                scheme, swift_host)})
        remote_swift.get_container(container)

    def test_propagate_delete(self):
        migration = self.swift_migration()
        status = migrator_utils.TempMigratorStatus(migration)
        migrator = migrator_utils.MigratorFactory().get_migrator(
            migration, status)

        key = 'test_delete_object'
        content = 'D' * 2**10

        def _container_exists(client, container):
            try:
                client('get_container', container)
                return True
            except swiftclient.exceptions.ClientException as e:
                if e.http_status == 404:
                    return False
                raise

        self.remote_swift('put_object', migration['aws_bucket'], key,
                          StringIO.StringIO(content))

        migrator.next_pass()
        hdrs, listing = self.local_swift(
            'get_container', migration['container'])
        self.assertEqual(key, listing[0]['name'])
        self.assertIn('swift', listing[0]['content_location'])

        self.local_swift('delete_object', migration['container'], key)
        for swift in [self.local_swift, self.remote_swift]:
            _, listing = swift(
                'get_container', migration['container'])
            self.assertFalse(listing)

        self.local_swift('delete_container', migration['container'])
        self.assertFalse(_container_exists(self.local_swift,
                                           migration['container']))
        self.assertFalse(_container_exists(self.remote_swift,
                                           migration['aws_bucket']))

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

    def test_migrate_account_metadata(self):
        migration = self.swift_migration()
        conn_remote = self.conn_for_acct(migration['aws_account'])
        conn_local = self.conn_for_acct(migration['account'])
        status = migrator_utils.TempMigratorStatus(migration)
        migrator = migrator_utils.MigratorFactory().get_migrator(
            migration, status)

        acl_dict = {'read-write': [u'AUTH_\u062aacct2']}
        acl = format_acl(version=2, acl_dict=acl_dict)
        expected_headers = {
            'x-account-meta-test1': 'mytestval',
            u'x-account-meta-\u062atest1': u'mytestval\u062a',
            'x-account-meta-temp-url-key': 'mysecret',
            ACCOUNT_ACL_KEY: acl
        }

        def validate_acct_metadata(assertit=False):
            hdrs = conn_local.head_account()
            for hdr in expected_headers:
                if assertit:
                    self.assertEqual(expected_headers[hdr], hdrs.get(hdr),
                                     'value wrong for "%s" expected: "%s", '
                                     'got: "%s"' % (hdr, expected_headers[hdr],
                                                    hdrs.get(hdr)))
                else:
                    if expected_headers[hdr] != hdrs.get(hdr):
                        return False
            return True

        def validate_no_metadata():
            hdrs = conn_local.head_account()
            for hdr in expected_headers:
                if hdrs.get(hdr) is not None:
                    return False
            return True

        if not validate_no_metadata():
            # This can happen if the previous run failed and the clean up
            # code at end of test doesn't run.
            # TODO: make clean up account metadata part of tearDown
            conn_local.post_account({
                'x-account-meta-test1': '',
                'x-account-meta-temp-url-key': '',
                u'x-account-meta-\u062atest1': '',
                ACCOUNT_ACL_KEY: '',
            })
            time.sleep(2)

        conn_remote.post_account({
            'x-account-meta-test1': 'mytestval',
            'x-account-meta-temp-url-key': 'mysecret',
            u'x-account-meta-\u062atest1': u'mytestval\u062a',
            ACCOUNT_ACL_KEY: acl,
        })

        self.assertTrue(validate_no_metadata())
        migrator.next_pass()
        self.assertTrue(validate_acct_metadata())
        conn_remote.post_account({
            'x-account-meta-test1': '',
            u'x-account-meta-\u062atest1': '',
            'x-account-meta-temp-url-key': '',
            ACCOUNT_ACL_KEY: '',
        })
        migrator.next_pass()
        self.assertTrue(validate_no_metadata())

    def test_propagate_container_meta_changes(self):
        migration = self._find_migration(
            lambda cont: cont['aws_bucket'] == '/*')
        status = migrator_utils.TempMigratorStatus(migration)
        migrator = migrator_utils.MigratorFactory().get_migrator(
            migration, status)

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
        etag = remote_swift.put_object('metadata_test', 'test', 'test')
        self.assertEqual(hashlib.md5('test').hexdigest(), etag)

        # sanity check
        hdrs = conn_noshunt.head_container('metadata_test')
        self.assertEqual(val2, hdrs[key2])
        self.assertFalse(key1 in hdrs)

        migrator.next_pass()

        # verify really copied
        hdrs = conn_noshunt.head_container('metadata_test')
        self.assertIn(key1, hdrs)
        self.assertEqual(val1, hdrs[key1])
        self.assertIn(key3, hdrs)
        self.assertEqual(val3, hdrs[key3])
        self.assertFalse(key2 in hdrs)

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

        status = migrator_utils.TempMigratorStatus(migration)
        migrator = migrator_utils.MigratorFactory().get_migrator(
            migration, status)

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
        migrator.next_pass()
        self.assertTrue(is_where('local'))

    def test_object_metadata_copied_only_when_newer(self):
        migration = self.swift_migration()
        key = u'test_object-own'
        content = 'test object'

        where_header = 'x-object-meta-where'

        status = migrator_utils.TempMigratorStatus(migration)
        migrator = migrator_utils.MigratorFactory().get_migrator(
            migration, status)

        conn_local = self.conn_for_acct(migration['account'])
        conn_remote = self.conn_for_acct(migration['aws_account'])
        conn_noshunt = self.conn_for_acct_noshunt(migration['account'])

        def is_where(val):
            try:
                test_hdrs = conn_noshunt.head_object(
                    migration['container'], key)
                if test_hdrs[where_header] == val:
                    return True
            except swiftclient.exceptions.ClientException as e:
                if e.http_status == 404:
                    return False
                raise
            return False

        conn_local.put_object(
            migration['container'], key, StringIO.StringIO(content),
            headers={where_header: 'local'})
        conn_remote.put_object(
            migration['aws_bucket'], key, StringIO.StringIO(content),
            headers={where_header: 'remote'})
        conn_local.post_object(
            migration['container'], key, headers={where_header: 'local'})
        migrator.next_pass()
        self.assertTrue(is_where('local'))

    def test_propagate_object_meta_to_remote_swift(self):
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

    def test_propagate_object_meta_to_remote_s3(self):
        migration = self.s3_migration()
        key = u'test_object-\u062a'
        content = 'test object'
        expected_meta = {
            'x-object-meta-custom-migration': 'value'}

        self.s3('put_object', Bucket=migration['aws_bucket'],
                Key=key,
                Body=StringIO.StringIO(content))
        self.local_swift('post_object', migration['container'], key,
                         headers=expected_meta)
        resp = self.s3('get_object', Bucket=migration['aws_bucket'], Key=key)

        # sanity, content hasn't changed
        self.assertEqual(content, resp['Body'].read())

        # should have shunted metadata to the source
        hdrs = resp['Metadata']
        self.assertEqual('value', hdrs.get('custom-migration'))

        clear_swift_container(self.swift_dst, migration['aws_bucket'])

    def test_swift_versions_location(self):
        migration = self._find_migration(
            lambda cont: cont['container'] == 'no-auto-versioning')
        status = migrator_utils.TempMigratorStatus(migration)
        migrator = migrator_utils.MigratorFactory().get_migrator(
            migration, status)

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

        migrator.next_pass()

        hdrs, listing = self.local_swift(
            'get_container', migration['container'])
        self.assertEqual(versions_container, hdrs.get('x-versions-location'))
        self.assertIn('swift', listing[0].get('content_location', []))
        self.assertEqual(1, len(listing))
        self.assertEqual('object', listing[0]['name'])
        self.assertEqual(hashlib.md5('B' * 2**20).hexdigest(),
                         listing[0]['hash'])

        hdrs, listing = self.local_swift('get_container', versions_container)
        self.assertEqual(hashlib.md5('A' * 2**20).hexdigest(),
                         listing[0]['hash'])

        clear_swift_container(self.swift_dst, versions_container)
        clear_swift_container(self.swift_dst, migration['container'])
        clear_swift_container(self.swift_src, versions_container)
        clear_swift_container(self.swift_src, migration['container'])

    def test_swift_history_location(self):
        migration = self._find_migration(
            lambda cont: cont['container'] == 'no-auto-history')
        status = migrator_utils.TempMigratorStatus(migration)
        migrator = migrator_utils.MigratorFactory().get_migrator(
            migration, status)

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

        migrator.next_pass()

        hdrs, listing = self.local_swift(
            'get_container', migration['container'])
        self.assertEqual(history_container, hdrs.get('x-history-location'))
        self.assertEqual(1, len(listing))
        self.assertIn('swift', listing[0].get('content_location'))
        self.assertEqual('object', listing[0]['name'])
        self.assertEqual(hashlib.md5('B' * 2**20).hexdigest(),
                         listing[0]['hash'])

        _, listing = self.local_swift(
            'get_container', history_container)
        self.assertTrue(len(listing) > 0)
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
        status = migrator_utils.TempMigratorStatus(migration)
        migrator = migrator_utils.MigratorFactory().get_migrator(
            migration, status)

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

        for test in tests:
            for name, body, headers in objects:
                conn_remote.put_object(migration['aws_bucket'], name,
                                       StringIO.StringIO(body),
                                       headers=headers)
            _, listing = conn_local.get_container(migration['container'])
            swift_names = [obj['name'] for obj in listing
                           if 'swift' not in obj.get('content_location', '')]
            self.assertEqual(
                set([obj[0] for obj in objects]), set(swift_names))
            migrator.next_pass()

            _, listing = conn_local.get_container(migration['container'])
            swift_names = [obj['name'] for obj in listing
                           if 'swift' in obj.get('content_location', '')]
            self.assertEqual(
                set([obj[0] for obj in objects]), set(swift_names))

            for obj in test:
                conn_remote.delete_object(migration['aws_bucket'], obj)

            expected = [obj[0] for obj in objects if obj[0] not in test]

            migrator.next_pass()
            _, listing = conn_local.get_container(migration['container'])
            local_objects = [obj['name'] for obj in listing
                             if 'swift' in obj.get('content_location', '')]
            self.assertEqual(expected, local_objects)

            clear_swift_container(conn_local, migration['container'])
            clear_swift_container(conn_remote, migration['aws_bucket'])

    def test_post_deleted_source_objects(self):
        migration = self.swift_migration()
        status = migrator_utils.TempMigratorStatus(migration)
        migrator = migrator_utils.MigratorFactory().get_migrator(
            migration, status)

        test_objects = [
            (u'post-blob-\u062a', 'blob content',
             {u'x-object-meta-\u062a': u'\u062a'}),
            (u'removed-blob', 'deleted content',
             {u'x-object-meta-\u062a-removed': u'\u062a'})]

        conn_remote = self.conn_for_acct(migration['aws_account'])
        conn_local = self.conn_for_acct(migration['account'])

        for name, body, headers in test_objects:
            conn_remote.put_object(migration['aws_bucket'], name,
                                   StringIO.StringIO(body),
                                   headers=headers)

        _, listing = conn_local.get_container(migration['container'])
        swift_names = [obj['name'] for obj in listing
                       if 'swift' in obj.get('content_location', '')]
        self.assertEqual(0, len(swift_names))

        migrator.next_pass()

        _, listing = conn_local.get_container(migration['container'])
        swift_names = [obj['name'] for obj in listing
                       if 'swift' in obj.get('content_location', '')]
        self.assertEqual(set([obj[0] for obj in test_objects]),
                         set(swift_names))

        conn_local.post_object(migration['container'], u'post-blob-\u062a',
                               {'x-object-meta-foo': 'foo'})
        for obj in test_objects:
            conn_remote.delete_object(migration['aws_bucket'], obj[0])

        migrator.next_pass()

        _, listing = conn_local.get_container(migration['container'])
        self.assertEqual(1, len(listing))
        self.assertFalse('content_location' in listing[0])
        self.assertEqual(u'post-blob-\u062a', listing[0]['name'])

    def _setup_deleted_container_test(self, migration, migrator):
        '''Common code for the deleted origin container tests'''
        conn_source = self.conn_for_acct(migration['aws_account'])
        conn_destination = self.conn_for_acct(migration['account'])

        conn_source.put_container('test-delete')
        conn_source.put_object('test-delete', 'source_object', 'body')

        migrator.next_pass()

        _, listing = conn_destination.get_container('test-delete')
        self.assertEqual('source_object', listing[0]['name'])
        self.assertIn('swift', listing[0]['content_location'])

    def test_deleted_source_container(self):
        '''Delete the migrated container after it is erased from the source.'''
        migration = self._find_migration(
            lambda cont: cont['aws_bucket'] == '/*')
        status = migrator_utils.TempMigratorStatus(migration)
        migrator = migrator_utils.MigratorFactory().get_migrator(
            migration, status)

        self._setup_deleted_container_test(migration, migrator)

        conn_source = self.conn_for_acct(migration['aws_account'])
        conn_destination = self.conn_for_acct(migration['account'])

        conn_source.delete_object('test-delete', 'source_object')
        conn_source.delete_container('test-delete')

        # reset the migrator config to /*
        migrator.config['container'] = '/*'
        migrator.config['aws_bucket'] = '/*'
        migrator.next_pass()

        _, listing = conn_destination.get_account()
        self.assertEqual(0, len(listing))

    def test_deleted_container_after_new_object(self):
        '''Destination container remains if it has extra objects'''
        migration = self._find_migration(
            lambda cont: cont['aws_bucket'] == '/*')
        status = migrator_utils.TempMigratorStatus(migration)
        migrator = migrator_utils.MigratorFactory().get_migrator(
            migration, status)

        conn_source = self.conn_for_acct(migration['aws_account'])
        conn_destination = self.conn_for_acct(migration['account'])

        self._setup_deleted_container_test(migration, migrator)
        conn_destination.put_object('test-delete', 'new-object', 'new')
        conn_source.delete_object('test-delete', 'source_object')
        conn_source.delete_container('test-delete')

        with self.assertRaises(swiftclient.exceptions.ClientException) as cm:
            conn_source.head_container('test-delete')
        self.assertEqual(404, cm.exception.http_status)

        # reset the migrator config, as it is now set to a single container
        migrator.config['container'] = '/*'
        migrator.config['aws_bucket'] = '/*'
        migrator.next_pass()

        _, listing = conn_destination.get_container('test-delete')
        self.assertEqual(1, len(listing))
        self.assertEqual('new-object', listing[0]['name'])

    def test_deleted_container_after_post(self):
        '''Destination container remains if it has been modified'''
        migration = self._find_migration(
            lambda cont: cont['aws_bucket'] == '/*')
        status = migrator_utils.TempMigratorStatus(migration)
        migrator = migrator_utils.MigratorFactory().get_migrator(
            migration, status)

        conn_source = self.conn_for_acct(migration['aws_account'])
        conn_destination = self.conn_for_acct(migration['account'])

        self._setup_deleted_container_test(migration, migrator)
        conn_destination.post_container(
            'test-delete',
            {'x-container-meta-new-meta': 'value'})
        conn_source.delete_object('test-delete', 'source_object')
        conn_source.delete_container('test-delete')

        with self.assertRaises(swiftclient.exceptions.ClientException) as cm:
            conn_source.head_container('test-delete')
        self.assertEqual(404, cm.exception.http_status)

        # reset the migrator config, as it is now set to a single container
        migrator.config['container'] = '/*'
        migrator.config['aws_bucket'] = '/*'
        migrator.next_pass()

        hdrs, listing = conn_destination.get_container('test-delete')
        self.assertIn('x-container-meta-new-meta', hdrs)
        self.assertEqual(0, len(listing))

    def test_deleted_object_pagination(self):
        '''Make sure objects are not removed if they're not yet listed.'''
        migration = self.swift_migration()
        status = migrator_utils.TempMigratorStatus(migration)
        migrator = migrator_utils.MigratorFactory().get_migrator(
            migration, status)

        conn_source = self.conn_for_acct(migration['aws_account'])
        conn_destination = self.conn_for_acct(migration['account'])

        container = migration['aws_bucket']
        test_objects = [(str(i), 'test-%s' % i, {}) for i in range(10)]

        # Upload the second half of the objects first and migrate them.
        conn_source.put_container(container)
        for i in range(5, 10):
            conn_source.put_object(container, *test_objects[i])

        migrator.next_pass()

        _, listing = conn_destination.get_container(container)
        first_migrated = [entry for entry in listing
                          if 'swift' in entry.get('content_location', [])]
        self.assertEqual(5, len(first_migrated))

        # Upload the first 5 objects. The migrator runs with a 5 object limit
        # and we rely on that in this test.
        for i in range(0, 5):
            conn_source.put_object(container, *test_objects[i])

        migrator.next_pass()

        _, listing = conn_destination.get_container(container)
        new_migrated = [entry for entry in listing
                        if 'swift' in entry.get('content_location', [])]
        self.assertEqual(10, len(new_migrated))

        # The older migrated objects should not have been changed by the
        # migrator
        for i in range(0, 5):
            self.assertEqual(first_migrated[i], new_migrated[i + 5])

    def test_migrate_many_slos(self):
        '''Ensure we can migrate numerous SLOs with multiple segments'''
        # The object_queue is sized to be 2*workers. This test makes sure that
        # we can handle SLOs that fill the object_queue.
        migration = self._find_migration(
            lambda cont: cont['aws_bucket'] == '/*')
        status = migrator_utils.TempMigratorStatus(migration)
        migrator = migrator_utils.MigratorFactory().get_migrator(
            migration, status)
        conn_source = self.conn_for_acct(migration['aws_account'])
        conn_destination = self.conn_for_acct(migration['account'])

        segments = 10
        slos = 5
        # we want the SLO container to sort before all others,
        # as this is a /* migration.
        container = '1-test-slos'
        conn_source.put_container(container)

        def _make_manifest(index):
            manifest = [
                {'path': 'segments-%d/segment-%d' % (index, j)}
                for j in range(segments)]
            return manifest

        for i in range(slos):
            conn_source.put_container('segments-%d' % i)
            for j in range(segments):
                conn_source.put_object(
                    'segments-%d' % i, 'segment-%d' % j,
                    chr(ord('A') + j) * 4)
            conn_source.put_object(
                container, 'slo-%d' % i,
                json.dumps(_make_manifest(i)),
                query_string='multipart-manifest=put')

        migrator.next_pass()
        expected_body = ''.join([
            chr(ord('A') + i) * 4 for i in range(segments)])
        _, listing = conn_destination.get_container(container)
        local = [entry for entry in listing
                 if 'swift' in entry.get('content_location', [])]
        self.assertEqual(slos, len(local))

        for i in range(slos):
            hdrs, body = conn_destination.get_object(
                container, 'slo-%d' % i)
            for hdr in hdrs.keys():
                # make sure the response is local to us
                self.assertFalse(hdr.startswith('Remote-'))
            self.assertEqual('True', hdrs['x-static-large-object'])
            self.assertEqual(expected_body, body)

            _, body = conn_destination.get_object(
                container, 'slo-%d' % i,
                query_string='multipart-manifest=get&format=raw')
            self.assertEqual(
                ['/' + entry['path'] for entry in _make_manifest(i)],
                [entry['path'] for entry in json.loads(body)])

    def test_migrate_many_dlos(self):
        '''Ensure we can migrate many DLOs'''
        migration = self.swift_migration()

        conn_source = self.conn_for_acct(migration['aws_account'])
        conn_destination = self.conn_for_acct(migration['account'])

        status = migrator_utils.TempMigratorStatus(migration)
        migrator = migrator_utils.MigratorFactory().get_migrator(
            migration, status)

        segments = migrator.work_chunk
        manifests = migrator.work_chunk * 2

        conn_source.put_container('nested-segments')
        for i in range(segments):
            conn_source.put_object(
                'nested-segments', 'segment-%d' % i,
                chr(ord('A') + i) * 4)

        for i in range(manifests):
            conn_source.put_container('segments-%d' % i)
            for j in range(segments):
                conn_source.put_object(
                    'segments-%d' % i, 'segment-%d' % j,
                    chr(ord('A') + j) * 4)
            conn_source.put_object(
                'segments-%d' % i, 'segment-dlo', '',
                headers={'x-object-manifest': 'nested-segments/segment'})
            conn_source.put_object(
                migration['aws_bucket'], 'dlo-%d' % i, '',
                headers={'x-object-manifest': 'segments-%d/segment-' % i})

        def _check_migrated_objects(container, expected_count):
            _, listing = conn_destination.get_container(migration['container'])
            local = [entry for entry in listing
                     if 'swift' in entry.get('content_location', [])]
            if len(local) != expected_count:
                return False
            return local

        migrator.next_pass()
        self.assertEqual(
            'dlo-%d' % (manifests / 2 - 1),
            status.get_migration(migration).get('marker'))
        migrator.next_pass()
        self.assertEqual(
            'dlo-%d' % (manifests - 1),
            status.get_migration(migration).get('marker'))
        self.assertEqual(
            # We have to copy the DLO manifests, the segments in each manifest
            # (including one nested manifest), and the segments in the nested
            # manifest, which is shared across the top level DLOs, so we copy
            # it once.
            manifests + manifests * (segments + 1) + segments,
            status.get_migration(migration).get('moved_count'))
        self.assertEqual(
            # We have to scan the DLO manifests, the segment in each manifest
            # (which include a nested manifest), and the segments in the nested
            # manifest (we won't copy them, but will examine more than once).
            manifests + manifests * (segments + 1) + manifests * segments,
            status.get_migration(migration).get('scanned_count'))
        self.assertTrue(_check_migrated_objects(
            migration['container'], manifests))

        expected_body = ''.join([
            chr(ord('A') + i) * 4 for i in range(segments)])
        for i in range(manifests):
            hdrs, body = conn_destination.get_object(
                migration['container'], 'dlo-%d' % i)
            for hdr in hdrs.keys():
                # make sure the response is local to us
                self.assertFalse(hdr.startswith('Remote-'))
            self.assertEqual('segments-%d/segment-' % i,
                             hdrs['x-object-manifest'])
            self.assertEqual(20, int(hdrs['content-length']))
            self.assertEqual(expected_body, body)

            hdrs, body = conn_destination.get_object(
                'segments-%d' % i, 'segment-dlo')
            self.assertEqual(expected_body, body)
            self.assertEqual(
                'nested-segments/segment',
                hdrs['x-object-manifest'])

    def _upload_mpu(self, key='mpu-example', parts=2, part_size=5 * 2**20):
        session = boto3.session.Session(
            aws_access_key_id=self.aws_identity,
            aws_secret_access_key=self.aws_secret)
        conf = boto3.session.Config(signature_version='s3v4',
                                    s3={'aws_chunked': True})
        client = session.client('s3', config=conf)

        body = 'A' * (parts * part_size)
        resp = client.create_multipart_upload(
            Bucket=self.aws_bucket,
            Key=key)
        upload_id = resp['UploadId']
        parts_etags = []
        for part in range(parts):
            part_resp = client.upload_part(
                Bucket=self.aws_bucket,
                Key=key,
                UploadId=upload_id,
                PartNumber=part + 1,
                Body=body[part * part_size:((part + 1) * part_size)],
                ContentLength=part_size)
            parts_etags.append(part_resp['ETag'])
        return client.complete_multipart_upload(
            Bucket=self.aws_bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={
                'Parts': [{'PartNumber': i + 1, 'ETag': parts_etags[i]}
                          for i in range(parts)]
            })

    def test_s3_large_file_migration(self):
        # write large file on s3proxy (3 * 2**20)
        key = 'large_file_' + str(uuid.uuid4())
        content = 'A' * (3 * 2**20)
        s3_migration = self.s3_migration()
        self.s3('put_object', Bucket=s3_migration['aws_bucket'],
                Key=key,
                Body=StringIO.StringIO(content))
        # run migrator with custom migration definition
        config = {
            'migrations': [s3_migration],
            'migrator_settings': {
                'items_chunk': 1000,
                'log_file': '/var/log/swift-s3-migrator.log',
                'log_level': 'debug',
                'poll_interval': 300,
                'process': 0,
                'processes': 1,
                'status_file': '/var/lib/swift-s3-sync/migrator.status',
                'workers': 10,
                'segment_size': 1024 * 1024,
            }
        }
        conn_local = self.conn_for_acct(s3_migration['account'])
        with tempfile.NamedTemporaryFile() as fp:
            json.dump(config, fp)
            fp.flush()
            status = migrator_utils.TempMigratorStatus(s3_migration)
            migrator = migrator_utils.MigratorFactory(
                conf_path=fp.name).get_migrator(s3_migration, status)
        with mock.patch('s3_sync.migrator.swift.common.constraints'
                        '.MAX_FILE_SIZE', 2 * 1024 * 1024):
            migrator.next_pass()

        _, listing = conn_local.get_container(s3_migration['container'])
        key_entry = [x for x in listing if x['name'] == key]
        self.assertEqual(1, len(key_entry))
        key_entry = key_entry[0]
        slo_hdrs, slo_body = conn_local.get_object(
            s3_migration['container'], key)
        self.assertTrue(content == slo_body)
        _, body = conn_local.get_object(
            s3_migration['container'], key,
            query_string='multipart-manifest=get&format=raw')
        _, listing = conn_local.get_container(
            s3_migration['container'] + '_segments')
        names = [x['name'] for x in listing]
        self.assertEqual(3, len(names))
        for entry in json.loads(body):
            self.assertIn(entry['path'].split('/', 2)[2], names)
        self.s3('delete_object', Bucket=s3_migration['aws_bucket'],
                Key=key)
        # Clean up (these aren't standard config)
        conn = self.conn_for_acct_noshunt(s3_migration['account'])
        container = s3_migration['container']
        clear_swift_container(conn, container)
        clear_swift_container(conn, container + '_segments')
        conn.delete_container(container + '_segments')

    def test_s3_mpu_migration(self):
        if not self.has_aws:
            raise unittest.SkipTest("AWS Credentials not defined.")
        if not self.run_long_tests:
            raise unittest.SkipTest("Skipping long test")
        # Create mpu on S3
        key = 'mpu_test_' + str(uuid.uuid4())
        part_size = 5 * 2**20
        parts = 2
        self._upload_mpu(key=key, part_size=part_size)
        # Run migrator with custom migration definition
        s3_mig = self.s3_migration()
        migration = {
            'account': s3_mig['account'],
            'aws_bucket': self.aws_bucket,
            'aws_identity': self.aws_identity,
            'aws_secret': self.aws_secret,
            'container': s3_mig['container'],
            'older_than': 0,
            'prefix': '',
            'propagate_account_metadata': False,
            'protocol': 's3',
            'remote_account': '',
        }
        config = {
            'migrations': [migration],
            'migrator_settings': {
                'items_chunk': 1000,
                'log_file': '/var/log/swift-s3-migrator.log',
                'log_level': 'debug',
                'poll_interval': 300,
                'process': 0,
                'processes': 1,
                'status_file': '/var/lib/swift-s3-sync/migrator.status',
                'workers': 10,
            },
        }
        conn_local = self.conn_for_acct(s3_mig['account'])
        with tempfile.NamedTemporaryFile() as fp:
            json.dump(config, fp)
            fp.flush()
            status = migrator_utils.TempMigratorStatus(migration)
            migrator = migrator_utils.MigratorFactory(
                conf_path=fp.name).get_migrator(migration, status)
        migrator.next_pass()
        _, listing = conn_local.get_container(migration['container'])
        key_entry = [x for x in listing if x['name'] == key]
        self.assertEqual(1, len(key_entry))
        key_entry = key_entry[0]
        slo_hdrs, slo_body = conn_local.get_object(
            migration['container'], key)
        content = 'A' * (parts * part_size)
        self.assertTrue(content == slo_body)
        _, body = conn_local.get_object(
            migration['container'], key,
            query_string='multipart-manifest=get&format=raw')
        _, listing = conn_local.get_container(
            migration['container'] + '_segments')
        names = [x['name'] for x in listing]
        for entry in json.loads(body):
            self.assertIn(entry['path'].split('/', 2)[2], names)
        # Clean up (these aren't standard config)
        conn = self.conn_for_acct_noshunt(s3_mig['account'])
        container = migration['container']
        clear_swift_container(conn, container)
        clear_swift_container(conn, container + '_segments')
        conn.delete_container(container)
        conn.delete_container(container + '_segments')
