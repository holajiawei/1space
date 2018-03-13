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


import json
import StringIO
import swiftclient
import urllib
from . import (TestCloudSyncBase, clear_swift_container, clear_s3_bucket,
               wait_for_condition)


class TestMigrator(TestCloudSyncBase):
    @classmethod
    def _find_migration(klass, matcher):
        for migration in klass.test_conf['migrations']:
            if matcher(migration):
                return migration
        raise RuntimeError('No matching migration')

    @classmethod
    def s3_migration(klass):
        return klass._find_migration(lambda cont: cont['protocol'] == 's3')

    @classmethod
    def swift_migration(klass):
        return klass._find_migration(lambda cont: cont['protocol'] == 'swift')

    def test_s3_migration(self):
        migration = self.s3_migration()

        test_objects = [
            ('s3-blob', 's3 content', {}),
            ('s3-unicod\u00e9', '\xde\xad\xbe\xef', {}),
            ('s3-with-headers', 'header-blob', {'custom-header': 'value',
                                                'unicod\u00e9': '\u262f'})]

        def _check_objects_copied():
            hdrs, listing = self.local_swift(
                'get_container', migration['container'])
            swift_names = [obj['name'] for obj in listing]
            return set([obj[0] for obj in test_objects]) == set(swift_names)

        for name, body, headers in test_objects:
            self.s3('put_object', Bucket=migration['aws_bucket'], Key=name,
                    Body=StringIO.StringIO(body), Metadata=headers)

        wait_for_condition(5, _check_objects_copied)

        for name, expected_body, user_meta in test_objects:
            hdrs, body = self.local_swift(
                'get_object', migration['container'], name)
            self.assertEqual(body, body)
            for k, v in user_meta.items():
                self.assertIn('x-object-meta-' + k, hdrs)
                self.assertEqual(v, hdrs['x-object-meta-' + k])

        for name, expected_body, user_meta in test_objects:
            resp = self.s3('get_object', Bucket=migration['aws_bucket'],
                           Key=name)
            self.assertEqual(user_meta, resp['Metadata'])
            self.assertEqual(expected_body, resp['Body'].read())

        clear_s3_bucket(self.s3_client, migration['aws_bucket'])
        clear_swift_container(self.swift_src, migration['container'])

    def test_swift_migration(self):
        migration = self.swift_migration()

        test_objects = [
            ('swift-blob', 'blob content', {}),
            ('swift-unicod\u00e9', '\xde\xad\xbe\xef', {}),
            ('swift-with-headers',
             'header-blob',
             {'x-object-meta-custom-header': 'value',
              'x-object-meta-unicod\u00e9': '\u262f'})]

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
                self.assertEqual(body, body)
                for k, v in user_meta.items():
                    self.assertIn(k, hdrs)
                    self.assertEqual(v, hdrs[k])

        clear_swift_container(self.swift_dst, migration['aws_bucket'])
        clear_swift_container(self.swift_src, migration['container'])

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

        clear_swift_container(self.swift_dst, migration['aws_bucket'])
        clear_swift_container(self.swift_dst, segments_container)
        clear_swift_container(self.swift_src, migration['container'])
        clear_swift_container(self.swift_src, segments_container)

    def test_container_meta(self):
        migration = self._find_migration(
            lambda cont: cont['container'] == 'no-auto-acl')

        self.remote_swift('put_container', migration['aws_bucket'],
                          headers={'x-container-read': 'AUTH_test2',
                                   'x-container-write': 'AUTH_test2',
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
