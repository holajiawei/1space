"""
Copyright 2019 SwiftStack

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
import logging
import lxml
import mock
import StringIO
import tempfile
import unittest

from swift.common import swob

from s3_sync.base_sync import ProviderResponse
from s3_sync import shunt
from s3_sync import sync_s3
from s3_sync import sync_swift
from s3_sync import utils
from .utils import FakeSwift


class TestShunt(unittest.TestCase):
    def _assert_xml_listing(self, xml_string, elements, container=None,
                            account=None):
        def _assert_xml_entry(entry, xml_props):
            for k in entry.keys():
                if k == 'content_location':
                    self.assertTrue(k not in xml_props)
                else:
                    self.assertEqual(entry[k], xml_props[k])

        root = lxml.etree.fromstring(xml_string)
        context = lxml.etree.iterwalk(root, events=("start", "end"))
        element_index = 0
        cur_elem_properties = {}
        for action, elem in context:
            if action == 'end':
                if elem.tag == 'account':
                    self.assertEqual(account, elem.get('name'))
                elif elem.tag == 'container':
                    if container:
                        self.assertEqual(container, elem.get('name'))
                    else:
                        _assert_xml_entry(elements[element_index],
                                          cur_elem_properties)
                        element_index += 1
                elif elem.tag == 'object':
                    _assert_xml_entry(elements[element_index],
                                      cur_elem_properties)
                    element_index += 1
                else:
                    try:
                        int_value = int(elem.text)
                        cur_elem_properties[elem.tag] = int_value
                    except ValueError:
                        cur_elem_properties[elem.tag] = elem.text

    def setUp(self):
        self.logger = logging.getLogger()
        self.stream = StringIO.StringIO()
        self.logger.addHandler(logging.StreamHandler(self.stream))
        self.patchers = [mock.patch(name) for name in (
            's3_sync.sync_swift.SyncSwift.shunt_object',
            's3_sync.sync_s3.SyncS3.shunt_object',
            's3_sync.sync_swift.SyncSwift.list_objects',
            's3_sync.sync_s3.SyncS3.list_objects')]
        self.mock_shunt_swift = self.patchers[0].__enter__()
        self.mock_shunt_swift.return_value = (
            '200 OK', [
                ('Remote-x-openstack-request-id', 'also some trans id'),
                ('Remote-x-trans-id', 'some trans id'),
                ('CONNECTION', 'bad'),
                ('keep-alive', 'bad'),
                ('proxy-authenticate', 'bad'),
                ('proxy-authorization', 'bad'),
                ('te', 'bad'),
                ('trailer', 'bad'),
                ('Transfer-Encoding', 'bad'),
                ('Upgrade', 'bad'),
                ('Content-Length', len('remote swift')),
                ('etag', 'deadbeef')
            ], StringIO.StringIO('remote swift'))
        self.mock_shunt_s3 = self.patchers[1].__enter__()
        self.mock_shunt_s3.return_value = (
            '200 OK', [
                ('Remote-x-amz-id-2', 'also some trans id'),
                ('Remote-x-amz-request-id', 'some trans id'),
                ('CONNECTION', 'bad'),
                ('keep-alive', 'bad'),
                ('proxy-authenticate', 'bad'),
                ('proxy-authorization', 'bad'),
                ('te', 'bad'),
                ('trailer', 'bad'),
                ('Transfer-Encoding', 'bad'),
                ('Upgrade', 'bad'),
                ('etag', 'deadbeef')], ['remote s3'])

        self.mock_list_swift = self.patchers[2].__enter__()
        self.mock_list_s3 = self.patchers[3].__enter__()

        self.conf = {
            'containers': [
                {
                    'account': 'AUTH_a',
                    'container': u'sw\u00e9ft',
                    'merge_namespaces': True,
                    'propagate_delete': False,
                    'protocol': 'swift',
                    'aws_bucket': 'dest-container',
                    'aws_identity': 'user',
                    'aws_secret': 'key',
                    'aws_endpoint': 'https://swift.example.com/auth/v1.0',
                    'restore_object': True,
                },
                {
                    'account': 'AUTH_a',
                    'container': 's3',
                    'merge_namespaces': True,
                    'propagate_delete': False,
                    'aws_bucket': 'dest-bucket',
                    'aws_identity': 'user',
                    'aws_secret': 'key',
                },
                {
                    'account': 'AUTH_a',
                    'container': 's3prop',
                    'propagate_delete': True,
                    'merge_namespaces': True,
                    'aws_bucket': 'dest_prop',
                    'aws_identity': 'user',
                    'aws_secret': 'key',
                },
                {
                    'account': 'AUTH_b',
                    'container': '/*',
                    'merge_namespaces': True,
                    'propagate_delete': False,
                    'aws_bucket': 'dest-bucket',
                    'aws_identity': 'user',
                    'aws_secret': 'key',
                },
                {
                    'account': 'AUTH_tee',
                    'container': 'tee',
                    'merge_namespaces': True,
                    'propagate_delete': False,
                    'restore_object': True,
                    'aws_bucket': 'dest-bucket',
                    'aws_identity': 'user',
                    'aws_secret': 'key',
                }],
            'migrations': [
                {'account': 'AUTH_migrate',
                 'container': 'destination',
                 'aws_bucket': 'source',
                 'aws_identity': 'migration',
                 'aws_secret': 'migration_key',
                 'protocol': 's3'},
                {'account': 'AUTH_migrate-star',
                 'container': '/*',
                 'aws_bucket': '/*',
                 'aws_identity': 'migration',
                 'aws_secret': 'migration_key',
                 'protocol': 's3'},
            ],
            'migrator_settings': {
                'segment_size': 3000
            }
        }

        with tempfile.NamedTemporaryFile() as fp:
            json.dump(self.conf, fp)
            fp.flush()
            self.swift = FakeSwift()
            self.app = shunt.filter_factory(
                {'conf_file': fp.name})(self.swift)

    def tearDown(self):
        for patcher in self.patchers:
            patcher.__exit__()

    def pop_log_lines(self):
        lines = self.stream.getvalue()
        self.stream.seek(0)
        self.stream.truncate()
        return lines

    def test_bad_config_noops(self):
        app = shunt.filter_factory(
            {'conf_file': '/etc/doesnt/exist'})(FakeSwift()).shunted_app
        self.assertEqual(app.sync_profiles, {})

        with tempfile.NamedTemporaryFile() as fp:
            # empty
            app = shunt.filter_factory(
                {'conf_file': fp.name})(FakeSwift()).shunted_app
            self.assertEqual(app.sync_profiles, {})

            # not json
            fp.write('{"containers":')
            fp.flush()
            app = shunt.filter_factory(
                {'conf_file': fp.name})(FakeSwift()).shunted_app
            self.assertEqual(app.sync_profiles, {})

    def test_init(self):
        self.maxDiff = None
        self.assertEqual(self.app.shunted_app.sync_profiles, {
            ('AUTH_a', 'sw\xc3\xa9ft'): {
                'account': 'AUTH_a',
                'container': 'sw\xc3\xa9ft'.decode('utf-8'),
                'merge_namespaces': True,
                'propagate_delete': False,
                'protocol': 'swift',
                'aws_bucket': 'dest-container',
                'aws_identity': 'user',
                'aws_secret': 'key',
                'aws_endpoint': 'https://swift.example.com/auth/v1.0',
                'restore_object': True,
            },
            ('AUTH_a', 's3'): {
                'account': 'AUTH_a',
                'container': 's3',
                'merge_namespaces': True,
                'propagate_delete': False,
                'aws_bucket': 'dest-bucket',
                'aws_identity': 'user',
                'aws_secret': 'key',
            },
            ('AUTH_a', 's3prop'): {
                'account': 'AUTH_a',
                'container': 's3prop',
                'propagate_delete': True,
                'merge_namespaces': True,
                'aws_bucket': 'dest_prop',
                'aws_identity': 'user',
                'aws_secret': 'key',
            },
            ('AUTH_b', '/*'): {
                'account': 'AUTH_b',
                'container': '/*',
                'merge_namespaces': True,
                'propagate_delete': False,
                'aws_bucket': 'dest-bucket',
                'aws_identity': 'user',
                'aws_secret': 'key',
            },
            ('AUTH_migrate', 'destination'): {
                'account': 'AUTH_migrate',
                'container': 'destination',
                'migration': True,
                'restore_object': True,
                'aws_bucket': 'source',
                'aws_identity': 'migration',
                'aws_secret': 'migration_key',
                'custom_prefix': '',
                'protocol': 's3'
            },
            ('AUTH_migrate-star', '/*'): {
                'account': 'AUTH_migrate-star',
                'container': '/*',
                'migration': True,
                'restore_object': True,
                'aws_bucket': '/*',
                'aws_identity': 'migration',
                'aws_secret': 'migration_key',
                'custom_prefix': '',
                'protocol': 's3'
            },
            ('AUTH_tee', 'tee'): {
                'account': 'AUTH_tee',
                'container': 'tee',
                'merge_namespaces': True,
                'propagate_delete': False,
                'restore_object': True,
                'aws_bucket': 'dest-bucket',
                'aws_identity': 'user',
                'aws_secret': 'key',
            },
        })

    def test_init_with_migrations(self):
        migrations = [
            {
                'account': 'AUTH_s3',
                'protocol': 's3',
                'aws_endpoint': '',
                'aws_identity': 'my-id',
                'aws_secret': 's3kr!t',
                'aws_bucket': 'some-bucket',
            },
            {
                'account': 'AUTH_all-their-containers',
                'protocol': 'swift',
                'aws_endpoint': 'http://saio:8080/auth/v1.0',
                'aws_identity': 'test:tester',
                'aws_secret': 'testing',
                'aws_bucket': '/*',
                'remote_account': 'AUTH_all-my-containers',
            },
        ]
        with tempfile.NamedTemporaryFile() as fp:
            json.dump({"migrations": migrations}, fp)
            fp.flush()
            app = shunt.filter_factory(
                {'conf_file': fp.name})(self.swift).shunted_app
        self.assertEqual(app.sync_profiles, {
            ('AUTH_s3', 'some-bucket'): {
                'account': 'AUTH_s3',
                'protocol': 's3',
                'aws_endpoint': '',
                'aws_identity': 'my-id',
                'aws_secret': 's3kr!t',
                'aws_bucket': 'some-bucket',
                # Wasn't present before! But since we *migrating*,
                # assume that we *want the data to move*
                'restore_object': True,
                # Also, inserted just for the migration, as we don't want to
                # use the sharded S3 namespace
                'custom_prefix': '',
                # Need a container key, or the provider balks. Migrations move
                # data from one name to the same name, so crib from aws_bucket
                'container': 'some-bucket',
                'migration': True,
            },
            ('AUTH_all-their-containers', '/*'): {
                'account': 'AUTH_all-their-containers',
                'protocol': 'swift',
                'aws_endpoint': 'http://saio:8080/auth/v1.0',
                'aws_identity': 'test:tester',
                'aws_secret': 'testing',
                'aws_bucket': '/*',
                'remote_account': 'AUTH_all-my-containers',
                'restore_object': True,
                'container': '/*',
                'custom_prefix': '',
                'migration': True,
            },
        })

    @mock.patch('s3_sync.shunt.getmtime')
    @mock.patch('s3_sync.shunt.time')
    def test_init_and_reload(self, mock_time, mock_getmtime):
        # mock time and gmtime
        mock_time.side_effect = [100, 101, 102, 103]
        mock_getmtime.side_effect = [50, 50, 102, 103]
        # load init and verify
        conf = {
            'containers': [
                {
                    'account': 'AUTH_a',
                    'container': u'sw\u00e9ft',
                    'merge_namespaces': True,
                    'propagate_delete': False,
                    'protocol': 'swift',
                    'aws_bucket': 'dest-container',
                    'aws_identity': 'user',
                    'aws_secret': 'key',
                    'aws_endpoint': 'https://swift.example.com/auth/v1.0',
                    'restore_object': True,
                }]}
        profiles = {
            ('AUTH_a', 'sw\xc3\xa9ft'): {
                'account': 'AUTH_a',
                'container': 'sw\xc3\xa9ft'.decode('utf-8'),
                'merge_namespaces': True,
                'propagate_delete': False,
                'protocol': 'swift',
                'aws_bucket': 'dest-container',
                'aws_identity': 'user',
                'aws_secret': 'key',
                'aws_endpoint': 'https://swift.example.com/auth/v1.0',
                'restore_object': True,
            }
        }
        with tempfile.NamedTemporaryFile() as fp:
            json.dump(conf, fp)
            fp.flush()
            app = shunt.filter_factory(
                {'conf_file': fp.name})(self.swift).shunted_app
            self.assertEqual(app.sync_profiles, profiles)
            self.assertEqual(app._mtime, 50)
            self.assertEqual(app._rtime, 100 + app.reload_time)
            conf['containers'][0]['aws_secret'] = 'key2'
            fp.seek(0)
            json.dump(conf, fp)
            fp.flush()
            app._reload()
            self.assertEqual(app._mtime, 50)
            self.assertEqual(app._rtime, 101 + app.reload_time)
            self.assertEqual(app.sync_profiles, profiles)
            app._reload()
            self.assertEqual(app._mtime, 102)
            self.assertEqual(app._rtime, 102 + app.reload_time)
            profiles[('AUTH_a', 'sw\xc3\xa9ft')]['aws_secret'] = 'key2'
            self.assertEqual(app.sync_profiles, profiles)
            fp.seek(0)
            json.dump({}, fp)
            fp.flush()
            app._reload()
            self.assertEqual(app._mtime, 103)
            self.assertEqual(app._rtime, 103 + app.reload_time)
            self.assertEqual(app.sync_profiles, {})

    def test_unshunted_requests(self):
        def _do_test(path, method='GET'):
            req = swob.Request.blank(path, method=method, environ={
                'swift.trans_id': 'local trans id'})
            req.call_application(self.app)
            self.assertEqual(self.mock_shunt_swift.mock_calls, [])
            self.assertEqual(self.mock_shunt_s3.mock_calls, [])

        def _test_methods(path, methods=('OPTIONS', 'GET', 'HEAD', 'PUT',
                                         'POST', 'DELETE', 'COPY')):
            if 'OPTIONS' in methods:
                _do_test(path, 'OPTIONS')
            if 'GET' in methods:
                _do_test(path, 'GET')
            if 'HEAD' in methods:
                _do_test(path, 'HEAD')
            if 'PUT' in methods:
                _do_test(path, 'PUT')
            if 'POST' in methods:
                _do_test(path, 'POST')
            if 'DELETE' in methods:
                _do_test(path, 'DELETE')
            # doesn't necessarily apply to all paths, but whatever
            if 'COPY' in methods:
                _do_test(path, 'COPY')

        # Only shunt object GETs
        _test_methods('/some/weird/non/swift/path')
        _test_methods('/v1/AUTH_a')
        _test_methods('/v1/AUTH_a/')
        # Not an affected container
        _test_methods('/v1/AUTH_a/c')
        _test_methods('/v1/AUTH_a/c/')
        _test_methods('/v1/AUTH_a/c/o')
        # Affected container, but not relevant methods
        _test_methods(u'/v1/AUTH_a/sw\u00e9ft',
                      ('OPTIONS', 'HEAD', 'PUT', 'POST', 'DELETE'))
        _test_methods(u'/v1/AUTH_a/sw\u00e9ft/',
                      ('OPTIONS', 'HEAD', 'PUT', 'POST', 'DELETE'))
        _test_methods('/v1/AUTH_a/s3',
                      ('OPTIONS', 'HEAD', 'PUT', 'POST', 'DELETE'))
        _test_methods('/v1/AUTH_a/s3/',
                      ('OPTIONS', 'HEAD', 'PUT', 'POST', 'DELETE'))
        _test_methods(u'/v1/AUTH_a/sw\u00e9ft/o',
                      ('OPTIONS', 'PUT', 'POST', 'DELETE'))
        _test_methods('/v1/AUTH_a/s3/o',
                      ('OPTIONS', 'PUT', 'POST', 'DELETE'))

    @mock.patch.object(sync_swift.SyncSwift, 'get_manifest')
    @mock.patch.object(sync_s3.SyncS3, 'get_manifest')
    def test_object_shunt(self, mock_s3_manifest, mock_swift_manifest):

        self.app.shunted_app.logger = self.logger

        def _test_no_shunt(path, status):
            req = swob.Request.blank(path, environ={
                '__test__.status': status,
                'swift.trans_id': 'local trans id'})
            req.call_application(self.app)
            self.assertEqual(self.mock_shunt_swift.mock_calls, [])
            self.assertEqual(self.mock_shunt_s3.mock_calls, [])
        # Just plain bad; crazy talk
        _test_no_shunt('/v1//', '404 Not Found')
        _test_no_shunt('/not/a/swift/request', '404 Not Found')
        # Not an affected container
        _test_no_shunt('/v1/AUTH_a/c/o', '404 Not Found')
        _test_no_shunt('/v1/AUTH_a/c/o', '404 Not Found')
        # Affected container, but not 404
        _test_no_shunt(u'/v1/AUTH_a/sw\u00e9ft/o', '200 OK')
        _test_no_shunt('/v1/AUTH_a/s3/o', '200 OK')
        _test_no_shunt(u'/v1/AUTH_a/sw\u00e9ft/o', '503 Not Available')
        _test_no_shunt('/v1/AUTH_a/s3/o', '400 Bad Request')

        # Do the shunt!
        def _test_shunted(path, expect_s3):
            req = swob.Request.blank(path, environ={
                '__test__.status': '404 Not Found',
                'swift.trans_id': 'local trans id'})
            status, headers, body_iter = req.call_application(self.app)
            if expect_s3:
                self.assertEqual(
                    self.mock_shunt_swift.mock_calls, [])
                self.assertEqual(self.mock_shunt_s3.mock_calls, [
                    mock.call(mock.ANY, path.split('/', 4)[4])])
                received_req = self.mock_shunt_s3\
                    .mock_calls[0][1][0]
                self.assertEqual(req.environ, received_req.environ)
                self.assertEqual(status, '200 OK')
                self.assertEqual(headers, [
                    ('Remote-x-amz-id-2', 'also some trans id'),
                    ('Remote-x-amz-request-id', 'some trans id'),
                    ('etag', 'deadbeef'),
                ])
                self.assertEqual(b''.join(body_iter), b'remote s3')
                self.mock_shunt_s3.reset_mock()
            else:
                self.assertEqual(self.mock_shunt_s3.mock_calls, [])
                self.assertEqual(self.mock_shunt_swift.mock_calls, [
                    mock.call(mock.ANY, path.split('/', 4)[4])])
                received_req = self.mock_shunt_swift\
                    .mock_calls[0][1][0]
                self.assertEqual(req.environ, received_req.environ)
                self.assertEqual(status, '200 OK')
                self.assertEqual(headers, [
                    ('Remote-x-openstack-request-id', 'also some trans id'),
                    ('Remote-x-trans-id', 'some trans id'),
                    ('Content-Length', '12'),
                    ('etag', 'deadbeef'),
                ])
                self.assertEqual(b''.join(body_iter), b'remote swift')
                self.mock_shunt_swift.reset_mock()
            log_lines = self.pop_log_lines()
            self.assertNotIn('key', log_lines)
            self.assertIn("(redacted)", log_lines)
        _test_shunted(u'/v1/AUTH_a/sw\u00e9ft/o', False)
        _test_shunted('/v1/AUTH_a/s3/o', True)
        _test_shunted('/v1/AUTH_a/s3prop/o', True)
        _test_shunted('/v1/AUTH_b/c1/o', True)
        _test_shunted('/v1/AUTH_b/c2/o', True)

    @mock.patch.object(sync_swift.SyncSwift, 'get_manifest')
    @mock.patch.object(sync_s3.SyncS3, 'get_manifest')
    @mock.patch.object(sync_swift.SyncSwift, 'shunt_object')
    @mock.patch.object(sync_s3.SyncS3, 'shunt_object')
    def test_tee(self, mock_s3_shunt, mock_swift_shunt, mock_s3_get_manifest,
                 mock_swift_get_manifest):
        payload = 'bytes from remote'
        responses = [
            ('AUTH_tee/tee',
             ('200 OK',
              [('Content-Length', len(payload)),
               (utils.SLO_HEADER, 'True'),
               ('etag', 'deadbeef-2')],
              StringIO.StringIO(payload)),
             mock_s3_shunt, True),
            (u'AUTH_a/sw\u00e9ft',
             ('200 OK',
              [('Content-Length', len(payload)),
               (utils.SLO_HEADER, 'True'),
               ('etag', 'etag')],
              StringIO.StringIO(payload)),
             mock_swift_shunt, True),
            ('AUTH_tee/tee',
             ('200 OK', [('Content-Length', len(payload)), ('etag', 'etag')],
              StringIO.StringIO(payload)),
             mock_s3_shunt, True),
            (u'AUTH_a/sw\u00e9ft',
             ('200 OK', [('Content-Length', len(payload)), ('etag', 'etag')],
              StringIO.StringIO(payload)),
             mock_swift_shunt, True)
        ]

        env = {
            '__test__.response_dict': {
                'GET': {
                    'status': '404 Not Found'
                }
            }
        }

        for path, resp, mock_call, is_put_back in responses:
            if dict(resp[1])['etag'].endswith('-2') or\
                    utils.SLO_HEADER in dict(resp[1]):
                is_slo = True
            else:
                is_slo = False
            if is_slo:
                manifest = [{
                    'bytes': len(payload),
                    'name': '/segments/part1',
                    'hash': 'etag'}]
                mock_s3_get_manifest.return_value = manifest
                mock_swift_get_manifest.return_value = manifest
            mock_call.return_value = resp
            req = swob.Request.blank(u'/v1/%s/foo' % path, environ=env)
            status, headers, body_iter = req.call_application(self.app)
            resp_body = b''.join(body_iter)
            path = path.encode('utf-8')
            account = path.split('/', 1)[0]
            if not is_put_back:
                self.assertEqual(
                    [(e['REQUEST_METHOD'], e['PATH_INFO'])
                     for e in self.swift.calls],
                    [
                        ('HEAD', '/v1/%s' % account),
                        ('GET', '/v1/%s/foo' % path),
                    ])
            elif is_slo:
                self.assertEqual(
                    [(e['REQUEST_METHOD'], e['PATH_INFO'])
                     for e in self.swift.calls],
                    [
                        ('HEAD', '/v1/%s' % account),
                        ('GET', '/v1/%s/foo' % path),
                        ('PUT', '/v1/%s/segments' % account),
                        ('PUT', '/v1/%s/segments/part1' % account),
                        ('PUT', '/v1/%s/foo' % path),
                    ])
                self.assertEqual('multipart-manifest=put',
                                 self.swift.calls[-1]['QUERY_STRING'])
            else:
                self.assertEqual(
                    [(e['REQUEST_METHOD'], e['PATH_INFO'])
                     for e in self.swift.calls],
                    [
                        ('HEAD', '/v1/%s' % account),
                        ('GET', '/v1/%s/foo' % path),
                        ('PUT', '/v1/%s/foo' % path),
                    ])
            self.assertEqual(payload, resp_body)
            mock_call.reset_mock()
            self.swift.calls = []

    @mock.patch.object(sync_s3.SyncS3, 'head_object')
    @mock.patch.object(sync_s3.SyncS3, 'shunt_object')
    def test_tee_migration_mpu(self, mock_shunt_object, mock_head_object):
        payload = 'content' * 1024
        parts = [3000, 3000, len(payload) - 6000]
        parts_etags = []
        offset = 0
        for part_size in parts:
            parts_etags.append(hashlib.md5(
                payload[offset:offset + part_size]).digest())
            offset += part_size
        etag = '%s-%d' % (hashlib.md5(''.join(parts_etags)).hexdigest(),
                          len(parts))

        mock_shunt_object.return_value = (
            '200 OK',
            [('Content-Length', len(payload)), ('etag', etag),
             ('x-static-large-object', True),
             ('last-modified', 'Thu, 16 Jan 2014 21:12:31 GMT')],
            StringIO.StringIO(payload))

        def _head_object(*args, **kwargs):
            return ProviderResponse(
                True, 200,
                {'Content-Length': parts[kwargs['PartNumber'] - 1]}, '')

        def _part_segment(swift_path, number):
            return '/v1/%s_segments/foo/%0.1f/%d/%d/%d' % (
                swift_path, 1389906751.0, len(payload), 3000, number)

        path = 'AUTH_migrate/destination'
        account = path.split('/', 1)[0]

        mock_head_object.side_effect = _head_object
        env = {
            '__test__.response_dict': {
                'GET': {
                    'status': '404 Not Found'
                },
            }
        }
        self.swift.responses = {'PUT': [
            lambda headers, body: ('200 OK', {}, '')] +
            [lambda headers, body: (
                '200 OK', {'ETag': hashlib.md5(body).hexdigest()}, '')] * 3 +
            [lambda headers, body: ('200 OK', {}, '')]}
        req = swob.Request.blank(u'/v1/%s/foo' % path, environ=env)
        status, headers, body_iter = req.call_application(self.app)
        resp_body = b''.join(body_iter)
        self.assertEqual([
            ('HEAD', '/v1/%s' % account),
            ('GET', '/v1/%s/foo' % path),
            ('HEAD', '/v1/%s' % path),
            ('PUT', '/v1/%s_segments' % path),
            ('PUT', _part_segment(path, 0)),
            ('PUT', _part_segment(path, 1)),
            ('PUT', _part_segment(path, 2)),
            ('PUT', '/v1/%s/foo' % path)
        ],
            [(e['REQUEST_METHOD'], e['PATH_INFO'])
             for e in self.swift.calls])
        self.assertEqual('multipart-manifest=put',
                         self.swift.calls[-1]['QUERY_STRING'])
        self.assertEqual(payload, resp_body)

    @mock.patch('s3_sync.shunt.constraints')
    @mock.patch.object(sync_s3.SyncS3, 'shunt_object')
    def test_tee_migration_big(self, mock_shunt_object, mock_constraints):
        self.conf['migrator_settings'] = {'segment_size': 3000}
        mock_constraints.EFFECTIVE_CONSTRAINTS = {'max_file_size': 3000}
        payload = 'content' * 1024
        etag = hashlib.md5(payload).hexdigest()

        mock_shunt_object.return_value = (
            '200 OK',
            [('Content-Length', len(payload)), ('etag', etag),
             ('last-modified', 'Thu, 16 Jan 2014 21:12:31 GMT')],
            StringIO.StringIO(payload))

        def _part_segment(swift_path, number):
            return '/v1/%s_segments/foo/%0.1f/%d/%d/%d' % (
                swift_path, 1389906751.0, len(payload), 3000, number)

        path = 'AUTH_migrate/destination'
        account = path.split('/', 1)[0]

        env = {
            '__test__.response_dict': {
                'GET': {
                    'status': '404 Not Found'
                },
            }
        }
        self.swift.responses = {'PUT': [
            lambda headers, body: ('200 OK', {}, '')] +
            [lambda headers, body: (
                '200 OK', {'ETag': hashlib.md5(body).hexdigest()}, '')] * 3 +
            [lambda headers, body: ('200 OK', {}, '')]}
        req = swob.Request.blank(u'/v1/%s/foo' % path, environ=env)
        status, headers, body_iter = req.call_application(self.app)
        resp_body = b''.join(body_iter)
        self.assertEqual([
            ('HEAD', '/v1/%s' % account),
            ('GET', '/v1/%s/foo' % path),
            ('HEAD', '/v1/%s' % path),
            ('PUT', '/v1/%s_segments' % path),
            ('PUT', _part_segment(path, 0)),
            ('PUT', _part_segment(path, 1)),
            ('PUT', _part_segment(path, 2)),
            ('PUT', '/v1/%s/foo' % path)
        ],
            [(e['REQUEST_METHOD'], e['PATH_INFO'])
             for e in self.swift.calls])
        self.assertEqual('multipart-manifest=put',
                         self.swift.calls[-1]['QUERY_STRING'])
        self.assertEqual(payload, resp_body)

    @mock.patch.object(sync_s3.SyncS3, 'get_manifest')
    @mock.patch.object(sync_s3.SyncS3, 'shunt_object')
    def test_missing_slo_manifest(
            self, mock_s3_shunt, mock_s3_get_manifest):
        payload = 'bytes from remote'
        path = 'AUTH_tee/tee'
        resp = ('200 OK',
                [('Content-Length', len(payload)),
                 (utils.SLO_HEADER, 'True'),
                 ('etag', 'deadbeef-2')],
                StringIO.StringIO(payload))

        env = {
            '__test__.response_dict': {
                'GET': {
                    'status': '404 Not Found'
                }
            }
        }

        # the manifest is missing
        mock_s3_get_manifest.return_value = None
        mock_s3_shunt.return_value = resp
        req = swob.Request.blank(u'/v1/%s/foo' % path, environ=env)
        status, headers, body_iter = req.call_application(self.app)
        resp_body = b''.join(body_iter)
        path = path.encode('utf-8')
        account = path.split('/', 1)[0]
        self.assertEqual(
            [(e['REQUEST_METHOD'], e['PATH_INFO'])
             for e in self.swift.calls],
            [
                ('HEAD', '/v1/%s' % account),
                ('GET', '/v1/%s/foo' % path),
            ])
        self.assertEqual(payload, resp_body)

    def test_list_container_no_shunt(self):
        req = swob.Request.blank(
            '/v1/AUTH_a/foo',
            environ={'__test__.status': '200 OK',
                     'swift.trans_id': 'id'})
        req.call_application(self.app)

        self.assertEqual(self.mock_list_swift.mock_calls, [])
        self.assertEqual(self.mock_list_s3.mock_calls, [])

    def test_list_container_shunt_s3(self):
        self.mock_list_s3.side_effect = [
            ProviderResponse(
                True, 200, {},
                [{'name': 'abc',
                  'hash': 'ffff',
                  'bytes': 42,
                  'last_modified': 'date',
                  'content_type': 'type',
                  'content_location': 'mock-s3:bucket'},
                 {'name': u'unicod\xe9',
                  'hash': 'ffff',
                  'bytes': 1000,
                  'last_modified': 'date',
                  'content_type': 'type',
                  'content_location': 'mock-s3:bucket'}]),
            ProviderResponse(True, 200, {}, [])]
        req = swob.Request.blank(
            '/v1/AUTH_a/s3',
            environ={'__test__.status': '200 OK',
                     '__test__.body': '[]',
                     'swift.trans_id': 'id'})
        status, headers, body_iter = req.call_application(self.app)
        self.assertEqual(self.mock_shunt_swift.mock_calls, [])
        self.mock_list_s3.assert_has_calls([
            mock.call(marker='', limit=10000, prefix='', delimiter=''),
            mock.call(marker='unicod\xc3\xa9', limit=10000, prefix='',
                      delimiter='')])
        names = body_iter.split('\n')
        self.assertEqual(['abc', u'unicod\xe9'.encode('utf-8')], names)

    def test_list_container_shunt_s3_xml(self):
        elements = [{'name': 'abc',
                     'hash': 'ffff',
                     'bytes': 42,
                     'last_modified': 'date',
                     'content_type': 'type',
                     'content_location': 'http://some-swift'},
                    {'name': u'unicod\xc3\xa9',
                     'hash': 'ffff',
                     'bytes': 1000,
                     'last_modified': 'date',
                     'content_type': 'type',
                     'content_location': 'http://some-swift'}]
        self.mock_list_s3.side_effect = [
            ProviderResponse(True, 200, {}, elements),
            ProviderResponse(True, 200, {}, [])]
        req = swob.Request.blank(
            '/v1/AUTH_a/s3?format=xml',
            environ={'__test__.status': '200 OK',
                     '__test__.body': '[]',
                     'swift.trans_id': 'id'})
        status, headers, body_iter = req.call_application(self.app)
        self.assertEqual(self.mock_shunt_swift.mock_calls, [])
        self.mock_list_s3.assert_has_calls([
            mock.call(marker='', limit=10000, prefix='', delimiter=''),
            mock.call(marker=u'unicod\xc3\xa9'.encode('utf-8'), limit=10000,
                      prefix='', delimiter='')])
        self._assert_xml_listing(body_iter, elements, 's3')

    def test_list_container_accept_xml(self):
        elements = [{'name': 'abc',
                     'hash': 'ffff',
                     'bytes': 42,
                     'last_modified': 'date',
                     'content_type': 'type',
                     'content_location': 'mock-s3:bucket'},
                    {'name': u'unicod\xc3\xa9',
                     'hash': 'ffff',
                     'bytes': 1000,
                     'last_modified': 'date',
                     'content_type': 'type',
                     'content_location': 'mock-s3:bucket'}]
        mock_result = [dict(entry) for entry in elements]
        self.mock_list_s3.side_effect = [
            ProviderResponse(True, 200, {}, mock_result),
            ProviderResponse(True, 200, {}, [])]
        req = swob.Request.blank(
            '/v1/AUTH_a/s3',
            environ={'__test__.status': '200 OK',
                     '__test__.body': '[]',
                     'swift.trans_id': 'id'},
            headers={'Accept': 'application/xml'})
        status, headers, body_iter = req.call_application(self.app)
        self.assertEqual(self.mock_shunt_swift.mock_calls, [])
        self.mock_list_s3.assert_has_calls([
            mock.call(marker='', limit=10000, prefix='', delimiter=''),
            mock.call(marker=u'unicod\xc3\xa9'.encode('utf-8'), limit=10000,
                      prefix='', delimiter='')])
        self._assert_xml_listing(body_iter, elements, 's3')

    def test_list_container_shunt_s3_json(self):
        elements = [{'name': 'abc',
                     'hash': 'ffff',
                     'bytes': 42,
                     'last_modified': 'date',
                     'content_type': 'type',
                     'content_location': 's3-account:bucket'},
                    {'name': u'unicod\xc3\xa9',
                     'hash': 'ffff',
                     'bytes': 1000,
                     'last_modified': 'date',
                     'content_type': 'type',
                     'content_location': 's3-account:bucket'}]
        mock_result = [dict(entry) for entry in elements]
        self.mock_list_s3.side_effect = [
            ProviderResponse(True, 200, {}, mock_result),
            ProviderResponse(True, 200, {}, [])]
        req = swob.Request.blank(
            '/v1/AUTH_a/s3?format=json',
            environ={'__test__.status': '200 OK',
                     '__test__.body': '[]',
                     'swift.trans_id': 'id'})
        status, headers, body_iter = req.call_application(self.app)
        self.assertEqual(self.mock_shunt_swift.mock_calls, [])
        self.mock_list_s3.assert_has_calls([
            mock.call(marker='', limit=10000, prefix='', delimiter=''),
            mock.call(marker=u'unicod\xc3\xa9'.encode('utf-8'), limit=10000,
                      prefix='', delimiter='')])
        results = json.loads(body_iter)
        for i, entry in enumerate(results):
            for k in entry.keys():
                if k == 'content_location':
                    self.assertEqual([elements[i][k]], entry[k])
                else:
                    self.assertEqual(elements[i][k], entry[k])

    def test_list_container_accept_json(self):
        elements = [{'name': 'abc',
                     'hash': 'ffff',
                     'bytes': 42,
                     'last_modified': 'date',
                     'content_type': 'type',
                     'content_location': 'mock-s3:bucket'},
                    {'name': u'unicod\xc3\xa9',
                     'hash': 'ffff',
                     'bytes': 1000,
                     'last_modified': 'date',
                     'content_type': 'type',
                     'content_location': 'mock-s3:bucket'}]
        mock_result = [dict(entry) for entry in elements]
        self.mock_list_s3.side_effect = [
            ProviderResponse(True, 200, {}, mock_result),
            ProviderResponse(True, 200, {}, [])]
        req = swob.Request.blank(
            '/v1/AUTH_a/s3',
            environ={'__test__.status': '200 OK',
                     '__test__.body': '[]',
                     'swift.trans_id': 'id'},
            headers={'Accept': 'application/json'})
        status, headers, body_iter = req.call_application(self.app)
        self.assertEqual(self.mock_shunt_swift.mock_calls, [])
        self.mock_list_s3.assert_has_calls([
            mock.call(marker='', limit=10000, prefix='', delimiter=''),
            mock.call(marker=u'unicod\xc3\xa9'.encode('utf-8'), limit=10000,
                      prefix='', delimiter='')])
        results = json.loads(body_iter)
        for i, entry in enumerate(results):
            for k in elements[i].keys():
                if k == 'content_location':
                    self.assertEqual([elements[i][k]], entry[k])
                else:
                    self.assertEqual(elements[i][k], entry[k])

    @mock.patch('s3_sync.shunt.create_provider')
    def test_list_container_shunt_all_containers(self, create_mock):
        create_mock.return_value = mock.Mock()
        create_mock.return_value.list_objects.return_value = ProviderResponse(
            True, 200, {}, [])
        req = swob.Request.blank(
            '/v1/AUTH_b/s3',
            environ={'__test__.status': '200 OK',
                     '__test__.body': '[]',
                     'swift.trans_id': 'id'})
        status, headers, body_iter = req.call_application(self.app)
        create_mock.assert_called_once_with({
            'account': 'AUTH_b',
            'container': 's3',
            'merge_namespaces': True,
            'propagate_delete': False,
            'aws_bucket': 'dest-bucket',
            'aws_identity': 'user',
            'aws_secret': 'key'}, max_conns=1, per_account=True)

        # Follow it up with another request to a *different* container to make
        # sure we didn't bleed state
        create_mock.reset_mock()
        req = swob.Request.blank(
            '/v1/AUTH_b/s4',
            environ={'__test__.status': '200 OK',
                     '__test__.body': '[]',
                     'swift.trans_id': 'id'})
        status, headers, body_iter = req.call_application(self.app)
        create_mock.assert_called_once_with({
            'account': 'AUTH_b',
            'container': 's4',
            'merge_namespaces': True,
            'propagate_delete': False,
            'aws_bucket': 'dest-bucket',
            'aws_identity': 'user',
            'aws_secret': 'key'}, max_conns=1, per_account=True)

    def test_list_container_shunt_swift(self):
        self.mock_list_swift.side_effect = [
            ProviderResponse(
                True, 200, {},
                [{'name': 'abc',
                  'hash': 'ffff',
                  'bytes': 42,
                  'last_modified': 'date',
                  'content_type': 'type',
                  'content_location': 'http://some-swift'},
                 {'name': u'unicod\xe9',
                  'hash': 'ffff',
                  'bytes': 1000,
                  'last_modified': 'date',
                  'content_type': 'type',
                  'content_location': 'http://some-swift'}]),
            ProviderResponse(True, 200, {}, [])]
        req = swob.Request.blank(
            '/v1/AUTH_a/sw\xc3\xa9ft',
            environ={'__test__.status': '200 OK',
                     '__test__.body': '[]',
                     'swift.trans_id': 'id'})
        status, headers, body_iter = req.call_application(self.app)
        self.assertEqual(self.mock_shunt_swift.mock_calls, [])
        self.mock_list_swift.assert_has_calls([
            mock.call(marker='', limit=10000, prefix='', delimiter=''),
            mock.call(marker=u'unicod\xe9'.encode('utf-8'), limit=10000,
                      prefix='', delimiter='')])
        names = body_iter.split('\n')
        self.assertEqual(['abc', u'unicod\xe9'.encode('utf-8')], names)

    def test_list_container_shunt_with_duplicates(self):
        self.mock_list_swift.side_effect = [
            ProviderResponse(
                True, 200, {},
                [{'subdir': u'a/',
                  'content_location': 'http://some-swift'},
                 {'name': u'unicod\xe9',
                  'hash': 'ffff',
                  'bytes': 1000,
                  'last_modified': 'date',
                  'content_type': 'type',
                  'content_location': 'http://some-swift'},
                 {'subdir': u'z/',
                  'content_location': 'http://some-swift'},
                 {'name': u'zzzzz',
                  'hash': 'ffff',
                  'bytes': 1000,
                  'last_modified': 'date',
                  'content_type': 'type',
                  'content_location': 'http://some-swift'}]),
            ProviderResponse(True, 200, {}, [])]
        # simulate being partially migrated
        local_data = [
            {'name': u'a',
             'hash': 'ffff',
             'bytes': 42,
             'last_modified': 'date',
             'content_type': 'type'},
            {'name': u'unicod\xe9',
             'hash': 'ffff',
             'bytes': 1000,
             'last_modified': 'date',
             'content_type': 'type'},
        ]
        req = swob.Request.blank(
            '/v1/AUTH_a/sw\xc3\xa9ft?delimiter=/&limit=4',
            environ={'__test__.status': '200 OK',
                     '__test__.body': json.dumps(local_data),
                     'swift.trans_id': 'id'})
        status, headers, body_iter = req.call_application(self.app)
        self.assertEqual(self.mock_shunt_swift.mock_calls, [])
        self.mock_list_swift.assert_called_once_with(
            marker='', limit=4, prefix='', delimiter='/')
        names = body_iter.split('\n')
        self.assertEqual(names, [
            'a', 'a/', u'unicod\xe9'.encode('utf-8'), 'z/',
        ])

    @mock.patch('s3_sync.sync_s3.SyncS3.list_buckets')
    def test_list_account_accept_json(self, mock_list_buckets):
        elements = [{'name': 'abc',
                     'last_modified': 'date',
                     'content_location': 'AWS S3',
                     'count': 0,
                     'bytes': 0},
                    {'name': u'unicod\xc3\xa9',
                     'last_modified': 'date',
                     'content_location': 'AWS S3',
                     'count': 0,
                     'bytes': 0}]
        mock_result = [dict(entry) for entry in elements]
        mock_list_buckets.side_effect = [
            ProviderResponse(True, 200, {}, mock_result),
            ProviderResponse(True, 200, {}, [])]
        req = swob.Request.blank(
            '/v1/AUTH_migrate-star?format=json',
            environ={'__test__.status': '200 OK',
                     '__test__.body': '[]',
                     'swift.trans_id': 'id'})
        status, headers, body_iter = req.call_application(self.app)
        self.assertEqual(self.mock_shunt_swift.mock_calls, [])
        self.assertEqual(
            [mock.call(marker='', limit=10000, prefix='', delimiter=''),
             mock.call(marker=u'unicod\xc3\xa9'.encode('utf-8'), limit=10000,
                       prefix='', delimiter='')],
            mock_list_buckets.mock_calls)
        results = json.loads(body_iter)
        for index, entry in enumerate(results):
            for k, v in entry.items():
                if k == 'content_location':
                    self.assertEqual([elements[index][k]], entry[k])
                else:
                    self.assertEqual(elements[index][k], entry[k])

    @mock.patch('s3_sync.sync_s3.SyncS3.list_buckets')
    def test_list_account_accept_plain(self, mock_list_buckets):
        elements = [{'name': 'abc',
                     'last_modified': 'date',
                     'content_location': 'AWS S3',
                     'count': 0,
                     'bytes': 0},
                    {'name': u'unicod\xc3\xa9',
                     'last_modified': 'date',
                     'content_location': 'AWS S3',
                     'count': 0,
                     'bytes': 0}]
        mock_result = [dict(entry) for entry in elements]
        mock_list_buckets.side_effect = [
            ProviderResponse(True, 200, {}, mock_result),
            ProviderResponse(True, 200, {}, [])]
        req = swob.Request.blank(
            '/v1/AUTH_migrate-star',
            environ={'__test__.status': '200 OK',
                     '__test__.body': '[]',
                     'swift.trans_id': 'id'})
        status, headers, body_iter = req.call_application(self.app)
        self.assertEqual(self.mock_shunt_swift.mock_calls, [])
        self.assertEqual(
            [mock.call(marker='', limit=10000, prefix='', delimiter=''),
             mock.call(marker=u'unicod\xc3\xa9'.encode('utf-8'), limit=10000,
                       prefix='', delimiter='')],
            mock_list_buckets.mock_calls)
        self.assertEqual(['abc', u'unicod\xc3\xa9'.encode('utf-8')],
                         body_iter.split('\n'))

    @mock.patch('s3_sync.sync_s3.SyncS3.list_buckets')
    def test_list_account_accept_xml(self, mock_list_buckets):
        elements = [{'name': 'abc',
                     'last_modified': 'date',
                     'content_location': 'AWS S3',
                     'count': 0,
                     'bytes': 0},
                    {'name': u'unicod\xc3\xa9',
                     'last_modified': 'date',
                     'content_location': 'AWS S3',
                     'count': 0,
                     'bytes': 0}]
        mock_result = [dict(entry) for entry in elements]
        mock_list_buckets.side_effect = [
            ProviderResponse(True, 200, {}, mock_result),
            ProviderResponse(True, 200, {}, [])]
        req = swob.Request.blank(
            '/v1/AUTH_migrate-star?format=xml',
            environ={'__test__.status': '200 OK',
                     '__test__.body': '[]',
                     'swift.trans_id': 'id'})
        status, headers, body_iter = req.call_application(self.app)
        self.assertEqual(self.mock_shunt_swift.mock_calls, [])
        self.assertEqual(
            [mock.call(marker='', limit=10000, prefix='', delimiter=''),
             mock.call(marker=u'unicod\xc3\xa9'.encode('utf-8'), limit=10000,
                       prefix='', delimiter='')],
            mock_list_buckets.mock_calls)
        self._assert_xml_listing(body_iter, elements,
                                 account='AUTH_migrate-star')

    @mock.patch('s3_sync.sync_s3.SyncS3.list_buckets')
    def test_list_account_splice_delimiter(self, mock_list_buckets):
        mock_result = [{'subdir': u'unicod\xc3\xa9-',
                        'content_location': 'AWS S3'}]
        mock_list_buckets.side_effect = [
            ProviderResponse(True, 200, {}, mock_result),
            ProviderResponse(True, 200, {}, [])]
        req = swob.Request.blank(
            '/v1/AUTH_migrate-star?delimiter=-',
            environ={'__test__.status': '200 OK',
                     '__test__.body': '[{"subdir": "abc-"}, '
                                      '{"subdir": "xyz-"}]',
                     'swift.trans_id': 'id'})
        status, headers, body_iter = req.call_application(self.app)
        self.assertEqual(self.mock_shunt_swift.mock_calls, [])
        self.assertEqual(
            [mock.call(marker='', limit=10000, prefix='', delimiter='-'),
             mock.call(marker=u'unicod\xc3\xa9-'.encode('utf-8'), limit=10000,
                       prefix='', delimiter='-')],
            mock_list_buckets.mock_calls)
        self.assertEqual(['abc-', u'unicod\xc3\xa9-'.encode('utf-8'), 'xyz-'],
                         body_iter.split('\n'))

    def test_shunt_migration_put_object_missing_container(self):
        responses = {'PUT': {
            '/v1/AUTH_migrate/destination/object': [
                {'status': '404 Not Found'},
                {'status': '200 OK'}],
            '/v1/AUTH_migrate/destination': [{'status': '200 OK'}]
        }}

        req = swob.Request.blank(
            '/v1/AUTH_migrate/destination/object',
            method='PUT',
            environ={'swift.trans_id': 'id',
                     '__test__.response_dict': responses})

        status, headers, body_iter = req.call_application(self.app)
        expected_calls = [
            ('PUT', '/v1/AUTH_migrate/destination/object'),
            ('PUT', '/v1/AUTH_migrate/destination'),
            ('PUT', '/v1/AUTH_migrate/destination/object')]
        for index, env in enumerate(self.app.base_app.calls[1:]):
            call_id = (env['REQUEST_METHOD'], env['PATH_INFO'])
            self.assertEqual(expected_calls[index], call_id)

    def test_shunt_migration_put_object(self):
        responses = {'PUT': {
            '/v1/AUTH_migrate/destination/object': {'status': '200 OK'}}}

        req = swob.Request.blank(
            '/v1/AUTH_migrate/destination/object',
            method='PUT',
            environ={'swift.trans_id': 'id',
                     '__test__.response_dict': responses})

        status, headers, body_iter = req.call_application(self.app)
        expected_calls = [('PUT', '/v1/AUTH_migrate/destination/object')]
        for index, env in enumerate(self.app.base_app.calls[1:]):
            call_id = (env['REQUEST_METHOD'], env['PATH_INFO'])
            self.assertEqual(expected_calls[index], call_id)
