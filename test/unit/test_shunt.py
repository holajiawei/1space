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

import json
import lxml
import mock
import StringIO
import tempfile
import unittest

from swift.common import swob

from s3_sync import shunt
from s3_sync import sync_s3
from s3_sync import sync_swift
from s3_sync import utils


class FakeSwift(object):
    def __init__(self):
        self.calls = []

    def __call__(self, env, start_response):
        self.calls.append(env)
        # Let the tests set up the responses they want
        if env.get('__test__.response_dict'):
            resp_dict = env['__test__.response_dict']
            method = env['REQUEST_METHOD']
            if method in resp_dict:
                status = resp_dict[method].get('status', '200 OK')
                headers = resp_dict[method].get('headers', [])
                start_response(status, headers)
                return resp_dict[method].get('body', '')

        status = env.get('__test__.status', '200 OK')
        headers = env.get('__test__.headers', [])
        body = env.get('__test__.body', ['pass'])

        start_response(status, headers)
        return body


class TestShunt(unittest.TestCase):
    def setUp(self):
        self.patchers = [mock.patch(name) for name in (
            's3_sync.sync_swift.SyncSwift.shunt_object',
            's3_sync.sync_s3.SyncS3.shunt_object',
            's3_sync.sync_swift.SyncSwift.list_objects',
            's3_sync.sync_s3.SyncS3.list_objects')]
        self.mock_shunt_swift = self.patchers[0].__enter__()
        self.mock_shunt_swift.return_value = (
            200, [
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
                ('Content-Length', len('remote swift'))
            ], StringIO.StringIO('remote swift'))
        self.mock_shunt_s3 = self.patchers[1].__enter__()
        self.mock_shunt_s3.return_value = (
            200, [
                ('Remote-x-amz-id-2', 'also some trans id'),
                ('Remote-x-amz-request-id', 'some trans id'),
                ('CONNECTION', 'bad'),
                ('keep-alive', 'bad'),
                ('proxy-authenticate', 'bad'),
                ('proxy-authorization', 'bad'),
                ('te', 'bad'),
                ('trailer', 'bad'),
                ('Transfer-Encoding', 'bad'),
                ('Upgrade', 'bad')], ['remote s3'])

        self.mock_list_swift = self.patchers[2].__enter__()
        self.mock_list_s3 = self.patchers[3].__enter__()

        self.conf = {
            'containers': [
                {
                    'account': 'AUTH_a',
                    'container': u'sw\u00e9ft',
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
                    'propagate_delete': False,
                    'aws_bucket': 'dest-bucket',
                    'aws_identity': 'user',
                    'aws_secret': 'key',
                },
                {
                    'account': 'AUTH_b',
                    'container': '/*',
                    'propagate_delete': False,
                    'aws_bucket': 'dest-bucket',
                    'aws_identity': 'user',
                    'aws_secret': 'key',
                },
                {
                    'account': 'AUTH_tee',
                    'container': 'tee',
                    'propagate_delete': False,
                    'restore_object': True,
                    'aws_bucket': 'dest-bucket',
                    'aws_identity': 'user',
                    'aws_secret': 'key',
                }]
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
                'propagate_delete': False,
                'aws_bucket': 'dest-bucket',
                'aws_identity': 'user',
                'aws_secret': 'key',
            },
            ('AUTH_b', '/*'): {
                'account': 'AUTH_b',
                'container': '/*',
                'propagate_delete': False,
                'aws_bucket': 'dest-bucket',
                'aws_identity': 'user',
                'aws_secret': 'key',
            },
            ('AUTH_tee', 'tee'): {
                'account': 'AUTH_tee',
                'container': 'tee',
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
                'native': True,
                # Need a container key, or the provider balks. Migrations move
                # data from one name to the same name, so crib from aws_bucket
                'container': 'some-bucket',
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
            },
        })

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
        def _test_no_shunt(path, status):
            req = swob.Request.blank(path, environ={
                '__test__.status': status,
                'swift.trans_id': 'local trans id'})
            req.call_application(self.app)
            self.assertEqual(self.mock_shunt_swift.mock_calls, [])
            self.assertEqual(self.mock_shunt_s3.mock_calls, [])
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
                    ('Content-Length', 12)
                ])
                self.assertEqual(b''.join(body_iter), b'remote swift')
                self.mock_shunt_swift.reset_mock()
        _test_shunted(u'/v1/AUTH_a/sw\u00e9ft/o', False)
        _test_shunted('/v1/AUTH_a/s3/o', True)
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
             (200,
              [('Content-Length', len(payload)),
               (utils.SLO_HEADER, 'True'),
               ('etag', 'deadbeef-2')],
              StringIO.StringIO(payload)),
             mock_s3_shunt, True),
            (u'AUTH_a/sw\u00e9ft',
             (200,
              [('Content-Length', len(payload)),
               (utils.SLO_HEADER, 'True')],
              StringIO.StringIO(payload)),
             mock_swift_shunt, True),
            ('AUTH_tee/tee',
             (200, [('Content-Length', len(payload))],
              StringIO.StringIO(payload)),
             mock_s3_shunt, True),
            (u'AUTH_a/sw\u00e9ft',
             (200, [('Content-Length', len(payload))],
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
            if dict(resp[1]).get('etag', '').endswith('-2') or\
                    utils.SLO_HEADER in dict(resp[1]):
                is_slo = True
            else:
                is_slo = False
            if is_slo:
                manifest = [{
                    'bytes': len(payload),
                    'name': '/segments/part1'}]
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
            (200, [{'name': 'abc',
                    'hash': 'ffff',
                    'bytes': 42,
                    'last_modified': 'date',
                    'content_type': 'type'},
                   {'name': u'unicod\xe9',
                    'hash': 'ffff',
                    'bytes': 1000,
                    'last_modified': 'date',
                    'content_type': 'type'}]),
            (200, [])]
        req = swob.Request.blank(
            '/v1/AUTH_a/s3',
            environ={'__test__.status': '200 OK',
                     '__test__.body': '[]',
                     'swift.trans_id': 'id'})
        status, headers, body_iter = req.call_application(self.app)
        self.assertEqual(self.mock_shunt_swift.mock_calls, [])
        self.mock_list_s3.assert_has_calls([
            mock.call('', 10000, '', ''),
            mock.call('unicod\xc3\xa9', 10000, '', '')])
        names = body_iter.split('\n')
        self.assertEqual(['abc', u'unicod\xe9'], names)

    def test_list_container_shunt_s3_xml(self):
        elements = [{'name': 'abc',
                     'hash': 'ffff',
                     'bytes': 42,
                     'last_modified': 'date',
                     'content_type': 'type'},
                    {'name': u'unicod\xc3\xa9',
                     'hash': 'ffff',
                     'bytes': 1000,
                     'last_modified': 'date',
                     'content_type': 'type'}]
        self.mock_list_s3.side_effect = [
            (200, elements), (200, [])]
        req = swob.Request.blank(
            '/v1/AUTH_a/s3?format=xml',
            environ={'__test__.status': '200 OK',
                     '__test__.body': '[]',
                     'swift.trans_id': 'id'})
        status, headers, body_iter = req.call_application(self.app)
        self.assertEqual(self.mock_shunt_swift.mock_calls, [])
        self.mock_list_s3.assert_has_calls([
            mock.call('', 10000, '', ''),
            mock.call(u'unicod\xc3\xa9'.encode('utf-8'), 10000, '', '')])
        root = lxml.etree.fromstring(body_iter)
        context = lxml.etree.iterwalk(root, events=("start", "end"))
        element_index = 0
        cur_elem_properties = {}
        for action, elem in context:
            if action == 'end':
                if elem.tag == 'container':
                    self.assertEqual('s3', elem.get('name'))
                elif elem.tag == 'object':
                    self.assertEqual(elements[element_index],
                                     cur_elem_properties)
                    element_index += 1
                else:
                    try:
                        int_value = int(elem.text)
                        cur_elem_properties[elem.tag] = int_value
                    except ValueError:
                        cur_elem_properties[elem.tag] = elem.text

    def test_list_container_accept_xml(self):
        elements = [{'name': 'abc',
                     'hash': 'ffff',
                     'bytes': 42,
                     'last_modified': 'date',
                     'content_type': 'type'},
                    {'name': u'unicod\xc3\xa9',
                     'hash': 'ffff',
                     'bytes': 1000,
                     'last_modified': 'date',
                     'content_type': 'type'}]
        self.mock_list_s3.side_effect = [
            (200, elements), (200, [])]
        req = swob.Request.blank(
            '/v1/AUTH_a/s3',
            environ={'__test__.status': '200 OK',
                     '__test__.body': '[]',
                     'swift.trans_id': 'id'},
            headers={'Accept': 'application/xml'})
        status, headers, body_iter = req.call_application(self.app)
        self.assertEqual(self.mock_shunt_swift.mock_calls, [])
        self.mock_list_s3.assert_has_calls([
            mock.call('', 10000, '', ''),
            mock.call(u'unicod\xc3\xa9'.encode('utf-8'), 10000, '', '')])
        root = lxml.etree.fromstring(body_iter)
        context = lxml.etree.iterwalk(root, events=("start", "end"))
        element_index = 0
        cur_elem_properties = {}
        for action, elem in context:
            if action == 'end':
                if elem.tag == 'container':
                    self.assertEqual('s3', elem.get('name'))
                elif elem.tag == 'object':
                    self.assertEqual(elements[element_index],
                                     cur_elem_properties)
                    element_index += 1
                else:
                    try:
                        int_value = int(elem.text)
                        cur_elem_properties[elem.tag] = int_value
                    except ValueError:
                        cur_elem_properties[elem.tag] = elem.text

    def test_list_container_shunt_s3_json(self):
        elements = [{'name': 'abc',
                     'hash': 'ffff',
                     'bytes': 42,
                     'last_modified': 'date',
                     'content_type': 'type'},
                    {'name': u'unicod\xc3\xa9',
                     'hash': 'ffff',
                     'bytes': 1000,
                     'last_modified': 'date',
                     'content_type': 'type'}]
        self.mock_list_s3.side_effect = [
            (200, elements), (200, [])]
        req = swob.Request.blank(
            '/v1/AUTH_a/s3?format=json',
            environ={'__test__.status': '200 OK',
                     '__test__.body': '[]',
                     'swift.trans_id': 'id'})
        status, headers, body_iter = req.call_application(self.app)
        self.assertEqual(self.mock_shunt_swift.mock_calls, [])
        self.mock_list_s3.assert_has_calls([
            mock.call('', 10000, '', ''),
            mock.call(u'unicod\xc3\xa9'.encode('utf-8'), 10000, '', '')])
        results = json.loads(body_iter)
        for i, entry in enumerate(results):
            self.assertEqual(elements[i], entry)

    def test_list_container_accept_json(self):
        elements = [{'name': 'abc',
                     'hash': 'ffff',
                     'bytes': 42,
                     'last_modified': 'date',
                     'content_type': 'type'},
                    {'name': u'unicod\xc3\xa9',
                     'hash': 'ffff',
                     'bytes': 1000,
                     'last_modified': 'date',
                     'content_type': 'type'}]
        self.mock_list_s3.side_effect = [
            (200, elements), (200, [])]
        req = swob.Request.blank(
            '/v1/AUTH_a/s3',
            environ={'__test__.status': '200 OK',
                     '__test__.body': '[]',
                     'swift.trans_id': 'id'},
            headers={'Accept': 'application/json'})
        status, headers, body_iter = req.call_application(self.app)
        self.assertEqual(self.mock_shunt_swift.mock_calls, [])
        self.mock_list_s3.assert_has_calls([
            mock.call('', 10000, '', ''),
            mock.call(u'unicod\xc3\xa9'.encode('utf-8'), 10000, '', '')])
        results = json.loads(body_iter)
        for i, entry in enumerate(results):
            self.assertEqual(elements[i], entry)

    @mock.patch('s3_sync.shunt.create_provider')
    def test_list_container_shunt_all_containers(self, create_mock):
        create_mock.return_value = mock.Mock()
        create_mock.return_value.list_objects.return_value = (200, [])
        req = swob.Request.blank(
            '/v1/AUTH_b/s3',
            environ={'__test__.status': '200 OK',
                     '__test__.body': '[]',
                     'swift.trans_id': 'id'})
        status, headers, body_iter = req.call_application(self.app)
        create_mock.assert_called_once_with({
            'account': 'AUTH_b',
            'container': 's3',
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
            'propagate_delete': False,
            'aws_bucket': 'dest-bucket',
            'aws_identity': 'user',
            'aws_secret': 'key'}, max_conns=1, per_account=True)

    def test_list_container_shunt_swift(self):
        self.mock_list_swift.side_effect = [
            (200, [{'name': 'abc',
                    'hash': 'ffff',
                    'bytes': 42,
                    'last_modified': 'date',
                    'content_type': 'type'},
                   {'name': u'unicod\xe9',
                    'hash': 'ffff',
                    'bytes': 1000,
                    'last_modified': 'date',
                    'content_type': 'type'}]),
            (200, [])]
        req = swob.Request.blank(
            '/v1/AUTH_a/sw\xc3\xa9ft',
            environ={'__test__.status': '200 OK',
                     '__test__.body': '[]',
                     'swift.trans_id': 'id'})
        status, headers, body_iter = req.call_application(self.app)
        self.assertEqual(self.mock_shunt_swift.mock_calls, [])
        self.mock_list_swift.assert_has_calls([
            mock.call('', 10000, '', ''),
            mock.call(u'unicod\xe9'.encode('utf-8'), 10000, '', '')])
        names = body_iter.split('\n')
        self.assertEqual(['abc', u'unicod\xe9'], names)
