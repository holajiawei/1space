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

from contextlib import contextmanager
import json
import mock
import os

from swift.common import swob

from s3_sync.cloud_connector import auth

from .test_cloud_connector_app import TestCloudConnectorBase


class FakeApp(object):
    def __init__(self, status_int=200, body='I am a meat popsicle'):
        self.status_int = status_int
        self.body = body

    def __call__(self, env, start_response):
        return swob.status_map[self.status_int](body=self.body)(
            env, start_response)


def make_req(path='/s3-sync/some/obj', key_id='test:tester', key_val='testing',
             **req_kwargs):
    def _checker(given_key):
        return given_key == key_val

    s3_auth_details = {
        'check_signature': _checker,
        'access_key': key_id,
    }
    return swob.Request.blank(path=path, environ={
        'swift3.auth_details': s3_auth_details,
        'PATH_INFO': '/v1/' + key_id + path,  # swift3 does this,
    }, **req_kwargs)


class TestCloudConnectorAuth(TestCloudConnectorBase):
    def setUp(self):
        super(TestCloudConnectorAuth, self).setUp()

        self.s3_passwd_path = os.path.join(self.tempdir, 's3_passwd.json')
        patcher = mock.patch('s3_sync.cloud_connector.auth.S3_PASSWD_PATH',
                             new=self.s3_passwd_path)
        patcher.start()
        self.addCleanup(patcher.stop)

        self.s3_passwd = [{
            "access_key": "admin:admin",
            "secret_key": "admin",
            "account": "AUTH_admin"
        }, {
            "access_key": "test:tester",
            "secret_key": "testing",
            "account": "AUTH_test"
        }, {
            "access_key": "test2:tester2",
            "secret_key": "testing2",
            "account": "AUTH_test2"
        }, {
            "access_key": u"\u062aacct:\u062auser",
            "secret_key": u"\u062apass",
            "account": u"AUTH_\u062aacct"
        }, {
            "access_key": u"\u062aacct2:\u062auser2",
            "secret_key": u"\u062apass2",
            "account": u"AUTH_\u062aacct2"
        }, {
            "access_key": "nacct:nuser",
            "secret_key": "npass",
            "account": "AUTH_nacct"
        }, {
            "access_key": "nacct2:nuser2",
            "secret_key": "npass2",
            "account": "AUTH_nacct2"
        }, {
            "access_key": "slashacct:slashuser",
            "secret_key": "slashpass",
            "account": "AUTH_slashacct"
        }]

    @contextmanager
    def auth_mw(self, app_kwargs=None, global_conf=None, **kwargs):
        app_kwargs = app_kwargs or {}
        global_conf = global_conf or {}
        app = FakeApp(**app_kwargs)
        factory = auth.filter_factory(global_conf, **kwargs)
        patchers = []
        mocks = {}
        try:
            patcher = mock.patch('s3_sync.cloud_connector.auth.get_logger')
            mocks['get_logger'] = patcher.start()
            patchers.append(patcher)

            patcher = mock.patch(
                's3_sync.cloud_connector.auth.get_env_options')
            mocks['get_env_options'] = patcher.start()
            mocks['get_env_options'].return_value = 'stub_env_options'
            patchers.append(patcher)

            def _config_writer(*args, **kwargs):
                with open(self.s3_passwd_path, 'wb') as fh:
                    json.dump(self.s3_passwd, fh)
                    fh.flush()

            patcher = mock.patch(
                's3_sync.cloud_connector.auth.get_and_write_conf_file_from_s3')
            mocks['get_and_write'] = patcher.start()
            mocks['get_and_write'].side_effect = _config_writer
            patchers.append(patcher)

            mw = factory(app)
            mw._mocks = mocks
            yield mw
        finally:
            for patcher in patchers:
                patcher.stop()

    def test_get_and_write_conf_failure(self):
        with self.auth_mw() as mw:
            for m in mw._mocks.values():
                m.reset_mock()

            mw._mocks['get_and_write'].side_effect = \
                auth.GetAndWriteFileException

            factory = auth.filter_factory({})
            with self.assertRaises(SystemExit):
                factory(FakeApp())

            self.assertEqual([
                mock.call({}, log_route='cloud_connect_auth'),
                mock.call().fatal('%s; no S3 API requests will work without '
                                  'the passwd DB.', ''),
            ], mw._mocks['get_logger'].mock_calls)

    def test_passwd_read_error(self):
        with self.auth_mw() as mw:
            for m in mw._mocks.values():
                m.reset_mock()

            def _config_writer(*args, **kwargs):
                with open(self.s3_passwd_path, 'wb') as fh:
                    fh.write("I ain't JSON, bro!")
                    fh.flush()

            mw._mocks['get_and_write'].side_effect = _config_writer

            factory = auth.filter_factory({})
            with self.assertRaises(SystemExit):
                factory(FakeApp())

            self.assertEqual([
                mock.call({}, log_route='cloud_connect_auth'),
                mock.call().fatal(
                    "Couldn't read s3 passwd path %r: %s; exiting" % (
                        self.s3_passwd_path,
                        'No JSON object could be decoded')),
            ], mw._mocks['get_logger'].mock_calls)

    def test_basic_success_with_defaults(self):
        with self.auth_mw() as mw:
            req = make_req()
            resp = req.get_response(mw)

            self.assertEqual(200, resp.status_int)
            self.assertEqual('I am a meat popsicle', resp.body)
            self.assertEqual([
                mock.call({}, log_route='cloud_connect_auth'),
                mock.call().debug('key id %r authorized for acct %r: %r',
                                  'test:tester', 'AUTH_test',
                                  '/v1/AUTH_test/s3-sync/some/obj'),
            ], mw._mocks['get_logger'].mock_calls)
            self.assertEqual(mw._mocks['get_logger'].return_value,
                             mw.logger)
            self.assertEqual([
                mock.call('etc/swift-s3-sync/s3-passwd.json',
                          self.s3_passwd_path, 'stub_env_options',
                          user='swift'),
            ], mw._mocks['get_and_write'].mock_calls)

    def test_basic_success_with_unicodes(self):
        user_acct = u'\u062aacct:\u062auser'.encode('utf-8')
        acct = u"AUTH_\u062aacct".encode('utf8')
        with self.auth_mw() as mw:
            req = make_req(key_id=user_acct,
                           key_val=u"\u062apass".encode('utf-8'))
            resp = req.get_response(mw)

            self.assertEqual(200, resp.status_int)
            self.assertEqual('I am a meat popsicle', resp.body)
            self.assertEqual([
                mock.call({}, log_route='cloud_connect_auth'),
                mock.call().debug('key id %r authorized for acct %r: %r',
                                  user_acct, acct, '/v1/%s/s3-sync/some/obj' %
                                  acct),
            ], mw._mocks['get_logger'].mock_calls)
            self.assertEqual(mw._mocks['get_logger'].return_value,
                             mw.logger)
            self.assertEqual([
                mock.call('etc/swift-s3-sync/s3-passwd.json',
                          self.s3_passwd_path, 'stub_env_options',
                          user='swift'),
            ], mw._mocks['get_and_write'].mock_calls)

    def test_basic_success_with_non_defaults(self):
        with self.auth_mw(global_conf={'user': 'nobody'},
                          s3_passwd_json='/tmp/foo.bar') as mw:
            req = make_req()
            resp = req.get_response(mw)

            self.assertEqual(200, resp.status_int)
            self.assertEqual('I am a meat popsicle', resp.body)
            self.assertEqual([
                mock.call({'user': 'nobody',
                           's3_passwd_json': '/tmp/foo.bar'},
                          log_route='cloud_connect_auth'),
                mock.call().debug('key id %r authorized for acct %r: %r',
                                  'test:tester', 'AUTH_test',
                                  '/v1/AUTH_test/s3-sync/some/obj'),
            ], mw._mocks['get_logger'].mock_calls)
            self.assertEqual(mw._mocks['get_logger'].return_value,
                             mw.logger)
            self.assertEqual([
                mock.call('tmp/foo.bar', self.s3_passwd_path,
                          'stub_env_options', user='nobody'),
            ], mw._mocks['get_and_write'].mock_calls)

    def test_no_swift3_auth_details(self):
        with self.auth_mw() as mw:
            req = make_req()
            del req.environ['swift3.auth_details']
            resp = req.get_response(mw)

            self.assertEqual(400, resp.status_int)
            self.assertEqual('Only S3 API requests are supported '
                             'at this time.', resp.body)

    def test_no_check_signature_in_swift3_auth_details(self):
        with self.auth_mw() as mw:
            req = make_req()
            del req.environ['swift3.auth_details']['check_signature']

            with self.assertRaises(SystemExit):
                req.get_response(mw)

            self.assertEqual([
                mock.call({}, log_route='cloud_connect_auth'),
                mock.call().fatal("Swift3 did not provide a check_signature "
                                  "function"),
            ], mw._mocks['get_logger'].mock_calls)

    def test_rando_key_id(self):
        with self.auth_mw() as mw:
            req = make_req(key_id='aljfalkdjflakfjl')
            resp = req.get_response(mw)

            self.assertEqual(401, resp.status_int)
            self.assertIn('Unauthorized', resp.body)
            self.assertEqual('Cloud-connector realm="unknown"',
                             resp.headers['Www-Authenticate'])

    def test_bad_key_value(self):
        with self.auth_mw() as mw:
            req = make_req(key_val='aljfalkdjflakfjl')
            resp = req.get_response(mw)

            self.assertEqual(401, resp.status_int)
            self.assertIn('Unauthorized', resp.body)
            self.assertEqual('Cloud-connector realm="unknown"',
                             resp.headers['Www-Authenticate'])
