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
import mock
import os
import pwd
import shutil
import sys
import tempfile
import unittest
import urllib

from swift.common import swob, utils as swift_utils, wsgi

from s3_sync.cloud_connector import app


class TestCloudConnectorBase(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        self.cloud_connector_conf_path = os.path.join(
            self.tempdir, 'test.conf')
        patcher = mock.patch.object(app, 'CLOUD_CONNECTOR_CONF_PATH',
                                    self.cloud_connector_conf_path)
        patcher.start()
        self.addCleanup(patcher.stop)

        self.cloud_connector_sync_conf_path = os.path.join(
            self.tempdir, 'sync.json')
        patcher = mock.patch.object(app, 'CLOUD_CONNECTOR_SYNC_CONF_PATH',
                                    self.cloud_connector_sync_conf_path)
        patcher.start()
        self.addCleanup(patcher.stop)

        # In the dev container, these tests run as `root` but no sense in
        # really making that a requirement to get line & branch coverage.
        patcher = mock.patch.object(app, 'ROOT_UID', os.geteuid())
        patcher.start()
        self.addCleanup(patcher.stop)

        self.sync_conf = {
            'containers': [
                {
                    'account': u'AUTH_\u062aa',
                    'container': u'sw\u00e9ft',
                    'propagate_delete': False,
                    'protocol': 'swift',
                    'aws_bucket': u'dest-\u062acontainer',
                    'aws_identity': u'\u062auser',
                    'aws_secret': u'\u062akey',
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
                    'account': u'AUTH_b\u062a',
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
                }],
        }

    def tearDown(self):
        shutil.rmtree(self.tempdir, ignore_errors=True)


class TestCloudConnectorApp(TestCloudConnectorBase):
    def setUp(self):
        super(TestCloudConnectorApp, self).setUp()

        self.swift_baseurl = 'http://1.2.3.4:5678'

        # Get ourselves an Application instance to play with
        patcher = mock.patch('s3_sync.cloud_connector.app.get_env_options')
        patcher.start()
        self.addCleanup(patcher.stop)

        patcher = mock.patch(
            's3_sync.cloud_connector.app.get_and_write_conf_file_from_s3')
        mock_get_and_write = patcher.start()
        self.addCleanup(patcher.stop)

        def _config_writer(*args, **kwargs):
            with open(self.cloud_connector_sync_conf_path, 'wb') as fh:
                json.dump(self.sync_conf, fh)
                fh.flush()

        mock_get_and_write.side_effect = _config_writer

        conf = {'swift_baseurl': self.swift_baseurl}
        self.app = app.CloudConnectorApplication(conf, logger=mock.MagicMock())

    def controller_for(self, account, container, obj, verb):
        req = swob.Request.blank('http://a.b.c:123/v1/%s/%s/%s' % (
            urllib.quote(account.encode('utf8')),
            urllib.quote(container.encode('utf8')),
            urllib.quote(obj.encode('utf8'))),
            environ={'REQUEST_METHOD': verb})
        klass, kwargs = self.app.get_controller(req)
        return klass(self.app, **kwargs), req

    def test_controller_init(self):
        controller, _ = self.controller_for(u'AUTH_b\u062a', 'jojo', 'oo',
                                            'GET')

        self.assertEqual('v1', controller.version)
        self.assertEqual(u'AUTH_b\u062a'.encode('utf8'),
                         controller.account_name)
        self.assertEqual('jojo', controller.container_name)
        self.assertEqual('oo', controller.object_name)
        exp_profile = self.sync_conf['containers'][2].copy()
        exp_profile['container'] = 'jojo'
        self.assertEqual(exp_profile, controller.sync_profile)
        provider = controller.provider
        self.assertTrue(provider._per_account)

    def test_not_implemented_yet(self):
        for verb in ('GET', 'HEAD', 'PUT', 'POST', 'OPTIONS', 'DELETE'):
            controller, req = self.controller_for(u'AUTH_b\u062a', 'jojo',
                                                  'oo', verb)
            fn = getattr(controller, verb)
            got = fn(req)
            self.assertEqual('501 Not Implemented', got.status)

    def test_get_controller_no_acct_actions_allowed(self):
        for verb in ('GET', 'HEAD', 'PUT', 'POST', 'OPTIONS', 'DELETE'):
            req = swob.Request.blank('http://a.b.c:123/v1/AUTH_jojo',
                                     environ={'REQUEST_METHOD': verb})
            with self.assertRaises(swob.HTTPException) as cm:
                self.app.get_controller(req)
            self.assertEqual('403 Forbidden', cm.exception.status)
            self.assertEqual('Account operations are not supported.',
                             cm.exception.body)

    def test_get_controller_no_non_get_container_actions_allowed(self):
        for verb in ('HEAD', 'PUT', 'POST', 'OPTIONS', 'DELETE'):
            req = swob.Request.blank('http://a.b.c:123/v1/AUTH_jojo/c',
                                     environ={'REQUEST_METHOD': verb})
            with self.assertRaises(swob.HTTPException) as cm:
                self.app.get_controller(req)
            self.assertEqual('403 Forbidden', cm.exception.status)
            self.assertEqual('The only supported container operation is '
                             'GET (listing).', cm.exception.body)

    def test_get_controller_no_sync_profile_match(self):
        req = swob.Request.blank('http://a.b.c:123/v1/AUTH_jojo/c',
                                 environ={'REQUEST_METHOD': 'GET'})
        with self.assertRaises(swob.HTTPException) as cm:
            self.app.get_controller(req)
        self.assertEqual('403 Forbidden', cm.exception.status)
        self.assertEqual('No matching sync profile.', cm.exception.body)

        for verb in ('HEAD', 'PUT', 'POST', 'OPTIONS', 'DELETE'):
            req = swob.Request.blank('http://a.b.c:123/v1/AUTH_jojo/c/o',
                                     environ={'REQUEST_METHOD': verb})
            with self.assertRaises(swob.HTTPException) as cm:
                self.app.get_controller(req)
            self.assertEqual('403 Forbidden', cm.exception.status)
            self.assertEqual('No matching sync profile.', cm.exception.body)

    def test_get_controller_ok(self):
        for verb in ('HEAD', 'PUT', 'POST', 'OPTIONS', 'DELETE'):
            for a, c, profile in [(u'AUTH_\u062aa', u'sw\u00e9ft',
                                   self.sync_conf['containers'][0]),
                                  ('AUTH_a', 's3',
                                   self.sync_conf['containers'][1]),
                                  (u'AUTH_b\u062a', 'crazy1',
                                   self.sync_conf['containers'][2]),
                                  (u'AUTH_b\u062a', u'cr\u062azy2',
                                   self.sync_conf['containers'][2])]:
                req = swob.Request.blank('http://a.b.c:123/v1/%s/%s/o' % (
                    urllib.quote(a.encode('utf8')),
                    urllib.quote(c.encode('utf8'))),
                    environ={'REQUEST_METHOD': verb})
                klass, kwargs = self.app.get_controller(req)
                self.assertEqual(app.CloudConnectorController, klass)
                self.assertEqual({
                    'sync_profile': profile,
                    'version': 'v1',
                    'account_name': a.encode('utf8'),
                    'container_name': c.encode('utf8'),
                    'object_name': 'o',
                }, kwargs)


class TestCloudConnectorAppConstruction(TestCloudConnectorBase):
    @mock.patch('s3_sync.cloud_connector.app.get_env_options')
    @mock.patch('s3_sync.cloud_connector.app.get_and_write_conf_file_from_s3')
    def test_app_init_json_load_error(self, mock_get_and_write,
                                      mock_get_env_options):
        conf = {'swift_baseurl': 'abbc'}

        # No file written to read
        self.assertRaises(SystemExit, app.CloudConnectorApplication,
                          conf, logger='a')

        # Bad JSON in file
        def _config_writer(*args, **kwargs):
            with open(self.cloud_connector_sync_conf_path, 'wb') as fh:
                fh.write("I ain't valid JSON!")
                fh.flush()

        mock_get_and_write.side_effect = _config_writer

        self.assertRaises(SystemExit, app.CloudConnectorApplication,
                          conf, logger='a')

    @mock.patch('s3_sync.cloud_connector.app.utils.get_logger')
    @mock.patch('s3_sync.cloud_connector.app.get_env_options')
    @mock.patch('s3_sync.cloud_connector.app.get_and_write_conf_file_from_s3')
    def test_app_init_defaults(self, mock_get_and_write, mock_get_env_options,
                               mock_get_logger):
        def _config_writer(*args, **kwargs):
            with open(self.cloud_connector_sync_conf_path, 'wb') as fh:
                json.dump(self.sync_conf, fh)
                fh.flush()

        mock_get_and_write.side_effect = _config_writer
        conf = {'swift_baseurl': 'abbc'}
        mock_get_logger.return_value = 'stub_logger'
        mock_get_env_options.return_value = 'stub_env_opts'

        app_instance = app.CloudConnectorApplication(conf)

        self.assertEqual('stub_logger', app_instance.logger)
        self.assertEqual([mock.call(conf, log_route='cloud-connector')],
                         mock_get_logger.mock_calls)
        self.assertEqual([], app_instance.deny_host_headers)
        self.assertEqual([mock.call('etc/swift-s3-sync/sync.json',
                                    self.cloud_connector_sync_conf_path,
                                    'stub_env_opts', user='swift')],
                         mock_get_and_write.mock_calls)
        self.assertEqual({
            (u'AUTH_\u062aa'.encode('utf8'), u'sw\u00e9ft'.encode('utf8')):
            self.sync_conf['containers'][0],
            ('AUTH_a', 's3'): self.sync_conf['containers'][1],
            (u'AUTH_b\u062a'.encode('utf8'), '/*'):
            self.sync_conf['containers'][2],
            ('AUTH_tee', 'tee'): self.sync_conf['containers'][3],
        }, app_instance.sync_profiles)
        self.assertEqual('abbc', app_instance.swift_baseurl)

    @mock.patch('s3_sync.cloud_connector.app.get_env_options')
    @mock.patch('s3_sync.cloud_connector.app.get_and_write_conf_file_from_s3')
    def test_app_init_non_defaults(self, mock_get_and_write,
                                   mock_get_env_options):
        def _config_writer(*args, **kwargs):
            with open(self.cloud_connector_sync_conf_path, 'wb') as fh:
                json.dump(self.sync_conf, fh)
                fh.flush()

        mock_get_and_write.side_effect = _config_writer
        conf = {'swift_baseurl': 'abbc', 'deny_host_headers': ' a , c,d ',
                'user': 'stubb_user', 'conf_file': 'stub_conf_file_path'}
        mock_get_env_options.return_value = 'stub_env_opts'

        app_instance = app.CloudConnectorApplication(
            conf, logger='another_stub_logger')

        self.assertEqual('another_stub_logger', app_instance.logger)
        self.assertEqual(['a', 'c', 'd'], app_instance.deny_host_headers)
        self.assertEqual([mock.call('stub_conf_file_path',
                                    self.cloud_connector_sync_conf_path,
                                    'stub_env_opts', user='stubb_user')],
                         mock_get_and_write.mock_calls)
        self.assertEqual({
            (u'AUTH_\u062aa'.encode('utf8'), u'sw\u00e9ft'.encode('utf8')):
            self.sync_conf['containers'][0],
            ('AUTH_a', 's3'): self.sync_conf['containers'][1],
            (u'AUTH_b\u062a'.encode('utf8'), '/*'):
            self.sync_conf['containers'][2],
            ('AUTH_tee', 'tee'): self.sync_conf['containers'][3],
        }, app_instance.sync_profiles)
        self.assertEqual('abbc', app_instance.swift_baseurl)

    @mock.patch('s3_sync.cloud_connector.util.pwd.getpwnam')
    @mock.patch('s3_sync.cloud_connector.app.os.chown')
    @mock.patch('s3_sync.cloud_connector.app.CloudConnectorApplication')
    def test_app_factory_when_root_default_user(self, mock_app_klass,
                                                mock_chown, mock_getpwnam):
        global_conf = {'g1': 'g1v', 'a_key': 'a_g_val'}
        local_conf = {'l1': 'l1v', 'a_key': 'a_l_val'}

        mock_app_klass.return_value = 'stub_app_instance'

        # Chown user should default to "swift"
        user_ent = pwd.struct_passwd(
            ('swift', 'pass', 42, 43, 'gecos', 'dir', 'shell'))

        def _getpwnam(name):
            self.assertEqual(name, 'swift')
            return user_ent

        mock_getpwnam.side_effect = _getpwnam

        got_app = app.app_factory(global_conf, **local_conf)

        self.assertEqual('stub_app_instance', got_app)
        self.assertEqual([mock.call({'g1': 'g1v', 'a_key': 'a_l_val',
                                     'l1': 'l1v'})],
                         mock_app_klass.mock_calls)
        self.assertEqual([mock.call(self.cloud_connector_conf_path,
                                    user_ent.pw_uid, user_ent.pw_gid)],
                         mock_chown.mock_calls)

    @mock.patch('s3_sync.cloud_connector.app.os.chown')
    @mock.patch('s3_sync.cloud_connector.app.CloudConnectorApplication')
    def test_app_factory_when_root_specified_user(self, mock_app_klass,
                                                  mock_chown):
        global_conf = {'g1': 'g1v', 'a_key': 'a_g_val'}
        local_conf = {'l1': 'l1v', 'a_key': 'a_l_val',
                      'user': 'daemon'}

        mock_app_klass.return_value = 'stub_app_instance'

        user_ent = pwd.getpwnam('daemon')

        got_app = app.app_factory(global_conf, **local_conf)

        self.assertEqual('stub_app_instance', got_app)
        self.assertEqual([mock.call({'g1': 'g1v', 'a_key': 'a_l_val',
                                     'l1': 'l1v', 'user': 'daemon'})],
                         mock_app_klass.mock_calls)
        self.assertEqual([mock.call(self.cloud_connector_conf_path,
                                    user_ent.pw_uid, user_ent.pw_gid)],
                         mock_chown.mock_calls)

    @mock.patch('s3_sync.cloud_connector.app.os.chown')
    @mock.patch('s3_sync.cloud_connector.app.CloudConnectorApplication')
    def test_app_factory_when_NOT_root(self, mock_app_klass, mock_chown):
        global_conf = {'g1': 'g1v', 'a_key': 'a_g_val'}
        local_conf = {'l1': 'l1v', 'a_key': 'a_l_val'}

        mock_app_klass.return_value = 'stub_app_instance'

        nonroot_uid = os.geteuid() + 1
        with mock.patch(
                's3_sync.cloud_connector.app.os.geteuid') as mock_geteuid:
            mock_geteuid.return_value = nonroot_uid
            got_app = app.app_factory(global_conf, **local_conf)

        self.assertEqual('stub_app_instance', got_app)
        self.assertEqual([mock.call({'g1': 'g1v', 'a_key': 'a_l_val',
                                     'l1': 'l1v'})],
                         mock_app_klass.mock_calls)
        self.assertEqual([], mock_chown.mock_calls)
        self.assertEqual([mock.call()], mock_geteuid.mock_calls)

    @mock.patch('s3_sync.cloud_connector.app.get_env_options')
    @mock.patch('s3_sync.cloud_connector.app.get_and_write_conf_file_from_s3')
    @mock.patch('swift.common.utils.parse_options')
    @mock.patch('swift.common.wsgi.run_wsgi')
    def test_main(self, mock_run_wsgi, mock_parse_options, mock_get_and_write,
                  mock_get_env_options):
        orig_utils_validate = swift_utils.validate_configuration
        orig_wsgi_validate = wsgi.validate_configuration
        self.assertEqual(orig_utils_validate, orig_wsgi_validate)

        # The real validate_configuration chokes on InvalidHashPathConfigError
        # Later, we'll make sure the monkey-patched ones don't
        with mock.patch('swift.common.utils.validate_hash_conf',
                        side_effect=swift_utils.InvalidHashPathConfigError):
            self.assertRaises(SystemExit, orig_utils_validate)

        mock_get_env_options.return_value = {'CONF_NAME': 'shimmy/jam.conf'}
        mock_parse_options.return_value = ('stub_conf_file', {'k': 'v'})
        mock_run_wsgi.return_value = 42

        orig_sys_argv = list(sys.argv)
        with self.assertRaises(SystemExit) as cm:
            app.main()

        self.assertEqual(42, cm.exception.message)
        self.assertEqual([mock.call('shimmy/jam.conf',
                          self.cloud_connector_conf_path,
                          mock_get_env_options.return_value)],
                         mock_get_and_write.mock_calls)
        self.assertEqual([orig_sys_argv[0]] +
                         [self.cloud_connector_conf_path] + orig_sys_argv[1:],
                         sys.argv)
        self.assertEqual([mock.call('stub_conf_file', 'proxy-server', k='v')],
                         mock_run_wsgi.mock_calls)

        # Now, after having been monkeypatched, validate_configuration won't
        # try to exit after InvalidHashPathConfigError is raised.
        swift_utils.validate_configuration()
        wsgi.validate_configuration()
        with mock.patch('swift.common.utils.validate_hash_conf',
                        side_effect=swift_utils.InvalidHashPathConfigError):
            swift_utils.validate_configuration()
            wsgi.validate_configuration()

        # Restore monkeypatched functions
        swift_utils.validate_configuration = orig_utils_validate
        wsgi.validate_configuration = orig_wsgi_validate
