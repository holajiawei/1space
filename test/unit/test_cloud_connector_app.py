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

from itertools import cycle
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

from s3_sync.base_sync import ProviderResponse
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
    maxDiff = None

    def setUp(self):
        super(TestCloudConnectorApp, self).setUp()

        self.swift_baseurl = 'http://1.2.3.4:5678'
        self.s3_identity = {
            'access_key': u'\u062akey id',
            'secret_key': u'\u062akey val',
        }

        patcher = mock.patch('s3_sync.cloud_connector.app.create_provider')
        self.mock_create_provider = patcher.start()
        self.addCleanup(patcher.stop)

        self.mock_ltm_provider = mock.Mock()
        self.mock_rtm_provider = mock.Mock()
        # We happen to know the local-to-me provider is created first.
        self.mock_create_provider.side_effect = cycle([
            self.mock_ltm_provider, self.mock_rtm_provider])

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
        self.mock_logger = mock.MagicMock()
        self.app = app.CloudConnectorApplication(conf, logger=self.mock_logger)

    def controller_for(self, account, container, obj=None, verb='GET',
                       query_string=None, req_kwargs=None):
        query_string = '?' + query_string if query_string else ''
        uri = 'http://a.b.c:123/v1/%s/%s%s' % (
            urllib.quote(account.encode('utf8')),
            urllib.quote(container.encode('utf8')),
            query_string)
        if obj is not None:
            uri += '/' + urllib.quote(obj.encode('utf8'))
        req_kwargs = req_kwargs or {}
        req = swob.Request.blank(uri, environ={
            'REQUEST_METHOD': verb,
            app.S3_IDENTITY_ENV_KEY: self.s3_identity,
        }, **req_kwargs)
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
        self.assertEqual(exp_profile, controller.local_to_me_profile)
        self.assertEqual([
            mock.call(controller.local_to_me_profile, max_conns=1,
                      per_account=True, logger=self.app.logger),
            mock.call(controller.remote_to_me_profile, max_conns=1,
                      per_account=False, logger=self.app.logger,
                      extra_headers={'x-cloud-sync-shunt-bypass': 'true'}),
        ], self.mock_create_provider.mock_calls)
        self.assertEqual([
            mock.call.debug(
                'For %s using local_to_me profile %r',
                urllib.quote(u'AUTH_b\u062a/jojo/oo'.encode('utf8')),
                {k: v for k, v in exp_profile.items() if 'secret' not in k}),
            mock.call.debug(
                'For %s using remote_to_me profile %r',
                urllib.quote(u'AUTH_b\u062a/jojo/oo'.encode('utf8')),
                {k: v for k, v in controller.remote_to_me_profile.items()
                 if 'secret' not in k}),
        ], self.mock_logger.mock_calls)

    def test_container_get_only_remote_bucket_exists(self):
        controller, req = self.controller_for(
            u'AUTH_b\u062a', 'jojo',
            query_string='limit=10&marker=a%20b&prefix=abc&delimiter=def',
            req_kwargs={'headers': {'Accept': 'application/json'}})
        self.mock_ltm_provider.list_objects.side_effect = [
            ProviderResponse(False, 404, {}, ''),
        ]
        self.mock_rtm_provider.list_objects.side_effect = [
            ProviderResponse(True, 200, {}, [{
                'hash': 'fjoEtagggg',
                'name': u'b\u062ac',
                'last_modified': '2018-02-01T22:21:57Z',
                'bytes': 90,
                'content_location': 'a;loc3',
                'content_type': 'application/octet-stream',
            }, {
                'subdir': u'd\u062ae',
                'content_location': 'another;loc3',
            }]),
            ProviderResponse(True, 200, {}, [{
                'subdir': u'f\u062ag',
                'content_location': 'another;loc4',
            }, {
                'hash': 'abcEtagdef',
                'name': u'h\u062ai',
                'last_modified': '2018-03-01T22:21:57Z',
                'bytes': 91,
                'content_location': 'a;loc4',
                'content_type': 'application/octet-stream',
            }]),
            ProviderResponse(True, 200, {}, []),
        ]
        got = controller.GET(req)

        self.assertEqual([{
            'hash': 'fjoEtagggg',
            'name': u'b\u062ac',
            'last_modified': '2018-02-01T22:21:57Z',
            'bytes': 90,
            'content_location': ['a;loc3'],
            'content_type': 'application/octet-stream',
        }, {
            'subdir': u'd\u062ae',
            'content_location': ['another;loc3'],
        }, {
            'subdir': u'f\u062ag',
            'content_location': ['another;loc4'],
        }, {
            'hash': 'abcEtagdef',
            'name': u'h\u062ai',
            'last_modified': '2018-03-01T22:21:57Z',
            'bytes': 91,
            'content_location': ['a;loc4'],
            'content_type': 'application/octet-stream',
        }], json.loads(got.body))
        self.assertEqual([
            # marker, limit, prefix, delimiter
            mock.call.list_objects('a b', 10, 'abc', 'def'),
        ], self.mock_ltm_provider.mock_calls)
        self.assertEqual([
            mock.call.list_objects('a b', 10, 'abc', 'def'),
            mock.call.list_objects('d\xd8\xaae', 10, 'abc', 'def'),
            mock.call.list_objects('h\xd8\xaai', 10, 'abc', 'def')
        ], self.mock_rtm_provider.mock_calls)

    def test_container_get_local_has_non_404_error(self):
        controller, req = self.controller_for(
            u'AUTH_b\u062a', 'jojo',
            query_string='limit=10&marker=a%20b&prefix=abc&delimiter=def',
            req_kwargs={'headers': {'Accept': 'application/json'}})
        self.mock_ltm_provider.list_objects.side_effect = [
            ProviderResponse(False, 400, {}, ''),
        ]
        got = controller.GET(req)

        self.assertEqual(400, got.status_int)
        self.assertEqual([
            # marker, limit, prefix, delimiter
            mock.call.list_objects('a b', 10, 'abc', 'def'),
        ], self.mock_ltm_provider.mock_calls)
        self.assertEqual([], self.mock_rtm_provider.mock_calls)

    def test_container_get_local_404_remote_non_404_error(self):
        controller, req = self.controller_for(
            u'AUTH_b\u062a', 'jojo',
            query_string='limit=10&marker=a%20b&prefix=abc&delimiter=def',
            req_kwargs={'headers': {'Accept': 'application/json'}})
        self.mock_ltm_provider.list_objects.side_effect = [
            ProviderResponse(False, 404, {}, ''),
        ]
        self.mock_rtm_provider.list_objects.side_effect = [
            ProviderResponse(False, 400, {}, ''),
        ]
        got = controller.GET(req)

        self.assertEqual(400, got.status_int)
        self.assertEqual([
            # marker, limit, prefix, delimiter
            mock.call.list_objects('a b', 10, 'abc', 'def'),
        ], self.mock_ltm_provider.mock_calls)
        self.assertEqual([
            mock.call.list_objects('a b', 10, 'abc', 'def'),
        ], self.mock_rtm_provider.mock_calls)

    def test_container_get_local_and_remote_buckets_404_bucket_ok(self):
        controller, req = self.controller_for(
            u'AUTH_b\u062a', 'jojo',
            query_string='limit=10&marker=a%20b&prefix=abc&delimiter=def',
            req_kwargs={'headers': {'Accept': 'application/json'}})
        self.mock_ltm_provider.list_objects.side_effect = [
            ProviderResponse(False, 404, {}, ''),
        ]
        self.mock_rtm_provider.list_objects.side_effect = [
            ProviderResponse(False, 404, {}, ''),
        ]
        got = controller.GET(req)

        self.assertEqual(404, got.status_int)
        self.assertEqual([
            # marker, limit, prefix, delimiter
            mock.call.list_objects('a b', 10, 'abc', 'def'),
        ], self.mock_ltm_provider.mock_calls)
        self.assertEqual([
            mock.call.list_objects('a b', 10, 'abc', 'def'),
        ], self.mock_rtm_provider.mock_calls)

    def test_container_get_only_local_bucket_exists(self):
        controller, req = self.controller_for(
            u'AUTH_b\u062a', 'jojo',
            query_string='limit=10&marker=a%20b&prefix=abc&delimiter=def',
            req_kwargs={'headers': {'Accept': 'application/json'}})
        self.mock_ltm_provider.list_objects.side_effect = [
            ProviderResponse(True, 200, {}, [{
                'hash': 'abcEtagdef',
                'name': u'a\u062ab',
                'last_modified': '2018-05-01T22:21:57Z',
                'bytes': 88,
                'content_location': 'a;loc',
                'content_type': 'application/octet-stream',
            }, {
                'subdir': u'c\u062ad',
                'content_location': 'another;loc',
            }]),
            ProviderResponse(True, 200, {}, [{
                'subdir': u'e\u062af',
                'content_location': 'another;loc2',
            }, {
                'hash': 'ijkEtaglmn',
                'name': u'g\u062ah',
                'last_modified': '2018-04-01T22:21:57Z',
                'bytes': 89,
                'content_location': 'a;loc2',
                'content_type': 'application/octet-stream',
            }]),
            ProviderResponse(True, 200, {}, []),
        ]
        self.mock_rtm_provider.list_objects.side_effect = [
            ProviderResponse(False, 404, {}, ''),
        ]
        got = controller.GET(req)

        self.assertEqual([{
            'hash': 'abcEtagdef',
            'name': u'a\u062ab',
            'last_modified': '2018-05-01T22:21:57Z',
            'bytes': 88,
            'content_location': ['a;loc'],
            'content_type': 'application/octet-stream',
        }, {
            'subdir': u'c\u062ad',
            'content_location': ['another;loc'],
        }, {
            'subdir': u'e\u062af',
            'content_location': ['another;loc2'],
        }, {
            'hash': 'ijkEtaglmn',
            'name': u'g\u062ah',
            'last_modified': '2018-04-01T22:21:57Z',
            'bytes': 89,
            'content_location': ['a;loc2'],
            'content_type': 'application/octet-stream',
        }], json.loads(got.body))
        self.assertEqual([
            # marker, limit, prefix, delimiter
            mock.call.list_objects('a b', 10, 'abc', 'def'),
            mock.call.list_objects('c\xd8\xaad', 10, 'abc', 'def'),
            mock.call.list_objects('g\xd8\xaah', 10, 'abc', 'def'),
        ], self.mock_ltm_provider.mock_calls)
        self.assertEqual([
            mock.call.list_objects('a b', 10, 'abc', 'def'),
        ], self.mock_rtm_provider.mock_calls)

    def test_container_get_remote_non_404_errors(self):
        # Is this wise? Or should the non-404 error from the remote be what we
        # return??
        controller, req = self.controller_for(
            u'AUTH_b\u062a', 'jojo',
            query_string='limit=10&marker=a%20b&prefix=abc&delimiter=def',
            req_kwargs={'headers': {'Accept': 'application/json'}})
        self.mock_ltm_provider.list_objects.side_effect = [
            ProviderResponse(True, 200, {}, [{
                'hash': 'abcEtagdef',
                'name': u'a\u062ab',
                'last_modified': '2018-05-01T22:21:57Z',
                'bytes': 88,
                'content_location': 'a;loc',
                'content_type': 'application/octet-stream',
            }, {
                'subdir': u'c\u062ad',
                'content_location': 'another;loc',
            }]),
            ProviderResponse(True, 200, {}, [{
                'subdir': u'e\u062af',
                'content_location': 'another;loc2',
            }, {
                'hash': 'ijkEtaglmn',
                'name': u'g\u062ah',
                'last_modified': '2018-04-01T22:21:57Z',
                'bytes': 89,
                'content_location': 'a;loc2',
                'content_type': 'application/octet-stream',
            }]),
            ProviderResponse(True, 200, {}, []),
        ]
        self.mock_rtm_provider.list_objects.side_effect = [
            ProviderResponse(False, 400, {}, ''),
        ]
        got = controller.GET(req)

        self.assertEqual([{
            'hash': 'abcEtagdef',
            'name': u'a\u062ab',
            'last_modified': '2018-05-01T22:21:57Z',
            'bytes': 88,
            'content_location': ['a;loc'],
            'content_type': 'application/octet-stream',
        }, {
            'subdir': u'c\u062ad',
            'content_location': ['another;loc'],
        }, {
            'subdir': u'e\u062af',
            'content_location': ['another;loc2'],
        }, {
            'hash': 'ijkEtaglmn',
            'name': u'g\u062ah',
            'last_modified': '2018-04-01T22:21:57Z',
            'bytes': 89,
            'content_location': ['a;loc2'],
            'content_type': 'application/octet-stream',
        }], json.loads(got.body))
        self.assertEqual([
            # marker, limit, prefix, delimiter
            mock.call.list_objects('a b', 10, 'abc', 'def'),
            mock.call.list_objects('c\xd8\xaad', 10, 'abc', 'def'),
            mock.call.list_objects('g\xd8\xaah', 10, 'abc', 'def'),
        ], self.mock_ltm_provider.mock_calls)
        self.assertEqual([
            mock.call.list_objects('a b', 10, 'abc', 'def'),
        ], self.mock_rtm_provider.mock_calls)

    def test_container_get_limited_only_local(self):
        controller, req = self.controller_for(
            u'AUTH_b\u062a', 'jojo',
            query_string='limit=3&marker=a%20b&prefix=abc&delimiter=def',
            req_kwargs={'headers': {'Accept': 'application/json'}})
        self.mock_ltm_provider.list_objects.side_effect = [
            ProviderResponse(True, 200, {}, [{
                'hash': 'abcEtagdef',
                'name': u'a\u062ab',
                'last_modified': '2018-05-01T22:21:57Z',
                'bytes': 88,
                'content_location': 'a;loc',
                'content_type': 'application/octet-stream',
            }, {
                'subdir': u'c\u062ad',
                'content_location': 'another;loc',
            }]),
            ProviderResponse(True, 200, {}, [{
                'subdir': u'e\u062af',
                'content_location': 'another;loc2',
            }, {
                'hash': 'ijkEtaglmn',
                'name': u'g\u062ah',
                'last_modified': '2018-04-01T22:21:57Z',
                'bytes': 89,
                'content_location': 'a;loc2',
                'content_type': 'application/octet-stream',
            }]),
            ProviderResponse(True, 200, {}, []),
        ]
        self.mock_rtm_provider.list_objects.side_effect = [
            ProviderResponse(True, 200, {}, []),
        ]
        got = controller.GET(req)

        self.assertEqual([{
            'hash': 'abcEtagdef',
            'name': u'a\u062ab',
            'last_modified': '2018-05-01T22:21:57Z',
            'bytes': 88,
            'content_location': ['a;loc'],
            'content_type': 'application/octet-stream',
        }, {
            'subdir': u'c\u062ad',
            'content_location': ['another;loc'],
        }, {
            'subdir': u'e\u062af',
            'content_location': ['another;loc2'],
        }], json.loads(got.body))
        self.assertEqual([
            # marker, limit, prefix, delimiter
            mock.call.list_objects('a b', 3, 'abc', 'def'),
            mock.call.list_objects('c\xd8\xaad', 3, 'abc', 'def'),
        ], self.mock_ltm_provider.mock_calls)
        self.assertEqual([
            mock.call.list_objects('a b', 3, 'abc', 'def'),
        ], self.mock_rtm_provider.mock_calls)

    def test_container_get_limited(self):
        controller, req = self.controller_for(
            u'AUTH_b\u062a', 'jojo',
            query_string='limit=6&marker=a%20b&prefix=abc&delimiter=def',
            req_kwargs={'headers': {'Accept': 'application/json'}})
        self.mock_ltm_provider.list_objects.side_effect = [
            ProviderResponse(True, 200, {}, [{
                'hash': 'abcEtagdef',
                'name': u'a\u062ab',
                'last_modified': '2018-05-01T22:21:57Z',
                'bytes': 88,
                'content_location': 'a;loc',
                'content_type': 'application/octet-stream',
            }, {
                'subdir': u'c\u062ad',
                'content_location': 'another;loc',
            }]),
            ProviderResponse(True, 200, {}, [{
                'subdir': u'e\u062af',
                'content_location': 'another;loc2',
            }, {
                'hash': 'ijkEtaglmn',
                'name': u'g\u062ah',
                'last_modified': '2018-04-01T22:21:57Z',
                'bytes': 89,
                'content_location': 'a;loc2',
                'content_type': 'application/octet-stream',
            }]),
            ProviderResponse(True, 200, {}, []),
        ]
        self.mock_rtm_provider.list_objects.side_effect = [
            ProviderResponse(True, 200, {}, [{
                'hash': 'fjoEtagggg',
                'name': u'b\u062ac',
                'last_modified': '2018-02-01T22:21:57Z',
                'bytes': 90,
                'content_location': 'a;loc3',
                'content_type': 'application/octet-stream',
            }, {
                'subdir': u'd\u062ae',
                'content_location': 'another;loc3',
            }]),
            ProviderResponse(True, 200, {}, [{
                'subdir': u'f\u062ag',
                'content_location': 'another;loc4',
            }, {
                'hash': 'abcEtagdef',
                'name': u'h\u062ai',
                'last_modified': '2018-03-01T22:21:57Z',
                'bytes': 91,
                'content_location': 'a;loc4',
                'content_type': 'application/octet-stream',
            }]),
            ProviderResponse(True, 200, {}, []),
        ]
        got = controller.GET(req)

        self.assertEqual([{
            'hash': 'abcEtagdef',
            'name': u'a\u062ab',
            'last_modified': '2018-05-01T22:21:57Z',
            'bytes': 88,
            'content_location': ['a;loc'],
            'content_type': 'application/octet-stream',
        }, {
            'hash': 'fjoEtagggg',
            'name': u'b\u062ac',
            'last_modified': '2018-02-01T22:21:57Z',
            'bytes': 90,
            'content_location': ['a;loc3'],
            'content_type': 'application/octet-stream',
        }, {
            'subdir': u'c\u062ad',
            'content_location': ['another;loc'],
        }, {
            'subdir': u'd\u062ae',
            'content_location': ['another;loc3'],
        }, {
            'subdir': u'e\u062af',
            'content_location': ['another;loc2'],
        }, {
            'subdir': u'f\u062ag',
            'content_location': ['another;loc4'],
        }], json.loads(got.body))
        self.assertEqual([
            # marker, limit, prefix, delimiter
            mock.call.list_objects('a b', 6, 'abc', 'def'),
            mock.call.list_objects('c\xd8\xaad', 6, 'abc', 'def'),
        ], self.mock_ltm_provider.mock_calls)
        self.assertEqual([
            mock.call.list_objects('a b', 6, 'abc', 'def'),
            mock.call.list_objects('d\xd8\xaae', 6, 'abc', 'def'),
        ], self.mock_rtm_provider.mock_calls)

    def test_container_get(self):
        controller, req = self.controller_for(
            u'AUTH_b\u062a', 'jojo',
            query_string='limit=10&marker=a%20b&prefix=abc&delimiter=def',
            req_kwargs={'headers': {'Accept': 'application/json'}})
        self.mock_ltm_provider.list_objects.side_effect = [
            ProviderResponse(True, 200, {}, [{
                'hash': 'abcEtagdef',
                'name': u'a\u062ab',
                'last_modified': '2018-05-01T22:21:57Z',
                'bytes': 88,
                'content_location': 'a;loc',
                'content_type': 'application/octet-stream',
            }, {
                'subdir': u'c\u062ad',
                'content_location': 'another;loc',
            }]),
            ProviderResponse(True, 200, {}, [{
                'subdir': u'e\u062af',
                'content_location': 'another;loc2',
            }, {
                'hash': 'ijkEtaglmn',
                'name': u'g\u062ah',
                'last_modified': '2018-04-01T22:21:57Z',
                'bytes': 89,
                'content_location': 'a;loc2',
                'content_type': 'application/octet-stream',
            }]),
            ProviderResponse(True, 200, {}, []),
        ]
        self.mock_rtm_provider.list_objects.side_effect = [
            ProviderResponse(True, 200, {}, [{
                'hash': 'fjoEtagggg',
                'name': u'b\u062ac',
                'last_modified': '2018-02-01T22:21:57Z',
                'bytes': 90,
                'content_location': 'a;loc3',
                'content_type': 'application/octet-stream',
            }, {
                'subdir': u'd\u062ae',
                'content_location': 'another;loc3',
            }]),
            ProviderResponse(True, 200, {}, [{
                'subdir': u'f\u062ag',
                'content_location': 'another;loc4',
            }, {
                'hash': 'abcEtagdef',
                'name': u'h\u062ai',
                'last_modified': '2018-03-01T22:21:57Z',
                'bytes': 91,
                'content_location': 'a;loc4',
                'content_type': 'application/octet-stream',
            }]),
            ProviderResponse(True, 200, {}, []),
        ]
        got = controller.GET(req)

        self.assertEqual([{
            'hash': 'abcEtagdef',
            'name': u'a\u062ab',
            'last_modified': '2018-05-01T22:21:57Z',
            'bytes': 88,
            'content_location': ['a;loc'],
            'content_type': 'application/octet-stream',
        }, {
            'hash': 'fjoEtagggg',
            'name': u'b\u062ac',
            'last_modified': '2018-02-01T22:21:57Z',
            'bytes': 90,
            'content_location': ['a;loc3'],
            'content_type': 'application/octet-stream',
        }, {
            'subdir': u'c\u062ad',
            'content_location': ['another;loc'],
        }, {
            'subdir': u'd\u062ae',
            'content_location': ['another;loc3'],
        }, {
            'subdir': u'e\u062af',
            'content_location': ['another;loc2'],
        }, {
            'subdir': u'f\u062ag',
            'content_location': ['another;loc4'],
        }, {
            'hash': 'ijkEtaglmn',
            'name': u'g\u062ah',
            'last_modified': '2018-04-01T22:21:57Z',
            'bytes': 89,
            'content_location': ['a;loc2'],
            'content_type': 'application/octet-stream',
        }, {
            'hash': 'abcEtagdef',
            'name': u'h\u062ai',
            'last_modified': '2018-03-01T22:21:57Z',
            'bytes': 91,
            'content_location': ['a;loc4'],
            'content_type': 'application/octet-stream',
        }], json.loads(got.body))
        self.assertEqual([
            # marker, limit, prefix, delimiter
            mock.call.list_objects('a b', 10, 'abc', 'def'),
            mock.call.list_objects('c\xd8\xaad', 10, 'abc', 'def'),
            mock.call.list_objects('g\xd8\xaah', 10, 'abc', 'def'),
        ], self.mock_ltm_provider.mock_calls)
        self.assertEqual([
            mock.call.list_objects('a b', 10, 'abc', 'def'),
            mock.call.list_objects('d\xd8\xaae', 10, 'abc', 'def'),
            mock.call.list_objects('h\xd8\xaai', 10, 'abc', 'def')
        ], self.mock_rtm_provider.mock_calls)

    def test_get_and_head_hits_local_first_succeeds(self):
        obj_name = u'\u062aoo'
        for verb, fn_name in (('HEAD', 'head_object'), ('GET', 'get_object')):
            mock_provider_fn = getattr(self.mock_ltm_provider, fn_name)
            mock_provider_fn.return_value = ProviderResponse(
                success=True, status=200, headers={'Content-Length': '88'},
                body=iter(['']) if verb == 'HEAD' else iter('A' * 88))
            controller, req = self.controller_for(u'AUTH_b\u062a', 'jojo',
                                                  obj_name, verb)
            fn = getattr(controller, verb)
            got = fn(req)
            self.assertEqual(200, got.status_int)
            self.assertEqual('88', got.headers['Content-Length'])
            if verb == 'HEAD':
                self.assertEqual('', ''.join(got.body))
            else:
                self.assertEqual('A' * 88, ''.join(got.body))

    def test_get_and_head_fall_back_to_remote_succeeds(self):
        obj_name = u'\u062aoo'
        for verb, fn_name in (('HEAD', 'head_object'), ('GET', 'get_object')):
            mock_provider_fn = getattr(self.mock_ltm_provider, fn_name)
            mock_provider_fn.return_value = ProviderResponse(
                success=False, status=404, headers={'Content-Length': '88'},
                body=iter(['']) if verb == 'HEAD' else iter('A' * 88))
            mock_provider_fn = getattr(self.mock_rtm_provider, fn_name)
            mock_provider_fn.return_value = ProviderResponse(
                success=True, status=200, headers={'Content-Length': '87'},
                body=iter(['']) if verb == 'HEAD' else iter('A' * 87))
            controller, req = self.controller_for(u'AUTH_b\u062a', 'jojo',
                                                  obj_name, verb)
            fn = getattr(controller, verb)
            got = fn(req)
            self.assertEqual(200, got.status_int)
            self.assertEqual('87', got.headers['Content-Length'])
            if verb == 'HEAD':
                self.assertEqual('', ''.join(got.body))
            else:
                self.assertEqual('A' * 87, ''.join(got.body))

    def test_get_and_head_fall_back_to_remote_fails(self):
        obj_name = u'\u062aoo'
        for verb, fn_name in (('HEAD', 'head_object'), ('GET', 'get_object')):
            mock_provider_fn = getattr(self.mock_ltm_provider, fn_name)
            mock_provider_fn.return_value = ProviderResponse(
                success=False, status=404, headers={'Content-Length': '88'},
                body=iter(['']) if verb == 'HEAD' else iter('A' * 88))
            mock_provider_fn = getattr(self.mock_rtm_provider, fn_name)
            mock_provider_fn.return_value = ProviderResponse(
                success=False, status=403, headers={'Content-Length': '86'},
                body=iter(['']) if verb == 'HEAD' else iter('A' * 86))
            controller, req = self.controller_for(u'AUTH_b\u062a', 'jojo',
                                                  obj_name, verb)
            fn = getattr(controller, verb)
            got = fn(req)
            self.assertEqual(403, got.status_int)
            self.assertEqual('86', got.headers['Content-Length'])
            if verb == 'HEAD':
                self.assertEqual('', ''.join(got.body))
            else:
                self.assertEqual('A' * 86, ''.join(got.body))

    def test_get_and_head_both_404_container_exists(self):
        obj_name = u'\u062aoo'
        for verb, fn_name in (('HEAD', 'head_object'), ('GET', 'get_object')):
            mock_provider_fn = getattr(self.mock_ltm_provider, fn_name)
            mock_provider_fn.return_value = ProviderResponse(
                success=False, status=404, headers={'Content-Length': '88'},
                body=iter(['']) if verb == 'HEAD' else iter('A' * 88))
            mock_provider_fn = getattr(self.mock_rtm_provider, fn_name)
            body = 'The specified key does not exist.'
            mock_provider_fn.return_value = ProviderResponse(
                success=False, status=404,
                headers={'Content-Length': str(len(body))},
                body=iter(['']) if verb == 'HEAD' else iter(body))
            controller, req = self.controller_for(u'AUTH_b\u062a', 'jojo',
                                                  obj_name, verb)
            fn = getattr(controller, verb)
            got = fn(req)
            self.assertEqual(404, got.status_int)
            self.assertEqual(str(len(body)), got.headers['Content-Length'])
            if verb == 'HEAD':
                self.assertEqual('', ''.join(got.body))
            else:
                self.assertEqual(body, ''.join(got.body))
            if verb == 'GET':
                infocache = req.environ['swift.infocache']
                cache_item = infocache[(u'container/%s/%s' % (
                    u'AUTH_b\u062a', 'jojo')).encode('utf8')]
                self.assertEqual(200, cache_item['status'])
            else:
                self.assertNotIn('swift.infocache', req.environ)

    def test_get_and_head_both_404_container_does_not_exist(self):
        obj_name = u'\u062aoo'
        for verb, fn_name in (('HEAD', 'head_object'), ('GET', 'get_object')):
            mock_provider_fn = getattr(self.mock_ltm_provider, fn_name)
            mock_provider_fn.return_value = ProviderResponse(
                success=False, status=404, headers={'Content-Length': '88'},
                body=iter(['']) if verb == 'HEAD' else iter('A' * 88))
            mock_provider_fn = getattr(self.mock_rtm_provider, fn_name)
            body = 'No bucket, bro.'
            mock_provider_fn.return_value = ProviderResponse(
                success=False, status=404,
                headers={'Content-Length': str(len(body))},
                body=iter(['']) if verb == 'HEAD' else iter(body))
            controller, req = self.controller_for(u'AUTH_b\u062a', 'jojo',
                                                  obj_name, verb)
            fn = getattr(controller, verb)
            got = fn(req)
            self.assertEqual(404, got.status_int)
            self.assertEqual(str(len(body)), got.headers['Content-Length'])
            if verb == 'HEAD':
                self.assertEqual('', ''.join(got.body))
            else:
                self.assertEqual(body, ''.join(got.body))
            self.assertNotIn('swift.infocache', req.environ)

    def test_put_success(self):
        obj_name = u'\u062aoo'
        mock_provider_fn = self.mock_ltm_provider.put_object
        mock_provider_fn.return_value = ProviderResponse(
            success=True, status=200, headers={'Content-Length': '68'},
            body=iter('B' * 68))

        controller, req = self.controller_for(u'AUTH_b\u062a', 'jojo',
                                              obj_name, 'PUT')
        got = controller.PUT(req)
        self.assertEqual(201, got.status_int)  # swift3 mw will expect 201
        self.assertEqual('68', got.headers['Content-Length'])
        self.assertEqual('B' * 68, ''.join(got.body))

    def test_put_failure(self):
        obj_name = u'\u062aoo'
        mock_provider_fn = self.mock_ltm_provider.put_object
        mock_provider_fn.return_value = ProviderResponse(
            success=False, status=400, headers={'Content-Length': '63'},
            body=iter('B' * 63))

        controller, req = self.controller_for(u'AUTH_b\u062a', 'jojo',
                                              obj_name, 'PUT')
        got = controller.PUT(req)
        self.assertEqual(400, got.status_int)
        self.assertEqual('63', got.headers['Content-Length'])
        self.assertEqual('B' * 63, ''.join(got.body))

    def test_obj_not_implemented_yet(self):
        # Remove these as object verb support is added (where applicable)
        for verb in ('POST', 'OPTIONS', 'DELETE'):
            controller, req = self.controller_for(u'AUTH_b\u062a', 'jojo',
                                                  'oo', verb)
            fn = getattr(controller, verb)
            got = fn(req)
            self.assertEqual('501 Not Implemented', got.status)

    def test_get_controller_no_acct_actions_allowed(self):
        for verb in ('GET', 'GET', 'HEAD', 'PUT', 'POST', 'OPTIONS', 'DELETE'):
            req = swob.Request.blank(
                'http://a.b.c:123/v1/AUTH_jojo',
                environ={'REQUEST_METHOD': verb,
                         app.S3_IDENTITY_ENV_KEY: self.s3_identity})
            with self.assertRaises(swob.HTTPException) as cm:
                self.app.get_controller(req)
            self.assertEqual('403 Forbidden', cm.exception.status)
            self.assertEqual('Account operations are not supported.',
                             cm.exception.body)

    def test_get_controller_no_non_get_container_actions_allowed(self):
        for verb in ('HEAD', 'PUT', 'POST', 'OPTIONS', 'DELETE'):
            req = swob.Request.blank(
                'http://a.b.c:123/v1/AUTH_jojo/c',
                environ={'REQUEST_METHOD': verb,
                         app.S3_IDENTITY_ENV_KEY: self.s3_identity})
            with self.assertRaises(swob.HTTPException) as cm:
                self.app.get_controller(req)
            self.assertEqual('403 Forbidden', cm.exception.status)
            self.assertEqual('The only supported container operation is '
                             'GET (listing).', cm.exception.body)

    def test_get_controller_no_sync_profile_match(self):
        req = swob.Request.blank(
            'http://a.b.c:123/v1/AUTH_jojo/c',
            environ={'REQUEST_METHOD': 'GET',
                     app.S3_IDENTITY_ENV_KEY: self.s3_identity})
        with self.assertRaises(swob.HTTPException) as cm:
            self.app.get_controller(req)
        self.assertEqual('403 Forbidden', cm.exception.status)
        self.assertEqual('No matching sync profile.', cm.exception.body)

        for verb in ('HEAD', 'GET', 'PUT', 'POST', 'OPTIONS', 'DELETE'):
            req = swob.Request.blank(
                'http://a.b.c:123/v1/AUTH_jojo/c/o',
                environ={'REQUEST_METHOD': verb,
                         app.S3_IDENTITY_ENV_KEY: self.s3_identity})
            with self.assertRaises(swob.HTTPException) as cm:
                self.app.get_controller(req)
            self.assertEqual('403 Forbidden', cm.exception.status)
            self.assertEqual('No matching sync profile.', cm.exception.body)

    def test_get_controller_not_s3_api(self):
        # For now, only S3 API access is supported
        for verb in ('HEAD', 'GET', 'PUT', 'POST', 'OPTIONS', 'DELETE'):
            for a, c, profile in [(u'AUTH_\u062aa', u'sw\u00e9ft',
                                   self.sync_conf['containers'][0]),
                                  ('AUTH_a', 's3',
                                   self.sync_conf['containers'][1]),
                                  (u'AUTH_b\u062a', 'crazy1',
                                   self.sync_conf['containers'][2])]:
                req = swob.Request.blank('http://a.b.c:123/v1/%s/%s/o' % (
                    urllib.quote(a.encode('utf8')),
                    urllib.quote(c.encode('utf8'))),
                    environ={'REQUEST_METHOD': verb})
                with self.assertRaises(swob.HTTPException) as cm:
                    self.app.get_controller(req)
                self.assertEqual('501 Not Implemented', cm.exception.status)

    def test_get_controller_ok(self):
        for verb in ('HEAD', 'GET', 'PUT', 'POST', 'OPTIONS', 'DELETE'):
            for a, c, profile in [(u'AUTH_\u062aa', u'sw\u00e9ft',
                                   self.sync_conf['containers'][0]),
                                  ('AUTH_a', 's3',
                                   self.sync_conf['containers'][1]),
                                  (u'AUTH_b\u062a', 'crazy1',
                                   self.sync_conf['containers'][2]),
                                  # NOTE: for S3 API access, containers can't
                                  # ever be invalid S3 bucket names (i.e. no
                                  # Unicode, etc.).
                                  ]:
                req = swob.Request.blank('http://a.b.c:123/v1/%s/%s/o' % (
                    urllib.quote(a.encode('utf8')),
                    urllib.quote(c.encode('utf8'))),
                    environ={'REQUEST_METHOD': verb,
                             app.S3_IDENTITY_ENV_KEY: self.s3_identity})
                klass, kwargs = self.app.get_controller(req)
                self.assertEqual(app.CloudConnectorController, klass)
                self.assertEqual({
                    'local_to_me_profile': profile,
                    'remote_to_me_profile': {
                        "account": profile['account'],
                        "container": profile['container'],
                        "aws_bucket": profile['container'],
                        "aws_endpoint": self.swift_baseurl,
                        "aws_identity": self.s3_identity['access_key'],
                        "aws_secret": self.s3_identity['secret_key'],
                        "protocol": "s3",
                        "custom_prefix": "",  # "native" access
                    },
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
