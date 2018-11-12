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

import mock
import s3_sync
import sys
import unittest


class TestMain(unittest.TestCase):

    def setUp(self):
        # avoid loading boto3 and SyncContainer
        sys.modules['s3_sync.sync_container'] = mock.Mock()

    def tearDown(self):
        del sys.modules['s3_sync.sync_container']

    @mock.patch('s3_sync.daemon_utils.os.path.exists')
    @mock.patch('s3_sync.daemon_utils.logging')
    @mock.patch('s3_sync.__main__.Crawler')
    def test_log_lvl(self, crawler_mock, logging_mock, exists_mock):
        exists_mock.return_value = True
        mock_logger = mock.Mock()
        logging_mock.getLogger.return_value = mock_logger

        test_params = [
            {'conf_level': None,
             'args': [],
             'expected': 'INFO'},
            {'conf_level': 'debug',
             'args': [],
             'expected': 'DEBUG'},
            {'conf_level': 'warn',
             'args': ['--log-level', 'debug'],
             'expected': 'DEBUG'},
        ]
        defaults = ['main', '--console', '--conf', '/sample/config']

        for params in test_params:
            with mock.patch('s3_sync.daemon_utils.load_config') \
                    as conf_mock, \
                    mock.patch('s3_sync.daemon_utils.sys') as sys_mock:
                sys_mock.argv = defaults + params['args']

                conf_mock.return_value = {}
                if params['conf_level']:
                    conf_mock.return_value['log_level'] = \
                        params['conf_level']

                s3_sync.__main__.main()
            mock_logger.setLevel.assert_has_calls(
                [mock.call(params['expected'])] * 3)
            mock_logger.reset_mock()

    @mock.patch('s3_sync.__main__.os')
    @mock.patch('s3_sync.__main__.initialize_loggers')
    @mock.patch('s3_sync.__main__.setup_context')
    @mock.patch('s3_sync.__main__.Crawler')
    def test_proxy_settings(self, crawler_mock, context_mock, logger_mock,
                            os_mock):
        test_params = [
            {'http_proxy': 'http://some-proxy:1234'},
            {'https_proxy': 'https://some-proxy:1443'}
        ]

        mock_args = mock.Mock(console=False, log_level='debug', once=False)

        for conf in test_params:
            context_mock.return_value = (mock_args, conf)
            os_mock.environ = {}
            s3_sync.__main__.main()
            if 'http_proxy' in conf:
                self.assertEqual(
                    conf['http_proxy'], os_mock.environ['http_proxy'])
                self.assertNotIn('https_proxy', os_mock.environ)
            else:
                self.assertEqual(
                    conf['https_proxy'], os_mock.environ['https_proxy'])
                self.assertNotIn('http_proxy', os_mock.environ)
            context_mock.reset_mock()

    @mock.patch('s3_sync.__main__.initialize_loggers')
    @mock.patch('s3_sync.__main__.setup_context')
    @mock.patch('s3_sync.__main__.Crawler')
    def test_verification_slack(self, crawler_mock, context_mock, logger_mock):
        test_params = [
            {'verification_slack': 24 * 60},
            {}
        ]

        mock_args = mock.Mock(console=False, log_level='debug', once=False)

        for conf in test_params:
            context_mock.return_value = (mock_args, conf)

            s3_sync.__main__.main()
            if 'verification_slack' in conf:
                old_value = conf['verification_slack']
                self.assertEqual(old_value, conf['verification_slack'])
            else:
                self.assertEqual(60, conf['verification_slack'])
            context_mock.reset_mock()

    @mock.patch('s3_sync.__main__.initialize_loggers')
    @mock.patch('s3_sync.__main__.setup_context')
    @mock.patch('s3_sync.__main__.Crawler')
    def test_run_once(self, crawler_mock, context_mock, logger_mock):
        tests = [mock.Mock(console=False, log_level='debug', once=False),
                 mock.Mock(console=False, log_level='debug', once=True)]

        # avoid loading boto3 and SyncContainer
        sys.modules['s3_sync.sync_container'] = mock.Mock()
        for test in tests:
            context_mock.return_value = (test, {})
            mocked_crawler = crawler_mock.return_value
            s3_sync.__main__.main()
            if test.once:
                mocked_crawler.run_once.assert_called_once_with()
            else:
                mocked_crawler.run_always.assert_called_once_with()

            crawler_mock.reset_mock()
            context_mock.reset_mock()
