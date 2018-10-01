import mock
import s3_sync.daemon_utils
import StringIO
import sys
import unittest
import socket


class TestDaemonUtils(unittest.TestCase):

    def setUp(self):
        self.old_swift_module = sys.modules.get('swift')
        if 'swift' in sys.modules:
            del sys.modules['swift']

    def tearDown(self):
        if 'swift' in sys.modules:
            del sys.modules['swift']
        if self.old_swift_module:
            sys.modules['swift'] = self.old_swift_module

    @mock.patch('s3_sync.daemon_utils.logging')
    def test_old_swift_raises_exception(self, logging_mock):
        sys.modules['swift'] = mock.Mock(__version__='1.0')
        with self.assertRaises(RuntimeError) as context:
            s3_sync.daemon_utils.load_swift('logger')
        error = context.exception
        self.assertTrue(error.message.startswith(
            'Swift version must be at least'))

    @mock.patch('__builtin__.__import__')
    @mock.patch('s3_sync.daemon_utils.time')
    @mock.patch('s3_sync.daemon_utils.logging')
    def test_retry_swift_import(self, logging_mock, time_mock, import_mock):
        errors = [0]

        def _import(*args):
            self.assertEqual('swift', args[0])
            if not errors[0]:
                errors[0] = 1
                raise ImportError('failed to import swift')
            swift_mock = mock.Mock(__version__='5.0')
            sys.modules['swift'] = swift_mock
            return swift_mock

        import_mock.side_effect = _import
        s3_sync.daemon_utils.load_swift('logger')
        time_mock.sleep.assert_called_once_with(5)
        import_mock.assert_has_calls(
            [mock.call('swift', mock.ANY, None, None)] * 2)

    @mock.patch('__builtin__.__import__')
    @mock.patch('s3_sync.daemon_utils.logging')
    def test_swift_import_once(self, logging_mock, import_mock):
        def _import(*args):
            raise ImportError('failed to import swift')

        import_mock.side_effect = _import
        with self.assertRaises(ImportError):
            s3_sync.daemon_utils.load_swift('logger', once=True)
        import_mock.assert_called_once_with('swift', mock.ANY, None, None)

    @mock.patch('sys.stdout', new_callable=StringIO.StringIO)
    @mock.patch('s3_sync.daemon_utils.sys')
    @mock.patch('s3_sync.daemon_utils.os.path.exists')
    def test_setup_context_exits(self, exists_mock, sys_mock, stdout_mock):
        exists_mock.return_value = False
        sys_mock.argv = ['', '--config', '/foo/bar']
        with self.assertRaises(SystemExit):
            s3_sync.daemon_utils.setup_context()
        self.assertEqual('Configuration file does not exist\n',
                         stdout_mock.getvalue())

    @mock.patch('s3_sync.daemon_utils.logging')
    def test_log_file_required(self, logging_mock):
        with self.assertRaises(RuntimeError) as cm:
            s3_sync.daemon_utils.setup_logger('test logger', {})
        self.assertEqual('log file must be set', cm.exception.message)

    @mock.patch('s3_sync.daemon_utils.logging')
    def test_log_file_handler(self, logging_mock):
        handler = logging_mock.handlers.RotatingFileHandler.return_value
        s3_sync.daemon_utils.setup_logger(
            'test logger', {'log_file': '/test/test_file'})
        logging_mock.handlers.RotatingFileHandler.assert_called_once_with(
            '/test/test_file', maxBytes=s3_sync.daemon_utils.MAX_LOG_SIZE,
            backupCount=5)
        logging_mock.getLogger.assert_has_calls([
            mock.call('test logger'),
            mock.call().setLevel('INFO'),
            mock.call().addHandler(handler),
            mock.call('boto3'),
            mock.call().setLevel('INFO'),
            mock.call().addHandler(handler),
            mock.call('botocore'),
            mock.call().setLevel('INFO'),
            mock.call().addHandler(handler)])

    @mock.patch('s3_sync.daemon_utils.logging')
    def test_syslog_handler(self, logging_mock):
        handler = logging_mock.handlers.RotatingFileHandler.return_value
        syslog_handler = logging_mock.handlers.SysLogHandler.return_value
        s3_sync.daemon_utils.setup_logger(
            'test logger', {'log_file': '/test/test_file',
                            'syslog': {'host': 'testhost'}})
        logging_mock.handlers.SysLogHandler.assert_called_once_with(
            address=('testhost', 514),
            socktype=socket.SOCK_DGRAM)
        logging_mock.getLogger.assert_has_calls([
            mock.call('test logger'),
            mock.call().setLevel('INFO'),
            mock.call().addHandler(handler),
            mock.call('boto3'),
            mock.call().setLevel('INFO'),
            mock.call().addHandler(handler),
            mock.call('botocore'),
            mock.call().setLevel('INFO'),
            mock.call().addHandler(handler),
            mock.call().debug('Using syslog'),
            mock.call().setLevel('INFO'),
            mock.call().addHandler(syslog_handler),
            mock.call('boto3'),
            mock.call().setLevel('INFO'),
            mock.call().addHandler(syslog_handler),
            mock.call('botocore'),
            mock.call().setLevel('INFO'),
            mock.call().addHandler(syslog_handler)])
