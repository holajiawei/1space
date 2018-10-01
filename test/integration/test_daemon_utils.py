from . import TestCloudSyncBase
from s3_sync.daemon_utils import initialize_loggers


class TestDaemonUtils(TestCloudSyncBase):
    def setUp(self):
        pass

    def test_syslog(self):
        initialize_loggers({'log_file': '/tmp/test_file',
                            'log_level': 'debug',
                            'syslog': {'host': 'localhost'}})
        self.assertTrue("s3-sync [DEBUG]: Using syslog" in
                        open("/var/log/syslog").read())
