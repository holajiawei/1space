import time
from . import TestCloudSyncBase
from s3_sync.daemon_utils import initialize_loggers


class TestDaemonUtils(TestCloudSyncBase):
    def test_syslog(self):
        initialize_loggers({'log_file': '/tmp/test_file',
                            'log_level': 'debug',
                            'syslog': {'host': '1space'}})
        retries = 15
        asserts = set(
            ['s3-sync [DEBUG]: Using syslog',
             'boto3 [DEBUG]: Using syslog',
             'botocore [DEBUG]: Using syslog'])
        while retries and asserts:
            asserted = set()
            try:
                syslog = open('/var/log/syslog').read()
                for line in asserts:
                    self.assertIn(line, syslog)
                    asserted.add(line)
            except AssertionError:
                retries -= 1
                if not retries:
                    raise
                time.sleep(0.1)
            asserts -= asserted
