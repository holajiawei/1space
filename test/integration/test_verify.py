from functools import wraps
import s3_sync.verify
from . import TestCloudSyncBase, clear_swift_container, clear_s3_bucket


def swift_is_unchanged(func):
    @wraps(func)
    def wrapper(test):
        before = test.get_swift_tree(test.swift_dst)
        func(test)
        test.assertEqual(before, test.get_swift_tree(test.swift_dst))
    return wrapper


def s3_is_unchanged(func):
    @wraps(func)
    def wrapper(test):
        before = test.get_s3_tree()
        func(test)
        test.assertEqual(before, test.get_s3_tree())
    return wrapper


class TestVerify(TestCloudSyncBase):
    def setUp(self):
        self.swift_container = self.s3_bucket = None
        for container in self.test_conf['containers']:
            if container['protocol'] == 'swift':
                self.swift_container = container['aws_bucket']
            else:
                self.s3_bucket = container['aws_bucket']
            if self.swift_container and self.s3_bucket:
                break

    def tearDown(self):
        clear_s3_bucket(self.s3_client, self.s3_bucket)
        clear_swift_container(self.swift_dst, self.swift_container)

    @swift_is_unchanged
    def test_swift_no_container(self):
        self.assertEqual(0, s3_sync.verify.main([
            '--protocol=swift',
            '--endpoint=' + self.SWIFT_CREDS['authurl'],
            '--username=' + self.SWIFT_CREDS['dst']['user'],
            '--password=' + self.SWIFT_CREDS['dst']['key'],
        ]))

    @swift_is_unchanged
    def test_swift_single_container(self):
        self.assertEqual(0, s3_sync.verify.main([
            '--protocol=swift',
            '--endpoint=' + self.SWIFT_CREDS['authurl'],
            '--username=' + self.SWIFT_CREDS['dst']['user'],
            '--password=' + self.SWIFT_CREDS['dst']['key'],
            '--bucket=' + self.swift_container,
        ]))

    @swift_is_unchanged
    def test_swift_admin_cross_account(self):
        self.assertEqual(0, s3_sync.verify.main([
            '--protocol=swift',
            '--endpoint=' + self.SWIFT_CREDS['authurl'],
            '--username=' + self.SWIFT_CREDS['admin']['user'],
            '--password=' + self.SWIFT_CREDS['admin']['key'],
            '--account=AUTH_\xd8\xaaacct2',
            '--bucket=' + self.swift_container,
        ]))

    @swift_is_unchanged
    def test_swift_admin_wrong_account(self):
        # Note: "wrong account", here, means an account that doesn't have the
        # specified bucket.
        # ...which is basically the same as `test_swift_bad_container` but with
        # an admin user.
        msg = ('Unexpected status code checking PUT: 404 Not Found')
        self.assertEqual(msg, s3_sync.verify.main([
            '--protocol=swift',
            '--endpoint=' + self.SWIFT_CREDS['authurl'],
            '--username=' + self.SWIFT_CREDS['admin']['user'],
            '--password=' + self.SWIFT_CREDS['admin']['key'],
            '--account=AUTH_test',
            '--bucket=' + self.swift_container,
        ]))

    @swift_is_unchanged
    def test_swift_all_containers(self):
        self.assertEqual(0, s3_sync.verify.main([
            '--protocol=swift',
            '--endpoint=' + self.SWIFT_CREDS['authurl'],
            '--username=' + self.SWIFT_CREDS['dst']['user'],
            '--password=' + self.SWIFT_CREDS['dst']['key'],
            '--bucket=/*',
        ]))

    @swift_is_unchanged
    def test_swift_bad_creds(self):
        msg = ('Unexpected status code checking PUT: 401 Unauthorized')
        self.assertEqual(msg, s3_sync.verify.main([
            '--protocol=swift',
            '--endpoint=' + self.SWIFT_CREDS['authurl'],
            '--username=' + self.SWIFT_CREDS['dst']['user'],
            '--password=not-the-password',
            '--bucket=' + self.swift_container,
        ]))

    def test_swift_bad_creds_no_container(self):
        msg = 'Failed to list containers/buckets: 401 Unauthorized'
        self.assertEqual(msg, s3_sync.verify.main([
            '--protocol=swift',
            '--endpoint=' + self.SWIFT_CREDS['authurl'],
            '--username=' + self.SWIFT_CREDS['dst']['user'],
            '--password=not-the-password'
        ]))

    @swift_is_unchanged
    def test_swift_bad_container(self):
        msg = ('Unexpected status code checking PUT: 404 Not Found')
        self.assertEqual(msg, s3_sync.verify.main([
            '--protocol=swift',
            '--endpoint=' + self.SWIFT_CREDS['authurl'],
            '--username=' + self.SWIFT_CREDS['dst']['user'],
            '--password=' + self.SWIFT_CREDS['dst']['key'],
            '--bucket=does-not-exist',
        ]))

    @s3_is_unchanged
    def test_s3_no_bucket(self):
        self.assertEqual(0, s3_sync.verify.main([
            '--protocol=s3',
            '--endpoint=' + self.S3_CREDS['endpoint'],
            '--username=' + self.S3_CREDS['user'],
            '--password=' + self.S3_CREDS['key'],
        ]))

    @s3_is_unchanged
    def test_s3_single_bucket(self):
        self.assertEqual(0, s3_sync.verify.main([
            '--protocol=s3',
            '--endpoint=' + self.S3_CREDS['endpoint'],
            '--username=' + self.S3_CREDS['user'],
            '--password=' + self.S3_CREDS['key'],
            '--bucket=' + self.s3_bucket,
        ]))

    def test_s3_bad_creds_no_bucket(self):
        msg = 'Failed to list containers/buckets: 403 Forbidden'
        self.assertEqual(msg, s3_sync.verify.main([
            '--protocol=s3',
            '--endpoint=' + self.S3_CREDS['endpoint'],
            '--username=' + self.S3_CREDS['user'],
            '--password=' + 'not a real key',
        ]))

    @s3_is_unchanged
    def test_s3_bad_creds(self):
        msg = ('Unexpected status code checking PUT: 403 Forbidden')
        self.assertEqual(msg, s3_sync.verify.main([
            '--protocol=s3',
            '--endpoint=' + self.S3_CREDS['endpoint'],
            '--username=' + self.S3_CREDS['user'],
            '--password=not-the-password',
            '--bucket=' + self.s3_bucket,
        ]))

    @s3_is_unchanged
    def test_s3_bad_bucket(self):
        msg = ('Unexpected status code checking PUT: 404 Not Found')
        self.assertEqual(msg, s3_sync.verify.main([
            '--protocol=s3',
            '--endpoint=' + self.S3_CREDS['endpoint'],
            '--username=' + self.S3_CREDS['user'],
            '--password=' + self.S3_CREDS['key'],
            '--bucket=does-not-exist',
        ]))

    def test_s3_read_only(self):
        self.s3_client.put_object(
            Bucket=self.s3_bucket, Key='verify-key', Body='A' * 10)
        self.assertEqual(0, s3_sync.verify.main([
            '--protocol=s3',
            '--endpoint=' + self.S3_CREDS['endpoint'],
            '--username=' + self.S3_CREDS['user'],
            '--password=' + self.S3_CREDS['key'],
            '--bucket=' + self.s3_bucket,
            '--prefix=',
            '--read-only']))

    def test_s3_read_only_all_containers(self):
        bucket = 'a-verify-test'
        self.s3_client.create_bucket(Bucket=bucket)
        self.s3_client.put_object(
            Bucket=bucket, Key='verify-key', Body='A' * 10)
        self.assertEqual(0, s3_sync.verify.main([
            '--protocol=s3',
            '--endpoint=' + self.S3_CREDS['endpoint'],
            '--username=' + self.S3_CREDS['user'],
            '--password=' + self.S3_CREDS['key'],
            '--bucket=/*',
            '--prefix=',
            '--read-only']))
        clear_s3_bucket(self.s3_client, bucket)
        self.s3_client.delete_bucket(Bucket=bucket)

    def test_swift_read_only(self):
        self.swift_dst.put_object(self.swift_container, 'verify-key', 'A' * 10)
        self.assertEqual(0, s3_sync.verify.main([
            '--protocol=swift',
            '--endpoint=' + self.SWIFT_CREDS['authurl'],
            '--username=' + self.SWIFT_CREDS['dst']['user'],
            '--password=' + self.SWIFT_CREDS['dst']['key'],
            '--bucket=' + self.swift_container,
            '--read-only'
        ]))

    def test_swift_read_only_all_containers(self):
        container = 'a-verify-test'
        self.swift_dst.put_container(container)
        self.swift_dst.put_object(container, 'verify-key', 'A' * 10)
        self.assertEqual(0, s3_sync.verify.main([
            '--protocol=swift',
            '--endpoint=' + self.SWIFT_CREDS['authurl'],
            '--username=' + self.SWIFT_CREDS['dst']['user'],
            '--password=' + self.SWIFT_CREDS['dst']['key'],
            '--bucket=/*',
            '--read-only'
        ]))
        clear_swift_container(self.swift_dst, container)
        self.swift_dst.delete_container(container)

    def test_s3_read_only_bad_bucket(self):
        msg = 'Unexpected status code when listing objects: 404 Not Found'
        self.assertEqual(msg, s3_sync.verify.main([
            '--protocol=s3',
            '--endpoint=' + self.S3_CREDS['endpoint'],
            '--username=' + self.S3_CREDS['user'],
            '--password=' + self.S3_CREDS['key'],
            '--bucket=does-not-exist',
            '--read-only'
        ]))

    def test_s3_read_only_bad_creds(self):
        msg = ('Unexpected status code when listing objects: 403 Forbidden')
        self.assertEqual(msg, s3_sync.verify.main([
            '--protocol=s3',
            '--endpoint=' + self.S3_CREDS['endpoint'],
            '--username=' + self.S3_CREDS['user'],
            '--password=not-the-password',
            '--bucket=' + self.s3_bucket,
            '--read-only'
        ]))

    def test_swift_read_only_bad_creds(self):
        msg = ('Unexpected status code when listing objects: 401 Unauthorized')
        self.assertEqual(msg, s3_sync.verify.main([
            '--protocol=swift',
            '--endpoint=' + self.SWIFT_CREDS['authurl'],
            '--username=' + self.SWIFT_CREDS['dst']['user'],
            '--password=not-the-password',
            '--bucket=' + self.swift_container,
            '--read-only'
        ]))

    def test_swift_read_only_bad_container(self):
        msg = ('Unexpected status code when listing objects: 404 Not Found')
        self.assertEqual(msg, s3_sync.verify.main([
            '--protocol=swift',
            '--endpoint=' + self.SWIFT_CREDS['authurl'],
            '--username=' + self.SWIFT_CREDS['dst']['user'],
            '--password=' + self.SWIFT_CREDS['dst']['key'],
            '--bucket=does-not-exist',
            '--read-only'
        ]))
