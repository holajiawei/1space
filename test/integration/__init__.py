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

import boto3
import botocore.exceptions
from contextlib import contextmanager
import hashlib
import json
import os
import subprocess
import swiftclient
import time
import unittest
import urllib


def clear_swift_container(client, container):
    try:
        _, list_results = client.get_container(container)
    except swiftclient.exceptions.ClientException as e:
        if e.http_status == 404:
            return
        raise
    for obj in list_results:
        try:
            client.delete_object(container, obj['name'])
        except swiftclient.exceptions.ClientException as e:
            if e.http_status == 404:
                continue
            raise


def clear_s3_bucket(client, bucket):
    list_results = client.list_objects(Bucket=bucket)
    for obj in list_results.get('Contents', []):
        client.delete_object(Bucket=bucket, Key=obj['Key'])


def wait_for_condition(timeout, checker):
    start = time.time()
    while time.time() < start + timeout:
        ret = checker()
        if ret:
            return ret
        time.sleep(0.1)
    raise RuntimeError('Timeout expired')


def s3_prefix(account, container, key):
    md5_prefix = hashlib.md5('%s/%s' % (account, container))
    return hex(long(md5_prefix.hexdigest(), 16) % 16 ** 6)[2:-1]


def s3_key_name(mapping, key):
    prefix = s3_prefix(
        mapping['account'],
        mapping['container'],
        key)
    return '%s/%s/%s/%s' % (
        prefix, mapping['account'], mapping['container'], key)


def swift_content_location(mapping):
    return '%s;%s;%s' % (mapping['aws_endpoint'],
                         mapping['aws_identity'],
                         mapping['aws_bucket'])


def get_container_ports(image_name):
    if 'DOCKER' in os.environ:
        return dict(swift=8080, s3=10080, cloud_connector=8081)
    if 'TEST_CONTAINER' in os.environ:
        container = os.environ['TEST_CONTAINER']
    else:
        cmd = 'docker ps -f ancestor=%s -f status=running '\
              '--format "{{.Names}}"' % image_name
        images = subprocess.check_output(cmd.split())
        if not images:
            raise RuntimeError('Cannot find container from image %s' %
                               image_name)
        container = images.split()[0][1:-1]

    cmd = 'docker port %s' % container
    try:
        ports = {}
        for line in subprocess.check_output(cmd.split()).split('\n'):
            if not line.strip():
                continue
            docker, host = line.split(' -> ')
            docker_port = int(docker.split('/')[0])
            host_port = int(host.split(':')[1])
            if docker_port == 8080:
                ports['swift'] = host_port
            elif docker_port == 10080:
                ports['s3'] = host_port
            elif docker_port == 8001:
                ports['cloud_connector'] = host_port
    except subprocess.CalledProcessError as e:
        print e.output
        print e.retcode
        raise
    return ports


class TestCloudSyncBase(unittest.TestCase):
    IMAGE_NAME = 'swift-s3-sync'
    PORTS = get_container_ports(IMAGE_NAME)

    CLOUD_SYNC_CONF = os.path.join(
        os.path.dirname(__file__), '../container/swift-s3-sync.conf')
    SWIFT_CREDS = {
        'authurl': 'http://localhost:%d/auth/v1.0' % PORTS['swift'],
        'src': {
            'user': 'test:tester',
            'key': 'testing',
        },
        'nuser': {
            'user': 'nacct:nuser',
            'key': 'npass',
        },
        'dst': {
            'user': u"\u062aacct2:\u062auser2".encode('utf8'),
            'key': u"\u062apass2".encode('utf8'),
        },
        'admin': {
            'user': 'admin:admin',
            'key': 'admin',
        },
        'cloud-connector': {
            'user': u"\u062aacct:\u062auser".encode('utf8'),
            'key': u"\u062apass".encode('utf8'),
        },
    }
    S3_CREDS = {}

    @classmethod
    def setUpClass(klass):
        klass.test_conf = klass._get_s3_sync_conf()
        # It can be handy to be the reseller admin (see admin_conn_for context
        # manager)
        klass.admin_conn = swiftclient.client.Connection(
            klass.SWIFT_CREDS['authurl'],
            klass.SWIFT_CREDS['admin']['user'],
            klass.SWIFT_CREDS['admin']['key'],
            retries=0)
        klass.swift_src = swiftclient.client.Connection(
            klass.SWIFT_CREDS['authurl'],
            klass.SWIFT_CREDS['src']['user'],
            klass.SWIFT_CREDS['src']['key'],
            retries=0)
        klass.swift_dst = swiftclient.client.Connection(
            klass.SWIFT_CREDS['authurl'],
            klass.SWIFT_CREDS['dst']['user'],
            klass.SWIFT_CREDS['dst']['key'],
            retries=0)
        klass.cloud_connector_client = swiftclient.Connection(
            'http://localhost:%d/auth/v1.0' % klass.PORTS['cloud_connector'],
            klass.SWIFT_CREDS['cloud-connector']['user'],
            klass.SWIFT_CREDS['cloud-connector']['key'],
            retries=0)
        s3 = [container for container in klass.test_conf['containers']
              if container.get('protocol', 's3') == 's3'][0]
        klass.S3_CREDS.update({
            'endpoint': 'http://localhost:%d' % klass.PORTS['s3'],
            'user': s3['aws_identity'],
            'key': s3['aws_secret'],
        })
        session = boto3.session.Session(
            aws_access_key_id=s3['aws_identity'],
            aws_secret_access_key=s3['aws_secret'])
        conf = boto3.session.Config(s3={'addressing_style': 'path'})
        klass.s3_client = session.client(
            's3', config=conf,
            endpoint_url='http://localhost:%d' % klass.PORTS['s3'])

        for container in \
                klass.test_conf['containers'] + klass.test_conf['migrations']:
            if container['container'].startswith('no-auto-'):
                continue
            if container['protocol'] == 'swift':
                klass.swift_dst.put_container(container['aws_bucket'])
            else:
                try:
                    klass.s3_client.create_bucket(
                        Bucket=container['aws_bucket'])
                except botocore.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == 409:
                        pass
            with klass.admin_conn_for(container['account']) as conn:
                conn.put_container(container['container'])

    @classmethod
    def tearDownClass(klass):
        if 'NO_TEARDOWN' in os.environ:
            return
        all_containers = klass.test_conf['containers'] + \
            klass.test_conf['migrations']
        for container in all_containers:
            if container['protocol'] == 'swift':
                klass._remove_swift_container(klass.swift_dst,
                                              container['aws_bucket'])
            else:
                try:
                    clear_s3_bucket(klass.s3_client, container['aws_bucket'])
                except botocore.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == 'NoSuchBucket':
                        continue
                klass.s3_client.delete_bucket(
                    Bucket=container['aws_bucket'])

        for container in all_containers:
            with klass.admin_conn_for(container['account']) as conn:
                klass._remove_swift_container(conn, container['container'])

        for client in [klass.swift_src, klass.swift_dst]:
            if client:
                client.close()

    @classmethod
    def _get_s3_sync_conf(klass):
        with open(klass.CLOUD_SYNC_CONF) as conf_handle:
            conf = json.load(conf_handle)
            return conf

    @staticmethod
    def _remove_swift_container(client, container):
        clear_swift_container(client, container)
        clear_swift_container(client, container + '_segments')
        try:
            client.delete_container(container)
        except swiftclient.exceptions.ClientException as e:
            if e.http_status == 404:
                pass
        try:
            client.delete_container(container + '_segments')
        except swiftclient.exceptions.ClientException as e:
            if e.http_status == 404:
                pass

    def local_swift(self, method, *args, **kwargs):
        return getattr(self.swift_src, method)(*args, **kwargs)

    def remote_swift(self, method, *args, **kwargs):
        return getattr(self.swift_dst, method)(*args, **kwargs)

    def cloud_connector(self, method, *args, **kwargs):
        return getattr(self.cloud_connector_client, method)(*args, **kwargs)

    def nuser_swift(self, method, *args, **kwargs):
        return getattr(self.swift_nuser, method)(*args, **kwargs)

    def s3(self, method, *args, **kwargs):
        return getattr(self.s3_client, method)(*args, **kwargs)

    @classmethod
    @contextmanager
    def admin_conn_for(klass, swift_account):
        """
        Takes a Swift account in the "main" (onprem) Swift cluster as a
        UTF8-encoded string or Unicode string and returns a
        swiftclient.client.Connection instance with a reseller_admin token
        pointed at that account.
        """
        if not klass.admin_conn.url:
            klass.admin_conn.get_auth()
        old_url = klass.admin_conn.url
        old_conn = klass.admin_conn.http_conn
        try:
            if isinstance(swift_account, unicode):
                swift_account = swift_account.encode('utf8')
            klass.admin_conn.url = old_url.rsplit('/', 1)[0] + '/' + \
                urllib.quote(swift_account)
            yield klass.admin_conn
        finally:
            klass.admin_conn.url = old_url
            klass.admin_conn.http_conn = old_conn

    @classmethod
    def _find_mapping(klass, matcher):
        for mapping in klass.test_conf['containers']:
            if matcher(mapping):
                return mapping
        raise RuntimeError('No matching mapping')

    @classmethod
    def s3_sync_cc_mapping(klass):
        return klass._find_mapping(
            lambda cont: (cont['protocol'] == 's3' and cont['retain_local'] and
                          'cl-conn' in cont['container']))

    @classmethod
    def s3_sync_mapping(klass):
        return klass._find_mapping(
            lambda cont: cont['protocol'] == 's3' and cont['retain_local'])

    @classmethod
    def s3_archive_mapping(klass):
        return klass._find_mapping(
            lambda cont: cont['protocol'] == 's3' and not cont['retain_local'])

    @classmethod
    def s3_restore_mapping(klass):
        return klass._find_mapping(
            lambda cont:
                cont['protocol'] == 's3' and cont.get('restore_object', False))

    @classmethod
    def swift_restore_mapping(klass):
        return klass._find_mapping(
            lambda cont:
                cont['protocol'] == 'swift' and
                cont.get('restore_object', False))

    @classmethod
    def swift_sync_mapping(klass):
        return klass._find_mapping(
            lambda cont: cont['protocol'] == 'swift' and cont['retain_local'])

    @classmethod
    def swift_archive_mapping(klass):
        return klass._find_mapping(
            lambda cont: cont['protocol'] == 'swift' and
            not cont['retain_local'])

    @classmethod
    def _find_migration(klass, matcher):
        for migration in klass.test_conf['migrations']:
            if matcher(migration):
                return migration
        raise RuntimeError('No matching migration')

    @classmethod
    def s3_migration(klass):
        return klass._find_migration(lambda cont: cont['protocol'] == 's3')

    @classmethod
    def swift_migration(klass):
        return klass._find_migration(lambda cont: cont['protocol'] == 'swift')
