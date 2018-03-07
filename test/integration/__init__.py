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
import hashlib
import json
import os
import subprocess
import swiftclient
import time
import unittest


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
        return dict(swift=8080, s3=10080, proxymc=8081)
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
            elif docker_port == 8081:
                ports['proxymc'] = host_port
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
        'dst': {
            'user': 'test2:tester2',
            'key': 'testing2'
        },
        'admin': {
            'user': 'admin:admin',
            'key': 'admin',
        },
    }
    S3_CREDS = {}

    @classmethod
    def setUpClass(self):
        self.test_conf = self._get_s3_sync_conf()
        self.swift_src = swiftclient.client.Connection(
            self.SWIFT_CREDS['authurl'],
            self.SWIFT_CREDS['src']['user'],
            self.SWIFT_CREDS['src']['key'])
        self.swift_dst = swiftclient.client.Connection(
            self.SWIFT_CREDS['authurl'],
            self.SWIFT_CREDS['dst']['user'],
            self.SWIFT_CREDS['dst']['key'])
        self.proxymc_client = swiftclient.client.Connection(
            self.SWIFT_CREDS['authurl'],
            self.SWIFT_CREDS['src']['user'],
            self.SWIFT_CREDS['src']['key'],
            preauthurl='http://localhost:%d/v1/AUTH_test' % (
                self.PORTS['proxymc'],),
        )
        s3 = [container for container in self.test_conf['containers']
              if container.get('protocol', 's3') == 's3'][0]
        self.S3_CREDS.update({
            'endpoint': 'http://localhost:%d' % self.PORTS['s3'],
            'user': s3['aws_identity'],
            'key': s3['aws_secret'],
        })
        session = boto3.session.Session(
            aws_access_key_id=s3['aws_identity'],
            aws_secret_access_key=s3['aws_secret'])
        conf = boto3.session.Config(s3={'addressing_style': 'path'})
        self.s3_client = session.client(
            's3', config=conf,
            endpoint_url='http://localhost:%d' % self.PORTS['s3'])

        for container in \
                self.test_conf['containers'] + self.test_conf['migrations']:
            if container['container'].startswith('no-auto-'):
                continue
            if container['protocol'] == 'swift':
                self.swift_dst.put_container(container['aws_bucket'])
            else:
                try:
                    self.s3_client.create_bucket(
                        Bucket=container['aws_bucket'])
                except botocore.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == 409:
                        pass
            self.swift_src.put_container(container['container'])

    @classmethod
    def tearDownClass(self):
        if 'NO_TEARDOWN' in os.environ:
            return
        all_containers = self.test_conf['containers'] + \
            self.test_conf['migrations']
        for container in all_containers:
            if container['protocol'] == 'swift':
                self._remove_swift_container(
                    self.swift_dst, container['aws_bucket'])
            else:
                try:
                    clear_s3_bucket(self.s3_client, container['aws_bucket'])
                except botocore.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == 'NoSuchBucket':
                        continue
                self.s3_client.delete_bucket(Bucket=container['aws_bucket'])

        for container in all_containers:
            self._remove_swift_container(
                self.swift_src, container['container'])

        for client in [self.swift_src, self.swift_dst]:
            if client:
                client.close()

    @classmethod
    def _get_s3_sync_conf(self):
        with open(self.CLOUD_SYNC_CONF) as conf_handle:
            conf = json.load(conf_handle)
            return conf

    @staticmethod
    def _remove_swift_container(client, container):
        clear_swift_container(client, container)
        try:
            client.delete_container(container)
        except swiftclient.exceptions.ClientException as e:
            if e.http_status == 404:
                return

    def local_swift(self, method, *args, **kwargs):
        return getattr(self.swift_src, method)(*args, **kwargs)

    def proxymc(self, method, *args, **kwargs):
        return getattr(self.proxymc_client, method)(*args, **kwargs)

    def remote_swift(self, method, *args, **kwargs):
        return getattr(self.swift_dst, method)(*args, **kwargs)

    def s3(self, method, *args, **kwargs):
        return getattr(self.s3_client, method)(*args, **kwargs)
