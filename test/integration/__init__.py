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
import psutil
from s3_sync.utils import ACCOUNT_ACL_KEY
import signal
import subprocess
from swift.common.middleware.acl import format_acl
import swiftclient
import time
import unittest
import urllib


class WaitTimedOut(RuntimeError):
    pass


def wait_for_condition(timeout, checker):
    start = time.time()
    while time.time() < start + timeout:
        ret = checker()
        if ret:
            return ret
        time.sleep(0.1)
    raise WaitTimedOut('Timeout (%s) expired' % timeout)


def kill_a_pid(a_pid, timeout=4):
    """
    Kills a PID with SIGTERM, waits a specified number of seconds, then sends a
    SIGKILL if the process hasn't exited yet.
    """
    def not_running():
        try:
            # NOTE: this is critical for migrator_running() to work
            os.waitpid(a_pid, os.WNOHANG)  # reap zombie if possible
        except Exception:
            pass
        try:
            return not psutil.Process(a_pid).is_running()
        except psutil.NoSuchProcess:
            return True

    os.kill(a_pid, signal.SIGTERM)
    try:
        wait_for_condition(timeout, not_running)
    except RuntimeError:
        os.kill(a_pid, signal.SIGKILL)
        try:
            wait_for_condition(timeout, not_running)
        except RuntimeError:
            raise Exception('Failed to kill pid %d' % a_pid)


def is_migrator_running():
    """
    Returns the PID of a running migrator, or a false value otherwise.
    """
    for proc in psutil.process_iter():
        try:
            if '/usr/local/bin/swift-s3-migrator' in proc.cmdline():
                return proc.pid
        except Exception:
            pass


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


def clear_swift_account(client):
    for cont in client.get_account()[1]:
        clear_swift_container(client, cont['name'])
        client.delete_container(cont['name'])


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
        return dict(swift=8080, s3=10080, cloud_connector=8081,
                    noshunt=8082, keystone=5000)
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
                ports['cloud_connector'] = host_port
            elif docker_port == 8082:
                ports['noshunt'] = host_port
            elif docker_port == 5000:
                ports['keystone'] = host_port
    except subprocess.CalledProcessError as e:
        print e.output
        print e.retcode
        raise
    return ports


class TestCloudSyncBase(unittest.TestCase):
    IMAGE_NAME = 'swift-s3-sync'
    PORTS = get_container_ports(IMAGE_NAME)
    SWIFT_STORAGE_BASE = 'http://localhost:%d/v1/' % PORTS['swift']
    CLOUD_CONNECTOR_STORAGE_BASE = 'http://cloud-connector:%d/v1/' % (
        PORTS['cloud_connector'],)
    NOSHUNT_STORAGE_BASE = 'http://localhost:%d/v1/' % PORTS['noshunt']

    CLOUD_SYNC_CONF = os.path.join(
        os.path.dirname(__file__),
        '../../containers/swift-s3-sync/swift-s3-sync.conf')
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
        'nuser2': {
            'user': 'nacct2:nuser2',
            'key': 'npass2',
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

    conn_by_acct = {}  # utf8_acct => Connection()
    conn_by_acct_noshunt = {}  # as above, but bypasses any shunt
    conn_by_acct_cc = {}  # as above, but through cloud-connector

    @classmethod
    def _add_conn_for_swift_acct(klass, mapping, storage_base, acct_utf8,
                                 admin_token):
        if acct_utf8 not in mapping:
            storage_url = storage_base + urllib.quote(acct_utf8)
            mapping[acct_utf8] = swiftclient.client.Connection(
                retries=0, preauthurl=storage_url, preauthtoken=admin_token)

    @classmethod
    def _add_conns_for_swift_acct(klass, acct_utf8, admin_token):
        klass._add_conn_for_swift_acct(klass.conn_by_acct,
                                       klass.SWIFT_STORAGE_BASE, acct_utf8,
                                       admin_token)
        klass._add_conn_for_swift_acct(klass.conn_by_acct_noshunt,
                                       klass.NOSHUNT_STORAGE_BASE, acct_utf8,
                                       admin_token)
        klass._add_conn_for_swift_acct(klass.conn_by_acct_cc,
                                       klass.CLOUD_CONNECTOR_STORAGE_BASE,
                                       acct_utf8, admin_token)

    @classmethod
    def conn_for_acct(klass, acct):
        acct_utf8 = acct.encode('utf8') if isinstance(acct, unicode) else acct
        return klass.conn_by_acct[acct_utf8]

    @classmethod
    def conn_for_acct_noshunt(klass, acct):
        acct_utf8 = acct.encode('utf8') if isinstance(acct, unicode) else acct
        return klass.conn_by_acct_noshunt[acct_utf8]

    @classmethod
    def conn_for_acct_cc(klass, acct):
        acct_utf8 = acct.encode('utf8') if isinstance(acct, unicode) else acct
        return klass.conn_by_acct_cc[acct_utf8]

    @classmethod
    def setUpClass(klass):
        klass.test_conf = klass._get_s3_sync_conf()

        # Get a reseller_admin token so we don't have to mess with any auth
        # silliness.
        klass.admin_conn = swiftclient.client.Connection(
            klass.SWIFT_CREDS['authurl'],
            klass.SWIFT_CREDS['admin']['user'],
            klass.SWIFT_CREDS['admin']['key'],
            retries=0)
        _, admin_token = klass.admin_conn.get_auth()

        # Get our s3 client biz
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

        url_user_key_to_acct = {}  # temporary for deduping account lookups
        for mapping in \
                klass.test_conf['containers'] + klass.test_conf['migrations']:
            # Make sure we have a connection for any Swift account on either
            # "end".  For remote ends, we have to auth once to find the account
            # name; we use url_user_key_to_acct to only do that once per input
            # tuple that could give us different answers.
            acct_utf8 = mapping['account'].encode('utf8')
            klass._add_conns_for_swift_acct(acct_utf8, admin_token)

            if mapping['protocol'] == 'swift':
                # Get conns for the other side, too
                if mapping.get('remote_account'):
                    acct_utf8 = mapping['remote_account'].encode('utf8')
                else:
                    if mapping.get('auth_type') == 'keystone_v2':
                        conn_key = (mapping['aws_endpoint'],
                                    mapping['aws_identity'],
                                    mapping['aws_secret'],
                                    mapping['tenant_name'])
                    elif mapping.get('auth_type') == 'keystone_v3':
                        conn_key = (mapping['aws_endpoint'],
                                    mapping['aws_identity'],
                                    mapping['aws_secret'],
                                    mapping['project_name'],
                                    mapping['user_domain_name'],
                                    mapping['project_domain_name'])
                    else:
                        conn_key = (mapping['aws_endpoint'],
                                    mapping['aws_identity'],
                                    mapping['aws_secret'])
                    acct_utf8 = url_user_key_to_acct.get(conn_key)
                if not acct_utf8:
                    connection_kwargs = {
                        'authurl': mapping['aws_endpoint'],
                        'user': mapping['aws_identity'],
                        'key': mapping['aws_secret'],
                        'retries': 0
                    }

                    if mapping.get('auth_type') == 'keystone_v2':
                        connection_kwargs['os_options'] = {
                            'tenant_name': mapping.get('tenant_name'),
                        }
                        connection_kwargs['auth_version'] = '2'
                    elif mapping.get('auth_type') == 'keystone_v3':
                        connection_kwargs['os_options'] = {
                            'project_name': mapping.get('project_name'),
                            'user_domain_name': mapping.get(
                                'user_domain_name'),
                            'project_domain_name':
                            mapping.get('project_domain_name')
                        }
                        connection_kwargs['auth_version'] = '3'

                    # Need to auth to get acct name, then add it to the cache
                    conn = swiftclient.client.Connection(**connection_kwargs)
                    url, _ = conn.get_auth()
                    acct_utf8 = urllib.unquote(url.rsplit('/')[-1])
                    url_user_key_to_acct[conn_key] = acct_utf8
                klass._add_conns_for_swift_acct(acct_utf8, admin_token)
                # As a convenience for ourselves, we'll stick the resolved
                # remote Swift account name in the mapping as "aws_account"
                # (stored as Unicode string for consistency)
                mapping['aws_account'] = acct_utf8.decode('utf8')

            # Now maybe auto-create some containers
            if mapping.get('aws_bucket') == '/*':
                continue
            if container['aws_bucket'].startswith('no-auto-'):
                # Remove any no-auto create containers for swift
                if mapping['protocol'] == 'swift':
                    try:
                        conn = klass.conn_for_acct_noshunt(
                            mapping['aws_account'])
                        conn.delete_container(mapping['aws_bucket'])
                    except swiftclient.exceptions.ClientException as e:
                        if e.http_status == 404:
                            continue
                        raise
            else:
                if mapping['protocol'] == 'swift' and \
                        mapping.get('aws_bucket'):
                    # For now, the aws_bucket is just a prefix, not a container
                    # name for a swift destination that has a source container
                    # of /*.  So don't create a container of that name.
                    if mapping.get('container') != '/*':
                        acct = mapping.get('remote_bucket',
                                           mapping['aws_account'])
                        conn = klass.conn_for_acct_noshunt(acct)
                        conn.put_container(mapping['aws_bucket'])
                else:
                    try:
                        klass.s3_client.create_bucket(
                            Bucket=mapping['aws_bucket'])
                    except botocore.exceptions.ClientError as e:
                        if e.response['Error']['Code'] == 409:
                            pass
            if mapping.get('container') and mapping.get('container') != '/*':
                conn = klass.conn_for_acct_noshunt(mapping['account'])
                if mapping['container'].startswith('no-auto-'):
                    try:
                        conn.delete_container(mapping['container'])
                    except swiftclient.exceptions.ClientException as e:
                        if e.http_status != 404:
                            raise
                else:
                    conn.put_container(mapping['container'])

        klass.swift_src = klass.conn_for_acct('AUTH_test')
        klass.swift_dst = klass.conn_for_acct(
            u"AUTH_\u062aacct2".encode('utf8'))
        klass.swift_nuser = klass.conn_for_acct('AUTH_nacct')
        klass.swift_nuser2 = klass.conn_for_acct('AUTH_nacct2')
        klass.swift_nuser2 = klass.conn_for_acct('AUTH_nacct2')
        # We actually test auth through this connection, so give it real creds:
        klass.cloud_connector_client = swiftclient.Connection(
            'http://cloud-connector:%d/auth/v1.0' %
            klass.PORTS['cloud_connector'],
            klass.SWIFT_CREDS['cloud-connector']['user'],
            klass.SWIFT_CREDS['cloud-connector']['key'],
            retries=0)

        # A test is going to need this Keystone user (tenant? whatever) able to
        # read/write this tempauth account.
        keystone_uuid = url_user_key_to_acct[(
            "http://1space-keystone:5000/v2.0",
            "tester", "testing", "test")].replace('KEY_', '')
        acl_dict = {'read-write': [keystone_uuid]}
        acl = format_acl(version=2, acl_dict=acl_dict)
        conn = klass.conn_for_acct(u"AUTH_\u062aacct2")
        resp_headers, body = conn.post_account({ACCOUNT_ACL_KEY: acl})

    @classmethod
    def tearDownClass(klass):
        if 'NO_TEARDOWN' in os.environ:
            return
        # We'll use the shunt-bypassing connections for tear-down just in case
        # the shunt middleware tries to do any funny business.
        for conn in klass.conn_by_acct_noshunt.values():
            clear_swift_account(conn)
        all_containers = klass.test_conf['containers'] + \
            klass.test_conf['migrations']
        for container in all_containers:
            if container.get('aws_bucket') == '/*':
                continue
            if container['protocol'] != 'swift':
                try:
                    clear_s3_bucket(klass.s3_client, container['aws_bucket'])
                except botocore.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == 'NoSuchBucket':
                        continue
                klass.s3_client.delete_bucket(
                    Bucket=container['aws_bucket'])

        all_conns = klass.conn_by_acct.values() + \
            klass.conn_by_acct_noshunt.values() + \
            klass.conn_by_acct_cc.values()
        for client in all_conns:
            client.close()

    @classmethod
    def _get_s3_sync_conf(klass):
        with open(klass.CLOUD_SYNC_CONF) as conf_handle:
            conf = json.load(conf_handle)
            return conf

    def local_swift(self, method, *args, **kwargs):
        return getattr(self.swift_src, method)(*args, **kwargs)

    def remote_swift(self, method, *args, **kwargs):
        return getattr(self.swift_dst, method)(*args, **kwargs)

    def cloud_connector(self, method, *args, **kwargs):
        return getattr(self.cloud_connector_client, method)(*args, **kwargs)

    def nuser_swift(self, method, *args, **kwargs):
        return getattr(self.swift_nuser, method)(*args, **kwargs)

    def nuser2_swift(self, method, *args, **kwargs):
        return getattr(self.swift_nuser2, method)(*args, **kwargs)

    def s3(self, method, *args, **kwargs):
        return getattr(self.s3_client, method)(*args, **kwargs)

    @contextmanager
    def migrator_running(self):
        """
        Context manager that starts the migrator and then ensures it gets
        stopped when the context is exited.
        """
        old_migrator_pid = is_migrator_running()
        if old_migrator_pid:
            kill_a_pid(old_migrator_pid)

        devnull = open('/dev/null', 'wb')
        proc = subprocess.Popen(
            ['/usr/bin/python', '/usr/local/bin/swift-s3-migrator',
             '--config',
             '/swift-s3-sync/containers/swift-s3-sync/swift-s3-sync.conf'],
            close_fds=True,
            cwd='/',
            stdout=devnull, stderr=devnull)
        try:
            yield
        finally:
            devnull.close()
            if proc.poll() is None:
                kill_a_pid(proc.pid)

    def get_swift_tree(self, conn):
        return [
            container['name']
            for container in conn.get_account()[1]
        ] + [
            container['name'] + '/' + obj['name']
            for container in conn.get_account()[1]
            for obj in conn.get_container(container['name'])[1]]

    def get_s3_tree(self):
        return [
            bucket['Name']
            for bucket in self.s3_client.list_buckets()['Buckets']
        ] + [
            bucket['Name'] + '/' + obj['Key']
            for bucket in self.s3_client.list_buckets()['Buckets']
            for obj in self.s3_client.list_objects(
                Bucket=bucket['Name']).get('Contents', [])]

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
            lambda cont: cont['protocol'] == 's3' and
            not cont['retain_local'] and
            not cont.get('selection_criteria'))

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
