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

import os
import shutil
import swiftclient
import time

import migrator_utils

from test.integration import TestCloudSyncBase, clear_swift_container, \
    wait_for_condition, clear_swift_account


class TestNasConnector(TestCloudSyncBase):
    def setUp(self):
        self.nas_dir = '/srv/nasconnector/nas-migration-s3'

        # clean out nas connector files
        if os.path.exists(self.nas_dir):
            shutil.rmtree(self.nas_dir)

        os.makedirs(self.nas_dir)

    def tearDown(self):
        def _clear_and_maybe_delete_container(account, container):
            conn = self.conn_for_acct_noshunt(account)
            clear_swift_container(conn, container)
            clear_swift_container(conn, container + '_segments')
            if container.startswith('no-auto-'):
                try:
                    conn.delete_container(container)
                    conn.delete_container(container + '_segments')
                except swiftclient.exceptions.ClientException as e:
                    if e.http_status != 404:
                        raise

        # Make sure all migration-related containers are cleared
        for container in self.test_conf['migrations']:
            if container.get('container'):
                _clear_and_maybe_delete_container(container['account'],
                                                  container['container'])

        # Clean out all container accounts
        clear_swift_account(self.swift_nuser)
        clear_swift_account(self.swift_nuser2)

    def test_nas_connector_migration(self):
        migration = self.nas_connector_migration()
        status = migrator_utils.TempMigratorStatus(migration)
        migrator = migrator_utils.MigratorFactory().get_migrator(
            migration, status)

        test_files = [
            ('s3-blob', 's3 content'),
            ('s3-file', 'test content')]
        expected_objects = [
            ('s3-blob', 's3 content'),
            ('s3-file', 'test content')]

        conn_noshunt = self.conn_for_acct_noshunt(migration['account'])
        conn_local = self.conn_for_acct(migration['account'])

        def _check_objects_copied(conn):
            hdrs, listing = conn.get_container(migration['container'])
            swift_names = [obj['name'] for obj in listing]
            return set([obj[0] for obj in test_files]) == set(swift_names)

        for name, body in test_files:
            filename = os.path.join(self.nas_dir, name)
            with open(filename, 'w') as f:
                f.write(body)

        time.sleep(1)

        self.local_swift('put_container', 'nas-migration-s3')

        # Sanity-check (not actually migrated yet)
        self.assertFalse(_check_objects_copied(conn_noshunt))

        # But they are visible through the shunt
        self.assertTrue(_check_objects_copied(conn_local))

        migrator.next_pass()

        for name, expected_body in expected_objects:
            for conn in (conn_noshunt, conn_local):
                hdrs, body = conn.get_object(migration['container'], name)
                self.assertEqual(expected_body, body)
