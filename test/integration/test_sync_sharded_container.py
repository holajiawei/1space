"""
Copyright 2019 SwiftStack

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
import hashlib
import os
from uuid import uuid4

from swift.cli.manage_shard_ranges import find_replace_shard_ranges
from swift.common import utils as swift_utils
from swift.common.manager import Manager
from swift.container.backend import ContainerBroker
from swiftclient import ClientException
from . import TestCloudSyncBase, CONTAINER_RING, clear_swift_container

import utils


class TestSyncSharded(TestCloudSyncBase):

    @classmethod
    def setUpClass(klass):
        super(TestSyncSharded, klass).setUpClass()
        klass.sharders = Manager(['container-sharder'])
        klass.updaters = Manager(['container-updater'])
        klass.initial_object_count = 30
        klass.additional_object_count = 10

    @classmethod
    def tearDownClass(klass):
        super(TestSyncSharded, klass).tearDownClass()
        Manager(['container-sharder', 'container-updater']).kill()

    def _get_storage_dir(self, part, node, account=None, container=None):
        # TODO: fix hard-coded path
        datadir = os.path.join('/srv/1/node/sdb1', 'containers')
        container_hash = swift_utils.hash_path(account, container)
        return (swift_utils.storage_directory(datadir, part, container_hash),
                container_hash)

    def _get_broker(self, part, node, account=None, container=None):
        container_dir, container_hash = self._get_storage_dir(
            part, node, account=account, container=container)
        db_file = os.path.join(container_dir, container_hash + '.db')
        return ContainerBroker(db_file)

    def assert_container_metadata(self, conn, cont,
                                  expected_obj_count, expected_user_meta=None):
        headers = conn.head_container(cont)
        self.assertIn('x-container-object-count', headers)
        self.assertEqual(str(expected_obj_count),
                         headers['x-container-object-count'])
        if expected_user_meta:
            for key in expected_user_meta.keys():
                self.assertIn(key, headers)
                self.assertEqual(str(expected_user_meta[key]),
                                 headers[key])

    def assert_container_delete_fails(self, conn, cont):
        with self.assertRaises(ClientException) as cm:
            conn.delete_container(cont)
        self.assertEqual(409, cm.exception.http_status)

    def _shard_container(self, object_count):
        class ShardArgs(object):
            def __init__(self, object_count):
                self.rows_per_shard = int(object_count / 3)
                self.shards_account_prefix = '.shards_'
                self.enable = True
                self.verbose = False
                self.replace_timeout = 600
                self.force = True
                self.enable_timeout = 300

        part, nodes = CONTAINER_RING.get_nodes(self.acct,
                                               self.cont)
        broker = self._get_broker(
            part, nodes[0], self.acct, self.cont)

        args = ShardArgs(object_count)
        ret = find_replace_shard_ranges(broker, args)
        if ret != 0:
            self.fail('failed to create shard ranges')

        # 1space will not synchronize objects when sharding
        # has started, but objects are still in root container
        # and not yet in its own shard db. The objects that
        # already made to its shard are synced.
        self.sharders.once()  # create 1st and 2nd shard
        self.sharders.once()  # create 3rd shard
        self.updaters.once()  # create .shards_ account db

    def _upload_objects(self, conn, start_range, end_range):
        for i in range(start_range, end_range):
            obj_uuid = uuid4().hex
            obj_name = 'obj_%s_%s' % (i, obj_uuid)
            body = 'obj body %s' % obj_uuid
            expected_etag = hashlib.md5(body).hexdigest()
            self.test_args.append((obj_name, body, expected_etag))
            etag = conn.put_object(self.cont, obj_name, body)
            # sanity test - check PUT success
            self.assertEquals(expected_etag, etag)

    def _verify_target(self, expected_user_meta=None):
        target_conn = self.conn_for_acct_noshunt(self.mapping['aws_account'])
        self.assert_container_metadata(
            target_conn, self.mapping['aws_bucket'], len(self.test_args),
            expected_user_meta)
        for obj, body, etag in self.test_args:
            headers, target_body = target_conn.get_object(
                self.mapping['aws_bucket'], obj)
            self.assertEqual(etag, headers['etag'])
            self.assertEqual(body, target_body)

    def setUp(self):
        super(TestSyncSharded, self).setUp()
        self.mapping = self._find_mapping(
            lambda m: m['aws_bucket'] == 'sharded-target')

        self.acct = self.mapping['account']
        self.cont = '%s_%s' % (self.mapping['container'], uuid4().hex)
        self.mapping['container'] = self.cont

        conn = self.conn_for_acct(self.acct)
        conn.put_container(self.cont)
        self.test_args = []
        self._upload_objects(conn, 0, self.initial_object_count)

        # sanity test
        self.assert_container_metadata(conn, self.cont,
                                       self.initial_object_count)

    def tearDown(self):
        conn = self.conn_for_acct(self.acct)
        clear_swift_container(self.swift_src, self.cont)
        self.assert_container_delete_fails(conn, self.cont)

        # after deleting objects, run sharder to shrink back to root
        # then delete
        self.sharders.once()
        self.assert_container_metadata(conn, self.cont, 0)

        conn.delete_container(self. cont)

    def test_shard_after_initial_sync(self):
        '''
        Test syncing before sharding, then sync again after sharding has
        started and new objects have been added to container
        '''
        # sync to target before sharding
        crawler = utils.get_container_crawler(self.mapping)
        crawler.run_once()
        self._verify_target()

        # shard container
        self._shard_container(self.initial_object_count)

        # add more objects
        conn = self.conn_for_acct(self.acct)
        self._upload_objects(
            conn, self.initial_object_count,
            self.initial_object_count + self.additional_object_count)

        # add container metadata
        cont_hdr = {'x-container-meta-test-sharder': 'doit'}
        conn.post_container(self.cont, cont_hdr)

        # sanity test
        self.sharders.once()  # update container count
        self.assert_container_metadata(
            conn, self.cont,
            self.initial_object_count + self.additional_object_count,
            cont_hdr)

        # sync to target again
        crawler.run_once()
        self._verify_target(cont_hdr)

        # test that statsd server got the correct results for root container
        self._assert_stats(
            self.mapping,
            {'copied_objects': len(self.test_args),
             'bytes': len(self.test_args) * (len('obj body ') + 32)})
        clear_swift_container(self.swift_dst, self.mapping['aws_bucket'])
