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

import eventlet
eventlet.patcher.monkey_patch(all=True)

import json
import logging
import os
import os.path
from swift.common.utils import decode_timestamps, Timestamp
from swift.common.internal_client import UnexpectedResponse
import time

import container_crawler.base_sync
from .provider_factory import create_provider
from container_crawler import RetryError


class SyncContainer(container_crawler.base_sync.BaseSync):
    # There is an implicit link between the names of the json fields and the
    # object fields -- they have to be the same.
    POLICY_FIELDS = ['copy_after',
                     'retain_local',
                     'propagate_delete']

    def __init__(self, status_dir, sync_settings, max_conns=10,
                 per_account=False):
        super(SyncContainer, self).__init__(
            status_dir, sync_settings, per_account)
        self.logger = logging.getLogger('s3-sync')
        self.aws_bucket = sync_settings['aws_bucket']
        self.copy_after = int(sync_settings.get('copy_after', 0))
        self.retain_local = sync_settings.get('retain_local', True)
        self.propagate_delete = sync_settings.get('propagate_delete', True)
        self.provider = create_provider(sync_settings, max_conns,
                                        per_account=self._per_account)

    def get_last_row(self, db_id):
        if not os.path.exists(self._status_file):
            return 0
        with open(self._status_file) as f:
            try:
                status = json.load(f)
                # First iteration did not include the bucket and DB ID
                if 'last_row' in status:
                    return status['last_row']
                if db_id in status:
                    entry = status[db_id]
                    if entry['aws_bucket'] != self.aws_bucket:
                        return 0
                    # Prior to 0.1.18, policy was not included in the status
                    if 'policy' in status[db_id]:
                        for field in self.POLICY_FIELDS:
                            value = getattr(self, field)
                            if status[db_id]['policy'][field] != value:
                                return 0
                    return entry['last_row']
                return 0
            except ValueError:
                return 0

    def save_last_row(self, row, db_id):
        if not os.path.exists(self._status_account_dir):
            os.mkdir(self._status_account_dir)
        if not os.path.exists(self._status_file):
            with open(self._status_file, 'w') as f:
                json.dump({db_id: dict(last_row=row,
                                       aws_bucket=self.aws_bucket)}, f)
                return

        with open(self._status_file, 'r+') as f:
            status = json.load(f)
            # The first version did not include the DB ID and aws_bucket in the
            # status entries
            policy = {}
            for field in self.POLICY_FIELDS:
                policy[field] = getattr(self, field)
            new_status = dict(last_row=row,
                              aws_bucket=self.aws_bucket,
                              policy=policy)
            if 'last_row' in status:
                status = {db_id: new_status}
            else:
                status[db_id] = new_status
            f.seek(0)
            json.dump(status, f)
            f.truncate()

    def handle(self, row, swift_client):
        if row['deleted']:
            if self.propagate_delete:
                self.provider.delete_object(row['name'])
        else:
            # The metadata timestamp should always be the latest timestamp
            _, _, meta_ts = decode_timestamps(row['created_at'])
            if time.time() <= self.copy_after + meta_ts.timestamp:
                raise RetryError('Object is not yet eligible for archive')
            self.provider.upload_object(row['name'],
                                        row['storage_policy_index'],
                                        swift_client)

            if not self.retain_local:
                # NOTE: We rely on the DELETE object X-Timestamp header to
                # mitigate races where the object may be overwritten. We
                # increment the offset to ensure that we never remove new
                # customer data.
                self.logger.debug("Creating a new TS: %f %f" % (
                    meta_ts.offset, meta_ts.timestamp))
                delete_ts = Timestamp(meta_ts, offset=meta_ts.offset + 1)
                try:
                    swift_client.delete_object(
                        self._account, self._container, row['name'],
                        headers={'X-Timestamp': delete_ts.internal})
                except UnexpectedResponse as e:
                    if '409 Conflict' in e.message:
                        pass
