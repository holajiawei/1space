# Copyright 2017 SwiftStack
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import eventlet
eventlet.patcher.monkey_patch(all=True)

import container_crawler.base_sync
from container_crawler.exceptions import RetryError

import json
import logging
import md5
import os
import os.path
import pystatsd.statsd
from swift.common.utils import decode_timestamps, Timestamp
from swift.common.internal_client import UnexpectedResponse
import time
import traceback
import urllib

from .base_sync import BaseSync
from .provider_factory import create_provider

from .base_sync import LOGGER_NAME


def hash_dict(data):
    # Only used for metadata, so keys and values are strings
    return md5.md5(str(sorted(data.items()))).hexdigest()


class SyncContainer(container_crawler.base_sync.BaseSync):
    # There is an implicit link between the names of the json fields and the
    # object fields -- they have to be the same.
    POLICY_FIELDS = ['copy_after',
                     'retain_local',
                     'propagate_delete']

    PROCESSED_ROW_KEY = 'last_row'
    VERIFIED_ROW_KEY = 'last_verified_row'
    METADATA_HASH_KEY = 'metadata_hash'

    def __init__(self, status_dir, sync_settings, max_conns=10,
                 per_account=False, statsd_client=None):
        super(SyncContainer, self).__init__(
            status_dir, sync_settings, per_account)
        self.logger = logging.getLogger(LOGGER_NAME)
        self.aws_bucket = sync_settings['aws_bucket']
        self.copy_after = int(sync_settings.get('copy_after', 0))
        self.retain_local = sync_settings.get('retain_local', True)
        self.propagate_delete = sync_settings.get('propagate_delete', True)
        self._settings = sync_settings
        self.provider = create_provider(sync_settings, max_conns,
                                        per_account=self._per_account)
        self.statsd_client = statsd_client

        statsd_prefix_parts = [
            self._settings.get('aws_endpoint', 'S3'),
            '/'.join(filter(
                None, [self.aws_bucket, self._settings.get('custom_prefix')])),
            self._settings['account'],
            self._settings['container']
        ]
        self.statsd_prefix = '.'.join(
            [urllib.quote(part.encode('utf-8'), safe='').replace('.', '%2E')
             for part in statsd_prefix_parts])

    def _get_status_row(self, row_field, db_id):
        if not os.path.exists(self._status_file):
            return 0
        with open(self._status_file) as f:
            try:
                status = json.load(f)
                # First iteration did not include the bucket and DB ID
                if row_field == self.PROCESSED_ROW_KEY and row_field in status:
                    return status[row_field]
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
                    try:
                        return entry[row_field]
                    except KeyError:
                        # Happens for the new last_verified_row field.
                        if row_field == self.VERIFIED_ROW_KEY:
                            return entry.get(self.PROCESSED_ROW_KEY, 0)
                        return 0
                return 0
            except ValueError:
                return 0

    def _save_status_row(self, row, row_field, db_id):
        policy = {}
        for field in self.POLICY_FIELDS:
            policy[field] = getattr(self, field)

        if not os.path.exists(self._status_account_dir):
            os.mkdir(self._status_account_dir)
        if not os.path.exists(self._status_file):
            with open(self._status_file, 'w') as f:
                new_entry = {self.PROCESSED_ROW_KEY: 0,
                             self.VERIFIED_ROW_KEY: 0,
                             'aws_bucket': self.aws_bucket,
                             'policy': policy}
                new_entry[row_field] = row
                json.dump({db_id: new_entry}, f)
                return

        with open(self._status_file, 'r+') as f:
            status = json.load(f)
            old_entry = status.get(db_id, {})
            new_entry = {'aws_bucket': self.aws_bucket,
                         'policy': policy}
            # The first version did not include the DB ID and aws_bucket in the
            # status entries
            if self.PROCESSED_ROW_KEY in status:
                new_entry[self.PROCESSED_ROW_KEY] =\
                    status[self.PROCESSED_ROW_KEY]
            new_entry[self.PROCESSED_ROW_KEY] = old_entry.get(
                self.PROCESSED_ROW_KEY, 0)
            new_entry[self.VERIFIED_ROW_KEY] = old_entry.get(
                self.VERIFIED_ROW_KEY, new_entry[self.PROCESSED_ROW_KEY])
            new_entry[row_field] = row
            status[db_id] = new_entry

            f.seek(0)
            json.dump(status, f)
            f.truncate()

    def get_last_processed_row(self, db_id):
        return self._get_status_row(self.PROCESSED_ROW_KEY, db_id)

    def get_last_verified_row(self, db_id):
        return self._get_status_row(self.VERIFIED_ROW_KEY, db_id)

    def save_last_processed_row(self, row, db_id):
        self._save_status_row(row, self.PROCESSED_ROW_KEY, db_id)

    def save_last_verified_row(self, row, db_id):
        return self._save_status_row(row, self.VERIFIED_ROW_KEY, db_id)

    def statsd_increment(self, metric, value):
        if not self.statsd_client:
            return
        self.statsd_client.update_stats(
            '.'.join([self.statsd_prefix, metric]), value)

    def _get_metadata_hash(self, db_id):
        res = self._get_status_row(self.METADATA_HASH_KEY, db_id)
        # if the row doesn't exist or if there is a KeyError it returns 0
        # signifying that there is no metadata hash stored for this db_id
        # which for most conceivable purposes is the same as if the hash has
        # changed.
        if res == 0:
            return None
        return res

    def handle_container_metadata(self, metadata_dict, db_id):
        relevant_metadata = self.provider.select_container_metadata(
            metadata_dict)
        metadata_hash = hash_dict(relevant_metadata)
        last_hash = self._get_metadata_hash(db_id)
        if last_hash == metadata_hash:
            return
        post_resp = self.provider.post_container(relevant_metadata)
        if post_resp.success:
            self._save_status_row(metadata_hash, self.METADATA_HASH_KEY, db_id)
            self.logger.debug(
                'Container metadata updated for %s' % (self.aws_bucket,))
        else:
            self.logger.error('Failed to update container metadata -- %s: %s' %
                              (post_resp.wsgi_status, post_resp.exc_info[1]))
            self.logger.debug(
                ''.join(traceback.format_tb(post_resp.exc_info[2])))

    def handle(self, row, swift_client):
        if row['deleted']:
            if self.propagate_delete:
                self.provider.delete_object(row['name'])
                self.statsd_increment('deleted_objects', 1)
        else:
            # The metadata timestamp should always be the latest timestamp
            _, _, meta_ts = decode_timestamps(row['created_at'])
            if time.time() <= self.copy_after + meta_ts.timestamp:
                raise RetryError('Object is not yet eligible for archive')
            status = self.provider.upload_object(row, swift_client)
            if status == BaseSync.UploadStatus.PUT:
                self.statsd_increment('copied_objects', 1)

            uploaded_statuses = [
                BaseSync.UploadStatus.PUT,
                BaseSync.UploadStatus.POST,
                # NOOP means the object already exists
                BaseSync.UploadStatus.NOOP]
            if not self.retain_local and status in uploaded_statuses:
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


class SyncContainerFactory(object):
    def __init__(self, config, handler_class=SyncContainer):
        if 'status_dir' not in config:
            raise RuntimeError('Configuration option "status_dir" is missing')
        self.config = config
        self._handler_class = handler_class

    def __str__(self):
        return 'SyncContainer'

    def instance(self, settings, per_account=False):
        if 'statsd_host' in self.config:
            statsd_client = pystatsd.statsd.Client(
                self.config['statsd_host'],
                self.config.get('statsd_port', 8125),
                self.config.get('statsd_prefix'))
        else:
            statsd_client = None

        return self._handler_class(
            self.config['status_dir'],
            settings,
            per_account=per_account,
            statsd_client=statsd_client)
