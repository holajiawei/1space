import copy
import json
import logging
import s3_sync.daemon_utils
import s3_sync.migrator
from swift.common.ring import Ring
from swift.common.utils import whataremyips


CONTAINER_RING = Ring('/etc/swift', ring_name='container')
MYIPS = whataremyips('0.0.0.0')


class TempMigratorStatus(object):
    def __init__(self, config):
        new_config = copy.deepcopy(config)
        self.status_all = [{'config': new_config, 'status': {}}]

    def save_migration(self, config, marker, copied, scanned, bytes_count,
                       is_reset):
        status = self.get_migration(config)
        status['marker'] = marker
        s3_sync.migrator._update_status_counts(
            status, copied, scanned, bytes_count, is_reset)

    def get_migration(self, config):
        # Currently, we only support a single migration configuration
        for stat in self.status_all:
            if s3_sync.migrator.equal_migration(stat['config'], config):
                return stat['status']
        # doesn't exist
        new_config = copy.deepcopy(config)
        self.status_all.append({'config': new_config, 'status': {}})
        return self.status_all[-1]['status']


class MigratorFactory(object):
    CONFIG = '/swift-s3-sync/containers/swift-s3-sync/swift-s3-sync.conf'
    SWIFT_DIR = '/etc/swift'

    def __init__(self, conf_path=CONFIG):
        with open(conf_path) as conf_fh:
            self.config = json.load(conf_fh)
        self.migrator_config = self.config['migrator_settings']
        self.logger = logging.getLogger(s3_sync.migrator.LOGGER_NAME)
        if not self.logger.handlers:
            s3_sync.daemon_utils.setup_logger(
                s3_sync.migrator.LOGGER_NAME, self.migrator_config)

    def get_migrator(self, migration, status):
        ic_pool = s3_sync.migrator.create_ic_pool(
            self.config, self.SWIFT_DIR, self.migrator_config['workers'] + 1)
        selector = s3_sync.migrator.Selector(MYIPS, CONTAINER_RING)
        return s3_sync.migrator.Migrator(
            migration, status, self.migrator_config['items_chunk'],
            self.migrator_config['workers'], ic_pool, self.logger,
            selector, self.migrator_config.get('segment_size', 100000000))
