import json
import logging
import s3_sync.daemon_utils
import s3_sync.migrator


class TempMigratorStatus(object):
    def __init__(self, config):
        self.config = config
        self.status = {}

    def save_migration(self, config, marker, copied, scanned, bytes_count,
                       is_reset):
        self.status['marker'] = marker
        s3_sync.migrator._update_status_counts(
            self.status, copied, scanned, bytes_count, is_reset)

    def get_migration(self, config):
        # Currently, we only support a single migration configuration
        if not s3_sync.migrator.equal_migration(self.config, config):
            raise NotImplementedError
        return self.status


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
        return s3_sync.migrator.Migrator(
            migration, status, self.migrator_config['items_chunk'],
            self.migrator_config['workers'], ic_pool, self.logger, 0, 1,
            self.migrator_config.get('segment_size', 100000000))
