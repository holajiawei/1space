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

import argparse
from distutils.version import LooseVersion
import json
import logging
import logging.handlers
import os
import sys
import time
import socket

from .providers.base_provider import LOGGER_NAME

MAX_LOG_SIZE = 100 * 1024 * 1024
MIN_SWIFT_VERSION = LooseVersion('2.13')


def initialize_loggers(config):
    setup_logger(LOGGER_NAME, config)
    setup_logger('boto3', config)
    setup_logger('botocore', config)

    if config.get('log_level') == 'debug':
        config_copy = dict(config)
        config_copy['log_level'] = 'info'
        setup_logger('botocore.hooks', config_copy)
        setup_logger('botocore.session', config_copy)
        setup_logger('botocore.loaders', config_copy)
        setup_logger('botocore.client', config_copy)


def setup_logger(logger_name, config):
    level = config.get('log_level', 'info').upper()
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    formatter = logging.Formatter(
        '[%(asctime)s] %(name)s [%(levelname)s]: %(message)s')
    if config.get('console'):
        handler = logging.StreamHandler()
    elif config.get('log_file'):
        handler = logging.handlers.RotatingFileHandler(config['log_file'],
                                                       maxBytes=MAX_LOG_SIZE,
                                                       backupCount=5)
    else:
        raise RuntimeError('log file must be set')

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    if config.get('syslog'):
        syslog_config = config['syslog']

        host = syslog_config.get('host', 'localhost')
        port = syslog_config.get('port', 514)
        proto = syslog_config.get('proto', 'udp')

        if proto == 'udp':
            socktype = socket.SOCK_DGRAM
        else:
            socktype = socket.SOCK_STREAM

        syslog_handler = logging.handlers.SysLogHandler(
            address=(host, port),
            socktype=socktype
        )

        syslog_handler.setFormatter(formatter)
        logger.addHandler(syslog_handler)
        logger.debug("Using syslog")


def load_swift(logger_name, once=False):
    logger = logging.getLogger(logger_name)

    while True:
        try:
            import swift  # NOQA
            if LooseVersion(swift.__version__) < MIN_SWIFT_VERSION:
                raise RuntimeError(
                    'Swift version must be at least %s' % MIN_SWIFT_VERSION)
            break
        except ImportError as e:
            if once:
                raise e
            else:
                logger.warning('Failed to load swift: %s' % str(e))
                time.sleep(5)


def load_config(conf_file):
    with open(conf_file, 'r') as f:
        return json.load(f)


def parse_args(args, description):
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('--config', metavar='conf', type=str, required=True,
                        help='path to the configuration file')
    parser.add_argument('--once', action='store_true',
                        help='run once')
    parser.add_argument('--log-level', metavar='level', type=str,
                        choices=['debug', 'info', 'warning', 'error'],
                        help='logging level; defaults to info')
    parser.add_argument('--console', action='store_true',
                        help='log messages to console')
    return parser.parse_args(args)


def setup_context(description='Swift-S3 synchronization daemon'):
    args = parse_args(sys.argv[1:], description)
    if not os.path.exists(args.config):
        print 'Configuration file does not exist'
        exit(0)

    conf = load_config(args.config)
    return args, conf
