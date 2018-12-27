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

import logging
import os
import traceback

from container_crawler.crawler import Crawler
from .daemon_utils import load_swift, setup_context, initialize_loggers

from .providers.base_provider import LOGGER_NAME


def main():
    args, conf = setup_context()
    if args.log_level:
        conf['log_level'] = args.log_level
    conf['console'] = args.console

    initialize_loggers(conf)
    load_swift(LOGGER_NAME, args.once)

    from .sync_container import SyncContainerFactory
    logger = logging.getLogger(LOGGER_NAME)
    logger.debug('Starting S3Sync')

    if 'http_proxy' in conf:
        logger.debug('Using HTTP proxy %r', conf['http_proxy'])
        os.environ['http_proxy'] = conf['http_proxy']
    if 'https_proxy' in conf:
        logger.debug('Using HTTPS proxy %r', conf['https_proxy'])
        os.environ['https_proxy'] = conf['https_proxy']

    # Set a reasonable verification slack default for 1space.
    # ContainerCrawler defaults to 0, but for 1space 1 hour is a more sensible
    # trade-off to avoid doing useless work. The value is in minutes.
    if 'verification_slack' not in conf:
        conf['verification_slack'] = 60
    factory = SyncContainerFactory(conf)

    try:
        crawler = Crawler(conf, factory, logger)
        if args.once:
            crawler.run_once()
        else:
            crawler.run_always()
    except Exception as e:
        logger.error("S3Sync failed: %s" % repr(e))
        logger.error(traceback.format_exc(e))
        exit(1)


if __name__ == '__main__':
    main()
