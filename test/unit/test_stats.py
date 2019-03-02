# -*- coding: UTF-8 -*-

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

import unittest
import mock
from s3_sync.stats import StatsReporterFactory


class TestStatsReporter(unittest.TestCase):

    @mock.patch('s3_sync.stats.pystatsd.statsd.Client')
    def setUp(self, statsd_client_mock):
        self.factory = StatsReporterFactory("host", 8125, "prefix")

    def test_statsd_client(self):
        instance = self.factory.instance('metric_prefix')

        instance.increment('metric')
        self.factory.statsd_client.increment.assert_called_once_with(
            'metric_prefix.metric', 1)

        instance.timing('timing_metric', 100)
        self.factory.statsd_client.timing.assert_called_once_with(
            'metric_prefix.timing_metric', 100)
