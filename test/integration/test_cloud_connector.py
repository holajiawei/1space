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

from . import TestCloudSyncBase


class TestCloudConnector(TestCloudSyncBase):
    def test_auth(self):
        # TODO actually test S3 API auth (not easy with s3cmd, at least,
        # because there's no way to see our stub "NotImplemented" responses as
        # any kind of success.
        pass
