# Copyright 2018 SwiftStack
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


class CloudConnectorSloFilter(object):
    """
    Currently this is just a stub NOOP filter to make `swift3` allow multipart
    upload stuff, which the main cloud-connector app will actually handle.

    :param app: The next WSGI app in the pipeline
    :param conf: The dict of configuration values from the Paste config file
    """

    def __init__(self, app, conf):
        self.app = app
        self.conf = conf

    def __call__(self, env, start_response):
        return self.app(env, start_response)


def filter_factory(global_conf, **local_conf):
    """Returns a WSGI filter app for use with paste.deploy."""
    conf = global_conf.copy()
    conf.update(local_conf)

    def slo_filter(app):
        return CloudConnectorSloFilter(app, conf)
    return slo_filter
