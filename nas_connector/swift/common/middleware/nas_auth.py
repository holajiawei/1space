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
import os
from swift.common.swob import HTTPBadRequest, HTTPUnauthorized
from swift.common.utils import get_logger

NAS_CONNECTOR_DEFAULT_ACCT = 'nasconnector'


class NasConnectAuth(object):
    def __init__(self, app, conf):
        self.app = app
        self.conf = conf
        self.logger = get_logger(conf, log_route='nas_connector_auth')
        self.secret = os.environ.get('NAS_CONNECT_SECRET')

    def __call__(self, env, start_response):
        self.logger.error('inside nasconnectauth')
        s3 = env.get('s3api.auth_details')
        if not s3:
            # TODO: remove this when adding support for Swift API
            # requests
            return HTTPBadRequest(body='Only S3 API requests are supported '
                                  'at this time.')(env, start_response)
        if s3 and self.s3_ok(env, s3):
            self.logger.error('all good, proceed')
            return self.app(env, start_response)

        # Unauthorized or missing token
        return HTTPUnauthorized(headers={
            'Www-Authenticate': 'Cloud-connector realm="unknown"'})(
                env, start_response)

    def s3_ok(self, env, s3_auth_details):
        self.logger.error('s3_ok')
        if 'check_signature' not in s3_auth_details:
            msg = 'Swift3 did not provide a check_signature function'
            self.logger.error(msg)
            exit(msg)
        key_id = s3_auth_details['access_key']
        if not key_id:
            return False
        if not s3_auth_details['check_signature'](
                # XXX make sure s3api wants UTF-8 encoded value here
                self.secret.encode('utf-8')):
            self.logger.error('Unable to authenticate, failed check_signature')
            return False
        self.logger.error('PATH_INFO before replace: %s' % env['PATH_INFO'])
        env['PATH_INFO'] = env['PATH_INFO'].replace(
            key_id, NAS_CONNECTOR_DEFAULT_ACCT, 1)
        self.logger.error('PATH_INFO: %s' % env['PATH_INFO'])
        return True


def filter_factory(global_conf, **local_conf):
    """Returns a WSGI filter app for use with paste.deploy."""
    conf = global_conf.copy()
    conf.update(local_conf)

    def auth_filter(app):
        return NasConnectAuth(app, conf)
    return auth_filter
