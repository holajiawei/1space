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

import json
import os

from swift.common.swob import HTTPBadRequest, HTTPUnauthorized
from swift.common.utils import get_logger

from s3_sync.cloud_connector.util import (
    get_and_write_conf_file_from_s3, get_env_options, GetAndWriteFileException)


S3_PASSWD_PATH = os.path.sep + os.path.join(
    'tmp', 's3-passwd.json')
S3_IDENTITY_ENV_KEY = 'cloud-connector.auth.s3-identity'


class CloudConnectAuth(object):
    """
    :param app: The next WSGI app in the pipeline
    :param conf: The dict of configuration values from the Paste config file
    """

    def __init__(self, app, conf):
        self.app = app
        self.conf = conf
        self.logger = get_logger(conf, log_route='cloud_connect_auth')
        # TODO(darrell): add support for Swift v1 authen/authz

        # Download and load the S3 user database
        # TODO(darrell): refactor this when we add automatic
        # new-file-download-and-config-refresh logic
        unpriv_user = conf.get('user', 'swift')
        s3_passwd_obj_name = conf.get(
            's3_passwd_json', '/etc/swift-s3-sync/s3-passwd.json').lstrip('/')
        env_options = get_env_options()
        try:
            get_and_write_conf_file_from_s3(
                s3_passwd_obj_name, S3_PASSWD_PATH, env_options,
                user=unpriv_user)
        except GetAndWriteFileException as e:
            self.logger.fatal('%s; no S3 API requests will work without '
                              'the passwd DB.', e.message)
            # TODO(darrell): when we add support for Swift API requests, make
            # this non-fatal
            exit(e.message)

        try:
            with open(S3_PASSWD_PATH, 'rb') as fp:
                self.s3_users = {d['access_key'].encode('utf-8'): d
                                 for d in json.load(fp)}
        except (IOError, ValueError) as e:
            # TODO(darrell): make this non-fatal when we add Swift API requests
            # (if the config wasn't fetched--if a fetched config errors, we
            # should probably abort)
            msg = "Couldn't read s3 passwd path %r: %s; exiting" % (
                S3_PASSWD_PATH, e)
            self.logger.fatal(msg)
            exit(msg)

    def __call__(self, env, start_response):
        """
        Accepts a standard WSGI application call.

        TODO(darrell): add support for Swift v1 authen/authz
        TODO(darrell): add support for X-Auth-Token header

        Currently only supports authentication (via swift3 callback) and
        authorization for S3 API requests.
        """
        # TODO(darrell): sniff out and handle Swift v1 auth requests

        s3 = env.get('swift3.auth_details')
        # TODO(darrell): look for and do something with X-Auth-Token (with
        # fallback to X-Storage-Token)
        # TODO(darrell): maybe care about HTTP_X_SERVICE_TOKEN if we need to?
        if not s3:
            # TDOO(darrell): remove this when adding support for Swift API
            # requests
            return HTTPBadRequest(body='Only S3 API requests are supported '
                                  'at this time.')(env, start_response)

        if s3 and self.s3_ok(env, s3):
            return self.app(env, start_response)

        # Unauthorized or missing token
        return HTTPUnauthorized(headers={
            'Www-Authenticate': 'Cloud-connector realm="unknown"'})(
                env, start_response)

    def s3_ok(self, env, s3_auth_details):
        if 'check_signature' not in s3_auth_details:
            msg = 'Swift3 did not provide a check_signature function'
            self.logger.fatal(msg)
            exit(msg)

        key_id = s3_auth_details['access_key']  # XXX make sure this is UTF8
        if key_id not in self.s3_users:
            return False
        db_entry = self.s3_users[key_id]
        if not s3_auth_details['check_signature'](
                # XXX make sure swift3 wants UTF-8 encoded value here
                db_entry['secret_key'].encode('utf-8')):
            return False
        acct = db_entry['account'].encode('utf-8')
        env['PATH_INFO'] = env['PATH_INFO'].replace(key_id, acct, 1)
        self.logger.debug('key id %r authorized for acct %r: %r', key_id,
                          acct, env['PATH_INFO'])

        # Store the matching identity from the DB for the application to use
        env[S3_IDENTITY_ENV_KEY] = db_entry
        return True


def filter_factory(global_conf, **local_conf):
    """Returns a WSGI filter app for use with paste.deploy."""
    conf = global_conf.copy()
    conf.update(local_conf)

    def auth_filter(app):
        return CloudConnectAuth(app, conf)
    return auth_filter
