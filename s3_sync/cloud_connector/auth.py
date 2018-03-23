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

from __future__ import print_function

from eventlet import Timeout
from time import time
from traceback import format_exc
import urllib
from urlparse import urlsplit

from swift.common.middleware.tempauth import DEFAULT_TOKEN_LIFE
from swift.common.swob import Request
from swift.common.swob import HTTPBadRequest, HTTPNotFound, HTTPUnauthorized
from swift.common.utils import cache_from_env, get_logger, split_path

from s3_sync.cloud_connector.util import forward_raw_swift_req


class CloudConnectAuth(object):
    """
    :param app: The next WSGI app in the pipeline
    :param conf: The dict of configuration values from the Paste config file
    """

    def __init__(self, app, conf):
        self.app = app
        self.conf = conf
        self.logger = get_logger(conf, log_route='cloud_connect_auth')
        self.auth_prefix = conf.get('auth_prefix', '/auth/')
        if not self.auth_prefix or not self.auth_prefix.strip('/'):
            self.logger.warning('Rewriting invalid auth prefix "%s" to '
                                '"/auth/" (Non-empty auth prefix path '
                                'is required)' % self.auth_prefix)
            self.auth_prefix = '/auth/'
        if not self.auth_prefix.startswith('/'):
            self.auth_prefix = '/' + self.auth_prefix
        if not self.auth_prefix.endswith('/'):
            self.auth_prefix += '/'

        # Ideally something would ensure this value matches the value used by
        # whatever authentication middleware is handing out tokens in the
        # onprem Swift cluster.
        self.token_life = int(conf.get('token_life', DEFAULT_TOKEN_LIFE))

        self.swift_baseurl = conf.get('swift_baseurl')

    def __call__(self, env, start_response):
        """
        Accepts a standard WSGI application call.

        If it's an auth request, it is sent on to the onprem Swift cluster.
        Any returned auth token and swift account extracted from the returned
        storage URL are cached locally to allow rudimentary authz for
        cloud-connect requests into S3 (or whatever).  The returned storage URL
        is mutated to point to this cloud-connect server, not the Swift cluster
        to which we forwarded the auth request.

        Otherwise, the request requires an X-Auth-Token header and its value
        must be in our cache.  Furthermore, for any specific token, we only
        allow accesses for the account that was in the returned storage URL
        from the onprem Swift cluster's auth response that delivered up that
        token.
        """
        if env.get('PATH_INFO', '').startswith(self.auth_prefix):
            return self.handle_auth(env, start_response)
        # s3 = env.get('swift3.auth_details')
        # TODO(darrell): do something with what swift3 gives us?

        token = env.get('HTTP_X_AUTH_TOKEN', env.get('HTTP_X_STORAGE_TOKEN'))
        # TODO(darrell): maybe care about HTTP_X_SERVICE_TOKEN if we need to?
        if token:
            valid_acct = self.get_valid_account(env, token)
            # Make sure it matches the account in the current request
            try:
                ver, acct, cont, obj = split_path(env['PATH_INFO'], 1, 4, True)
                acct = urllib.unquote(acct)
            except ValueError:
                acct = None

            if valid_acct and valid_acct == acct:
                return self.app(env, start_response)

        # Unauthorized or missing token
        return HTTPUnauthorized(headers={
            'Www-Authenticate': 'Cloud-connector realm="unknown"'})(
                env, start_response)

    def get_valid_account(self, env, token):
        """
        Return the Swift Account name for which this token is valid, if any.

        :param env: The current WSGI environment dictionary.
        :param token: Token to validate and return a Swift Account for.
        :returns: the Swift Account extracted from the storage URL returned by
                  a prior successful authentication or None if the token is
                  invalid.
        """
        acct = None
        memcache_client = cache_from_env(env)
        if not memcache_client:
            raise Exception('Memcache required')
        memcache_token_key = 'cloud_connect_auth/token/%s' % token
        cached_auth_data = memcache_client.get(memcache_token_key)
        if cached_auth_data:
            expires, acct = cached_auth_data
            if expires < time():
                acct = None

        return acct

    def handle_auth(self, env, start_response):
        """
        WSGI entry point for auth requests (ones that match the
        self.auth_prefix).
        Wraps env in swob.Request object and passes it down.

        :param env: WSGI environment dictionary
        :param start_response: WSGI callable
        """
        try:
            req = Request(env)
            if self.auth_prefix:
                req.path_info_pop()
            if 'x-storage-token' in req.headers and \
                    'x-auth-token' not in req.headers:
                req.headers['x-auth-token'] = req.headers['x-storage-token']
            return self.handle_auth_request(req)(env, start_response)
        except (Exception, Timeout):
            print("EXCEPTION IN handle_auth: %s: %s" % (format_exc(), env))
            self.logger.increment('errors')
            start_response('500 Server Error',
                           [('Content-Type', 'text/plain')])
            return ['Internal server error.\n']

    def handle_auth_request(self, req):
        """
        Entry point for auth requests (ones that match the self.auth_prefix).
        Should return a WSGI-style callable (such as swob.Response).

        We just forward the request on down to the real onprem Swift cluster,
        and if there's a successful response with a token, we save that
        association for later authorization decisions.

        :param req: swob.Request object
        """
        req.start_time = time()  # for proxy-logging I guess?
        handler = None
        try:
            version, account, user, _junk = split_path(req.path_info,
                                                       1, 4, True)
        except ValueError:
            return HTTPNotFound(request=req)
        # TODO(darrell): handle v2 here, also?
        if version in ('v1', 'v1.0', 'auth'):
            if req.method == 'GET':
                handler = self.handle_get_token
        if not handler:
            req.response = HTTPBadRequest(request=req)
        else:
            req.response = handler(req)
        return req.response

    def handle_get_token(self, req):
        """
        Proxy the auth request back to the onprem Swift cluster and cache any
        token <-> account mapping in a successful response.

        :param req: The swob.Request to process.
        :returns: swob.Response, 2xx on success with data as returned by the
                  onprem Swift cluster.
        """
        resp = forward_raw_swift_req(self.swift_baseurl, req)

        if resp.status_int // 100 == 2:
            # Yay!
            # Cache the relationship between the returned token and storage URL
            # For now, we'll just support V1 auth responses.  But I think we
            # could, in principle, do the same thing for V2 responses.

            # Get memcache client
            memcache_client = cache_from_env(req.environ)
            if not memcache_client:
                raise Exception('Memcache required')
            storage_url = resp.headers.get('x-storage-url', None)
            if storage_url:
                token = resp.headers.get(
                    'x-auth-token', resp.headers.get('x-storage-token', None))
                if token:
                    try:
                        token_expiry = resp.headers.get('x-auth-token-expires')
                        token_life = float(token_expiry)
                    except Exception:
                        token_life = self.token_life
                    memcache_token_key = 'cloud_connect_auth/token/%s' % token
                    swift_account = urllib.unquote(
                        storage_url.rsplit('/', 1)[-1])
                    expires = time() + token_life
                    memcache_client.set(memcache_token_key,
                                        (expires, swift_account),
                                        time=float(expires - time()))

                # Rewrite storage URL to point to us
                scheme, netloc, path, query, fragment = urlsplit(storage_url)
                storage_url = req.host_url + path
                if query:
                    storage_url += '?' + query
                if fragment:
                    storage_url += '#' + fragment
                resp.headers['x-storage-url'] = storage_url

        return resp


def filter_factory(global_conf, **local_conf):
    """Returns a WSGI filter app for use with paste.deploy."""
    conf = global_conf.copy()
    conf.update(local_conf)

    def auth_filter(app):
        return CloudConnectAuth(app, conf)
    return auth_filter
