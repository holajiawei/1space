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
import pwd
import sys
import urllib

from swift.common import swob, utils, wsgi
from swift.proxy.controllers.base import Controller, get_cache_key
from swift.proxy.server import Application as ProxyApplication

from s3_sync.cloud_connector.auth import S3_IDENTITY_ENV_KEY
from s3_sync.cloud_connector.util import (
    get_and_write_conf_file_from_s3, get_env_options)
from s3_sync.provider_factory import create_provider
from s3_sync.shunt import maybe_munge_profile_for_all_containers
from s3_sync.utils import (
    get_list_params, filter_hop_by_hop_headers, iter_listing, splice_listing,
    format_listing_response, SHUNT_BYPASS_HEADER, get_listing_content_type)


CLOUD_CONNECTOR_CONF_PATH = os.path.sep + os.path.join(
    'tmp', 'cloud-connector.conf')
CLOUD_CONNECTOR_SYNC_CONF_PATH = os.path.sep + os.path.join(
    'tmp', 'sync.json')
ROOT_UID = 0


def safe_profile_config(profile):
    return {
        k: v for k, v in profile.items()
        if 'secret' not in k
    }


class CloudConnectorController(Controller):
    server_type = 'cloud-connector'

    def __init__(self, app, local_to_me_profile, remote_to_me_profile, version,
                 account_name, container_name, object_name):
        super(CloudConnectorController, self).__init__(app)

        self.version = version
        self.account_name = account_name  # UTF8-encoded string
        self.container_name = container_name  # UTF8-encoded string
        self.object_name = object_name  # UTF8-encoded string
        self.local_to_me_profile, per_account = \
            maybe_munge_profile_for_all_containers(local_to_me_profile,
                                                   container_name)
        self.local_to_me_provider = create_provider(
            self.local_to_me_profile, max_conns=1, per_account=per_account,
            logger=self.app.logger)

        self.remote_to_me_profile, per_account = \
            maybe_munge_profile_for_all_containers(remote_to_me_profile,
                                                   container_name)
        self.remote_to_me_provider = create_provider(
            self.remote_to_me_profile, max_conns=1, per_account=per_account,
            logger=self.app.logger,
            extra_headers={SHUNT_BYPASS_HEADER: 'true'})

        self.aco_str = urllib.quote('/'.join(filter(None, (
            account_name, container_name, object_name))))
        self.app.logger.debug('For %s using local_to_me profile %r',
                              self.aco_str,
                              safe_profile_config(self.local_to_me_profile))
        self.app.logger.debug('For %s using remote_to_me profile %r',
                              self.aco_str,
                              safe_profile_config(self.remote_to_me_profile))

    def _cache_container_exists(self, req, account, container):
        """
        The swift3 middleware, at least, changes what it says in an object 404
        response based on whether or not swift3 thinks the actual underlying
        Swift container exists.  So we need to stick our knowledge of
        underlying Swift container existence in the cache for swift3 to find.
        The actual values are obviously made up garbage, but swift3 just looks
        for existence.
        """
        fake_info = {
            'status': 200,
            'read_acl': '',
            'write_acl': '',
            'sync_key': '',
            'object_count': '0',
            'bytes': '0',
            'versions': None,
            'storage_policy': '0',
            'cors': {
                'allow_origin': None,
                'expose_headers': None,
                'max_age': None,
            },
            'meta': {},
            'sysmeta': {},
        }
        cache_key = get_cache_key(account, container)
        infocache = req.environ.setdefault('swift.infocache', {})
        infocache[cache_key] = fake_info

    def handle_object_listing(self, req):
        # XXX(darrell): this is still uncomfortably-duplicated with
        # S3SyncShunt.handle_listing()
        resp_type = get_listing_content_type(req)
        limit, marker, prefix, delimiter, path = get_list_params(req, 1000)
        # TODO(darrell): handle "path" presence kind of like the Shunt does?
        # Figure that out when adding Swift API support.

        local_resp, local_iter = iter_listing(
            self.local_to_me_provider.list_objects, self.app.logger, marker,
            limit, prefix, delimiter)
        if local_resp.success:
            final_status = local_resp.status
            final_headers = local_resp.headers
        else:
            if local_resp.status != 404:
                self.app.logger.debug('handle_object_listing: local-to-me '
                                      'for %s got %d', self.aco_str,
                                      local_resp.status)
                headers = dict(filter_hop_by_hop_headers(
                    local_resp.headers.items()))
                return swob.Response(app_iter=iter(local_resp.body),
                                     status=local_resp.status,
                                     headers=headers, request=req)
            # This is ok because splice_listing() only iterates over the
            # local_iter--it doesn't try to call next() or anything if it's
            # empty.
            local_iter = []

        remote_resp, remote_iter = iter_listing(
            self.remote_to_me_provider.list_objects, self.app.logger, marker,
            limit, prefix, delimiter)
        if not remote_resp.success:
            if not local_resp.success:
                # Two strikes and you're OUT!
                # If we got here, we know the first "error" was 404, so we'll
                # return whatever we have, here, which will either be a 404
                # (fine) or some other error (also fine since that's more
                # interesting than any 404).
                if remote_resp.status != 404:
                    self.app.logger.debug('handle_object_listing: '
                                          'remote-to-me for %s got %d',
                                          self.aco_str, remote_resp.status)
                headers = dict(filter_hop_by_hop_headers(
                    remote_resp.headers.items()))
                return swob.Response(app_iter=iter(remote_resp.body),
                                     status=remote_resp.status,
                                     headers=headers, request=req)
            # This one does need to be an actual iterator and conform to the
            # contract of yielding (None, None) when it's "done".
            remote_iter = iter([(None, None)])
        elif not local_resp.success:
            final_status = remote_resp.status
            final_headers = remote_resp.headers
        self.app.logger.debug('handle_object_listing: final_status/headers: '
                              '%r %r', final_status, final_headers)

        spliced = splice_listing(local_iter, remote_iter, limit)
        self.app.logger.debug('handle_object_listing: spliced: %r', spliced)

        response_body = format_listing_response(spliced, resp_type,
                                                self.container_name)
        self.app.logger.debug('handle_object_listing: response_body: %r',
                              response_body)
        encoded_headers = {
            k.encode('utf8'): v.encode('utf8')
            for k, v in final_headers.items()
            if k.lower not in ('content-type', 'content-length')}

        no_hop_headers = dict(
            filter_hop_by_hop_headers(encoded_headers.items()))
        return swob.Response(body=response_body,
                             status=final_status,
                             headers=no_hop_headers, request=req,
                             content_type=resp_type)

    def GETorHEAD(self, req):
        # Note: account operations were already filtered out in
        # get_controller(), and the only allowed container operation was GET.
        #
        if not self.object_name:
            return self.handle_object_listing(req)

        # Try local-to-me first, then fall back to remote-to-me on any
        # non-success.
        provider_fn_name = 'get_object' if req.method == 'GET' else \
            'head_object'
        for provider_name in ('local_to_me_provider', 'remote_to_me_provider'):
            provider = getattr(self, provider_name)
            provider_fn = getattr(provider, provider_fn_name)
            self.app.logger.debug('Calling %s(%r) on %s', provider_fn_name,
                                  self.object_name, provider_name)
            provider_resp = provider_fn(
                self.object_name.decode('utf8'),  # boto wants Unicode?
            )
            self.app.logger.debug('Got %s for %s(%r) on %s',
                                  provider_resp.status, provider_fn_name,
                                  self.object_name, provider_name)
            if provider_resp.status // 100 == 2:
                break

        # If we know the container ("bucket" if the request we're servicing
        # came in via S3 API middleware) exists, and the response is a 404, we
        # need to note that with a fake cache entry.
        if provider_resp.status == 404:
            body = ''.join(provider_resp.body)
            provider_resp.body = [body]
            if body and 'The specified key does not exist.' in body:
                self._cache_container_exists(req, self.account_name,
                                             self.container_name)

        # Note convert_to_swift_headers() will have already been called on
        # the returned S3 headers.
        # ...but we should still call filter_hop_by_hop_headers()
        headers = dict(filter_hop_by_hop_headers(
            provider_resp.headers.items()))
        return swob.Response(app_iter=iter(provider_resp.body),
                             status=provider_resp.status,
                             headers=headers, request=req)

    @utils.public
    def GET(self, req):
        # Note: account GETs were already filtered out in get_controller()
        return self.GETorHEAD(req)

    @utils.public
    def HEAD(self, req):
        # Note: account and container HEADs were already filtered out in
        # get_controller()
        return self.GETorHEAD(req)

    @utils.public
    def PUT(self, req):
        # Note: account and controller operations were already filtered out in
        # get_controller()

        # For PUTs, cloud-connector only writes to the local-to-me object
        # store.
        self.app.logger.debug('Calling %s(%r) on %s', 'put_object',
                              self.object_name, 'local_to_me_provider')
        provider_resp = self.local_to_me_provider.put_object(
            self.object_name.decode('utf8'),  # boto wants Unicode?
            req.headers, req.body_file,
        )
        self.app.logger.debug('Got %s for %s(%r) on %s',
                              provider_resp.status, 'put_object',
                              self.object_name, 'local_to_me_provider')
        if provider_resp.status == 200:
            # S3 returns 200 but `swift3` expects what Swift returns, which is
            # 201.  So we catch the 200, turn it into 201, then swift3 will
            # like that 201 and return 200 to the actual S3 client.
            provider_resp.status = 201

        headers = dict(filter_hop_by_hop_headers(
            provider_resp.headers.items()))
        return swob.Response(app_iter=iter(provider_resp.body),
                             status=provider_resp.status,
                             headers=headers, request=req)

    @utils.public
    def POST(self, req):
        # TODO: implement this
        return swob.HTTPNotImplemented()

    @utils.public
    def DELETE(self, req):
        # TODO: implement this
        return swob.HTTPNotImplemented()

    @utils.public
    def OPTIONS(self, req):
        # TODO: implement this (or not)
        return swob.HTTPNotImplemented()


class CloudConnectorApplication(ProxyApplication):
    """
    Implements a Swift API endpoint (and eventually also a S3 API endpoint via
    swift3 middleware) to run on cloud compute nodes and
    seamlessly provide R/W access to the "cloud sync" data namespace.
    """
    # We're going to want fine-grained control over our pipeline and, for
    # example, don't want proxy code sticking the "copy" middleware back in
    # when we can't have it.  This setting disables automatic mucking with our
    # pipeline.
    modify_wsgi_pipeline = None

    def __init__(self, conf, logger=None):
        self.conf = conf

        if logger is None:
            self.logger = utils.get_logger(conf, log_route='cloud-connector')
        else:
            self.logger = logger
        self.deny_host_headers = [
            host.strip() for host in
            conf.get('deny_host_headers', '').split(',') if host.strip()]
        # We shouldn't _need_ any caching, but the proxy server will access
        # this attribute, and if it's None, it'll try to get a cache client
        # from the env.
        self.memcache = 'look but dont touch'

        # This gets called more than once on startup; first as root, then as
        # the configured user.  If root writes conf files down, then when the
        # 2nd invocation tries to write it down, it'll fail.  We solve this
        # writing it down the first time (instantiation of our stuff will fail
        # otherwise), but chowning the file to the final user if we're
        # currently euid == ROOT_UID.
        unpriv_user = conf.get('user', 'swift')
        sync_conf_obj_name = conf.get(
            'conf_file', '/etc/swift-s3-sync/sync.json').lstrip('/')
        env_options = get_env_options()
        get_and_write_conf_file_from_s3(
            sync_conf_obj_name, CLOUD_CONNECTOR_SYNC_CONF_PATH, env_options,
            user=unpriv_user)

        try:
            with open(CLOUD_CONNECTOR_SYNC_CONF_PATH, 'rb') as fp:
                self.sync_conf = json.load(fp)
        except (IOError, ValueError) as err:
            # There's no sane way we should get executed without something
            # having fetched and placed a sync config where our config is
            # telling us to look.  So if we can't find it, there's nothing
            # better to do than to fully exit the process.
            exit("Couldn't read sync_conf_path %r: %s; exiting" % (
                CLOUD_CONNECTOR_SYNC_CONF_PATH, err))

        self.sync_profiles = {}
        for cont in self.sync_conf['containers']:
            key = (cont['account'].encode('utf-8'),
                   cont['container'].encode('utf-8'))
            self.sync_profiles[key] = cont

        self.swift_baseurl = conf.get('swift_baseurl')

    def get_controller(self, req):
        # Maybe handle /info specially here, like our superclass'
        # get_controller() does?

        # Note: the only difference I can see between doing
        # "split_path(req.path, ...)" vs. req.split_path() is that req.path
        # will quote the path string with urllib.quote() prior to
        # splititng it.  Unlike our superclass' similarly-named method, we're
        # going to leave the acct/cont/obj values UTF8-encoded and unquoted.
        ver, acct, cont, obj = req.split_path(1, 4, True)
        if not obj and not cont:
            # We've decided to not support any actions on accounts...
            raise swob.HTTPForbidden(body="Account operations are not "
                                     "supported.")

        if not obj and req.method != 'GET':
            # We've decided to only support container listings (GET)
            raise swob.HTTPForbidden(body="The only supported container "
                                     "operation is GET (listing).")

        profile_key1, profile_key2 = (acct, cont), (acct, '/*')
        profile = self.sync_profiles.get(
            profile_key1, self.sync_profiles.get(profile_key2, None))
        if not profile:
            self.logger.debug('No matching sync profile for %r or %r',
                              profile_key1, profile_key2)
            raise swob.HTTPForbidden(body="No matching sync profile.")

        # Cook up a Provider "profile" here that can talk to the onprem Swift
        # cluster (the "profile" details will depend on whether or not this
        # request came into cloud-connector via S3 API or Swift API because of
        # the differences in authentication between the two APIs.
        # For now, only S3 API access is supported.
        if S3_IDENTITY_ENV_KEY in req.environ:
            # We got a S3 API request
            s3_identity = req.environ[S3_IDENTITY_ENV_KEY]
            onprem_swift_profile = {
                # Other code will expect Unicode strings in here
                "account": acct.decode('utf8'),
                "container": profile['container'],
                # All the "aws_*" stuff will be pointing to the onprem Swift
                # cluster
                "aws_bucket": profile['container'],
                "aws_endpoint": self.swift_baseurl,
                "aws_identity": s3_identity['access_key'],  # already Unicode
                "aws_secret": s3_identity['secret_key'],  # already Unicode
                "protocol": "s3",
                "custom_prefix": "",  # "native" access
            }
        else:
            raise swob.HTTPNotImplemented(
                'UNEXPECTED: only S3 access supported, but '
                'S3_IDENTITY_ENV_KEY missing??')

        d = dict(local_to_me_profile=profile,
                 remote_to_me_profile=onprem_swift_profile,
                 version=ver,
                 account_name=acct,
                 container_name=cont,
                 object_name=obj)
        return CloudConnectorController, d


def app_factory(global_conf, **local_conf):
    """paste.deploy app factory for creating WSGI proxy apps."""
    conf = global_conf.copy()
    conf.update(local_conf)

    # See comment in CloudConnectorApplication.__init__().
    # For the same reason, if we don't chown the main conf file, it can't be
    # read after we drop privileges.  NOTE: we didn't know the configured user
    # to which we will drop privileges the first time we wrote the main config
    # file, in main(), so we couldn't do this then.
    if os.geteuid() == ROOT_UID:
        unpriv_user = conf.get('user', 'swift')
        user_ent = pwd.getpwnam(unpriv_user)
        os.chown(CLOUD_CONNECTOR_CONF_PATH,
                 user_ent.pw_uid, user_ent.pw_gid)

    app = CloudConnectorApplication(conf)
    return app


def main():
    """
    cloud-connector daemon entry point.

    Loads main config file from a S3 endpoint per environment configuration and
    then starts the wsgi server.
    """

    # We need to monkeypatch out the hash validation stuff
    def _new_validate_configuration():
        try:
            utils.validate_hash_conf()
        except utils.InvalidHashPathConfigError:
            pass

    utils.validate_configuration = _new_validate_configuration
    wsgi.validate_configuration = _new_validate_configuration

    env_options = get_env_options()

    get_and_write_conf_file_from_s3(env_options['CONF_NAME'],
                                    CLOUD_CONNECTOR_CONF_PATH, env_options)
    sys.argv.insert(1, CLOUD_CONNECTOR_CONF_PATH)

    conf_file, options = utils.parse_options()
    # Calling this "proxy-server" in the pipeline is a little white lie to keep
    # the swift3 pipeline check from blowing up.
    sys.exit(wsgi.run_wsgi(conf_file, 'proxy-server', **options))
