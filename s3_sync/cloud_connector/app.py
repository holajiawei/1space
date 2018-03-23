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
from swift.proxy.controllers.base import Controller
from swift.proxy.server import Application as ProxyApplication

from s3_sync.cloud_connector.util import (
    forward_raw_swift_req, get_and_write_conf_file_from_s3, get_env_options)
from s3_sync.provider_factory import create_provider
from s3_sync.shunt import maybe_munge_profile_for_all_containers
from s3_sync.utils import filter_hop_by_hop_headers


CLOUD_CONNECTOR_CONF_PATH = os.path.sep + os.path.join(
    'tmp', 'cloud-connector.conf')


class CloudConnectorController(Controller):
    server_type = 'cloud-connector'

    def __init__(self, app, sync_profile, version, account_name,
                 container_name, object_name):
        super(CloudConnectorController, self).__init__(app)

        self.version = version
        self.account_name = account_name  # UTF8-encoded string
        self.container_name = container_name  # UTF8-encoded string
        self.object_name = object_name  # UTF8-encoded string
        self.sync_profile, per_account = \
            maybe_munge_profile_for_all_containers(sync_profile)
        self.provider = create_provider(self.sync_profile, max_conns=5,
                                        per_account=per_account)

        aco_str = urllib.quote('/'.join(filter(None, (
            account_name, container_name, object_name))))
        self.app.logger.debug('For %s using profile %r', aco_str,
                              self.sync_profile)

    def GETorHEAD(self, req):
        # Note: account operations were already filtered out in
        # get_controller()
        if not self.object_name:
            # container listing; we'll list the remote store first, then
            # overlay any listing results from the onprem Swift cluster.
            #
            # TODO: implement this
            return swob.HTTPNotImplemented()

        # Try "remote" (with respect to config--this store should actually be
        # "closer" to this daemon) first
        provider_fn = self.provider.get_object if req.method == 'GET' else \
            self.provider.head_object
        remote_resp = provider_fn(self.object_name)
        if remote_resp.status // 100 == 2:
            # successy!
            # NOTE: any S3 headers will have already had
            # convert_to_swift_headers() called on them.
            resp_headers = dict(filter_hop_by_hop_headers(
                remote_resp.headers.items()))
            return swob.Response(app_iter=remote_resp.body,
                                 status=remote_resp.status,
                                 headers=resp_headers,
                                 request=req)

        # Nope... try "local" (with respect to config--this swift cluster
        # should actually be "further away from" this daemon) swift.
        return forward_raw_swift_req(self.app.swift_baseurl, req)

    @utils.public
    def GET(self, req):
        return self.GETorHEAD(req)

    @utils.public
    def HEAD(self, req):
        return self.GETorHEAD(req)

    @utils.public
    def PUT(self, req):
        # Note: account operations were already filtered out in
        # get_controller()
        #
        # TODO: implement this
        return swob.HTTPNotImplemented()

    @utils.public
    def POST(self, req):
        return swob.HTTPNotImplemented()

    @utils.public
    def DELETE(self, req):
        return swob.HTTPNotImplemented()


class CloudConnectorApplication(ProxyApplication):
    """
    Implements a Swift API endpoint (and eventually also a S3 API endpoint via
    swift3 middleware) to run on cloud compute nodes and
    seamlessly provide R/W access to the "cloud sync" data namespace.
    """
    def __init__(self, sync_conf_path, conf, memcache=None, logger=None):
        self.conf = conf

        if logger is None:
            self.logger = utils.get_logger(conf, log_route='cloud-connector')
        else:
            self.logger = logger
        self.memcache = memcache
        self.deny_host_headers = [
            host.strip() for host in
            conf.get('deny_host_headers', '').split(',') if host.strip()]

        try:
            with open(sync_conf_path, 'rb') as fp:
                self.sync_conf = json.load(fp)
        except (IOError, ValueError) as err:
            # There's no sane way we should get executed without something
            # having fetched and placed a sync config where our config is
            # telling us to look.  So if we can't find it, there's nothing
            # better to do than to fully exit the process.
            exit("Couldn't read sync_conf_path %r: %s; exiting" % (
                sync_conf_path, err))

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
            raise swob.HTTPException(status="403 Can't Touch This",
                                     body="Account operations are not "
                                     "supported.")

        profile_key1, profile_key2 = (acct, cont), (acct, '/*')
        profile = self.sync_profiles.get(
            profile_key1, self.sync_profiles.get(profile_key2, None))
        if not profile:
            raise swob.HTTPException(status="403 Can't Touch This",
                                     body="No matching sync profile.")

        d = dict(sync_profile=profile,
                 version=ver,
                 account_name=acct,
                 container_name=cont,
                 object_name=obj)
        return CloudConnectorController, d


def app_factory(global_conf, **local_conf):
    """paste.deploy app factory for creating WSGI proxy apps."""
    conf = global_conf.copy()
    conf.update(local_conf)

    # This gets called more than once on startup; first as root, then as the
    # configured user.  If root writes conf files down, then when the 2nd
    # invocation tries to write it down, it'll fail.  We solve this writing it
    # down the first time (instantiation of our stuff will fail otherwise), but
    # chowning the file to the final user if we're currently euid == 0.
    unpriv_user = conf.get('user', 'swift')
    sync_conf_path = os.path.sep + os.path.join('tmp', 'sync.json')
    sync_conf_obj_name = conf.get('conf_file',
                                  '/etc/swift-s3-sync/sync.json').lstrip('/')

    env_options = get_env_options()
    get_and_write_conf_file_from_s3(sync_conf_obj_name, sync_conf_path,
                                    env_options, user=unpriv_user)

    # For the same reason, if we don't chown the main conf file, it can't be
    # read after we drop privileges.  NOTE: we didn't know the configured user
    # to which we will drop privileges the first time we wrote the main config
    # file, in main(), so we couldn't do this then.
    if os.geteuid() == 0:
        user_ent = pwd.getpwnam(unpriv_user)
        os.chown(CLOUD_CONNECTOR_CONF_PATH,
                 user_ent.pw_uid, user_ent.pw_gid)

    app = CloudConnectorApplication(sync_conf_path, conf)
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
