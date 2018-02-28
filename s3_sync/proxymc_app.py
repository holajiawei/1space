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

import json
import urllib
from urlparse import urlsplit

from swift.common import swob, utils, bufferedhttp
from swift.proxy.controllers.base import Controller
from swift.proxy.server import Application as ProxyApplication

from .provider_factory import create_provider
from .utils import (
    ClosingResourceIterable, filter_hop_by_hop_headers,
    convert_to_swift_headers)


class ProxyMCController(Controller):
    server_type = 'proxymc'

    def __init__(self, app, sync_profile, version, account_name,
                 container_name, object_name):
        super(ProxyMCController, self).__init__(app)

        self.sync_profile = sync_profile
        self.version = version
        self.account_name = account_name  # UTF8-encoded string
        self.container_name = container_name  # UTF8-encoded string
        self.object_name = object_name  # UTF8-encoded string

        aco_str = urllib.quote('/'.join(filter(None, (
            account_name, container_name, object_name))))

        # XXX duplicated logic with s3_sync.shunt.S3SyncShunt.__call__
        if (not self.sync_profile['provider'] and
                self.sync_profile['container'] == '/*'):
            profile = dict(self.sync_profile,
                           container=self.container_name.decode('utf-8'))
            self.app.logger.debug('Cooked up "/*" container provider for %s',
                                  aco_str)
            self.provider = create_provider(profile, max_conns=5,
                                            per_account=True)
        elif self.sync_profile['provider']:
            self.app.logger.debug('Creating provider for %s', aco_str)
            self.provider = self.sync_profile['provider']
        else:
            self.app.logger.debug('Rejecting request; no mapping for %s',
                                  aco_str)
            raise swob.HTTPForbidden()

    def forward_raw_swift_req(self, req):
        scheme, netloc, _, _, _ = urlsplit(self.app.swift_baseurl)
        ssl = (scheme == 'https')
        swift_host, swift_port = utils.parse_socket_string(netloc,
                                                           443 if ssl else 80)
        swift_port = int(swift_port)
        if ssl:
            conn = bufferedhttp.HTTPSConnection(swift_host, port=swift_port)
        else:
            conn = bufferedhttp.BufferedHTTPConnection(swift_host,
                                                       port=swift_port)
        conn.path = req.path_qs
        conn.putrequest(req.method, req.path_qs, skip_host=True)
        for header, value in filter_hop_by_hop_headers(req.headers.items()):
            if header.lower() == 'host':
                continue
            conn.putheader(header, str(value))
        conn.putheader('Host', str(swift_host))
        conn.endheaders()

        resp = conn.getresponse()
        headers = dict(filter_hop_by_hop_headers(resp.getheaders()))
        # XXX If this is a GET, do we want to "tee" the Swift object into the
        # remote (S3) store as it's fed back out to the client??
        body_len = 0 if req.method == 'HEAD' \
            else int(headers['content-length'])
        app_iter = ClosingResourceIterable(
            resource=conn, data_src=resp,
            length=body_len)
        return swob.Response(app_iter=app_iter,
                             status=resp.status,
                             headers=headers,
                             request=req)

    def GETorHEAD(self, req):
        # Note: account operations were already filtered out in
        # get_controller()
        if not self.object_name:
            # container listing; we'll list the remote store first, then
            # overlay any listing results from the onprem Swift cluster.
            #
            # ... but for now, just only forward to onprem Swift cluster just
            # so we get a valid response back to a client instead of a 500.
            return self.forward_raw_swift_req(req)

        # Try "remote" (with respect to config--this store should actually be
        # "closer" to this daemon) first
        provider_fn = self.provider.get_object if req.method == 'GET' else \
            self.provider.head_object
        remote_resp = provider_fn(self.object_name)
        if remote_resp.status // 100 == 2:
            # successy!
            # XXX probably need to call convert_to_swift_headers()
            # XXX and also probably filter_hop_by_hop_headers()
            return swob.Response(app_iter=remote_resp.body,
                                 status=remote_resp.status,
                                 headers=remote_resp.headers,
                                 request=req)

        # Nope... try "local" (with respect to config--this swift cluster
        # should actually be "further away from" this daemon) swift.
        return self.forward_raw_swift_req(req)

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
        if not self.object_name:
            # container create; we'll ... just do something?  store user
            # metadata headers in S3 so it can be sync'ed down into the Swift
            # cluster later?  Who knows!
            self.app.logger.debug('Forwarding container PUT to real Swift')
            return self.forward_raw_swift_req(req)
        self.app.logger.debug('put_object_from_swift_req: %s %r %r',
                              self.object_name, self.provider.__dict__, req)
        remote_resp = self.provider.put_object_from_swift_req(
            self.object_name, req)
        self.app.logger.debug('PUT(%s): %r', self.object_name, remote_resp)
        resp_meta = remote_resp['ResponseMetadata']
        resp_headers = convert_to_swift_headers(resp_meta['HTTPHeaders'])
        resp_body = remote_resp.get('Body', [''])
        return swob.Response(app_iter=resp_body,
                             status=int(resp_meta['HTTPStatusCode']),
                             headers=resp_headers,
                             request=req)

    @utils.public
    def POST(self, req):
        pass

    @utils.public
    def DELETE(self, req):
        pass


class ProxyMCApplication(ProxyApplication):
    """
    I'm not a puppet, I'm a REAL BOY!

    Implements a Swift API endpoint to run on cloud compute nodes and
    seamlessly provides R/W access to the "cloud sync" data namespace.
    """
    def __init__(self, conf_file, conf, memcache=None, logger=None):
        if logger is None:
            self.logger = utils.get_logger(conf, log_route='proxymc')
        else:
            self.logger = logger
        self.memcache = memcache
        self.deny_host_headers = [
            host.strip() for host in
            conf.get('deny_host_headers', '').split(',') if host.strip()]

        try:
            with open(conf_file, 'rb') as fp:
                self.json_conf = json.load(fp)
        except (IOError, ValueError) as err:
            self.logger.warning("Couldn't read conf_file %r: %s; disabling",
                                conf_file, err)
            self.json_conf = {'containers': []}

        self.sync_profiles = {}
        for cont in self.json_conf['containers']:
            key = (cont['account'].encode('utf-8'),
                   cont['container'].encode('utf-8'))
            if cont['container'] and cont['container'] != '/*':
                cont['provider'] = create_provider(cont, max_conns=256,
                                                   per_account=False)
            else:
                cont['provider'] = None
            self.sync_profiles[key] = cont

        self.swift_baseurl = self.json_conf['proxymc_config']['swift_baseurl']

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
        return ProxyMCController, d


def app_factory(global_conf, **local_conf):
    """paste.deploy app factory for creating WSGI proxy apps."""
    conf = global_conf.copy()
    conf.update(local_conf)
    conf_file = conf.get('conf_file', '/etc/swift-s3-sync/sync.json')
    app = ProxyMCApplication(conf_file, conf)
    return app
