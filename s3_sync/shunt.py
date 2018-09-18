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

from os.path import getmtime
from swift.common import constraints, swob, utils
from swift.common.wsgi import make_subrequest
from swift.proxy.controllers.base import get_account_info
from time import time

from .provider_factory import create_provider
from .utils import (check_slo, SwiftPutWrapper, SwiftSloPutWrapper,
                    RemoteHTTPError, convert_to_local_headers,
                    response_is_complete, filter_hop_by_hop_headers,
                    iter_listing, get_container_headers,
                    MigrationContainerStates, get_sys_migrator_header,
                    get_list_params, format_listing_response,
                    format_container_listing_response, splice_listing,
                    SHUNT_BYPASS_HEADER, get_listing_content_type)


class S3SyncProxyFSSwitch(object):
    def __init__(self, base_app, shunted_app, conf):
        self.base_app = base_app
        self.shunted_app = shunted_app
        self.should_handle_proxyfs = utils.config_true_value(
            conf.get('handle_proxyfs', 'false'))

    def __call__(self, env, start_response):
        if self.is_proxy_fs(env) == self.should_handle_proxyfs:
            return self.shunted_app(env, start_response)
        return self.base_app(env, start_response)

    def is_proxy_fs(self, env):
        if 'pfs.is_bimodal' in env:
            return env['pfs.is_bimodal']

        try:
            # Need at least an account
            vers, a, c, o = utils.split_path(env['PATH_INFO'], 2, 4, True)
            if not constraints.valid_api_version(vers):
                raise ValueError
        except ValueError:
            # If it's not a swift request, it can't be a ProxyFS request
            return False

        account_info = get_account_info(env, self.base_app, swift_source='')
        # ignore status; rely on something elsewhere in the pipeline
        # to propagate the error
        return utils.config_true_value(account_info["sysmeta"].get(
            'proxyfs-bimodal'))


SECRETS = ('aws_secret',)


def redact_secrets(profile):
    newdict = dict(profile)
    for key in newdict.keys():
        if key in SECRETS:
            newdict[key] = "(redacted)"
    return newdict


def maybe_munge_profile_for_all_containers(sync_profile, container_name):
    """
    Takes a sync profile config and a UTF8-encoded container name, and returns
    a sync profile and a `per_account` boolean.

    If if the config applies to all containers, the returned config will be a
    copy of the supplied sync_profile and will include the given specific
    container name and per_account will be True (except in the case of
    migrations).  FWIW, note that container names in sync profiles are Unicode
    strings.

    Otherwise, the original profile is returned and per_account will be False.
    """
    # This can only occur for migrations
    if sync_profile['aws_bucket'] == '/*':
        new_profile = dict(sync_profile,
                           aws_bucket=container_name.decode('utf-8'),
                           container=container_name.decode('utf-8'))
        return new_profile, False

    if sync_profile['container'] == '/*':
        new_profile = dict(sync_profile,
                           container=container_name.decode('utf-8'))
        if sync_profile.get('migration'):
            per_account = False
        else:
            per_account = True
        return new_profile, per_account
    return sync_profile, False


class S3SyncShunt(object):
    def __init__(self, app, conf_file, conf):
        self.logger = utils.get_logger(
            conf, name='proxy-server:s3_sync.shunt',
            log_route='s3_sync.shunt')

        self.app = app
        self.conf_file = conf_file
        self.sync_profiles = {}
        self.reload_time = 15
        self._rtime = 0
        self._mtime = 0
        self._reload(True)

    def _reload(self, force=False):
        self._rtime = time() + self.reload_time
        try:
            mtime = getmtime(self.conf_file)
            if mtime == self._mtime and not force:
                return

            self._mtime = mtime

            with open(self.conf_file, 'rb') as fp:
                conf = json.load(fp)
        except (IOError, ValueError, OSError):
            conf = {'containers': []}

        self.sync_profiles = {}
        for cont in conf.get('containers', []):
            # ONLY use shunt if merge_namespaces is set to true for sync
            if not cont.get('merge_namespaces', False):
                continue
            key = (cont['account'].encode('utf-8'),
                   cont['container'].encode('utf-8'))
            self.sync_profiles[key] = cont

        for migration in conf.get('migrations', []):
            profile = dict(migration)
            # Migrations should have some sane defaults if they aren't present
            profile.setdefault('restore_object', True)
            profile.setdefault('container', profile['aws_bucket'])
            profile.setdefault('migration', True)
            # if, in the future, we support custom_prefix on the S3 side,
            # we may need to change this. Swift side code ignores custom_prefix
            profile['custom_prefix'] = ''
            key = (profile['account'].encode('utf-8'),
                   profile['container'].encode('utf-8'))
            self.sync_profiles[key] = profile

    def __call__(self, env, start_response):
        if time() > self._rtime:
            self._reload()

        req = swob.Request(env)
        try:
            vers, acct, cont, obj = req.split_path(2, 4, True)
        except ValueError:
            return self.app(env, start_response)

        if req.headers.get(SHUNT_BYPASS_HEADER, ''):
            self.logger.debug('Bypassing shunt (%s header) for %r',
                              SHUNT_BYPASS_HEADER, req.path_info)
            return self.app(env, start_response)

        if not constraints.valid_api_version(vers):
            return self.app(env, start_response)

        if not cont:
            sync_profile = self.sync_profiles.get((acct, '/*'))
            if req.method == 'GET' and sync_profile and\
                    sync_profile.get('migration'):
                # TODO: make the container an optional parameter
                profile, _ = maybe_munge_profile_for_all_containers(
                    sync_profile, '.stub-container')
                return self.handle_account(req, start_response, profile, acct)

            return self.app(env, start_response)

        sync_profile = next((self.sync_profiles[(acct, c)]
                             for c in (cont, '/*')
                             if (acct, c) in self.sync_profiles), None)
        if sync_profile is None:
            return self.app(env, start_response)
        sync_profile, per_account = maybe_munge_profile_for_all_containers(
            sync_profile, cont)

        if req.method == 'DELETE' and sync_profile.get('migration'):
            return self.handle_delete(
                req, start_response, sync_profile, obj, per_account)

        if not obj:
            if req.method == 'GET':
                return self.handle_listing(
                    req, start_response, sync_profile, cont, per_account)
            if req.method == 'HEAD' and sync_profile.get('migration'):
                return self.handle_container_head(
                    req, start_response, sync_profile, cont, per_account)
        if obj and req.method in ('GET', 'HEAD'):
            # TODO: think about what to do for POST, COPY
            return self.handle_object(req, start_response, sync_profile, obj,
                                      per_account)
        if req.method == 'POST':
            return self.handle_post(req, start_response, sync_profile, obj,
                                    per_account)

        if obj and req.method == 'PUT' and sync_profile.get('migration'):
            return self.handle_object_put(
                req, start_response, sync_profile, per_account)

        return self.app(env, start_response)

    def iter_remote_objects(
            self, sync_profile, per_account, marker, limit, prefix, delimiter):
        provider = create_provider(sync_profile, max_conns=1,
                                   per_account=per_account)
        return iter_listing(
            provider.list_objects, self.logger, marker, limit, prefix,
            delimiter)

    def iter_remote_account(
            self, sync_profile, marker, limit, prefix, delimiter):
        '''Iterate through the remote listing of containers.'''
        provider = create_provider(sync_profile, max_conns=1)
        return iter_listing(
            provider.list_buckets, self.logger, marker, limit, prefix, False)

    def handle_account(self, req, start_response, sync_profile, account):
        limit, marker, prefix, delimiter, _ = get_list_params(
            req, constraints.ACCOUNT_LISTING_LIMIT)
        resp_type = get_listing_content_type(req)

        # We always make the request with the json format and convert to the
        # client-expected response.
        req.params = dict(req.params, format='json')
        status, headers, app_iter = req.call_application(self.app)
        if not status.startswith('200 '):
            # Only splice 200 (since it's JSON, we know there won't be a 204).
            # The account must exist in both clusters.
            start_response(status, headers)
            return app_iter

        _, remote_iter = self.iter_remote_account(
            sync_profile, marker, limit, prefix, delimiter)
        spliced = splice_listing(
            json.load(utils.FileLikeIter(app_iter)), remote_iter, limit)
        response = format_container_listing_response(
            spliced, resp_type, account)
        dict_headers = dict(headers)
        dict_headers['Content-Length'] = len(response)
        dict_headers['Content-Type'] = resp_type
        start_response(status, dict_headers.items())
        return response

    def handle_object_put(
            self, req, start_response, sync_profile, per_account):

        status, headers, app_iter = req.call_application(self.app)

        if not status.startswith('404 '):
            start_response(status, headers)
            return app_iter

        provider = create_provider(sync_profile, max_conns=1,
                                   per_account=per_account)
        headers = {}
        if sync_profile.get('protocol') == 'swift':
            try:
                headers = get_container_headers(provider)
            except RemoteHTTPError as e:
                self.logger.warning(
                    'Failed to query the remote container (%d): %s' % (
                        e.resp.status, e.resp.body))
                start_response(status, headers)
                return app_iter

        vers, acct, cont, _ = req.split_path(4, 4, True)
        container_path = '/%s' % '/'.join([
            vers, utils.quote(acct), utils.quote(cont)])
        put_container_req = make_subrequest(
            req.environ,
            method='PUT',
            path=container_path,
            headers=headers,
            swift_source='CloudSync Shunt')
        put_container_req.environ['swift_owner'] = True
        status, headers, body = put_container_req.call_application(self.app)
        utils.close_if_possible(body)

        if int(status.split()[0]) // 100 != 2:
            self.logger.warning('Failed to create container: %s' % status)

        status, headers, app_iter = req.call_application(self.app)
        start_response(status, headers)
        return app_iter

    def handle_listing(self, req, start_response, sync_profile, cont,
                       per_account):
        limit, marker, prefix, delimiter, path = get_list_params(
            req, constraints.CONTAINER_LISTING_LIMIT)

        if path:
            # We do not support the path parameter in listings
            status, headers, app_iter = req.call_application(self.app)
            start_response(status, headers)
            return app_iter

        resp_type = get_listing_content_type(req)
        # We always make the request with the json format and convert to the
        # client-expected response.
        req.params = dict(req.params, format='json')
        status, headers, app_iter = req.call_application(self.app)

        if not status.startswith('200 ') and not\
                (status.startswith('404 ') and sync_profile.get('migration')):
            # Only splice 200 or 404 on migrations (since it's JSON, we know
            # there won't be a 204)
            start_response(status, headers)
            return app_iter

        remote_resp, remote_iter = self.iter_remote_objects(
            sync_profile, per_account, marker, limit, prefix, delimiter)

        if status.startswith('404 '):
            # This must be a migration, where the container has not yet been
            # created
            headers = {}
            for hdr in remote_resp.headers:
                # These are set after we mutate the request in the appropriate
                # format
                if hdr == 'content-type' or hdr == 'Content-Length':
                    continue
                headers[hdr.encode('utf8')] =\
                    remote_resp.headers[hdr].encode('utf8')
            # TODO: If to_wsgi does the utf8 header encoding, we wouldn't have
            # to worry about it here.
            status = remote_resp.to_wsgi()[0]
            spliced = splice_listing([], remote_iter, limit)
        else:
            spliced = splice_listing(
                json.load(utils.FileLikeIter(app_iter)), remote_iter, limit)

        response = format_listing_response(spliced, resp_type, cont)
        dict_headers = dict(headers)
        dict_headers['Content-Length'] = len(response)
        dict_headers['Content-Type'] = resp_type
        start_response(status, dict_headers.items())
        return response

    def handle_container_head(self, req, start_response, sync_profile, cont,
                              per_account):
        status, headers, app_iter = req.call_application(self.app)
        if not status.startswith('404 '):
            # Only shunt 404s
            start_response(status, headers)
            return app_iter

        # Save off any existing trans-id headers so we can add them back later
        trans_id_headers = [(h, v) for h, v in headers if h.lower() in (
            'x-trans-id', 'x-openstack-request-id')]

        provider = create_provider(sync_profile, max_conns=1,
                                   per_account=per_account)

        resp = provider.head_bucket(sync_profile['aws_bucket'])
        if resp.status != 200:
            # return original failure
            start_response(status, headers)
            return app_iter

        headers = [(k.encode('utf-8'), unicode(v).encode('utf-8'))
                   for k, v in resp.headers.iteritems()]
        self.logger.debug('Remote resp: %s' % resp.status)

        headers = filter_hop_by_hop_headers(headers)
        headers.extend(trans_id_headers)

        # TODO: Unfortunately, on HEAD bucket, S3 returns a 200 OK. To
        # normalize this, we made the Swift provider also return 200 OK on
        # HEAD. We should change the provider to return 204. When we fix the
        # provider, we should return its return code.
        start_response('204 No Content', headers)
        return []

    def handle_object(self, req, start_response, sync_profile, obj,
                      per_account):
        status, headers, app_iter = req.call_application(self.app)
        if not status.startswith('404 '):
            # Only shunt 404s
            start_response(status, headers)
            return app_iter
        self.logger.debug('404 for %s; shunting to %r'
                          % (req.path, redact_secrets(sync_profile)))

        # Save off any existing trans-id headers so we can add them back later
        trans_id_headers = [(h, v) for h, v in headers if h.lower() in (
            'x-trans-id', 'x-openstack-request-id')]

        utils.close_if_possible(app_iter)

        provider = create_provider(sync_profile, max_conns=1,
                                   per_account=per_account)
        if req.method == 'GET' and sync_profile.get('restore_object', False) \
                and 'range' not in req.headers:
            # We incur an extra request hit by checking for a possible SLO.
            obj = obj.decode('utf-8')
            manifest = provider.get_manifest(obj)
            self.logger.debug("Manifest: %s" % manifest)
            status, headers, app_iter = provider.shunt_object(req, obj)

            if response_is_complete(int(status.split()[0]), headers):
                put_headers = convert_to_local_headers(headers)
                if check_slo(put_headers):
                    if manifest:
                        app_iter = SwiftSloPutWrapper(
                            app_iter, put_headers, req.environ['PATH_INFO'],
                            self.app, manifest, self.logger)
                    else:
                        # if slo manifest is missing, log error, don't attempt
                        # to restore object, but continue shunt
                        self.logger.error('Failed to restore slo object due '
                                          'to missing manifest: %s' % obj)
                else:
                    app_iter = SwiftPutWrapper(
                        app_iter, put_headers, req.environ['PATH_INFO'],
                        self.app, self.logger)
        else:
            status, headers, app_iter = provider.shunt_object(req, obj)
        headers = [(k.encode('utf-8'), unicode(v).encode('utf-8'))
                   for k, v in headers]
        self.logger.debug('Remote resp: %s' % status)

        headers = filter_hop_by_hop_headers(headers)
        headers.extend(trans_id_headers)

        start_response(status, headers)
        return app_iter

    def handle_delete(
            self, req, start_response, sync_profile, obj, per_account):
        status, headers, app_iter = req.call_application(self.app)

        if sync_profile.get('protocol') != 'swift':
            start_response(status, headers)
            return app_iter

        if sync_profile.get('migration'):
            provider = create_provider(sync_profile, max_conns=1,
                                       per_account=per_account)
            remote_resp = provider.shunt_delete(req, obj)

        if status.startswith('404'):
            start_response(remote_resp[0], remote_resp[1])
            return remote_resp[2]

        start_response(status, headers)
        return app_iter

    def handle_post(
            self, req, start_response, sync_profile, obj, per_account):
        if not obj and sync_profile.get('migration'):
            req.headers[get_sys_migrator_header('container')] =\
                MigrationContainerStates.MODIFIED
        status, headers, app_iter = req.call_application(self.app)

        if not status.startswith('404'):
            start_response(status, headers)
            return app_iter

        if sync_profile.get('protocol') != 'swift':
            # TODO: Add S3 support
            start_response(status, headers)
            return app_iter

        if not obj and not sync_profile.get('migration'):
            start_response(status, headers)
            return app_iter

        provider = create_provider(sync_profile, max_conns=1,
                                   per_account=per_account)
        status, headers, app_iter = provider.shunt_post(req, obj)
        start_response(status, headers)
        return app_iter


def filter_factory(global_conf, **local_conf):
    conf = dict(global_conf, **local_conf)

    def app_filter(app):
        conf_file = conf.get('conf_file', '/etc/swift-s3-sync/sync.json')
        shunt = S3SyncShunt(app, conf_file, conf)
        return S3SyncProxyFSSwitch(app, shunt, conf)
    return app_filter
