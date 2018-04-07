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
from lxml import etree

from swift.common import constraints, swob, utils
try:
    from swift.common.middleware.listing_formats import (
        get_listing_content_type)
except ImportError:
    # compat for < ss-swift-2.15.1.3
    from swift.common.request_helpers import get_listing_content_type
from swift.proxy.controllers.base import get_account_info

from .provider_factory import create_provider
from .utils import (check_slo, SwiftPutWrapper, SwiftSloPutWrapper,
                    convert_to_local_headers, response_is_complete,
                    filter_hop_by_hop_headers)


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

        parts = env['PATH_INFO'].split('/', 3)
        if len(parts) < 3 or parts[1] not in ('v1', 'v1.0'):
            # If it's not a swift request, it can't be a ProxyFS request
            return False

        account_info = get_account_info(env, self.base_app, swift_source='')
        # ignore status; rely on something elsewhere in the pipeline
        # to propagate the error
        return utils.config_true_value(account_info["sysmeta"].get(
            'proxyfs-bimodal'))


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
        try:
            with open(conf_file, 'rb') as fp:
                conf = json.load(fp)
        except (IOError, ValueError) as err:
            self.logger.warning("Couldn't read conf_file %r: %s; disabling",
                                conf_file, err)
            conf = {'containers': []}

        self.sync_profiles = {}
        for cont in conf.get('containers', []):
            if cont.get('propagate_delete', True):
                # object shouldn't exist in remote
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
        req = swob.Request(env)
        try:
            vers, acct, cont, obj = req.split_path(3, 4, True)
        except ValueError:
            return self.app(env, start_response)

        if not constraints.valid_api_version(vers):
            return self.app(env, start_response)

        if not cont:
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

        if not obj and req.method == 'GET':
            return self.handle_listing(req, start_response, sync_profile, cont,
                                       per_account)
        elif obj and req.method in ('GET', 'HEAD'):
            # TODO: think about what to do for POST, COPY
            return self.handle_object(req, start_response, sync_profile, obj,
                                      per_account)
        elif req.method == 'POST':
            return self.handle_post(req, start_response, sync_profile, obj,
                                    per_account)

        return self.app(env, start_response)

    def iter_remote(self, sync_profile, per_account, marker, limit, prefix,
                    delimiter):
        provider = create_provider(sync_profile, max_conns=1,
                                   per_account=per_account)
        while True:
            status, resp = provider.list_objects(
                marker, limit, prefix, delimiter)
            if status != 200:
                self.logger.error('Failed to list the remote store: %s' % resp)
                break
            if not resp:
                break
            for item in resp:
                if 'name' in item:
                    marker = item['name']
                else:
                    marker = item['subdir']
                item['content_location'] = [item['content_location']]
                yield item, marker
            # WSGI supplies the request parameters as UTF-8 encoded
            # strings. We should do the same when submitting
            # subsequent requests.
            marker = marker.encode('utf-8')
        yield None, None  # just to simplify some book-keeping

    def handle_listing(self, req, start_response, sync_profile, cont,
                       per_account):
        limit = int(req.params.get(
            'limit', constraints.CONTAINER_LISTING_LIMIT))
        marker = req.params.get('marker', '')
        prefix = req.params.get('prefix', '')
        delimiter = req.params.get('delimiter', '')
        path = req.params.get('path', None)
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

        internal_resp = None
        remote_iter = None
        orig_status = None

        if sync_profile.get('migration') and status.startswith('404 '):
            # No local container, send remote only
            remote_iter = self.iter_remote(sync_profile, per_account,
                                           marker, limit, prefix,
                                           delimiter)

            orig_status = status
            provider = create_provider(sync_profile, max_conns=1,
                                       per_account=per_account)
            rem_resp = provider.head_bucket(sync_profile['aws_bucket'])
            if rem_resp.status == 200:
                status = '200 OK'
                headers = {}
                for hdr in rem_resp.headers:
                    headers[hdr.encode('utf8')] = \
                        rem_resp.headers[hdr].encode('utf8')
                internal_resp = []

        if not status.startswith('200 '):
            # Only splice 200 (since it's JSON, we know there won't be a 204)
            start_response(status, headers)
            return app_iter

        if remote_iter is None:
            remote_iter = self.iter_remote(sync_profile, per_account, marker,
                                           limit, prefix, delimiter)

        remote_item, remote_key = next(remote_iter)
        if not remote_item:
            if orig_status is not None:
                dict_headers = dict(headers)
                res = self._format_listing_response([], resp_type, cont)
                dict_headers['Content-Length'] = len(res)
                dict_headers['Content-Type'] = resp_type
                start_response(status, dict_headers.items())
                return res
            start_response(status, headers)
            return app_iter

        if internal_resp is None:
            internal_resp = json.load(utils.FileLikeIter(app_iter))

        spliced_response = []
        for local_item in internal_resp:
            if len(spliced_response) == limit:
                break

            if not remote_item:
                spliced_response.append(local_item)
                continue

            if 'name' in local_item:
                local_key = local_item['name']
            else:
                local_key = local_item['subdir']

            while remote_item and remote_key < local_key:
                spliced_response.append(remote_item)
                remote_item, remote_key = next(remote_iter)

            if remote_key == local_key:
                # duplicate!
                remote_item['content_location'].append('swift')
                spliced_response.append(remote_item)
                remote_item, remote_key = next(remote_iter)
            else:
                spliced_response.append(local_item)

        while remote_item:
            if len(spliced_response) == limit:
                break
            spliced_response.append(remote_item)
            remote_item, _junk = next(remote_iter)

        res = self._format_listing_response(spliced_response, resp_type, cont)
        dict_headers = dict(headers)
        dict_headers['Content-Length'] = len(res)
        dict_headers['Content-Type'] = resp_type
        start_response(status, dict_headers.items())
        return res

    def handle_object(self, req, start_response, sync_profile, obj,
                      per_account):
        status, headers, app_iter = req.call_application(self.app)
        if not status.startswith('404 '):
            # Only shunt 404s
            start_response(status, headers)
            return app_iter
        self.logger.debug('404 for %s; shunting to %r'
                          % (req.path, sync_profile))

        # Save off any existing trans-id headers so we can add them back later
        trans_id_headers = [(h, v) for h, v in headers if h.lower() in (
            'x-trans-id', 'x-openstack-request-id')]

        utils.close_if_possible(app_iter)

        provider = create_provider(sync_profile, max_conns=1,
                                   per_account=per_account)
        if req.method == 'GET' and sync_profile.get('restore_object', False) \
                and 'range' not in req.headers:
            # We incur an extra request hit by checking for a possible SLO.
            manifest = provider.get_manifest(obj)
            self.logger.debug("Manifest: %s" % manifest)
            status, headers, app_iter = provider.shunt_object(req, obj)
            put_headers = convert_to_local_headers(headers)

            if response_is_complete(int(status.split()[0]), headers):
                if check_slo(put_headers) and manifest:
                    app_iter = SwiftSloPutWrapper(
                        app_iter, put_headers, req.environ['PATH_INFO'],
                        self.app, manifest, self.logger)
                else:
                    app_iter = SwiftPutWrapper(
                        app_iter, put_headers, req.environ['PATH_INFO'],
                        self.app, self.logger)
        else:
            status, headers, app_iter = provider.shunt_object(req, obj)
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

    @staticmethod
    def _format_listing_response(list_results, list_format, container):
        if list_format == 'application/json':
            return json.dumps(list_results)
        if list_format.endswith('/xml'):
            fields = ['name', 'content_type', 'hash', 'bytes', 'last_modified',
                      'subdir']
            root = etree.Element('container', name=container)
            for entry in list_results:
                obj = etree.Element('object')
                for f in fields:
                    if f not in entry:
                        continue
                    el = etree.Element(f)
                    text = entry[f]
                    if type(text) == str:
                        text = text.decode('utf-8')
                    elif type(text) == int:
                        text = str(text)
                    el.text = text
                    obj.append(el)
                root.append(obj)
            resp = etree.tostring(root, encoding='UTF-8', xml_declaration=True)
            return resp.replace("<?xml version='1.0' encoding='UTF-8'?>",
                                '<?xml version="1.0" encoding="UTF-8"?>', 1)

        # Default to plain format
        return u'\n'.join(entry['name'] if 'name' in entry else entry['subdir']
                          for entry in list_results).encode('utf-8')


def filter_factory(global_conf, **local_conf):
    conf = dict(global_conf, **local_conf)

    def app_filter(app):
        conf_file = conf.get('conf_file', '/etc/swift-s3-sync/sync.json')
        shunt = S3SyncShunt(app, conf_file, conf)
        return S3SyncProxyFSSwitch(app, shunt, conf)
    return app_filter
