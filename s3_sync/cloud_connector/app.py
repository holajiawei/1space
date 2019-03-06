# Copyright 2019 SwiftStack
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

import json
import os
import pwd
from six.moves.urllib.parse import urlparse
import sys
import urllib

from swift3 import request as swift3_request
from swift3 import controllers as swift3_controllers
from swift3.controllers.multi_upload import MAX_COMPLETE_UPLOAD_BODY_SIZE
from swift3.etree import Element, SubElement, tostring, fromstring

from swift.common import swob, utils, wsgi
from swift.proxy.controllers.base import Controller, get_cache_key
from swift.proxy.server import Application as ProxyApplication

from s3_sync.cloud_connector.auth import S3_IDENTITY_ENV_KEY
from s3_sync.cloud_connector.util import (
    get_and_write_conf_file_from_s3, get_env_options, ConfigReloaderMixin)
from s3_sync.provider_factory import create_provider
from s3_sync.shunt import maybe_munge_profile_for_all_containers
from s3_sync.utils import (
    get_list_params, filter_hop_by_hop_headers, iter_listing, splice_listing,
    format_listing_response, SHUNT_BYPASS_HEADER, get_listing_content_type,
    SeekableFileLikeIter)


CLOUD_CONNECTOR_CONF_PATH = os.path.sep + os.path.join(
    'tmp', 'cloud-connector.conf')
ROOT_UID = 0
CC_SWIFT_REQ_KEY = 'cloud_connector.swift3_req'


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
                return local_resp.to_swob_response(req=req)
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
                return remote_resp.to_swob_response(req=req)
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

    def _arg_str(self, *args, **kwargs):
        return ', '.join(map(repr, args) + [
            '%s=%r' % (k, v) for k, v in kwargs])

    def _log_provider_call(self, provider_name, provider_fn_name, *args,
                           **kwargs):
        self.app.logger.debug('Calling %s.%s(%s)', provider_name,
                              provider_fn_name, self._arg_str(*args, **kwargs))

    def _log_provider_response(self, provider_name, provider_fn_name, resp,
                               *args, **kwargs):
        self.app.logger.debug('Got %s for %s.%s(%s)', resp.status,
                              provider_name, provider_fn_name,
                              self._arg_str(*args, **kwargs))

    def _provider_fn_with_fallback(self, provider_fn_name, *args, **kwargs):
        """
        Calls the method name in provider_fn_name on the local-to-me provider,
        and then if the result is not 2xx, calls it on the remote-to-me
        provider.

        In all cases, the last ProviderResponse instance is returned
        """
        for provider_name in ('local_to_me_provider', 'remote_to_me_provider'):
            provider = getattr(self, provider_name)
            provider_fn = getattr(provider, provider_fn_name)
            self._log_provider_call(provider_name, provider_fn_name, *args,
                                    **kwargs)
            provider_resp = provider_fn(*args, **kwargs)
            self._log_provider_response(provider_name, provider_fn_name,
                                        provider_resp, *args, **kwargs)
            if provider_resp.status // 100 == 2:
                break
        return provider_resp

    def GETorHEAD(self, req):
        # Note: account operations were already filtered out in
        # get_controller(), and container operations were already handled in
        # GET() or HEAD().
        #

        # Try local-to-me first, then fall back to remote-to-me on any
        # non-success.
        provider_fn_name = 'get_object' if req.method == 'GET' else \
            'head_object'
        provider_resp = self._provider_fn_with_fallback(
            # Apparently boto3 wants Unicode?
            provider_fn_name, self.object_name.decode('utf8'))

        # If we know the container ("bucket" if the request we're servicing
        # came in via S3 API middleware) exists, and the response is a 404, we
        # need to note that with a fake cache entry.
        if provider_resp.status == 404:
            body = ''.join(provider_resp.body)
            provider_resp.body = [body]
            if body and 'The specified key does not exist.' in body:
                self._cache_container_exists(req, self.account_name,
                                             self.container_name)

        return provider_resp.to_swob_response(req=req)

    @utils.public
    def GET(self, req):
        # Note: account GETs were already filtered out in get_controller()
        if not self.object_name:
            return self.handle_object_listing(req)
        return self.GETorHEAD(req)

    @utils.public
    def HEAD(self, req):
        # Note: account HEADs were already filtered out in
        # get_controller()
        if not self.object_name:
            # Try local-to-me first, then fall back to remote-to-me on any
            # non-success.
            provider_resp = self._provider_fn_with_fallback('head_bucket')
            return provider_resp.to_swob_response(req=req)
        return self.GETorHEAD(req)

    def _handle_copy_put_from_swift3(self, req):
        if req.headers['X-Amz-Metadata-Directive'] != 'REPLACE':
            raise swob.HTTPNotImplemented(
                'X-Amz-Metadata-Directive was not "REPLACE"; '
                'only object-overwrite copying is supported.')
        raw_path = urllib.unquote(req.environ['RAW_PATH_INFO'])
        copy_from = urllib.unquote(req.headers['x-copy-from'])
        if raw_path != copy_from:
            raise swob.HTTPNotImplemented(
                'X-Amz-Copy-Source != object path; '
                'only object-overwrite copying is supported.')

        return self._provider_fn_with_fallback(
            'post_object', self.object_name.decode('utf8'), req.headers)

    @utils.public
    def PUT(self, req):
        # Note: account and controller operations were already filtered out in
        # get_controller()

        # Object metadata updates (POST in Swift API) that come in through the
        # S3 API are actually copying PUTs, and end up here with
        # x-amz-copy-source set, x-amz-metadata-directive set to "REPLACE", and
        # a content-length of '0'.
        #
        # Headers will have:
        #  ('X-Amz-Metadata-Directive', 'REPLACE'),
        #  ('X-Copy-From', '/<contaienr>/<key>'),  (uri quoted)
        #
        # Environ will have:
        # 'PATH_INFO': '/v1/<acct>/<container>/<key>',
        # 'CONTENT_LENGTH': '0',
        # 'HTTP_X_AMZ_METADATA_DIRECTIVE': 'REPLACE',
        # 'HTTP_X_COPY_FROM': '/<container>/<key>', (uri quoted)
        # 'swift.source': 'S3',
        # 'RAW_PATH_INFO': '/<container>/<key>', (uri quoted)
        # 'headers_raw': (
        #     ('Content-Length', '0'),
        #     ('x-amz-copy-source', '<bucket>/<key>'),  (uri qutoed)
        #     ('kx-amz-metadata-directive', 'REPLACE')
        # )
        if 'uploadId' in req.params:
            # Multipart upload part
            return self._handle_multipart_upload_part(req)
        elif req.environ.get('swift.source') == 'S3' and \
                'X-Amz-Metadata-Directive' in req.headers:
            provider_resp = self._handle_copy_put_from_swift3(req)
        else:
            # For PUTs, cloud-connector only writes to the local-to-me object
            # store.
            self._log_provider_call(
                'local_to_me_provider', 'put_object',
                self.object_name.decode('utf8'),
                req.headers, req.body_file)
            provider_resp = self.local_to_me_provider.put_object(
                self.object_name.decode('utf8'),  # boto3 wants Unicode?
                req.headers, req.body_file,
            )
            self._log_provider_response(
                'local_to_me_provider', 'put_object', provider_resp,
                self.object_name.decode('utf8'),
                req.headers, req.body_file)

        if provider_resp.status == 200:
            # S3 returns 200 but `swift3` expects what Swift returns, which is
            # 201.  So we catch the 200, turn it into 201, then swift3 will
            # like that 201 and return 200 to the actual S3 client.
            provider_resp.status = 201

        return provider_resp.to_swob_response(req=req)

    def _handle_multipart_upload_part(self, req):
        s3_key = self.local_to_me_provider.get_s3_name(self.object_name)
        if 'x-copy-from' in req.headers:
            # upload_part_copy
            _, src_key = req.headers['x-copy-from'].split('/', 1)
            s3_src_key = self.local_to_me_provider.get_s3_name(
                urllib.unquote(src_key))
            boto3_resp = self.local_to_me_provider._upload_part_copy(
                s3_key, self.local_to_me_provider.aws_bucket,
                s3_src_key, req.params['uploadId'],
                req.params['partNumber'],
                req.headers.get('x-amz-copy-source-range', None))
            headers = {
                'ETag': boto3_resp['CopyPartResult']['ETag'],
                'Last-Modified': boto3_resp['CopyPartResult']['LastModified'],
            }
        else:
            content_length = int(req.headers['content-length'])
            body = SeekableFileLikeIter(req.body_file, length=content_length)
            boto3_resp = self.local_to_me_provider._upload_part(
                s3_key, body=body, content_length=content_length,
                upload_id=req.params['uploadId'],
                part_number=req.params['partNumber'])
            headers = {'ETag': boto3_resp['ETag']}

        return swob.HTTPCreated(headers=headers)

    @utils.public
    def DELETE(self, req):
        # For DELETEs, cloud-connector only deletes from the local-to-me object
        # store.
        self._log_provider_call('local_to_me_provider', 'delete_object',
                                self.object_name.decode('utf8'))
        provider_resp = self.local_to_me_provider.delete_object(
            self.object_name.decode('utf8'),  # boto3 wants Unicode?
        )
        self._log_provider_response('local_to_me_provider', 'delete_object',
                                    provider_resp,
                                    self.object_name.decode('utf8'))

        return provider_resp.to_swob_response(req=req)

    def _handle_create_multipart_upload(self, s3_key, req):
        req.headers.setdefault('content-type', 'application/octet-stream')
        boto3_resp = self.local_to_me_provider._create_multipart_upload(
            req.headers, s3_key)

        # NOTE: we forced swift3 to fully delegate to us, so we are
        # responsible for returning a valid S3 API response.
        result_elem = Element('InitiateMultipartUploadResult')
        SubElement(result_elem, 'Bucket').text = self.container_name
        SubElement(result_elem, 'Key').text = self.object_name
        SubElement(result_elem, 'UploadId').text = boto3_resp['UploadId']
        body = tostring(result_elem)

        # Note: swift3 mw requires obj POST to return 202
        return swob.HTTPAccepted(body=body, content_type='application/xml')

    def _handle_complete_multipart_upload(self, s3_key, req):
        upload_id = req.params['uploadId']

        parts = []
        xml = req.environ[CC_SWIFT_REQ_KEY].xml(MAX_COMPLETE_UPLOAD_BODY_SIZE)

        complete_elem = fromstring(xml, 'CompleteMultipartUpload')
        for part_elem in complete_elem.iterchildren('Part'):
            part_number = int(part_elem.find('./PartNumber').text)
            etag = part_elem.find('./ETag').text
            parts.append({'ETag': etag, 'PartNumber': part_number})

        boto3_resp = self.local_to_me_provider._complete_multipart_upload(
            s3_key, upload_id, parts)

        # NOTE: we forced swift3 to fully delegate to us, so we are
        # responsible for returning a valid S3 API response.
        # NOTE (for the note): this workaround for a client library (boto) was
        # copied verbatim from the "swift3" codebase.  It may or may not be
        # relevant (I think it is for any client talking to cloud-connector
        # using boto and a non-default port number), but this comment and the
        # workaround both come from "swift3" and should be safe for us as well.

        # (sic) vvvvvvvvvvvvvvvvvvvvvvvvvv
        # NOTE: boto with sig v4 appends port to HTTP_HOST value at the
        # request header when the port is non default value and it
        # makes req.host_url like as http://localhost:8080:8080/path
        # that obviously invalid. Probably it should be resolved at
        # swift.common.swob though, tentatively we are parsing and
        # reconstructing the correct host_url info here.
        # in detail, https://github.com/boto/boto/pull/3513
        # (sic) ^^^^^^^^^^^^^^^^^^^^^^^^^^
        parsed_url = urlparse(req.host_url)
        host_url = '%s://%s' % (parsed_url.scheme, parsed_url.hostname)
        if parsed_url.port:
            host_url += ':%s' % parsed_url.port

        result_elem = Element('CompleteMultipartUploadResult')
        SubElement(result_elem, 'Location').text = host_url + req.path
        SubElement(result_elem, 'Bucket').text = self.container_name
        SubElement(result_elem, 'Key').text = self.object_name
        SubElement(result_elem, 'ETag').text = boto3_resp['ETag']
        body = tostring(result_elem)

        # Note: swift3 mw requires obj POST to return 202
        return swob.HTTPAccepted(body=body, content_type='application/xml')

    @utils.public
    def POST(self, req):
        # NOTE: obj metadata update via S3 API (`swift3` middleware) is
        # implemented in the PUT method.
        s3_key = self.local_to_me_provider.get_s3_name(self.object_name)
        if 'uploads' in req.params:
            # Multipart upload initiation
            return self._handle_create_multipart_upload(s3_key, req)
        elif 'uploadId' in req.params:
            # Multipart upload completion
            return self._handle_complete_multipart_upload(s3_key, req)

        return swob.HTTPNotImplemented()

    @utils.public
    def OPTIONS(self, req):
        # TODO: implement this (or not)
        return swob.HTTPNotImplemented()


class CloudConnectorApplication(ConfigReloaderMixin, ProxyApplication):
    """
    Implements an S3 API endpoint (and eventually also a Swift endpoint)
    to run on cloud compute nodes and seamlessly provide R/W access to the
    1space data namespace.
    """
    # We're going to want fine-grained control over our pipeline and, for
    # example, don't want proxy code sticking the "copy" middleware back in
    # when we can't have it.  This setting disables automatic mucking with our
    # pipeline.
    modify_wsgi_pipeline = None
    CHECK_PERIOD = 30  # seconds
    CONF_FILES = None  # can't be known until __init__ runs

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

        self.swift_baseurl = conf.get('swift_baseurl')
        sync_conf_obj_name = conf.get(
            'conf_file', '/etc/swift-s3-sync/sync.json').lstrip('/')

        self.CONF_FILES = [{
            'key': sync_conf_obj_name,
            'load_cb': self.load_sync_config,
        }]
        try:
            self.reload_confs()
        except Exception as e:
            # There's no sane way we should get executed without something
            # having fetched and placed a sync config where our config is
            # telling us to look.  So if we can't find it, there's nothing
            # better to do than to fully exit the process.
            exit("Couldn't load sync_conf: %s; exiting" % e)

    def load_sync_config(self, sync_conf_contents):
        self.sync_conf = json.loads(sync_conf_contents)

        self.sync_profiles = {}
        for cont in self.sync_conf['containers']:
            key = (cont['account'].encode('utf-8'),
                   cont['container'].encode('utf-8'))
            self.sync_profiles[key] = cont

    def __call__(self, *args, **kwargs):
        self.reload_confs()
        return super(CloudConnectorApplication, self).__call__(*args, **kwargs)

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

        if not obj and req.method not in ('GET', 'HEAD'):
            # We've decided to only support container listings (GET)
            raise swob.HTTPForbidden(body="The only supported container "
                                     "operations are GET and HEAD.")

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


class PassThroughController(swift3_controllers.Controller):
    """
    "Controller" class that just passes every verb we care about on to the
    application.  This is used to let the cloud-connector application fully
    handle multipart uploads.

    We also faithfully pass through query parameters.
    """

    @utils.public
    def GET(self, req):
        try:
            req.environ[CC_SWIFT_REQ_KEY] = req
            return req.get_response(self.app, query=req.params)
        finally:
            req.environ.pop(CC_SWIFT_REQ_KEY, None)

    @utils.public
    def PUT(self, req):
        try:
            req.environ[CC_SWIFT_REQ_KEY] = req
            resp = req.get_response(self.app, query=req.params)
        finally:
            req.environ.pop(CC_SWIFT_REQ_KEY, None)

        # Just like the PartController.PUT() method does:
        if 'X-Amz-Copy-Source' in req.headers:
            resp.append_copy_resp_body('Part',
                                       resp.headers.pop('Last-Modified'))

        resp.status = 200
        return resp

    @utils.public
    def POST(self, req):
        try:
            req.environ[CC_SWIFT_REQ_KEY] = req
            return req.get_response(self.app, query=req.params)
        finally:
            req.environ.pop(CC_SWIFT_REQ_KEY, None)

    @utils.public
    def DELETE(self, req):
        try:
            req.environ[CC_SWIFT_REQ_KEY] = req
            return req.get_response(self.app, query=req.params)
        finally:
            req.environ.pop(CC_SWIFT_REQ_KEY, None)


class CloudConnectorS3RequestMixin(object):
    @property
    def controller(self):
        cont_klass = super(CloudConnectorS3RequestMixin, self).controller
        if cont_klass in (swift3_controllers.PartController,
                          swift3_controllers.UploadsController,
                          swift3_controllers.UploadController):
            return PassThroughController
        return cont_klass


class CCRequest(CloudConnectorS3RequestMixin, swift3_request.Request):
    pass


class CCSigV4Request(CloudConnectorS3RequestMixin,
                     swift3_request.SigV4Request):
    pass


class CCS3AclRequest(CloudConnectorS3RequestMixin,
                     swift3_request.S3AclRequest):
    pass


class CCSigV4S3AclRequest(CloudConnectorS3RequestMixin,
                          swift3_request.SigV4S3AclRequest):
    pass


def _cc_get_request_class(env):
    """
    Cough up request classes that we've modified (inserted a mixin into).
    """
    if swift3_request.CONF.s3_acl:
        request_classes = (CCS3AclRequest, CCSigV4S3AclRequest)
    else:
        request_classes = (CCRequest, CCSigV4Request)

    req = swob.Request(env)
    if 'X-Amz-Credential' in req.params or \
            req.headers.get('Authorization', '').startswith(
                'AWS4-HMAC-SHA256 '):
        # This is an Amazon SigV4 request
        return request_classes[1]
    else:
        # The others using Amazon SigV2 class
        return request_classes[0]


def monkeypatch_swift3_requests():
    swift3_request.get_request_class = _cc_get_request_class


def monkeypatch_hash_validation():
    def _new_validate_configuration():
        try:
            utils.validate_hash_conf()
        except utils.InvalidHashPathConfigError:
            pass

    utils.validate_configuration = _new_validate_configuration
    wsgi.validate_configuration = _new_validate_configuration


def main():
    """
    cloud-connector daemon entry point.

    Loads main config file from a S3 endpoint per environment configuration and
    then starts the wsgi server.
    """

    # We need to monkeypatch out the hash validation stuff
    monkeypatch_hash_validation()

    # We need to monkeypatch swift3 request class determination so we can
    # override some behavior in multipart uploads
    monkeypatch_swift3_requests()

    env_options = get_env_options()

    get_and_write_conf_file_from_s3(env_options['CONF_NAME'],
                                    CLOUD_CONNECTOR_CONF_PATH, env_options)
    sys.argv.insert(1, CLOUD_CONNECTOR_CONF_PATH)

    conf_file, options = utils.parse_options()
    # Calling this "proxy-server" in the pipeline is a little white lie to keep
    # the swift3 pipeline check from blowing up.
    sys.exit(wsgi.run_wsgi(conf_file, 'proxy-server', **options))
