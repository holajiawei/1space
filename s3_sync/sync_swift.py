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

import datetime
import eventlet
import json
import swiftclient
from swift.common.internal_client import UnexpectedResponse
from swift.common.utils import FileLikeIter
import traceback
import urllib

from .base_sync import BaseSync
from .base_sync import ProviderResponse
from .utils import (FileWrapper, ClosingResourceIterable, check_slo,
                    SWIFT_USER_META_PREFIX, SWIFT_TIME_FMT)


class SyncSwift(BaseSync):
    def __init__(self, settings, max_conns=10, per_account=False):
        super(SyncSwift, self).__init__(settings, max_conns, per_account)
        # Used to verify the remote container in case of per_account uploads
        self.verified_container = False

    @property
    def remote_container(self):
        if not self._per_account:
            return self.aws_bucket
        else:
            # In this case the aws_bucket is treated as a prefix
            return self.aws_bucket + self.container

    def _get_client_factory(self):
        # TODO: support LDAP auth
        # TODO: support v2 auth
        username = self.settings['aws_identity']
        key = self.settings['aws_secret']
        # Endpoint must be defined for the Swift clusters and should be the
        # auth URL
        endpoint = self.settings['aws_endpoint']
        os_options = {}
        if self.settings.get('remote_account'):
            scheme, rest = endpoint.split(':', 1)
            host = urllib.splithost(rest)[0]
            path = '/v1/%s' % urllib.quote(self.settings['remote_account'])
            os_options = {
                'object_storage_url': '%s:%s%s' % (scheme, host, path)}

        def swift_client_factory():
            return swiftclient.client.Connection(
                authurl=endpoint, user=username, key=key, retries=3,
                os_options=os_options)
        return swift_client_factory

    def upload_object(self, name, policy, internal_client):
        if self._per_account and not self.verified_container:
            with self.client_pool.get_client() as swift_client:
                try:
                    swift_client.head_container(self.remote_container)
                except swiftclient.exceptions.ClientException as e:
                    if e.http_status != 404:
                        raise
                    swift_client.put_container(self.remote_container)
            self.verified_container = True

        try:
            with self.client_pool.get_client() as swift_client:
                remote_meta = swift_client.head_object(self.remote_container,
                                                       name)
        except swiftclient.exceptions.ClientException as e:
            if e.http_status == 404:
                remote_meta = None
            else:
                raise

        swift_req_hdrs = {
            'X-Backend-Storage-Policy-Index': policy,
            'X-Newest': True
        }

        try:
            metadata = internal_client.get_object_metadata(
                self.account, self.container, name, headers=swift_req_hdrs)
        except UnexpectedResponse as e:
            if '404 Not Found' in e.message:
                return
            raise

        if check_slo(metadata):
            try:
                # fetch the remote etag
                with self.client_pool.get_client() as swift_client:
                    # This relies on the fact that getting the manifest results
                    # in the etag being the md5 of the JSON. The internal
                    # client pipeline does not have SLO and also returns the
                    # md5 of the JSON, making our comparison valid.
                    headers, _ = swift_client.get_object(
                        self.remote_container, name,
                        query_string='multipart-manifest=get',
                        headers={'Range': 'bytes=0-0'})
                if headers['etag'] == metadata['etag']:
                    if not self._is_meta_synced(metadata, headers):
                        self.update_metadata(name, metadata)
                    return
            except swiftclient.exceptions.ClientException as e:
                if e.http_status != 404:
                    raise
            self._upload_slo(name, swift_req_hdrs, internal_client)
            return

        if remote_meta and metadata['etag'] == remote_meta['etag']:
            if not self._is_meta_synced(metadata, remote_meta):
                self.update_metadata(name, metadata)
            return

        with self.client_pool.get_client() as swift_client:
            wrapper_stream = FileWrapper(internal_client,
                                         self.account,
                                         self.container,
                                         name,
                                         swift_req_hdrs)
            headers = self._get_user_headers(wrapper_stream.get_headers())
            self.logger.debug('Uploading %s with meta: %r' % (
                name, headers))

            swift_client.put_object(self.remote_container,
                                    name,
                                    wrapper_stream,
                                    etag=wrapper_stream.get_headers()['etag'],
                                    headers=headers,
                                    content_length=len(wrapper_stream))

    def delete_object(self, name, internal_client=None):
        """Delete an object from the remote cluster.

        This is slightly more complex than when we deal with S3/GCS, as the
        remote store may have SLO manifests, as well. Because of that, this
        turns into HEAD+DELETE.
        """
        with self.client_pool.get_client() as swift_client:
            try:
                headers = swift_client.head_object(self.remote_container, name)
            except swiftclient.exceptions.ClientException as e:
                if e.http_status == 404:
                    return
                raise

            if not check_slo(headers):
                try:
                    swift_client.delete_object(self.remote_container, name)
                except swiftclient.exceptions.ClientException as e:
                    if e.http_status != 404:
                        raise
            else:
                try:
                    swift_client.delete_object(
                        self.remote_container, name,
                        query_string='multipart-manifest=delete')
                except swiftclient.exceptions.ClientException as e:
                    if e.http_status != 404:
                        raise

    def shunt_object(self, req, name):
        """Fetch an object from the remote cluster to stream back to a client.

        :returns: (status, headers, body_iter) tuple
        """
        headers_to_copy = ('Range', 'If-Match', 'If-None-Match',
                           'If-Modified-Since', 'If-Unmodified-Since')
        headers = {header: req.headers[header]
                   for header in headers_to_copy
                   if header in req.headers}
        headers['X-Trans-Id-Extra'] = req.environ['swift.trans_id']

        if req.method == 'GET':
            resp = self.get_object(
                name, resp_chunk_size=65536, headers=headers)
        elif req.method == 'HEAD':
            resp = self.head_object(name, headers=headers)
        else:
            raise ValueError('Expected GET or HEAD, not %s' %
                             req.method)
        return resp.to_wsgi()

    def head_object(self, key, bucket=None, **options):
        if bucket is None:
            bucket = self.remote_container
        resp = self._call_swiftclient('head_object', bucket, key, **options)
        resp.body = ['']
        return resp

    def get_object(self, key, bucket=None, **options):
        if bucket is None:
            bucket = self.remote_container
        return self._call_swiftclient(
            'get_object', bucket, key, **options)

    def head_bucket(self, bucket=None, **options):
        if bucket is None:
            bucket = self.remote_container
        return self._call_swiftclient(
            'head_container', bucket, None, **options)

    def list_buckets(self):
        resp = self._call_swiftclient('get_account', None, None)
        if resp.status == 200:
            for container in resp.body:
                container['last_modified'] = datetime.datetime.strptime(
                    container['last_modified'], SWIFT_TIME_FMT)
        return resp

    def _call_swiftclient(self, op, container, key, **args):
        def translate(header, value):
            if header.lower() in ('x-trans-id', 'x-openstack-request-id'):
                return ('Remote-' + header, value)
            if header == 'content-length':
                # Capitalize, so eventlet doesn't try to add its own
                return ('Content-Length', value)
            return (header, value)

        def _perform_op(client):
            try:
                if not container:
                    resp = getattr(client, op)(**args)
                elif container and not key:
                    resp = getattr(client, op)(container, **args)
                else:
                    resp = getattr(client, op)(container, key, **args)
                if isinstance(resp, tuple):
                    headers, body = resp
                else:
                    headers = resp
                    body = ['']
                status = 206 if 'content-range' in headers else 200
                headers = dict([translate(header, value)
                                for header, value in headers.items()])
                return ProviderResponse(True, status, headers, body)
            except swiftclient.exceptions.ClientException as e:
                headers = dict([translate(header, value)
                                for header, value in
                                e.http_response_headers.items()])
                return ProviderResponse(False, e.http_status, headers,
                                        iter(e.http_response_content))
            except Exception:
                self.logger.exception('Error contacting remote swift cluster')
                return ProviderResponse(False, 502, {}, iter('Bad Gateway'))

        if op == 'get_object' and 'resp_chunk_size' in args:
            entry = self.client_pool.get_client()
            resp = _perform_op(entry.client)
            if resp.success:
                resp.body = ClosingResourceIterable(
                    entry, resp.body, resp.body.resp.close)
            else:
                resp.body = ClosingResourceIterable(
                    entry, resp.body, lambda: None)
            return resp
        else:
            with self.client_pool.get_client() as swift_client:
                return _perform_op(swift_client)

    def list_objects(self, marker, limit, prefix, delimiter=None):
        try:
            with self.client_pool.get_client() as swift_client:
                hdrs, results = swift_client.get_container(
                    self.remote_container, marker=marker, limit=limit,
                    prefix=prefix, delimiter=delimiter)
                for entry in results:
                    entry['content_location'] = '%s;%s;%s' % (
                        self.endpoint,
                        self.settings['aws_identity'],
                        self.remote_container)
                return (200, results)
        except swiftclient.exceptions.ClientException as e:
            return (e.http_status, e.message)

    def update_metadata(self, name, metadata):
        with self.client_pool.get_client() as swift_client:
            swift_client.post_object(self.remote_container, name,
                                     self._get_user_headers(metadata))

    def _upload_slo(self, name, swift_headers, internal_client):
        status, headers, body = internal_client.get_object(
            self.account, self.container, name, headers=swift_headers)
        if status != 200:
            body.close()
            raise RuntimeError('Failed to get the manifest')
        manifest = json.load(FileLikeIter(body))
        body.close()
        self.logger.debug("JSON manifest: %s" % str(manifest))

        work_queue = eventlet.queue.Queue(self.SLO_QUEUE_SIZE)
        worker_pool = eventlet.greenpool.GreenPool(self.SLO_WORKERS)
        workers = []
        for _ in range(0, self.SLO_WORKERS):
            workers.append(
                worker_pool.spawn(self._upload_slo_worker, swift_headers,
                                  work_queue, internal_client))
        for segment in manifest:
            work_queue.put(segment)
        work_queue.join()
        for _ in range(0, self.SLO_WORKERS):
            work_queue.put(None)

        errors = []
        for thread in workers:
            errors += thread.wait()

        # TODO: errors list contains the failed segments. We should retry
        # them on failure.
        if errors:
            raise RuntimeError('Failed to upload an SLO %s' % name)

        # we need to mutate the container in the manifest
        container = self.remote_container + '_segments'
        new_manifest = []
        for segment in manifest:
            _, obj = segment['name'].split('/', 2)[1:]
            new_manifest.append(dict(path='/%s/%s' % (container, obj),
                                     etag=segment['hash'],
                                     size_bytes=segment['bytes']))

        self.logger.debug(json.dumps(new_manifest))
        # Upload the manifest itself
        with self.client_pool.get_client() as swift_client:
            swift_client.put_object(self.remote_container, name,
                                    json.dumps(new_manifest),
                                    headers=self._get_user_headers(headers),
                                    query_string='multipart-manifest=put')

    def get_manifest(self, key, bucket=None):
        if bucket is None:
            bucket = self.remote_container
        with self.client_pool.get_client() as swift_client:
            try:
                headers, body = swift_client.get_object(
                    bucket, key,
                    query_string='multipart-manifest=get')
                if 'x-static-large-object' not in headers:
                    return None
                return json.loads(body)
            except Exception as e:
                self.logger.warning('Failed to fetch the manifest: %s' % e)
                return None

    def _upload_slo_worker(self, req_headers, work_queue, internal_client):
        errors = []
        while True:
            segment = work_queue.get()
            if not segment:
                work_queue.task_done()
                return errors

            try:
                self._upload_segment(segment, req_headers, internal_client)
            except:
                errors.append(segment)
                self.logger.error('Failed to upload segment %s: %s' % (
                    self.account + segment['name'], traceback.format_exc()))
            finally:
                work_queue.task_done()

    def _upload_segment(self, segment, req_headers, internal_client):
        container, obj = segment['name'].split('/', 2)[1:]
        dest_container = self.remote_container + '_segments'
        with self.client_pool.get_client() as swift_client:
            wrapper = FileWrapper(internal_client, self.account, container,
                                  obj, req_headers)
            self.logger.debug('Uploading segment %s: %s bytes' % (
                self.account + segment['name'], segment['bytes']))
            try:
                swift_client.put_object(dest_container, obj, wrapper,
                                        etag=segment['hash'],
                                        content_length=len(wrapper))
            except swiftclient.exceptions.ClientException as e:
                # The segments may not exist, so we need to create it
                if e.http_status == 404:
                    self.logger.debug('Creating a segments container %s' % (
                        dest_container))
                    # Creating a container may take some (small) amount of time
                    # and we should attempt to re-upload in the following
                    # iteration
                    swift_client.put_container(dest_container)
                    raise RuntimeError('Missing segments container')

    @staticmethod
    def _is_meta_synced(local_metadata, remote_metadata):
        remote_keys = [key.lower() for key in remote_metadata.keys()
                       if key.lower().startswith(SWIFT_USER_META_PREFIX)]
        local_keys = [key.lower() for key in local_metadata.keys()
                      if key.lower().startswith(SWIFT_USER_META_PREFIX)]
        if set(remote_keys) != set(local_keys):
            return False
        for key in local_keys:
            if local_metadata[key] != remote_metadata[key]:
                return False
        return True

    @staticmethod
    def _get_user_headers(all_headers):
        return dict([(key, value) for key, value in all_headers.items()
                     if key.lower().startswith(SWIFT_USER_META_PREFIX) or
                     key.lower() == 'content-type'])
