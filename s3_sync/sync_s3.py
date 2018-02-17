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

import boto3
import botocore.exceptions
from botocore.handlers import (
    conditionally_calculate_md5, set_list_objects_encoding_type_url)
import eventlet
import hashlib
import json
import re
import traceback
import urllib

from swift.common.internal_client import UnexpectedResponse
from swift.common.utils import FileLikeIter
from .base_sync import BaseSync
from .base_sync import ProviderResponse
from .utils import (
    convert_to_s3_headers, convert_to_swift_headers, FileWrapper,
    SLOFileWrapper, ClosingResourceIterable, get_slo_etag, check_slo,
    SLO_ETAG_FIELD, SLO_HEADER, SWIFT_USER_META_PREFIX, SWIFT_TIME_FMT)


class SyncS3(BaseSync):
    # S3 prefix space: 6 16 digit characters
    PREFIX_LEN = 6
    PREFIX_SPACE = 16 ** PREFIX_LEN
    MIN_PART_SIZE = 5 * BaseSync.MB
    MAX_PART_SIZE = 5 * BaseSync.GB
    MAX_PARTS = 10000
    GOOGLE_API = 'https://storage.googleapis.com'
    CLOUD_SYNC_VERSION = '5.0'
    GOOGLE_UA_STRING = 'CloudSync/%s (GPN:SwiftStack)' % CLOUD_SYNC_VERSION
    SLO_MANIFEST_SUFFIX = '.swift_slo_manifest'

    def _is_amazon(self):
        return not self.endpoint or self.endpoint.endswith('amazonaws.com')

    def _google(self):
        return self.endpoint == self.GOOGLE_API

    def _get_client_factory(self):
        aws_identity = self.settings['aws_identity']
        aws_secret = self.settings['aws_secret']
        self.encryption = self.settings.get('encryption', True)

        boto_session = boto3.session.Session(
            aws_access_key_id=aws_identity,
            aws_secret_access_key=aws_secret)
        if not self.endpoint or self.endpoint.endswith('amazonaws.com'):
            # We always use v4 signer with Amazon, as it will support all
            # regions.
            boto_config = boto3.session.Config(signature_version='s3v4',
                                               s3={'aws_chunked': True})
        else:
            # For the other providers, we default to v2 signer, as a lot of
            # them don't support v4 (e.g. Google)
            boto_config = boto3.session.Config(s3={'addressing_style': 'path'})
            if self._google():
                boto_config.user_agent = "%s %s" % (
                    self.GOOGLE_UA_STRING, boto_session._session.user_agent())

        def boto_client_factory():
            s3_client = boto_session.client('s3',
                                            endpoint_url=self.endpoint,
                                            config=boto_config)
            # Remove the Content-MD5 computation as we will supply the MD5
            # header ourselves
            s3_client.meta.events.unregister('before-call.s3.PutObject',
                                             conditionally_calculate_md5)
            s3_client.meta.events.unregister('before-call.s3.UploadPart',
                                             conditionally_calculate_md5)
            if self._google():
                s3_client.meta.events.unregister(
                    'before-parameter-build.s3.ListObjects',
                    set_list_objects_encoding_type_url)
            return s3_client
        return boto_client_factory

    def upload_object(self, swift_key, storage_policy_index, internal_client):
        s3_key = self.get_s3_name(swift_key)
        try:
            with self.client_pool.get_client() as s3_client:
                s3_meta = s3_client.head_object(Bucket=self.aws_bucket,
                                                Key=s3_key)
        except botocore.exceptions.ClientError as e:
            resp_meta = e.response.get('ResponseMetadata', {})
            if resp_meta.get('HTTPStatusCode', 0) == 404:
                s3_meta = None
            else:
                raise e
        swift_req_hdrs = {
            'X-Backend-Storage-Policy-Index': storage_policy_index,
            'X-Newest': True
        }

        try:
            metadata = internal_client.get_object_metadata(
                self.account, self.container, swift_key,
                headers=swift_req_hdrs)
        except UnexpectedResponse as e:
            if '404 Not Found' in e.message:
                return
            raise

        self.logger.debug("Metadata: %s" % str(metadata))
        if check_slo(metadata):
            self.upload_slo(swift_key, storage_policy_index, s3_meta,
                            internal_client)
            return

        if s3_meta and self.check_etag(metadata['etag'], s3_meta['ETag']):
            if self.is_object_meta_synced(s3_meta, metadata):
                return
            elif not self.in_glacier(s3_meta):
                self.update_metadata(swift_key, metadata)
                return

        with self.client_pool.get_client() as s3_client:
            wrapper_stream = FileWrapper(internal_client,
                                         self.account,
                                         self.container,
                                         swift_key,
                                         swift_req_hdrs)
            self.logger.debug('Uploading %s with meta: %r' % (
                s3_key, wrapper_stream.get_s3_headers()))

            params = dict(
                Bucket=self.aws_bucket,
                Key=s3_key,
                Body=wrapper_stream,
                Metadata=wrapper_stream.get_s3_headers(),
                ContentLength=len(wrapper_stream),
                ContentType=metadata['content-type']
            )
            if self._is_amazon() and self.encryption:
                params['ServerSideEncryption'] = 'AES256'
            s3_client.put_object(**params)

    def _delete_not_found(self, s3_key):
        '''Deletes the object and ignores the 404 Not Found error.'''
        with self.client_pool.get_client() as s3_client:
            try:
                s3_client.delete_object(Bucket=self.aws_bucket, Key=s3_key)
            except botocore.exceptions.ClientError as e:
                resp_meta = e.response.get('ResponseMetadata', {})
                if resp_meta.get('HTTPStatusCode', 0) == 404:
                    self.logger.warning('%s already removed from %s' % (
                        s3_key, self.aws_bucket))
                else:
                    raise

    def delete_object(self, swift_key, internal_client=None):
        s3_key = self.get_s3_name(swift_key)
        self.logger.debug('Deleting object %s' % s3_key)
        self._delete_not_found(s3_key)
        # If there is a manifest uploaded for this object, remove it as well
        self._delete_not_found(self.get_manifest_name(s3_key))

    def shunt_object(self, req, swift_key):
        """Fetch an object from the remote cluster to stream back to a client.

        :returns: (status, headers, body_iter) tuple
        """
        if req.method not in ('GET', 'HEAD'):
            # sanity check
            raise ValueError('Expected GET or HEAD, not %s' % req.method)

        headers_to_copy = ('Range', 'If-Match', 'If-None-Match',
                           'If-Modified-Since', 'If-Unmodified-Since')
        kwargs = {}
        kwargs.update({
            header.replace('-', ''): req.headers[header]
            for header in headers_to_copy if header in req.headers})
        if req.method == 'GET':
            response = self.get_object(swift_key, **kwargs)
        else:
            response = self.head_object(swift_key, **kwargs)

        if not response.success:
            return response.to_wsgi()

        # Previously, we did not set the x-static-large-object header.
        # Infer whether it should be set from the ETag (MPUs have a
        # trailing -[0-9]+ (e.g. deadbeef-5).
        if re.match('[0-9a-z]+-\d+$', response.headers.get('etag', '')):
            if SLO_HEADER not in response.headers:
                response.headers[SLO_HEADER] = True

        return response.to_wsgi()

    def head_object(self, key, bucket=None, **options):
        if not self.settings.get('native'):
            key = self.get_s3_name(key)
        if bucket is None:
            bucket = self.aws_bucket
        response = self._call_boto(
            'head_object', Bucket=bucket, Key=key, **options)
        response.body = ['']
        return response

    def get_object(self, key, bucket=None, **options):
        if not self.settings.get('native'):
            key = self.get_s3_name(key)
        if bucket is None:
            bucket = self.aws_bucket
        return self._call_boto(
            'get_object', Bucket=bucket, Key=key, **options)

    def head_bucket(self, bucket=None, **options):
        if not bucket:
            bucket = self.aws_bucket
        return self._call_boto('head_bucket', bucket, None, **options)

    def list_buckets(self):
        resp = self._call_boto('list_buckets')
        resp.body = [
            {'last_modified': bucket['CreationDate'],
             'count': 0,
             'bytes': 0,
             'name': bucket['Name']} for bucket in resp.body['Buckets']]
        return resp

    def _call_boto(self, op, **args):
        def _perform_op(s3_client):
            try:
                resp = getattr(s3_client, op)(**args)
                body = resp.get('Body', [''])
                return ProviderResponse(
                    True, resp['ResponseMetadata']['HTTPStatusCode'],
                    convert_to_swift_headers(
                        resp['ResponseMetadata']['HTTPHeaders']),
                    body)
            except botocore.exceptions.ClientError as e:
                return ProviderResponse(
                    False, e.response['ResponseMetadata']['HTTPStatusCode'],
                    convert_to_swift_headers(
                        e.response['ResponseMetadata']['HTTPHeaders']),
                    iter(e.response['Error']['Message']))
            except Exception:
                self.logger.exception('Error contacting remote s3 cluster')
                return ProviderResponse(False, 502, {}, iter('Bad Gateway'))

        if op == 'get_object':
            entry = self.client_pool.get_client()
            resp = _perform_op(entry.client)
            if resp.success:
                resp.body = ClosingResourceIterable(
                    entry,
                    resp.body,
                    length=int(resp.headers['Content-Length']))
            else:
                resp.body = ClosingResourceIterable(
                    entry, resp.body, lambda: None)
            return resp
        else:
            with self.client_pool.get_client() as s3_client:
                return _perform_op(s3_client)

    def list_objects(self, marker, limit, prefix, delimiter=None):
        if limit > 1000:
            limit = 1000
        args = dict(Bucket=self.aws_bucket)
        args['MaxKeys'] = limit
        if prefix is None:
            prefix = ''
        try:
            with self.client_pool.get_client() as s3_client:
                if self.settings.get('native'):
                    key_prefix = ''
                else:
                    key_prefix = '%s/%s/%s/' % (
                        self.get_prefix(), self.account, self.container)
                prefix = '%s%s' % (key_prefix, prefix.decode('utf-8'))
                if prefix:
                    args['Prefix'] = prefix
                # Works around an S3 proxy bug where empty-string prefix and
                # delimiter still results in a listing with CommonPrefixes and
                # no objects
                if marker:
                    args['Marker'] = '%s%s' % (
                        key_prefix, marker.decode('utf-8'))
                if delimiter:
                    args['Delimiter'] = delimiter.decode('utf-8')
                resp = s3_client.list_objects(**args)
                # s3proxy does not include the ETag information when used with
                # the filesystem provider
                key_offset = len(key_prefix)
                location_prefix = self.endpoint
                if self._google():
                    location_prefix = 'Google Cloud Storage'
                elif not location_prefix:
                    location_prefix = 'AWS S3'
                location_parts = [location_prefix, self.aws_bucket]
                if key_prefix:
                    location_parts.append(key_prefix)
                content_location = ';'.join(location_parts)
                keys = [dict(hash=row.get('ETag', '').replace('"', ''),
                             name=urllib.unquote(row['Key'])[key_offset:],
                             last_modified=row['LastModified'].strftime(
                             SWIFT_TIME_FMT),
                             bytes=row['Size'],
                             content_location=content_location,
                             # S3 does not include content-type in listings
                             content_type='application/octet-stream')
                        for row in resp.get('Contents', [])]
                prefixes = [
                    dict(subdir=urllib.unquote(row['Prefix'])[key_offset:],
                         content_location=content_location)
                    for row in resp.get('CommonPrefixes', [])]
                return (200, sorted(
                    keys + prefixes,
                    key=lambda x: x['name'] if 'name' in x else x['subdir']))
        except botocore.exceptions.ClientError as e:
            return (e.response['Error']['Code'], e.message)

    def upload_slo(self, swift_key, storage_policy_index, s3_meta,
                   internal_client):
        # Converts an SLO into a multipart upload. We use the segments as
        # is, for the part sizes.
        # NOTE: If the SLO segment is < 5MB and is not the last segment, the
        # UploadPart call will fail. We need to stitch segments together in
        # that case.
        #
        # For Google Cloud Storage, we will convert the SLO into a single
        # object put, assuming the SLO is < 5TB. If the SLO is > 5TB, we have
        # to fail the upload. With GCS _compose_, we could support larger
        # objects, but defer this work for the time being.
        swift_req_hdrs = {
            'X-Backend-Storage-Policy-Index': storage_policy_index,
            'X-Newest': True
        }
        status, headers, body = internal_client.get_object(
            self.account, self.container, swift_key, headers=swift_req_hdrs)
        if status != 200:
            body.close()
            raise RuntimeError('Failed to get the manifest')
        manifest = json.load(FileLikeIter(body))
        body.close()
        self.logger.debug("JSON manifest: %s" % str(manifest))
        s3_key = self.get_s3_name(swift_key)

        if not self._validate_slo_manifest(manifest):
            # We do not raise an exception here -- we should not retry these
            # errors and they will be logged.
            # TODO: When we report statistics, we need to account for permanent
            # failures.
            self.logger.error('Failed to validate the SLO manifest for %s' %
                              self._full_name(swift_key))
            return

        if self._google():
            if s3_meta:
                slo_etag = s3_meta['Metadata'].get(SLO_ETAG_FIELD, None)
                if slo_etag == headers['etag']:
                    if self.is_object_meta_synced(s3_meta, headers):
                        return
                    self.update_metadata(swift_key, headers)
                    return
            self._upload_google_slo(manifest, headers, s3_key, swift_req_hdrs,
                                    internal_client)
        else:
            expected_etag = get_slo_etag(manifest)

            if s3_meta and self.check_etag(expected_etag, s3_meta['ETag']):
                if self.is_object_meta_synced(s3_meta, headers):
                    return
                elif not self.in_glacier(s3_meta):
                    self.update_slo_metadata(headers, manifest, s3_key,
                                             swift_req_hdrs, internal_client)
                    return
            self._upload_slo(manifest, headers, s3_key, swift_req_hdrs,
                             internal_client)

        with self.client_pool.get_client() as s3_client:
            # We upload the manifest so that we can restore the object in
            # Swift and have it match the S3 multipart ETag. To avoid name
            # length issues, we hash the object name and append the suffix
            params = dict(
                Bucket=self.aws_bucket,
                Key=self.get_manifest_name(s3_key),
                Body=json.dumps(manifest),
                ContentLength=len(json.dumps(manifest)),
                ContentType='application/json')
            if self._is_amazon() and self.encryption:
                params['ServerSideEncryption'] = 'AES256'
            s3_client.put_object(**params)

    def _upload_google_slo(self, manifest, metadata, s3_key, req_hdrs,
                           internal_client):

        with self.client_pool.get_client() as s3_client:
            slo_wrapper = SLOFileWrapper(
                internal_client, self.account, manifest, metadata, req_hdrs)
            s3_client.put_object(Bucket=self.aws_bucket,
                                 Key=s3_key,
                                 Body=slo_wrapper,
                                 Metadata=slo_wrapper.get_s3_headers(),
                                 ContentLength=len(slo_wrapper),
                                 ContentType=metadata['content-type'])

    def _validate_slo_manifest(self, manifest):
        parts = len(manifest)
        if parts > self.MAX_PARTS:
            self.logger.error('Cannot upload a manifest with more than %d '
                              'segments. ' % self.MAX_PARTS)
            return False

        for index, segment in enumerate(manifest):
            if 'bytes' not in segment or 'hash' not in segment:
                # Should never happen
                self.logger.error('SLO segment %s must include size and etag' %
                                  segment['name'])
                return False
            size = int(segment['bytes'])
            if size < self.MIN_PART_SIZE and index < parts - 1:
                self.logger.error('SLO segment %s must be greater than %d MB' %
                                  (segment['name'],
                                   self.MIN_PART_SIZE / self.MB))
                return False
            if size > self.MAX_PART_SIZE:
                self.logger.error('SLO segment %s must be smaller than %d GB' %
                                  (segment['name'],
                                   self.MAX_PART_SIZE / self.GB))
                return False
            if 'range' in segment:
                self.logger.error('Found unsupported "range" parameter for %s '
                                  'segment ' % segment['name'])
                return False
        return True

    def _upload_slo(self, manifest, object_meta, s3_key, req_headers,
                    internal_client):
        with self.client_pool.get_client() as s3_client:
            params = dict(
                Bucket=self.aws_bucket,
                Key=s3_key,
                Metadata=convert_to_s3_headers(object_meta),
                ContentType=object_meta['content-type']
            )
            if self._is_amazon() and self.encryption:
                params['ServerSideEncryption'] = 'AES256'
            multipart_resp = s3_client.create_multipart_upload(**params)
        upload_id = multipart_resp['UploadId']

        work_queue = eventlet.queue.Queue(self.SLO_QUEUE_SIZE)
        worker_pool = eventlet.greenpool.GreenPool(self.SLO_WORKERS)
        workers = []
        for _ in range(0, self.SLO_WORKERS):
            workers.append(
                worker_pool.spawn(self._upload_part_worker, upload_id, s3_key,
                                  req_headers, work_queue, len(manifest),
                                  internal_client))
        for segment_number, segment in enumerate(manifest):
            work_queue.put((segment_number + 1, segment))

        work_queue.join()
        for _ in range(0, self.SLO_WORKERS):
            work_queue.put(None)

        errors = []
        for thread in workers:
            errors += thread.wait()

        # TODO: errors list contains the failed part numbers. We should retry
        # those parts on failure.
        if errors:
            self._abort_upload(s3_key, upload_id)
            raise RuntimeError('Failed to upload an SLO as %s' % s3_key)

        with self.client_pool.get_client() as s3_client:
            # TODO: Validate the response ETag
            try:
                s3_client.complete_multipart_upload(
                    Bucket=self.aws_bucket,
                    Key=s3_key,
                    MultipartUpload={'Parts': [
                        {'PartNumber': number + 1,
                         'ETag': segment['hash']}
                        for number, segment in enumerate(manifest)]
                    },
                    UploadId=upload_id)
            except:
                self._abort_upload(s3_key, upload_id, client=s3_client)
                raise

    def _abort_upload(self, s3_key, upload_id, client=None):
        if not client:
            with self.client_pool.get_client() as s3_client:
                s3_client.abort_multipart_upload(
                    Bucket=self.aws_bucket, Key=s3_key, UploadId=upload_id)
        else:
            s3_client.abort_multipart_upload(
                Bucket=self.aws_bucket, Key=s3_key, UploadId=upload_id)

    def _upload_part_worker(self, upload_id, s3_key, req_headers, queue,
                            part_count, internal_client):
        errors = []
        while True:
            work = queue.get()
            if not work:
                queue.task_done()
                return errors

            try:
                part_number, segment = work
                container, obj = segment['name'].split('/', 2)[1:]

                with self.client_pool.get_client() as s3_client:
                    self.logger.debug('Uploading part %d from %s: %s bytes' % (
                        part_number, self.account + segment['name'],
                        segment['bytes']))
                    wrapper = FileWrapper(internal_client, self.account,
                                          container, obj, req_headers)
                    resp = s3_client.upload_part(
                        Bucket=self.aws_bucket,
                        Body=wrapper,
                        Key=s3_key,
                        ContentLength=len(wrapper),
                        UploadId=upload_id,
                        PartNumber=part_number)
                    if not self.check_etag(segment['hash'], resp['ETag']):
                        self.logger.error('Part %d ETag mismatch (%s): %s %s' %
                                          (part_number,
                                           self.account + segment['name'],
                                           segment['hash'], resp['ETag']))
                        errors.append(part_number)
            except:
                self.logger.error('Failed to upload part %d for %s: %s' % (
                    part_number, self.account + segment['name'],
                    traceback.format_exc()))
                errors.append(part_number)
            finally:
                queue.task_done()

    def get_prefix(self):
        md5_hash = hashlib.md5('%s/%s' % (
            self.account.encode('utf-8'),
            self.container.encode('utf-8'))).hexdigest()
        # strip off 0x and L
        return hex(long(md5_hash, 16) % self.PREFIX_SPACE)[2:-1]

    def get_s3_name(self, key):
        return u'%s/%s' % (self.get_prefix(), self._full_name(key))

    def get_manifest_name(self, s3_name):
        # Split 3 times, as the format is:
        # <prefix>/<account>/<container>/<object>
        prefix, account, container, obj = s3_name.split('/', 3)
        obj_hash = hashlib.sha256(obj.encode('utf-8')).hexdigest()
        return u'/'.join([
            prefix, '.manifests', account, container,
            '%s%s' % (obj_hash, self.SLO_MANIFEST_SUFFIX)])

    def get_manifest(self, key, bucket=None):
        if bucket is None:
            bucket = self.aws_bucket
        with self.client_pool.get_client() as s3_client:
            try:
                resp = s3_client.get_object(
                    Bucket=bucket,
                    Key=self.get_manifest_name(self.get_s3_name(key)))
                return json.load(resp['Body'])
            except Exception as e:
                self.logger.warning(
                    'Failed to fetch the manifest: %s' % e)
                return None

    def update_slo_metadata(self, swift_meta, manifest, s3_key, req_headers,
                            internal_client):
        # For large objects, we should use the multipart copy, which means
        # creating a new multipart upload, with copy-parts
        # NOTE: if we ever stich MPU objects, we need to replicate the
        # stitching calculation to get the offset correctly.
        with self.client_pool.get_client() as s3_client:
            params = dict(
                Bucket=self.aws_bucket,
                Key=s3_key,
                Metadata=convert_to_s3_headers(swift_meta),
                ContentType=swift_meta['content-type']
            )
            if self._is_amazon() and self.encryption:
                params['ServerSideEncryption'] = 'AES256'
            multipart_resp = s3_client.create_multipart_upload(**params)

            # The original manifest must match the MPU parts to ensure that
            # ETags match
            offset = 0
            for part_number, segment in enumerate(manifest):
                container, obj = segment['name'].split('/', 2)[1:]
                segment_meta = internal_client.get_object_metadata(
                    self.account, container, obj, headers=req_headers)
                length = int(segment_meta['content-length'])
                resp = s3_client.upload_part_copy(
                    Bucket=self.aws_bucket,
                    CopySource={'Bucket': self.aws_bucket, 'Key': s3_key},
                    CopySourceRange='bytes=%d-%d' % (offset,
                                                     offset + length - 1),
                    Key=s3_key,
                    PartNumber=part_number + 1,
                    UploadId=multipart_resp['UploadId'])
                s3_etag = resp['CopyPartResult']['ETag']
                if not self.check_etag(segment['hash'], s3_etag):
                    raise RuntimeError('Part %d ETag mismatch (%s): %s %s' % (
                                       part_number + 1,
                                       self.account + segment['name'],
                                       segment['hash'], resp['ETag']))
                offset += length

            s3_client.complete_multipart_upload(
                Bucket=self.aws_bucket,
                Key=s3_key,
                MultipartUpload={'Parts': [
                    {'PartNumber': number + 1,
                     'ETag': segment['hash']}
                    for number, segment in enumerate(manifest)]
                },
                UploadId=multipart_resp['UploadId'])

    def update_metadata(self, swift_key, swift_meta):
        s3_key = self.get_s3_name(swift_key)
        self.logger.debug('Updating metadata for %s to %r' % (
            s3_key, convert_to_s3_headers(swift_meta)))
        with self.client_pool.get_client() as s3_client:
            if not check_slo(swift_meta) or self._google():
                meta = convert_to_s3_headers(swift_meta)
                if self._google() and check_slo(swift_meta):
                    meta[SLO_ETAG_FIELD] = swift_meta['etag']
                params = dict(
                    CopySource={'Bucket': self.aws_bucket,
                                'Key': s3_key},
                    MetadataDirective='REPLACE',
                    Metadata=meta,
                    Bucket=self.aws_bucket,
                    Key=s3_key,
                    ContentType=swift_meta['content-type']
                )
                if self._is_amazon() and self.encryption:
                    params['ServerSideEncryption'] = 'AES256'
                s3_client.copy_object(**params)

    @staticmethod
    def check_etag(swift_etag, s3_etag):
        # S3 ETags are enclosed in ""
        return s3_etag == '"%s"' % swift_etag

    @staticmethod
    def in_glacier(s3_meta):
        if 'StorageClass' in s3_meta and s3_meta['StorageClass'] == 'GLACIER':
            return True
        return False

    @staticmethod
    def is_object_meta_synced(s3_meta, swift_meta):
        swift_keys = set([key.lower()[len(SWIFT_USER_META_PREFIX):]
                          for key in swift_meta
                          if key.lower().startswith(SWIFT_USER_META_PREFIX)])
        s3_keys = set([key.lower()
                       for key in s3_meta['Metadata'].keys()
                       if key != SLO_HEADER])
        if SLO_HEADER in swift_meta:
            if swift_meta[SLO_HEADER] != s3_meta['Metadata'].get(SLO_HEADER):
                return False
            if SLO_ETAG_FIELD in s3_keys:
                # We include the SLO ETag for Google SLO uploads for content
                # verification
                s3_keys.remove(SLO_ETAG_FIELD)
        if swift_keys != s3_keys:
            return False
        for key in s3_keys:
            swift_value = urllib.quote(
                swift_meta[SWIFT_USER_META_PREFIX + key])
            if s3_meta['Metadata'][key] != swift_value:
                return False
        if s3_meta['ContentType'] != swift_meta['content-type']:
            return False
        return True
