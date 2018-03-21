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

import eventlet
import hashlib
import json
import StringIO
import urllib

from swift.common.swob import Request
from swift.common.utils import FileLikeIter, close_if_possible


SWIFT_USER_META_PREFIX = 'x-object-meta-'
S3_USER_META_PREFIX = 'x-amz-meta-'
MANIFEST_HEADER = 'x-object-manifest'
SLO_HEADER = 'x-static-large-object'
SLO_ETAG_FIELD = 'swift-slo-etag'
SWIFT_TIME_FMT = '%Y-%m-%dT%H:%M:%S.%f'
# Blacklist of known hop-by-hop headers taken from
# https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers
HOP_BY_HOP_HEADERS = set([
    'connection',
    'keep-alive',
    'proxy-authenticate',
    'proxy-authorization',
    'te',
    'trailer',
    'transfer-encoding',
    'upgrade',
])


class FileWrapper(object):
    def __init__(self, swift_client, account, container, key, headers={}):
        self._swift = swift_client
        self._account = account
        self._container = container
        self._key = key
        self.swift_req_hdrs = headers
        self._bytes_read = 0
        self.open_object_stream()

    def open_object_stream(self):
        status, self._headers, body = self._swift.get_object(
            self._account, self._container, self._key,
            headers=self.swift_req_hdrs)
        if status != 200:
            raise RuntimeError('Failed to get the object')
        self._bytes_read = 0
        self._swift_stream = body
        self._iter = FileLikeIter(body)
        self._s3_headers = convert_to_s3_headers(self._headers)

    def tell(self):
        return self._bytes_read

    def seek(self, pos, flag=0):
        if pos != 0:
            raise RuntimeError('Arbitrary seeks are not supported')
        if self._bytes_read == 0:
            return
        self._swift_stream.close()
        self.open_object_stream()

    def reset(self, *args, **kwargs):
        self.seek(0)

    def read(self, size=-1):
        if self._bytes_read == self.__len__():
            return ''

        data = self._iter.read(size)
        self._bytes_read += len(data)
        # TODO: we do not need to read an extra byte after
        # https://review.openstack.org/#/c/363199/ is released
        if self._bytes_read == self.__len__():
            self._iter.read(1)
            self._swift_stream.close()
        return data

    def __len__(self):
        if 'Content-Length' not in self._headers:
            raise RuntimeError('Length is not implemented')
        return int(self._headers['Content-Length'])

    def __iter__(self):
        return self._iter

    def get_s3_headers(self):
        return self._s3_headers

    def get_headers(self):
        return self._headers

    def close(self):
        self._swift_stream.close()


class SLOFileWrapper(object):

    # For Google Cloud Storage, we convert SLO to a single object. We can't do
    # that easily with InternalClient, as it does not allow query parameters.
    # This means that if we turn on SLO in the pipeline, we will not be able to
    # retrieve the manifest object itself. In the future, this may be converted
    # to a resumable upload or we may resort to using compose.
    #
    # For the headers, we must also attach the Swift manifest ETag, as we have
    # no way of verifying the object has been uploaded otherwise.
    def __init__(self, swift_client, account, manifest, manifest_meta,
                 headers={}):
        self._swift = swift_client
        self._manifest = manifest
        self._account = account
        self._swift_req_headers = headers
        self._s3_headers = convert_to_s3_headers(manifest_meta)
        self._s3_headers[SLO_ETAG_FIELD] = manifest_meta['etag']
        self._segment = None
        self._segment_index = 0
        self._size = sum([int(segment['bytes']) for segment in self._manifest])

    def seek(self, pos, flag=0):
        if pos != 0:
            raise RuntimeError('Arbitrary seeks are not supported')
        if not self._segment:
            return
        self._segment.close()
        self._segment = None
        self._segment_index = 0

    def reset(self, *args, **kwargs):
        self.seek(0)

    def _open_next_segment(self):
        segment = self._manifest[self._segment_index]
        container, key = segment['name'].split('/', 2)[1:]
        self._segment = FileWrapper(self._swift, self._account, container,
                                    key, self._swift_req_headers)
        self._segment_index += 1

    def read(self, size=-1):
        if not self._segment:
            self._open_next_segment()
        data = self._segment.read(size)
        if not data:
            if self._segment_index < len(self._manifest):
                self._open_next_segment()
                data = self._segment.read(size)
        return data

    def next(self):
        data = self.read()
        if not data:
            raise StopIteration()

    def __iter__(self):
        return self

    def __len__(self):
        return self._size

    def get_s3_headers(self):
        return self._s3_headers


class BlobstorePutWrapper(object):
    def __init__(self, chunk_size, chunk_queue):
        self.chunk_size = chunk_size
        self.queue = chunk_queue

        self.chunk = None
        self.chunk_offset = 0
        self.closed = False

    def read(self, size=-1):
        if self.closed:
            return ''
        if size == -1 or size > self.chunk_size:
            size = self.chunk_size
        resp = ''
        while size:
            if size < 0:
                raise RuntimeError('Negative chunk size')
            if self.chunk == '':
                self.closed = True
                break
            if not self.chunk or self.chunk_offset == len(self.chunk):
                self.chunk = self.queue.get()
                self.chunk_offset = 0

            read_sz = min(size, len(self.chunk) - self.chunk_offset)
            new_offset = self.chunk_offset + read_sz
            resp += self.chunk[self.chunk_offset:new_offset]
            size -= read_sz
            self.chunk_offset = new_offset
        return resp

    def close(self):
        self.closed = True
        if self.chunk:
            self.chunk = None
            self.chunk_offset = 0
        return


class SwiftPutWrapper(object):
    CHUNK_SIZE = 65536

    def __init__(self, body, headers, path, app, logger):
        self.body = body
        self.app = app
        self.headers = headers
        self.path = path
        self.logger = logger
        self.queue = eventlet.queue.Queue(maxsize=15)
        self.put_wrapper = BlobstorePutWrapper(self.CHUNK_SIZE, self.queue)
        self.put_thread = eventlet.greenthread.spawn(
            self._create_put_request().get_response, self.app)

    def _create_put_request(self):
        env = {'REQUEST_METHOD': 'PUT',
               'wsgi.input': self.put_wrapper}
        return Request.blank(
            self.path,
            environ=env,
            headers=self.headers)

    def _read_chunk(self, size):
        if size == -1 or size > self.CHUNK_SIZE:
            size = self.CHUNK_SIZE
        if hasattr(self.body, 'read'):
            chunk = self.body.read(size)
        else:
            try:
                chunk = next(self.body)
            except StopIteration:
                chunk = ''
        return chunk

    def _wait_for_put(self):
        resp = self.put_thread.wait()
        if not resp.is_success and self.logger:
            self.logger.warning(
                'Failed to restore the object: %d' % resp.status)
        close_if_possible(resp.app_iter)
        return resp

    def read(self, size=-1):
        chunk = self._read_chunk(size)
        self.queue.put(chunk)
        if not chunk:
            self._wait_for_put()
        return chunk

    def __iter__(self):
        return self

    def next(self):
        chunk = self.read(self.CHUNK_SIZE)
        if not chunk:
            raise StopIteration
        return chunk


class SwiftSloPutWrapper(SwiftPutWrapper):
    def __init__(self, body, headers, path, app, manifest, logger):
        self.manifest = manifest
        self.segment_index = 0
        self.remainder = self.manifest[0]['bytes']
        self.failed = False

        super(SwiftSloPutWrapper, self).__init__(
            body, headers, path, app, logger)

    def _create_request_path(self, target):
        # The path is /<version>/<account>/<container>/<object>. We strip off
        # the container and object from the path.
        parts = self.path.split('/', 3)[:3]
        # [1:] strips off the leading "/" that manifest names include
        parts.append(target)
        return '/'.join(parts)

    def _ensure_segments_container(self):
        env = {'REQUEST_METHOD': 'PUT'}
        segment_path = self.manifest[self.segment_index]['name']
        container_path = segment_path.split('/', 2)[1]
        req = Request.blank(
            # The manifest path is /<container>/<object>
            self._create_request_path(container_path),
            environ=env)
        resp = req.get_response(self.app)
        if not resp.is_success:
            self.failed = True
            if self.logger:
                self.logger.warning(
                    'Failed to create the segment container %s: %s' % (
                        container_path, resp.status))
        close_if_possible(resp.app_iter)

    def _create_put_request(self):
        self._ensure_segments_container()
        env = {'REQUEST_METHOD': 'PUT',
               'wsgi.input': self.put_wrapper,
               'CONTENT_LENGTH': self.manifest[self.segment_index]['bytes']}
        return Request.blank(
            self._create_request_path(
                self.manifest[self.segment_index]['name'][1:]),
            environ=env)

    def _upload_manifest(self):
        SLO_FIELD_MAP = {
            'bytes': 'size_bytes',
            'hash': 'etag',
            'name': 'path',
            'range': 'range'
        }

        env = {}
        env['REQUEST_METHOD'] = 'PUT'
        # We have to transform the SLO fields, as Swift internally uses a
        # different representation from what the client submits. Unfortunately,
        # when we extract the manifest with the InternalClient, we don't have
        # SLO in the pipeline and retrieve the internal represenation.
        put_manifest = [
            dict([(SLO_FIELD_MAP[k], v) for k, v in entry.items()
                  if k in SLO_FIELD_MAP])
            for entry in self.manifest]

        content = json.dumps(put_manifest)
        env['wsgi.input'] = StringIO.StringIO(content)
        env['CONTENT_LENGTH'] = len(content)
        env['QUERY_STRING'] = 'multipart-manifest=put'
        # The SLO header must not be set on manifest PUT and we should remove
        # the content length of the whole SLO, as we will overwrite it with the
        # length of the manifest itself.
        if SLO_HEADER in self.headers:
            del self.headers[SLO_HEADER]
        del self.headers['Content-Length']
        req = Request.blank(self.path, environ=env, headers=self.headers)
        resp = req.get_response(self.app)
        if self.logger:
            if resp.status_int == 202:
                self.logger.warning(
                    'SLO %s possibly already overwritten' % self.path)
            elif not resp.is_success:
                self.logger.warning('Failed to create the manifest %s: %s' % (
                    self.path, resp.status))
        close_if_possible(resp.app_iter)

    def read(self, size=-1):
        chunk = self._read_chunk(size)
        # On failure, we pass through the data and abort the attempt to restore
        # into the object store.
        if self.failed:
            return chunk

        if self.remainder - len(chunk) >= 0:
            self.remainder -= len(chunk)
            self.queue.put(chunk)
        else:
            if self.remainder > 0:
                self.queue.put(chunk[:self.remainder])
            self.queue.put('')
            resp = self._wait_for_put()
            if not resp.is_success:
                if self.logger:
                    self.logger.warning(
                        'Failed to restore segment %s: %s' % (
                            self.manifest[self.segment_index]['name'],
                            resp.status))
                self.failed = True
                return chunk

            self.segment_index += 1
            self.put_wrapper = BlobstorePutWrapper(self.CHUNK_SIZE, self.queue)
            self.put_thread = eventlet.greenthread.spawn(
                self._create_put_request().get_response, self.app)
            self.queue.put(chunk[self.remainder:])
            segment_length = self.manifest[self.segment_index]['bytes']
            self.remainder = segment_length - (len(chunk) - self.remainder)

        if not chunk:
            self._wait_for_put()
            # Upload the manifest
            self._upload_manifest()
        return chunk


class ClosingResourceIterable(object):
    """
        Wrapper to ensure the resource is returned back to the pool after the
        data is consumed.
    """
    def __init__(self, resource, data_src, close_callable=None,
                 read_chunk=65536, length=None):
        self.closed = False
        self.exhausted = False
        self.data_src = data_src
        self.read_chunk = read_chunk
        self.resource = resource
        self.length = length
        self.amt_read = 0

        if close_callable:
            self.close_data = close_callable
        elif hasattr(self.data_src, 'close'):
            self.close_data = self.data_src.close
        else:
            raise ValueError('No closeable to close the data source defined')

        try:
            iter(self.data_src)
            self.iterable = True
        except TypeError:
            self.iterable = False

        if not self.iterable and not hasattr(self.data_src, 'read'):
            raise TypeError('Cannot iterate over the data source')
        if not self.iterable and length is None:
            raise ValueError('Must supply length with non-iterable objects')

    def next(self):
        if self.closed:
            raise ValueError()
        try:
            if self.iterable:
                return next(self.data_src)
            else:
                if self.amt_read == self.length:
                    raise StopIteration
                ret = self.data_src.read(self.read_chunk)
                self.amt_read += len(ret)
                if not ret:
                    raise StopIteration
                return ret
        except Exception as e:
            if type(e) != StopIteration:
                # Likely, a partial read
                self.close_data()
            self.closed = True
            self.resource.close()
            self.exhausted = True
            raise

    def close(self):
        if not self.exhausted:
            self.close_data()
        if not self.closed:
            self.closed = True
            self.resource.close()

    def __next__(self):
        return self.next()

    def __iter__(self):
        return self

    def __del__(self):
        """
            There could be an exception raised, the resource is not put back in
            the pool, and the content is not consumed. We handle this in the
            destructor.
        """
        self.close()


def filter_hop_by_hop_headers(headers):
    # Take an iterable of (key, value) tuples and return a list of (key, value)
    # tuples with any hop-by-hop headers removed.
    return [(k, v) for (k, v) in headers
            if k.lower() not in HOP_BY_HOP_HEADERS]


def convert_to_s3_headers(swift_headers):
    s3_headers = {}
    for hdr in swift_headers.keys():
        if hdr.lower().startswith(SWIFT_USER_META_PREFIX):
            s3_header_name = hdr[len(SWIFT_USER_META_PREFIX):].lower()
            s3_headers[s3_header_name] = urllib.quote(swift_headers[hdr])
        elif hdr.lower() == MANIFEST_HEADER:
            s3_headers[MANIFEST_HEADER] = urllib.quote(swift_headers[hdr])
        elif hdr.lower() == SLO_HEADER:
            s3_headers[SLO_HEADER] = urllib.quote(swift_headers[hdr])

    return s3_headers


def convert_to_swift_headers(s3_headers):
    swift_headers = {}
    for header, value in s3_headers.items():
        if header in ('x-amz-id-2', 'x-amz-request-id'):
            swift_headers['Remote-' + header] = value
        elif header.endswith((MANIFEST_HEADER, SLO_HEADER)):
            swift_headers[header[len(S3_USER_META_PREFIX):]] = value
        elif header.startswith(S3_USER_META_PREFIX):
            key = SWIFT_USER_META_PREFIX + header[len(S3_USER_META_PREFIX):]
            swift_headers[key] = value
        elif header == 'content-length':
            # Capitalize, so eventlet doesn't try to add its own
            swift_headers['Content-Length'] = value
        elif header == 'etag':
            # S3 returns ETag in quotes
            swift_headers['etag'] = value[1:-1]
        else:
            swift_headers[header] = value
    return swift_headers


def convert_to_local_headers(headers, remove_timestamp=True):
    put_headers = dict([(k, v) for k, v in headers
                        if not k.startswith('Remote-')])
    if 'etag' in put_headers:
        put_headers['Content-MD5'] = put_headers['etag']
        del put_headers['etag']
    # We must remove the X-Timestamp header, as otherwise objects may
    # never be restored if a tombstone is present (as the remote
    # timestamp may be older than then tombstone). Only happens if
    # restoring from Swift.
    if 'x-timestamp' in put_headers and remove_timestamp:
        del put_headers['x-timestamp']
    return put_headers


def get_slo_etag(manifest):
    etags = [segment['hash'].decode('hex') for segment in manifest]
    md5_hash = hashlib.md5()
    md5_hash.update(''.join(etags))
    return md5_hash.hexdigest() + '-%d' % len(manifest)


def check_slo(swift_meta):
    if SLO_HEADER not in swift_meta:
        return False
    return swift_meta[SLO_HEADER].lower() == 'true'


def response_is_complete(status_code, headers):
    if status_code == 200:
        return True
    if status_code != 206:
        return False
    # Let's see if that Partial Content really is "partial"
    cr = ''
    for header, value in headers:
        if header.lower() == 'content-range':
            cr = value
            break
    if not cr.startswith('bytes 0-'):
        return False
    end, length = cr[8:].partition('/')[::2]
    try:
        end = int(end)
        length = int(length)
    except ValueError:
        return False
    return end + 1 == length
