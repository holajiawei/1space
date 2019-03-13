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

import datetime
import eventlet
import hashlib
import json
import string
import StringIO
import urllib

from email.header import Header, decode_header
from lxml import etree

# Old (prior to 2.11) versions of swift cannot import this, but cloud sync
# cannot be set up for such old clusters. This just allows the cloud shunt to
# do nothing quietly.
try:
    from swift.common.middleware.versioned_writes import (
        SYSMETA_VERSIONS_LOC, SYSMETA_VERSIONS_MODE)
except ImportError:
    try:
        from swift.common.middleware.versioned_writes import (
            VERSIONS_LOC_SYSMETA, VERSIONS_MODE_SYSMETA)
        SYSMETA_VERSIONS_LOC = VERSIONS_LOC_SYSMETA
        SYSMETA_VERSIONS_MODE = VERSIONS_MODE_SYSMETA
    except ImportError:
        SYSMETA_VERSIONS_LOC = None
        SYSMETA_VERSIONS_MODE = None

# Just keeping conditional import in one common place:
try:
    from swift.common.middleware.listing_formats import (
        get_listing_content_type)
except ImportError:
    # compat for swift < 2.16
    from swift.common.request_helpers import get_listing_content_type  # noqa

from swift.common.http import HTTP_NOT_FOUND
from swift.common.request_helpers import (
    get_sys_meta_prefix, get_object_transient_sysmeta)
from swift.common.swob import Request
from swift.common.utils import FileLikeIter, close_if_possible, quote


SWIFT_USER_META_PREFIX = 'x-object-meta-'
S3_USER_META_PREFIX = 'x-amz-meta-'
MANIFEST_HEADER = 'x-object-manifest'
COMBINED_ETAG_FIELD = 'combined-etag'
DLO_ETAG_FIELD = 'swift-source-dlo-etag'
SLO_HEADER = 'x-static-large-object'
SLO_ETAG_FIELD = 'swift-slo-etag'
EPOCH = datetime.datetime.utcfromtimestamp(0)
SWIFT_TIME_FMT = '%Y-%m-%dT%H:%M:%S.%f'
LAST_MODIFIED_FMT = '%a, %d %b %Y %H:%M:%S %Z'
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
PROPAGATED_HDRS = ['x-container-read', 'x-container-write']
MIGRATOR_HEADER = 'multi-cloud-internal-migrator'
SHUNT_BYPASS_HEADER = 'x-cloud-sync-shunt-bypass'
ACCOUNT_ACL_KEY = 'x-account-access-control'
SYSMETA_ACCOUNT_ACL_KEY = \
    get_sys_meta_prefix('account') + 'core-access-control'
PFS_ETAG_PREFIX = 'pfs'
DEFAULT_CHUNK_SIZE = 65536
DEFAULT_SEGMENT_SIZE = 100 * 1024 * 1024
REMOTE_ETAG = get_object_transient_sysmeta(
    'multi-cloud-internal-migrator-remote-etag')


class MigrationContainerStates(object):
    '''Possible states of the migrated containers.

    When a container is migrated, we have to handle the possible state
    transitions it could go through. Initially, a container only exists in the
    source cluster.  Once we create it in the destination cluster, we tag it to
    be in the MIGRATING state. In this state, we process any new objects or
    updated metadata. When an object is removed, we will also remove it from
    the destination container.

    When a container is removed on the source and the destination container is
    in the MIGRATING state, it will be removed.

    When the metadata of the destination container is changed, we will tag the
    container as MODIFIED. When the source container is removed, we will not
    remove the destination container. We will remove any object that still has
    the transient migrator system metadata tag. In the end, we may end up with
    an empty container, but one we will not attempt to remove from the system.

    When an object is PUT on the destination side, it implicitly means the
    container will not be removed when the source container is removed. Our
    attempt to remove the container after clearing all objects with the
    transient metadata will result in a 409, as there will still be objects in
    the container. We do not consider this an error.

    Once we have processed the removal of the source container, we will tag the
    destination container as "SRC_DELETED". This is a flag that informs us that
    we do not need to scan the objects in the container again.

    If the source container exists, but the destination container is in the
    SRC_DELETED state, we will update our system metadata tag to be in the
    MODIFIED state, meaning the container will not be removed (but we will
    rescan the container and remove objects if the source container is
    removed).
    '''
    MIGRATING = 'migrating'
    MODIFIED = 'modified'
    SRC_DELETED = 'src_deleted'


class RemoteHTTPError(Exception):
    def __init__(self, resp, *args, **kwargs):
        self.resp = resp
        super(RemoteHTTPError, self).__init__(*args, **kwargs)

    def __unicode__(self):
        return u'Error (%d): %s' % (self.resp.status, self.resp.body)


class IterableFromFileLike(object):
    def __init__(self, filelike):
        self.filelike = filelike

    def next(self):
        got = self.filelike.read(DEFAULT_CHUNK_SIZE)
        if got:
            return got
        raise StopIteration
    __next__ = next

    def __iter__(self):
        return self


class SeekableFileLikeIter(FileLikeIter):
    """
    Like Swift's FileLikeIter, with the following changes in/additions of
    behavior:

    * You can give it an existing file-like object (like Swift's
      InputProxy) and iteration will just successively call that object's
      read() method with some reasonable chunk size.
    * You can optionally specify a length, and reads past that length will
      return data only up to that length, and EOF after that.
    * If a length is specified, len() will work on this object, otherwise
      it will raise a TypeError.
    * If you specify a `seek_zero_cb`, it will be called when a seek to
      position zero (regardless of args), if any data has already been
      read.  The callback must return a new iterator, which will be
      used for subsequent reads.  In other words, the callback must
      actually take some action that allows the data coming out to be
      correct for an actual offset of zero.  Also, without a callback
      specified, any attempt to seek after any data has been read will
      result in a RuntimeError.
    """
    def __init__(self, iterable_or_filelike, length=None, seek_zero_cb=None):
        try:
            super(SeekableFileLikeIter, self).__init__(iterable_or_filelike)
        except TypeError as e:
            if 'is not iterable' in e.message and hasattr(iterable_or_filelike,
                                                          'read'):
                # It's wrappers all the way down!  Wrap the given file-like so
                # it behaves like an iterable so we can make it behave like a
                # file-like according to _our_ interface and extra semantics.
                iterable = IterableFromFileLike(iterable_or_filelike)
                super(SeekableFileLikeIter, self).__init__(iterable)
            else:
                raise

        self.length = length
        self.seek_zero_cb = seek_zero_cb
        self._bytes_delivered = 0  # capped by length, if given

    def tell(self):
        return self._bytes_delivered

    def _length_exceeded(self):
        if self.length is None:
            return False
        bytes_we_can_return = self.length - self.tell()
        return not bytes_we_can_return

    def seek(self, pos, flag=0):
        if pos != 0:
            raise RuntimeError('%s: arbitrary seeks are not supported' %
                               self.__class__.__name__)
        if self._bytes_delivered == 0:
            return
        if self.seek_zero_cb:
            self.iterator = self.seek_zero_cb()
            self.buf = None
            self._bytes_delivered = 0
        else:
            raise RuntimeError('%s: seeking after reading only '
                               'supported if a callback is '
                               'supplied' % self.__class__.__name__)

    def reset(self, *args, **kwargs):
        self.seek(0)

    def _account_data_delivered(self, data):
        if self.length is not None:
            bytes_we_can_return = self.length - self.tell()
            if len(data) > bytes_we_can_return:
                # Keep the buffer in case the application needs to use it
                self.buf = data[bytes_we_can_return:]
                data = data[:bytes_we_can_return]
        self._bytes_delivered += len(data)
        return data

    # This is hoisted in from base class and changed so we can get our
    # bytes-delivered accounting correct in all cases.  Sigh.
    def next(self, called_from_read=False):
        """
        next(x) -> the next value, or raise StopIteration
        """
        if self.closed:
            raise ValueError('I/O operation on closed file')
        if self._length_exceeded():
            raise StopIteration
        if self.buf:
            rv = self.buf
            self.buf = None
        else:
            rv = next(self.iterator)
        if not called_from_read:
            # If we *were* called from read(), then it will do the truncation,
            # if necessary, and accounting.
            rv = self._account_data_delivered(rv)
        return rv
    __next__ = next

    # This is hoisted in from base class and changed so we can get our
    # bytes-delivered accounting correct in all cases.  Sigh.
    def read(self, size=-1):
        """
        read([size]) -> read at most size bytes, returned as a bytes string.

        If the size argument is negative or omitted, read until EOF is reached.
        Notice that when in non-blocking mode, less data than what was
        requested may be returned, even if no size parameter was given.
        """
        if self.closed:
            raise ValueError('I/O operation on closed file')
        if self._length_exceeded():
            return ''
        if size < 0:
            return b''.join(self)
        elif not size:
            chunk = b''
        elif self.buf:
            chunk = self.buf
            self.buf = None
        else:
            try:
                chunk = self.next(called_from_read=True)
            except StopIteration:
                return b''
        if len(chunk) > size:
            self.buf = chunk[size:]
            chunk = chunk[:size]
        return self._account_data_delivered(chunk)

    def __len__(self):
        if self.length is not None:
            return self.length
        raise TypeError("object of type '%s' has no len()" %
                        self.__class__.__name__)


class FileWrapper(SeekableFileLikeIter):
    def __init__(self, swift_client, account, container, key, headers={},
                 stats_cb=None):
        self._swift = swift_client
        self._account = account
        self._container = container
        self._key = key
        self.swift_req_hdrs = headers
        self._stats_cb = stats_cb

        self.iterator = None
        self._swift_stream = None
        self.content_length = None

        self.open_object_stream()  # sets self.content_length & .length

        super(FileWrapper, self).__init__(self._swift_stream,
                                          length=self.content_length,
                                          seek_zero_cb=self.open_object_stream)

    def open_object_stream(self):
        if self._swift_stream:
            self._swift_stream.close()

        status, self._headers, body = self._swift.get_object(
            self._account, self._container, self._key,
            headers=self.swift_req_hdrs)
        if status != 200:
            raise RuntimeError('Failed to get the object')

        self._swift_stream = body
        self._s3_headers = convert_to_s3_headers(self._headers)
        if 'Content-Length' in self._headers:
            self.content_length = self.length = int(
                self._headers['Content-Length'])
        else:
            self.content_length = self.length = None

        return iter(self._swift_stream)

    def next(self, called_from_read=False):
        the_data = super(FileWrapper, self).next(called_from_read)
        _bytes_read = self.tell()
        if called_from_read:
            _bytes_read += len(the_data)
        # TODO: we do not need to read an extra byte once we drop support for
        # swift<2.10.0 (see https://github.com/openstack/swift/commit/66c905e)
        if self.length is not None and _bytes_read == len(self):
            try:
                next(self.iterator)
            except StopIteration:
                pass
        if self._stats_cb:
            self._stats_cb(len(the_data))
        return the_data

    def get_s3_headers(self):
        return self._s3_headers

    def get_headers(self):
        return self._headers

    def close(self):
        if self._swift_stream:
            self._swift_stream.close()
            self._swift_stream = None
        return super(FileWrapper, self).close()


class CombinedFileWrapper(object):
    '''Upload multiple objects as one concatenated object.

    Given a list of objects, the account, internal client, target object
    metadata, and total size, returns a file-like object that can be passed to
    boto3 or swiftclient to upload the objects as a single blob.

    :param swift_client: InternalClient instance.
    :param account: Swift account for the originating objects.
    :param metadata: Metadata headers to be supplied with the request to upload
                     the object.
    :param source_objects: A list of tuples to specify the contents of the
                           object. The tuples are (container, key).
    :param total_size: The total size of the uploaded object.
    :param internal_headers: Optional headers to be used with InternalClient
                             reqeusts.
    :returns: A CombinedFileWrapper instance.
    '''
    def __init__(self, swift_client, account, source_objects, total_size,
                 internal_headers={}, stats_cb=None):
        self._swift = swift_client
        self._source_objects = source_objects
        self._account = account
        self._internal_headers = internal_headers
        self._segment = None
        self._segment_index = 0
        self._size = total_size
        self._combined_etag = hashlib.md5()
        self._segment_etags = []
        self._current_segment_hash = hashlib.md5()
        self._stats_cb = stats_cb

    def seek(self, pos, flag=0):
        if pos != 0:
            raise RuntimeError('Arbitrary seeks are not supported')
        if not self._segment:
            return
        self._segment.close()
        self._segment = None
        self._segment_index = 0
        self._combined_etag = hashlib.md5()
        self._current_segment_hash = hashlib.md5()
        self._segment_etags = []

    def reset(self, *args, **kwargs):
        self.seek(0)

    def _open_next_segment(self):
        container, key = self._source_objects[self._segment_index]
        self._segment = FileWrapper(
            self._swift, self._account, container, key, self._internal_headers,
            self._stats_cb)
        self._segment_index += 1

    def read(self, size=-1):
        if not self._segment:
            self._open_next_segment()
        data = self._segment.read(size)
        if not data:
            self._segment_etags.append(
                self._current_segment_hash.hexdigest())
            self._current_segment_hash = hashlib.md5()
            self._segment.close()
            if self._segment_index < len(self._source_objects):
                self._open_next_segment()
                data = self._segment.read(size)
        self._combined_etag.update(data)
        self._current_segment_hash.update(data)
        return data

    def next(self):
        data = self.read(DEFAULT_CHUNK_SIZE)
        if data:
            return data
        raise StopIteration()

    def __iter__(self):
        return self

    def __len__(self):
        return self._size

    def close(self):
        if self._segment:
            self._segment.close()
        # If we close early or read exactly the available number of bytes, the
        # last segment's ETag is never appended, so we do it on close.
        if len(self._segment_etags) < len(self._source_objects):
            self._segment_etags.append(self._current_segment_hash.hexdigest())

    def etag(self):
        return (self._combined_etag.hexdigest(), self._segment_etags)


class SLOFileWrapper(CombinedFileWrapper):
    # For Google Cloud Storage, we convert SLO to a single object. We can't do
    # that easily with InternalClient, as it does not allow query parameters.
    # This means that if we turn on SLO in the pipeline, we will not be able to
    # retrieve the manifest object itself. In the future, this may be converted
    # to a resumable upload or we may resort to using compose.
    def __init__(self, swift_client, account, manifest, headers={},
                 stats_cb=None):
        size = sum([int(segment['bytes']) for segment in manifest])
        segments = [segment['name'].split('/', 2)[1:] for segment in manifest]
        super(SLOFileWrapper, self).__init__(
            swift_client, account, segments, size, internal_headers=headers,
            stats_cb=stats_cb)


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
    def __init__(self, body, headers, path, app, logger):
        self.body = body
        self.app = app
        self.headers = headers
        self.path = path
        self.logger = logger
        self.queue = eventlet.queue.Queue(maxsize=15)
        self.put_wrapper = BlobstorePutWrapper(DEFAULT_CHUNK_SIZE, self.queue)
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
        if size == -1 or size > DEFAULT_CHUNK_SIZE:
            size = DEFAULT_CHUNK_SIZE
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
                'Failed to restore the object: %s' % resp.status)
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
        chunk = self.read(DEFAULT_CHUNK_SIZE)
        if not chunk:
            raise StopIteration
        return chunk


class BaseSwiftSloPutWrapper(SwiftPutWrapper):
    '''Abstract class for all of the wrappers that create a static large object
    from single objects in the origin.

    Inheriting classes must implement:
        - _get_segment_size()
        - _get_segment_name()
        - _get_segment_etag()
        - _get_manifest()
        - _get_segments_container()

    One method is optional:
        - _update_manifest_headers() -- used for updating the headers before
          uploading the manifest. By default, a NOOP.
    '''
    def __init__(self, body, headers, path, app, logger):
        self._segment_index = 0
        self._failed = False
        self._remainder = self._get_segment_size()
        self._segments_container_checked = False

        super(BaseSwiftSloPutWrapper, self).__init__(
            body, headers, path, app, logger)

    def _get_segment_size(self):
        raise NotImplementedError()

    def _get_segment_name(self):
        raise NotImplementedError()

    def _get_segment_etag(self):
        raise NotImplementedError()

    def _get_manifest(self):
        raise NotImplementedError()

    def _get_segments_container(self):
        raise NotImplementedError()

    def _update_manifest_headers(self, manifest_headers):
        pass

    def _create_request_path(self, container, key=''):
        # The path is /<version>/<account>/<container>/<object>. We strip off
        # the container and object from the path.
        parts = self.path.split('/', 3)[:3]
        # [1:] strips off the leading "/" that manifest names include
        parts.append(container)
        if key:
            parts.append(key)
        return '/'.join(parts)

    def _ensure_segments_container(self):
        if self._segments_container_checked:
            return
        env = {'REQUEST_METHOD': 'PUT'}
        req = Request.blank(
            # The manifest path is /<container>/<object>
            self._create_request_path(self._get_segments_container()),
            environ=env)
        resp = req.get_response(self.app)
        if not resp.is_success:
            self._failed = True
            if self.logger:
                self.logger.warning(
                    'Failed to create the segment container %s: %s' % (
                        self._get_segments_container(), resp.status))
        close_if_possible(resp.app_iter)
        self._segments_container_checked = True

    def _create_put_request(self):
        self._ensure_segments_container()
        env = {'REQUEST_METHOD': 'PUT',
               'wsgi.input': self.put_wrapper,
               'CONTENT_LENGTH': self._get_segment_size()}
        headers = {}
        if self._get_segment_etag():
            headers['ETag'] = self._get_segment_etag()
        return Request.blank(
            self._create_request_path(
                self._get_segments_container(), self._get_segment_name()),
            environ=env,
            headers=headers)

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
        manifest = self._get_manifest()
        put_manifest = [
            dict([(SLO_FIELD_MAP[k], v) for k, v in entry.items()
                  if k in SLO_FIELD_MAP])
            for entry in self._get_manifest()]

        content = json.dumps(put_manifest)
        env['wsgi.input'] = StringIO.StringIO(content)
        env['CONTENT_LENGTH'] = len(content)
        env['QUERY_STRING'] = 'multipart-manifest=put'
        # The SLO header must not be set on manifest PUT and we should remove
        # the content length of the whole SLO, as we will overwrite it with the
        # length of the manifest itself.
        manifest_headers = dict(self.headers)
        if SLO_HEADER in manifest_headers:
            del manifest_headers[SLO_HEADER]
        del manifest_headers['Content-Length']
        etag = hashlib.md5()
        for entry in manifest:
            etag.update(entry['hash'])
        manifest_headers['ETag'] = etag.hexdigest()
        self._update_manifest_headers(manifest_headers)
        req = Request.blank(self.path, environ=env, headers=manifest_headers)
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
        if self._failed:
            return chunk

        # We may have very small segments (at least in tests)
        chunk_offset = 0
        while chunk_offset < len(chunk):
            if self._remainder - (len(chunk) - chunk_offset) >= 0:
                self._remainder -= (len(chunk) - chunk_offset)
                self.queue.put(chunk[chunk_offset:])
                break

            self.queue.put(chunk[chunk_offset:self._remainder + chunk_offset])
            if self._remainder:
                # Otherwise, the empty string has already been PUT
                self.queue.put('')
            resp = self._wait_for_put()
            if not resp.is_success:
                if self.logger:
                    self.logger.warning(
                        'Failed to restore segment %s/%s: %s' % (
                            self._get_segments_container(),
                            self._get_segment_name(),
                            resp.status))
                self._failed = True
            if self._failed:
                break
            chunk_offset += self._remainder

            self._segment_index += 1
            self._remainder = self._get_segment_size()
            if not self._remainder or self._failed:
                # The _get_segment_size() call may have failed
                break

            self.put_wrapper = BlobstorePutWrapper(
                DEFAULT_CHUNK_SIZE, self.queue)
            self.put_thread = eventlet.greenthread.spawn(
                self._create_put_request().get_response, self.app)

        if not chunk:
            self.queue.put(chunk)
            self._wait_for_put()
            # Upload the manifest
            self._upload_manifest()
        return chunk


class SwiftLargeObjectPutWrapper(BaseSwiftSloPutWrapper):
    def __init__(self, body, headers, path, app, logger, segment_size):
        self._segment_size = segment_size
        self._total_size = int(headers['Content-Length'])
        container, key = path.split('/', 4)[3:]
        ts = create_x_timestamp_from_hdrs(headers)
        self._segments_container = container + '_segments'
        self._segment_name_prefix = '/'.join((
            key, str(ts), str(self._total_size), str(self._segment_size)))
        self._key = key
        self._manifest = []
        self.digest = hashlib.md5()

        super(SwiftLargeObjectPutWrapper, self).__init__(
            body, headers, path, app, logger)

    def _get_segment_size(self):
        current_read_size = (self._segment_index + 1) * self._segment_size
        if current_read_size <= self._total_size:
            return self._segment_size
        return self._total_size - current_read_size + self._segment_size

    def _get_segment_name(self):
        return '/'.join((self._segment_name_prefix, str(self._segment_index)))

    def _get_segment_etag(self):
        return None

    def _get_manifest(self):
        return self._manifest

    def _get_segments_container(self):
        return self._segments_container

    def _update_manifest_headers(self, manifest_headers):
        manifest_headers[REMOTE_ETAG] = self.headers['etag']

    def _wait_for_put(self):
        resp = super(SwiftLargeObjectPutWrapper, self)._wait_for_put()
        self._manifest.append(
            {'name': '/'.join(('', self._get_segments_container(),
                               self._get_segment_name())),
             'bytes': self._get_segment_size(),
             'hash': resp.headers['ETag']})
        return resp

    def _read_chunk(self, size=-1):
        chunk = super(SwiftLargeObjectPutWrapper, self)._read_chunk(size)
        self.digest.update(chunk)
        return chunk

    def _upload_manifest(self):
        source_etag = self.headers['etag'].replace('"', '')
        if source_etag != self.digest.hexdigest():
            if self.logger:
                self.logger.warning(
                    'ETag mismatch while restoring %s: %s vs %s' %
                    (self.path, source_etag, self.digest.hexdigest()))
            # TODO: clean up segments!
            return
        super(SwiftLargeObjectPutWrapper, self)._upload_manifest()


class SwiftMPUPutWrapper(SwiftLargeObjectPutWrapper):
    def __init__(self, body, headers, path, app, logger, provider):
        # NOTE: We do a look-up on the first part, as the segment size is used
        # in segments container name
        _, self._key = path.split('/', 4)[3:]
        self._segment_index = 0
        self._segment_meta = {}
        self._provider = provider
        self._load_segment_meta()
        self._parts_etags = []

        super(SwiftMPUPutWrapper, self).__init__(
            body, headers, path, app, logger, self._get_segment_size())

    def _load_segment_meta(self):
        resp = self._provider.head_object(
            self._key, PartNumber=self._segment_index + 1)
        if not resp.success:
            self._failed = True
            return
        self._segment_meta = resp.headers

    def _get_segment_size(self):
        if not self._segment_meta:
            # TODO: consider prefetching parts' metadata
            self._load_segment_meta()
        return int(self._segment_meta.get('Content-Length'))

    def _wait_for_put(self):
        resp = super(SwiftMPUPutWrapper, self)._wait_for_put()
        self._segment_meta = {}
        return resp

    def _upload_manifest(self):
        uploaded_etag = get_slo_etag(self._manifest)
        if uploaded_etag != self.headers['etag']:
            if self.logger:
                self.logger.warning(
                    'ETag mismatch while restoring %s: %s vs %s' % (
                        self.path, self.headers['etag'], uploaded_etag))
            # TODO: clean up segments!
            return
        super(SwiftLargeObjectPutWrapper, self)._upload_manifest()


class SwiftSloPutWrapper(BaseSwiftSloPutWrapper):
    def __init__(self, body, headers, path, app, logger, manifest):
        self._manifest = manifest
        super(SwiftSloPutWrapper, self).__init__(
            body, headers, path, app, logger)

    def _get_segment_size(self):
        return self._manifest[self._segment_index]['bytes']

    def _get_segment_name(self):
        return self._manifest[self._segment_index]['name'].split('/', 2)[2]

    def _get_segment_etag(self):
        return self._manifest[self._segment_index]['hash']

    def _get_manifest(self):
        return self._manifest

    def _get_segments_container(self):
        return self._manifest[self._segment_index]['name'].split('/', 2)[1]


class ClosingResourceIterable(object):
    """
        Wrapper to ensure the resource is returned back to the pool after the
        data is consumed.
    """
    def __init__(self, resource, data_src, close_callable=None,
                 read_chunk=DEFAULT_CHUNK_SIZE, length=None):
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
            self.iterator = iter(self.data_src)
        except TypeError:
            self.iterator = None

        if not self.iterator and not hasattr(self.data_src, 'read'):
            raise TypeError('Cannot iterate over the data source')
        if not self.iterator and length is None:
            raise ValueError('Must supply length with non-iterable objects')

    def next(self):
        if self.closed:
            raise ValueError()
        try:
            if self.iterator:
                return next(self.iterator)
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


def _propagated_hdr(hdr):
    return hdr.startswith('x-container-meta-') or hdr in PROPAGATED_HDRS


def get_container_headers(provider, aws_bucket=None):
    resp = provider.head_bucket(aws_bucket)
    if resp.status != 200:
        raise RemoteHTTPError(resp)

    headers = {}
    for hdr in resp.headers:
        if SYSMETA_VERSIONS_LOC and SYSMETA_VERSIONS_MODE:
            if hdr == 'x-history-location':
                headers[SYSMETA_VERSIONS_LOC] = \
                    resp.headers[hdr].encode('utf8')
                headers[SYSMETA_VERSIONS_MODE] = 'history'
                continue

            if hdr == 'x-versions-location':
                headers[SYSMETA_VERSIONS_LOC] = \
                    resp.headers[hdr].encode('utf8')
                headers[SYSMETA_VERSIONS_MODE] = 'stack'
                continue

        if _propagated_hdr(hdr):
            # Dunno why, really, but the internal client app will 503
            # with utf8-encoded header values IF the header key is a
            # unicode instance (even if it's just low ascii chars in
            # that unicode instance).  Go figure...
            headers[hdr.encode('utf8')] = \
                resp.headers[hdr].encode('utf8')

    return headers


def get_local_versioning_headers(headers):
    if not SYSMETA_VERSIONS_LOC or not SYSMETA_VERSIONS_MODE:
        return {}

    ret = {}
    if 'x-history-location' in headers:
        ret[SYSMETA_VERSIONS_LOC] = \
            headers['x-history-location'].encode('utf8')
        ret[SYSMETA_VERSIONS_MODE] = 'history'

    if 'x-versions-location' in headers:
        ret[SYSMETA_VERSIONS_LOC] = \
            headers['x-versions-location'].encode('utf8')
        ret[SYSMETA_VERSIONS_MODE] = 'stack'
    return ret


def diff_container_headers(remote_headers, local_headers):
    # remote_headers are unicode, local are str returns str
    rem_headers = dict([(k.encode('utf8'), v.encode('utf8')) for k, v in
                        remote_headers.items() if _propagated_hdr(k)])
    rem_headers.update(get_local_versioning_headers(remote_headers))

    matching_keys = set(rem_headers.keys()).intersection(local_headers.keys())
    missing_remote_keys = set(
        local_headers.keys()).difference(rem_headers.keys())

    missing_local_keys = set(
        rem_headers.keys()).difference(local_headers.keys())

    # TODO: we can probably sink some of these checks into the
    # _propagated_hdr() function.
    versioning_headers = [SYSMETA_VERSIONS_LOC, SYSMETA_VERSIONS_MODE]
    return dict([(k, rem_headers[k])
                 for k in matching_keys if
                 rem_headers[k] != local_headers.get(k)] +
                [(k, rem_headers[k]) for k in missing_local_keys] +
                [(k, '') for k in missing_remote_keys
                 if _propagated_hdr(k) or k in versioning_headers])


def diff_account_headers(remote_headers, local_headers):
    # account acls need to be translated to sysmeta
    def _fix_acl(key):
        if key == ACCOUNT_ACL_KEY:
            return SYSMETA_ACCOUNT_ACL_KEY
        return key

    def _prop_hdr(hdr):
        propagate_keys = [ACCOUNT_ACL_KEY, SYSMETA_ACCOUNT_ACL_KEY]
        return hdr.startswith('x-account-meta') or hdr in propagate_keys

    # remote_headers need to be utf8-encoded to match local,
    # returns utf8-encoded strings
    rem_headers = dict([(_fix_acl(k.lower().encode('utf8')), v.encode('utf8'))
                        for k, v in remote_headers.items()
                        if _prop_hdr(k.lower())])
    loc_headers = dict([(_fix_acl(k.lower()), v) for k, v in
                        local_headers.items()
                        if _prop_hdr(k.lower())])
    matching_keys = set(rem_headers.keys()).intersection(loc_headers.keys())
    missing_remote_keys = set(
        loc_headers.keys()).difference(rem_headers.keys())
    missing_local_keys = set(
        rem_headers.keys()).difference(loc_headers.keys())
    return dict([(k, rem_headers[k])
                 for k in matching_keys if
                 rem_headers[k] != loc_headers.get(k)] +
                [(k, rem_headers[k]) for k in missing_local_keys] +
                [(k, '') for k in missing_remote_keys])


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
            s3_headers[s3_header_name] = sanitize_s3_header(swift_headers[hdr])
        elif hdr.lower() == MANIFEST_HEADER:
            s3_headers[MANIFEST_HEADER] = urllib.quote(swift_headers[hdr])
        elif hdr.lower() == SLO_HEADER:
            s3_headers[SLO_HEADER] = urllib.quote(swift_headers[hdr])

    return s3_headers


def sanitize_s3_header(value):
    if set(value).issubset(string.printable):
        return value

    value = Header(value, 'UTF-8').encode()
    if value.startswith('=?utf-8?q?'):
        return '=?UTF-8?Q?' + value[10:]
    elif value.startswith('=?utf-8?b?'):
        return '=?UTF-8?B?' + value[10:]
    else:
        return value


def decode_s3_header(value):
    header, charset = decode_header(value)[0]
    if charset not in ('utf-8', 'UTF-8', 'utf8'):
        return value
    return header.decode(charset)


def convert_to_swift_headers(s3_headers):
    swift_headers = {}
    for header, value in s3_headers.items():
        if header in ('x-amz-id-2', 'x-amz-request-id'):
            swift_headers['Remote-' + header] = value
        elif header.endswith((MANIFEST_HEADER, SLO_HEADER)):
            swift_headers[header[len(S3_USER_META_PREFIX):]] = value
        elif header.startswith(S3_USER_META_PREFIX):
            key = SWIFT_USER_META_PREFIX + header[len(S3_USER_META_PREFIX):]
            swift_headers[key] = decode_s3_header(value)
        elif header == 'content-length':
            # Capitalize, so eventlet doesn't try to add its own
            swift_headers['Content-Length'] = value
        elif header == 'etag':
            # S3 returns ETag in quotes
            swift_headers['etag'] = value[1:-1]
        else:
            swift_headers[header] = decode_s3_header(value)
    return swift_headers


def convert_to_local_headers(headers, remove_timestamp=True):
    put_headers = dict([(k.encode('utf-8'), unicode(v).encode('utf-8'))
                        for k, v in headers
                        if not k.startswith('Remote-')])
    # We must remove the X-Timestamp header, as otherwise objects may
    # never be restored if a tombstone is present (as the remote
    # timestamp may be older than then tombstone). Only happens if
    # restoring from Swift.
    if 'x-timestamp' in put_headers and remove_timestamp:
        del put_headers['x-timestamp']
    if put_headers['etag'].startswith('"%s' % PFS_ETAG_PREFIX):
        # ProxyFS does not store a valid etag and we cannot use it to validate
        # the upload.
        del put_headers['etag']
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


def get_dlo_prefix(swift_meta):
    if MANIFEST_HEADER not in swift_meta:
        return ''
    return swift_meta[MANIFEST_HEADER]


def get_internal_manifest(acc, cont, obj, internal_client, swift_headers={}):
    """
    Return SLO manifest. multipart-manifest=get is not used here because slo
    middleware is not in internal_client pipeline. Only use with a real
    internal_client object, do not use for getting slo manifests in remote
    clusters.
    """
    status, headers, body = internal_client.get_object(
        acc, cont, obj, headers=swift_headers)
    if status != 200:
        body.close()
        raise RuntimeError('Failed to get the manifest')
    manifest = json.load(FileLikeIter(body))
    body.close()
    return headers, manifest


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


def iter_listing(list_func, logger, marker, limit, prefix, delimiter):
    def _results_iterator(_resp):
        while True:
            if _resp.status != 200:
                logger.error(
                    'Failed to list the remote store: %s' %
                    _resp.status)
                break
            if not _resp.body:
                break
            for item in _resp.body:
                if 'name' in item:
                    marker = item['name']
                else:
                    marker = item['subdir']
                item['content_location'] = [item['content_location']]

                yield item, marker
            _resp = list_func(
                marker=marker, limit=limit, prefix=prefix, delimiter=delimiter)
        yield None, None  # just to simplify some book-keeping

    resp = list_func(marker=marker, limit=limit, prefix=prefix,
                     delimiter=delimiter)
    return resp, _results_iterator(resp)


def splice_listing(local_iter, remote_iter, limit):
    remote_item, remote_key = next(remote_iter)
    # There used to be an unnecessary short-circuit here if remote_item is
    # false. However, it's easier to handle "local_iter" possibly only yielding
    # items and not tuples of (item, key) if that's only handled in one place.

    spliced_response = []
    for local_item in local_iter:
        # If local_iter came from iter_listing() then it has local_key in it
        # already, otherwise it will have come from a local Swift cluster
        # listing and we're being called from the shunt, and we have to compute
        # the local_key value (duplicating some logic, incidentally).
        if isinstance(local_item, tuple):
            local_item, local_key = local_item
        else:
            if 'name' in local_item:
                local_key = local_item['name']
            else:
                local_key = local_item['subdir']

        if local_item is None:
            # local_iter came from iter_listing() and it's exhausted
            break

        if len(spliced_response) == limit:
            break

        if not remote_item:
            spliced_response.append(local_item)
            continue

        while (remote_item and remote_key < local_key and
               len(spliced_response) < limit):
            spliced_response.append(remote_item)
            remote_item, remote_key = next(remote_iter)

        if len(spliced_response) == limit:
            break

        if remote_key == local_key:
            # duplicate!
            # XXX(darrell): this is backward for cloud-connector; the value to
            # append here will have to come from the caller--we don't have
            # enough context.  OR, could we just append
            # local_item['content_location'] or
            # local_item['content_location'][0] or something??
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
    return spliced_response


def iter_internal_listing(
        client, account, container=None, marker='', prefix=None):
    '''Calls GET on the specified path to list items.

    Useful in case we cannot use the InternalClient.iter_{containers,
    objects}(). The InternalClient generators make multiple calls to the
    object store and require holding the client out of the InternalClient
    pool.

    :param client: Swift InternalClient or context manager that produces one.
    :param account: Swift account for the listing.
    :param container: Optional container parameter. Omit if listing the
                      acccount.
    :param marker: Optional marker parameter -- specifies the object name from
                   which to resume the listing.
    :param prefix: Optional prefix parameter for listing.
    :returns: Iterator that produces listing entries (dictionary).
    '''
    while True:
        def _make_internal_request(ic):
            path = ic.make_path(account, container)
            query_string = 'format=json&marker=%s' % quote(marker)
            if prefix:
                query_string += '&prefix=%s' % quote(prefix)
            return ic.make_request(
                'GET', '%s?%s' % (path, query_string), {},
                (2, HTTP_NOT_FOUND))

        if callable(client):
            with client() as ic:
                resp = _make_internal_request(ic)
        else:
            resp = _make_internal_request(client)

        if resp.status_int != 200:
            break
        if not resp.body:
            break

        listing = json.loads(resp.body)
        if not listing:
            break
        for entry in listing:
            yield entry
        marker = listing[-1]['name'].encode('utf-8')
    # Simplifies the bookkeeping
    yield None


def format_xml_listing(
        list_results, root_node, root_name, entry_node, fields):
    root = etree.Element(root_node, name=root_name)
    for entry in list_results:
        obj = etree.Element(entry_node)
        for field in fields:
            if field not in entry:
                continue
            elem = etree.Element(field)
            text = entry[field]
            if type(text) == str:
                text = text.decode('utf-8')
            elif type(text) == int:
                text = str(text)
            elem.text = text
            obj.append(elem)
        root.append(obj)
    resp = etree.tostring(root, encoding='UTF-8', xml_declaration=True)
    return resp.replace("<?xml version='1.0' encoding='UTF-8'?>",
                        '<?xml version="1.0" encoding="UTF-8"?>', 1)


def format_container_listing_response(list_results, list_format, account):
    if list_format == 'application/json':
        return json.dumps(list_results)
    if list_format.endswith('/xml'):
        fields = ['name', 'count', 'bytes', 'last_modified', 'subdir']
        return format_xml_listing(
            list_results, 'account', account, 'container', fields)
    # Default to plain format
    return u'\n'.join(entry['name'] if 'name' in entry else entry['subdir']
                      for entry in list_results).encode('utf-8')


def format_listing_response(list_results, list_format, container):
    if list_format == 'application/json':
        return json.dumps(list_results)
    if list_format.endswith('/xml'):
        fields = ['name', 'content_type', 'hash', 'bytes', 'last_modified',
                  'subdir']
        return format_xml_listing(
            list_results, 'container', container, 'object', fields)

    # Default to plain format
    return u'\n'.join(entry['name'] if 'name' in entry else entry['subdir']
                      for entry in list_results).encode('utf-8')


def get_list_params(req, list_limit):
    limit = int(req.params.get('limit', list_limit))
    marker = req.params.get('marker', '').decode('utf-8')
    prefix = req.params.get('prefix', '').decode('utf-8')
    delimiter = req.params.get('delimiter', '').decode('utf-8')
    path = req.params.get('path', None)
    return limit, marker, prefix, delimiter, path


def get_sys_migrator_header(path_type):
    if path_type == 'object':
        return get_object_transient_sysmeta(MIGRATOR_HEADER)
    return '%s%s' % (get_sys_meta_prefix(path_type), MIGRATOR_HEADER)


def create_x_timestamp_from_hdrs(hdrs, use_x_timestamp=True):
    if use_x_timestamp and 'x-timestamp' in hdrs:
        return float(hdrs['x-timestamp'])
    if 'last-modified' in hdrs:
        ts = datetime.datetime.strptime(hdrs['last-modified'],
                                        LAST_MODIFIED_FMT)
        return (ts - EPOCH).total_seconds()
    return None
