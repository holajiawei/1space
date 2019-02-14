# -*- coding: UTF-8 -*-

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

from itertools import repeat
import hashlib
import json
import mock
import os
import StringIO
import unittest

from .utils import FakeStream, FakeSwift

from s3_sync import utils
from s3_sync import base_sync
from s3_sync.sync_s3 import SyncS3


class TestUtilsFunctions(unittest.TestCase):
    def test_s3_headers_conversion(self):
        input_hdrs = {'x-object-meta-foo': 'Foo',
                      'x-object-meta-Bar': 'Bar',
                      'X-Object-Meta-upper': '1',
                      'X-ObJeCT-Meta-CraZy': 'CrAzY',
                      'x-object-meta-non-ascii': '\xc3\xa9',
                      'x-object-meta-non-ascii-prefix': '\x04w',
                      'x-object-meta-non-ascii-suffix': 'h\x04',
                      'X-Object-Manifest': 'container/key/123415/prefix',
                      'Content-Type': 'application/testing'}
        out = utils.convert_to_s3_headers(input_hdrs)
        expected = dict([(key[len('x-object-meta-'):].lower(), value) for
                         key, value in input_hdrs.items() if
                         key.lower().startswith(utils.SWIFT_USER_META_PREFIX)])
        expected[utils.MANIFEST_HEADER] = input_hdrs['X-Object-Manifest']
        expected['non-ascii'] = '=?UTF-8?B?w6k=?='
        expected['non-ascii-prefix'] = '=?UTF-8?Q?=04w?='
        expected['non-ascii-suffix'] = '=?UTF-8?Q?h=04?='
        self.assertEqual(set(expected.keys()), set(out.keys()))
        for key in out.keys():
            self.assertEqual(expected[key], out[key])

    def test_swift_headers_conversion(self):
        input_hdrs = {'x-amz-meta-custom-header': 'value',
                      'x-amz-meta-unreadable-prefix': '=?UTF-8?Q?=04w?=',
                      'x-amz-meta-unreadable-suffix': '=?UTF-8?Q?h=04?=',
                      'x-amz-meta-lots-of-unprint': '=?UTF-8?B?BAQEBAQ=?=',
                      'content-length': '128',
                      'etag': '"deadbeef"',
                      'content-type': 'migrator/test',
                      'content-disposition': "attachment; filename='test.jpg'",
                      'content-encoding': 'identity'}

        exp_hdrs = {'x-object-meta-custom-header': 'value',
                    'x-object-meta-unreadable-prefix': '\x04w',
                    'x-object-meta-unreadable-suffix': 'h\x04',
                    'x-object-meta-lots-of-unprint': 5 * '\x04',
                    'Content-Length': '128',
                    'etag': 'deadbeef',
                    'content-type': 'migrator/test',
                    'content-disposition': "attachment; filename='test.jpg'",
                    'content-encoding': 'identity'}
        out = utils.convert_to_swift_headers(input_hdrs)
        self.assertEqual(set(exp_hdrs.keys()), set(out.keys()))
        for key in out.keys():
            self.assertEqual(exp_hdrs[key], out[key])

    def test_get_slo_etag(self):
        sample_manifest = [{'hash': 'abcdef'}, {'hash': 'fedcba'}]
        # We expect the md5 sum of the concatenated strings (converted to hex
        # bytes) followed by the number of parts (segments)
        expected_tag = 'ce7989f0e2f1f3e4fdd2a01dda0844ae-2'
        self.assertEqual(expected_tag, utils.get_slo_etag(sample_manifest))

    def test_response_is_complete(self):
        def do_test(status, headers):
            self.assertTrue(utils.response_is_complete(status, headers))

        do_test(200, [('any', 'headers')])
        do_test(206, [('Content-Range', 'bytes 0-1/2')])
        do_test(206, [('Content-Range', 'bytes 0-1000/1001')])

        def do_test(status, headers):
            self.assertFalse(utils.response_is_complete(status, headers))

        do_test(206, [('Content-Range', 'some random crap')])
        do_test(206, [('Content-Range', 'bytes 1-1000/1001')])
        do_test(206, [('Content-Range', 'bytes 0-1000/1100')])
        do_test(206, [('Content-Range', 'bytes 0-1000/*')])
        do_test(206, [('Content-Range', 'bytes 0-wat/999')])
        do_test(206, [('no', 'Content-Range')])
        do_test(500, [('Content-Range', 'bytes 0-1000/1001')])

    def test_convert_to_local_headers(self):
        self.assertEqual({'content-type': 'application/test',
                          'x-object-meta': 'object-meta',
                          'etag': 'deadbeef'},
                         utils.convert_to_local_headers(
                         {'Remote-x-transaction-id': 'some id',
                          'content-type': 'application/test',
                          'x-timestamp': 12345,
                          'x-object-meta': 'object-meta',
                          'etag': 'deadbeef'}.items()))
        self.assertEqual(
            {'content-type': 'application/test',
             'x-timestamp': '12345',
             'etag': 'deadbeef'},
            utils.convert_to_local_headers(
                {'Remote-x-object-meta': 'foo',
                 'x-timestamp': 12345,
                 'content-type': 'application/test',
                 'etag': 'deadbeef'}.items(),
                remove_timestamp=False))

        self.assertEqual(
            {'content-type': 'application/test'},
            utils.convert_to_local_headers(
                {'etag': '"pfs-etag-some-value"',
                 'content-type': 'application/test'}.items()))

    def test_diff_container_headers(self):
        # old swifts don't have the versioning constants defined
        internal_location = 'x-container-sysmeta-versions-location'
        internal_mode = 'x-container-sysmeta-versions-mode'
        tests = [
            ({'x-history-location': u'fo\u00f3',
              'boogaloo': 'bangarang',
              u'x-container-meta-test-\u062a': u'remote-\u062a-new',
              u'x-container-meta-keep-\u062a': u'keepval-\u062a',
              u'x-container-meta-test-new-\u062a': u'myval\u062a'},
             {'poohbear': 'eeyore',
              'x-container-meta-test-\xd8\xaa': 'remote-\xd8\xaa',
              'x-container-meta-keep-\xd8\xaa': 'keepval-\xd8\xaa',
              'x-container-meta-test-old-\xd8\xaa': 'myval\xd8\xaa-oldval'},
             {'x-container-meta-test-\xd8\xaa': 'remote-\xd8\xaa-new',
              'x-container-meta-test-new-\xd8\xaa': 'myval\xd8\xaa',
              'x-container-meta-test-old-\xd8\xaa': ''},
             {internal_location: 'fo\xc3\xb3',
              internal_mode: 'history'}),
            ({'x-versions-location': u'version\u00e9d'},
             {},
             {},
             {internal_location: 'version\xc3\xa9d',
              internal_mode: 'stack'}),
            ({},
             {internal_location: 'foo',
              internal_mode: 'history'},
             {},
             {internal_location: '',
              internal_mode: ''}),
            ({'x-versions-location': u'version\u00e9d'},
             {internal_location: 'fo\xc3\b3',
              internal_mode: 'history'},
             {},
             {internal_location: 'version\xc3\xa9d',
              internal_mode: 'stack'}),
            ({'x-history-location': 'history'},
             {internal_location: 'other',
              internal_mode: 'stack'},
             {},
             {internal_location: 'history',
              internal_mode: 'history'})]

        for remote, local, expected, expected_versioning in tests:
            if utils.SYSMETA_VERSIONS_LOC and utils.SYSMETA_VERSIONS_MODE:
                expected.update(expected_versioning)
            self.assertEqual(expected, utils.diff_container_headers(
                remote, local))

    def test_diff_account_headers(self):
        tests = [
            ({'boogaloo': 'bangarang',
              u'x-account-meta-test-\u062a': u'remote-\u062a-new',
              u'x-account-meta-keep-\u062a': u'keepval-\u062a',
              u'x-account-meta-test-new-\u062a': u'myval\u062a',
              utils.ACCOUNT_ACL_KEY: '{"read-only":["AUTH_test"]}',
              'x-account-meta-temp-url-key': 'newsecret'},
             {'poohbear': 'eeyore',
              'x-account-meta-test-\xd8\xaa': 'remote-\xd8\xaa',
              'x-account-meta-keep-\xd8\xaa': 'keepval-\xd8\xaa',
              'x-account-meta-test-old-\xd8\xaa': 'myval\xd8\xaa-oldval'},
             {'x-account-meta-test-\xd8\xaa': 'remote-\xd8\xaa-new',
              'x-account-meta-test-new-\xd8\xaa': 'myval\xd8\xaa',
              'x-account-meta-test-old-\xd8\xaa': '',
              utils.SYSMETA_ACCOUNT_ACL_KEY: '{"read-only":["AUTH_test"]}',
              'x-account-meta-temp-url-key': 'newsecret'}),
            ({'boogaloo': 'bangarang',
              u'x-account-meta-test-\u062a': u'remote-\u062a-new',
              u'x-account-meta-keep-\u062a': u'keepval-\u062a',
              u'x-account-meta-test-new-\u062a': u'myval\u062a',
              'x-account-meta-temp-url-key': 'newsecret'},
             {'poohbear': 'eeyore',
              'x-account-meta-test-\xd8\xaa': 'remote-\xd8\xaa-new',
              'x-account-meta-keep-\xd8\xaa': 'keepval-\xd8\xaa',
              'x-account-meta-test-new-\xd8\xaa': 'myval\xd8\xaa',
              utils.ACCOUNT_ACL_KEY: '{"read-only":["AUTH_test"]}',
              'x-account-meta-temp-url-key': 'oldsecret'},
             {utils.SYSMETA_ACCOUNT_ACL_KEY: '',
              'x-account-meta-temp-url-key': 'newsecret'}),
            ({'boogaloo': 'bangarang',
              u'x-account-meta-test-\u062a': u'remote-\u062a-new',
              u'x-account-meta-keep-\u062a': u'keepval-\u062a',
              u'x-account-meta-test-new-\u062a': u'myval\u062a',
              'x-account-meta-temp-url-key': 'secret'},
             {'poohbear': 'eeyore',
              'x-account-meta-test-\xd8\xaa': 'remote-\xd8\xaa-new',
              'x-account-meta-keep-\xd8\xaa': 'keepval-\xd8\xaa',
              'x-account-meta-test-new-\xd8\xaa': 'myval\xd8\xaa',
              'x-account-meta-temp-url-key': 'secret'},
             {}),
            ({},
             {},
             {}),
            ({'x-account-meta-temp-url-key': 'mysecret'},
             {},
             {'x-account-meta-temp-url-key': 'mysecret'}),
            ({},
             {'x-account-meta-temp-url-key': 'mysecret'},
             {'x-account-meta-temp-url-key': ''}),
        ]

        for remote, local, expected in tests:
            self.assertEqual(expected, utils.diff_account_headers(
                remote, local))

    def test_sys_migrator_header(self):
        self.assertEqual(
            'x-account-sysmeta-' + utils.MIGRATOR_HEADER,
            utils.get_sys_migrator_header('account'))
        self.assertEqual(
            'x-container-sysmeta-' + utils.MIGRATOR_HEADER,
            utils.get_sys_migrator_header('container'))
        self.assertEqual(
            'x-object-transient-sysmeta-' + utils.MIGRATOR_HEADER,
            utils.get_sys_migrator_header('object'))


class FakeSwiftClient(object):
    def __init__(self, status=200, size=1024, content_length='UNSPECIFIED',
                 content=None):
        self.status = status
        self.content = content
        if self.content:
            self.size = len(content)
        else:
            self.size = size
        self.content_length = self.size if content_length == 'UNSPECIFIED' \
            else content_length

    def get_object(self, account, container, key, headers=None):
        self.got_headers = headers
        if self.content:
            self.fake_stream = FakeStream(content=self.content)
        else:
            self.fake_stream = FakeStream(self.size)
        headers = {'X-Object-Meta-Foo': 'baR'}
        if self.content_length is not None:
            headers['Content-Length'] = str(self.content_length)
        return (self.status, headers, self.fake_stream)


class JustLikeAFile(object):
    def __init__(self):
        self.buffer = 'ABCD' * 2**16

    def read(self, size=-1):
        size = min(size, 2**16 - 12)
        chunk = self.buffer[:size]
        self.buffer = self.buffer[size:]
        return chunk


class TestSeekableFileLikeIter(unittest.TestCase):
    def setUp(self):
        self.mock_swift = FakeSwiftClient()
        self.seeker = utils.SeekableFileLikeIter(['abc', 'def', 'ijk'])

    def test_body_not_iter_str_unicode_or_filelike(self):
        with self.assertRaises(TypeError) as cm:
            self.seeker = utils.SeekableFileLikeIter(None)
            self.assertEqual("'NoneType' object is not iterable",
                             cm.exception.message)

    def test_takes_a_filelike(self):
        filelike = JustLikeAFile()
        self.seeker = utils.SeekableFileLikeIter(filelike)

        self.assertEqual('ABCD' * 2**16, ''.join(self.seeker))
        self.assertEqual(2**16 * 4, self.seeker.tell())
        self.assertEqual('', self.seeker.read())
        self.assertEqual('', self.seeker.read(1))
        self.assertRaises(StopIteration, self.seeker.next)
        with self.assertRaises(StopIteration):
            next(self.seeker)

    def test_takes_a_filelike_with_shorter_length(self):
        filelike = JustLikeAFile()
        self.seeker = utils.SeekableFileLikeIter(filelike, length=2**16 + 48)

        self.assertEqual('ABCD' * ((2**16 + 48) / 4), ''.join(self.seeker))
        self.assertEqual(2**16 + 48, self.seeker.tell())
        self.assertEqual('', self.seeker.read())
        self.assertEqual('', self.seeker.read(1))
        self.assertRaises(StopIteration, self.seeker.next)
        with self.assertRaises(StopIteration):
            next(self.seeker)

    def test_bounded_reading(self):
        # Pete and Repete were in a boat; Pete fell out.  Who was left??
        self.seeker = utils.SeekableFileLikeIter(repeat('abcd'), length=10)

        self.assertEqual('abcdabcdab', ''.join(self.seeker))
        self.assertEqual(10, self.seeker.tell())
        self.assertEqual('', self.seeker.read())
        self.assertEqual('', self.seeker.read(1))
        self.assertRaises(StopIteration, self.seeker.next)
        with self.assertRaises(StopIteration):
            next(self.seeker)

        # Seeking after reading data, without a callback, is most likely a
        # coding bug and is explicitly disallowed.
        self.assertRaises(RuntimeError, self.seeker.reset)

        # the remaining buffer should be saved if the caller needs to use it
        self.assertEqual('cd', self.seeker.buf)

    def test_close(self):
        self.assertEqual('ab', self.seeker.read(2))
        self.seeker.close()
        self.seeker.close()  # idempotent
        self.assertRaises(ValueError, self.seeker.read)
        self.assertRaises(ValueError, self.seeker.readlines)
        self.assertRaises(ValueError, self.seeker.next)
        with self.assertRaises(ValueError):
            next(self.seeker)

    def test_seek_zero_with_cb(self):
        cb = mock.Mock()
        cb.side_effect = lambda: iter(['1', '23', '456'])

        self.seeker = utils.SeekableFileLikeIter(['abc', 'def', 'ijk'],
                                                 seek_zero_cb=cb)

        self.assertEqual(0, self.seeker.tell())
        self.seeker.seek(0)  # flag is optional
        self.assertEqual(0, self.seeker.tell())
        self.seeker.seek(0, os.SEEK_CUR)  # flag is actually ignored
        # A seek-zero with no bytes read is a NOOP
        self.assertEqual([], cb.mock_calls)

        self.assertEqual('ab', self.seeker.read(2))
        self.assertEqual(2, self.seeker.tell())
        self.seeker.seek(0, os.SEEK_END)  # LOL
        self.assertEqual(0, self.seeker.tell())
        self.assertEqual('123456', ''.join(self.seeker))
        self.assertEqual(6, self.seeker.tell())
        self.assertEqual('', self.seeker.read())
        self.assertEqual('', self.seeker.read(1))
        self.assertRaises(StopIteration, self.seeker.next)

    def test_seek_zero_no_cb(self):
        self.assertEqual(0, self.seeker.tell())
        self.seeker.seek(0, os.SEEK_CUR)  # flag is actually ignored
        self.assertEqual(0, self.seeker.tell())
        self.seeker.seek(0, os.SEEK_END)  # flag is actually ignored
        self.assertEqual(0, self.seeker.tell())
        self.seeker.seek(0, os.SEEK_SET)  # flag is actually ignored
        self.assertEqual(0, self.seeker.tell())
        self.seeker.reset()  # synonym for `.seek(0, ...)`
        self.assertEqual(0, self.seeker.tell())

        with self.assertRaises(RuntimeError) as cm:
            self.seeker.seek(-1, 'I am totally ignored!')
        self.assertEqual('SeekableFileLikeIter: '
                         'arbitrary seeks are not supported',
                         cm.exception.message)

        with self.assertRaises(RuntimeError) as cm:
            self.seeker.seek(1, 'I am totally ignored!')
        self.assertEqual('SeekableFileLikeIter: '
                         'arbitrary seeks are not supported',
                         cm.exception.message)

        self.assertEqual('ab', self.seeker.read(2))
        self.assertEqual(2, self.seeker.tell())

        # Seeking after reading data, without a callback, is most likely a
        # coding bug and is explicitly disallowed.
        self.assertRaises(RuntimeError, self.seeker.reset)

    def test_seekable_read_all(self):
        self.assertEqual(0, self.seeker.tell())
        self.assertEqual('abcdefijk', ''.join(c for c in self.seeker))
        self.assertEqual(9, self.seeker.tell())
        self.assertEqual('', ''.join(c for c in self.seeker))
        self.assertEqual(9, self.seeker.tell())

        self.seeker = utils.SeekableFileLikeIter(['abc', 'def', 'ijk'])
        self.assertEqual(0, self.seeker.tell())
        self.assertEqual('abcdefijk', self.seeker.read())
        self.assertEqual(9, self.seeker.tell())
        self.assertEqual('', self.seeker.read())
        self.assertEqual(9, self.seeker.tell())

    def test_seekable_short_reads_and_next(self):
        self.assertEqual('a', self.seeker.read(1))
        self.assertEqual(1, self.seeker.tell())
        # short read served from buffer (only)
        self.assertEqual('bc', self.seeker.read(3))
        self.assertEqual(3, self.seeker.tell())
        self.assertEqual('d', self.seeker.read(1))
        self.assertEqual(4, self.seeker.tell())
        # read(0) is a NOP
        self.assertEqual('', self.seeker.read(0))
        self.assertEqual(4, self.seeker.tell())
        # next served from buffer (only)
        self.assertEqual('ef', next(self.seeker))
        self.assertEqual(6, self.seeker.tell())
        # next served from iterator
        self.assertEqual('ijk', self.seeker.next())
        self.assertEqual(9, self.seeker.tell())
        # final read delivers EOF
        self.assertEqual('', self.seeker.read(1))


class TestFileWrapper(unittest.TestCase):
    def setUp(self):
        self.mock_swift = FakeSwiftClient()

    def test_failed_req(self):
        self.mock_swift = FakeSwiftClient(status=401)
        with self.assertRaises(RuntimeError) as cm:
            utils.FileWrapper(self.mock_swift,
                              'account', 'container', 'key',
                              headers={'a': 'b'})
        self.assertEqual('Failed to get the object', cm.exception.message)

    def test_no_content_length(self):
        content = 'shimmyjimmy' * 3
        self.mock_swift = FakeSwiftClient(content_length=None, content=content)
        wrapper = utils.FileWrapper(self.mock_swift,
                                    'account',
                                    'container',
                                    'key', headers={'a': 'b'})

        with self.assertRaises(TypeError):
            len(wrapper)
        self.assertEqual({
            'X-Object-Meta-Foo': 'baR',
        }, wrapper.get_headers())
        self.assertEqual({
            # gets lowercased, I guess
            'foo': 'baR',
        }, wrapper.get_s3_headers())
        self.assertEqual({'a': 'b'}, self.mock_swift.got_headers)

        chunk_size = wrapper._swift_stream.chunk_size
        self.assertGreater(chunk_size, 2)
        self.assertEqual(content[:2], wrapper.read(2))
        self.assertEqual(content[2:chunk_size], wrapper.next())

        wrapper.reset()
        self.assertEqual(content, wrapper.read())

        wrapper.reset()
        self.assertEqual(content, ''.join(wrapper))

    def test_close(self):
        wrapper = utils.FileWrapper(self.mock_swift,
                                    'account',
                                    'container',
                                    'key', headers={'a': 'b'})
        self.assertEqual(1024, len(wrapper))
        chunk_size = self.mock_swift.fake_stream.chunk_size
        self.assertEqual('A' * chunk_size, next(wrapper))
        self.assertNotEqual(None, wrapper._swift_stream)
        wrapper.close()
        self.assertEqual(None, wrapper._swift_stream)
        wrapper.close()  # idemptotent
        self.assertRaises(ValueError, wrapper.read)
        self.assertRaises(ValueError, wrapper.readlines)
        self.assertRaises(ValueError, wrapper.next)
        with self.assertRaises(ValueError):
            next(wrapper)

    def test_no_content_length_then_a_content_length(self):
        self.mock_swift = mock.Mock()
        self.mock_swift.get_object.side_effect = [
            (200, {}, FakeStream()),
            (200, {'Content-Length': '1024'}, FakeStream()),
        ]
        wrapper = utils.FileWrapper(self.mock_swift,
                                    'account', 'container', 'key',
                                    headers={'a': 'b'})

        with self.assertRaises(TypeError):
            len(wrapper)

        chunk_size = wrapper._swift_stream.chunk_size
        self.assertEqual('A' * (2 * chunk_size), wrapper.read(4000) +
                         next(wrapper))

        wrapper.seek(0, 'ignored')

        self.assertEqual(1024, len(wrapper))
        self.assertEqual('A' * 1024, wrapper.read())

    def test_content_length_then_no_content_length(self):
        self.mock_swift = mock.Mock()
        self.mock_swift.get_object.side_effect = [
            (200, {'Content-Length': '1024'}, FakeStream()),
            (200, {}, FakeStream()),
        ]
        wrapper = utils.FileWrapper(self.mock_swift,
                                    'account', 'container', 'key',
                                    headers={'a': 'b'})

        self.assertEqual(1024, len(wrapper))
        chunk_size = wrapper._swift_stream.chunk_size
        self.assertEqual('A' * (2 * chunk_size), wrapper.read(4000) +
                         next(wrapper))

        wrapper.seek(0, 'ignored')

        with self.assertRaises(TypeError):
            len(wrapper)
        self.assertEqual('A' * 1024, wrapper.read())

    def test_open(self):
        wrapper = utils.FileWrapper(self.mock_swift,
                                    'account',
                                    'container',
                                    'key', headers={'a': 'b'})
        self.assertEqual(1024, len(wrapper))
        self.assertEqual({
            'Content-Length': '1024',
            'X-Object-Meta-Foo': 'baR',
        }, wrapper.get_headers())
        self.assertEqual({
            # gets lowercased, I guess
            'foo': 'baR',
        }, wrapper.get_s3_headers())
        self.assertEqual({'a': 'b'}, self.mock_swift.got_headers)

    def test_seek(self):
        wrapper = utils.FileWrapper(self.mock_swift,
                                    'account',
                                    'container',
                                    'key')
        chunk_size = self.mock_swift.fake_stream.chunk_size
        self.assertGreater(256, chunk_size)  # sanity check
        self.assertEqual('A' * chunk_size, wrapper.read(256))
        self.assertEqual(chunk_size, wrapper.tell())
        self.assertEqual(None, wrapper.buf)
        # A short read will be of that length, leaving remainder in a buffer
        self.assertEqual('A', wrapper.read(1))
        self.assertEqual('A' * (chunk_size - 1), wrapper.buf)
        self.assertEqual(chunk_size + 1, wrapper.tell())
        # A turn of the crank will just return remaining buffer
        got_next = wrapper.next()
        self.assertEqual('A' * (chunk_size - 1), got_next,
                         'Exp %d bytes, got %d' % (chunk_size - 1,
                                                   len(got_next)))
        self.assertEqual(2 * chunk_size, wrapper.tell())
        wrapper.seek(0)
        self.assertEqual(0, wrapper.tell())
        got_all = ''.join(wrapper)
        self.assertEqual('A' * 1024, got_all,
                         'Exp %d bytes, got %d' % (1024, len(got_all)))
        self.assertEqual(1024, wrapper.tell())

    def test_reads_extra_byte(self):
        content = 'deadbeef' * 12
        self.mock_swift = FakeSwiftClient(
            content_length=len(content),
            content=content)
        wrapper = utils.FileWrapper(
            self.mock_swift, 'account', 'container', 'key')
        self.assertEqual(len(content), len(wrapper))
        recvd_content = ''
        while len(recvd_content) < len(content):
            recvd_content += wrapper.read(len(content) - len(recvd_content))
        self.assertTrue(wrapper._swift_stream.raised_stop_iter)
        self.assertEqual(content, recvd_content)


class TestSLOFileWrapper(unittest.TestCase):
    def setUp(self):
        self.manifest = [
            {'name': '/foo/part1',
             'bytes': 500},
            {'name': '/foo/part2',
             'bytes': 1000}
        ]
        self.swift = mock.Mock()

    def test_slo_length(self):
        slo = utils.SLOFileWrapper(self.swift, 'account', self.manifest,
                                   {'etag': 'deadbeef'})
        self.assertEqual(1500, len(slo))

    def test_seek_after_read(self):
        fake_segment = FakeStream(content='A' * 500)
        self.assertEqual(False, fake_segment.closed)

        def get_object(account, container, key, headers={}):
            if account != 'account':
                raise RuntimeError('unknown account')
            if container != 'foo':
                raise RuntimeError('unknown container')
            if key == 'part1':
                return (200, {'Content-Length': 500}, fake_segment)
            raise RuntimeError('unknown key (%r, %r, %r, %r)' % (
                account, container, key, headers))

        self.swift.get_object.side_effect = get_object
        slo = utils.SLOFileWrapper(self.swift, 'account', self.manifest,
                                   headers={'etag': 'deadbeef'})
        data = slo.read()
        slo.seek(0)
        self.assertEqual(True, fake_segment.closed)
        self.assertEqual('A' * 500, data)
        self.swift.get_object.assert_called_once_with(
            'account', 'foo', 'part1', headers={'etag': 'deadbeef'})

    def test_read_manifest(self):
        part1_content = FakeStream(content='A' * 500)
        part2_content = FakeStream(content='B' * 1000)

        def get_object(account, container, key, headers={}):
            if account != 'account':
                raise RuntimeError('unknown account')
            if container != 'foo':
                raise RuntimeError('unknown container')
            if key == 'part1':
                return (200, {'Content-Length': 500}, part1_content)
            if key == 'part2':
                return (200, {'Content-Length': 1000}, part2_content)
            raise RuntimeError('unknown key')

        self.swift.get_object.side_effect = get_object
        slo = utils.SLOFileWrapper(self.swift, 'account', self.manifest,
                                   {'etag': 'deadbeef'})
        content = ''
        while True:
            data = slo.read()
            content += data
            if not data:
                break
        self.assertEqual(1500, len(content))
        self.assertEqual('A' * 500, content[0:500])
        self.assertEqual('B' * 1000, content[500:1500])

        self.swift.get_object.has_calls(
            mock.call('account', 'foo', 'part1', {}),
            mock.call('account', 'foo', 'part2', {}))
        self.assertEqual(True, part1_content.closed)
        self.assertEqual(True, part2_content.closed)
        total_etag, segments_etags = slo.etag()
        content = ['A' * 500, 'B' * 1000]
        self.assertEqual(
            total_etag, hashlib.md5(''.join(content)).hexdigest())
        self.assertEqual(
            [hashlib.md5(part).hexdigest() for part in content],
            segments_etags)

    @mock.patch.object(utils, 'DEFAULT_CHUNK_SIZE', 10)
    def test_iter_manifest(self):
        part1_content = FakeStream(content='A' * 15)
        part1_content.chunk_size = 100
        part2_content = FakeStream(content='B' * 30)
        part2_content.chunk_size = 100
        part3_content = FakeStream(content='C' * 3)
        part3_content.chunk_size = 100

        def get_object(account, container, key, headers={}):
            if account != 'account':
                raise RuntimeError('unknown account')
            if container != 'foo':
                raise RuntimeError('unknown container')
            if key == 'part1':
                return (200, {'Content-Length': 15}, part1_content)
            if key == 'part2':
                return (200, {'Content-Length': 30}, part2_content)
            if key == 'part3':
                return (200, {'Content-Length': 3}, part3_content)
            raise RuntimeError('unknown key')

        self.swift.get_object.side_effect = get_object
        slo = utils.SLOFileWrapper(self.swift, 'account', [
            {'name': '/foo/part1',
             'bytes': 15},
            {'name': '/foo/part2',
             'bytes': 30},
            {'name': '/foo/part3',
             'bytes': 3},
        ], {'etag': 'deadbeef'})
        self.assertEqual([x for x in slo], [
            'A' * 10, 'A' * 5,
            'B' * 10, 'B' * 10, 'B' * 10,
            'C' * 3
        ])

        self.swift.get_object.has_calls(
            mock.call('account', 'foo', 'part1', {}),
            mock.call('account', 'foo', 'part2', {}))
        self.assertEqual(True, part1_content.closed)
        self.assertEqual(True, part2_content.closed)


class TestClosingResourceIterable(unittest.TestCase):
    def test_resource_close_afted_read(self):
        pool = mock.Mock()
        resource = base_sync.BaseSync.HttpClientPoolEntry(None, pool)
        self.assertTrue(resource.acquire())
        self.assertEqual(0, resource.semaphore.balance)
        data_src = StringIO.StringIO('test data')
        closing_iter = utils.ClosingResourceIterable(
            resource, data_src)
        data = next(closing_iter)
        with self.assertRaises(StopIteration):
            next(closing_iter)
        self.assertEqual('test data', data)
        self.assertEqual(1, resource.semaphore.balance)
        self.assertTrue(closing_iter.closed)

    def test_resource_close(self):
        pool = mock.Mock()
        resource = base_sync.BaseSync.HttpClientPoolEntry(None, pool)
        self.assertTrue(resource.acquire())
        self.assertEqual(0, resource.semaphore.balance)
        data_src = StringIO.StringIO('test data')
        closing_iter = utils.ClosingResourceIterable(
            resource, data_src)
        closing_iter.close()
        self.assertEqual(1, resource.semaphore.balance)
        self.assertTrue(closing_iter.closed)

    def test_resource_close_destructor(self):
        pool = mock.Mock()
        resource = base_sync.BaseSync.HttpClientPoolEntry(None, pool)
        self.assertTrue(resource.acquire())
        self.assertEqual(0, resource.semaphore.balance)
        data_src = StringIO.StringIO('test data')
        closing_iter = utils.ClosingResourceIterable(
            resource, data_src)
        del closing_iter
        self.assertEqual(1, resource.semaphore.balance)

    def test_closed_resource_destructor(self):
        pool = mock.Mock()
        resource = base_sync.BaseSync.HttpClientPoolEntry(None, pool)
        self.assertTrue(resource.acquire())
        self.assertEqual(0, resource.semaphore.balance)
        data_src = StringIO.StringIO('test data')
        closing_iter = utils.ClosingResourceIterable(
            resource, data_src)
        closing_iter.close()
        self.assertEqual(1, resource.semaphore.balance)
        self.assertTrue(closing_iter.closed)
        del closing_iter
        self.assertEqual(1, resource.semaphore.balance)

    def test_double_close(self):
        pool = mock.Mock()
        resource = base_sync.BaseSync.HttpClientPoolEntry(None, pool)
        self.assertTrue(resource.acquire())
        self.assertEqual(0, resource.semaphore.balance)
        data_src = StringIO.StringIO('test data')
        closing_iter = utils.ClosingResourceIterable(
            resource, data_src)
        ''.join(closing_iter)
        self.assertEqual(1, resource.semaphore.balance)
        closing_iter.close()
        self.assertEqual(1, resource.semaphore.balance)


class TestPutWrapper(unittest.TestCase):
    def test_stores_object_with_read(self):
        swift = FakeSwift()
        content = 'A' * 1024
        body = StringIO.StringIO(content)
        path = '/v1/AUTH_foo/bar/object'
        wrapper = utils.SwiftPutWrapper(body, {}, path, swift, None)
        result = []
        chunk = wrapper.read()
        while chunk:
            result.append(chunk)
            chunk = wrapper.read()
        self.assertEqual(content, ''.join(result))
        self.assertEqual('PUT', swift.calls[0]['REQUEST_METHOD'])
        self.assertEqual(path, swift.calls[0]['PATH_INFO'])
        self.assertEqual(content, swift.calls[0]['body'])

    def test_stores_generator(self):
        swift = FakeSwift()
        content = 'A' * 1024
        path = '/v1/AUTH_foo/bar/object'

        def _content_generator():
            for chunk in range(0, len(content), 128):
                yield content[chunk:chunk + 128]

        wrapper = utils.SwiftPutWrapper(
            _content_generator(), {}, path, swift, None)
        result = []
        chunk = wrapper.read()
        while chunk:
            result.append(chunk)
            chunk = wrapper.read()
        self.assertEqual(content, ''.join(result))
        self.assertEqual('PUT', swift.calls[0]['REQUEST_METHOD'])
        self.assertEqual(path, swift.calls[0]['PATH_INFO'])
        self.assertEqual(content, swift.calls[0]['body'])

    def test_stores_slo_with_segments(self):
        swift = FakeSwift()
        manifest = [
            {'name': '/segments/1',
             'bytes': 1024,
             'hash': 'deadbeef'},
            {'name': '/segments/2',
             'bytes': 1337,
             'hash': 'fefefefe'}]
        content = 'A' * 1024 + 'B' * 1337
        path = '/v1/AUTH_foo/bar/object'
        body = StringIO.StringIO(content)
        wrapper = utils.SwiftSloPutWrapper(
            body, {'Content-Length': len(content)}, path, swift, None,
            manifest)
        result = []
        chunk = wrapper.read()
        while chunk:
            result.append(chunk)
            chunk = wrapper.read()
        self.assertEqual(content, ''.join(result))
        self.assertEqual('PUT', swift.calls[0]['REQUEST_METHOD'])
        self.assertEqual(
            '/v1/AUTH_foo/segments', swift.calls[0]['PATH_INFO'])
        self.assertEqual('PUT', swift.calls[1]['REQUEST_METHOD'])
        self.assertEqual(
            '/v1/AUTH_foo/segments/1', swift.calls[1]['PATH_INFO'])
        self.assertEqual(content[:1024], swift.calls[1]['body'])
        self.assertEqual('PUT', swift.calls[2]['REQUEST_METHOD'])
        self.assertEqual(
            '/v1/AUTH_foo/segments/2', swift.calls[2]['PATH_INFO'])
        self.assertEqual(content[1024:], swift.calls[2]['body'])
        self.assertEqual(path, swift.calls[3]['PATH_INFO'])
        expected_manifest = [
            {'path': manifest[idx]['name'],
             'size_bytes': manifest[idx]['bytes'],
             'etag': manifest[idx]['hash']} for idx in range(len(manifest))]
        self.assertEqual(expected_manifest, json.loads(swift.calls[3]['body']))

    def test_stores_mpu_with_segments(self):
        provider = mock.Mock(
            SyncS3({'aws_identity': 'key',
                    'aws_secret': 'secret',
                    'account': 'account',
                    'container': 'container',
                    'aws_bucket': 'bucket'}))
        parts = {1: 10,
                 2: 20,
                 3: 50,
                 4: 10}
        ts = '1542760538.81'

        def _head_object(*args, **kwargs):
            return base_sync.ProviderResponse(
                True, 204, {'Content-Length': parts[kwargs['PartNumber']]}, '')

        provider.head_object.side_effect = _head_object

        responses = {'PUT': [
            # segments container PUT
            (lambda headers, body: ('200 OK', [], ''))] +
            [lambda headers, body: (
                '200 OK', {'etag': hashlib.md5(body).hexdigest()}, '')
             for _ in range(len(parts) + 1)]  # extra response for manifest PUT
        }
        swift = FakeSwift(responses)
        content = ''.join([chr(ord('A') + k) * parts[k]
                           for k in sorted(parts.keys())])
        offset = 0
        parts_etags = []
        manifest = []
        for k in sorted(parts.keys()):
            parts_etags.append(hashlib.md5(
                content[offset:parts[k] + offset]).digest())
            manifest.append(
                {'path': '/bar_segments/mpu/%s/%d/%d/%d' % (
                    ts, len(content), parts[1], (k - 1)),
                 'size_bytes': parts[k],
                 'etag': hashlib.md5(
                     content[offset:parts[k] + offset]).hexdigest()})

            offset += parts[k]
        mpu_etag = '%s-%d' % (
            hashlib.md5(''.join(parts_etags)).hexdigest(), len(parts))

        path = '/v1/AUTH_foo/bar/mpu'
        body = StringIO.StringIO(content)
        wrapper = utils.SwiftMPUPutWrapper(
            body,
            {'Content-Length': len(content),
             'etag': mpu_etag,
             'x-timestamp': ts},
            path, swift, None, provider)
        result = []
        chunk = wrapper.read()
        while chunk:
            result.append(chunk)
            chunk = wrapper.read()
        self.assertEqual(content, ''.join(result))
        self.assertEqual(1 + len(parts) + 1, len(swift.calls))
        self.assertEqual('PUT', swift.calls[0]['REQUEST_METHOD'])
        self.assertEqual(
            '/v1/AUTH_foo/bar_segments', swift.calls[0]['PATH_INFO'])
        offset = 0
        for part in sorted(parts.keys()):
            self.assertEqual(
                'PUT', swift.calls[part]['REQUEST_METHOD'])
            self.assertEqual(
                '/v1/AUTH_foo/bar_segments/mpu/%s/%d/%d/%d' % (
                    ts, len(content), parts[1], part - 1),
                swift.calls[part]['PATH_INFO'])
            self.assertEqual(content[offset:offset + parts[part]],
                             swift.calls[part]['body'])
            offset += parts[part]
        self.assertEqual(manifest, json.loads(swift.calls[-1]['body']))
        self.assertEqual(path, swift.calls[-1]['PATH_INFO'])

    def test_mpu_error_does_not_upload_manifest(self):
        provider = mock.Mock(
            SyncS3({'aws_identity': 'key',
                    'aws_secret': 'secret',
                    'account': 'account',
                    'container': 'container',
                    'aws_bucket': 'bucket'}))
        parts = {1: 10,
                 2: 20,
                 3: 50,
                 4: 10}

        def _head_object(*args, **kwargs):
            return base_sync.ProviderResponse(
                True, 204, {'Content-Length': parts[kwargs['PartNumber']]}, '')

        provider.head_object.side_effect = _head_object

        responses = {'PUT': [
            # segments container PUT
            (lambda headers, body: ('200 OK', [], ''))] +
            [lambda headers, body: (
                '200 OK', {'etag': 'deadbeef'}, '')
             for _ in range(len(parts))]
        }
        swift = FakeSwift(responses)
        content = ''.join([chr(ord('A') + k) * parts[k]
                           for k in sorted(parts.keys())])
        etag_md5 = hashlib.md5()
        offset = 0
        for k in sorted(parts.keys()):
            etag_md5.update(
                hashlib.md5(content[offset:offset + parts[k]]).digest())
            offset += parts[k]
        mpu_etag = '%s-%d' % (etag_md5.hexdigest(), len(parts))
        ts = '1542760538.81'

        path = '/v1/AUTH_foo/bar/mpu'
        body = StringIO.StringIO(content)
        wrapper = utils.SwiftMPUPutWrapper(
            body,
            {'Content-Length': len(content),
             'x-timestamp': ts,
             'etag': mpu_etag},
            path, swift, None, provider)
        result = []
        chunk = wrapper.read()
        while chunk:
            result.append(chunk)
            chunk = wrapper.read()
        self.assertEqual(content, ''.join(result))
        self.assertEqual(1 + len(parts), len(swift.calls))
        self.assertEqual('PUT', swift.calls[0]['REQUEST_METHOD'])
        self.assertEqual(
            '/v1/AUTH_foo/bar_segments', swift.calls[0]['PATH_INFO'])
        offset = 0
        for part in sorted(parts.keys()):
            self.assertEqual(
                'PUT', swift.calls[part]['REQUEST_METHOD'])
            self.assertEqual(
                '/v1/AUTH_foo/bar_segments/mpu/%s/%d/%d/%d' % (
                    ts, len(content), parts[1], part - 1),
                swift.calls[part]['PATH_INFO'])
            self.assertEqual(content[offset:offset + parts[part]],
                             swift.calls[part]['body'])
            offset += parts[part]

    def test_stores_large_object(self):
        content = 'A' * 1024 + 'B' * 1337
        content_etag = hashlib.md5(content).hexdigest()

        segment_size = 2048
        parts = [segment_size, len(content) - segment_size]
        responses = {'PUT': [
            # segments container PUT
            (lambda headers, body: ('200 OK', [], ''))] +
            [lambda headers, body: (
                '200 OK', {'etag': hashlib.md5(body).hexdigest()}, '')
             for _ in range(len(parts))] +
            [lambda headers, body: ('200 OK', [], '')]}
        ts = '1542760538.81'
        swift = FakeSwift(responses)
        path = '/v1/AUTH_foo/bar/big-object'
        manifest = []
        offset = 0
        for index, part_size in enumerate(parts):
            manifest.append(
                {'path': '/bar_segments/big-object/%s/%d/%d/%d' % (
                    ts, len(content), segment_size, index),
                 'size_bytes': part_size,
                 'etag': hashlib.md5(
                     content[offset:part_size + offset]).hexdigest()})
            offset += part_size

        wrapper = utils.SwiftLargeObjectPutWrapper(
            StringIO.StringIO(content),
            {'Content-Length': len(content),
             'x-timestamp': ts,
             'etag': content_etag},
            path, swift, None, segment_size)
        result = []
        chunk = wrapper.read()
        while chunk:
            result.append(chunk)
            chunk = wrapper.read()

        self.assertEqual(content, ''.join(result))
        # segments container, two segments, and the manifest
        self.assertEqual(4, len(swift.calls))
        self.assertEqual('PUT', swift.calls[0]['REQUEST_METHOD'])
        self.assertEqual(
            '/v1/AUTH_foo/bar_segments', swift.calls[0]['PATH_INFO'])
        offset = 0
        for index, part_size in enumerate(parts):
            self.assertEqual(
                'PUT', swift.calls[index + 1]['REQUEST_METHOD'])
            self.assertEqual(
                '/v1/AUTH_foo/bar_segments/big-object/%s/%d/%d/%d' % (
                    ts, len(content), segment_size, index),
                swift.calls[index + 1]['PATH_INFO'])
            self.assertEqual(content[offset:offset + part_size],
                             swift.calls[index + 1]['body'])
            offset += part_size
        self.assertEqual(manifest, json.loads(swift.calls[-1]['body']))

    def test_big_object_error_does_not_upload_manifest(self):
        content = 'A' * 1024 + 'B' * 1337
        segment_size = 2048
        parts = [segment_size, len(content) - segment_size]
        responses = {'PUT': [
            # segments container PUT
            (lambda headers, body: ('200 OK', [], ''))] +
            [lambda headers, body: (
                '200 OK', {'etag': hashlib.md5(body).hexdigest()}, '')
             for _ in range(len(parts))] +
            [lambda headers, body: ('200 OK', [], '')]}
        ts = '1542760538.81'
        swift = FakeSwift(responses)
        path = '/v1/AUTH_foo/bar/big-object'

        wrapper = utils.SwiftLargeObjectPutWrapper(
            StringIO.StringIO(content),
            {'Content-Length': len(content),
             'x-timestamp': ts,
             'etag': 'deadbeef'},
            path, swift, None, segment_size)
        result = []
        chunk = wrapper.read()
        while chunk:
            result.append(chunk)
            chunk = wrapper.read()

        self.assertEqual(content, ''.join(result))
        # segments container, two segments, but no manifest
        self.assertEqual(3, len(swift.calls))
        self.assertEqual('PUT', swift.calls[0]['REQUEST_METHOD'])
        self.assertEqual(
            '/v1/AUTH_foo/bar_segments', swift.calls[0]['PATH_INFO'])
        offset = 0
        for index, part_size in enumerate(parts):
            self.assertEqual(
                'PUT', swift.calls[index + 1]['REQUEST_METHOD'])
            self.assertEqual(
                '/v1/AUTH_foo/bar_segments/big-object/%s/%d/%d/%d' % (
                    ts, len(content), segment_size, index),
                swift.calls[index + 1]['PATH_INFO'])
            self.assertEqual(content[offset:offset + part_size],
                             swift.calls[index + 1]['body'])
            offset += part_size
