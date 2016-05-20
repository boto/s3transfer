# Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.
import os.path
import shutil
import tempfile

from tests import unittest
from tests import RecordingSubscriber
from s3transfer.compat import six
from s3transfer.futures import TransferFuture
from s3transfer.futures import TransferMeta
from s3transfer.utils import get_callbacks
from s3transfer.utils import random_file_extension
from s3transfer.utils import invoke_progress_callbacks
from s3transfer.utils import calculate_range_parameter
from s3transfer.utils import CallArgs
from s3transfer.utils import FunctionContainer
from s3transfer.utils import OSUtils
from s3transfer.utils import ReadFileChunk
from s3transfer.utils import StreamReaderProgress


class TestGetCallbacks(unittest.TestCase):
    def setUp(self):
        self.subscriber = RecordingSubscriber()
        self.second_subscriber = RecordingSubscriber()
        self.call_args = CallArgs(subscribers=[
            self.subscriber, self.second_subscriber]
        )
        self.transfer_meta = TransferMeta(self.call_args)
        self.transfer_future = TransferFuture(self.transfer_meta)

    def test_get_callbacks(self):
        callbacks = get_callbacks(self.transfer_future, 'queued')
        # Make sure two callbacks were added as both subscribers had
        # an on_queued method.
        self.assertEqual(len(callbacks), 2)

        # Ensure that the callback was injected with the future by calling
        # one of them and checking that the future was used in the call.
        callbacks[0]()
        self.assertEqual(
            self.subscriber.on_queued_calls,
            [{'future': self.transfer_future}]
        )

    def test_get_callbacks_for_missing_type(self):
        callbacks = get_callbacks(self.transfer_future, 'fake_state')
        # There should be no callbacks as the subscribers will not have the
        # on_fake_state method
        self.assertEqual(len(callbacks), 0)


class TestCallArgs(unittest.TestCase):
    def test_call_args(self):
        call_args = CallArgs(foo='bar', biz='baz')
        self.assertEqual(call_args.foo, 'bar')
        self.assertEqual(call_args.biz, 'baz')


class TestFunctionContainer(unittest.TestCase):
    def get_args_kwargs(self, *args, **kwargs):
        return args, kwargs

    def test_call(self):
        func_container = FunctionContainer(
            self.get_args_kwargs, 'foo', bar='baz')
        self.assertEqual(func_container(), (('foo',), {'bar': 'baz'}))

    def test_repr(self):
        func_container = FunctionContainer(
            self.get_args_kwargs, 'foo', bar='baz')
        self.assertEqual(
            str(func_container), 'Function: %s with args %s and kwargs %s' % (
                self.get_args_kwargs, ('foo',), {'bar': 'baz'}))


class TestRandomFileExtension(unittest.TestCase):
    def test_has_proper_length(self):
        self.assertEqual(
            len(random_file_extension(num_digits=4)), 4)


class TestInvokeProgressCallbacks(unittest.TestCase):
    def test_invoke_progress_callbacks(self):
        recording_subscriber = RecordingSubscriber()
        invoke_progress_callbacks([recording_subscriber.on_progress], 2)
        self.assertEqual(recording_subscriber.calculate_bytes_seen(), 2)

    def test_invoke_progress_callbacks_with_no_progress(self):
        recording_subscriber = RecordingSubscriber()
        invoke_progress_callbacks([recording_subscriber.on_progress], 0)
        self.assertEqual(len(recording_subscriber.on_progress_calls), 0)


class TestCalculateRangeParameter(unittest.TestCase):
    def setUp(self):
        self.part_size = 5
        self.part_index = 1
        self.num_parts = 3

    def test_calculate_range_paramter(self):
        range_val = calculate_range_parameter(
            self.part_size, self.part_index, self.num_parts)
        self.assertEqual(range_val, 'bytes=5-9')

    def test_last_part_with_no_total_size(self):
        range_val = calculate_range_parameter(
            self.part_size, self.part_index, num_parts=2)
        self.assertEqual(range_val, 'bytes=5-')

    def test_last_part_with_total_size(self):
        range_val = calculate_range_parameter(
            self.part_size, self.part_index, num_parts=2, total_size=8)
        self.assertEqual(range_val, 'bytes=5-7')


class BaseUtilsTest(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        self.filename = os.path.join(self.tempdir, 'foo')
        self.content = b'abc'
        with open(self.filename, 'wb') as f:
            f.write(self.content)
        self.amounts_seen = []

    def tearDown(self):
        shutil.rmtree(self.tempdir)

    def callback(self, bytes_transferred):
        self.amounts_seen.append(bytes_transferred)


class TestOSUtils(BaseUtilsTest):
    def test_get_file_size(self):
        self.assertEqual(
            OSUtils().get_file_size(self.filename), len(self.content))

    def test_open_file_chunk_reader(self):
        reader = OSUtils().open_file_chunk_reader(
            self.filename, 0, 3, [self.callback])

        # The returned reader should be a ReadFileChunk.
        self.assertIsInstance(reader, ReadFileChunk)
        # The content of the reader should be correct.
        self.assertEqual(reader.read(), self.content)
        # Callbacks should be disabled depspite being passed in.
        self.assertEqual(self.amounts_seen, [])

    def test_open_file(self):
        fileobj = OSUtils().open(os.path.join(self.tempdir, 'foo'), 'w')
        self.assertTrue(hasattr(fileobj, 'write'))

    def test_remove_file_ignores_errors(self):
        non_existent_file = os.path.join(self.tempdir, 'no-exist')
        # This should not exist to start.
        self.assertFalse(os.path.exists(non_existent_file))
        try:
            OSUtils().remove_file(non_existent_file)
        except OSError as e:
            self.fail('OSError should have been caught: %s' % e)

    def test_remove_file_proxies_remove_file(self):
        OSUtils().remove_file(self.filename)
        self.assertFalse(os.path.exists(self.filename))

    def test_rename_file(self):
        new_filename = os.path.join(self.tempdir, 'newfoo')
        OSUtils().rename_file(self.filename, new_filename)
        self.assertFalse(os.path.exists(self.filename))
        self.assertTrue(os.path.exists(new_filename))


class TestReadFileChunk(BaseUtilsTest):
    def test_read_entire_chunk(self):
        filename = os.path.join(self.tempdir, 'foo')
        with open(filename, 'wb') as f:
            f.write(b'onetwothreefourfivesixseveneightnineten')
        chunk = ReadFileChunk.from_filename(
            filename, start_byte=0, chunk_size=3)
        self.assertEqual(chunk.read(), b'one')
        self.assertEqual(chunk.read(), b'')

    def test_read_with_amount_size(self):
        filename = os.path.join(self.tempdir, 'foo')
        with open(filename, 'wb') as f:
            f.write(b'onetwothreefourfivesixseveneightnineten')
        chunk = ReadFileChunk.from_filename(
            filename, start_byte=11, chunk_size=4)
        self.assertEqual(chunk.read(1), b'f')
        self.assertEqual(chunk.read(1), b'o')
        self.assertEqual(chunk.read(1), b'u')
        self.assertEqual(chunk.read(1), b'r')
        self.assertEqual(chunk.read(1), b'')

    def test_reset_stream_emulation(self):
        filename = os.path.join(self.tempdir, 'foo')
        with open(filename, 'wb') as f:
            f.write(b'onetwothreefourfivesixseveneightnineten')
        chunk = ReadFileChunk.from_filename(
            filename, start_byte=11, chunk_size=4)
        self.assertEqual(chunk.read(), b'four')
        chunk.seek(0)
        self.assertEqual(chunk.read(), b'four')

    def test_read_past_end_of_file(self):
        filename = os.path.join(self.tempdir, 'foo')
        with open(filename, 'wb') as f:
            f.write(b'onetwothreefourfivesixseveneightnineten')
        chunk = ReadFileChunk.from_filename(
            filename, start_byte=36, chunk_size=100000)
        self.assertEqual(chunk.read(), b'ten')
        self.assertEqual(chunk.read(), b'')
        self.assertEqual(len(chunk), 3)

    def test_tell_and_seek(self):
        filename = os.path.join(self.tempdir, 'foo')
        with open(filename, 'wb') as f:
            f.write(b'onetwothreefourfivesixseveneightnineten')
        chunk = ReadFileChunk.from_filename(
            filename, start_byte=36, chunk_size=100000)
        self.assertEqual(chunk.tell(), 0)
        self.assertEqual(chunk.read(), b'ten')
        self.assertEqual(chunk.tell(), 3)
        chunk.seek(0)
        self.assertEqual(chunk.tell(), 0)

    def test_file_chunk_supports_context_manager(self):
        filename = os.path.join(self.tempdir, 'foo')
        with open(filename, 'wb') as f:
            f.write(b'abc')
        with ReadFileChunk.from_filename(filename,
                                         start_byte=0,
                                         chunk_size=2) as chunk:
            val = chunk.read()
            self.assertEqual(val, b'ab')

    def test_iter_is_always_empty(self):
        # This tests the workaround for the httplib bug (see
        # the source for more info).
        filename = os.path.join(self.tempdir, 'foo')
        open(filename, 'wb').close()
        chunk = ReadFileChunk.from_filename(
            filename, start_byte=0, chunk_size=10)
        self.assertEqual(list(chunk), [])

    def test_callback_is_invoked_on_read(self):
        chunk = ReadFileChunk.from_filename(
            self.filename, start_byte=0, chunk_size=3,
            callbacks=[self.callback])
        chunk.read(1)
        chunk.read(1)
        chunk.read(1)
        self.assertEqual(self.amounts_seen, [1, 1, 1])

    def test_all_callbacks_invoked_on_read(self):
        chunk = ReadFileChunk.from_filename(
            self.filename, start_byte=0, chunk_size=3,
            callbacks=[self.callback, self.callback])
        chunk.read(1)
        chunk.read(1)
        chunk.read(1)
        # The list should be twice as long because there are two callbacks
        # recording the amount read.
        self.assertEqual(self.amounts_seen, [1, 1, 1, 1, 1, 1])

    def test_callback_can_be_disabled(self):
        chunk = ReadFileChunk.from_filename(
            self.filename, start_byte=0, chunk_size=3,
            callbacks=[self.callback])
        chunk.disable_callback()
        # Now reading from the ReadFileChunk should not invoke
        # the callback.
        chunk.read()
        self.assertEqual(self.amounts_seen, [])

    def test_callback_will_also_be_triggered_by_seek(self):
        chunk = ReadFileChunk.from_filename(
            self.filename, start_byte=0, chunk_size=3,
            callbacks=[self.callback])
        chunk.read(2)
        chunk.seek(0)
        chunk.read(2)
        chunk.seek(1)
        chunk.read(2)
        self.assertEqual(self.amounts_seen, [2, -2, 2, -1, 2])


class TestStreamReaderProgress(BaseUtilsTest):
    def test_proxies_to_wrapped_stream(self):
        original_stream = six.StringIO('foobarbaz')
        wrapped = StreamReaderProgress(original_stream)
        self.assertEqual(wrapped.read(), 'foobarbaz')

    def test_callback_invoked(self):
        original_stream = six.StringIO('foobarbaz')
        wrapped = StreamReaderProgress(
            original_stream, [self.callback, self.callback])
        self.assertEqual(wrapped.read(), 'foobarbaz')
        self.assertEqual(self.amounts_seen, [9, 9])
