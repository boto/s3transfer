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
import os

from tests import BaseTaskTest
from tests import StreamWithError
from tests import FileCreator
from s3transfer.compat import six
from s3transfer.compat import SOCKET_ERROR
from s3transfer.exceptions import RetriesExceededError
from s3transfer.download import GetObjectTask
from s3transfer.download import IOWriteTask
from s3transfer.download import IORenameFileTask
from s3transfer.futures import BoundedExecutor
from s3transfer.utils import OSUtils


class DownloadException(Exception):
    pass


class WriteCollector(object):
    """A utility to collect information about writes and seeks"""
    def __init__(self):
        self._pos = 0
        self.writes = []

    def seek(self, pos):
        self._pos = pos

    def write(self, data):
        self.writes.append((self._pos, data))


class CancelledStreamWrapper(object):
    """A wrapper to trigger a cancellation while stream reading

    Forces the transfer coordinator to cancel after a certain amount of reads
    :param stream: The underlying stream to read from
    :param transfer_coordinator: The coordinator for the transfer
    :param num_reads: On which read to sigal a cancellation. 0 is the first
        read.
    """
    def __init__(self, stream, transfer_coordinator, num_reads=0):
        self._stream = stream
        self._transfer_coordinator = transfer_coordinator
        self._num_reads = num_reads
        self._count = 0

    def read(self, *args, **kwargs):
        if self._num_reads == self._count:
            self._transfer_coordinator.cancel()
        self._stream.read(*args, **kwargs)
        self._count += 1


class TestGetObjectTask(BaseTaskTest):
    def setUp(self):
        super(TestGetObjectTask, self).setUp()
        self.bucket = 'mybucket'
        self.key = 'mykey'
        self.extra_args = {}
        self.callbacks = []
        self.max_attempts = 5
        self.io_executor = BoundedExecutor(0, 1)
        self.content = b'my content'
        self.stream = six.BytesIO(self.content)
        self.f = WriteCollector()

    def get_download_task(self, **kwargs):
        default_kwargs = {
            'client': self.client, 'bucket': self.bucket, 'key': self.key,
            'f': self.f, 'extra_args': self.extra_args,
            'callbacks': self.callbacks,
            'max_attempts': self.max_attempts, 'io_executor': self.io_executor
        }
        default_kwargs.update(kwargs)
        return self.get_task(GetObjectTask, main_kwargs=default_kwargs)

    def assert_io_writes(self, expected_writes):
        # Let the io executor process all of the writes before checking
        # what writes were sent to it.
        self.io_executor.shutdown()
        self.assertEqual(self.f.writes, expected_writes)

    def test_main(self):
        self.stubber.add_response(
            'get_object', service_response={'Body': self.stream},
            expected_params={'Bucket': self.bucket, 'Key': self.key}
        )
        task = self.get_download_task()
        task()

        self.stubber.assert_no_pending_responses()
        self.assert_io_writes([(0, self.content)])

    def test_extra_args(self):
        self.stubber.add_response(
            'get_object', service_response={'Body': self.stream},
            expected_params={
                'Bucket': self.bucket, 'Key': self.key, 'Range': 'bytes=0-'
            }
        )
        self.extra_args['Range'] = 'bytes=0-'
        task = self.get_download_task()
        task()

        self.stubber.assert_no_pending_responses()
        self.assert_io_writes([(0, self.content)])

    def test_control_chunk_size(self):
        self.stubber.add_response(
            'get_object', service_response={'Body': self.stream},
            expected_params={'Bucket': self.bucket, 'Key': self.key}
        )
        task = self.get_download_task()
        task.STREAM_CHUNK_SIZE = 1
        task()

        self.stubber.assert_no_pending_responses()
        expected_contents = []
        for i in range(len(self.content)):
            expected_contents.append((i, bytes(self.content[i:i+1])))

        self.assert_io_writes(expected_contents)

    def test_start_index(self):
        self.stubber.add_response(
            'get_object', service_response={'Body': self.stream},
            expected_params={'Bucket': self.bucket, 'Key': self.key}
        )
        task = self.get_download_task(start_index=5)
        task()

        self.stubber.assert_no_pending_responses()
        self.assert_io_writes([(5, self.content)])

    def test_retries_succeeds(self):
        self.stubber.add_response(
            'get_object', service_response={
                'Body': StreamWithError(SOCKET_ERROR)
            },
            expected_params={'Bucket': self.bucket, 'Key': self.key}
        )
        self.stubber.add_response(
            'get_object', service_response={'Body': self.stream},
            expected_params={'Bucket': self.bucket, 'Key': self.key}
        )
        task = self.get_download_task()
        task()

        # Retryable error should have not affected the bytes placed into
        # the io queue.
        self.stubber.assert_no_pending_responses()
        self.assert_io_writes([(0, self.content)])

    def test_retries_failure(self):
        for _ in range(self.max_attempts):
            self.stubber.add_response(
                'get_object', service_response={
                    'Body': StreamWithError(SOCKET_ERROR)
                },
                expected_params={'Bucket': self.bucket, 'Key': self.key}
            )

        task = self.get_download_task()
        task()
        self.transfer_coordinator.announce_done()

        # Should have failed out on a RetriesExceededError
        with self.assertRaises(RetriesExceededError):
            self.transfer_coordinator.result()
        self.stubber.assert_no_pending_responses()

    def test_cancels_out_of_queueing(self):
        self.stubber.add_response(
            'get_object',
            service_response={
                'Body': CancelledStreamWrapper(
                    self.stream, self.transfer_coordinator)
            },
            expected_params={'Bucket': self.bucket, 'Key': self.key}
        )
        task = self.get_download_task()
        task()

        self.stubber.assert_no_pending_responses()
        # Make sure that no contents were added to the queue because the task
        # should have been canceled before trying to add the contents to the
        # io queue.
        self.assert_io_writes([])


class BaseIOTaskTest(BaseTaskTest):
    def setUp(self):
        super(BaseIOTaskTest, self).setUp()
        self.files = FileCreator()
        self.osutil = OSUtils()
        self.temp_filename = os.path.join(self.files.rootdir, 'mytempfile')
        self.final_filename = os.path.join(self.files.rootdir, 'myfile')

    def tearDown(self):
        super(BaseIOTaskTest, self).tearDown()
        self.files.remove_all()


class TestIOWriteTask(BaseIOTaskTest):
    def test_main(self):
        with open(self.temp_filename, 'wb') as f:
            # Write once to the file
            task = self.get_task(
                IOWriteTask,
                main_kwargs={
                    'f': f,
                    'data': b'foo',
                    'offset': 0
                }
            )
            task()

            # Write again to the file
            task = self.get_task(
                IOWriteTask,
                main_kwargs={
                    'f': f,
                    'data': b'bar',
                    'offset': 3
                }
            )
            task()

        with open(self.temp_filename, 'rb') as f:
            self.assertEqual(f.read(), b'foobar')


class TestIORenameFileTask(BaseIOTaskTest):
    def test_main(self):
        with open(self.temp_filename, 'wb') as f:
            task = self.get_task(
                IORenameFileTask,
                main_kwargs={
                    'f': f,
                    'final_filename': self.final_filename,
                    'osutil': self.osutil
                }
            )
            task()
        self.assertTrue(os.path.exists(self.final_filename))
        self.assertFalse(os.path.exists(self.temp_filename))
