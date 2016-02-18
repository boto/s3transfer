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
from s3transfer.download import SHUTDOWN_SENTINEL
from s3transfer.futures import BoundedExecutor
from s3transfer.utils import ShutdownQueue
from s3transfer.utils import OSUtils


class DownloadException(Exception):
    pass


class TestGetObjectTask(BaseTaskTest):
    def setUp(self):
        super(TestGetObjectTask, self).setUp()
        self.bucket = 'mybucket'
        self.key = 'mykey'
        self.extra_args = {}
        self.callbacks = []
        self.max_attempts = 5
        self.io_queue = ShutdownQueue(0)
        self.content = b'my content'
        self.stream = six.BytesIO(self.content)

    def get_download_task(self, **kwargs):
        default_kwargs = {
            'client': self.client, 'bucket': self.bucket, 'key': self.key,
            'extra_args': self.extra_args, 'callbacks': self.callbacks,
            'max_attempts': self.max_attempts, 'io_queue': self.io_queue
        }
        default_kwargs.update(kwargs)
        return self.get_task(GetObjectTask, main_kwargs=default_kwargs)

    def assert_io_queue_contents(self, expected_contents):
        io_queue_contents = []
        while not self.io_queue.empty():
            io_queue_contents.append(self.io_queue.get())
        self.assertEqual(io_queue_contents, expected_contents)

    def test_main(self):
        self.stubber.add_response(
            'get_object', service_response={'Body': self.stream},
            expected_params={'Bucket': self.bucket, 'Key': self.key}
        )
        task = self.get_download_task()
        task()

        self.stubber.assert_no_pending_responses()
        self.assert_io_queue_contents([(0, self.content)])

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
        self.assert_io_queue_contents([(0, self.content)])

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

        self.assert_io_queue_contents(expected_contents)

    def test_start_index(self):
        self.stubber.add_response(
            'get_object', service_response={'Body': self.stream},
            expected_params={'Bucket': self.bucket, 'Key': self.key}
        )
        task = self.get_download_task(start_index=5)
        task()

        self.stubber.assert_no_pending_responses()
        self.assert_io_queue_contents([(5, self.content)])

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
        self.assert_io_queue_contents([(0, self.content)])

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

    def test_queue_shutdown(self):
        self.io_queue.trigger_shutdown()
        self.stubber.add_response(
            'get_object', service_response={'Body': self.stream},
            expected_params={'Bucket': self.bucket, 'Key': self.key}
        )
        task = self.get_download_task()
        task()

        self.stubber.assert_no_pending_responses()
        # Make sure that no contents were added to the queue because of the
        # shutdown.
        self.assert_io_queue_contents([])


class TestIOWriteTask(BaseTaskTest):
    def setUp(self):
        super(TestIOWriteTask, self).setUp()
        self.files = FileCreator()
        self.io_queue = ShutdownQueue(0)
        self.osutil = OSUtils()
        self.temp_filename = os.path.join(self.files.rootdir, 'mytempfile')
        self.final_filename = os.path.join(self.files.rootdir, 'myfile')

    def tearDown(self):
        super(TestIOWriteTask, self).tearDown()
        self.files.remove_all()

    def get_io_write_task(self, **kwargs):
        default_kwargs = {
            'filename': self.temp_filename,
            'final_filename': self.final_filename,
            'osutil': self.osutil,
            'io_queue': self.io_queue
        }
        default_kwargs.update(kwargs)
        return self.get_task(
            IOWriteTask, main_kwargs=default_kwargs, is_final=True)

    def test_main(self):
        self.io_queue.put((0, b'foo'))
        self.io_queue.put((3, b'bar'))
        self.io_queue.put(SHUTDOWN_SENTINEL)

        task = self.get_io_write_task()
        task()

        with open(self.final_filename, 'rb') as f:
            self.assertEqual(f.read(), b'foobar')
        self.assertFalse(os.path.exists(self.temp_filename))

    def test_failure(self):
        self.io_queue.put((0, b'foo'))
        self.io_queue.put((3, b'bar'))

        task = self.get_io_write_task()

        executor = BoundedExecutor(0, 1)
        executor.submit(task)

        # Simulate an error is some other task and then send in the
        # shutdown signal.
        self.transfer_coordinator.set_exception(DownloadException())
        self.io_queue.put(SHUTDOWN_SENTINEL)

        # Confirm that it would have failed
        with self.assertRaises(DownloadException):
            self.transfer_coordinator.result()

        # Make sure the temporary file does not exist nor the final file
        self.assertFalse(os.path.exists(self.temp_filename))
        self.assertFalse(os.path.exists(self.final_filename))

    def test_exception_in_write(self):
        self.io_queue.put((0, b'foo'))
        self.io_queue.put(SHUTDOWN_SENTINEL)

        # Create a job that will fail because it is writing to a directory
        # that does not exist.
        self.temp_filename = os.path.join(
            self.files.rootdir, 'fakedir', 'myfile')
        task = self.get_io_write_task()
        task()

        # Confirm that it would have failed
        with self.assertRaises(IOError):
            self.transfer_coordinator.result()

        # Make sure the temporary file does not exist nor the final file
        self.assertFalse(os.path.exists(self.temp_filename))
        self.assertFalse(os.path.exists(self.final_filename))
