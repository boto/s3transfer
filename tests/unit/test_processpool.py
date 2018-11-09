# Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import mock
from six.moves import queue
from botocore.exceptions import ClientError
from botocore.exceptions import ReadTimeoutError
from botocore.client import BaseClient

from tests import unittest
from tests import FileCreator
from tests import StreamWithError
from tests import StubbedClientTest
from s3transfer.compat import six
from s3transfer.exceptions import RetriesExceededError
from s3transfer.utils import OSUtils
from s3transfer.processpool import SHUTDOWN_SIGNAL
from s3transfer.processpool import DownloadFileRequest
from s3transfer.processpool import GetObjectJob
from s3transfer.processpool import ProcessTransferConfig
from s3transfer.processpool import TransferMonitor
from s3transfer.processpool import ClientFactory
from s3transfer.processpool import DownloadFilePlanner
from s3transfer.processpool import GetObjectWorker


class RenameFailingOSUtils(OSUtils):
    def __init__(self, exception):
        self.exception = exception

    def rename_file(self, current_filename, new_filename):
        raise self.exception


class TestClientFactory(unittest.TestCase):
    def test_create_client(self):
        client = ClientFactory().create_client()
        self.assertIsInstance(client, BaseClient)
        self.assertEqual(client.meta.service_model.service_name, 's3')

    def test_create_client_with_client_kwargs(self):
        client = ClientFactory({'region_name': 'myregion'}).create_client()
        self.assertEqual(client.meta.region_name, 'myregion')


class TestTransferMonitor(unittest.TestCase):
    def test_notify_get_exception(self):
        transfer_id = 1
        exception = Exception()
        monitor = TransferMonitor()
        monitor.notify_exception(transfer_id, exception)
        self.assertEqual(monitor.get_exception(transfer_id), exception)

    def test_get_no_exception(self):
        monitor = TransferMonitor()
        self.assertIsNone(monitor.get_exception(1))

    def test_notify_jobs(self):
        monitor = TransferMonitor()
        transfer_id = 1
        monitor.notify_expected_jobs_to_complete(transfer_id, 2)
        self.assertEqual(monitor.notify_job_complete(transfer_id), 1)
        self.assertEqual(monitor.notify_job_complete(transfer_id), 0)

    def test_notify_jobs_for_multiple_transfers(self):
        monitor = TransferMonitor()
        transfer_id = 1
        other_transfer_id = 2
        monitor.notify_expected_jobs_to_complete(transfer_id, 2)
        monitor.notify_expected_jobs_to_complete(other_transfer_id, 2)
        self.assertEqual(monitor.notify_job_complete(transfer_id), 1)
        self.assertEqual(monitor.notify_job_complete(other_transfer_id), 1)


class TestDownloadFilePlanner(StubbedClientTest):
    def setUp(self):
        super(TestDownloadFilePlanner, self).setUp()
        self.transfer_config = ProcessTransferConfig()
        self.client_factory = mock.Mock(ClientFactory)
        self.client_factory.create_client.return_value = self.client
        self.transfer_monitor = TransferMonitor()
        self.osutil = mock.Mock(OSUtils)
        self.download_request_queue = queue.Queue()
        self.worker_queue = queue.Queue()
        self.planner = DownloadFilePlanner(
            transfer_config=self.transfer_config,
            client_factory=self.client_factory,
            transfer_monitor=self.transfer_monitor,
            osutil=self.osutil,
            download_request_queue=self.download_request_queue,
            worker_queue=self.worker_queue,
        )
        self.transfer_id = 1
        self.bucket = 'bucket'
        self.key = 'key'
        self.filename = 'myfile'
        self.temp_filename = 'myfile.temp'
        self.osutil.get_temp_filename.return_value = self.temp_filename
        self.extra_args = {}
        self.expected_size = None

    def add_download_file_request(self, **override_kwargs):
        kwargs = {
            'transfer_id': self.transfer_id,
            'bucket': self.bucket,
            'key': self.key,
            'filename': self.filename,
            'extra_args': self.extra_args,
            'expected_size': self.expected_size
        }
        kwargs.update(override_kwargs)
        self.download_request_queue.put(DownloadFileRequest(**kwargs))

    def add_shutdown(self):
        self.download_request_queue.put(SHUTDOWN_SIGNAL)

    def assert_submitted_get_object_jobs(self, expected_jobs):
        actual_jobs = []
        while not self.worker_queue.empty():
            actual_jobs.append(self.worker_queue.get())
        self.assertEqual(actual_jobs, expected_jobs)

    def test_run_for_non_ranged_download(self):
        self.add_download_file_request(expected_size=1)
        self.add_shutdown()
        self.planner.run()
        self.osutil.truncate.assert_called_with(self.temp_filename, 1)
        self.assert_submitted_get_object_jobs([
            GetObjectJob(
                transfer_id=self.transfer_id,
                bucket=self.bucket,
                key=self.key,
                temp_filename=self.temp_filename,
                offset=0,
                extra_args={},
                filename=self.filename,
            )
        ])

    def test_run_for_ranged_download(self):
        self.transfer_config.multipart_chunksize = 2
        self.transfer_config.multipart_threshold = 4
        self.add_download_file_request(expected_size=4)
        self.add_shutdown()
        self.planner.run()
        self.osutil.truncate.assert_called_with(self.temp_filename, 4)
        self.assert_submitted_get_object_jobs([
            GetObjectJob(
                transfer_id=self.transfer_id,
                bucket=self.bucket,
                key=self.key,
                temp_filename=self.temp_filename,
                offset=0,
                extra_args={'Range': 'bytes=0-1'},
                filename=self.filename,
            ),
            GetObjectJob(
                transfer_id=self.transfer_id,
                bucket=self.bucket,
                key=self.key,
                temp_filename=self.temp_filename,
                offset=2,
                extra_args={'Range': 'bytes=2-'},
                filename=self.filename,
            ),
        ])

    def test_run_when_expected_size_not_provided(self):
        self.stubber.add_response(
            'head_object', {'ContentLength': 1},
            expected_params={
                'Bucket': self.bucket,
                'Key': self.key
            }
        )
        self.add_download_file_request(expected_size=None)
        self.add_shutdown()
        self.planner.run()
        self.stubber.assert_no_pending_responses()
        self.osutil.truncate.assert_called_with(self.temp_filename, 1)
        self.assert_submitted_get_object_jobs([
            GetObjectJob(
                transfer_id=self.transfer_id,
                bucket=self.bucket,
                key=self.key,
                temp_filename=self.temp_filename,
                offset=0,
                extra_args={},
                filename=self.filename,
            )
        ])

    def test_run_with_extra_args(self):
        self.stubber.add_response(
            'head_object', {'ContentLength': 1},
            expected_params={
                'Bucket': self.bucket,
                'Key': self.key,
                'VersionId': 'versionid'
            }
        )
        self.add_download_file_request(
            extra_args={'VersionId': 'versionid'},
            expected_size=None
        )
        self.add_shutdown()
        self.planner.run()
        self.stubber.assert_no_pending_responses()
        self.osutil.truncate.assert_called_with(self.temp_filename, 1)
        self.assert_submitted_get_object_jobs([
            GetObjectJob(
                transfer_id=self.transfer_id,
                bucket=self.bucket,
                key=self.key,
                temp_filename=self.temp_filename,
                offset=0,
                extra_args={'VersionId': 'versionid'},
                filename=self.filename,
            )
        ])

    def test_run_with_exception(self):
        self.stubber.add_client_error('head_object', 'NoSuchKey', 404)
        self.add_download_file_request(expected_size=None)
        self.add_shutdown()
        self.planner.run()
        self.stubber.assert_no_pending_responses()
        self.assert_submitted_get_object_jobs([])
        self.assertIsInstance(
            self.transfer_monitor.get_exception(self.transfer_id), ClientError)

    def test_run_with_os_error_in_allocating_temp_file(self):
        self.osutil.truncate.side_effect = OSError()
        self.add_download_file_request(expected_size=1)
        self.add_shutdown()
        self.planner.run()
        self.assert_submitted_get_object_jobs([])
        self.assertIsInstance(
            self.transfer_monitor.get_exception(self.transfer_id), OSError)
        self.osutil.remove_file.assert_called_with(self.temp_filename)

    def test_run_with_io_error_in_allocating_temp_file(self):
        self.osutil.truncate.side_effect = IOError()
        self.add_download_file_request(expected_size=1)
        self.add_shutdown()
        self.planner.run()
        self.assert_submitted_get_object_jobs([])
        self.assertIsInstance(
            self.transfer_monitor.get_exception(self.transfer_id), IOError)
        self.osutil.remove_file.assert_called_with(self.temp_filename)


class TestGetObjectWorker(StubbedClientTest):
    def setUp(self):
        super(TestGetObjectWorker, self).setUp()
        self.files = FileCreator()
        self.queue = queue.Queue()
        self.client_factory = mock.Mock(ClientFactory)
        self.client_factory.create_client.return_value = self.client
        self.transfer_monitor = TransferMonitor()
        self.osutil = OSUtils()
        self.worker = GetObjectWorker(
            queue=self.queue,
            client_factory=self.client_factory,
            transfer_monitor=self.transfer_monitor,
            osutil=self.osutil
        )
        self.transfer_id = 1
        self.bucket = 'bucket'
        self.key = 'key'
        self.remote_contents = b'my content'
        self.temp_filename = self.files.create_file('tempfile', '')
        self.extra_args = {}
        self.offset = 0
        self.final_filename = self.files.full_path('final_filename')
        self.stream = six.BytesIO(self.remote_contents)
        self.transfer_monitor.notify_expected_jobs_to_complete(
            self.transfer_id, 1000)

    def tearDown(self):
        super(TestGetObjectWorker, self).tearDown()
        self.files.remove_all()

    def add_get_object_job(self, **override_kwargs):
        kwargs = {
            'transfer_id': self.transfer_id,
            'bucket': self.bucket,
            'key': self.key,
            'temp_filename': self.temp_filename,
            'extra_args': self.extra_args,
            'offset': self.offset,
            'filename': self.final_filename
        }
        kwargs.update(override_kwargs)
        self.queue.put(GetObjectJob(**kwargs))

    def add_shutdown(self):
        self.queue.put(SHUTDOWN_SIGNAL)

    def add_stubbed_get_object_response(self, body=None, expected_params=None):
        if body is None:
            body = self.stream
        get_object_response = {'Body': body}

        if expected_params is None:
            expected_params = {
                'Bucket': self.bucket,
                'Key': self.key
            }

        self.stubber.add_response(
            'get_object', get_object_response, expected_params)

    def assert_contents(self, filename, contents):
        self.assertTrue(os.path.exists(filename))
        with open(filename, 'rb') as f:
            self.assertEqual(f.read(), contents)

    def assert_does_not_exist(self, filename):
        self.assertFalse(os.path.exists(filename))

    def test_run_is_final_job(self):
        self.add_get_object_job()
        self.add_shutdown()
        self.add_stubbed_get_object_response()
        self.transfer_monitor.notify_expected_jobs_to_complete(
            self.transfer_id, 1)

        self.worker.run()
        self.stubber.assert_no_pending_responses()
        self.assert_does_not_exist(self.temp_filename)
        self.assert_contents(self.final_filename, self.remote_contents)

    def test_run_jobs_is_not_final_job(self):
        self.add_get_object_job()
        self.add_shutdown()
        self.add_stubbed_get_object_response()
        self.transfer_monitor.notify_expected_jobs_to_complete(
            self.transfer_id, 1000)

        self.worker.run()
        self.stubber.assert_no_pending_responses()
        self.assert_contents(self.temp_filename, self.remote_contents)
        self.assert_does_not_exist(self.final_filename)

    def test_run_with_extra_args(self):
        self.add_get_object_job(extra_args={'VersionId': 'versionid'})
        self.add_shutdown()
        self.add_stubbed_get_object_response(
            expected_params={
                'Bucket': self.bucket,
                'Key': self.key,
                'VersionId': 'versionid'
            }
        )

        self.worker.run()
        self.stubber.assert_no_pending_responses()

    def test_run_with_offset(self):
        offset = 1
        self.add_get_object_job(offset=offset)
        self.add_shutdown()
        self.add_stubbed_get_object_response()

        self.worker.run()
        with open(self.temp_filename, 'rb') as f:
            f.seek(offset)
            self.assertEqual(f.read(), self.remote_contents)

    def test_run_error_in_get_object(self):
        self.add_get_object_job()
        self.add_shutdown()
        self.stubber.add_client_error('get_object', 'NoSuchKey', 404)
        self.add_stubbed_get_object_response()

        self.worker.run()
        self.assertIsInstance(
            self.transfer_monitor.get_exception(self.transfer_id), ClientError)

    def test_run_does_retries_for_get_object(self):
        self.add_get_object_job()
        self.add_shutdown()
        self.add_stubbed_get_object_response(
            body=StreamWithError(
                self.stream, ReadTimeoutError(endpoint_url='')))
        self.add_stubbed_get_object_response()

        self.worker.run()
        self.stubber.assert_no_pending_responses()
        self.assert_contents(self.temp_filename, self.remote_contents)

    def test_run_can_exhaust_retries_for_get_object(self):
        self.add_get_object_job()
        self.add_shutdown()
        # 5 is the current setting for max number of GetObject attempts
        for _ in range(5):
            self.add_stubbed_get_object_response(
                body=StreamWithError(
                    self.stream, ReadTimeoutError(endpoint_url='')))

        self.worker.run()
        self.stubber.assert_no_pending_responses()
        self.assertIsInstance(
            self.transfer_monitor.get_exception(self.transfer_id),
            RetriesExceededError
        )

    def test_run_skips_get_object_on_previous_exception(self):
        self.add_get_object_job()
        self.add_shutdown()
        self.transfer_monitor.notify_exception(self.transfer_id, Exception())

        self.worker.run()
        # Note we did not add a stubbed response for get_object
        self.stubber.assert_no_pending_responses()

    def test_run_final_job_removes_file_on_previous_exception(self):
        self.add_get_object_job()
        self.add_shutdown()
        self.transfer_monitor.notify_exception(self.transfer_id, Exception())
        self.transfer_monitor.notify_expected_jobs_to_complete(
            self.transfer_id, 1)

        self.worker.run()
        self.stubber.assert_no_pending_responses()
        self.assert_does_not_exist(self.temp_filename)
        self.assert_does_not_exist(self.final_filename)

    def test_run_fails_to_rename_file(self):
        exception = OSError()
        osutil = RenameFailingOSUtils(exception)
        self.worker = GetObjectWorker(
            queue=self.queue,
            client_factory=self.client_factory,
            transfer_monitor=self.transfer_monitor,
            osutil=osutil
        )
        self.add_get_object_job()
        self.add_shutdown()
        self.add_stubbed_get_object_response()
        self.transfer_monitor.notify_expected_jobs_to_complete(
            self.transfer_id, 1)

        self.worker.run()
        self.assertEqual(
            self.transfer_monitor.get_exception(self.transfer_id), exception)
        self.assert_does_not_exist(self.temp_filename)
        self.assert_does_not_exist(self.final_filename)
