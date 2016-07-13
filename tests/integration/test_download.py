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
import glob
import os
import time

from concurrent.futures import CancelledError

from tests import assert_files_equal
from tests import RecordingSubscriber
from tests import NonSeekableWriter
from tests.integration import BaseTransferManagerIntegTest
from s3transfer.manager import TransferConfig


class TestDownload(BaseTransferManagerIntegTest):
    def setUp(self):
        super(TestDownload, self).setUp()
        self.multipart_threshold = 5 * 1024 * 1024
        self.config = TransferConfig(
            multipart_threshold=self.multipart_threshold
        )

    def test_below_threshold(self):
        transfer_manager = self.create_transfer_manager(self.config)

        filename = self.files.create_file_with_size(
            'foo.txt', filesize=1024 * 1024)
        self.upload_file(filename, '1mb.txt')

        download_path = os.path.join(self.files.rootdir, '1mb.txt')
        future = transfer_manager.download(
            self.bucket_name, '1mb.txt', download_path)
        future.result()
        assert_files_equal(filename, download_path)

    def test_above_threshold(self):
        transfer_manager = self.create_transfer_manager(self.config)

        filename = self.files.create_file_with_size(
            'foo.txt', filesize=20 * 1024 * 1024)
        self.upload_file(filename, '20mb.txt')

        download_path = os.path.join(self.files.rootdir, '20mb.txt')
        future = transfer_manager.download(
            self.bucket_name, '20mb.txt', download_path)
        future.result()
        assert_files_equal(filename, download_path)

    def test_large_download_exits_quicky_on_exception(self):
        transfer_manager = self.create_transfer_manager(self.config)

        filename = self.files.create_file_with_size(
            'foo.txt', filesize=20 * 1024 * 1024)
        self.upload_file(filename, '20mb.txt')

        download_path = os.path.join(self.files.rootdir, '20mb.txt')
        sleep_time = 0.5
        try:
            with transfer_manager:
                start_time = time.time()
                future = transfer_manager.download(
                    self.bucket_name, '20mb.txt', download_path)
                # Sleep for a little to get the transfer process going
                time.sleep(sleep_time)
                # Raise an exception which should cause the preceeding
                # download to cancel and exit quickly
                raise KeyboardInterrupt()
        except KeyboardInterrupt:
            pass
        end_time = time.time()
        # The maximum time allowed for the transfer manager to exit.
        # This means that it should take less than a couple second after
        # sleeping to exit.
        max_allowed_exit_time = sleep_time + 1
        self.assertTrue(end_time - start_time < max_allowed_exit_time)

        # Make sure the future was cancelled because of the KeyboardInterrupt
        with self.assertRaises(CancelledError):
            future.result()

        # Make sure the actual file and the temporary do not exist
        # by globbing for the file and any of its extensions
        possible_matches = glob.glob('%s*' % download_path)
        self.assertEqual(possible_matches, [])

    def test_progress_subscribers_on_download(self):
        subscriber = RecordingSubscriber()
        transfer_manager = self.create_transfer_manager(self.config)

        filename = self.files.create_file_with_size(
            'foo.txt', filesize=20 * 1024 * 1024)
        self.upload_file(filename, '20mb.txt')

        download_path = os.path.join(self.files.rootdir, '20mb.txt')

        future = transfer_manager.download(
            self.bucket_name, '20mb.txt', download_path,
            subscribers=[subscriber])
        future.result()
        self.assertEqual(subscriber.calculate_bytes_seen(), 20 * 1024 * 1024)

    def test_below_threshold_for_fileobj(self):
        transfer_manager = self.create_transfer_manager(self.config)

        filename = self.files.create_file_with_size(
            'foo.txt', filesize=1024 * 1024)
        self.upload_file(filename, '1mb.txt')

        download_path = os.path.join(self.files.rootdir, '1mb.txt')
        with open(download_path, 'wb') as f:
            future = transfer_manager.download(
                self.bucket_name, '1mb.txt', f)
            future.result()
        assert_files_equal(filename, download_path)

    def test_above_threshold_for_fileobj(self):
        transfer_manager = self.create_transfer_manager(self.config)

        filename = self.files.create_file_with_size(
            'foo.txt', filesize=20 * 1024 * 1024)
        self.upload_file(filename, '20mb.txt')

        download_path = os.path.join(self.files.rootdir, '20mb.txt')
        with open(download_path, 'wb') as f:
            future = transfer_manager.download(
                self.bucket_name, '20mb.txt', f)
            future.result()
        assert_files_equal(filename, download_path)

    def test_below_threshold_for_nonseekable_fileobj(self):
        transfer_manager = self.create_transfer_manager(self.config)

        filename = self.files.create_file_with_size(
            'foo.txt', filesize=1024 * 1024)
        self.upload_file(filename, '1mb.txt')

        download_path = os.path.join(self.files.rootdir, '1mb.txt')
        with open(download_path, 'wb') as f:
            future = transfer_manager.download(
                self.bucket_name, '1mb.txt', NonSeekableWriter(f))
            future.result()
        assert_files_equal(filename, download_path)

    def test_above_threshold_for_nonseekable_fileobj(self):
        transfer_manager = self.create_transfer_manager(self.config)

        filename = self.files.create_file_with_size(
            'foo.txt', filesize=20 * 1024 * 1024)
        self.upload_file(filename, '20mb.txt')

        download_path = os.path.join(self.files.rootdir, '20mb.txt')
        with open(download_path, 'wb') as f:
            future = transfer_manager.download(
                self.bucket_name, '20mb.txt', NonSeekableWriter(f))
            future.result()
        assert_files_equal(filename, download_path)
