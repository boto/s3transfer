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

from tests import assert_files_equal
from tests import RecordingSubscriber
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
