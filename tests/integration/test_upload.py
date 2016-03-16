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
from tests import UnseekableStream
from tests import RecordingSubscriber
from tests.integration import BaseTransferManagerIntegTest
from s3transfer.manager import TransferConfig


class TestUpload(BaseTransferManagerIntegTest):
    def setUp(self):
        super(TestUpload, self).setUp()
        self.multipart_threshold = 5 * 1024 * 1024
        self.config = TransferConfig(
            multipart_threshold=self.multipart_threshold)

    def test_upload_below_threshold(self):
        transfer_manager = self.create_transfer_manager(self.config)
        filename = self.files.create_file_with_size(
            '1mb.txt', filesize=1024 * 1024)
        future = transfer_manager.upload(filename, self.bucket_name, '1mb.txt')
        self.addCleanup(self.delete_object, '1mb.txt')

        future.result()
        self.assertTrue(self.object_exists('1mb.txt'))

    def test_upload_above_threshold(self):
        transfer_manager = self.create_transfer_manager(self.config)
        filename = self.files.create_file_with_size(
            '20mb.txt', filesize=20 * 1024 * 1024)
        future = transfer_manager.upload(
            filename, self.bucket_name, '20mb.txt')
        self.addCleanup(self.delete_object, '20mb.txt')

        future.result()
        self.assertTrue(self.object_exists('20mb.txt'))

    def test_upload_open_file_below_threshold(self):
        transfer_manager = self.create_transfer_manager(self.config)
        filename = self.files.create_file_with_size(
            '1mb.txt', filesize=1024 * 1024)
        with open(filename, 'rb') as f:
            future = transfer_manager.upload(f, self.bucket_name, '1mb.txt')
            self.addCleanup(self.delete_object, '1mb.txt')

        future.result()
        self.assertTrue(self.object_exists('1mb.txt'))

    def test_upload_open_file_above_threshold(self):
        transfer_manager = self.create_transfer_manager(self.config)
        filename = self.files.create_file_with_size(
            '20mb.txt', filesize=20 * 1024 * 1024)
        with open(filename, 'rb') as f:
            future = transfer_manager.upload(f, self.bucket_name, '20mb.txt')
            self.addCleanup(self.delete_object, '20mb.txt')

        future.result()
        self.assertTrue(self.object_exists('20mb.txt'))

    def test_upload_unseekable_stream_below_threshold(self):
        transfer_manager = self.create_transfer_manager(self.config)
        filename = self.files.create_file_with_size(
            '1mb.txt', filesize=1024 * 1024)
        with open(filename, 'rb') as f:
            with UnseekableStream(f) as stream:            
                future = transfer_manager.upload(
                    stream, self.bucket_name, '1mb.txt')
                self.addCleanup(self.delete_object, '1mb.txt')

        future.result()
        self.assertTrue(self.object_exists('1mb.txt'))

    def test_upload_unseekable_stream_above_threshold(self):
        transfer_manager = self.create_transfer_manager(self.config)
        filename = self.files.create_file_with_size(
            '20mb.txt', filesize=20 * 1024 * 1024)
        with open(filename, 'rb') as f:
            with UnseekableStream(f) as stream:            
                future = transfer_manager.upload(
                    stream, self.bucket_name, '20mb.txt')
                self.addCleanup(self.delete_object, '20mb.txt')

        future.result()
        self.assertTrue(self.object_exists('20mb.txt'))

    def test_progress_subscribers_on_upload(self):
        subscriber = RecordingSubscriber()
        transfer_manager = self.create_transfer_manager(self.config)
        filename = self.files.create_file_with_size(
            '20mb.txt', filesize=20 * 1024 * 1024)
        future = transfer_manager.upload(
            filename, self.bucket_name, '20mb.txt',
            subscribers=[subscriber])
        self.addCleanup(self.delete_object, '20mb.txt')

        future.result()
        # The callback should have been called enough times such that
        # the total amount of bytes we've seen (via the "amount"
        # arg to the callback function) should be the size
        # of the file we uploaded.
        self.assertEqual(subscriber.calculate_bytes_seen(), 20 * 1024 * 1024)
