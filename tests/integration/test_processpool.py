# Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
from tests.integration import BaseTransferManagerIntegTest
from s3transfer.processpool import ProcessTransferConfig
from s3transfer.processpool import ProcessPoolDownloader


class TestProcessPoolDownloader(BaseTransferManagerIntegTest):
    def setUp(self):
        super(TestProcessPoolDownloader, self).setUp()
        self.multipart_threshold = 5 * 1024 * 1024
        self.config = ProcessTransferConfig(
            multipart_threshold=self.multipart_threshold
        )
        self.client_kwargs = {'region_name': self.region}

    def create_process_pool_downloader(self, client_kwargs=None, config=None):
        if client_kwargs is None:
            client_kwargs = self.client_kwargs
        if config is None:
            config = self.config
        return ProcessPoolDownloader(
            client_kwargs=client_kwargs, config=config)

    def test_below_threshold(self):
        downloader = self.create_process_pool_downloader()
        filename = self.files.create_file_with_size(
            'foo.txt', filesize=1024 * 1024)
        self.upload_file(filename, '1mb.txt')

        download_path = os.path.join(self.files.rootdir, '1mb.txt')
        with downloader:
            downloader.download_file(
                self.bucket_name, '1mb.txt', download_path)
        assert_files_equal(filename, download_path)

    def test_above_threshold(self):
        downloader = self.create_process_pool_downloader()
        filename = self.files.create_file_with_size(
            'foo.txt', filesize=20 * 1024 * 1024)
        self.upload_file(filename, '20mb.txt')

        download_path = os.path.join(self.files.rootdir, '20mb.txt')
        with downloader:
            downloader.download_file(
                self.bucket_name, '20mb.txt', download_path)
        assert_files_equal(filename, download_path)
