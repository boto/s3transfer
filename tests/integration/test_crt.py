# Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import glob

from tests.integration import BaseTransferManagerIntegTest
from tests import assert_files_equal

import s3transfer.crt as crt
from awscrt.exceptions import AwsCrtError


class TestCRTS3Transfers(BaseTransferManagerIntegTest):
    """Tests for the high level s3transfer based on CRT implementation."""

    def _create_s3_transfer(self, config=None):
        return crt.CRTTransferManager(
            self.session, config=config
        )

    def _assert_has_public_read_acl(self, response):
        grants = response['Grants']
        public_read = [g['Grantee'].get('URI', '') for g in grants
                       if g['Permission'] == 'READ']
        self.assertIn('groups/global/AllUsers', public_read[0])

    def test_upload_below_multipart_chunksize(self):
        config = crt.CRTTransferConfig(
            multipart_chunksize=5 * 1024 * 1024)
        transfer = self._create_s3_transfer(config)
        filename = self.files.create_file_with_size(
            'foo.txt', filesize=1024 * 1024)
        self.addCleanup(self.delete_object, 'foo.txt')

        with transfer:
            future = transfer.upload(self.bucket_name,
                                     'foo.txt', filename)
            future.result()

        self.assertTrue(self.object_exists('foo.txt'))

    def test_upload_above_multipart_chunksize(self):
        config = crt.CRTTransferConfig(
            multipart_chunksize=5 * 1024 * 1024)
        transfer = self._create_s3_transfer(config)
        filename = self.files.create_file_with_size(
            '20mb.txt', filesize=20 * 1024 * 1024)
        self.addCleanup(self.delete_object, '20mb.txt')

        with transfer:
            future = transfer.upload(self.bucket_name,
                                     '20mb.txt', filename)
            future.result()
        self.assertTrue(self.object_exists('20mb.txt'))

    def test_upload_file_above_threshold_with_acl(self):
        config = crt.CRTTransferConfig(
            multipart_chunksize=5 * 1024 * 1024)
        transfer = self._create_s3_transfer(config)
        filename = self.files.create_file_with_size(
            '6mb.txt', filesize=6 * 1024 * 1024)
        extra_args = {'ACL': 'public-read'}
        self.addCleanup(self.delete_object, '6mb.txt')

        with transfer:
            future = transfer.upload(self.bucket_name,
                                     '6mb.txt', filename, extra_args=extra_args)
            future.result()

        self.assertTrue(self.object_exists('6mb.txt'))
        response = self.client.get_object_acl(
            Bucket=self.bucket_name, Key='6mb.txt')
        self._assert_has_public_read_acl(response)

    def test_upload_file_above_threshold_with_ssec(self):
        key_bytes = os.urandom(32)
        extra_args = {
            'SSECustomerKey': key_bytes,
            'SSECustomerAlgorithm': 'AES256',
        }
        config = crt.CRTTransferConfig(
            multipart_chunksize=5 * 1024 * 1024)
        transfer = self._create_s3_transfer(config)
        filename = self.files.create_file_with_size(
            '6mb.txt', filesize=6 * 1024 * 1024)
        self.addCleanup(self.delete_object, '6mb.txt')
        with transfer:
            future = transfer.upload(self.bucket_name,
                                     '6mb.txt', filename, extra_args=extra_args)
            future.result()
        # A head object will fail if it has a customer key
        # associated with it and it's not provided in the HeadObject
        # request so we can use this to verify our functionality.
        oringal_extra_args = {
            'SSECustomerKey': key_bytes,
            'SSECustomerAlgorithm': 'AES256',
        }
        self.wait_object_exists('6mb.txt', oringal_extra_args)
        response = self.client.head_object(
            Bucket=self.bucket_name,
            Key='6mb.txt', **oringal_extra_args)
        self.assertEqual(response['SSECustomerAlgorithm'], 'AES256')

    def test_can_send_extra_params_on_download(self):
        # We're picking the customer provided sse feature
        # of S3 to test the extra_args functionality of
        # S3.
        key_bytes = os.urandom(32)
        extra_args = {
            'SSECustomerKey': key_bytes,
            'SSECustomerAlgorithm': 'AES256',
        }
        filename = self.files.create_file('foo.txt', 'hello world')
        self.upload_file(filename, 'foo.txt', extra_args)
        transfer = self._create_s3_transfer()

        download_path = os.path.join(self.files.rootdir, 'downloaded.txt')
        with transfer:
            future = transfer.download(self.bucket_name, 'foo.txt',
                                       download_path, extra_args=extra_args)
            future.result()
        with open(download_path, 'rb') as f:
            self.assertEqual(f.read(), b'hello world')

    def test_download_below_threshold(self):
        transfer = self._create_s3_transfer()
        filename = self.files.create_file_with_size(
            'foo.txt', filesize=1024 * 1024)
        self.upload_file(filename, 'foo.txt')

        download_path = os.path.join(self.files.rootdir, 'downloaded.txt')
        with transfer:
            future = transfer.download(self.bucket_name, 'foo.txt',
                                       download_path)
            future.result()
        assert_files_equal(filename, download_path)

    def test_download_above_threshold(self):
        transfer = self._create_s3_transfer()
        filename = self.files.create_file_with_size(
            'foo.txt', filesize=20 * 1024 * 1024)
        self.upload_file(filename, 'foo.txt')

        download_path = os.path.join(self.files.rootdir, 'downloaded.txt')
        with transfer:
            future = transfer.download(self.bucket_name, 'foo.txt',
                                       download_path)
            future.result()
        assert_files_equal(filename, download_path)

    def test_delete(self):
        transfer = self._create_s3_transfer()
        filename = self.files.create_file_with_size(
            'foo.txt', filesize=1024 * 1024)
        self.upload_file(filename, 'foo.txt')

        with transfer:
            future = transfer.delete(self.bucket_name, 'foo.txt')
            future.result()
        self.assertTrue(self.object_not_exists('foo.txt'))

    def test_many_files_download(self):
        transfer = self._create_s3_transfer()

        filename = self.files.create_file_with_size(
            '1mb.txt', filesize=1024 * 1024)
        self.upload_file(filename, '1mb.txt')

        filenames = []
        base_filename = os.path.join(self.files.rootdir, 'file')
        for i in range(10):
            filenames.append(base_filename + str(i))

        with transfer:
            for filename in filenames:
                transfer.download(
                    self.bucket_name, '1mb.txt', filename)
        for download_path in filenames:
            assert_files_equal(filename, download_path)

    def test_many_files_upload(self):
        transfer = self._create_s3_transfer()
        keys = []
        filenames = []
        base_key = 'foo'
        sufix = '.txt'
        for i in range(10):
            key = base_key + str(i) + sufix
            keys.append(key)
            filename = self.files.create_file_with_size(
                key, filesize=1024 * 1024)
            filenames.append(filename)
            self.addCleanup(self.delete_object, key)
        with transfer:
            for filename, key in zip(filenames, keys):
                transfer.upload(
                    self.bucket_name, key, filename)

        for key in keys:
            self.assertTrue(self.object_exists(key))

    def test_many_files_delete(self):
        transfer = self._create_s3_transfer()
        keys = []
        base_key = 'foo'
        sufix = '.txt'
        filename = self.files.create_file_with_size(
            '1mb.txt', filesize=1024 * 1024)
        for i in range(10):
            key = base_key + str(i) + sufix
            keys.append(key)
            self.upload_file(filename, key)

        with transfer:
            for key in keys:
                transfer.delete(self.bucket_name, key)
        for key in keys:
            self.assertTrue(self.object_not_exists(key))

    def test_upload_cancel(self):
        config = crt.CRTTransferConfig(
            multipart_chunksize=5 * 1024 * 1024)
        transfer = self._create_s3_transfer(config)
        filename = self.files.create_file_with_size(
            '20mb.txt', filesize=20 * 1024 * 1024)
        future = None
        try:
            with transfer:
                future = transfer.upload(self.bucket_name,
                                         '20mb.txt', filename)
                raise KeyboardInterrupt()
        except KeyboardInterrupt:
            pass

        with self.assertRaises(AwsCrtError) as cm:
            future.result()
            self.assertEqual(cm.name, 'AWS_ERROR_S3_CANCELED')
        self.assertTrue(self.object_not_exists('20mb.txt'))

    def test_download_cancel(self):
        transfer = self._create_s3_transfer()
        filename = self.files.create_file_with_size(
            'foo.txt', filesize=20 * 1024 * 1024)
        self.upload_file(filename, 'foo.txt')

        download_path = os.path.join(self.files.rootdir, 'downloaded.txt')
        future = None
        try:
            with transfer:
                future = transfer.download(self.bucket_name, 'foo.txt',
                                           download_path)
                raise KeyboardInterrupt()
        except KeyboardInterrupt:
            pass

        with self.assertRaises(AwsCrtError) as cm:
            future.result()
            self.assertEqual(cm.name, 'AWS_ERROR_S3_CANCELED')

        possible_matches = glob.glob('%s*' % download_path)
        self.assertEqual(possible_matches, [])
