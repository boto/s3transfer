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
import glob
from io import BytesIO
import os

from s3transfer.subscribers import BaseSubscriber
from s3transfer.utils import OSUtils
from tests import (
    HAS_CRT,
    NonSeekableReader,
    assert_files_equal,
    create_nonseekable_writer,
    requires_crt,
)
from tests.integration import BaseTransferManagerIntegTest

if HAS_CRT:
    from awscrt.exceptions import AwsCrtError

    import s3transfer.crt


class RecordingSubscriber(BaseSubscriber):
    def __init__(self):
        self.on_queued_called = False
        self.on_done_called = False
        self.bytes_transferred = 0

    def on_queued(self, **kwargs):
        self.on_queued_called = True

    def on_progress(self, future, bytes_transferred, **kwargs):
        self.bytes_transferred += bytes_transferred

    def on_done(self, **kwargs):
        self.on_done_called = True


class BaseCRTS3TransfersTest(BaseTransferManagerIntegTest):
    """Tests for the high level s3transfer based on CRT implementation."""

    def _create_s3_transfer(self):
        self.request_serializer = s3transfer.crt.BotocoreCRTRequestSerializer(
            self.session
        )
        credetial_resolver = self.session.get_component('credential_provider')
        self.s3_crt_client = s3transfer.crt.create_s3_crt_client(
            self.session.get_config_variable("region"), credetial_resolver
        )
        self.record_subscriber = RecordingSubscriber()
        self.osutil = OSUtils()
        return s3transfer.crt.CRTTransferManager(
            self.s3_crt_client, self.request_serializer
        )

    def _assert_has_public_read_acl(self, response):
        grants = response['Grants']
        public_read = [
            g['Grantee'].get('URI', '')
            for g in grants
            if g['Permission'] == 'READ'
        ]
        self.assertIn('groups/global/AllUsers', public_read[0])

    def _assert_subscribers_called(self, expected_bytes_transferred=None):
        self.assertTrue(self.record_subscriber.on_queued_called)
        self.assertTrue(self.record_subscriber.on_done_called)
        if expected_bytes_transferred:
            self.assertEqual(
                self.record_subscriber.bytes_transferred,
                expected_bytes_transferred,
            )


@requires_crt
class TestCRTUpload(BaseCRTS3TransfersTest):
    # CRTTransferManager upload tests. Defaults to using filepath, but
    # subclasses override the function below to use streaming fileobj instead.

    def get_input_fileobj(self, name, contents):
        # override this in subclasses to upload via fileobj instead of filepath
        mode = 'w' if isinstance(contents, str) else 'wb'
        return self.files.create_file(name, contents, mode)

    def get_input_fileobj_with_size(self, name, size):
        return self.get_input_fileobj(name, b'a' * size)

    def _assert_object_exists(self, key, expected_content_length, extra_args={}):
        self.assertTrue(self.object_exists(key, extra_args))
        response = self.client.head_object(Bucket=self.bucket_name, Key=key, **extra_args)
        self.assertEqual(response['ContentLength'], expected_content_length)

    def _test_basic_upload(self, key, file_size, extra_args=None):
        transfer = self._create_s3_transfer()
        file = self.get_input_fileobj_with_size(key, file_size)
        self.addCleanup(self.delete_object, key)

        with transfer:
            future = transfer.upload(
                file,
                self.bucket_name,
                key,
                extra_args,
                subscribers=[self.record_subscriber],
            )
            future.result()

        self._assert_object_exists(key, file_size)
        self._assert_subscribers_called(file_size)

    def test_below_multipart_chunksize(self):
        self._test_basic_upload('1mb.txt', file_size=1024 * 1024)

    def test_above_multipart_chunksize(self):
        self._test_basic_upload('20mb.txt', file_size=20 * 1024 * 1024)

    def test_empty_file(self):
        self._test_basic_upload('0mb.txt', file_size=0)

    def test_file_above_threshold_with_acl(self):
        key = '6mb.txt'
        file_size = 6 * 1024 * 1024
        extra_args = {'ACL': 'public-read'}
        self._test_basic_upload(key, file_size, extra_args)

        response = self.client.get_object_acl(
            Bucket=self.bucket_name, Key=key
        )
        self._assert_has_public_read_acl(response)

    def test_file_above_threshold_with_ssec(self):
        key_bytes = os.urandom(32)
        extra_args = {
            'SSECustomerKey': key_bytes,
            'SSECustomerAlgorithm': 'AES256',
        }
        key = '6mb.txt'
        file_size = 6 * 1024 * 1024
        transfer = self._create_s3_transfer()
        file = self.get_input_fileobj_with_size(key, file_size)
        self.addCleanup(self.delete_object, key)
        with transfer:
            future = transfer.upload(
                file,
                self.bucket_name,
                key,
                extra_args=extra_args,
                subscribers=[self.record_subscriber],
            )
            future.result()
        # A head object will fail if it has a customer key
        # associated with it and it's not provided in the HeadObject
        # request so we can use this to verify our functionality.
        original_extra_args = {
            'SSECustomerKey': key_bytes,
            'SSECustomerAlgorithm': 'AES256',
        }
        self._assert_object_exists(key, file_size, original_extra_args)
        response = self.client.head_object(
            Bucket=self.bucket_name, Key=key, **original_extra_args
        )
        self.assertEqual(response['SSECustomerAlgorithm'], 'AES256')
        self._assert_subscribers_called(file_size)

    def test_checksum_algorithm(self):
        key = 'sha1.txt'
        file_size = 1 * 1024 * 1024
        extra_args = {'ChecksumAlgorithm': 'SHA1'}
        self._test_basic_upload(key, file_size, extra_args)

        response = self.client.head_object(
            Bucket=self.bucket_name, Key=key, ChecksumMode='ENABLED',
        )
        self.assertIsNotNone(response.get('ChecksumSHA1'))

    def test_many_files(self):
        transfer = self._create_s3_transfer()
        keys = []
        file_size = 1024 * 1024
        files = []
        base_key = 'foo'
        suffix = '.txt'
        for i in range(10):
            key = base_key + str(i) + suffix
            keys.append(key)
            file = self.get_input_fileobj_with_size(key, file_size)
            files.append(file)
            self.addCleanup(self.delete_object, key)
        with transfer:
            for file, key in zip(files, keys):
                transfer.upload(file, self.bucket_name, key)

        for key in keys:
            self._assert_object_exists(key, file_size)

    def test_cancel(self):
        transfer = self._create_s3_transfer()
        key = '20mb.txt'
        file_size = 20 * 1024 * 1024
        file = self.get_input_fileobj_with_size(key, file_size)
        future = None
        try:
            with transfer:
                future = transfer.upload(file, self.bucket_name, key)
                raise KeyboardInterrupt()
        except KeyboardInterrupt:
            pass

        with self.assertRaises(AwsCrtError) as cm:
            future.result()
            self.assertEqual(cm.name, 'AWS_ERROR_S3_CANCELED')
        self.assertTrue(self.object_not_exists('20mb.txt'))


@requires_crt
class TestCRTUploadSeekableStream(TestCRTUpload):
    # Repeat upload tests, but use seekable streams
    def get_input_fileobj(self, name, contents):
        return BytesIO(contents)


@requires_crt
class TestCRTUploadNonSeekableStream(TestCRTUpload):
    # Repeat upload tests, but use nonseekable streams
    def get_input_fileobj(self, name, contents):
        return NonSeekableReader(contents)


@requires_crt
class TestCRTDownload(BaseCRTS3TransfersTest):
    # CRTTransferManager download tests. Defaults to using filepath, but
    # subclasses override the function below to use streaming fileobj instead.

    def get_output_fileobj(self, name):
        # override this in subclasses to download via fileobj instead of filepath
        return os.path.join(self.files.rootdir, name)

    def _assert_files_equal(self, orig_file, download_file):
        # download_file is either a path or a file-like object
        if isinstance(download_file, str):
            assert_files_equal(orig_file, download_file)
        else:
            download_file.close()
            assert_files_equal(orig_file, download_file.name)

    def _test_basic_download(self, file_size):
        transfer = self._create_s3_transfer()
        key = 'foo.txt'
        orig_file = self.files.create_file_with_size(key, file_size)
        self.upload_file(orig_file, key)

        download_file = self.get_output_fileobj('downloaded.txt')
        with transfer:
            future = transfer.download(
                self.bucket_name,
                key,
                download_file,
                subscribers=[self.record_subscriber],
            )
            future.result()
        self._assert_files_equal(orig_file, download_file)
        self._assert_subscribers_called(file_size)

    def test_below_threshold(self):
        self._test_basic_download(file_size=1024 * 1024)

    def test_above_threshold(self):
        self._test_basic_download(file_size=20 * 1024 * 1024)

    def test_empty_file(self):
        self._test_basic_download(file_size=0)

    def test_can_send_extra_params(self):
        # We're picking the customer provided sse feature
        # of S3 to test the extra_args functionality of S3.
        key_bytes = os.urandom(32)
        extra_args = {
            'SSECustomerKey': key_bytes,
            'SSECustomerAlgorithm': 'AES256',
        }
        key = 'foo.txt'
        orig_file = self.files.create_file(key, 'hello world')
        self.upload_file(orig_file, key, extra_args)

        transfer = self._create_s3_transfer()
        download_file = self.get_output_fileobj('downloaded.txt')
        with transfer:
            future = transfer.download(
                self.bucket_name,
                key,
                download_file,
                extra_args=extra_args,
                subscribers=[self.record_subscriber],
            )
            future.result()
        self._assert_files_equal(orig_file, download_file)
        self._assert_subscribers_called(len('hello world'))

    def test_many_files(self):
        transfer = self._create_s3_transfer()
        key = '1mb.txt'
        file_size = 1024 * 1024
        orig_file = self.files.create_file_with_size(key, file_size)
        self.upload_file(orig_file, key)

        files = []
        base_filename = os.path.join(self.files.rootdir, 'file')
        for i in range(10):
            files.append(self.get_output_fileobj(base_filename + str(i)))

        with transfer:
            for file in files:
                transfer.download(self.bucket_name, key, file)
        for download_file in files:
            self._assert_files_equal(orig_file, download_file)

    def test_cancel(self):
        transfer = self._create_s3_transfer()
        key = 'foo.txt'
        file_size = 20 * 1024 * 1024
        orig_file = self.files.create_file_with_size(key, file_size)
        self.upload_file(orig_file, key)

        download_file = self.get_output_fileobj('downloaded.txt')
        future = None
        try:
            with transfer:
                future = transfer.download(
                    self.bucket_name,
                    key,
                    download_file,
                    subscribers=[self.record_subscriber],
                )
                raise KeyboardInterrupt()
        except KeyboardInterrupt:
            pass

        with self.assertRaises(AwsCrtError) as err:
            future.result()
            self.assertEqual(err.name, 'AWS_ERROR_S3_CANCELED')

        # if passing filepath, assert that the file (and/or temp file) was removed
        if isinstance(download_file, str):
            possible_matches = glob.glob('%s*' % download_file)
            self.assertEqual(possible_matches, [])
        else:
            download_file.close()

        self._assert_subscribers_called()


@requires_crt
class TestCRTDownloadSeekableStream(TestCRTDownload):
    # Repeat download tests, but use seekable streams
    def get_output_fileobj(self, name):
        # Open stream to file on disk (vs just streaming to memory).
        # This lets tests check the results of a download in the same way
        # whether file path or file-like object was used.
        filepath = super().get_output_fileobj(name)
        return open(filepath, 'wb')


@requires_crt
class TestCRTDownloadNonSeekableStream(TestCRTDownload):
    # Repeat download tests, but use nonseekable streams
    def get_output_fileobj(self, name):
        filepath = super().get_output_fileobj(name)
        return create_nonseekable_writer(open(filepath, 'wb'))


@requires_crt
class TestCRTS3Transfers(BaseCRTS3TransfersTest):
    # for misc non-upload-or-download CRTTransferManager tests

    def test_delete(self):
        transfer = self._create_s3_transfer()
        key = 'foo.txt'
        filename = self.files.create_file_with_size(key, filesize=1)
        self.upload_file(filename, key)

        with transfer:
            future = transfer.delete(self.bucket_name, key)
            future.result()
        self.assertTrue(self.object_not_exists(key))

    def test_many_files_delete(self):
        transfer = self._create_s3_transfer()
        keys = []
        base_key = 'foo'
        suffix = '.txt'
        filename = self.files.create_file_with_size(
            '1mb.txt', filesize=1
        )
        for i in range(10):
            key = base_key + str(i) + suffix
            keys.append(key)
            self.upload_file(filename, key)

        with transfer:
            for key in keys:
                transfer.delete(self.bucket_name, key)
        for key in keys:
            self.assertTrue(self.object_not_exists(key))
