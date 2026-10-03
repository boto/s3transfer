# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
# https://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.
import base64
import hashlib

from botocore.exceptions import ClientError

from s3transfer.exceptions import (
    S3ObjectChecksumUnavailableError,
    S3ValidationError,
)
from s3transfer.idempotency import IdempotentUploader
from tests import FileCreator, mock, unittest


class TestIdempotentUploader(unittest.TestCase):
    def setUp(self):
        self.client = mock.Mock()
        self.manager = mock.Mock()
        self.future = self.manager.upload.return_value
        self.file_creator = FileCreator()
        self.contents = b'my content'
        self.filename = self.file_creator.create_file(
            'my-file', self.contents, mode='wb'
        )
        self.bucket = 'my-bucket'
        self.key = 'my-key'
        self.checksum = self._sha256(self.contents)
        self.uploader = IdempotentUploader(self.client, self.manager)

    def tearDown(self):
        self.file_creator.remove_all()

    def _sha256(self, contents):
        digest = hashlib.sha256(contents).digest()
        return base64.b64encode(digest).decode('ascii')

    def _client_error(self, code, status_code):
        return ClientError(
            {
                'Error': {'Code': code, 'Message': 'error'},
                'ResponseMetadata': {'HTTPStatusCode': status_code},
            },
            'HeadObject',
        )

    def _upload(self, extra_args=None, subscribers=None):
        return self.uploader.upload(
            self.filename,
            self.bucket,
            self.key,
            extra_args,
            subscribers,
        )

    def test_missing_object_uploads_with_if_none_match(self):
        self.client.head_object.side_effect = self._client_error('404', 404)
        extra_args = {
            'Metadata': {'purpose': 'test'},
            'RequestPayer': 'requester',
        }
        subscribers = [mock.sentinel.SUBSCRIBER]

        result = self._upload(extra_args, subscribers)

        self.assertTrue(result)
        self.client.head_object.assert_called_once_with(
            Bucket=self.bucket,
            Key=self.key,
            ChecksumMode='ENABLED',
            RequestPayer='requester',
        )
        self.manager.upload.assert_called_once_with(
            self.filename,
            self.bucket,
            self.key,
            {
                'Metadata': {'purpose': 'test'},
                'RequestPayer': 'requester',
                'IfNoneMatch': '*',
                'ChecksumSHA256': self.checksum,
            },
            subscribers,
        )
        self.future.result.assert_called_once_with()

    def test_matching_checksum_skips_upload(self):
        self.client.head_object.return_value = {
            'ChecksumSHA256': self.checksum,
            'ChecksumType': 'FULL_OBJECT',
            'ETag': '"etag"',
        }

        result = self._upload({'Metadata': {'changed': 'but ignored'}})

        self.assertFalse(result)
        self.manager.upload.assert_not_called()

    def test_checksum_without_type_is_treated_as_full_object(self):
        self.client.head_object.return_value = {
            'ChecksumSHA256': self.checksum,
            'ETag': '"etag"',
        }

        result = self._upload()

        self.assertFalse(result)
        self.manager.upload.assert_not_called()

    def test_different_checksum_uploads_with_if_match(self):
        self.client.head_object.return_value = {
            'ChecksumSHA256': self._sha256(b'old content'),
            'ChecksumType': 'FULL_OBJECT',
            'ETag': '"etag"',
        }

        result = self._upload()

        self.assertTrue(result)
        self.manager.upload.assert_called_once_with(
            self.filename,
            self.bucket,
            self.key,
            {'IfMatch': '"etag"', 'ChecksumSHA256': self.checksum},
            None,
        )

    def test_empty_file_checksum(self):
        self.filename = self.file_creator.create_file(
            'empty-file', b'', mode='wb'
        )
        self.checksum = self._sha256(b'')
        self.client.head_object.side_effect = self._client_error(
            'NoSuchKey', 404
        )

        result = self._upload()

        self.assertTrue(result)
        upload_args = self.manager.upload.call_args.args[3]
        self.assertEqual(upload_args['ChecksumSHA256'], self.checksum)

    def test_caller_extra_args_are_not_modified(self):
        self.client.head_object.side_effect = self._client_error(
            'NotFound', 404
        )
        extra_args = {'Metadata': {'purpose': 'test'}}

        self._upload(extra_args)

        self.assertEqual(extra_args, {'Metadata': {'purpose': 'test'}})

    def test_only_supported_extra_args_are_sent_to_head_object(self):
        self.client.head_object.side_effect = self._client_error('404', 404)
        extra_args = {
            'ACL': 'private',
            'ExpectedBucketOwner': '123456789012',
            'RequestPayer': 'requester',
            'SSECustomerAlgorithm': 'AES256',
            'SSECustomerKey': 'secret',
            'SSECustomerKeyMD5': 'checksum',
        }

        self._upload(extra_args)

        self.client.head_object.assert_called_once_with(
            Bucket=self.bucket,
            Key=self.key,
            ChecksumMode='ENABLED',
            ExpectedBucketOwner='123456789012',
            RequestPayer='requester',
            SSECustomerAlgorithm='AES256',
            SSECustomerKey='secret',
            SSECustomerKeyMD5='checksum',
        )

    def test_non_404_head_error_is_propagated(self):
        error = self._client_error('AccessDenied', 403)
        self.client.head_object.side_effect = error

        with self.assertRaises(ClientError) as raised:
            self._upload()

        self.assertIs(raised.exception, error)
        self.manager.upload.assert_not_called()

    def test_ambiguous_403_not_found_error_is_propagated(self):
        error = self._client_error('NotFound', 403)
        self.client.head_object.side_effect = error

        with self.assertRaises(ClientError) as raised:
            self._upload()

        self.assertIs(raised.exception, error)
        self.manager.upload.assert_not_called()

    def test_missing_checksum_is_rejected(self):
        self.client.head_object.return_value = {'ETag': '"etag"'}

        with self.assertRaises(S3ObjectChecksumUnavailableError):
            self._upload()

        self.manager.upload.assert_not_called()

    def test_composite_checksum_type_is_rejected(self):
        self.client.head_object.return_value = {
            'ChecksumSHA256': f'{self.checksum}-2',
            'ChecksumType': 'COMPOSITE',
            'ETag': '"etag"',
        }

        with self.assertRaises(S3ObjectChecksumUnavailableError):
            self._upload()

        self.manager.upload.assert_not_called()

    def test_composite_checksum_suffix_is_rejected_without_type(self):
        self.client.head_object.return_value = {
            'ChecksumSHA256': f'{self.checksum}-2',
            'ETag': '"etag"',
        }

        with self.assertRaises(S3ObjectChecksumUnavailableError):
            self._upload()

    def test_unknown_checksum_type_is_rejected(self):
        self.client.head_object.return_value = {
            'ChecksumSHA256': self.checksum,
            'ChecksumType': 'UNKNOWN',
            'ETag': '"etag"',
        }

        with self.assertRaises(S3ObjectChecksumUnavailableError):
            self._upload()

    def test_missing_etag_is_rejected_for_changed_object(self):
        self.client.head_object.return_value = {
            'ChecksumSHA256': self._sha256(b'old content'),
            'ChecksumType': 'FULL_OBJECT',
        }

        with self.assertRaises(S3ValidationError):
            self._upload()

        self.manager.upload.assert_not_called()

    def test_reserved_extra_args_are_rejected(self):
        reserved_args = [
            'ChecksumAlgorithm',
            'ChecksumCRC32',
            'ChecksumCRC32C',
            'ChecksumCRC64NVME',
            'ChecksumSHA1',
            'ChecksumSHA256',
            'ChecksumType',
            'IfMatch',
            'IfNoneMatch',
        ]
        for extra_arg in reserved_args:
            with self.subTest(extra_arg=extra_arg):
                with self.assertRaisesRegex(
                    ValueError, 'upload_file_idempotent manages'
                ):
                    self._upload({extra_arg: 'value'})

        self.client.head_object.assert_not_called()
        self.manager.upload.assert_not_called()

    def test_transfer_error_is_propagated_without_retry(self):
        self.client.head_object.side_effect = self._client_error('404', 404)
        error = self._client_error('PreconditionFailed', 412)
        self.future.result.side_effect = error

        with self.assertRaises(ClientError) as raised:
            self._upload()

        self.assertIs(raised.exception, error)
        self.manager.upload.assert_called_once()

    def test_transfer_conflict_is_propagated_without_retry(self):
        self.client.head_object.side_effect = self._client_error('404', 404)
        error = self._client_error('ConditionalRequestConflict', 409)
        self.future.result.side_effect = error

        with self.assertRaises(ClientError) as raised:
            self._upload()

        self.assertIs(raised.exception, error)
        self.manager.upload.assert_called_once()
