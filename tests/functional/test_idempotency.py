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
from botocore.stub import ANY

from s3transfer.idempotency import IdempotentUploader
from s3transfer.manager import TransferConfig, TransferManager
from s3transfer.utils import ChunksizeAdjuster
from tests import FileCreator, StubbedClientTest, mock


class BaseIdempotentUploadTest(StubbedClientTest):
    def setUp(self):
        super().setUp()
        self.file_creator = FileCreator()
        self.contents = b'my content'
        self.filename = self.file_creator.create_file(
            'my-file', self.contents, mode='wb'
        )
        self.bucket = 'my-bucket'
        self.key = 'my-key'
        self.checksum = self._sha256(self.contents)

    def tearDown(self):
        self.manager.shutdown()
        self.file_creator.remove_all()
        super().tearDown()

    def _sha256(self, contents):
        digest = hashlib.sha256(contents).digest()
        return base64.b64encode(digest).decode('ascii')

    def _add_head_response(self, checksum, etag='"etag"'):
        self.stubber.add_response(
            'head_object',
            {
                'ChecksumSHA256': checksum,
                'ChecksumType': 'FULL_OBJECT',
                'ETag': etag,
            },
            {
                'Bucket': self.bucket,
                'Key': self.key,
                'ChecksumMode': 'ENABLED',
            },
        )

    def _add_missing_head_response(self):
        self.stubber.add_client_error(
            'head_object',
            service_error_code='404',
            service_message='Not Found',
            http_status_code=404,
            expected_params={
                'Bucket': self.bucket,
                'Key': self.key,
                'ChecksumMode': 'ENABLED',
            },
        )

    def _upload(self):
        uploader = IdempotentUploader(self.client, self.manager)
        return uploader.upload(self.filename, self.bucket, self.key)


class TestNonMultipartIdempotentUpload(BaseIdempotentUploadTest):
    def setUp(self):
        super().setUp()
        config = TransferConfig(multipart_threshold=1024)
        self.manager = TransferManager(self.client, config)

    def test_missing_object_puts_with_full_checksum_and_condition(self):
        self._add_missing_head_response()
        self.stubber.add_response(
            'put_object',
            {},
            {
                'Body': ANY,
                'Bucket': self.bucket,
                'Key': self.key,
                'ChecksumSHA256': self.checksum,
                'IfNoneMatch': '*',
            },
        )

        self.assertTrue(self._upload())

        self.stubber.assert_no_pending_responses()

    def test_matching_object_skips_put(self):
        self._add_head_response(self.checksum)

        self.assertFalse(self._upload())

        self.stubber.assert_no_pending_responses()

    def test_conditional_failure_surfaces(self):
        self._add_missing_head_response()
        self.stubber.add_client_error(
            'put_object',
            service_error_code='PreconditionFailed',
            service_message='At least one precondition failed',
            http_status_code=412,
            expected_params={
                'Body': ANY,
                'Bucket': self.bucket,
                'Key': self.key,
                'ChecksumSHA256': self.checksum,
                'IfNoneMatch': '*',
            },
        )

        with self.assertRaises(ClientError):
            self._upload()

        self.stubber.assert_no_pending_responses()


class TestMultipartIdempotentUpload(BaseIdempotentUploadTest):
    def setUp(self):
        super().setUp()
        self.adjuster_patch = mock.patch(
            's3transfer.upload.ChunksizeAdjuster',
            lambda: ChunksizeAdjuster(min_size=1),
        )
        self.adjuster_patch.start()
        config = TransferConfig(
            multipart_threshold=1,
            multipart_chunksize=5,
            max_request_concurrency=1,
        )
        self.manager = TransferManager(self.client, config)

    def tearDown(self):
        self.adjuster_patch.stop()
        super().tearDown()

    def test_changed_object_conditions_multipart_completion(self):
        self._add_head_response(self._sha256(b'old content'))
        self.stubber.add_response(
            'create_multipart_upload',
            {'UploadId': 'upload-id'},
            {
                'Bucket': self.bucket,
                'Key': self.key,
                'ChecksumAlgorithm': 'SHA256',
                'ChecksumType': 'FULL_OBJECT',
            },
        )
        for part_number in (1, 2):
            self.stubber.add_response(
                'upload_part',
                {
                    'ETag': f'"etag-{part_number}"',
                    'ChecksumSHA256': f'part-{part_number}-checksum',
                },
                {
                    'Body': ANY,
                    'Bucket': self.bucket,
                    'Key': self.key,
                    'UploadId': 'upload-id',
                    'PartNumber': part_number,
                    'ChecksumAlgorithm': 'SHA256',
                },
            )
        self.stubber.add_response(
            'complete_multipart_upload',
            {},
            {
                'Bucket': self.bucket,
                'Key': self.key,
                'UploadId': 'upload-id',
                'MultipartUpload': {
                    'Parts': [
                        {
                            'ETag': '"etag-1"',
                            'PartNumber': 1,
                            'ChecksumSHA256': 'part-1-checksum',
                        },
                        {
                            'ETag': '"etag-2"',
                            'PartNumber': 2,
                            'ChecksumSHA256': 'part-2-checksum',
                        },
                    ]
                },
                'ChecksumSHA256': self.checksum,
                'ChecksumType': 'FULL_OBJECT',
                'IfMatch': '"etag"',
            },
        )

        self.assertTrue(self._upload())

        self.stubber.assert_no_pending_responses()
