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

from s3transfer.constants import FULL_OBJECT_CHECKSUM_ARGS
from s3transfer.exceptions import (
    S3ObjectChecksumUnavailableError,
    S3ValidationError,
)
from s3transfer.utils import OSUtils


class IdempotentUploader:
    """Coordinates a content-idempotent path upload."""

    _HASH_CHUNK_SIZE = 1024 * 1024
    _HEAD_ARGS = {
        'ExpectedBucketOwner',
        'RequestPayer',
        'SSECustomerAlgorithm',
        'SSECustomerKey',
        'SSECustomerKeyMD5',
    }
    _RESERVED_UPLOAD_ARGS = {
        'ChecksumAlgorithm',
        'ChecksumType',
        'IfMatch',
        'IfNoneMatch',
        *FULL_OBJECT_CHECKSUM_ARGS,
    }

    def __init__(self, client, transfer_manager, osutil=None):
        self._client = client
        self._transfer_manager = transfer_manager
        self._osutil = osutil or OSUtils()

    def upload(
        self,
        filename,
        bucket,
        key,
        extra_args=None,
        subscribers=None,
    ):
        """Upload a path only when its bytes differ from the S3 object."""
        upload_args = extra_args.copy() if extra_args else {}
        self._validate_extra_args(upload_args)
        checksum = self._calculate_checksum(filename)
        head_args = {
            arg: upload_args[arg]
            for arg in self._HEAD_ARGS
            if arg in upload_args
        }

        try:
            response = self._client.head_object(
                Bucket=bucket,
                Key=key,
                ChecksumMode='ENABLED',
                **head_args,
            )
        except ClientError as error:
            if not self._is_missing_object(error):
                raise
            upload_args['IfNoneMatch'] = '*'
        else:
            current_checksum = self._get_full_object_checksum(
                response, bucket, key
            )
            if current_checksum == checksum:
                return False
            etag = response.get('ETag')
            if etag is None:
                raise S3ValidationError(
                    f'S3 object s3://{bucket}/{key} has no ETag for a '
                    'conditional upload.'
                )
            upload_args['IfMatch'] = etag

        upload_args['ChecksumSHA256'] = checksum
        future = self._transfer_manager.upload(
            filename,
            bucket,
            key,
            upload_args,
            subscribers,
        )
        future.result()
        return True

    def _calculate_checksum(self, filename):
        checksum = hashlib.sha256()
        with self._osutil.open(filename, 'rb') as fileobj:
            while chunk := fileobj.read(self._HASH_CHUNK_SIZE):
                checksum.update(chunk)
        return base64.b64encode(checksum.digest()).decode('ascii')

    def _validate_extra_args(self, extra_args):
        reserved_args = self._RESERVED_UPLOAD_ARGS.intersection(extra_args)
        if reserved_args:
            formatted_args = ', '.join(sorted(reserved_args))
            raise ValueError(
                'upload_file_idempotent manages these ExtraArgs: '
                f'{formatted_args}'
            )

    def _get_full_object_checksum(self, response, bucket, key):
        checksum = response.get('ChecksumSHA256')
        checksum_type = response.get('ChecksumType')
        if (
            checksum is None
            or checksum_type == 'COMPOSITE'
            or (checksum_type is not None and checksum_type != 'FULL_OBJECT')
            or self._has_composite_suffix(checksum)
        ):
            raise S3ObjectChecksumUnavailableError(bucket, key)
        return checksum

    def _has_composite_suffix(self, checksum):
        separator, _, part_count = checksum.rpartition('-')
        return bool(separator and part_count.isdigit())

    def _is_missing_object(self, error):
        response = error.response
        status_code = response.get('ResponseMetadata', {}).get(
            'HTTPStatusCode'
        )
        error_code = response.get('Error', {}).get('Code')
        if status_code == 403:
            return False
        return status_code == 404 or error_code in {
            '404',
            'NoSuchKey',
            'NotFound',
        }
