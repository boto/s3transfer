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
import functools
import math

from s3transfer.tasks import Task, TaskSubmitter
from s3transfer.utils import get_callbacks
from s3transfer.utils import unique_id
from s3transfer.utils import disable_upload_callbacks
from s3transfer.utils import enable_upload_callbacks


class UploadTaskSubmitter(TaskSubmitter):
    """Task submitter for requesting an upload"""
    UPLOAD_PART_ARGS = [
        'SSECustomerKey',
        'SSECustomerAlgorithm',
        'SSECustomerKeyMD5',
        'RequestPayer',
    ]

    def _submit(self, transfer_future, transfer_coordinator):
        # Determine the size if it was not provided
        if transfer_future.meta.size is None:
            transfer_future.meta.provide_transfer_size(
                self._osutil.get_file_size(
                    transfer_future.meta.call_args.fileobj))

        # If it is greater than threshold do a multipart upload, otherwise
        # do a regular put object.
        if transfer_future.meta.size < self._config.multipart_threshold:
            self._submit_upload_request(transfer_future, transfer_coordinator)
        else:
            self._submit_multipart_request(
                transfer_future, transfer_coordinator)

    def _submit_upload_request(self, transfer_future, transfer_coordinator):
        call_args = transfer_future.meta.call_args

        # Get the needed callbacks for the task
        progress_callbacks = get_callbacks(transfer_future, 'progress')
        done_callbacks = get_callbacks(transfer_future, 'done')

        # Create a file like object to use for the upload
        fileobj = self._osutil.open_file_chunk_reader(
            call_args.fileobj, 0, transfer_future.meta.size,
            progress_callbacks)

        # Submit the request of a single upload.
        self._executor.submit(
            PutObjectTask(
                transfer_coordinator=transfer_coordinator,
                main_kwargs={
                    'client': self._client,
                    'body': fileobj,
                    'bucket': call_args.bucket,
                    'key': call_args.key,
                    'extra_args': call_args.extra_args,
                },
                done_callbacks=(
                    [fileobj.close] + done_callbacks),
                is_final=True
            )
        )

    def _submit_multipart_request(self, transfer_future, transfer_coordinator):
        call_args = transfer_future.meta.call_args

        # Submit the request to create a multipart upload.
        create_multipart_future = self._executor.submit(
            CreateMultipartUploadTask(
                transfer_coordinator=transfer_coordinator,
                main_kwargs={
                    'client': self._client,
                    'bucket': call_args.bucket,
                    'key': call_args.key,
                    'extra_args': call_args.extra_args,
                }
            )
        )

        # Determine how many parts are needed based on filesize and
        # desired chunksize.
        part_size = self._config.multipart_chunksize
        num_parts = int(
            math.ceil(transfer_future.meta.size / float(part_size)))

        # Submit requests to upload the parts of the file.
        part_futures = []
        progress_callbacks = get_callbacks(transfer_future, 'progress')
        extra_part_args = self._extra_upload_part_args(call_args.extra_args)

        for part_number in range(1, num_parts + 1):
            fileobj = self._osutil.open_file_chunk_reader(
                call_args.fileobj, part_size * (part_number - 1),
                part_size, progress_callbacks)
            part_futures.append(
                self._executor.submit(
                    UploadPartTask(
                        transfer_coordinator=transfer_coordinator,
                        main_kwargs={
                            'client': self._client,
                            'body': fileobj,
                            'bucket': call_args.bucket,
                            'key': call_args.key,
                            'part_number': part_number,
                            'extra_args': extra_part_args,
                        },
                        pending_main_kwargs={
                            'upload_id': create_multipart_future
                        },
                        done_callbacks=[fileobj.close]
                    )
                )
            )

        # Submit the request to complete the multipart upload.
        done_callbacks = get_callbacks(transfer_future, 'done')
        self._executor.submit(
            CompleteMultipartUploadTask(
                transfer_coordinator=transfer_coordinator,
                main_kwargs={
                    'client': self._client,
                    'bucket': call_args.bucket,
                    'key': call_args.key
                },
                pending_main_kwargs={
                    'upload_id': create_multipart_future,
                    'parts': part_futures
                },
                done_callbacks=done_callbacks,
                is_final=True
            )
        )

    def _extra_upload_part_args(self, extra_args):
        # Only the args in UPLOAD_PART_ARGS actually need to be passed
        # onto the upload_part calls.
        upload_parts_args = {}
        for key, value in extra_args.items():
            if key in self.UPLOAD_PART_ARGS:
                upload_parts_args[key] = value
        return upload_parts_args


class PutObjectTask(Task):
    """Task to do a nonmultipart upload"""
    def _main(self, client, body, bucket, key, extra_args):
        """
        :param client: The client to use when calling PutObject
        :param body: The body to upload. It can be binary or a file-like object
        :param bucket: The name of the bucket to upload to
        :param key: The name of the key to upload to
        :param extra_args: A dictionary of any extra arguments that may be
            used in the upload.
        """
        client.put_object(Bucket=bucket, Key=key, Body=body, **extra_args)


class CreateMultipartUploadTask(Task):
    """Task to initiate a multipart upload"""
    def _main(self, client, bucket, key, extra_args):
        """
        :param client: The client to use when calling PutObject
        :param bucket: The name of the bucket to upload to
        :param key: The name of the key to upload to
        :param extra_args: A dictionary of any extra arguments that may be
            used in the intialization.

        :returns: The upload id of the multipart upload
        """
        # Create the multipart upload.
        response = client.create_multipart_upload(
            Bucket=bucket, Key=key, **extra_args)
        upload_id = response['UploadId']

        # Add a cleanup if the multipart upload fails at any point.
        self._transfer_coordinator.add_failure_cleanup(
            client.abort_multipart_upload, Bucket=bucket, Key=key,
            UploadId=upload_id
        )
        return upload_id


class UploadPartTask(Task):
    """Task to upload a part in a multipart upload"""
    def _main(self, client, body, bucket, key, upload_id, part_number,
              extra_args):
        """
        :param client: The client to use when calling PutObject
        :param body: The body to upload. It can be binary or a file-like object
        :param bucket: The name of the bucket to upload to
        :param key: The name of the key to upload to
        :param upload_id: The id of the upload
        :param part_number: The number representing the part of the multipart
            upload
        :param extra_args: A dictionary of any extra arguments that may be
            used in the upload.

        :rtype: dict
        :returns: A dictionary representing a part::

            {'Etag': etag_value, 'PartNumber': part_number}

            This value can be appended to a list to be used to complete
            the multipart upload.
        """
        response = client.upload_part(
            Bucket=bucket, Key=key,
            UploadId=upload_id, PartNumber=part_number,
            Body=body, **extra_args)
        etag = response['ETag']
        return {'ETag': etag, 'PartNumber': part_number}


class CompleteMultipartUploadTask(Task):
    """Task to complete a multipart upload"""
    def _main(self, client, bucket, key, upload_id, parts):
        """
        :param client: The client to use when calling PutObject
        :param bucket: The name of the bucket to upload to
        :param key: The name of the key to upload to
        :param upload_id: The id of the upload
        :param parts: A list of parts to use to complete the multipart upload::

            [{'Etag': etag_value, 'PartNumber': part_number}, ...]

            Each element in the list consists of a return value from
            ``UploadPartTask.main()``.
        """
        client.complete_multipart_upload(
            Bucket=bucket, Key=key, UploadId=upload_id,
            MultipartUpload={'Parts': parts})
