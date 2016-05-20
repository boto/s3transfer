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
import math

from botocore.compat import six

from s3transfer.tasks import Task, TaskSubmitter
from s3transfer.utils import get_callbacks, ReadFileChunk


def get_upload_utils_cls(transfer_future):
    """Retieves a utility class for guiding an upload based on file type"""
    fileobj = transfer_future.meta.call_args.fileobj
    if isinstance(fileobj, six.string_types):
        return UploadFilenameUtils
    elif hasattr(fileobj, 'seek') and hasattr(fileobj, 'tell'):
        return UploadSeekableStreamUtils
    else:
        return UploadUnseekableStreamUtils


class UploadUtils(object):
    """Base utility class for handling uploads for various types of files

    This class is typically used for the TaskSubmitter class to help
    determine the following:

        * How to determine the size of the file
        * How to determine if a multipart upload is required
        * How to retrieve the body for a PutObject
        * How to retrieve the bodies for a set of UploadParts

    The answers/implementations differ for the various types of file inputs
    that may be accepted. All implementations must subclass and override
    public methods from this class.
    """

    def provide_transfer_size(self, transfer_future):
        """Provides the transfer size of an upload

        :type transfer_future: s3transfer.futures.TransferFuture
        :param transfer_future: The future associated with upload request
        """
        raise NotImplementedError('must implement provide_transfer_size()')

    def requires_multipart_upload(self, transfer_future, config):
        """Determines where a multipart upload is required

        :type transfer_future: s3transfer.futures.TransferFuture
        :param transfer_future: The future associated with upload request

        :type config: s3transfer.manager.TransferConfig
        :param config: The config associated to the transfer manager

        :rtype: boolean
        :returns: True, if the upload should be multipart based on
            configuartion and size. False, otherwise.
        """
        raise NotImplementedError('must implement requires_multipart_upload()')

    def get_put_object_body(self, transfer_future):
        """Returns the body to use for PutObject

        :type transfer_future: s3transfer.futures.TransferFuture
        :param transfer_future: The future associated with upload request

        :type config: s3transfer.manager.TransferConfig
        :param config: The config associated to the transfer manager

        :rtype: s3transfer.utils.ReadFileChunk
        :returns: A ReadFileChunk including all progress callbacks
            associated with the transfer future.
        """
        raise NotImplementedError('must implement get_put_object_body()')
    
    def yield_upload_part_bodies(self, transfer_future, config):
        """Yields the part number and body to use for each UploadPart

        :type transfer_future: s3transfer.futures.TransferFuture
        :param transfer_future: The future associated with upload request

        :type config: s3transfer.manager.TransferConfig
        :param config: The config associated to the transfer manager

        :rtype: int, s3transfer.utils.ReadFileChunk
        :returns: Yields the part number and the ReadFileChunk including all
            progress callbacks associated with the transfer future for that
            specific yielded part.
        """
        raise NotImplementedError('must implement yield_upload_part_bodies()')


class UploadFilenameUtils(UploadUtils):
    """Upload utility for filenames"""
    def __init__(self, osutil):
        self._osutil = osutil

    def provide_transfer_size(self, transfer_future):
        transfer_future.meta.provide_transfer_size(
            self._osutil.get_file_size(
                transfer_future.meta.call_args.fileobj))

    def requires_multipart_upload(self, transfer_future, config):
        return transfer_future.meta.size >= config.multipart_threshold

    def get_put_object_body(self, transfer_future):
        fileobj = transfer_future.meta.call_args.fileobj
        callbacks = get_callbacks(transfer_future, 'progress')
        return self._osutil.open_file_chunk_reader(
            filename=fileobj, start_byte=0, size=transfer_future.meta.size,
            callbacks=callbacks)

    def yield_upload_part_bodies(self, transfer_future, config):
        part_size = config.multipart_chunksize
        num_parts = self._get_num_parts(transfer_future, part_size)
        for part_number in range(1, num_parts + 1):
            fileobj = transfer_future.meta.call_args.fileobj
            callbacks = get_callbacks(transfer_future, 'progress')
            fileobj = self._osutil.open_file_chunk_reader(
                filename=fileobj, start_byte=part_size * (part_number - 1),
                size=part_size, callbacks=callbacks
            )
            yield part_number, fileobj

    def _get_num_parts(self, transfer_future, part_size):
        return int(
            math.ceil(transfer_future.meta.size / float(part_size)))


class UploadSeekableStreamUtils(UploadFilenameUtils):
    """Upload utility for seekable streams"""
    def __init__(self, osutils):
        super(UploadSeekableStreamUtils, self).__init__(osutils)
        self._collected_body = None

    def provide_transfer_size(self, transfer_future):
        fileobj = transfer_future.meta.call_args.fileobj
        # To determine size, first determine the starting position
        # Seek to the end and then find the difference in the length
        # between the end and start positions.
        start_position = fileobj.tell()
        fileobj.seek(0, 2)
        end_position = fileobj.tell()
        fileobj.seek(start_position)
        transfer_future.meta.provide_transfer_size(
            end_position - start_position)

    def get_put_object_body(self, transfer_future):
        fileobj = transfer_future.meta.call_args.fileobj
        callbacks = get_callbacks(transfer_future, 'progress')
        fileobj, transfer_size = self._get_put_object_body_and_size(
            transfer_future)
        return ReadFileChunk(
            fileobj=fileobj, chunk_size=transfer_size,
            full_file_size=transfer_size, callbacks=callbacks,
            enable_callbacks=False
        )

    def _get_put_object_body_and_size(self, transfer_future):
        # We do not use the fileobj directly because if you pass the fileobj
        # to a thread the user may close it immediately, causing the
        # thread to be unable to read from the file.
        fileobj = transfer_future.meta.call_args.fileobj
        transfer_size = transfer_future.meta.size
        data = self._pull_from_stream(fileobj, transfer_size)
        fileobj = self._wrap_data(data)
        return fileobj, transfer_size

    def yield_upload_part_bodies(self, transfer_future, config):
        part_size = config.multipart_chunksize
        num_parts = self._get_num_parts(transfer_future, part_size)
        parts_iterator = self._get_parts_iterator(
            transfer_future, part_size, num_parts)

        for part_number in parts_iterator:
            fileobj = self._get_upload_part_body(
                transfer_future, part_size, part_number)
            yield part_number, fileobj

    def _get_parts_iterator(self, transfer_future, part_size, num_parts):
        fileobj = transfer_future.meta.call_args.fileobj

        def parts_iterator(fileobj, part_size, num_parts):
            for part_num in range(1, num_parts + 1):
                self._collected_body = self._pull_from_stream(
                    fileobj, part_size)
                yield part_num

        return parts_iterator(fileobj, part_size, num_parts)

    def _get_upload_part_body(self, transfer_future, part_size, part_number):
        callbacks = get_callbacks(transfer_future, 'progress')

        # From the previous retrieved collected body that happened in the
        # iterator, wrap the body and provide it to a ReadFileChunk
        transfer_size = len(self._collected_body)
        fileobj = self._wrap_data(self._collected_body)

        return ReadFileChunk(
            fileobj=fileobj, chunk_size=transfer_size,
            full_file_size=transfer_size, callbacks=callbacks,
            enable_callbacks=False
        )

    def _pull_from_stream(self, fileobj, amount):
        return fileobj.read(amount)

    def _wrap_data(self, data):
        return six.BytesIO(data)


class UploadUnseekableStreamUtils(UploadSeekableStreamUtils):
    """Upload utility for unseekable streams"""
    def provide_transfer_size(self, transfer_future):
        pass

    def _transfer_size_not_known(self, transfer_future):
        return transfer_future.meta.size is None

    def requires_multipart_upload(self, transfer_future, config):
        fileobj = transfer_future.meta.call_args.fileobj
        # If the transfer size is not known, pull the stream up to the
        # multipart threshold. If the amount of data pulled is less than
        # threshold, it will be a nonmultipart upload. If it is equal, there
        # is probably more data to pull so make it a multipart upload.
        if self._transfer_size_not_known(transfer_future):
            self._collected_body = self._pull_from_stream(
                fileobj, config.multipart_threshold)
            return len(self._collected_body) >= config.multipart_threshold
        return super(
            UploadUnseekableStreamUtils, self).requires_multipart_upload(
                transfer_future, config)

    def _get_put_object_body_and_size(self, transfer_future):
        # If a transfer size was not provided then a body was already
        # collected to determine the size, so use this collected body.
        if self._transfer_size_not_known(transfer_future):
            fileobj = self._wrap_data(self._collected_body)
            transfer_size = len(self._collected_body)
            return fileobj, transfer_size
        return super(
            UploadUnseekableStreamUtils, self)._get_put_object_body_and_size(
                transfer_future)

    def _get_num_parts(self, transfer_future, num_parts):
        # We cannot determine the number of parts if we do not know the
        # size of the entire transfer.
        if self._transfer_size_not_known(transfer_future):
            return None
        return super(UploadUnseekableStreamUtils, self)._get_num_parts(
            transfer_future, num_parts)

    def _get_parts_iterator(self, transfer_future, part_size, num_parts):
        fileobj = transfer_future.meta.call_args.fileobj

        # If the transfer size is not know, we need to keep pulling from
        # stream yielding the data until there is no more data.
        if self._transfer_size_not_known(transfer_future):
            def parts_iterator(fileobj, part_size):
                part_count = 1
                while self._collected_body:
                    yield part_count
                    self._collected_body = self._pull_from_stream(
                        fileobj, part_size)
                    part_count += 1

            return parts_iterator(fileobj, part_size)

        return super(UploadUnseekableStreamUtils, self)._get_parts_iterator(
            transfer_future, part_size, num_parts)


class UploadTaskSubmitter(TaskSubmitter):
    """Task submitter for requesting an upload"""
    UPLOAD_PART_ARGS = [
        'SSECustomerKey',
        'SSECustomerAlgorithm',
        'SSECustomerKeyMD5',
        'RequestPayer',
    ]

    def _submit(self, transfer_future, transfer_coordinator):
        upload_utils = get_upload_utils_cls(transfer_future)(self._osutil)

        # Determine the size if it was not provided
        if transfer_future.meta.size is None:
            upload_utils.provide_transfer_size(transfer_future)

        # Do a multipart upload if needed, otherwise do a regular put object.
        if upload_utils.requires_multipart_upload(
                transfer_future, self._config):
            self._submit_multipart_request(
                transfer_future, transfer_coordinator, upload_utils)
        else:
            self._submit_upload_request(
                transfer_future, transfer_coordinator, upload_utils)

    def _submit_upload_request(self, transfer_future, transfer_coordinator,
                               upload_utils):
        call_args = transfer_future.meta.call_args

        # Get the needed callbacks for the task
        done_callbacks = get_callbacks(transfer_future, 'done')

        main_kwargs = {
            'client': self._client,
            'bucket': call_args.bucket,
            'key': call_args.key,
            'extra_args': call_args.extra_args,
        }

        # Inject the body related parameters.
        main_kwargs['fileobj'] = upload_utils.get_put_object_body(
            transfer_future)

        # Submit the request of a single upload.
        self._executor.submit(
            PutObjectTask(
                transfer_coordinator=transfer_coordinator,
                main_kwargs=main_kwargs,
                done_callbacks=done_callbacks,
                is_final=True
            )
        )

    def _submit_multipart_request(self, transfer_future, transfer_coordinator,
                                  upload_utils):
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

        # Submit requests to upload the parts of the file.
        part_futures = []
        extra_part_args = self._extra_upload_part_args(call_args.extra_args)

        for part_number, fileobj in upload_utils.yield_upload_part_bodies(
                transfer_future, self._config):
            main_kwargs = {
                'client': self._client,
                'bucket': call_args.bucket,
                'fileobj': fileobj,
                'key': call_args.key,
                'part_number': part_number,
                'extra_args': extra_part_args
            }
            part_futures.append(
                self._executor.submit(
                    UploadPartTask(
                        transfer_coordinator=transfer_coordinator,
                        main_kwargs=main_kwargs,
                        pending_main_kwargs={
                            'upload_id': create_multipart_future
                        }
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
    def _main(self, client, fileobj, bucket, key, extra_args):
        """
        :param client: The client to use when calling PutObject
        :param fileobj: The file to upload.
        :param bucket: The name of the bucket to upload to
        :param key: The name of the key to upload to
        :param extra_args: A dictionary of any extra arguments that may be
            used in the upload.
        """
        with fileobj as body:
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
    def _main(self, client, fileobj, bucket, key, upload_id, part_number,
              extra_args):
        """
        :param client: The client to use when calling PutObject
        :param fileobj: The file to upload.
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
        with fileobj as body:
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
