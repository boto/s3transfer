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
import logging
import os
import socket
import math

from botocore.exceptions import IncompleteReadError
from botocore.vendored.requests.packages.urllib3.exceptions import \
    ReadTimeoutError

from s3transfer.compat import SOCKET_ERROR
from s3transfer.exceptions import RetriesExceededError
from s3transfer.utils import random_file_extension
from s3transfer.utils import get_callbacks
from s3transfer.utils import invoke_progress_callbacks
from s3transfer.utils import calculate_range_parameter
from s3transfer.utils import FunctionContainer
from s3transfer.utils import StreamReaderProgress
from s3transfer.tasks import Task
from s3transfer.tasks import SubmissionTask


logger = logging.getLogger(__name__)

S3_RETRYABLE_ERRORS = (
    socket.timeout, SOCKET_ERROR, ReadTimeoutError, IncompleteReadError
)


class DownloadSubmissionTask(SubmissionTask):
    """Task for submitting tasks to execute a download"""

    def _submit(self, client, config, osutil, request_executor, io_executor,
                transfer_future):
        """
        :param client: The client associated with the transfer manager

        :type config: s3transfer.manager.TransferConfig
        :param config: The transfer config associated with the transfer
            manager

        :type osutil: s3transfer.utils.OSUtil
        :param osutil: The os utility associated to the transfer manager

        :type request_executor: s3transfer.futures.BoundedExecutor
        :param request_executor: The request executor associated with the
            transfer manager

        :type io_executor: s3transfer.futures.BoundedExecutor
        :param io_executor: The io executor associated with the
            transfer manager

        :type transfer_future: s3transfer.futures.TransferFuture
        :param transfer_future: The transfer future associated with the
            transfer request that tasks are being submitted for
        """
        if transfer_future.meta.size is None:
            # If a size was not provided figure out the size for the
            # user.
            response = client.head_object(
                Bucket=transfer_future.meta.call_args.bucket,
                Key=transfer_future.meta.call_args.key,
                **transfer_future.meta.call_args.extra_args
            )
            transfer_future.meta.provide_transfer_size(
                response['ContentLength'])

        # Figure out a temporary filename for the file.
        temp_filename = (
            transfer_future.meta.call_args.fileobj + os.extsep +
            random_file_extension()
        )
        # If it is greater than threshold do a ranged download, otherwise
        # do a regular GetObject download.
        if transfer_future.meta.size < config.multipart_threshold:
            self._submit_download_request(
                client, config, osutil, request_executor, io_executor,
                temp_filename, transfer_future)
        else:
            self._submit_ranged_download_request(
                client, config, osutil, request_executor, io_executor,
                temp_filename, transfer_future)

    def _submit_download_request(self, client, config, osutil,
                                 request_executor, io_executor, temp_filename,
                                 transfer_future):
        call_args = transfer_future.meta.call_args

        # Get a handle to the temp file
        temp_fileobj = self._get_io_file_handle(temp_filename, osutil)

        # Get the needed callbacks for the task
        progress_callbacks = get_callbacks(transfer_future, 'progress')

        # Before starting any tasks for downloads set up a cleanup function
        # that will make sure that the temporary file always gets
        # cleaned up.
        self._transfer_coordinator.add_failure_cleanup(
            osutil.remove_file, temp_filename)

        # Submit the task to download the object.
        download_future = self._submit_task(
            request_executor,
            GetObjectTask(
                transfer_coordinator=self._transfer_coordinator,
                main_kwargs={
                    'client': client,
                    'bucket': call_args.bucket,
                    'key': call_args.key,
                    'fileobj': temp_fileobj,
                    'extra_args': call_args.extra_args,
                    'callbacks': progress_callbacks,
                    'max_attempts': config.num_download_attempts,
                    'io_executor': io_executor
                }
            )
        )

        # Send the necessary tasks to complete the download.
        self._complete_download(
            osutil, request_executor, io_executor, temp_fileobj,
            call_args.fileobj, [download_future])

    def _submit_ranged_download_request(self, client, config, osutil,
                                        request_executor, io_executor,
                                        temp_filename, transfer_future):
        call_args = transfer_future.meta.call_args

        # Get the needed progress callbacks for the task
        progress_callbacks = get_callbacks(transfer_future, 'progress')

        # Get a handle to the temp file
        temp_fileobj = self._get_io_file_handle(temp_filename, osutil)

        # Determine the number of parts
        part_size = config.multipart_chunksize
        num_parts = int(
            math.ceil(transfer_future.meta.size / float(part_size)))

        # Before starting any tasks for downloads set up a cleanup function
        # that will make sure that the temporary file always gets
        # cleaned up.
        self._transfer_coordinator.add_failure_cleanup(
            osutil.remove_file, temp_filename)

        ranged_downloads = []
        for i in range(num_parts):
            # Calculate the range parameter
            range_parameter = calculate_range_parameter(
                part_size, i, num_parts)

            # Inject the Range parameter to the parameters to be passed in
            # as extra args
            extra_args = {'Range': range_parameter}
            extra_args.update(call_args.extra_args)
            # Submit the ranged downloads
            ranged_downloads.append(
                self._submit_task(
                    request_executor,
                    GetObjectTask(
                        transfer_coordinator=self._transfer_coordinator,
                        main_kwargs={
                            'client': client,
                            'bucket': call_args.bucket,
                            'key': call_args.key,
                            'fileobj': temp_fileobj,
                            'extra_args': extra_args,
                            'callbacks': progress_callbacks,
                            'max_attempts': config.num_download_attempts,
                            'io_executor': io_executor,
                            'start_index': i * part_size
                        }
                    )
                )
            )
        # Send the necessary tasks to complete the download.
        self._complete_download(
            osutil, request_executor, io_executor, temp_fileobj,
            call_args.fileobj, ranged_downloads)

    def _get_io_file_handle(self, filename, osutil):
        f = osutil.open(filename, 'wb')
        self._transfer_coordinator.add_failure_cleanup(f.close)
        return f

    def _complete_download(self, osutil, request_executor, io_executor,
                           fileobj, final_filename, download_futures):
        # A task to rename the file from the temporary file to its final
        # location. This should be the last task needed to complete the
        # download.
        rename_task = IORenameFileTask(
            transfer_coordinator=self._transfer_coordinator,
            main_kwargs={
                'fileobj': fileobj,
                'final_filename': final_filename,
                'osutil': osutil
            },
            is_final=True
        )
        submit_rename_task = FunctionContainer(
            self._submit_task, io_executor, rename_task)

        # Submit a task to wait for all of the downloads to complete
        # and submit their downloaded content to the io executor before
        # submitting the renaming task to the io executor.
        self._submit_task(
            request_executor,
            JoinFuturesTask(
                transfer_coordinator=self._transfer_coordinator,
                pending_main_kwargs={
                    'futures_to_wait_on': download_futures
                },
                done_callbacks=[submit_rename_task]
            )
        )

    def _calculate_range_param(self, part_size, part_index, num_parts):
        # Used to calculate the Range parameter
        start_range = part_index * part_size
        if part_index == num_parts - 1:
            end_range = ''
        else:
            end_range = start_range + part_size - 1
        range_param = 'bytes=%s-%s' % (start_range, end_range)
        return range_param


class GetObjectTask(Task):
    STREAM_CHUNK_SIZE = 8 * 1024

    def _main(self, client, bucket, key, fileobj, extra_args, callbacks,
              max_attempts, io_executor, start_index=0):
        """Downloads an object and places content into io queue

        :param client: The client to use when calling GetObject
        :param bucket: The bucket to download from
        :param key: The key to download from
        :param fileobj: The file handle to write content to
        :param exta_args: Any extra arguements to include in GetObject request
        :param callbacks: List of progress callbacks to invoke on download
        :param max_attempts: The number of retries to do when downloading
        :param io_executor: The executor that will handle the writing of
            contents
        :param start_index: The location in the file to start writing the
            content of the key to.
        """
        last_exception = None
        for i in range(max_attempts):
            try:
                response = client.get_object(
                    Bucket=bucket, Key=key, **extra_args)
                streaming_body = StreamReaderProgress(
                    response['Body'], callbacks)

                current_index = start_index
                chunks = iter(
                    lambda: streaming_body.read(self.STREAM_CHUNK_SIZE), b'')
                for chunk in chunks:
                    # If the transfer is done because of a cancellation
                    # or error somewhere else, stop trying to submit more
                    # data to be written and break out of the download.
                    if not self._transfer_coordinator.done():
                        self._submit_task(
                            io_executor,
                            IOWriteTask(
                                self._transfer_coordinator,
                                main_kwargs={
                                    'fileobj': fileobj,
                                    'data': chunk,
                                    'offset': current_index
                                }
                            )
                        )
                        current_index += len(chunk)
                    else:
                        return
                return
            except S3_RETRYABLE_ERRORS as e:
                logger.debug("Retrying exception caught (%s), "
                             "retrying request, (attempt %s / %s)", e, i,
                             max_attempts, exc_info=True)
                last_exception = e
                # Also invoke the progress callbacks to indicate that we
                # are trying to download the stream again and all progress
                # for this GetObject has been lost.
                invoke_progress_callbacks(
                    callbacks, start_index - current_index)
                continue
        raise RetriesExceededError(last_exception)


class JoinFuturesTask(Task):
    """A task to wait for the completion of other futures

    :params future_to_wait_on: A list of futures to wait on
    """
    def _main(self, futures_to_wait_on):
        # The _main is a noop because the functionality of waiting on
        # futures lives in the signature of the _main method. If you
        # make the future_to_wait_on a pending_main_kwargs, all of those
        # futures need to complete before main is executed and at that point
        # nothing more needs to be executed.
        pass


class IOWriteTask(Task):
    def _main(self, fileobj, data, offset):
        """Pulls off an io queue to write contents to a file

        :param f: The file handle to write content to
        :param data: The data to write
        :param offset: The offset to write the data to.
        """
        fileobj.seek(offset)
        fileobj.write(data)


class IORenameFileTask(Task):
    """A task to rename a temporary file to its final filename

    :param f: The file handle that content was written to.
    :param final_filename: The final name of the file to rename to
        upon completion of writing the contents.
    :param osutil: OS utility
    """
    def _main(self, fileobj, final_filename, osutil):
        fileobj.close()
        osutil.rename_file(fileobj.name, final_filename)
