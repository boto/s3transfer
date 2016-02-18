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
import logging
import os
import socket
import math

from botocore.exceptions import IncompleteReadError
from botocore.vendored.requests.packages.urllib3.exceptions import \
    ReadTimeoutError

from s3transfer.compat import SOCKET_ERROR
from s3transfer.exceptions import RetriesExceededError
from s3transfer.exceptions import QueueShutdownError
from s3transfer.utils import random_file_extension
from s3transfer.utils import get_callbacks
from s3transfer.utils import StreamReaderProgress
from s3transfer.utils import ShutdownQueue
from s3transfer.tasks import Task
from s3transfer.tasks import TaskSubmitter


logger = logging.getLogger(__name__)

SHUTDOWN_SENTINEL = object()
S3_RETRYABLE_ERRORS = (
    socket.timeout, SOCKET_ERROR, ReadTimeoutError, IncompleteReadError
)


class DownloadTaskSubmitter(TaskSubmitter):
    def __init__(self, client, config, osutil, executor, io_executor):
        super(DownloadTaskSubmitter, self).__init__(
            client, config, osutil, executor)
        self._io_executor = io_executor

    def _submit(self, transfer_future, transfer_coordinator):
        if transfer_future.meta.size is None:
            # If a size was not provided figure out the size for the
            # user.
            response = self._client.head_object(
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
        if transfer_future.meta.size < self._config.multipart_threshold:
            self._submit_download_request(
                temp_filename, transfer_future, transfer_coordinator)
        else:
            self._submit_ranged_download_request(
                temp_filename, transfer_future, transfer_coordinator)

    def _submit_download_request(
            self, temp_filename, transfer_future, transfer_coordinator):
        call_args = transfer_future.meta.call_args

        # Get the needed callbacks for the task
        progress_callbacks = get_callbacks(transfer_future, 'progress')
        done_callbacks = get_callbacks(transfer_future, 'done')

        # Create the io queue that will be used for this download
        io_queue = ShutdownQueue(self._config.max_io_queue_size)

        # Start up the io task to write the downloads to disk as they
        # come in.
        self._submit_io_write_task(
            temp_filename, call_args.fileobj, transfer_coordinator,
            io_queue, done_callbacks)

        # Submit the task to download the object.
        self._executor.submit(
            GetObjectTask(
                transfer_coordinator=transfer_coordinator,
                main_kwargs={
                    'client': self._client,
                    'bucket': call_args.bucket,
                    'key': call_args.key,
                    'extra_args': call_args.extra_args,
                    'callbacks': progress_callbacks,
                    'max_attempts': self._config.num_download_attempts,
                    'io_queue': io_queue,
                },
                done_callbacks=[
                    functools.partial(io_queue.put, SHUTDOWN_SENTINEL)
                ]
            )
        )

    def _submit_ranged_download_request(
            self, temp_filename, transfer_future, transfer_coordinator):
        call_args = transfer_future.meta.call_args

        # Get the needed callbacks for the task
        progress_callbacks = get_callbacks(transfer_future, 'progress')
        done_callbacks = get_callbacks(transfer_future, 'done')

        # Determine the number of parts
        part_size = self._config.multipart_chunksize
        num_parts = int(
            math.ceil(transfer_future.meta.size / float(part_size)))

        # Create the io queue that will be used for this download
        io_queue = ShutdownQueue(self._config.max_io_queue_size)

        # Start up the io task to write the downloads to disk as they
        # come in.
        self._submit_io_write_task(
            temp_filename, transfer_future.meta.call_args.fileobj,
            transfer_coordinator, io_queue, done_callbacks)

        ranged_downloads = []
        for i in range(num_parts):
            # Calculate the range parameter
            range_parameter = self._calculate_range_param(
                part_size, i, num_parts)

            # Inject the Range parameter to the parameters to be passed in
            # as extra args
            extra_args = {'Range': range_parameter}
            extra_args.update(call_args.extra_args)

            # Submit the ranged downloads
            ranged_downloads.append(
                self._executor.submit(
                    GetObjectTask(
                        transfer_coordinator=transfer_coordinator,
                        main_kwargs={
                            'client': self._client,
                            'bucket': call_args.bucket,
                            'key': call_args.key,
                            'extra_args': extra_args,
                            'callbacks': progress_callbacks,
                            'max_attempts': self._config.num_download_attempts,
                            'io_queue': io_queue,
                            'start_index': i * part_size
                        }
                    )
                )
            )

        # Submit a task to wait for all of the downloads to complete
        # before telling the io task to stop writing.
        self._executor.submit(
            RangeDownloadJoinTask(
                transfer_coordinator=transfer_coordinator,
                pending_main_kwargs={
                    'ranged_downloads': ranged_downloads
                },
                done_callbacks=[
                    functools.partial(io_queue.put, SHUTDOWN_SENTINEL)
                ]
            )
        )

    def _submit_io_write_task(self, temp_filename, filename,
                              transfer_coordinator, io_queue, done_callbacks):
        self._io_executor.submit(
            IOWriteTask(
                transfer_coordinator=transfer_coordinator,
                main_kwargs={
                    'filename': temp_filename,
                    'final_filename': filename,
                    'osutil': self._osutil,
                    'io_queue': io_queue
                },
                done_callbacks=done_callbacks,
                is_final=True
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

    def _main(self, client, bucket, key, extra_args, callbacks,
              max_attempts, io_queue, start_index=0):
        """Downloads an object and places content into io queue

        :param client: The client to use when calling GetObject
        :param bucket: The bucket to download from
        :param key: The key to download from
        :param exta_args: Any extra arguements to include in GetObject request
        :param callbacks: List of progress callbacks to invoke on download
        :param max_attempts: The number of retries to do when downloading
        :param io_queue: The queue that content of the object will be
            placed in
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
                    # If the io queue was shutdown just stop sending to
                    # chunks to the queue and quit the download.
                    try:
                        io_queue.put((current_index, chunk))
                    except QueueShutdownError:
                        return
                    current_index += len(chunk)
                return
            except S3_RETRYABLE_ERRORS as e:
                logger.debug("Retrying exception caught (%s), "
                             "retrying request, (attempt %s / %s)", e, i,
                             max_attempts, exc_info=True)
                last_exception = e
                continue
        raise RetriesExceededError(last_exception)


class IOWriteTask(Task):
    def _main(self, filename, final_filename, osutil, io_queue):
        """Pulls off an io queue to write contents to a file

        :param filename: The name of the file to write contents to
        :param final_filename: The final name of the file to rename to
            upon completion of writing the contents.
        :param osutil: OS utility
        :param io_queue: The queue to retrieve contents of the downloaded
            file off of.
        """
        self._transfer_coordinator.add_failure_cleanup(
            self._remove_file_cleanup, filename, osutil)
        try:
            self._loop_on_io_writes(filename, osutil, io_queue)
        except Exception as e:
            logger.debug("Caught exception in IO thread: %s",
                         e, exc_info=True)
            io_queue.trigger_shutdown()
            raise
        if self._transfer_coordinator.status != 'failed':
            osutil.rename_file(filename, final_filename)

    def _loop_on_io_writes(self, filename, osutil, io_queue):
        with osutil.open(filename, 'wb') as f:
            while True:
                task = io_queue.get()
                if task is SHUTDOWN_SENTINEL:
                    logger.debug("Shutdown sentinel received in IO handler, "
                                 "shutting down IO handler.")
                    return
                else:
                    offset, data = task
                    f.seek(offset)
                    f.write(data)

    def _remove_file_cleanup(self, filename, osutil):
        if os.path.exists(filename):
            osutil.remove_file(filename)


class RangeDownloadJoinTask(Task):
    """A task to wait for all download tasks to complete"""
    def _main(self, ranged_downloads):
        pass
