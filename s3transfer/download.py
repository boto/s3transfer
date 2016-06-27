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

from botocore.compat import six
from botocore.exceptions import IncompleteReadError
from botocore.vendored.requests.packages.urllib3.exceptions import \
    ReadTimeoutError

from s3transfer.compat import SOCKET_ERROR
from s3transfer.compat import seekable
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


class DownloadOutputManager(object):
    """Base manager class for handling various types of files for downloads

    This class is typically used for the DownloadSubmissionTask class to help
    determine the following:

        * Provides the fileobj to write to downloads to
        * Get a task to complete once everything downloaded has been written

    The answers/implementations differ for the various types of file outputs
    that may be accepted. All implementations must subclass and override
    public methods from this class.
    """
    def __init__(self, osutil, transfer_coordinator):
        self._osutil = osutil
        self._transfer_coordinator = transfer_coordinator

    @classmethod
    def is_compatible(self, download_target):
        """Determines if the target for the download is compatible with manager

        :param download_target: The target for which the upload will write
            data to.

        :returns: True if the manager can handle the type of target specified
            otherwise returns False.
        """
        raise NotImplementedError('must implement is_compatible()')

    def get_fileobj_for_io_writes(self, transfer_future):
        """Get file-like object to use for io writes in the io executor

        :type transfer_future: s3transfer.futures.TransferFuture
        :param transfer_future: The future associated with upload request

        returns: A file-like object to write to
        """
        raise NotImplementedError('must implement get_fileobj_for_io_writes()')

    def get_final_io_task(self):
        """Get the final io task to complete the download

        This is needed because based on the architecture of the TransferManager
        the final tasks will be sent to the IO executor, but the executor
        needs a final task for it to signal that the transfer is done and
        all done callbacks can be run.

        :rtype: s3transfer.tasks.Task
        :returns: A final task to completed in the io executor
        """
        raise NotImplementedError(
            'must implement get_final_io_task()')


class DownloadFilenameOutputManager(DownloadOutputManager):
    def __init__(self, osutil, transfer_coordinator):
        super(DownloadFilenameOutputManager, self).__init__(
            osutil, transfer_coordinator)
        self._final_filename = None
        self._temp_filename = None
        self._temp_fileobj = None

    @classmethod
    def is_compatible(self, download_target):
        return isinstance(download_target, six.string_types)

    def get_fileobj_for_io_writes(self, transfer_future):
        fileobj = transfer_future.meta.call_args.fileobj
        self._final_filename = fileobj
        self._temp_filename = fileobj + os.extsep + random_file_extension()
        self._temp_fileobj = self._get_temp_fileobj()
        return self._temp_fileobj

    def get_final_io_task(self):
        # A task to rename the file from the temporary file to its final
        # location is needed. This should be the last task needed to complete
        # the download.
        return IORenameFileTask(
            transfer_coordinator=self._transfer_coordinator,
            main_kwargs={
                'fileobj': self._temp_fileobj,
                'final_filename': self._final_filename,
                'osutil': self._osutil
            },
            is_final=True
        )

    def _get_temp_fileobj(self):
        f = self._osutil.open(self._temp_filename, 'wb')
        # Make sure the file gets closed and we remove the temporary file
        # if anything goes wrong during the process.
        self._transfer_coordinator.add_failure_cleanup(f.close)
        self._transfer_coordinator.add_failure_cleanup(
            self._osutil.remove_file, self._temp_filename)
        return f


class DownloadSeekableOutputManager(DownloadOutputManager):
    @classmethod
    def is_compatible(self, download_target):
        return seekable(download_target)

    def get_fileobj_for_io_writes(self, transfer_future):
        # Return the fileobj provided to the future.
        return transfer_future.meta.call_args.fileobj

    def get_final_io_task(self):
        # This task will serve the purpose of signaling when all of the io
        # writes have finished so done callbacks can be called.
        return CompleteDownloadNOOPTask(
            transfer_coordinator=self._transfer_coordinator)


class DownloadSubmissionTask(SubmissionTask):
    """Task for submitting tasks to execute a download"""

    def _get_download_output_manager_cls(self, transfer_future):
        """Retrieves a class for managing output for a download

        :type transfer_future: s3transfer.futures.TransferFuture
        :param transfer_future: The transfer future for the request

        :rtype: class of DownloadOutputManager
        :returns: The appropriate class to use for managing a specific type of
            input for downloads.
        """
        download_manager_resolver_chain = [
            DownloadFilenameOutputManager,
            DownloadSeekableOutputManager
        ]

        fileobj = transfer_future.meta.call_args.fileobj
        for download_manager_cls in download_manager_resolver_chain:
            if download_manager_cls.is_compatible(fileobj):
                return download_manager_cls
        raise RuntimeError(
            'Output %s of type: %s is not supported.' % (
                fileobj, type(fileobj)))

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

        download_output_manager = self._get_download_output_manager_cls(
            transfer_future)(osutil, self._transfer_coordinator)

        # If it is greater than threshold do a ranged download, otherwise
        # do a regular GetObject download.
        if transfer_future.meta.size < config.multipart_threshold:
            self._submit_download_request(
                client, config, osutil, request_executor, io_executor,
                download_output_manager, transfer_future)
        else:
            self._submit_ranged_download_request(
                client, config, osutil, request_executor, io_executor,
                download_output_manager, transfer_future)

    def _submit_download_request(self, client, config, osutil,
                                 request_executor, io_executor,
                                 download_output_manager, transfer_future):
        call_args = transfer_future.meta.call_args

        # Get a handle to the file that will be used for writing downloaded
        # contents
        fileobj = download_output_manager.get_fileobj_for_io_writes(
            transfer_future)

        # Get the needed callbacks for the task
        progress_callbacks = get_callbacks(transfer_future, 'progress')

        # Submit the task to download the object.
        download_future = self._submit_task(
            request_executor,
            GetObjectTask(
                transfer_coordinator=self._transfer_coordinator,
                main_kwargs={
                    'client': client,
                    'bucket': call_args.bucket,
                    'key': call_args.key,
                    'fileobj': fileobj,
                    'extra_args': call_args.extra_args,
                    'callbacks': progress_callbacks,
                    'max_attempts': config.num_download_attempts,
                    'io_executor': io_executor
                }
            )
        )

        # Send the necessary tasks to complete the download.
        self._complete_download(
            request_executor, io_executor, download_output_manager,
            [download_future])

    def _submit_ranged_download_request(self, client, config, osutil,
                                        request_executor, io_executor,
                                        download_output_manager,
                                        transfer_future):
        call_args = transfer_future.meta.call_args

        # Get the needed progress callbacks for the task
        progress_callbacks = get_callbacks(transfer_future, 'progress')

        # Get a handle to the file that will be used for writing downloaded
        # contents
        fileobj = download_output_manager.get_fileobj_for_io_writes(
            transfer_future)

        # Determine the number of parts
        part_size = config.multipart_chunksize
        num_parts = int(
            math.ceil(transfer_future.meta.size / float(part_size)))

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
                            'fileobj': fileobj,
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
            request_executor, io_executor, download_output_manager,
            ranged_downloads)

    def _complete_download(self, request_executor, io_executor,
                           download_output_manager, download_futures):

        # Get the final io task that will be placed into the io queue once
        # all of the other GetObjectTasks have completed.
        final_task = download_output_manager.get_final_io_task()
        submit_final_task = FunctionContainer(
            self._submit_task, io_executor, final_task)

        # Submit a task to wait for all of the downloads to complete
        # and submit their downloaded content to the io executor before
        # submitting the final task to the io executor that ensures that all
        # of the io tasks have been completed.
        self._submit_task(
            request_executor,
            JoinFuturesTask(
                transfer_coordinator=self._transfer_coordinator,
                pending_main_kwargs={
                    'futures_to_wait_on': download_futures
                },
                done_callbacks=[submit_final_task]
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
    STREAM_CHUNK_SIZE = 64 * 1024

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


class CompleteDownloadNOOPTask(Task):
    """A NOOP task to serve as an indicator that the download is complete

    Note that the default for is_final is set to True because this should
    always be the last task.
    """
    def __init__(self, transfer_coordinator, main_kwargs=None,
                 pending_main_kwargs=None, done_callbacks=None,
                 is_final=True):
        super(CompleteDownloadNOOPTask, self).__init__(
            transfer_coordinator=transfer_coordinator,
            main_kwargs=main_kwargs,
            pending_main_kwargs=pending_main_kwargs,
            done_callbacks=done_callbacks,
            is_final=is_final
        )

    def _main(self):
        pass
