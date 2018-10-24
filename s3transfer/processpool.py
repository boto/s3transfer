# Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import collections
import logging
import multiprocessing
import threading

import botocore.session

from s3transfer.constants import MB
from s3transfer.exceptions import RetriesExceededError
from s3transfer.utils import S3_RETRYABLE_DOWNLOAD_ERRORS

logger = logging.getLogger(__name__)

SHUTDOWN_SIGNAL = 'SHUTDOWN'
GetObjectJob = collections.namedtuple(
    'GetObjectJob', [
        'transfer_id',
        'bucket',
        'key',
        'filename',
        'extra_args',
        'offset',
        'final_filename',
    ]
)


class ClientFactory(object):
    def __init__(self, client_kwargs=None):
        """Creates S3 clients for processes

        Botocore sessions and clients are not pickleable so they cannot be
        inherited across Process boundaries. Instead, they must be instantiated
        once a process is running.
        """
        self._client_kwargs = client_kwargs
        if self._client_kwargs is None:
            self._client_kwargs = {}

    def create_client(self):
        """Create a botocore S3 client"""
        return botocore.session.Session().create_client(
            's3', **self._client_kwargs)


class TransferMonitor(object):
    def __init__(self):
        """Monitors transfers for cross-proccess communication

        Notifications can be sent to the monitor and information can be
        retrieved from the monitor for a particular transfer. Even though
        """
        self._transfer_exceptions = {}
        self._transfer_job_count = {}
        self._job_count_lock = threading.Lock()

    def notify_exception(self, transfer_id, exception):
        """Notify an exception was encountered for a transfer

        :param transfer_id: Unique identifier for the transfer
        :param exception: The exception encountered for that transfer
        """
        # TODO: Not all exceptions are pickleable so if we are running
        # this in a  multiprocessing.BaseManager we will want to
        # make sure to update this signature to ensure pickleability of the
        # arguments or have the ProxyObject do the serialization.
        self._transfer_exceptions[transfer_id] = exception

    def get_exception(self, transfer_id):
        """Retrieve the exception encountered for the transfer

        :param transfer_id: Unique identifier for the transfer
        :return: The exception encountered for that transfer. Otherwise
            if there were no exceptions, returns None.
        """
        return self._transfer_exceptions.get(transfer_id)

    def notify_expected_jobs_to_complete(self, transfer_id, num_jobs):
        """Notify the amount of jobs expected for a transfer

        :param transfer_id: Unique identifier for the transfer
        :param num_jobs: The number of jobs to complete the transfer
        """
        self._transfer_job_count[transfer_id] = num_jobs

    def notify_job_complete(self, transfer_id):
        """Notify that a single job is completed for a transfer

        :param transfer_id: Unique identifier for the transfer
        :return: The number of jobs remaining to complete the transfer
        """
        with self._job_count_lock:
            self._transfer_job_count[transfer_id] -= 1
            return self._transfer_job_count[transfer_id]


class GetObjectWorker(multiprocessing.Process):
    _MAX_ATTEMPTS = 5
    _IO_CHUNKSIZE = 2 * MB

    def __init__(self, queue, client_factory, transfer_monitor, osutil):
        """Fulfills GetObjectJobs

        Downloads the S3 object, writes it to the specified file, and
        renames the file to its final location if it completes the final
        job for a particular transfer.

        :param queue: Queue for retrieving GetObjectJob's
        :param client_factory: ClientFactory for creating S3 clients
        :param transfer_monitor: Monitor for notifying
        :param osutil: OSUtils object to use for os-related behavior when
            performing the transfer.
        """
        super(GetObjectWorker, self).__init__()
        self._queue = queue
        self._client_factory = client_factory
        self._transfer_monitor = transfer_monitor
        self._osutil = osutil
        self._client = None

    def run(self):
        # Client are not pickleable so their instantiation cannot happen
        # in the __init__ for processes that are created under the
        # spawn method.
        self._client = self._client_factory.create_client()
        while True:
            job = self._queue.get()
            if job == SHUTDOWN_SIGNAL:
                return
            if not self._transfer_monitor.get_exception(job.transfer_id):
                self._run_get_object_job(job)
            remaining = self._transfer_monitor.notify_job_complete(
                job.transfer_id)
            if not remaining:
                self._finalize_download(
                    job.transfer_id, job.filename, job.final_filename
                )

    def _run_get_object_job(self, job):
        try:
            self._do_get_object(
                bucket=job.bucket, key=job.key, filename=job.filename,
                extra_args=job.extra_args, offset=job.offset
            )
        except Exception as e:
            self._transfer_monitor.notify_exception(job.transfer_id, e)

    def _do_get_object(self, bucket, key, extra_args, filename, offset):
        last_exception = None
        for i in range(self._MAX_ATTEMPTS):
            try:
                response = self._client.get_object(
                    Bucket=bucket, Key=key, **extra_args)
                self._write_to_file(filename, offset, response['Body'])
                return
            except S3_RETRYABLE_DOWNLOAD_ERRORS as e:
                logger.debug('Retrying exception caught (%s), '
                             'retrying request, (attempt %s / %s)', e, i,
                             self._MAX_ATTEMPTS, exc_info=True)
                last_exception = e
        raise RetriesExceededError(last_exception)

    def _write_to_file(self, filename, offset, body):
        with open(filename, 'rb+') as f:
            f.seek(offset)
            chunks = iter(lambda: body.read(self._IO_CHUNKSIZE), b'')
            for chunk in chunks:
                f.write(chunk)

    def _finalize_download(self, transfer_id, filename, final_filename):
        if self._transfer_monitor.get_exception(transfer_id):
            self._osutil.remove_file(filename)
        else:
            self._do_file_rename(transfer_id, filename, final_filename)

    def _do_file_rename(self, transfer_id, filename, final_filename):
        try:
            self._osutil.rename_file(filename, final_filename)
        except Exception as e:
            self._transfer_monitor.notify_exception(transfer_id, e)
            self._osutil.remove_file(filename)
