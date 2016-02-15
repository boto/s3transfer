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
from concurrent import futures
import functools
import threading


class TransferFuture(object):
    def __init__(self, meta=None, coordinator=None):
        """The future associated to a submitted transfer request

        :type meta: TransferMeta
        :param meta: The metadata associated to the request. This object
            is visible to the requester..

        :type coordinator: TransferCoordinator
        :param coordinator: The coordinator associated to the request. This
            object is not visible to the requester.
        """
        self._meta = meta
        if meta is None:
            self._meta = TransferMeta()

        self._coordinator = coordinator
        if coordinator is None:
            self._coordinator = TransferCoordinator()

    @property
    def meta(self):
        """The metadata associated tio the TransferFuture"""
        return self._meta

    def done(self):
        """Determines if a TransferFuture has completed

        :returns: True if completed. False, otherwise.
        """
        return self._coordinator.done()

    def result(self):
        """Waits until TransferFuture is done and returns the result

        If the TransferFuture succeeded, it will return the result. If the
        TransferFuture failed, it will raise the exception associated to the
        failure.
        """
        return self._coordinator.result()

    def cancel(self):
        """Cancels the request associated with the TransferFuture"""
        self._coordinator.cancel()


class TransferMeta(object):
    """Holds metadata about the TransferFuture"""
    def __init__(self, call_args=None):
        self._call_args = call_args
        self._size = None
        self._user_context = {}

    @property
    def call_args(self):
        """The call args used in the transfer request"""
        return self._call_args

    @property
    def size(self):
        """The size of the transfer request if known"""
        return self._size

    @property
    def user_context(self):
        """A dictionary that requesters can store data in"""
        return self._user_context

    def provide_transfer_size(self, size):
        """A method to provide the size of a transfer request

        By providing this value, the TransferManager will not try to
        call HeadObject or use the use OS to determine the size of the
        transfer.
        """
        self._size = size


class TransferCoordinator(object):
    """A helper class for managing TransferFuture"""
    def __init__(self):
        self._status = 'queued'
        self._result = None
        self._exception = None
        self._failure_cleanups = []
        self._done_event = threading.Event()
        self._lock = threading.Lock()

    @property
    def failure_cleanups(self):
        """The list of callbacks to call when the TransferFuture fails"""
        return self._failure_cleanups

    @property
    def status(self):
        """The status of the TransferFuture

        The currently supported states are:
            * queued - Has yet to start
            * running - Is inprogress
            * cancelled - Was cancelled
            * failed - An exception other than CancelledError was thrown
            * success - No exceptions were thrown and is done.
        """
        with self._lock:
            return self._status

    def set_result(self, result):
        """Set a result for the TransferFuture

        Implies that the TransferFuture succeeded.
        """
        with self._lock:
            self._result = result
            self._status = 'success'

    def set_exception(self, exception):
        """Set an exception for the TransferFuture

        Implies the TransferFuture failed.
        """
        with self._lock:
            self._exception = exception
            self._status = 'failed'

    def result(self):
        """Waits until TransferFuture is done and returns the result

        If the TransferFuture succeeded, it will return the result. If the
        TransferFuture failed, it will raise the exception associated to the
        failure.
        """
        self._done_event.wait()
        with self._lock:
            if self._exception:
                raise self._exception
            return self._result

    def cancel(self):
        """Cancels the TransferFuture"""
        with self._lock:
            self._exception = futures.CancelledError
            self._status = 'cancelled'

    def set_status_to_running(self):
        """Sets the TransferFuture's status to running"""
        with self._lock:
            self._status = 'running'

    def done(self):
        """Determines if a TransferFuture has completed

        :returns: False if status is equal to 'failed', 'cancelled', or
            'success'. True, otherwise
        """
        return self.status in ['failed', 'cancelled', 'success']

    def add_failure_cleanup(self, function, *args, **kwargs):
        """Adds a callback to call upon failure"""
        with self._lock:
            self._failure_cleanups.append(
                functools.partial(function, *args, **kwargs)
            )

    def announce_done(self):
        """Announce that future is done running

        This allows the result() to be unblocked.
        """
        self._done_event.set()


class BoundedExecutor(object):
    EXECUTOR_CLS = futures.ThreadPoolExecutor

    def __init__(self, max_size, max_num_threads):
        """An executor implentation that has a maximum queued up tasks

        The executor will block if the number of tasks that have been
        submitted and is currently working on is past its maximum.

        :params max_size: The size of the queue. A size of None means that
            the executor will have no bound.

        :params max_num_threads: The maximum number of threads the executor
            uses.
        """
        self._max_size = max_size
        self._max_num_threads = max_num_threads
        self._executor = self.EXECUTOR_CLS(max_workers=self._max_num_threads)
        self._currently_running_futures = set()
        self._lock = threading.Lock()

    def submit(self, fn, *args, **kwargs):
        """Submit a task to complete

        :param fn: The function to call
        :param args: The positional arguments to use to call the function
        :param kwargs: The keyword arguments to use to call the function

        :returns: The future assocaited to the submitted task
        """
        with self._lock:
            # First determine if there are already too many futures running
            if self._at_maximum_running_futures():
                # If there are too many futures running, wait till some
                # complete and save the remaining running futures as the
                # next set to wait for.
                self._currently_running_futures = futures.wait(
                    self._currently_running_futures,
                    return_when=futures.FIRST_COMPLETED
                ).not_done
            # Submit the task to the executor
            future = self._executor.submit(fn, *args, **kwargs)
            # If the queue is bounded then add the new submitted task
            # to the list of futures to wait for. If it is not bounded,
            # then we do not need to store the future as we are not keeping
            # track of how many futures are currently being ran.
            if self._max_size is not None:
                self._currently_running_futures.add(future)
        return future

    def shutdown(self, wait=True):
        self._executor.shutdown(wait)

    def _at_maximum_running_futures(self):
        return len(self._currently_running_futures) == self._max_size
