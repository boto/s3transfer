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
from collections import namedtuple
import copy
import logging
import threading

from s3transfer.compat import MAXINT
from s3transfer.utils import FunctionContainer


logger = logging.getLogger(__name__)


class TransferFuture(object):
    def __init__(self, meta=None, coordinator=None):
        """The future associated to a submitted transfer request

        :type meta: TransferMeta
        :param meta: The metadata associated to the request. This object
            is visible to the requester.

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
        try:
            # Usually the result() method blocks until the transfer is done,
            # however if a KeyboardInterrupt is raised we want want to exit
            # out of this and propogate the exception.
            return self._coordinator.result()
        except KeyboardInterrupt as e:
            self.cancel()
            raise e

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
        self._associated_futures = []
        self._failure_cleanups = []
        self._done_callbacks = []
        self._done_event = threading.Event()
        self._lock = threading.Lock()
        self._associated_futures_lock = threading.Lock()
        self._done_callbacks_lock = threading.Lock()
        self._failure_cleanups_lock = threading.Lock()

    @property
    def exception(self):
        return self._exception

    @property
    def associated_futures(self):
        """The list of futures associated to the inprogress TransferFuture

        Once the transfer finishes this list becomes empty as the transfer
        is considered done and there should be no running futures left.
        """
        with self._associated_futures_lock:
            # We return a copy of the list because we do not want to
            # processing the returned list while another thread is adding
            # more futures to the actual list.
            return copy.copy(self._associated_futures)

    @property
    def failure_cleanups(self):
        """The list of callbacks to call when the TransferFuture fails"""
        return self._failure_cleanups

    @property
    def status(self):
        """The status of the TransferFuture

        The currently supported states are:
            * queued - Has yet to start
            * running - Is inprogress. In-progress as of now means that
              the SubmissionTask that runs the transfer is being executed. So
              there is no guarantee any transfer requests had been made to
              S3 if this state is reached.
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
        if not self.done():
            with self._lock:
                self._result = result
                self._status = 'success'

    def set_exception(self, exception):
        """Set an exception for the TransferFuture

        Implies the TransferFuture failed.
        """
        if not self.done():
            with self._lock:
                self._exception = exception
                self._status = 'failed'

    def result(self):
        """Waits until TransferFuture is done and returns the result

        If the TransferFuture succeeded, it will return the result. If the
        TransferFuture failed, it will raise the exception associated to the
        failure.
        """
        # Doing a wait() with no timeout cannot be interrupted in python2 but
        # can be interrupted in python3 so we just wait with the largest
        # possible value integer value, which is on the scale of billions of
        # years...
        self._done_event.wait(MAXINT)

        # Once done waiting, raise an exception if present or return the
        # final result.
        with self._lock:
            if self._exception:
                raise self._exception
            return self._result

    def cancel(self):
        """Cancels the TransferFuture"""
        if not self.done():
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

    def add_associated_future(self, future):
        """Adds a future to be associated with the TransferFuture"""
        with self._associated_futures_lock:
            self._associated_futures.append(future)

    def add_done_callback(self, function, *args, **kwargs):
        """Add a done callback to be invoked when transfer is done"""
        with self._done_callbacks_lock:
            self._done_callbacks.append(
                FunctionContainer(function, *args, **kwargs)
            )

    def add_failure_cleanup(self, function, *args, **kwargs):
        """Adds a callback to call upon failure"""
        with self._failure_cleanups_lock:
            self._failure_cleanups.append(
                FunctionContainer(function, *args, **kwargs))

    def announce_done(self):
        """Announce that future is done running and run associated callbacks

        This will run any failure cleanups if the transfer failed if not
        they have not been run, allows the result() to be unblocked, and will
        run any done callbacks associated to the TransferFuture if they have
        not already been ran.
        """
        if self.status != 'success':
            self._run_failure_cleanups()
        self._done_event.set()
        self._remove_associated_futures()
        self._run_done_callbacks()

    def _remove_associated_futures(self):
        # Once the process is done, we want to empty the list so we do
        # not hold onto too many completed futures.
        with self._associated_futures_lock:
            self._associated_futures = []

    def _run_done_callbacks(self):
        # Run the callbacks and remove the callbacks from the internal
        # list so they do not get ran again if done is announced more than
        # once.
        with self._done_callbacks_lock:
            self._run_callbacks(self._done_callbacks)
            self._done_callbacks = []

    def _run_failure_cleanups(self):
        # Run the cleanup callbacks and remove the callbacks from the internal
        # list so they do not get ran again if done is announced more than
        # once.
        with self._failure_cleanups_lock:
            self._run_callbacks(self.failure_cleanups)
            self._failure_cleanups = []

    def _run_callbacks(self, callbacks):
        for callback in callbacks:
            self._run_callback(callback)

    def _run_callback(self, callback):
        try:
            callback()
        # We do not want a callback interrupting the process, especially
        # in the failure cleanups. So log and catch, the excpetion.
        except Exception:
            logger.debug("Exception raised in %s." % callback, exc_info=True)


class BoundedExecutor(object):
    EXECUTOR_CLS = futures.ThreadPoolExecutor

    def __init__(self, max_size, max_num_threads, tag_max_sizes=None):
        """An executor implentation that has a maximum queued up tasks

        The executor will block if the number of tasks that have been
        submitted and is currently working on is past its maximum.

        :params max_size: The maximum number of inflight futures. An inflight
            future means that the task is either queued up or is currently
            being executed. A size of None or 0 means that the executor will
            have no bound in terms of the number of inflight futures.

        :params max_num_threads: The maximum number of threads the executor
            uses.

        :type tag_max_sizes: dict
        :params tag_max_sizes: A dictionary where the key is the name of the
            tag and the value is the number of inflight futures can be running
            with that tag value. For example, a value of  ``{'my_tag': 3}``
            will ensure that where will be at most three inflight futures
            tagged with the value ``'my_tag'``. Note that no matter if a future
            is tagged, the future will be accounted against the executor's
            ``max_size`` param.
        """
        self._max_num_threads = max_num_threads
        self._executor = self.EXECUTOR_CLS(max_workers=self._max_num_threads)
        self._tags_to_track = [ALL_TAG]
        self._max_sizes = {
            ALL_TAG: max_size
        }
        self._currently_running_futures = {
            ALL_TAG: set()
        }
        if tag_max_sizes:
            for tag, max_size in tag_max_sizes.items():
                self._tags_to_track.append(tag)
                self._max_sizes[tag] = max_size
                self._currently_running_futures[tag] = set()
        self._lock = threading.Lock()

    def submit(self, fn, *args, **kwargs):
        """Submit a task to complete

        :param fn: The function to call
        :param args: The positional arguments to use to call the function
        :param kwargs: The keyword arguments to use to call the function. You
            can provide a tag for the submittion by providing the keyword arg
            ``'future_tag'``.

        :returns: The future assocaited to the submitted task
        """
        tag = kwargs.pop('future_tag', None)

        with self._lock:
            # Wait if there is a maximum reached
            self._wait_for_futures_to_complete_if_needed(tag)
            # Submit the task to the executor
            future = self._executor.submit(fn, *args, **kwargs)
            # Add the recently added future to the currently running futures
            self._add_future_to_currently_running(future, tag)
        return future

    def shutdown(self, wait=True):
        self._executor.shutdown(wait)

    def _get_tags_to_operate_on(self, tag):
        # No matter the tag provided, each future is included under the 'all'
        # tag to represent its membership in the set of all running futures.
        tag_to_operate_on = [ALL_TAG]
        # Only add the tag to the set of the tags to operate on if the tag
        # is not equal to all and the tag is being tracked for max sizes.
        if tag != ALL_TAG and tag in self._tags_to_track:
            tag_to_operate_on.append(tag)
        return tag_to_operate_on

    def _at_maximum_running_futures(self, tag):
        return len(self._currently_running_futures[tag]) == \
            self._max_sizes[tag]

    def _wait_for_futures_to_complete_if_needed(self, tag):
        tags_to_wait_on = self._get_tags_to_operate_on(tag)
        for tag in tags_to_wait_on:
            # First determine if there are already too many futures running
            if self._at_maximum_running_futures(tag):
                # If there are too many futures running, wait till some
                # complete and save the remaining running futures as the
                # next set to wait for.
                self._currently_running_futures[tag] = futures.wait(
                        self._currently_running_futures[tag],
                        return_when=futures.FIRST_COMPLETED
                ).not_done

    def _add_future_to_currently_running(self, future, tag):
        tags_to_add_future_to = self._get_tags_to_operate_on(tag)
        for tag in tags_to_add_future_to:
            # If the queue is bounded then add the new submitted task
            # to the list of futures to wait for. If it is not bounded,
            # then we do not need to store the future as we are not keeping
            # track of how many futures are currently being ran.
            if self._max_sizes[tag] != 0:
                self._currently_running_futures[tag].add(future)


FutureTag = namedtuple('FutureTag', ['name'])

ALL_TAG = FutureTag('all')
IN_MEMORY_UPLOAD_TAG = FutureTag('in_memory_upload')
