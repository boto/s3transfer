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
import copy
from concurrent import futures
import logging

from s3transfer.futures import TransferFuture
from s3transfer.futures import TransferContext
from s3transfer.futures import TransferMeta
from s3transfer.utils import get_callbacks


logger = logging.getLogger(__name__)


class TaskSubmitter(object):
    """Submits a series of task to fufill a request

    This is the base class and must be subclassed from and have the
    submit() method implemented.
    """
    def __init__(self, client, config, osutil, executor):
        """
        :param client: The client associated with the transfer manager

        :type config: s3transfer.manager.TransferConfig
        :param config: The transfer config associated with the transfer
            manager

        :type osutil: s3transfer.utils.OSUtil
        :param osutil: The os utility associated to the transfer manager

        :type executor: concurrent.futures.Executor-like object
        :param executor: The executor associated with the transfer manager
        """
        self._client = client
        self._config = config
        self._osutil = osutil
        self._executor = executor

    def _initialize_transfer_future(self, call_args):
        components = {
            'meta': TransferMeta(call_args),
            'context': TransferContext()
        }
        transfer_future = TransferFuture(**components)
        on_queued_callbacks = get_callbacks(transfer_future, 'queued')
        for on_queued_callback in on_queued_callbacks:
            on_queued_callback()
        return transfer_future, components

    def __call__(self, call_args=None):
        """Initializes and submits a transfer requests corresponding tasks

        :param call_args: The call arguments associated to the request

        :rtype: s3transfer.futures.TransferFuture
        :returns: The transfer future associated with the submition.
        """
        transfer_future, components = self._initialize_transfer_future(
            call_args)
        self._submit(transfer_future, transfer_context=components['context'])
        return transfer_future

    def _submit(self, transfer_future, transfer_context):
        """The submition method to be implemented

        The implementation of the method must accept two arguments:

        :type transfer_context: s3transfer.futures.TransferContext
        :param transfer_context: The transfer context associated to the
            transfer future.

        :type transfer_future: s3transfer.futures.TransferFuture
        :param transfer_future: The transfer future associated with the
            transfer request
        """
        raise NotImplementedError('_submit() must be implemented')


class Task(object):
    """A task associated to a TransferFuture request

    This is a base class for other classes to subclass from. All subclassed
    classes must implement the main() method.
    """
    def __init__(self, transfer_context, main_kwargs=None,
                 pending_main_kwargs=None, done_callbacks=None,
                 is_final=False):
        """
        :type transfer_context: s3transfer.futures.TransferContext
        :param transfer_context: The context associated to the TransferFuture
            for which this Task is associated with.

        :type main_kwargs: dict
        :param main_kwargs: The keyword args that can be immediately supplied
            to the _main() method of the task

        :type pending_main_kwargs: dict
        :param pending_main_kwargs: The keyword args that are depended upon
            by the result from a dependent future(s). The result returned by
            the future(s) will be used as the value for the keyword argument
            when _main() is called. The values for each key can be:
                * a single future - Once completed, its value will be the
                  result of that single future
                * a list of futures - Once all of the futures complete, the
                  value used will be a list of each completed future result
                  value in order of when they were originally supplied.

        :type done_callbacks: list of callbacks
        :param done_callbacks: A list of callbacks to call once the task is
            done completing. Each callback will be called with no arguments
            and will be called no matter if the task succeeds or an exception
            is raised.

        :type is_final: boolean
        :param is_final: True, to indicate that this task is the final task
            for the TransferFuture request. By setting this value to True, it
            will set the result of the entire TransferFuture to the result
            returned by this task's main() method.
        """
        self._transfer_context = transfer_context

        self._main_kwargs = main_kwargs
        if self._main_kwargs is None:
            self._main_kwargs = {}

        self._pending_main_kwargs = pending_main_kwargs
        if pending_main_kwargs is None:
            self._pending_main_kwargs = {}

        self._done_callbacks = done_callbacks
        if self._done_callbacks is None:
            self._done_callbacks = []

        self._is_final = is_final

    def __call__(self):
        """The callable to use when submitting a Task to an executor"""
        try:
            # If the TransferFuture's status is currently queued,
            # set it to running.
            if self._transfer_context.status == 'queued':
                self._transfer_context.set_status_to_running()
            # Wait for all of futures this task depends on.
            self._wait_on_dependent_futures()
            # Gather up all of the main keyword arguments for main().
            # This includes the immediately provided main_kwargs and
            # the values for pending_main_kwargs that source from the return
            # values from the task's depenent futures.
            kwargs = self._get_all_main_kwargs()
            # If the task is not done (really only if some other related
            # task to the TransferFuture had failed) then execute the task's
            # main() method.
            if not self._transfer_context.done():
                return_value = self._main(**kwargs)
                # If the task is the final task, then set the TransferFuture's
                # value to the return value from main().
                if self._is_final:
                    self._transfer_context.set_result(return_value)
                return return_value
        except Exception as e:
            logger.debug("Exception raised.", exc_info=True)
            # If an exception is ever thrown than set the exception for the
            # entire TransferFuture.
            self._transfer_context.set_exception(e)
        finally:
            # If this is the final task and it failed, then run the failure
            # cleanups associated with the TransferFuture. This needs to
            # happen in the final task as opposed to the Exception clause
            # becasue we do not want these being called twice or if the
            # task gets cancelled, it will never reach the exception clause.
            if self._is_final and self._transfer_context.status != 'success':
                for failure_cleanup in self._transfer_context.failure_cleanups:
                    # If any of the failure cleanups fail do not get the
                    # main process in a deadlock from waiting to announce
                    # that it is done.
                    try:
                        failure_cleanup()
                    except Exception as e:
                        logger.debug("Exception raised in cleanup.",
                                     exc_info=True)

            # If this is the final task announce that it is done if results
            # are waiting on its completion.
            if self._is_final:
                self._transfer_context.announce_done()
            # Run any done callbacks associated to the task no matter what.
            for done_callback in self._done_callbacks:
                done_callback()

    def _main(self, **kwargs):
        """The method that will be ran in the executor

        This method must be implemented by subclasses from Task. main() can
        be implemented with any arguments decided upon by the subclass.
        """
        raise NotImplementedError('_main() must be implemented')

    def _wait_on_dependent_futures(self):
        # Gather all of the futures into that main() depends on.
        futures_to_wait_on = []
        for _, future in self._pending_main_kwargs.items():
            # If the pending main keyword arg is a list then extend the list.
            if isinstance(future, list):
                futures_to_wait_on.extend(future)
            # If the pending main keword arg is a future append it to the list.
            else:
                futures_to_wait_on.append(future)
        # Now wait for all of the futures to complete.
        futures.wait(futures_to_wait_on)

    def _get_all_main_kwargs(self):
        # Copy over all of the kwargs that we know is available.
        kwargs = copy.copy(self._main_kwargs)

        # Iterate through the kwargs whose values are pending on the result
        # of a future.
        for key, pending_value in self._pending_main_kwargs.items():
            # If the value is a list of futures, iterate though the list
            # appending on the result from each future.
            if isinstance(pending_value, list):
                result = []
                for future in pending_value:
                    result.append(future.result())
            # Otherwise if the pending_value is a future, just wait for it.
            else:
                result = pending_value.result()
            # Add the retrieved value to the kwargs to be sent to the
            # main() call.
            kwargs[key] = result
        return kwargs
