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
from functools import partial

from tests import unittest
from tests import RecordingSubscriber
from s3transfer.futures import TransferFuture
from s3transfer.futures import TransferCoordinator
from s3transfer.tasks import Task
from s3transfer.tasks import TaskSubmitter
from s3transfer.utils import CallArgs


class TaskFailureException(Exception):
    pass


class SuccessTask(Task):
    def _main(self, return_value='success'):
        return return_value


class FailureTask(Task):
    def _main(self, exception=TaskFailureException):
        raise exception()


class ReturnKwargsTask(Task):
    def _main(self, **kwargs):
        return kwargs


class NOOPTaskSubmitter(TaskSubmitter):
    def _submit(self, transfer_future, transfer_coordinator):
        pass


class TestTaskSubmitter(unittest.TestCase):
    def setUp(self):
        self.client = object()
        self.config = object()
        self.osutil = object()
        self.executor = object()
        self.call_args = CallArgs(subscribers=[])

    def test_return_value(self):
        task_submitter = NOOPTaskSubmitter(
            self.client, self.config, self.osutil, self.executor)
        future = task_submitter(self.call_args)

        # Make sure a TransferFuture is returned
        self.assertIsInstance(future, TransferFuture)
        # Make sure the call args are associated to the TransferFuture.
        self.assertIs(future.meta.call_args, self.call_args)

    def test_on_queued_callbacks(self):
        task_submitter = NOOPTaskSubmitter(
            self.client, self.config, self.osutil, self.executor)

        subscriber = RecordingSubscriber()
        call_args = CallArgs(subscribers=[subscriber])
        future = task_submitter(call_args)
        # Make sure the on_queued callback of the subscriber is called.
        self.assertEqual(subscriber.on_queued_calls, [{'future': future}])


class TestTask(unittest.TestCase):
    def setUp(self):
        self.transfer_coordinator = TransferCoordinator()

    def test_context_status_transitioning_success(self):
        start_task = SuccessTask(self.transfer_coordinator)

        # Before any task, the status should be queued.
        self.assertEqual(self.transfer_coordinator.status, 'queued')

        # Once the task is called, the status should be set to running.
        start_task()
        self.assertEqual(self.transfer_coordinator.status, 'running')

        # If another task is called, the status still should be running.
        SuccessTask(self.transfer_coordinator)()
        self.assertEqual(self.transfer_coordinator.status, 'running')

        # Once the final task is called, the status should be set to success.
        SuccessTask(self.transfer_coordinator, is_final=True)()
        self.assertEqual(self.transfer_coordinator.status, 'success')

    def test_context_status_transitioning_failed(self):
        SuccessTask(self.transfer_coordinator)()
        self.assertEqual(self.transfer_coordinator.status, 'running')

        # A failure task should result in the failed status
        FailureTask(self.transfer_coordinator)()
        self.assertEqual(self.transfer_coordinator.status, 'failed')

        # Even if the final task comes in and succeeds, it should stay failed.
        SuccessTask(self.transfer_coordinator, is_final=True)()
        self.assertEqual(self.transfer_coordinator.status, 'failed')

    def test_result_setting_for_success(self):
        override_return = 'foo'
        SuccessTask(self.transfer_coordinator)()
        SuccessTask(self.transfer_coordinator, main_kwargs={
            'return_value': override_return}, is_final=True)()

        # The return value for the transfer future should be of the final
        # task.
        self.assertEqual(self.transfer_coordinator.result(), override_return)

    def test_result_setting_for_error(self):
        FailureTask(self.transfer_coordinator)()

        # If another failure comes in, the result should still throw the
        # original exception when result() is eventually called.
        FailureTask(self.transfer_coordinator, main_kwargs={
            'exception': Exception})()

        # Even if a success task comes along, the result of the future
        # should be the original exception
        SuccessTask(self.transfer_coordinator, is_final=True)()
        with self.assertRaises(TaskFailureException):
            self.transfer_coordinator.result()

    def test_done_callbacks_success(self):
        callback_results = []
        SuccessTask(self.transfer_coordinator, done_callbacks=[
            partial(callback_results.append, 'first'),
            partial(callback_results.append, 'second')
        ])()
        # For successful tasks, the done callbacks should get called.
        self.assertEqual(callback_results, ['first', 'second'])

    def test_done_callbacks_failure(self):
        callback_results = []
        FailureTask(self.transfer_coordinator, done_callbacks=[
            partial(callback_results.append, 'first'),
            partial(callback_results.append, 'second')
        ])()
        # For even failed tasks, the done callbacks should get called.
        self.assertEqual(callback_results, ['first', 'second'])

        # Callbacks should continue to be called even after a related failure
        SuccessTask(self.transfer_coordinator, done_callbacks=[
            partial(callback_results.append, 'third'),
            partial(callback_results.append, 'fourth')
        ])()
        self.assertEqual(
            callback_results, ['first', 'second', 'third', 'fourth'])

    def test_failure_cleanups_on_failure(self):
        callback_results = []
        self.transfer_coordinator.add_failure_cleanup(
            callback_results.append, 'first')
        self.transfer_coordinator.add_failure_cleanup(
            callback_results.append, 'second')
        FailureTask(self.transfer_coordinator)()
        # The failure callbacks should have not been called yet because it
        # is not the last task
        self.assertEqual(callback_results, [])

        # Now the failure callbacks should get called.
        SuccessTask(self.transfer_coordinator, is_final=True)()
        self.assertEqual(callback_results, ['first', 'second'])

    def test_no_failure_cleanups_on_success(self):
        callback_results = []
        self.transfer_coordinator.add_failure_cleanup(
            callback_results.append, 'first')
        self.transfer_coordinator.add_failure_cleanup(
            callback_results.append, 'second')
        SuccessTask(self.transfer_coordinator, is_final=True)()
        # The failure cleanups should not have been called because no task
        # failed for the transfer context.
        self.assertEqual(callback_results, [])

    def test_passing_main_kwargs(self):
        main_kwargs = {'foo': 'bar', 'baz': 'biz'}
        ReturnKwargsTask(
            self.transfer_coordinator, main_kwargs=main_kwargs,
            is_final=True)()
        # The kwargs should have been passed to the main()
        self.assertEqual(self.transfer_coordinator.result(), main_kwargs)

    def test_passing_pending_kwargs_single_futures(self):
        pending_kwargs = {}
        ref_main_kwargs = {'foo': 'bar', 'baz': 'biz'}

        # Pass some tasks to an executor
        with futures.ThreadPoolExecutor(1) as executor:
            pending_kwargs['foo'] = executor.submit(
                SuccessTask(
                    self.transfer_coordinator,
                    main_kwargs={'return_value': ref_main_kwargs['foo']}
                )
            )
            pending_kwargs['baz'] = executor.submit(
                SuccessTask(
                    self.transfer_coordinator,
                    main_kwargs={'return_value': ref_main_kwargs['baz']}
                )
            )

        # Create a task that depends on the tasks passed to the executor
        ReturnKwargsTask(
            self.transfer_coordinator, pending_main_kwargs=pending_kwargs,
            is_final=True)()
        # The result should have the pending keyword arg values flushed
        # out.
        self.assertEqual(self.transfer_coordinator.result(), ref_main_kwargs)

    def test_passing_pending_kwargs_list_of_futures(self):
        pending_kwargs = {}
        ref_main_kwargs = {'foo': ['first', 'second']}

        # Pass some tasks to an executor
        with futures.ThreadPoolExecutor(1) as executor:
            first_future = executor.submit(
                SuccessTask(
                    self.transfer_coordinator,
                    main_kwargs={'return_value': ref_main_kwargs['foo'][0]}
                )
            )
            second_future = executor.submit(
                SuccessTask(
                    self.transfer_coordinator,
                    main_kwargs={'return_value': ref_main_kwargs['foo'][1]}
                )
            )
            # Make the pending keyword arg value a list
            pending_kwargs['foo'] = [first_future, second_future]

        # Create a task that depends on the tasks passed to the executor
        ReturnKwargsTask(
            self.transfer_coordinator, pending_main_kwargs=pending_kwargs,
            is_final=True)()
        # The result should have the pending keyword arg values flushed
        # out in the expected order.
        self.assertEqual(self.transfer_coordinator.result(), ref_main_kwargs)

    def test_passing_pending_and_non_pending_kwargs(self):
        main_kwargs = {'nonpending_value': 'foo'}
        pending_kwargs = {}
        ref_main_kwargs = {
            'nonpending_value': 'foo',
            'pending_value': 'bar',
            'pending_list': ['first', 'second']
        }

        # Create the pending tasks
        with futures.ThreadPoolExecutor(1) as executor:
            pending_kwargs['pending_value'] = executor.submit(
                SuccessTask(
                    self.transfer_coordinator,
                    main_kwargs={'return_value':
                                 ref_main_kwargs['pending_value']}
                )
            )

            first_future = executor.submit(
                SuccessTask(
                    self.transfer_coordinator,
                    main_kwargs={'return_value':
                                 ref_main_kwargs['pending_list'][0]}
                )
            )
            second_future = executor.submit(
                SuccessTask(
                    self.transfer_coordinator,
                    main_kwargs={'return_value':
                                 ref_main_kwargs['pending_list'][1]}
                )
            )
            # Make the pending keyword arg value a list
            pending_kwargs['pending_list'] = [first_future, second_future]

        # Create a task that depends on the tasks passed to the executor
        # and just regular nonpending kwargs.
        ReturnKwargsTask(
            self.transfer_coordinator, main_kwargs=main_kwargs,
            pending_main_kwargs=pending_kwargs,
            is_final=True)()
        # The result should have all of the kwargs (both pending and
        # nonpending)
        self.assertEqual(self.transfer_coordinator.result(), ref_main_kwargs)

    def test_single_failed_pending_future(self):
        pending_kwargs = {}

        # Pass some tasks to an executor. Make one successful and the other
        # a failure.
        with futures.ThreadPoolExecutor(1) as executor:
            pending_kwargs['foo'] = executor.submit(
                SuccessTask(
                    self.transfer_coordinator,
                    main_kwargs={'return_value': 'bar'}
                )
            )
            pending_kwargs['baz'] = executor.submit(
                FailureTask(self.transfer_coordinator))

        # Create a task that depends on the tasks passed to the executor
        ReturnKwargsTask(
            self.transfer_coordinator, pending_main_kwargs=pending_kwargs,
            is_final=True)()
        # The end result should raise the exception from the initial
        # pending future value
        with self.assertRaises(TaskFailureException):
            self.transfer_coordinator.result()

    def test_single_failed_pending_future_in_list(self):
        pending_kwargs = {}

        # Pass some tasks to an executor. Make one successful and the other
        # a failure.
        with futures.ThreadPoolExecutor(1) as executor:
            first_future = executor.submit(
                SuccessTask(
                    self.transfer_coordinator,
                    main_kwargs={'return_value': 'bar'}
                )
            )
            second_future = executor.submit(
                FailureTask(self.transfer_coordinator))

            pending_kwargs['pending_list'] = [first_future, second_future]

        # Create a task that depends on the tasks passed to the executor
        ReturnKwargsTask(
            self.transfer_coordinator, pending_main_kwargs=pending_kwargs,
            is_final=True)()
        # The end result should raise the exception from the initial
        # pending future value in the list
        with self.assertRaises(TaskFailureException):
            self.transfer_coordinator.result()
