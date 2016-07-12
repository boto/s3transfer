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
import time
from concurrent.futures import wait
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import CancelledError

from tests import unittest
from tests import TransferCoordinatorWithInterrupt
from s3transfer.futures import TransferFuture
from s3transfer.futures import TransferMeta
from s3transfer.futures import TransferCoordinator
from s3transfer.futures import BoundedExecutor
from s3transfer.utils import FunctionContainer


def return_call_args(*args, **kwargs):
    return args, kwargs


class RecordingTransferCoordinator(TransferCoordinator):
    def __init__(self):
        self.all_transfer_futures_ever_associated = set()
        super(RecordingTransferCoordinator, self).__init__()

    def add_associated_future(self, future):
        self.all_transfer_futures_ever_associated.add(future)
        super(RecordingTransferCoordinator, self).add_associated_future(future)


class TestTransferFuture(unittest.TestCase):
    def setUp(self):
        self.meta = TransferMeta()
        self.coordinator = TransferCoordinator()
        self.future = self._get_transfer_future()

    def _get_transfer_future(self, **kwargs):
        components = {
            'meta': self.meta,
            'coordinator': self.coordinator,
        }
        for component_name, component in kwargs.items():
            components[component_name] = component
        return TransferFuture(**components)

    def test_meta(self):
        self.assertIs(self.future.meta, self.meta)

    def test_done(self):
        self.assertFalse(self.future.done())
        self.coordinator.set_result(None)
        self.assertTrue(self.future.done())

    def test_result(self):
        result = 'foo'
        self.coordinator.set_result(result)
        self.coordinator.announce_done()
        self.assertEqual(self.future.result(), result)

    def test_keyboard_interrupt_on_result_does_not_block(self):
        # This should raise a KeyboardInterrupt when result is called on it.
        self.coordinator = TransferCoordinatorWithInterrupt()
        self.future = self._get_transfer_future()

        # result() should not block and immediately raise the keyboard
        # interrupt exception.
        with self.assertRaises(KeyboardInterrupt):
            self.future.result()

    def test_cancel(self):
        self.future.cancel()
        self.assertTrue(self.future.done())
        self.assertEqual(self.coordinator.status, 'cancelled')


class TestTransferMeta(unittest.TestCase):
    def setUp(self):
        self.transfer_meta = TransferMeta()

    def test_size(self):
        self.assertEqual(self.transfer_meta.size, None)
        self.transfer_meta.provide_transfer_size(5)
        self.assertEqual(self.transfer_meta.size, 5)

    def test_call_args(self):
        call_args = object()
        transfer_meta = TransferMeta(call_args)
        # Assert the that call args provided is the same as is returned
        self.assertIs(transfer_meta.call_args, call_args)

    def test_id(self):
        transfer_meta = TransferMeta(id=1)
        self.assertEqual(transfer_meta.id, 1)

    def test_user_context(self):
        self.transfer_meta.user_context['foo'] = 'bar'
        self.assertEqual(self.transfer_meta.user_context, {'foo': 'bar'})


class TestTransferCoordinator(unittest.TestCase):
    def setUp(self):
        self.transfer_coordinator = TransferCoordinator()

    def test_id(self):
        transfer_coordinator = TransferCoordinator(id=1)
        self.assertEqual(transfer_coordinator.id, 1)

    def test_initial_status(self):
        # A TransferCoordinator with no progress should have the status
        # of queued
        self.assertEqual(self.transfer_coordinator.status, 'queued')

    def test_status_running(self):
        self.transfer_coordinator.set_status_to_running()
        self.assertEqual(self.transfer_coordinator.status, 'running')

    def test_set_result(self):
        success_result = 'foo'
        self.transfer_coordinator.set_result(success_result)
        self.transfer_coordinator.announce_done()
        # Setting result should result in a success state and the return value
        # that was set.
        self.assertEqual(self.transfer_coordinator.status, 'success')
        self.assertEqual(self.transfer_coordinator.result(), success_result)

    def test_set_exception(self):
        exception_result = RuntimeError
        self.transfer_coordinator.set_exception(exception_result)
        self.transfer_coordinator.announce_done()
        # Setting an exception should result in a failed state and the return
        # value should be the rasied exception
        self.assertEqual(self.transfer_coordinator.status, 'failed')
        self.assertEqual(self.transfer_coordinator.exception, exception_result)
        with self.assertRaises(exception_result):
            self.transfer_coordinator.result()

    def test_exception_cannot_override_done_state(self):
        self.transfer_coordinator.set_result('foo')
        self.transfer_coordinator.set_exception(RuntimeError)
        # It status should be success even after the exception is set because
        # succes is a done state.
        self.assertEqual(self.transfer_coordinator.status, 'success')

    def test_cancel(self):
        self.transfer_coordinator.cancel()
        self.transfer_coordinator.announce_done()
        # This should set the state to cancelled and raise the CancelledError
        # exception.
        self.assertEqual(self.transfer_coordinator.status, 'cancelled')
        with self.assertRaises(CancelledError):
            self.transfer_coordinator.result()

    def test_cancel_cannot_override_done_state(self):
        self.transfer_coordinator.set_result('foo')
        self.transfer_coordinator.cancel()
        # It status should be success even after cancel is called because
        # succes is a done state.
        self.assertEqual(self.transfer_coordinator.status, 'success')

    def test_set_result_can_override_cancel(self):
        self.transfer_coordinator.cancel()
        # Result setting should override any cancel or set exception as this
        # is always invoked by the final task.
        self.transfer_coordinator.set_result('foo')
        self.transfer_coordinator.announce_done()
        self.assertEqual(self.transfer_coordinator.status, 'success')

    def test_submit(self):
        # Submit a callable to the transfer coordinator. It should submit it
        # to the executor.
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = self.transfer_coordinator.submit(
                executor,
                return_call_args,
                future_tag='my-tag'
            )
        # Make sure the future got submit and executed as well by checking its
        # result value which should include the provided future tag.
        self.assertEqual(future.result(), ((), {'future_tag': 'my-tag'}))

    def test_association_and_disassociation_on_submit(self):
        self.transfer_coordinator = RecordingTransferCoordinator()

        # Submit a callable to the transfer coordinator.
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = self.transfer_coordinator.submit(
                executor, return_call_args)

        # Make sure the future that got submitted was associated to the
        # transfer future at some point.
        self.assertEqual(
            self.transfer_coordinator.all_transfer_futures_ever_associated,
            set([future])
        )

        # Make sure the future got disassociated once the future is now done
        # by looking at the currently associated futures.
        self.assertEqual(
            self.transfer_coordinator.associated_futures, set([]))

    def test_done(self):
        # These should result in not done state:
        # queued
        self.assertFalse(self.transfer_coordinator.done())
        # running
        self.transfer_coordinator.set_status_to_running()
        self.assertFalse(self.transfer_coordinator.done())

        # These should result in done state:
        # failed
        self.transfer_coordinator.set_exception(Exception)
        self.assertTrue(self.transfer_coordinator.done())

        # success
        self.transfer_coordinator.set_result('foo')
        self.assertTrue(self.transfer_coordinator.done())

        # cancelled
        self.transfer_coordinator.cancel()
        self.assertTrue(self.transfer_coordinator.done())

    def test_result_waits_until_done(self):
        execution_order = []

        def sleep_then_set_result(transfer_coordinator, execution_order):
            time.sleep(0.05)
            execution_order.append('setting_result')
            transfer_coordinator.set_result(None)
            self.transfer_coordinator.announce_done()

        with ThreadPoolExecutor(max_workers=1) as executor:
            executor.submit(
                sleep_then_set_result, self.transfer_coordinator,
                execution_order)
            self.transfer_coordinator.result()
            execution_order.append('after_result')

        # The result() call should have waited until the other thread set
        # the result after sleeping for 0.05 seconds.
        self.assertTrue(execution_order, ['setting_result', 'after_result'])

    def test_failure_cleanups(self):
        args = (1, 2)
        kwargs = {'foo': 'bar'}

        second_args = (2, 4)
        second_kwargs = {'biz': 'baz'}

        self.transfer_coordinator.add_failure_cleanup(
            return_call_args, *args, **kwargs)
        self.transfer_coordinator.add_failure_cleanup(
            return_call_args, *second_args, **second_kwargs)

        # Ensure the callbacks got added.
        self.assertEqual(len(self.transfer_coordinator.failure_cleanups), 2)

        result_list = []
        # Ensure they will get called in the correct order.
        for cleanup in self.transfer_coordinator.failure_cleanups:
            result_list.append(cleanup())
        self.assertEqual(
            result_list, [(args, kwargs), (second_args, second_kwargs)])

    def test_associated_futures(self):
        first_future = object()
        # Associate one future to the transfer
        self.transfer_coordinator.add_associated_future(first_future)
        associated_futures = self.transfer_coordinator.associated_futures
        # The first future should be in the returned list of futures.
        self.assertEqual(associated_futures, set([first_future]))

        second_future = object()
        # Associate another future to the transfer.
        self.transfer_coordinator.add_associated_future(second_future)
        # The association should not have mutated the returned list from
        # before.
        self.assertEqual(associated_futures, set([first_future]))

        # Both futures should be in the returned list.
        self.assertEqual(
            self.transfer_coordinator.associated_futures,
            set([first_future, second_future]))

    def test_associated_futures_on_done(self):
        future = object()
        self.transfer_coordinator.add_associated_future(future)
        self.assertEqual(
            self.transfer_coordinator.associated_futures, set([future]))

        self.transfer_coordinator.announce_done()
        # When the transfer completes that means all of the futures have
        # completed as well, leaving no need to keep the completed futures
        # around as the transfer is done.
        self.assertEqual(self.transfer_coordinator.associated_futures, set())

    def test_done_callbacks_on_done(self):
        done_callback_invocations = []
        callback = FunctionContainer(
            done_callback_invocations.append, 'done callback called')

        # Add the done callback to the transfer.
        self.transfer_coordinator.add_done_callback(callback)

        # Announce that the transfer is done. This should invoke the done
        # callback.
        self.transfer_coordinator.announce_done()
        self.assertEqual(done_callback_invocations, ['done callback called'])

        # If done is announced again, we should not invoke the callback again
        # because done has already been announced and thus the callback has
        # been ran as well.
        self.transfer_coordinator.announce_done()
        self.assertEqual(done_callback_invocations, ['done callback called'])

    def test_failure_cleanups_on_done(self):
        cleanup_invocations = []
        callback = FunctionContainer(
            cleanup_invocations.append, 'cleanup called')

        # Add the failure cleanup to the transfer.
        self.transfer_coordinator.add_failure_cleanup(callback)

        # Announce that the transfer is done. This should invoke the failure
        # cleanup.
        self.transfer_coordinator.announce_done()
        self.assertEqual(cleanup_invocations, ['cleanup called'])

        # If done is announced again, we should not invoke the cleanup again
        # because done has already been announced and thus the cleanup has
        # been ran as well.
        self.transfer_coordinator.announce_done()
        self.assertEqual(cleanup_invocations, ['cleanup called'])


class BaseBoundedExecutorTest(unittest.TestCase):
    def sleep_then_add_to_list(self, sleep_times, sleep_amt):
        time.sleep(sleep_amt)
        sleep_times.append(sleep_amt)


class TestBoundedExecutor(BaseBoundedExecutorTest):
    def test_submit_single_task(self):
        executor = BoundedExecutor(0, 1)
        args = (1, 2)
        kwargs = {'foo': 'bar'}
        # Ensure we can submit a callable with args and kwargs
        future = executor.submit(return_call_args, *args, **kwargs)
        # Ensure what we get back is a Future
        self.assertIsInstance(future, Future)
        # Ensure the callable got executed.
        self.assertEqual(future.result(), (args, kwargs))

    def test_executor_blocks_on_full_queue(self):
        executor = BoundedExecutor(1, 2)
        sleep_times = []
        slow_task = executor.submit(
            self.sleep_then_add_to_list, sleep_times, 0.05)
        fast_task = executor.submit(
            self.sleep_then_add_to_list, sleep_times, 0)
        # Ensure that fast task did not get executed until the slow task
        # was executed because the queue size of the executor only allows one
        # task to be executed at a time.
        wait([slow_task, fast_task])
        self.assertEqual(sleep_times, [0.05, 0])

    def test_executor_does_not_block_on_nonfull_queue(self):
        executor = BoundedExecutor(0, 2)
        sleep_times = []
        slow_task = executor.submit(
            self.sleep_then_add_to_list, sleep_times, 0.05)
        fast_task = executor.submit(
            self.sleep_then_add_to_list, sleep_times, 0)
        # Ensure that fast task did gets executed first even though it
        # was submitted after the slow task
        wait([slow_task, fast_task])
        self.assertEqual(sleep_times, [0, 0.05])

    def test_shutdown(self):
        executor = BoundedExecutor(0, 2)
        slow_task = executor.submit(time.sleep, 0.05)
        executor.shutdown()
        # Ensure that the shutdown waits until the task is done
        self.assertTrue(slow_task.done())

    def test_shutdown_no_wait(self):
        executor = BoundedExecutor(0, 2)
        slow_task = executor.submit(time.sleep, 0.05)
        executor.shutdown(False)
        # Ensure that the shutdown returns immediately even if the task is
        # not done, which it should not be because it it slow.
        self.assertFalse(slow_task.done())


class TestBoundedExecutorWithTags(BaseBoundedExecutorTest):
    def setUp(self):
        self.default_tag = 'my-tag'
        self.default_tag_size = 1
        self.tag_max_sizes = {self.default_tag: self.default_tag_size}

    def test_submit_single_task(self):
        executor = BoundedExecutor(0, 1, self.tag_max_sizes)
        args = (1, 2)
        kwargs = {'foo': 'bar', 'future_tag': self.default_tag}
        # Ensure we can submit a callable with args and kwargs
        future = executor.submit(return_call_args, *args, **kwargs)
        # Ensure what we get back is a Future
        self.assertIsInstance(future, Future)
        # Ensure the callable got executed.
        self.assertEqual(future.result(), (args, {'foo': 'bar'}))

    def test_executor_blocks_on_max_tag_size(self):
        # The executor is unbounded but their is a tag size set
        executor = BoundedExecutor(0, 2, self.tag_max_sizes)
        sleep_times = []
        slow_task = executor.submit(
            self.sleep_then_add_to_list, sleep_times, 0.05,
            future_tag=self.default_tag)
        fast_task = executor.submit(
            self.sleep_then_add_to_list, sleep_times, 0,
            future_tag=self.default_tag)
        # Ensure that fast task did not get executed until the slow task
        # was executed because the max size for that only allows one
        # task to be executed at a time.
        wait([slow_task, fast_task])
        self.assertEqual(sleep_times, [0.05, 0])

    def test_executor_does_not_block_nontagged_tasks_at_max_tag_size(self):
        # The executor is unbounded but their is a tag size set
        executor = BoundedExecutor(0, 3, self.tag_max_sizes)
        sleep_times = []
        # First submit a task to sleep for a period of time.
        slow_tagged_task = executor.submit(
            self.sleep_then_add_to_list, sleep_times, 0.05,
            future_tag=self.default_tag)
        # Submit a second nontagged task. This should not be blocked by
        # the max tag limit and complete first.
        nontagged_task = executor.submit(
            self.sleep_then_add_to_list, sleep_times, 0.025)
        # Submit a third tagged task that should get blocked by the first
        # slow tagged task, even though it is faster than the second submitted
        # task and finish last as a result.
        fast_tagged_task = executor.submit(
            self.sleep_then_add_to_list, sleep_times, 0,
            future_tag=self.default_tag)

        wait([slow_tagged_task, nontagged_task, fast_tagged_task])
        self.assertEqual(sleep_times, [0.025, 0.05, 0])

    def test_tags_account_toward_max_queue_size(self):
        # Create an executor that has a max size of 1
        executor = BoundedExecutor(1, 2, self.tag_max_sizes)
        sleep_times = []
        slow_task = executor.submit(
            self.sleep_then_add_to_list, sleep_times, 0.05)
        # The fact that this task is tagged should not affect the result of
        # having to wait for the first task due to the total max size of the
        # executor.
        fast_task = executor.submit(
            self.sleep_then_add_to_list, sleep_times, 0,
            future_tag=self.default_tag)
        # Ensure that fast task did not get executed until the slow task
        # was executed because the executor only allows one
        # task to be executed at a time, even though the fast task was tagged
        # and the slow one was not. They should both be equally accounted
        # toward the max size of the executor for all threads.
        wait([slow_task, fast_task])
        self.assertEqual(sleep_times, [0.05, 0])
