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
from s3transfer.futures import TransferFuture
from s3transfer.futures import TransferMeta
from s3transfer.futures import TransferContext
from s3transfer.futures import BoundedExecutor


def return_call_args(*args, **kwargs):
    return args, kwargs


class TestTransferFuture(unittest.TestCase):
    def setUp(self):
        self.meta = TransferMeta()
        self.context = TransferContext()
        self.future = TransferFuture(self.meta, self.context)

    def test_meta(self):
        self.assertIs(self.future.meta, self.meta)

    def test_done(self):
        self.assertFalse(self.future.done())
        self.context.set_result(None)
        self.assertTrue(self.future.done())

    def test_result(self):
        result = 'foo'
        self.context.set_result(result)
        self.context.announce_done()
        self.assertEqual(self.future.result(), result)

    def test_cancel(self):
        self.future.cancel()
        self.assertTrue(self.future.done())
        self.assertEqual(self.context.status, 'cancelled')


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

    def test_user_context(self):
        self.transfer_meta.user_context['foo'] = 'bar'
        self.assertEqual(self.transfer_meta.user_context, {'foo': 'bar'})


class TestTransferContext(unittest.TestCase):
    def setUp(self):
        self.transfer_context = TransferContext()

    def test_initial_status(self):
        # A TransferContext with no progress should have the status of queued
        self.assertEqual(self.transfer_context.status, 'queued')

    def test_status_running(self):
        self.transfer_context.set_status_to_running()
        self.assertEqual(self.transfer_context.status, 'running')

    def test_set_result(self):
        success_result = 'foo'
        self.transfer_context.set_result(success_result)
        self.transfer_context.announce_done()
        # Setting result should result in a success state and the return value
        # that was set.
        self.assertEqual(self.transfer_context.status, 'success')
        self.assertEqual(self.transfer_context.result(), success_result)

    def test_set_exception(self):
        exception_result = RuntimeError
        self.transfer_context.set_exception(exception_result)
        self.transfer_context.announce_done()
        # Setting an exception should result in a failed state and the return
        # value should be the rasied exception
        self.assertEqual(self.transfer_context.status, 'failed')
        with self.assertRaises(exception_result):
            self.transfer_context.result()

    def test_cancel(self):
        self.transfer_context.cancel()
        self.transfer_context.announce_done()
        # This should set the state to cancelled and raise the CancelledError
        # exception.
        self.assertEqual(self.transfer_context.status, 'cancelled')
        with self.assertRaises(CancelledError):
            self.transfer_context.result()

    def test_done(self):
        # These should result in not done state:
        # queued
        self.assertFalse(self.transfer_context.done())
        # running
        self.transfer_context.set_status_to_running()
        self.assertFalse(self.transfer_context.done())

        # These should result in done state:
        # failed
        self.transfer_context.set_exception(Exception)
        self.assertTrue(self.transfer_context.done())

        # success
        self.transfer_context.set_result('foo')
        self.assertTrue(self.transfer_context.done())

        # cancelled
        self.transfer_context.cancel()
        self.assertTrue(self.transfer_context.done())

    def test_result_waits_until_done(self):
        execution_order = []

        def sleep_then_set_result(transfer_context, execution_order):
            time.sleep(0.05)
            execution_order.append('setting_result')
            transfer_context.set_result(None)
            self.transfer_context.announce_done()

        with ThreadPoolExecutor(max_workers=1) as executor:
            executor.submit(
                sleep_then_set_result, self.transfer_context,
                execution_order)
            self.transfer_context.result()
            execution_order.append('after_result')

        # The result() call should have waited until the other thread set
        # the result after sleeping for 0.05 seconds.
        self.assertTrue(execution_order, ['setting_result', 'after_result'])

    def test_failure_cleanups(self):
        args = (1, 2)
        kwargs = {'foo': 'bar'}

        second_args = (2, 4)
        second_kwargs = {'biz': 'baz'}

        self.transfer_context.add_failure_cleanup(
            return_call_args, *args, **kwargs)
        self.transfer_context.add_failure_cleanup(
            return_call_args, *second_args, **second_kwargs)

        # Ensure the callbacks got added.
        self.assertEqual(len(self.transfer_context.failure_cleanups), 2)

        result_list = []
        # Ensure they will get called in the correct order.
        for cleanup in self.transfer_context.failure_cleanups:
            result_list.append(cleanup())
        self.assertEqual(
            result_list, [(args, kwargs), (second_args, second_kwargs)])


class TestBoundedExecutor(unittest.TestCase):
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

    def sleep_then_add_to_list(self, sleep_times, sleep_amt):
        time.sleep(sleep_amt)
        sleep_times.append(sleep_amt)

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
