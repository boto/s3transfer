# Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the 'License'). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the 'license' file accompanying this file. This file is
# distributed on an 'AS IS' BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.
from io import RawIOBase
from botocore.awsrequest import create_request_object
import mock

from tests import skip_if_using_serial_implementation
from tests import StubbedClientTest
from s3transfer.exceptions import CancelledError
from s3transfer.exceptions import FatalError
from s3transfer.futures import BaseExecutor
from s3transfer.manager import TransferManager
from s3transfer.manager import TransferConfig


class ArbitraryException(Exception):
    pass


class CallbackEnablingBody(RawIOBase):
    """A mocked body with callback enabling/disabling"""
    def __init__(self):
        super(CallbackEnablingBody, self).__init__()
        self.enable_callback_call_count = 0
        self.disable_callback_call_count = 0

    def enable_callback(self):
        self.enable_callback_call_count += 1

    def disable_callback(self):
        self.disable_callback_call_count += 1

    def seek(self, where):
        pass

    def tell(self):
        return 0

    def read(self, amount=0):
        return b''


class TestTransferManager(StubbedClientTest):
    @skip_if_using_serial_implementation(
        'Exception is thrown once all transfers are submitted. '
        'However for the serial implementation, transfers are performed '
        'in main thread meaning all transfers will complete before the '
        'exception being thrown.'
    )
    def test_error_in_context_manager_cancels_incomplete_transfers(self):
        # The purpose of this test is to make sure if an error is raised
        # in the body of the context manager, incomplete transfers will
        # be cancelled with value of the exception wrapped by a CancelledError

        # NOTE: The fact that delete() was chosen to test this is arbitrary
        # other than it is the easiet to set up for the stubber.
        # The specific operation is not important to the purpose of this test.
        num_transfers = 100
        futures = []
        ref_exception_msg = 'arbitrary exception'

        for _ in range(num_transfers):
            self.stubber.add_response('delete_object', {})

        manager = TransferManager(
            self.client,
            TransferConfig(
                max_request_concurrency=1, max_submission_concurrency=1)
        )
        try:
            with manager:
                for i in range(num_transfers):
                    futures.append(manager.delete('mybucket', 'mykey'))
                raise ArbitraryException(ref_exception_msg)
        except ArbitraryException:
            # At least one of the submitted futures should have been
            # cancelled.
            with self.assertRaisesRegexp(FatalError, ref_exception_msg):
                for future in futures:
                    future.result()

    @skip_if_using_serial_implementation(
        'Exception is thrown once all transfers are submitted. '
        'However for the serial implementation, transfers are performed '
        'in main thread meaning all transfers will complete before the '
        'exception being thrown.'
    )
    def test_cntrl_c_in_context_manager_cancels_incomplete_transfers(self):
        # The purpose of this test is to make sure if an error is raised
        # in the body of the context manager, incomplete transfers will
        # be cancelled with value of the exception wrapped by a CancelledError

        # NOTE: The fact that delete() was chosen to test this is arbitrary
        # other than it is the easiet to set up for the stubber.
        # The specific operation is not important to the purpose of this test.
        num_transfers = 100
        futures = []

        for _ in range(num_transfers):
            self.stubber.add_response('delete_object', {})

        manager = TransferManager(
            self.client,
            TransferConfig(
                max_request_concurrency=1, max_submission_concurrency=1)
        )
        try:
            with manager:
                for i in range(num_transfers):
                    futures.append(manager.delete('mybucket', 'mykey'))
                raise KeyboardInterrupt()
        except KeyboardInterrupt:
            # At least one of the submitted futures should have been
            # cancelled.
            with self.assertRaisesRegexp(
                    CancelledError, 'KeyboardInterrupt()'):
                for future in futures:
                    future.result()

    def test_enable_disable_callbacks_only_ever_registered_once(self):
        body = CallbackEnablingBody()
        request = create_request_object({
            'method': 'PUT',
            'url': 'https://s3.amazonaws.com',
            'body': body,
            'headers': {},
            'context': {}
        })
        # Create two TransferManager's using the same client
        TransferManager(self.client)
        TransferManager(self.client)
        self.client.meta.events.emit(
            'request-created.s3', request=request, operation_name='PutObject')
        # The client should have only have the enable/disable callback
        # handlers registered once depite being used for two different
        # TransferManagers.
        self.assertEqual(
            body.enable_callback_call_count, 1,
            'The enable_callback() should have only ever been registered once')
        self.assertEqual(
            body.disable_callback_call_count, 1,
            'The disable_callback() should have only ever been registered '
            'once')

    def test_use_custom_executor_implementation(self):
        mocked_executor_cls = mock.Mock(BaseExecutor)
        transfer_manager = TransferManager(
            self.client, executor_cls=mocked_executor_cls)
        transfer_manager.delete('bucket', 'key')
        self.assertTrue(mocked_executor_cls.return_value.submit.called)

    def test_unicode_exception_in_context_manager(self):
        with self.assertRaises(ArbitraryException):
            with TransferManager(self.client):
                raise ArbitraryException(u'\u2713')
