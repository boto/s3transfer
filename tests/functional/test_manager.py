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
from tests import StubbedClientTest
from s3transfer.exceptions import CancelledError
from s3transfer.exceptions import FatalError
from s3transfer.manager import TransferManager
from s3transfer.manager import TransferConfig


class ArbitraryException(Exception):
    pass


class TestTransferManager(StubbedClientTest):
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
