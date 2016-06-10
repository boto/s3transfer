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

from concurrent.futures import ThreadPoolExecutor

from tests import unittest
from s3transfer.futures import TransferCoordinator
from s3transfer.manager import TransferCoordinatorCanceler


class TestTransferCoordinatorCanceler(unittest.TestCase):
    def setUp(self):
        self.canceler = TransferCoordinatorCanceler()

    def sleep_then_announce_done(self, transfer_coordinator, sleep_time):
        time.sleep(sleep_time)
        transfer_coordinator.set_result('done')
        transfer_coordinator.announce_done()

    def assert_coordinator_is_not_cancelled(self, transfer_coordinator):
        self.assertNotEqual(transfer_coordinator.status, 'cancelled')

    def assert_coordinator_is_cancelled(self, transfer_coordinator):
        self.assertEqual(transfer_coordinator.status, 'cancelled')

    def test_add_transfer_coordinator(self):
        transfer_coordinator = TransferCoordinator()
        # Add the transfer coordinator
        self.canceler.add_transfer_coordinator(transfer_coordinator)
        # Cancel with the canceler
        self.canceler.cancel()
        # Check that coordinator got canceled
        self.assert_coordinator_is_cancelled(transfer_coordinator)

    def test_remove_transfer_coordinator(self):
        transfer_coordinator = TransferCoordinator()
        # Add the coordinator
        self.canceler.add_transfer_coordinator(transfer_coordinator)
        # Now remove the coordinator
        self.canceler.remove_transfer_coordinator(transfer_coordinator)
        self.canceler.cancel()
        # The coordinator should not have been canceled.
        self.assert_coordinator_is_not_cancelled(transfer_coordinator)

    def test_wait_for_done_transfer_coordinators(self):
        # Create a coordinator and add it to the canceler
        transfer_coordinator = TransferCoordinator()
        self.canceler.add_transfer_coordinator(transfer_coordinator)

        sleep_time = 0.02
        with ThreadPoolExecutor(max_workers=1) as executor:
            # In a seperate thread sleep and then set the transfer coordinator
            # to done after sleeping.
            start_time = time.time()
            executor.submit(
                self.sleep_then_announce_done, transfer_coordinator,
                sleep_time)
            # Now call wait to wait for the transfer coordinator to be done.
            self.canceler.wait(sleep_time)
            end_time = time.time()
            wait_time = end_time - start_time
        # The time waited should not be less than the time it took to sleep in
        # the seperate thread because the wait ending should be dependent on
        # the sleeping thread announcing that the transfer coordinator is done.
        self.assertTrue(sleep_time <= wait_time)
