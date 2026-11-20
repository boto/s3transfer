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
from unittest import TestCase, mock

from s3transfer.exceptions import CancelledError
from s3transfer.futures import TransferCoordinator
from s3transfer.manager import TransferManager


class TestTransferManager(TestCase):
    def test_shutdown_cancels_transfer_coordinator(self):
        client = mock.Mock()

        cancel_msg = 'Test message'

        transfer_manager = TransferManager(client)
        transfer_coordinator = TransferCoordinator()
        coordinator_controller = transfer_manager.coordinator_controller
        coordinator_controller.add_transfer_coordinator(transfer_coordinator)
        transfer_manager.shutdown(True, cancel_msg)

        self.assertIsInstance(transfer_coordinator.exception, CancelledError)
        self.assertEqual(str(transfer_coordinator.exception), cancel_msg)
