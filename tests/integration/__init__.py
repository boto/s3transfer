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
import botocore.session

from tests import unittest
from tests import FileCreator
from tests import random_bucket_name
from s3transfer.manager import TransferManager


class BaseTransferManagerIntegTest(unittest.TestCase):
    """Tests for the high level s3transfer module."""

    @classmethod
    def setUpClass(cls):
        cls.region = 'us-west-2'
        cls.session = botocore.session.get_session()
        cls.client = cls.session.create_client('s3', cls.region)
        cls.bucket_name = random_bucket_name()
        cls.client.create_bucket(
            Bucket=cls.bucket_name,
            CreateBucketConfiguration={'LocationConstraint': cls.region})

    def setUp(self):
        self.files = FileCreator()

    def tearDown(self):
        self.files.remove_all()

    @classmethod
    def tearDownClass(cls):
        cls.client.delete_bucket(Bucket=cls.bucket_name)

    def delete_object(self, key):
        self.client.delete_object(
            Bucket=self.bucket_name,
            Key=key)

    def object_exists(self, key):
        self.client.head_object(Bucket=self.bucket_name,
                                Key=key)
        return True

    def create_transfer_manager(self, config=None):
        return TransferManager(self.client, config=config)
