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
from botocore.exceptions import ClientError

from tests import unittest
from tests import FileCreator
from tests import random_bucket_name
from s3transfer.manager import TransferManager


def recursive_delete(client, bucket_name):
    # Recursively deletes a bucket and all of its contents.
    objects = client.get_paginator('list_objects').paginate(
        Bucket=bucket_name)
    for key in objects.search('Contents[].Key || `[]`'):
        if key:
            client.delete_object(Bucket=bucket_name, Key=key)
    client.delete_bucket(Bucket=bucket_name)


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
        recursive_delete(cls.client, cls.bucket_name)

    def delete_object(self, key):
        self.client.delete_object(
            Bucket=self.bucket_name,
            Key=key)

    def object_exists(self, key):
        try:
            self.client.head_object(Bucket=self.bucket_name, Key=key)
            return True
        except ClientError:
            return False

    def create_transfer_manager(self, config=None):
        return TransferManager(self.client, config=config)

    def upload_file(self, filename, key):
        with open(filename, 'rb') as f:
            self.client.put_object(Bucket=self.bucket_name,
                                   Key=key,
                                   Body=f)
            self.addCleanup(self.delete_object, key)
