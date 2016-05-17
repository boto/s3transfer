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
import os
import tempfile
import shutil

import mock

from tests import BaseTaskTest
from tests import BaseTaskSubmitterTest
from tests import FileSizeProvider
from s3transfer.upload import UploadTaskSubmitter
from s3transfer.upload import PutObjectTask
from s3transfer.upload import CreateMultipartUploadTask
from s3transfer.upload import UploadPartTask
from s3transfer.upload import CompleteMultipartUploadTask
from s3transfer.utils import CallArgs
from s3transfer.utils import OSUtils


class OSUtilsExceptionOnFileSize(OSUtils):
    def get_file_size(self, filename):
        raise AssertionError(
            "The file %s should not have been stated" % filename)


class BaseUploadTaskTest(BaseTaskTest):
    def setUp(self):
        super(BaseUploadTaskTest, self).setUp()
        self.bucket = 'mybucket'
        self.key = 'foo'
        self.osutil = OSUtils()

        self.tempdir = tempfile.mkdtemp()
        self.filename = os.path.join(self.tempdir, 'myfile')
        self.content = b'my content'

        with open(self.filename, 'wb') as f:
            f.write(self.content)

        # A list to keep track of all of the bodies sent over the wire
        # and their order.
        self.sent_bodies = []
        self.client.meta.events.register(
            'before-parameter-build.s3.*', self.collect_body)

    def tearDown(self):
        super(BaseUploadTaskTest, self).tearDown()
        shutil.rmtree(self.tempdir)

    def collect_body(self, params, **kwargs):
        if 'Body' in params:
            self.sent_bodies.append(params['Body'].read())


class TestUploadTaskSubmitter(BaseTaskSubmitterTest):
    def setUp(self):
        super(TestUploadTaskSubmitter, self).setUp()
        self.submitter = UploadTaskSubmitter(
            client=self.client, config=self.config,
            osutil=self.osutil, executor=self.executor
        )
        self.tempdir = tempfile.mkdtemp()
        self.filename = os.path.join(self.tempdir, 'myfile')
        self.content = b'my content'

        with open(self.filename, 'wb') as f:
            f.write(self.content)

        self.bucket = 'mybucket'
        self.key = 'mykey'
        self.extra_args = {}
        self.subscribers = []

        # A list to keep track of all of the bodies sent over the wire
        # and their order.
        self.sent_bodies = []
        self.client.meta.events.register(
            'before-parameter-build.s3.*', self.collect_body)

    def tearDown(self):
        super(TestUploadTaskSubmitter, self).tearDown()
        shutil.rmtree(self.tempdir)

    def collect_body(self, params, **kwargs):
        if 'Body' in params:
            self.sent_bodies.append(params['Body'].read())

    def get_call_args(self, **kwargs):
        default_call_args = {
            'fileobj': self.filename, 'bucket': self.bucket,
            'key': self.key, 'extra_args': self.extra_args,
            'subscribers': self.subscribers
        }
        default_call_args.update(kwargs)
        return CallArgs(**default_call_args)

    def test_provide_file_size_on_put(self):
        self.subscribers.append(FileSizeProvider(len(self.content)))
        call_args = self.get_call_args()
        self.stubber.add_response(
            method='put_object',
            service_response={},
            expected_params={
                'Body': mock.ANY, 'Bucket': self.bucket,
                'Key': self.key
            }
        )

        # With this submitter, it will fail to stat the file if a transfer
        # size is not provided.
        self.submitter = UploadTaskSubmitter(
            client=self.client, config=self.config,
            osutil=OSUtilsExceptionOnFileSize(), executor=self.executor
        )

        future = self.submitter(call_args)
        future.result()
        self.stubber.assert_no_pending_responses()
        self.assertEqual(self.sent_bodies, [self.content])


class TestPutObjectTask(BaseUploadTaskTest):
    def test_main(self):
        extra_args = {'Metadata': {'foo': 'bar'}}
        task = self.get_task(
            PutObjectTask,
            main_kwargs={
                'client': self.client,
                'fileobj': self.filename,
                'bucket': self.bucket,
                'key': self.key,
                'extra_args': extra_args,
                'osutil': self.osutil,
                'size': len(self.content),
                'progress_callbacks': []
            }
        )
        self.stubber.add_response(
            method='put_object',
            service_response={},
            expected_params={
                'Body': mock.ANY, 'Bucket': self.bucket, 'Key': self.key,
                'Metadata': {'foo': 'bar'}
            }
        )
        task()
        self.stubber.assert_no_pending_responses()
        self.assertEqual(self.sent_bodies, [self.content])


class TestCreateMultipartUploadTask(BaseUploadTaskTest):
    def test_main(self):
        upload_id = 'foo'
        extra_args = {'Metadata': {'foo': 'bar'}}
        response = {'UploadId': upload_id}
        task = self.get_task(
            CreateMultipartUploadTask,
            main_kwargs={
                'client': self.client,
                'bucket': self.bucket,
                'key': self.key,
                'extra_args': extra_args
            }
        )
        self.stubber.add_response(
            method='create_multipart_upload',
            service_response=response,
            expected_params={
              'Bucket': self.bucket, 'Key': self.key,
              'Metadata': {'foo': 'bar'}
            }
        )
        result_id = task()
        self.stubber.assert_no_pending_responses()
        # Ensure the upload id returned is correct
        self.assertEqual(upload_id, result_id)

        # Make sure that the abort was added as a cleanup failure
        self.assertEqual(len(self.transfer_coordinator.failure_cleanups), 1)

        # Make sure if it is called, it will abort correctly
        self.stubber.add_response(
            method='abort_multipart_upload',
            service_response={},
            expected_params={
                'Bucket': self.bucket,
                'Key': self.key,
                'UploadId': upload_id
            }
        )
        self.transfer_coordinator.failure_cleanups[0]()
        self.stubber.assert_no_pending_responses()


class TestUploadPartTask(BaseUploadTaskTest):
    def test_main(self):
        extra_args = {'RequestPayer': 'requester'}
        upload_id = 'my-id'
        part_number = 1
        etag = 'foo'
        task = self.get_task(
            UploadPartTask,
            main_kwargs={
                'client': self.client,
                'fileobj': self.filename,
                'bucket': self.bucket,
                'key': self.key,
                'upload_id': upload_id,
                'part_number': part_number,
                'extra_args': extra_args,
                'osutil': self.osutil,
                'part_size': len(self.content),
                'progress_callbacks': []
            }
        )
        self.stubber.add_response(
            method='upload_part',
            service_response={'ETag': etag},
            expected_params={
                'Body': mock.ANY, 'Bucket': self.bucket, 'Key': self.key,
                'UploadId': upload_id, 'PartNumber': part_number,
                'RequestPayer': 'requester'
            }
        )
        rval = task()
        self.stubber.assert_no_pending_responses()
        self.assertEqual(rval, {'ETag': etag, 'PartNumber': part_number})
        self.assertEqual(self.sent_bodies, [self.content])


class TestCompleteMultipartUploadTask(BaseUploadTaskTest):
    def test_main(self):
        upload_id = 'my-id'
        parts = [{'ETag': 'etag', 'PartNumber': 1}]
        task = self.get_task(
            CompleteMultipartUploadTask,
            main_kwargs={
                'client': self.client,
                'bucket': self.bucket,
                'key': self.key,
                'upload_id': upload_id,
                'parts': parts
            }
        )
        self.stubber.add_response(
            method='complete_multipart_upload',
            service_response={},
            expected_params={
                'Bucket': self.bucket, 'Key': self.key,
                'UploadId': upload_id,
                'MultipartUpload': {
                    'Parts': parts
                }
            }
        )
        task()
        self.stubber.assert_no_pending_responses()
