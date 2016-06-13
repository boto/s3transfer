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
import os
import tempfile
import shutil

from botocore.client import Config
from botocore.exceptions import ClientError
from botocore.awsrequest import AWSRequest
from botocore.stub import ANY

from tests import BaseGeneralInterfaceTest
from tests import RecordingSubscriber
from s3transfer.manager import TransferManager
from s3transfer.manager import TransferConfig


class BaseUploadTest(BaseGeneralInterfaceTest):
    def setUp(self):
        super(BaseUploadTest, self).setUp()
        self.config = TransferConfig(max_request_concurrency=1)
        self._manager = TransferManager(self.client, self.config)

        # Create a temporary directory with files to read from
        self.tempdir = tempfile.mkdtemp()
        self.filename = os.path.join(self.tempdir, 'myfile')
        self.content = b'my content'

        with open(self.filename, 'wb') as f:
            f.write(self.content)

        # Initialize some default arguments
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
        super(BaseUploadTest, self).tearDown()
        shutil.rmtree(self.tempdir)

    def collect_body(self, params, model, **kwargs):
        # A handler to simulate the reading of the body including the
        # request-created event that signals to simulate the progress
        # callbacks
        if 'Body' in params:
            # TODO: This is not ideal. Need to figure out a better idea of
            # simulating reading of the request across the wire to trigger
            # progress callbacks
            request = AWSRequest(
                method='PUT', url='https://s3.amazonaws.com',
                data=params['Body']
            )
            self.client.meta.events.emit(
                'request-created.s3.%s' % model.name,
                request=request, operation_name=model.name
            )
            self.sent_bodies.append(params['Body'].read())

    @property
    def manager(self):
        return self._manager

    @property
    def method(self):
        return self.manager.upload

    def create_call_kwargs(self):
        return {
            'fileobj': self.filename,
            'bucket': self.bucket,
            'key': self.key
        }

    def create_invalid_extra_args(self):
        return {
            'Foo': 'bar'
        }

    def create_stubbed_responses(self):
        return [{'method': 'put_object', 'service_response': {}}]

    def create_expected_progress_callback_info(self):
        return [{'bytes_transferred': 10}]

    def add_successful_upload_responses(
            self, expected_upload_params=None, expected_create_mpu_params=None,
            expected_complete_mpu_params=None):

        stubbed_responses = self.create_stubbed_responses()

        # If the length of copy responses is greater than one then it is
        # a multipart upload.
        upload_responses = stubbed_responses[0:1]
        if len(stubbed_responses) > 1:
            upload_responses = stubbed_responses[1:-1]

        # Add the expected create multipart upload params.
        if expected_create_mpu_params:
            stubbed_responses[0][
                'expected_params'] = expected_create_mpu_params

        # Add any expected copy parameters.
        if expected_upload_params:
            for i, upload_response in enumerate(upload_responses):
                if isinstance(expected_upload_params, list):
                    upload_response[
                        'expected_params'] = expected_upload_params[i]
                else:
                    upload_response['expected_params'] = expected_upload_params

        # Add the expected complete multipart upload params.
        if expected_complete_mpu_params:
            stubbed_responses[-1][
                'expected_params'] = expected_complete_mpu_params

        # Add the responses to the stubber.
        for stubbed_response in stubbed_responses:
            self.stubber.add_response(**stubbed_response)


class TestNonMultipartUpload(BaseUploadTest):
    __test__ = True

    def test_upload(self):
        self.extra_args['RequestPayer'] = 'requester'
        expected_params = {
            'Body': ANY, 'Bucket': self.bucket,
            'Key': self.key, 'RequestPayer': 'requester'
        }
        self.add_successful_upload_responses(
            expected_upload_params=expected_params)
        future = self.manager.upload(
            self.filename, self.bucket, self.key, self.extra_args)
        future.result()
        self.stubber.assert_no_pending_responses()
        self.assertEqual(self.sent_bodies, [self.content])

    def test_upload_for_fileobj(self):
        expected_params = {
            'Body': ANY, 'Bucket': self.bucket,
            'Key': self.key
        }
        self.add_successful_upload_responses(
            expected_upload_params=expected_params)
        with open(self.filename, 'rb') as f:
            future = self.manager.upload(
                f, self.bucket, self.key, self.extra_args)
            future.result()
        self.stubber.assert_no_pending_responses()
        self.assertEqual(self.sent_bodies, [self.content])

    def test_sigv4_progress_callbacks_invoked_once(self):
        # Reset the client and manager to use sigv4
        self.reset_stubber_with_new_client(
            {'config': Config(signature_version='s3v4')})
        self.client.meta.events.register(
            'before-parameter-build.s3.*', self.collect_body)
        self._manager = TransferManager(self.client, self.config)

        # Add the expected params
        expected_params = {
            'Body': ANY, 'Bucket': self.bucket,
            'Key': self.key
        }
        self.add_successful_upload_responses(
            expected_upload_params=expected_params)

        subscriber = RecordingSubscriber()
        future = self.manager.upload(
            self.filename, self.bucket, self.key, subscribers=[subscriber])
        future.result()
        self.stubber.assert_no_pending_responses()

        # The amount of bytes seen should be the same as the file size
        self.assertEqual(subscriber.calculate_bytes_seen(), len(self.content))


class TestMultipartUpload(BaseUploadTest):
    __test__ = True

    def setUp(self):
        super(TestMultipartUpload, self).setUp()
        self.config = TransferConfig(
            max_request_concurrency=1, multipart_threshold=1,
            multipart_chunksize=4)
        self._manager = TransferManager(self.client, self.config)
        self.multipart_id = 'my-upload-id'

    def create_stubbed_responses(self):
        return [
            {'method': 'create_multipart_upload',
             'service_response': {'UploadId': self.multipart_id}},
            {'method': 'upload_part',
             'service_response': {'ETag': 'etag-1'}},
            {'method': 'upload_part',
             'service_response': {'ETag': 'etag-2'}},
            {'method': 'upload_part',
             'service_response': {'ETag': 'etag-3'}},
            {'method': 'complete_multipart_upload', 'service_response': {}}
        ]

    def create_expected_progress_callback_info(self):
        return [
            {'bytes_transferred': 4},
            {'bytes_transferred': 4},
            {'bytes_transferred': 2}
        ]

    def _get_expected_params(self):
        # Add expected parameters for the create multipart
        expected_create_mpu_params = {
            'Bucket': self.bucket,
            'Key': self.key,
        }

        expected_upload_params = []
        # Add expected parameters to the copy part
        num_parts = 3
        for i in range(num_parts):
            expected_upload_params.append(
                {
                    'Bucket': self.bucket,
                    'Key': self.key,
                    'UploadId': self.multipart_id,
                    'Body': ANY,
                    'PartNumber': i + 1,
                }
            )

        # Add expected parameters for the complete multipart
        expected_complete_mpu_params = {
            'Bucket': self.bucket,
            'Key': self.key, 'UploadId': self.multipart_id,
            'MultipartUpload': {
                'Parts': [
                    {'ETag': 'etag-1', 'PartNumber': 1},
                    {'ETag': 'etag-2', 'PartNumber': 2},
                    {'ETag': 'etag-3', 'PartNumber': 3}
                ]
            }
        }

        return {
            'expected_create_mpu_params': expected_create_mpu_params,
            'expected_upload_params': expected_upload_params,
            'expected_complete_mpu_params': expected_complete_mpu_params,
        }

    def _add_params_to_expected_params(
            self, add_upload_kwargs, operation_types, new_params):
        # Helper method for adding any additional params to the typical
        # default expect parameters.
        expected_params_to_update = []
        for operation_type in operation_types:
            add_upload_kwargs_key = 'expected_' + operation_type + '_params'
            expected_params = add_upload_kwargs[add_upload_kwargs_key]
            if isinstance(expected_params, list):
                expected_params_to_update.extend(expected_params)
            else:
                expected_params_to_update.append(expected_params)

        for expected_params in expected_params_to_update:
            expected_params.update(new_params)

    def test_upload(self):
        # Set the threshold for multipart upload to something small
        # to trigger multipart.
        self.extra_args['RequestPayer'] = 'requester'

        # Add requester pays to expected upload args
        add_upload_kwargs = self._get_expected_params()
        self._add_params_to_expected_params(
            add_upload_kwargs, ['create_mpu', 'upload'], self.extra_args)
        self.add_successful_upload_responses(**add_upload_kwargs)

        future = self.manager.upload(
            self.filename, self.bucket, self.key, self.extra_args)
        future.result()
        self.stubber.assert_no_pending_responses()

    def test_upload_for_fileobj(self):
        add_upload_kwargs = self._get_expected_params()
        self.add_successful_upload_responses(**add_upload_kwargs)
        with open(self.filename, 'rb') as f:
            future = self.manager.upload(
                f, self.bucket, self.key, self.extra_args)
            future.result()
        self.stubber.assert_no_pending_responses()
        self.assertEqual(
            self.sent_bodies,
            [self.content[0:4], self.content[4:8], self.content[8:]])

    def test_upload_failure_invokes_abort(self):
        self.stubber.add_response(
            method='create_multipart_upload',
            service_response={
                'UploadId': self.multipart_id
            },
            expected_params={
                'Bucket': self.bucket,
                'Key': self.key
            }
        )
        self.stubber.add_response(
            method='upload_part',
            service_response={
                'ETag': 'etag-1'
            },
            expected_params={
                'Bucket': self.bucket, 'Body': ANY,
                'Key': self.key, 'UploadId': self.multipart_id,
                'PartNumber': 1
            }
        )
        # With the upload part failing this should immediately initiate
        # an abort multipart with no more upload parts called.
        self.stubber.add_client_error(method='upload_part')

        self.stubber.add_response(
            method='abort_multipart_upload',
            service_response={},
            expected_params={
                'Bucket': self.bucket,
                'Key': self.key, 'UploadId': self.multipart_id
            }
        )

        future = self.manager.upload(self.filename, self.bucket, self.key)
        # The exception should get propogated to the future and not be
        # a cancelled error or something.
        with self.assertRaises(ClientError):
            future.result()
        self.stubber.assert_no_pending_responses()

    def test_upload_passes_select_extra_args(self):
        self.extra_args['Metadata'] = {'foo': 'bar'}

        add_upload_kwargs = self._get_expected_params()
        # Add metadata to expected create multipart upload call
        self._add_params_to_expected_params(
            add_upload_kwargs, ['create_mpu'], self.extra_args)
        self.add_successful_upload_responses(**add_upload_kwargs)

        future = self.manager.upload(
            self.filename, self.bucket, self.key, self.extra_args)
        future.result()
        self.stubber.assert_no_pending_responses()
