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

import mock
from botocore.client import Config
from botocore.exceptions import ClientError
from botocore.awsrequest import AWSRequest

from tests import BaseGeneralInterfaceTest
from tests import RecordingSubscriber
from tests import UnseekableStream
from tests import FileSizeProvider
from s3transfer.manager import TransferManager
from s3transfer.manager import TransferConfig


class BaseUploadTest(BaseGeneralInterfaceTest):
    def setUp(self):
        super(BaseUploadTest, self).setUp()
        self.config = TransferConfig(max_concurrency=1)
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

    @property
    def call_kwargs(self):
        return {
            'fileobj': self.filename,
            'bucket': self.bucket,
            'key': self.key
        }

    @property
    def invalid_extra_args(self):
        return {
            'Foo': 'bar'
        }

    @property
    def stubbed_responses(self):
        return [{'method': 'put_object', 'service_response': {}}]

    @property
    def expected_progress_callback_info(self):
        return [{'bytes_transferred': 10}]


class TestNonMultipartUpload(BaseUploadTest):
    __test__ = True

    def test_upload(self):
        self.extra_args['RequestPayer'] = 'requester'
        self.stubber.add_response(
            method='put_object',
            service_response={},
            expected_params={
                'Body': mock.ANY, 'Bucket': self.bucket,
                'Key': self.key, 'RequestPayer': 'requester'
            }
        )
        future = self.manager.upload(
            self.filename, self.bucket, self.key, self.extra_args)
        future.result()
        self.stubber.assert_no_pending_responses()
        self.assertEqual(self.sent_bodies, [self.content])

    def test_upload_open_file(self):
        self.stubber.add_response(
            method='put_object',
            service_response={},
            expected_params={
                'Body': mock.ANY, 'Bucket': self.bucket, 'Key': self.key
            }
        )
        with open(self.filename, 'rb') as f:
            future = self.manager.upload(f, self.bucket, self.key)
        future.result()
        self.stubber.assert_no_pending_responses()
        self.assertEqual(self.sent_bodies, [self.content])

    def test_upload_unseekable_stream(self):
        self.stubber.add_response(
            method='put_object',
            service_response={},
            expected_params={
                'Body': mock.ANY, 'Bucket': self.bucket, 'Key': self.key
            }
        )
        with open(self.filename, 'rb') as f:
            with UnseekableStream(f) as stream:
                future = self.manager.upload(stream, self.bucket, self.key)
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

        self.stubber.add_response(
            method='put_object',
            service_response={},
            expected_params={
                'Body': mock.ANY, 'Bucket': self.bucket,
                'Key': self.key
            }
        )
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
            max_concurrency=1, multipart_threshold=1, multipart_chunksize=4)
        self._manager = TransferManager(self.client, self.config)

    @property
    def stubbed_responses(self):
        return [
            {'method': 'create_multipart_upload',
             'service_response': {'UploadId': 'my-upload-id'}},
            {'method': 'upload_part',
             'service_response': {'ETag': 'etag-1'}},
            {'method': 'upload_part',
             'service_response': {'ETag': 'etag-2'}},
            {'method': 'upload_part',
             'service_response': {'ETag': 'etag-3'}},
            {'method': 'complete_multipart_upload', 'service_response': {}}
        ]

    @property
    def expected_progress_callback_info(self):
        return [
            {'bytes_transferred': 4},
            {'bytes_transferred': 4},
            {'bytes_transferred': 2}
        ]

    def _get_stubbed_responses_with_expected_params(self):
        upload_id = 'my-upload-id'
        stubbed_responses = self.stubbed_responses

        stubbed_responses[0]['expected_params'] = {
            'Bucket': self.bucket, 'Key': self.key
        }

        expected_parts =  [
            {'ETag': 'etag-1', 'PartNumber': 1},
            {'ETag': 'etag-2', 'PartNumber': 2},
            {'ETag': 'etag-3', 'PartNumber': 3}
        ]
        for part in expected_parts:
            part_number = part['PartNumber']
            stubbed_responses[part_number]['expected_params'] = {
                'Bucket': self.bucket, 'Body': mock.ANY,
                'Key': self.key, 'UploadId': upload_id,
                'PartNumber': part_number
            }
            

        stubbed_responses[-1]['expected_params'] = {
            'Bucket': self.bucket,
            'Key': self.key, 'UploadId': upload_id,
            'MultipartUpload': {
                'Parts': [
                    {'ETag': 'etag-1', 'PartNumber': 1},
                    {'ETag': 'etag-2', 'PartNumber': 2},
                    {'ETag': 'etag-3', 'PartNumber': 3}
                ]
            }
        }

        return stubbed_responses

    def _filter_stubbed_responses_by_operation(
            self, stubbed_responses, operation_names):
        filtered_stubbed_responses = []
        for stubbed_response in stubbed_responses:
            if stubbed_response['method'] in operation_names:
                filtered_stubbed_responses.append(stubbed_response)
        return filtered_stubbed_responses

    def test_upload(self):
        self.extra_args['RequestPayer'] = 'requester'

        stubbed_responses = self._get_stubbed_responses_with_expected_params()
        filtered_responses = self._filter_stubbed_responses_by_operation(
            stubbed_responses, ['create_multipart_upload', 'upload_part']
        )

        # Add the request payer paramter to the operations that require it:
        # CreateMultipartUpload and UploadPart
        for stubbed_response in filtered_responses:
            stubbed_response['expected_params']['RequestPayer'] = 'requester'
        for stubbed_response in stubbed_responses:
            self.stubber.add_response(**stubbed_response)

        future = self.manager.upload(
            self.filename, self.bucket, self.key, self.extra_args)
        future.result()
        self.stubber.assert_no_pending_responses()
        self.assertEqual(
            self.sent_bodies,
            [self.content[0:4], self.content[4:8], self.content[8:]])

    def test_upload_open_file(self):
        stubbed_responses = self._get_stubbed_responses_with_expected_params()
        for stubbed_response in stubbed_responses:
            self.stubber.add_response(**stubbed_response)

        # Should be able to pass in an open file handle.
        with open(self.filename, 'rb') as f:
            future = self.manager.upload(f, self.bucket, self.key)
        future.result()
        self.stubber.assert_no_pending_responses()
        self.assertEqual(
            self.sent_bodies,
            [self.content[0:4], self.content[4:8], self.content[8:]])

    def test_upload_unseekable_stream(self):
        stubbed_responses = self._get_stubbed_responses_with_expected_params()
        for stubbed_response in stubbed_responses[:-1]:
            self.stubber.add_response(**stubbed_response)

        # The unseekable stream will require one more part. The part
        # comes from when we stream the file to determine if its size
        # breaks the threshold. That streammed part then becomes the first part
        # of the multipart upload if it breaks the threshold.
        self.stubber.add_response(
            method='upload_part', service_response={'ETag': 'etag-4'},
            expected_params={
                'Bucket': self.bucket, 'Body': mock.ANY,
                'Key': self.key, 'UploadId': 'my-upload-id',
                'PartNumber': 4
            }
        )
        stubbed_responses[-1]['expected_params']['MultipartUpload'][
            'Parts'].append({'ETag': 'etag-4', 'PartNumber': 4})
        self.stubber.add_response(**stubbed_responses[-1])

        with open(self.filename, 'rb') as f:
            with UnseekableStream(f) as stream:
                future = self.manager.upload(stream, self.bucket, self.key)
        future.result()
        self.stubber.assert_no_pending_responses()

        # Note the first body is of size one because the multipart threshold
        # is of size 1.
        self.assertEqual(
            self.sent_bodies,
            [self.content[0:1], self.content[1:5], self.content[5:9],
             self.content[9:]]
        )

    def test_upload_unseekable_stream_with_provided_size(self):
        file_size_provider = FileSizeProvider(len(self.content))
        stubbed_responses = self._get_stubbed_responses_with_expected_params()
        for stubbed_response in stubbed_responses:
            self.stubber.add_response(**stubbed_response)

        with open(self.filename, 'rb') as f:
            with UnseekableStream(f) as stream:
                future = self.manager.upload(
                    stream, self.bucket, self.key,
                    subscribers=[file_size_provider]
                )
        future.result()
        self.stubber.assert_no_pending_responses()

        # The uploads should be the normal partitions as we know the size
        # ahead of time and can divvy it out appropriately.
        self.assertEqual(
            self.sent_bodies,
            [self.content[0:4], self.content[4:8], self.content[8:]])

    def test_upload_failure_invokes_abort(self):
        upload_id = 'my-upload-id'
        stubbed_responses = self._get_stubbed_responses_with_expected_params()
        # Only stub the create mulitpart and the upload part
        for stubbed_response in stubbed_responses[0:2]:
            self.stubber.add_response(**stubbed_response)    

        # With the upload part failing this should immediately initiate
        # an abort multipart with no more upload parts called.
        self.stubber.add_client_error(method='upload_part')

        self.stubber.add_response(
            method='abort_multipart_upload',
            service_response={},
            expected_params={
                'Bucket': self.bucket,
                'Key': self.key, 'UploadId': upload_id
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

        stubbed_responses = self._get_stubbed_responses_with_expected_params()
        filtered_responses = self._filter_stubbed_responses_by_operation(
            stubbed_responses, ['create_multipart_upload']
        )

        # Add the metadata paramter to the operations that require it:
        # CreateMultipartUpload
        for stubbed_response in filtered_responses:
            stubbed_response['expected_params']['Metadata'] = {'foo': 'bar'}
        for stubbed_response in stubbed_responses:
            self.stubber.add_response(**stubbed_response)

        future = self.manager.upload(
            self.filename, self.bucket, self.key, self.extra_args)
        future.result()
        self.stubber.assert_no_pending_responses()
        self.assertEqual(
            self.sent_bodies,
            [self.content[0:4], self.content[4:8], self.content[8:]])
