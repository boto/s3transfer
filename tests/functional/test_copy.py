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
from botocore.exceptions import ClientError
from botocore.stub import Stubber

from tests import BaseGeneralInterfaceTest
from tests import FileSizeProvider
from s3transfer.manager import TransferManager
from s3transfer.manager import TransferConfig


class BaseCopyTest(BaseGeneralInterfaceTest):
    def setUp(self):
        super(BaseCopyTest, self).setUp()
        self.config = TransferConfig(max_concurrency=1)
        self._manager = TransferManager(self.client, self.config)

        # Initialize some default arguments
        self.bucket = 'mybucket'
        self.key = 'mykey'
        self.copy_source = {
            'Bucket': 'mysourcebucket',
            'Key': 'mysourcekey'
        }
        self.extra_args = {}
        self.subscribers = []

        self.content = b'my content'

    @property
    def manager(self):
        return self._manager

    @property
    def method(self):
        return self.manager.copy

    @property
    def call_kwargs(self):
        return {
            'copy_source': self.copy_source,
            'bucket': self.bucket,
            'key': self.key,
        }

    @property
    def invalid_extra_args(self):
        return {
            'Foo': 'bar'
        }

    @property
    def stubbed_responses(self):
        return [
            {
                'method': 'head_object',
                'service_response': {
                    'ContentLength': len(self.content)
                }
            },
            {
                'method': 'copy_object',
                'service_response': {}
            }
        ]

    @property
    def expected_progress_callback_info(self):
        return [
            {'bytes_transferred': len(self.content)},
        ]

    def test_can_provide_file_size(self):
        stubbed_responses = self.stubbed_responses
        # Remove the HeadObject response
        stubbed_responses.pop(0)

        for stubbed_response in stubbed_responses:
            self.stubber.add_response(**stubbed_response)

        call_kwargs = self.call_kwargs
        call_kwargs['subscribers'] = [FileSizeProvider(len(self.content))]

        future = self.manager.copy(**call_kwargs)
        future.result()

        # The HeadObject should have not happened and should have been able
        # to successfully copy the file.
        self.stubber.assert_no_pending_responses()

    def test_provide_copy_source_as_dict(self):
        self.copy_source['VersionId'] = 'mysourceversionid'
        # Modify the HeadObject to make sure the copy source is parsed
        # correctly
        stubbed_responses = self.stubbed_responses
        stubbed_responses[0]['expected_params'] = {
            'Bucket': 'mysourcebucket',
            'Key': 'mysourcekey',
            'VersionId': 'mysourceversionid'
        }

        for stubbed_response in stubbed_responses:
            self.stubber.add_response(**stubbed_response)

        future = self.manager.copy(**self.call_kwargs)
        future.result()
        self.stubber.assert_no_pending_responses()

    def test_provide_copy_source_as_string(self):
        self.copy_source = (
            'mysourcebucket/mysourcekey?versionId=mysourceversionid'
        )
        # Modify the HeadObject to make sure the copy source is parsed
        # correctly
        stubbed_responses = self.stubbed_responses
        stubbed_responses[0]['expected_params'] = {
            'Bucket': 'mysourcebucket',
            'Key': 'mysourcekey',
            'VersionId': 'mysourceversionid'
        }

        for stubbed_response in stubbed_responses:
            self.stubber.add_response(**stubbed_response)

        future = self.manager.copy(**self.call_kwargs)
        future.result()
        self.stubber.assert_no_pending_responses()

    def test_invalid_copy_source(self):
        self.copy_source = ['bucket', 'key']
        with self.assertRaises(TypeError):
            self.manager.copy(**self.call_kwargs)

    def test_provide_copy_source_client(self):
        stubbed_responses = self.stubbed_responses
        source_client = self.session.create_client(
            's3', 'eu-central-1', aws_access_key_id='foo',
            aws_secret_access_key='bar')
        source_stubber = Stubber(source_client)
        source_stubber.activate()
        self.addCleanup(source_stubber.deactivate)

        # Add the head object response to stubber for the source
        # client.
        source_stubber.add_response(**stubbed_responses[0])

        # Add the rest of the responses for the main stubber
        for stubbed_response in stubbed_responses[1:]:
            self.stubber.add_response(**stubbed_response)

        call_kwargs = self.call_kwargs
        call_kwargs['source_client'] = source_client
        future = self.manager.copy(**call_kwargs)
        future.result()

        # Make sure that all of the responses were properly
        # used for both clients.
        source_stubber.assert_no_pending_responses()
        self.stubber.assert_no_pending_responses()


class TestNonMultipartCopy(BaseCopyTest):
    __test__ = True

    def _get_stubbed_responses_with_expected_params(self):
        stubbed_responses = self.stubbed_responses

        # Add expected parameters to the head object
        stubbed_responses[0]['expected_params'] = {
            'Bucket': 'mysourcebucket',
            'Key': 'mysourcekey',
        }

        # Add expected parameters to the copy object
        stubbed_responses[1]['expected_params'] = {
            'Bucket': self.bucket,
            'Key': self.key,
            'CopySource': self.copy_source,
        }
        return stubbed_responses

    def test_copy(self):
        stubbed_responses = self._get_stubbed_responses_with_expected_params()
        for stubbed_response in stubbed_responses:
            self.stubber.add_response(**stubbed_response)

        future = self.manager.copy(**self.call_kwargs)
        future.result()
        self.stubber.assert_no_pending_responses()

    def test_copy_with_extra_args(self):
        self.extra_args['MetadataDirective'] = 'REPLACE'
        stubbed_responses = self._get_stubbed_responses_with_expected_params()

        # The copy object should have the MetadataDirective argument
        # passed to it
        stubbed_responses[1]['expected_params'][
            'MetadataDirective'] = 'REPLACE'

        for stubbed_response in stubbed_responses:
            self.stubber.add_response(**stubbed_response)

        call_kwargs = self.call_kwargs
        call_kwargs['extra_args'] = self.extra_args
        future = self.manager.copy(**call_kwargs)
        future.result()
        self.stubber.assert_no_pending_responses()

    def test_copy_maps_extra_args_to_head_object(self):
        self.extra_args['CopySourceSSECustomerAlgorithm'] = 'AES256'
        stubbed_responses = self._get_stubbed_responses_with_expected_params()

        # The CopySourceSSECustomerAlgorithm needs to get mapped to
        # SSECustomerAlgorithm for HeadObject
        stubbed_responses[0]['expected_params'][
            'SSECustomerAlgorithm'] = 'AES256'

        # However, it needs to remain the same for UploadPartCopy.
        stubbed_responses[1]['expected_params'][
            'CopySourceSSECustomerAlgorithm'] = 'AES256'

        for stubbed_response in stubbed_responses:
            self.stubber.add_response(**stubbed_response)

        call_kwargs = self.call_kwargs
        call_kwargs['extra_args'] = self.extra_args
        future = self.manager.copy(**call_kwargs)
        future.result()
        self.stubber.assert_no_pending_responses()


class TestMultipartCopy(BaseCopyTest):
    __test__ = True

    def setUp(self):
        super(TestMultipartCopy, self).setUp()
        self.config = TransferConfig(
            max_concurrency=1, multipart_threshold=1, multipart_chunksize=4)
        self._manager = TransferManager(self.client, self.config)

    @property
    def stubbed_responses(self):
        return [
            {
                'method': 'head_object',
                'service_response': {
                    'ContentLength': len(self.content)
                }
            },
            {
                'method': 'create_multipart_upload',
                'service_response': {
                    'UploadId': 'my-upload-id'
                }
            },
            {
                'method': 'upload_part_copy',
                'service_response': {
                    'CopyPartResult': {
                        'ETag': 'etag-1'
                    }
                }
            },
            {
                'method': 'upload_part_copy',
                'service_response': {
                    'CopyPartResult': {
                        'ETag': 'etag-2'
                    }
                }
            },
            {
                'method': 'upload_part_copy',
                'service_response': {
                    'CopyPartResult': {
                        'ETag': 'etag-3'
                    }
                }
            },
            {
                'method': 'complete_multipart_upload',
                'service_response': {}
            }
        ]

    @property
    def expected_progress_callback_info(self):
        # Note that last read is from the empty sentinel indicating
        # that the stream is done.
        return [
            {'bytes_transferred': 4},
            {'bytes_transferred': 4},
            {'bytes_transferred': 2}
        ]

    def _get_stubbed_responses_with_expected_params(self):
        upload_id = 'my-upload-id'
        stubbed_responses = self.stubbed_responses

        # Add expected parameters to the head object
        stubbed_responses[0]['expected_params'] = {
            'Bucket': 'mysourcebucket',
            'Key': 'mysourcekey',
        }

        # Add expected parameters for the create multipart
        stubbed_responses[1]['expected_params'] = {
            'Bucket': self.bucket,
            'Key': self.key,
        }

        # Add expected parameters to the copy part
        ranges = ['bytes=0-3', 'bytes=4-7', 'bytes=8-9']
        for i, range_val in enumerate(ranges):
            index = 2 + i
            stubbed_responses[index]['expected_params'] = {
                'Bucket': self.bucket,
                'Key': self.key,
                'CopySource': self.copy_source,
                'UploadId': upload_id,
                'PartNumber': i + 1,
                'CopySourceRange': range_val
            }

        # Add expected parameters for the complete multipart
        stubbed_responses[5]['expected_params'] = {
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

    def test_copy(self):
        stubbed_responses = self._get_stubbed_responses_with_expected_params()
        for stubbed_response in stubbed_responses:
            self.stubber.add_response(**stubbed_response)

        future = self.manager.copy(**self.call_kwargs)
        future.result()
        self.stubber.assert_no_pending_responses()

    def test_copy_with_extra_args(self):
        self.extra_args['RequestPayer'] = 'requester'

        stubbed_responses = self._get_stubbed_responses_with_expected_params()
        filtered_responses = self._filter_stubbed_responses_by_operation(
            stubbed_responses,
            ['head_object', 'create_multipart_upload', 'upload_part_copy']
        )

        # Add the requester payer arg to HeadObject, CreateMultipartUpload
        # and UploadPartCopy
        for stubbed_response in filtered_responses:
            stubbed_response['expected_params']['RequestPayer'] = 'requester'
        for stubbed_response in stubbed_responses:
            self.stubber.add_response(**stubbed_response)

        call_kwargs = self.call_kwargs
        call_kwargs['extra_args'] = self.extra_args
        future = self.manager.copy(**call_kwargs)
        future.result()
        self.stubber.assert_no_pending_responses()

    def test_copy_blacklists_args_to_create_multipart(self):
        # This argument can never be used for multipart uploads
        self.extra_args['MetadataDirective'] = 'COPY'

        stubbed_responses = self._get_stubbed_responses_with_expected_params()
        for stubbed_response in stubbed_responses:
            self.stubber.add_response(**stubbed_response)

        call_kwargs = self.call_kwargs
        call_kwargs['extra_args'] = self.extra_args
        future = self.manager.copy(**call_kwargs)
        future.result()
        self.stubber.assert_no_pending_responses()

    def test_copy_args_to_only_create_multipart(self):
        self.extra_args['ACL'] = 'private'

        stubbed_responses = self._get_stubbed_responses_with_expected_params()

        # ACL's should only be used for the CreateMultipartUpload
        create_multipart_resp = self._filter_stubbed_responses_by_operation(
            stubbed_responses, ['create_multipart_upload'])[0]
        create_multipart_resp['expected_params']['ACL'] = 'private'

        for stubbed_response in stubbed_responses:
            self.stubber.add_response(**stubbed_response)

        call_kwargs = self.call_kwargs
        call_kwargs['extra_args'] = self.extra_args
        future = self.manager.copy(**call_kwargs)
        future.result()
        self.stubber.assert_no_pending_responses()

    def test_copy_passes_args_to_create_multipart_and_upload_part(self):
        # This will only be used for the complete multipart upload
        # and upload part.
        self.extra_args['SSECustomerAlgorithm'] = 'AES256'

        stubbed_responses = self._get_stubbed_responses_with_expected_params()
        filtered_responses = self._filter_stubbed_responses_by_operation(
            stubbed_responses,
            ['create_multipart_upload', 'upload_part_copy']
        )

        for stubbed_response in filtered_responses:
            stubbed_response['expected_params'][
                'SSECustomerAlgorithm'] = 'AES256'
        for stubbed_response in stubbed_responses:
            self.stubber.add_response(**stubbed_response)

        call_kwargs = self.call_kwargs
        call_kwargs['extra_args'] = self.extra_args
        future = self.manager.copy(**call_kwargs)
        future.result()
        self.stubber.assert_no_pending_responses()

    def test_copy_maps_extra_args_to_head_object(self):
        self.extra_args['CopySourceSSECustomerAlgorithm'] = 'AES256'

        stubbed_responses = self._get_stubbed_responses_with_expected_params()

        # The CopySourceSSECustomerAlgorithm needs to get mapped to
        # SSECustomerAlgorithm for HeadObject
        head_object_responses = self._filter_stubbed_responses_by_operation(
            stubbed_responses, ['head_object'])
        head_object_responses[0]['expected_params'][
            'SSECustomerAlgorithm'] = 'AES256'

        # However, it needs to remain the same for UploadPartCopy.
        part_copy_responses = self._filter_stubbed_responses_by_operation(
            stubbed_responses, ['upload_part_copy'])
        for response in part_copy_responses:
            response['expected_params'][
                'CopySourceSSECustomerAlgorithm'] = 'AES256'

        for stubbed_response in stubbed_responses:
            self.stubber.add_response(**stubbed_response)

        call_kwargs = self.call_kwargs
        call_kwargs['extra_args'] = self.extra_args
        future = self.manager.copy(**call_kwargs)
        future.result()
        self.stubber.assert_no_pending_responses()

    def test_abort_on_failure(self):
        # First add the head object and create multipart upload
        for stubbed_response in self.stubbed_responses[0:2]:
            self.stubber.add_response(**stubbed_response)

        # Cause an error on upload_part_copy
        self.stubber.add_client_error('upload_part_copy', 'ArbitraryFailure')

        # Add the abort multipart to ensure it gets cleaned up on failure
        self.stubber.add_response(
            'abort_multipart_upload',
            service_response={},
            expected_params={
                'Bucket': self.bucket,
                'Key': self.key,
                'UploadId': 'my-upload-id'
            }
        )

        future = self.manager.copy(**self.call_kwargs)
        with self.assertRaisesRegexp(ClientError, 'ArbitraryFailure'):
            future.result()
        self.stubber.assert_no_pending_responses()
