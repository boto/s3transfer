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
import glob

from botocore.exceptions import ClientError

from tests import StreamWithError
from tests import FileSizeProvider
from tests import BaseGeneralInterfaceTest
from s3transfer.compat import six
from s3transfer.compat import SOCKET_ERROR
from s3transfer.exceptions import RetriesExceededError
from s3transfer.manager import TransferManager
from s3transfer.manager import TransferConfig


class BaseDownloadTest(BaseGeneralInterfaceTest):
    def setUp(self):
        super(BaseDownloadTest, self).setUp()
        self.config = TransferConfig(max_concurrency=1)
        self._manager = TransferManager(self.client, self.config)

        # Create a temporary directory to write to
        self.tempdir = tempfile.mkdtemp()
        self.filename = os.path.join(self.tempdir, 'myfile')

        # Initialize some default arguments
        self.bucket = 'mybucket'
        self.key = 'mykey'
        self.extra_args = {}
        self.subscribers = []

        # Create a stream to read from
        self.content = b'my content'
        self.stream = six.BytesIO(self.content)

    def tearDown(self):
        super(BaseDownloadTest, self).tearDown()
        shutil.rmtree(self.tempdir)

    @property
    def manager(self):
        return self._manager

    @property
    def method(self):
        return self.manager.download

    @property
    def call_kwargs(self):
        return {
            'bucket': self.bucket,
            'key': self.key,
            'fileobj': self.filename
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
                'method': 'get_object',
                'service_response': {
                    'Body': self.stream
                }
            }
        ]

    @property
    def expected_progress_callback_info(self):
        # Note that last read is from the empty sentinel indicating
        # that the stream is done.
        return [
            {'bytes_transferred': 10},
            {'bytes_transferred': 0}
        ]

    def test_download_temporary_file_does_not_exist(self):
        for stubbed_response in self.stubbed_responses:
            self.stubber.add_response(**stubbed_response)

        future = self.manager.download(**self.call_kwargs)
        future.result()
        # Make sure the file exists
        self.assertTrue(os.path.exists(self.filename))
        # Make sure the random temporary file does not exist
        possible_matches = glob.glob('%s*' % self.filename + os.extsep)
        self.assertEqual(possible_matches, [])

    def test_download_cleanup_on_failure(self):
        # Add the stubbed HeadObject response
        self.stubber.add_response(**self.stubbed_responses[0])

        # Throw an error on the download
        self.stubber.add_client_error('get_object')

        future = self.manager.download(**self.call_kwargs)

        with self.assertRaises(ClientError):
            future.result()
        # Make sure the actual file and the temporary do not exist
        # by globbing for the file and any of its extensions
        possible_matches = glob.glob('%s*' % self.filename)
        self.assertEqual(possible_matches, [])

    def test_download_with_nonexistent_directory(self):
        for stubbed_response in self.stubbed_responses:
            self.stubber.add_response(**stubbed_response)

        call_kwargs = self.call_kwargs
        call_kwargs['fileobj'] = os.path.join(
            self.tempdir, 'missing-directory', 'myfile')
        future = self.manager.download(**call_kwargs)

        with self.assertRaises(IOError):
            future.result()

    def test_retries_and_succeeds(self):
        stubbed_responses = self.stubbed_responses
        # Insert a response that will trigger a retry.
        stubbed_responses.insert(
            1,
            {
                'method': 'get_object',
                'service_response': {
                    'Body': StreamWithError(SOCKET_ERROR)
                }
            }
        )

        for stubbed_response in stubbed_responses:
            self.stubber.add_response(**stubbed_response)

        future = self.manager.download(**self.call_kwargs)
        future.result()

        # The retry should have been consumed and the process should have
        # continued using the successful responses.
        self.stubber.assert_no_pending_responses()
        with open(self.filename, 'rb') as f:
            self.assertEqual(self.content, f.read())

    def test_retry_failure(self):
        stubbed_responses = []
        # Add the HeadObject to the stubbed responses
        stubbed_responses.append(self.stubbed_responses[0])

        # Make responses that trigger three retries
        max_retries = 3
        self.config.num_download_attempts = max_retries
        self._manager = TransferManager(self.client, self.config)
        for _ in range(max_retries):
            stubbed_responses.append(
                {
                    'method': 'get_object',
                    'service_response': {
                        'Body': StreamWithError(SOCKET_ERROR)
                    }
                }
            )

        for stubbed_response in stubbed_responses:
            self.stubber.add_response(**stubbed_response)

        future = self.manager.download(**self.call_kwargs)

        # A retry exceeded error should have happened.
        with self.assertRaises(RetriesExceededError):
            future.result()

        # All of the retries should have been used up.
        self.stubber.assert_no_pending_responses()

    def test_can_provide_file_size(self):
        stubbed_responses = self.stubbed_responses
        # Remove the HeadObject response
        stubbed_responses.pop(0)

        for stubbed_response in stubbed_responses:
            self.stubber.add_response(**stubbed_response)

        call_kwargs = self.call_kwargs
        call_kwargs['subscribers'] = [FileSizeProvider(len(self.content))]

        future = self.manager.download(**call_kwargs)
        future.result()

        # The HeadObject should have not happened and should have been able
        # to successfully download the file.
        self.stubber.assert_no_pending_responses()
        with open(self.filename, 'rb') as f:
            self.assertEqual(self.content, f.read())


class TestNonRangedDownload(BaseDownloadTest):
    __test__ = True

    def test_download(self):
        self.extra_args['RequestPayer'] = 'requester'
        for stubbed_response in self.stubbed_responses:
            stubbed_response['expected_params'] = {
                'Bucket': self.bucket,
                'Key': self.key,
                'RequestPayer': 'requester'
            }
            self.stubber.add_response(**stubbed_response)
        future = self.manager.download(
            self.bucket, self.key, self.filename, self.extra_args)
        future.result()

        # Ensure that the contents are correct
        with open(self.filename, 'rb') as f:
            self.assertEqual(self.content, f.read())


class TestRangedDownload(BaseDownloadTest):
    __test__ = True

    def setUp(self):
        super(TestRangedDownload, self).setUp()
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
                'method': 'get_object',
                'service_response': {
                    'Body': six.BytesIO(self.content[0:4])
                }
            },
            {
                'method': 'get_object',
                'service_response': {
                    'Body': six.BytesIO(self.content[4:8])
                }
            },
            {
                'method': 'get_object',
                'service_response': {
                    'Body': six.BytesIO(self.content[8:])
                }
            }
        ]

    @property
    def expected_progress_callback_info(self):
        # Note that last read is from the empty sentinel indicating
        # that the stream is done.
        return [
            {'bytes_transferred': 4},
            {'bytes_transferred': 0},
            {'bytes_transferred': 4},
            {'bytes_transferred': 0},
            {'bytes_transferred': 2},
            {'bytes_transferred': 0}
        ]

    def test_download(self):
        self.extra_args['RequestPayer'] = 'requester'
        stubbed_responses = self.stubbed_responses
        for stubbed_response in stubbed_responses:
            stubbed_response['expected_params'] = {
                'Bucket': self.bucket,
                'Key': self.key,
                'RequestPayer': 'requester'
            }

        # Add the Range parameter for each ranged GET
        ranges = ['bytes=0-3', 'bytes=4-7', 'bytes=8-']
        for i, range_val in enumerate(ranges):
            stubbed_responses[i+1]['expected_params']['Range'] = range_val

        for stubbed_response in stubbed_responses:
            self.stubber.add_response(**stubbed_response)

        future = self.manager.download(
            self.bucket, self.key, self.filename, self.extra_args)
        future.result()

        # Ensure that the contents are correct
        with open(self.filename, 'rb') as f:
            self.assertEqual(self.content, f.read())
