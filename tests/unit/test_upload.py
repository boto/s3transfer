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
from tests import RecordingSubscriber
from tests import UnseekableStream
from s3transfer.futures import TransferFuture
from s3transfer.futures import TransferMeta
from s3transfer.manager import TransferConfig
from s3transfer.upload import get_upload_utils_cls
from s3transfer.upload import UploadFilenameUtils
from s3transfer.upload import UploadSeekableStreamUtils
from s3transfer.upload import UploadUnseekableStreamUtils
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


class BaseUploadTest(BaseTaskTest):
    def setUp(self):
        super(BaseUploadTest, self).setUp()
        self.bucket = 'mybucket'
        self.key = 'foo'
        self.osutil = OSUtils()

        self.tempdir = tempfile.mkdtemp()
        self.filename = os.path.join(self.tempdir, 'myfile')
        self.content = b'my content'

        with open(self.filename, 'wb') as f:
            f.write(self.content)

        self.subscribers = []

    def tearDown(self):
        super(BaseUploadTest, self).tearDown()
        shutil.rmtree(self.tempdir)

    def get_future(self, fileobj):
        meta = TransferMeta(
            CallArgs(fileobj=fileobj, subscribers=self.subscribers))
        return TransferFuture(meta, self.transfer_coordinator)


class TestGetUploadUtilsClsTest(BaseUploadTest):
    def test_for_filename(self):
        future = self.get_future(self.filename)
        self.assertIs(get_upload_utils_cls(future), UploadFilenameUtils)

    def test_for_seekable_file(self):
        with open(self.filename, 'rb') as f:
            future = self.get_future(f)
            self.assertIs(
                get_upload_utils_cls(future), UploadSeekableStreamUtils)

    def test_for_unseekable_file(self):
        with open(self.filename, 'rb') as f:
            with UnseekableStream(f) as stream:
                future = self.get_future(stream)
                self.assertIs(
                    get_upload_utils_cls(future), UploadUnseekableStreamUtils)


class BaseUploadUtilsTest(BaseUploadTest):
    def setUp(self):
        super(BaseUploadUtilsTest, self).setUp()
        self.osutil = OSUtils()
        self.config = TransferConfig()
        self.recording_subscriber = RecordingSubscriber()
        self.subscribers.append(self.recording_subscriber)

    def _get_expected_body_for_part(self, part_number, chunk_size, offset=0):
        total_size = len(self.content)
        start_index = (part_number - 1) * chunk_size - offset
        end_index = part_number * chunk_size - offset
        if end_index >= total_size:
            return self.content[start_index:]
        return self.content[start_index:end_index]


class TestUploadFilenameUtils(BaseUploadUtilsTest):
    def setUp(self):
        super(TestUploadFilenameUtils, self).setUp()
        self.upload_utils = UploadFilenameUtils(self.osutil)
        self.future = self.get_future(self.filename)
        self.future.meta.provide_transfer_size(len(self.content))

    def test_provide_transfer_size(self):
        self.upload_utils.provide_transfer_size(self.future)
        self.assertEqual(self.future.meta.size, len(self.content))

    def test_requires_multipart_upload(self):
        self.assertFalse(
            self.upload_utils.requires_multipart_upload(
                self.future, self.config))
        self.config.multipart_threshold = len(self.content)
        self.assertTrue(
            self.upload_utils.requires_multipart_upload(
                self.future, self.config))

    def test_get_put_object_body(self):
        read_file_chunk = self.upload_utils.get_put_object_body(self.future)
        read_file_chunk.enable_callback()
        self.assertEqual(read_file_chunk.read(), self.content)
        self.assertEqual(
            self.recording_subscriber.calculate_bytes_seen(),
            len(self.content))

    def test_yield_upload_part_bodies(self):
        chunk_size = 4
        self.config.multipart_chunksize = chunk_size
        part_iterator = self.upload_utils.yield_upload_part_bodies(
            self.future, self.config)
        expected_part_number = 1
        for part_number, read_file_chunk in part_iterator:
            # Ensure that the part number is as expected
            self.assertEqual(part_number, expected_part_number)
            read_file_chunk.enable_callback()
            # Ensure that the body is correct for that part.
            self.assertEqual(
                read_file_chunk.read(),
                self._get_expected_body_for_part(part_number, chunk_size))
            expected_part_number += 1
        self.assertEqual(
            self.recording_subscriber.calculate_bytes_seen(),
            len(self.content))


class TestUploadSeekableStreamUtils(TestUploadFilenameUtils):
    def setUp(self):
        super(TestUploadSeekableStreamUtils, self).setUp()
        self.upload_utils = UploadSeekableStreamUtils(self.osutil)
        self.fileobj = open(self.filename, 'rb')
        self.addCleanup(self.fileobj.close)
        self.future = self.get_future(self.fileobj)
        self.future.meta.provide_transfer_size(len(self.content))


class TestUploadUnseekableStreamUtils(BaseUploadUtilsTest):
    def setUp(self):
        super(TestUploadUnseekableStreamUtils, self).setUp()
        self.upload_utils = UploadUnseekableStreamUtils(self.osutil)
        self.fileobj = open(self.filename, 'rb')
        self.stream = UnseekableStream(self.fileobj)
        self.addCleanup(self.stream.close)
        self.future = self.get_future(self.stream)

    def test_provide_transfer_size(self):
        # We should not be able to determine the transfer size of an
        # unseekable stream because we would have to buffer that all into
        # memory.
        self.upload_utils.provide_transfer_size(self.future)
        self.assertEqual(self.future.meta.size, None)

    def test_requires_multipart_upload_with_provided_size(self):
        self.future.meta.provide_transfer_size(len(self.content))
        self.assertFalse(
            self.upload_utils.requires_multipart_upload(
                self.future, self.config))
        self.config.multipart_threshold = len(self.content)
        self.assertTrue(
            self.upload_utils.requires_multipart_upload(
                self.future, self.config))

    def test_requires_multipart_upload_with_no_provided_size(self):
        self.assertFalse(
            self.upload_utils.requires_multipart_upload(
                self.future, self.config))

        # The stream needs to be rewound as it determines multipart
        # uploads by streaming up to the threshold.
        self.fileobj.seek(0)
        self.config.multipart_threshold = len(self.content)
        self.assertTrue(
            self.upload_utils.requires_multipart_upload(
                self.future, self.config))

    def test_get_put_object_body_with_provided_size(self):
        self.future.meta.provide_transfer_size(len(self.content))
        read_file_chunk = self.upload_utils.get_put_object_body(self.future)
        read_file_chunk.enable_callback()
        self.assertEqual(read_file_chunk.read(), self.content)
        self.assertEqual(
            self.recording_subscriber.calculate_bytes_seen(),
            len(self.content))

    def test_get_put_object_body_with_no_provided_size(self):
        # This needs to be called first to pull from the stream initially.
        self.upload_utils.requires_multipart_upload(self.future, self.config)
        read_file_chunk = self.upload_utils.get_put_object_body(self.future)
        read_file_chunk.enable_callback()
        self.assertEqual(read_file_chunk.read(), self.content)
        self.assertEqual(
            self.recording_subscriber.calculate_bytes_seen(),
            len(self.content))

    def test_get_put_object_body_can_be_reread(self):
        # This needs to be called first to pull from the stream initially.
        self.upload_utils.requires_multipart_upload(self.future, self.config)
        read_file_chunk = self.upload_utils.get_put_object_body(self.future)
        read_file_chunk.enable_callback()
        self.assertEqual(read_file_chunk.read(), self.content)
        # We should still be able to seek and reread the stream if botocore
        # ever needed to rewind the stream.
        read_file_chunk.seek(0)
        self.assertEqual(read_file_chunk.read(), self.content)

    def test_yield_upload_part_bodies_with_provided_size(self):
        self.future.meta.provide_transfer_size(len(self.content))
        chunk_size = 4
        self.config.multipart_chunksize = chunk_size
        part_iterator = self.upload_utils.yield_upload_part_bodies(
            self.future, self.config)
        expected_part_number = 1
        for part_number, read_file_chunk in part_iterator:
            # Ensure that the part number is as expected
            self.assertEqual(part_number, expected_part_number)
            read_file_chunk.enable_callback()
            # Ensure that the body is correct for that part.
            self.assertEqual(
                read_file_chunk.read(),
                self._get_expected_body_for_part(part_number, chunk_size))
            expected_part_number += 1
        self.assertEqual(
            self.recording_subscriber.calculate_bytes_seen(),
            len(self.content))

    def test_yield_upload_part_bodies_with_no_provided_size(self):
        threshold = 2
        chunk_size = 4
        self.config.multipart_chunksize = chunk_size
        self.config.multipart_threshold = threshold

        # This needs to be called first to pull from the stream initially.
        self.upload_utils.requires_multipart_upload(self.future, self.config)
        # Then get the iterator to retrieve the part bodies.
        part_iterator = self.upload_utils.yield_upload_part_bodies(
            self.future, self.config)

        expected_part_number = 1
        for part_number, read_file_chunk in part_iterator:
            print(part_number)
            # Ensure that the part number is as expected
            self.assertEqual(part_number, expected_part_number)
            read_file_chunk.enable_callback()

            offset = threshold
            expected_chunk_size = chunk_size
            # The first part will have the size of the threshold as
            # the stream is pulled to see if it is large enough to
            # require a multipart upload
            if part_number == 1:
                offset = 0
                expected_chunk_size = threshold

            # Ensure that the body is correct for that part.
            self.assertEqual(
                read_file_chunk.read(),
                self._get_expected_body_for_part(
                    part_number, expected_chunk_size, offset))
            expected_part_number += 1

        self.assertEqual(
            self.recording_subscriber.calculate_bytes_seen(),
            len(self.content))

    def test_yield_upload_part_bodies_can_be_reread(self):
        self.future.meta.provide_transfer_size(len(self.content))
        chunk_size = 4
        self.config.multipart_chunksize = chunk_size
        part_iterator = self.upload_utils.yield_upload_part_bodies(
            self.future, self.config)
        expected_part_number = 1
        for part_number, read_file_chunk in part_iterator:
            # Ensure that the part number is as expected
            self.assertEqual(part_number, expected_part_number)
            read_file_chunk.enable_callback()
            # Ensure that the body is correct for that part.
            self.assertEqual(
                read_file_chunk.read(),
                self._get_expected_body_for_part(part_number, chunk_size))
            # Ensure that each part can be reread via seeking.
            read_file_chunk.seek(0)
            self.assertEqual(
                read_file_chunk.read(),
                self._get_expected_body_for_part(part_number, chunk_size))
            expected_part_number += 1


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

    def tearDown(self):
        super(TestUploadTaskSubmitter, self).tearDown()
        shutil.rmtree(self.tempdir)

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


class TestPutObjectTask(BaseUploadTest):
    def test_main(self):
        extra_args = {'Metadata': {'foo': 'bar'}}
        with open(self.filename, 'rb') as fileobj:
            task = self.get_task(
                PutObjectTask,
                main_kwargs={
                    'client': self.client,
                    'fileobj': fileobj,
                    'bucket': self.bucket,
                    'key': self.key,
                    'extra_args': extra_args,
                }
            )
            self.stubber.add_response(
                method='put_object',
                service_response={},
                expected_params={
                    'Body': fileobj, 'Bucket': self.bucket, 'Key': self.key,
                    'Metadata': {'foo': 'bar'}
                }
            )
            task()
        self.stubber.assert_no_pending_responses()


class TestCreateMultipartUploadTask(BaseUploadTest):
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


class TestUploadPartTask(BaseUploadTest):
    def test_main(self):
        extra_args = {'RequestPayer': 'requester'}
        upload_id = 'my-id'
        part_number = 1
        etag = 'foo'
        with open(self.filename, 'rb') as fileobj:
            task = self.get_task(
                UploadPartTask,
                main_kwargs={
                    'client': self.client,
                    'fileobj': fileobj,
                    'bucket': self.bucket,
                    'key': self.key,
                    'upload_id': upload_id,
                    'part_number': part_number,
                    'extra_args': extra_args
                }
            )
            self.stubber.add_response(
                method='upload_part',
                service_response={'ETag': etag},
                expected_params={
                    'Body': fileobj, 'Bucket': self.bucket, 'Key': self.key,
                    'UploadId': upload_id, 'PartNumber': part_number,
                    'RequestPayer': 'requester'
                }
            )
            rval = task()
        self.stubber.assert_no_pending_responses()
        self.assertEqual(rval, {'ETag': etag, 'PartNumber': part_number})


class TestCompleteMultipartUploadTask(BaseUploadTest):
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
