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
from s3transfer.utils import unique_id
from s3transfer.utils import get_callbacks
from s3transfer.utils import disable_upload_callbacks
from s3transfer.utils import enable_upload_callbacks
from s3transfer.utils import CallArgs
from s3transfer.utils import OSUtils
from s3transfer.futures import IN_MEMORY_UPLOAD_TAG
from s3transfer.futures import BoundedExecutor
from s3transfer.futures import TransferFuture
from s3transfer.futures import TransferMeta
from s3transfer.futures import TransferCoordinator
from s3transfer.download import DownloadSubmissionTask
from s3transfer.upload import UploadSubmissionTask
from s3transfer.copies import CopySubmissionTask


MB = 1024 * 1024


class TransferConfig(object):
    def __init__(self,
                 multipart_threshold=8 * MB,
                 multipart_chunksize=8 * MB,
                 max_request_concurrency=10,
                 max_submission_concurrency=5,
                 max_request_queue_size=0,
                 max_submission_queue_size=0,
                 max_io_queue_size=1000,
                 num_download_attempts=5,
                 max_in_memory_upload_chunks=10):
        """Configurations for the transfer mangager

        :param multipart_threshold: The threshold for which multipart
            transfers occur.

        :param max_request_concurrency: The maximum number of S3 API
            transfer-related requests that can happen at a time.

        :param max_submission_concurrency: The maximum number of threads
            processing a call to a TransferManager method. Processing a
            call usually entails determining which S3 API requests that need
            to be enqueued, but does **not** entail making any of the
            S3 API data transfering requests needed to perform the transfer.
            The threads controlled by ``max_request_concurrency`` is
            responsible for that.

        :param multipart_chunksize: The size of each transfer if a request
            becomes a multipart transfer.

        :param max_request_queue_size: The maximum amount of S3 API requests
            that can be queued at a time. A value of zero means that there
            is no maximum.

        :param max_submission_queue_size: The maximum amount of
            TransferManager method calls that can be queued at a time. A value
            of zero means that there is no maximum.

        :param max_io_queue_size: The maximum amount of read parts that
            can be queued to be written to disk per download. A value of zero
            means that there is no maximum. The default size for each element
            in this queue is 8 KB.

        :param num_download_attempts: The number of download attempts that
            will be tried upon errors with downloading an object in S3. Note
            that these retries account for errors that occur when streamming
            down the data from s3 (i.e. socket errors and read timeouts that
            occur after recieving an OK response from s3).
            Other retryable exceptions such as throttling errors and 5xx errors
            are already retried by botocore (this default is 5). The
            ``num_download_attempts`` does not take into account the
            number of exceptions retried by botocore.

        :param max_in_memory_upload_chunks: The number of chunks that can
            be stored in memory at a time for all ongoing upload requests.
            This pertains to chunks of data that need to be stored in memory
            during an upload if the data is sourced from a file-like object.
            The total maximum memory footprint due to a in-memory upload
            chunks is roughly equal to:

                max_in_memory_upload_chunks * multipart_chunksize
                + max_submission_concurrency * multipart_chunksize

            ``max_submission_concurrency`` has an affect on this value because
            for each thread pulling data off of a file-like object, they may
            be waiting with a single read chunk to be submitted for upload
            because the ``max_in_memory_upload_chunks`` value has been reached
            by the threads making the upload request.
        """
        self.multipart_threshold = multipart_threshold
        self.multipart_chunksize = multipart_chunksize
        self.max_request_concurrency = max_request_concurrency
        self.max_submission_concurrency = max_submission_concurrency
        self.max_request_queue_size = max_request_queue_size
        self.max_submission_queue_size = max_submission_queue_size
        self.max_io_queue_size = max_io_queue_size
        self.num_download_attempts = num_download_attempts
        self.max_in_memory_upload_chunks = max_in_memory_upload_chunks


class TransferManager(object):
    ALLOWED_DOWNLOAD_ARGS = [
        'VersionId',
        'SSECustomerAlgorithm',
        'SSECustomerKey',
        'SSECustomerKeyMD5',
        'RequestPayer',
    ]

    ALLOWED_UPLOAD_ARGS = [
        'ACL',
        'CacheControl',
        'ContentDisposition',
        'ContentEncoding',
        'ContentLanguage',
        'ContentType',
        'Expires',
        'GrantFullControl',
        'GrantRead',
        'GrantReadACP',
        'GrantWriteACL',
        'Metadata',
        'RequestPayer',
        'ServerSideEncryption',
        'StorageClass',
        'SSECustomerAlgorithm',
        'SSECustomerKey',
        'SSECustomerKeyMD5',
        'SSEKMSKeyId',
    ]

    ALLOWED_COPY_ARGS = ALLOWED_UPLOAD_ARGS + [
        'CopySourceIfMatch',
        'CopySourceIfModifiedSince',
        'CopySourceIfNoneMatch',
        'CopySourceIfUnmodifiedSince',
        'CopySourceSSECustomerAlgorithm',
        'CopySourceSSECustomerKey',
        'CopySourceSSECustomerKeyMD5',
        'MetadataDirective'
    ]

    def __init__(self, client, config=None):
        """A transfer manager interface for Amazon S3

        :param client: Client to be used by the manager
        :param config: TransferConfig to associate specific configurations
        """
        self._client = client
        self._config = config
        if config is None:
            self._config = TransferConfig()
        self._osutil = OSUtils()

        # The executor responsible for making S3 API transfer requests
        self._request_executor = BoundedExecutor(
            max_size=self._config.max_request_queue_size,
            max_num_threads=self._config.max_request_concurrency,
            tag_max_sizes={
                IN_MEMORY_UPLOAD_TAG: self._config.max_in_memory_upload_chunks
            }
        )

        # The executor responsible for submitting the necessary tasks to
        # perform the desired transfer
        self._submission_executor = BoundedExecutor(
            max_size=self._config.max_submission_queue_size,
            max_num_threads=self._config.max_submission_concurrency
        )

        # There is one thread available for writing to disk. It will handle
        # downloads for all files.
        self._io_executor = BoundedExecutor(
            max_size=self._config.max_io_queue_size,
            max_num_threads=1
        )
        self._register_handlers()

    def upload(self, fileobj, bucket, key, extra_args=None, subscribers=None):
        """Uploads a file to S3

        :type fileobj: str or seekable file-like object
        :param fileobj: The name of a file to upload or a seekable file-like
            object to upload. It is recommended to use a filename because
            file-like objects may result in higher memory usage.

        :type bucket: str
        :param bucket: The name of the bucket to upload to

        :type key: str
        :param key: The name of the key to upload to

        :type extra_args: dict
        :param extra_args: Extra arguments that may be passed to the
            client operation

        :type subscribers: list(s3transfer.subscribers.BaseSubscriber)
        :param subscribers: The list of subscribers to be invoked in the
            order provided based on the event emit during the process of
            the transfer request.

        :rtype: s3transfer.futures.TransferFuture
        :returns: Transfer future representing the upload
        """
        if extra_args is None:
            extra_args = {}
        if subscribers is None:
            subscribers = []
        self._validate_all_known_args(extra_args, self.ALLOWED_UPLOAD_ARGS)
        call_args = CallArgs(
            fileobj=fileobj, bucket=bucket, key=key, extra_args=extra_args,
            subscribers=subscribers
        )
        return self._submit_transfer(call_args, UploadSubmissionTask)

    def download(self, bucket, key, fileobj, extra_args=None,
                 subscribers=None):
        """Downloads a file from S3

        :type bucket: str
        :param bucket: The name of the bucket to download from

        :type key: str
        :param key: The name of the key to download from

        :type fileobj: str
        :param fileobj: The name of a file to download to.

        :type extra_args: dict
        :param extra_args: Extra arguments that may be passed to the
            client operation

        :type subscribers: list(s3transfer.subscribers.BaseSubscriber)
        :param subscribers: The list of subscribers to be invoked in the
            order provided based on the event emit during the process of
            the transfer request.

        :rtype: s3transfer.futures.TransferFuture
        :returns: Transfer future representing the download
        """
        if extra_args is None:
            extra_args = {}
        if subscribers is None:
            subscribers = []
        self._validate_all_known_args(extra_args, self.ALLOWED_DOWNLOAD_ARGS)
        call_args = CallArgs(
            bucket=bucket, key=key, fileobj=fileobj, extra_args=extra_args,
            subscribers=subscribers
        )
        return self._submit_transfer(call_args, DownloadSubmissionTask,
                                     {'io_executor': self._io_executor})

    def copy(self, copy_source, bucket, key, extra_args=None,
             subscribers=None, source_client=None):
        """Copies a file in S3

        :type copy_source: dict
        :param copy_source: The name of the source bucket, key name of the
            source object, and optional version ID of the source object. The
            dictionary format is:
            ``{'Bucket': 'bucket', 'Key': 'key', 'VersionId': 'id'}``. Note
            that the ``VersionId`` key is optional and may be omitted.

        :type bucket: str
        :param bucket: The name of the bucket to copy to

        :type key: str
        :param key: The name of the key to copy to

        :type extra_args: dict
        :param extra_args: Extra arguments that may be passed to the
            client operation

        :type subscribers: a list of subscribers
        :param subscribers: The list of subscribers to be invoked in the
            order provided based on the event emit during the process of
            the transfer request.

        :type source_client: botocore or boto3 Client
        :param source_client: The client to be used for operation that
            may happen at the source object. For example, this client is
            used for the head_object that determines the size of the copy.
            If no client is provided, the transfer manager's client is used
            as the client for the source object.

        :rtype: s3transfer.futures.TransferFuture
        :returns: Transfer future representing the copy
        """
        if extra_args is None:
            extra_args = {}
        if subscribers is None:
            subscribers = []
        if source_client is None:
            source_client = self._client
        self._validate_all_known_args(extra_args, self.ALLOWED_COPY_ARGS)
        call_args = CallArgs(
            copy_source=copy_source, bucket=bucket, key=key,
            extra_args=extra_args, subscribers=subscribers,
            source_client=source_client
        )
        return self._submit_transfer(call_args, CopySubmissionTask)

    def _validate_all_known_args(self, actual, allowed):
        for kwarg in actual:
            if kwarg not in allowed:
                raise ValueError(
                    "Invalid extra_args key '%s', "
                    "must be one of: %s" % (
                        kwarg, ', '.join(allowed)))

    def _submit_transfer(self, call_args, submission_task_cls,
                         extra_main_kwargs=None):
        if not extra_main_kwargs:
            extra_main_kwargs = {}

        # Create a TransferFuture to return back to the user
        transfer_future, components = self._get_future_with_components(
            call_args)

        # Add any provided done callbacks to the created transfer future
        # to be invoked on the transfer future being complete.
        for callback in get_callbacks(transfer_future, 'done'):
            components['coordinator'].add_done_callback(callback)

        # Get the main kwargs needed to instantiate the submission task
        main_kwargs = self._get_submission_task_main_kwargs(
            transfer_future, extra_main_kwargs)

        # Submit a SubmissionTask that will submit all of the necessary
        # tasks needed to complete the S3 transfer.
        self._submission_executor.submit(
            submission_task_cls(
                transfer_coordinator=components['coordinator'],
                main_kwargs=main_kwargs
            )
        )
        return transfer_future

    def _get_future_with_components(self, call_args):
        # Creates a new transfer future along with its components
        components = {
            'meta': TransferMeta(call_args),
            'coordinator': TransferCoordinator()
        }
        transfer_future = TransferFuture(**components)
        return transfer_future, components

    def _get_submission_task_main_kwargs(
            self, transfer_future, extra_main_kwargs):
        main_kwargs = {
            'client': self._client,
            'config': self._config,
            'osutil': self._osutil,
            'request_executor': self._request_executor,
            'transfer_future': transfer_future
        }
        main_kwargs.update(extra_main_kwargs)
        return main_kwargs

    def _register_handlers(self):
        # Register handlers to enable/disable callbacks on uploads.
        event_name = 'request-created.s3'
        enable_id = unique_id('s3upload-callback-enable')
        disable_id = unique_id('s3upload-callback-disable')
        self._client.meta.events.register_first(
            event_name, disable_upload_callbacks, unique_id=disable_id)
        self._client.meta.events.register_last(
            event_name, enable_upload_callbacks, unique_id=enable_id)

    def shutdown(self):
        """Shutdown the TransferManager

        It will wait till all requests complete before it complete shuts down.
        """
        self._submission_executor.shutdown()
        self._request_executor.shutdown()
        self._io_executor.shutdown()
