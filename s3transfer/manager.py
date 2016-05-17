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
from s3transfer.utils import disable_upload_callbacks
from s3transfer.utils import enable_upload_callbacks
from s3transfer.utils import CallArgs
from s3transfer.utils import OSUtils
from s3transfer.futures import BoundedExecutor
from s3transfer.download import DownloadTaskSubmitter
from s3transfer.upload import UploadTaskSubmitter


MB = 1024 * 1024


class TransferConfig(object):
    def __init__(self,
                 multipart_threshold=8 * MB,
                 max_concurrency=10,
                 multipart_chunksize=8 * MB,
                 max_queue_size=0,
                 max_io_queue_size=1000,
                 num_download_attempts=5):
        """Configurations for the transfer mangager

        :param multipart_threshold: The threshold for which multipart
            transfers occur.

        :param max_concurrency: The maximum number or requests that
            can happen at a time.

        :param multipart_chunksize: The size of each transfer if a request
            becomes a multipart transfer.

        :param max_queue_size: The maximum amount of requests that
            can be queued at a time. A value of zero means that there
            is no maximum.

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
        """
        self.multipart_threshold = multipart_threshold
        self.max_concurrency = max_concurrency
        self.multipart_chunksize = multipart_chunksize
        self.max_queue_size = max_queue_size
        self.max_io_queue_size = max_io_queue_size
        self.num_download_attempts = num_download_attempts


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
        self._executor = BoundedExecutor(
            max_size=self._config.max_queue_size,
            max_num_threads=self._config.max_concurrency
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

        :type fileobj: str
        :param fileobj: The name of a file to upload.

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
        upload_submitter = UploadTaskSubmitter(
            client=self._client, osutil=self._osutil, config=self._config,
            executor=self._executor)
        return upload_submitter(call_args)

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
        download_submitter = DownloadTaskSubmitter(
            client=self._client, osutil=self._osutil, config=self._config,
            executor=self._executor, io_executor=self._io_executor)
        return download_submitter(call_args)

    def _validate_all_known_args(self, actual, allowed):
        for kwarg in actual:
            if kwarg not in allowed:
                raise ValueError(
                    "Invalid extra_args key '%s', "
                    "must be one of: %s" % (
                        kwarg, ', '.join(allowed)))

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
        self._executor.shutdown()
        self._io_executor.shutdown()
