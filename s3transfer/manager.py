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
from botocore.compat import six

from s3transfer.compat import accepts_kwargs
from s3transfer.utils import CallArgs
from s3transfer.utils import OSUtils
from s3transfer.futures import BoundedExecutor
from s3transfer.upload import UploadTaskSubmitter


MB = 1024 * 1024


class TransferConfig(object):
    def __init__(self,
                 multipart_threshold=8 * MB,
                 max_concurrency=10,
                 multipart_chunksize=8 * MB,
                 max_queue_size=0):
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
        """
        self.multipart_threshold = multipart_threshold
        self.max_concurrency = max_concurrency
        self.multipart_chunksize = multipart_chunksize
        self.max_queue_size = max_queue_size


class TransferManager(object):
    VALID_SUBSCRIBER_TYPES = [
        'queued',
        'progress',
        'done'
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

        :type subscribers: a list of subscribers
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
        self._validate_subscribers(subscribers)
        self._validate_all_known_args(extra_args, self.ALLOWED_UPLOAD_ARGS)
        call_args = CallArgs(
            fileobj=fileobj, bucket=bucket, key=key, extra_args=extra_args,
            subscribers=subscribers
        )
        upload_submitter = UploadTaskSubmitter(
            client=self._client, osutil=self._osutil, config=self._config,
            executor=self._executor)
        return upload_submitter(call_args)

    def _validate_subscribers(self, subscribers):
        for subscriber in subscribers:
            for subscriber_type in self.VALID_SUBSCRIBER_TYPES:
                subscriber_method = 'on_' + subscriber_type
                if hasattr(subscriber, subscriber_method):
                    self._validate_is_callable_and_accepts_kwargs(
                        getattr(subscriber, subscriber_method))

    def _validate_is_callable_and_accepts_kwargs(self, func):
        if not six.callable(func):
            raise ValueError("Subscriber method %s must be callable." % func)

        if not accepts_kwargs(func):
            raise ValueError("Subscriber method %s must accept keyword "
                             "arguments (**kwargs)" % func)

    def _validate_all_known_args(self, actual, allowed):
        for kwarg in actual:
            if kwarg not in allowed:
                raise ValueError(
                    "Invalid extra_args key '%s', "
                    "must be one of: %s" % (
                        kwarg, ', '.join(allowed)))

    def shutdown(self):
        """Shutdown the TransferManager

        It will wait till all requests complete before it complete shuts down.
        """
        self._executor.shutdown()
