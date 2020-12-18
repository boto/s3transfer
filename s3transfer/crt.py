import botocore.awsrequest
import botocore.session
from botocore.utils import CrtUtil
from botocore import UNSIGNED
from botocore.config import Config
from botocore.compat import urlsplit, six
from awscrt.s3 import S3Client, S3RequestType
from awscrt.io import ClientBootstrap, DefaultHostResolver, EventLoopGroup, init_logging, LogLevel
from awscrt.auth import AwsCredentialsProvider
from s3transfer.futures import CRTTransferFuture, TransferMeta
from s3transfer import OSUtils
from s3transfer.utils import CallArgs
from s3transfer.constants import GIGA
import logging

import os

logger = logging.getLogger(__name__)


class CRTTransferManager(object):
    """
    Transfer manager based on CRT s3 client.
    """

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, *args):
        for i in self.futures:
            i.result()

    def __init__(self, session, config):
        self.config = config
        self._submitter = CRTSubmitter(
            session, config, session.get_config_variable("region"))

        self.futures = []

    @property
    def client(self):
        return self._submitter.client

    def download(self, bucket, key, fileobj, extra_args=None, subscribers=[]):
        callargs = CallArgs(bucket=bucket, key=key, fileobj=fileobj,
                            extra_args=extra_args, subscribers=subscribers, request_type="get_object")
        return self._submit_transfer(callargs)

    def upload(self, bucket, key, fileobj, extra_args=None, subscribers=[]):
        callargs = CallArgs(bucket=bucket, key=key, fileobj=fileobj,
                            extra_args=extra_args, subscribers=subscribers, request_type="put_object")
        return self._submit_transfer(callargs)

    def delete(self, bucket, key, extra_args=None, subscribers=[]):
        callargs = CallArgs(bucket=bucket, key=key,
                            extra_args=extra_args, subscribers=subscribers, request_type="delete_object")
        return self._submit_transfer(callargs)

    def _submit_transfer(self, call_args):
        future = self._submitter.submit(call_args)
        self.futures.append(future)
        return future


class FakeRawResponse(six.BytesIO):
    def stream(self, amt=1024, decode_content=None):
        while True:
            chunk = self.read(amt)
            if not chunk:
                break
            yield chunk


class CRTSubmitter(object):
    # using botocore client
    def __init__(self, session, config, region):
        # Turn off the signing process and depends on the crt client to sign the request
        client_config = Config(signature_version=UNSIGNED)
        self._client = session.create_client(
            's3', config=client_config)  # Initialize client

        self._client.meta.events.register(
            'request-created.s3.*', self._capture_http_request)
        self._client.meta.events.register(
            'after-call.s3.*', self._change_response_to_serialized_http_request)
        self._client.meta.events.register(
            'before-send.s3.*', self._make_fake_http_response)
        self._executor = CRTExecutor(config, session, region)
        self._osutil = OSUtils()

    def _capture_http_request(self, request, **kwargs):
        request.context['http_request'] = request

    def _change_response_to_serialized_http_request(self, context, parsed, **kwargs):
        request = context['http_request']
        parsed['HTTPRequest'] = request.prepare()

    def _make_fake_http_response(self, request, **kwargs):
        return botocore.awsrequest.AWSResponse(
            None,
            200,
            {},
            FakeRawResponse(b""),
        )

    @property
    def client(self):
        return self._client

    def submit(self, call_args):
        if call_args.request_type == 'get_object':
            serialized_request = self._client.get_object(
                Bucket=call_args.bucket, Key=call_args.key, **call_args.extra_args)["HTTPRequest"]
        elif call_args.request_type == 'put_object':
            # Set the body stream later
            serialized_request = self._client.put_object(
                Bucket=call_args.bucket, Key=call_args.key, **call_args.extra_args)["HTTPRequest"]
        elif call_args.request_type == 'delete_object':
            serialized_request = self._client.delete_object(
                Bucket=call_args.bucket, Key=call_args.key, **call_args.extra_args)["HTTPRequest"]
        elif call_args.request_type == 'copy_object':
            serialized_request = self._client.copy_object(
                CopySource=call_args.copy_source, Bucket=call_args.bucket, Key=call_args.key, **call_args.extra_args)["HTTPRequest"]
        return self._executor.submit(
            serialized_request, call_args)


class CrtCredentialProviderWrapper():
    """
    Provides the credential for CRT.
    CRT will invoke get_credential method and expected a dictionary return value back.
    """

    def __init__(self, session=None):
        self._session = session

    def get_credential(self):
        credentials = self._session.get_credentials().get_frozen_credentials()

        return {
            "AccessKeyId": credentials.access_key,
            "SecretAccessKey": credentials.secret_key,
            "SessionToken": credentials.token
        }


class CRTExecutor(object):
    def __init__(self, configs=None, session=None, region=None):
        # initialize crt client in here
        event_loop_group = EventLoopGroup(configs.max_request_concurrency)
        host_resolver = DefaultHostResolver(event_loop_group)
        bootstrap = ClientBootstrap(event_loop_group, host_resolver)
        # self._credential_provider = CrtCredentialProviderWrapper(session)
        credential_provider = AwsCredentialsProvider.new_py_provider(
            CrtCredentialProviderWrapper(session))

        # if max_bandwidth not set, we will target 100 Gbps, which means as much as possible.
        target_gbps = 100
        if configs.max_bandwidth:
            # Translate bytes to gigabits
            target_gbps = configs.max_bandwidth*8/(1000*1000)

        self._crt_client = S3Client(
            bootstrap=bootstrap,
            region=region,
            credential_provider=credential_provider,
            part_size=configs.multipart_chunksize,
            throughput_target_gbps=target_gbps)

    def submit(self, serialized_http_requests, call_args):
        logger.debug(serialized_http_requests)
        crt_request = CrtUtil.crt_request_from_aws_request(
            serialized_http_requests)
        if crt_request.headers.get("host") is None:
            # If host is not set, set it for the request before using CRT s3
            url_parts = urlsplit(serialized_http_requests.url)
            crt_request.headers.set("host", url_parts.netloc)
        future = CRTTransferFuture(None, TransferMeta(call_args))
        future.subscriber_manager.on_queued()

        if call_args.request_type == 'get_object':
            type = S3RequestType.GET_OBJECT
        elif call_args.request_type == 'put_object':
            type = S3RequestType.PUT_OBJECT
        else:
            type = S3RequestType.DEFAULT

        if type == S3RequestType.PUT_OBJECT:
            file_stats = os.stat(call_args.fileobj)
            data_len = file_stats.st_size
            crt_request.headers.set("Content-Length", str(data_len))
            content_type = "text/plain"
            if 'ContentType' in call_args.extra_args:
                content_type = call_args.extra_args['ContentType']
            crt_request.headers.set(
                "Content-Type", content_type)
            crt_request.body_stream = future.crt_body_stream
        # TODO CRT logs, may need to expose an option for user to enable/disable it from CLI?
        log_name = "error_log.txt"
        if os.path.exists(log_name):
            os.remove(log_name)

        init_logging(LogLevel.Error, log_name)
        s3_request = self._crt_client.make_request(request=crt_request,
                                                   type=type,
                                                   on_body=future.on_response_body,
                                                   on_done=future.on_done)
        future.set_s3_request(s3_request)
        return future
