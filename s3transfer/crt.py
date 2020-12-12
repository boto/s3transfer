from botocore import args
import botocore.awsrequest
import botocore.session
from botocore.utils import CrtUtil
from botocore import UNSIGNED
from botocore.config import Config
from botocore.compat import urlsplit, six
from awscrt.s3 import S3Client, AwsS3RequestType
from awscrt.io import InputStream, LazyReadStream, ClientBootstrap, ClientTlsContext, DefaultHostResolver, EventLoopGroup, TlsConnectionOptions, TlsContextOptions, init_logging, LogLevel
from awscrt.auth import AwsCredentialsProvider
from awscrt.http import HttpHeaders, HttpRequest
from urllib3.response import HTTPResponse
from s3transfer.futures import CRTTransferFuture, TransferMeta
from s3transfer import OSUtils
from s3transfer.utils import CallArgs

import os
import functools

import io


class CRTTransferManager(object):

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, *args):
        for i in self.futures:
            i.result()

    def __init__(self, session):
        self._submitter = CRTSubmitter(session)
        # session.set_debug_logger()

        self.futures = []

    def download_file(self, bucket, key, filename, extra_args=None, subscribers=[]):
        callargs = CallArgs(bucket=bucket, key=key, fileobj=filename,
                            extra_args=extra_args, subscribers=subscribers, download_type="get_object")
        return self._submit_transfer(callargs)

    def upload_file(self, filename, bucket, key, extra_args=None, subscribers=[]):
        callargs = CallArgs(bucket=bucket, key=key, fileobj=filename,
                            extra_args=extra_args, subscribers=subscribers, download_type="put_object")
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
    def __init__(self, session):
        # Turn off the signing process and depends on the crt client to sign the request
        config = Config(signature_version=UNSIGNED)
        self._client = session.create_client(
            's3', config=config)  # Initialize client
        # config = self._client.configure  # Get the configuration from CLI

        self._client.meta.events.register(
            'request-created.s3.*', self._capture_http_request)
        self._client.meta.events.register(
            'after-call.s3.*', self._change_response_to_serialized_http_request)
        self._client.meta.events.register(
            'before-send.s3.*', self._make_fake_http_response)
        self._executor = CRTExecutor()
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

    def submit(self, call_args):
        if call_args.download_type == 'get_object':
            serialized_request = self._client.get_object(
                Bucket=call_args.bucket, Key=call_args.key)["HTTPRequest"]
        elif call_args.download_type == 'put_object':
            # Set the body stream later
            serialized_request = self._client.put_object(
                Bucket=call_args.bucket, Key=call_args.key)["HTTPRequest"]
        return self._executor.submit(
            serialized_request, call_args)


class CRTExecutor(object):
    # Do the job for real
    def __init__(self, configs=None):
        # parse the configs from CLI

        # initialize crt client in here
        event_loop_group = EventLoopGroup()
        host_resolver = DefaultHostResolver(event_loop_group)
        bootstrap = ClientBootstrap(event_loop_group, host_resolver)
        credential_provider = AwsCredentialsProvider.new_default_chain(
            bootstrap)
        # TODO: The region in the request will still be the same as the region in the configuration.
        # Not sure what will be affected by the region of the crt S3 client.
        self._crt_client = S3Client(
            bootstrap=bootstrap,
            region="us-west-2",
            credential_provider=credential_provider,
            throughput_target_gbps=100)

    def submit(self, serialized_http_requests, call_args):
        # Filename may be needed for handle the body of get_object.
        crt_request = CrtUtil.crt_request_from_aws_request(
            serialized_http_requests)
        if crt_request.headers.get("host") is None:
            # If host is not set, set it for the request before using CRT s3
            url_parts = urlsplit(serialized_http_requests.url)
            crt_request.headers.set("host", url_parts.netloc)
        future = CRTTransferFuture(
            None, crt_request, TransferMeta(call_args))
        type = AwsS3RequestType.GET_OBJECT if call_args.download_type == 'get_object' else AwsS3RequestType.PUT_OBJECT
        if type == AwsS3RequestType.PUT_OBJECT:
            file_stats = os.stat(call_args.fileobj)
            data_len = file_stats.st_size
            crt_request.headers.set("Content-Length", str(data_len))
            crt_request.headers.set("Content-Type", "text/plain")
            crt_request.body_stream = future.crt_body_stream
        log_name = "error_log.txt"
        if os.path.exists(log_name):
            os.remove(log_name)

        init_logging(LogLevel.Error, log_name)
        # future.subscriber_manager.on_queued()
        s3_request = self._crt_client.make_request(request=crt_request,
                                                   type=type,
                                                   on_headers=future.on_response_headers,
                                                   on_body=future.on_response_body,
                                                   on_done=future.on_done)
        future.subscriber_manager.on_queued()
        future.set_s3_request(s3_request)
        return future
