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
from s3transfer.futures import CRTTransferFuture
from s3transfer import OSUtils

import io


class CRTTransferManager:
    def __init__(self, session):
        self._submitter = CRTSubmitter(session)

    def download_file(self, bucket, key, filename, extra_args=None,
                      expected_size=None):
        return self._submitter.submit('get_object', bucket, key, filename, extra_args)

    def upload_file(self, filename, bucket, key, extra_args=None):
        return self._submitter.submit('put_object', bucket, key, filename, extra_args)


class FakeRawResponse(six.BytesIO):
    def stream(self, amt=1024, decode_content=None):
        while True:
            chunk = self.read(amt)
            if not chunk:
                break
            yield chunk


class CRTSubmitter:
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

    def submit(self, download_type, bucket, key, filename, extra_args=None):
        if download_type == 'get_object':
            serialized_request = self._client.get_object(
                Bucket=bucket, Key=key)["HTTPRequest"]
        elif download_type == 'put_object':
            # Set the body stream later
            serialized_request = self._client.put_object(
                Bucket=bucket, Key=key)["HTTPRequest"]

        return self._executor.submit(
            serialized_request, filename, download_type, extra_args)


class CRTExecutor:
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
            part_size=5 * 1024 * 1024)

    def submit(self, serialized_http_requests, filename, download_type, extra_args=None):
        # Filename may be needed for handle the body of get_object.
        crt_request = CrtUtil.crt_request_from_aws_request(
            serialized_http_requests)
        if crt_request.headers.get("host") is None:
            # If host is not set, set it for the request before using CRT s3
            url_parts = urlsplit(serialized_http_requests.url)
            crt_request.headers.set("host", url_parts.netloc)
        type = AwsS3RequestType.GET_OBJECT if download_type == 'get_object' else AwsS3RequestType.PUT_OBJECT
        if type == AwsS3RequestType.PUT_OBJECT:
            # TODO the data_len will be removed once the native client makes the change.
            # TODO Content-Length should probably be set by the native client, if the oringal value is 0
            data_len = 10485760
            if extra_args:
                data_len = extra_args["data_len"]
            crt_request.headers.set("Content-Length", str(data_len))
            stream = LazyReadStream(filename, "r+b", data_len)
            crt_request.body_stream = stream

        def _on_response_headers(status_code, headers, **kwargs):
            print(status_code)
            print(dict(headers))

        def _on_response_body(offset, chunk, **kwargs):
            with open(filename, 'wb+') as f:
                print(filename)
                print(offset)
                # seems like the seek here may srew up the file.
                f.seek(offset)
                f.write(chunk)

        init_logging(LogLevel.Trace, "log.txt")
        s3_request = self._crt_client.make_request(request=crt_request,
                                                   type=type,
                                                   on_headers=_on_response_headers,
                                                   on_body=_on_response_body)
        return CRTTransferFuture(s3_request, crt_request)
