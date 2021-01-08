import logging
import os
from io import BytesIO

import botocore.awsrequest
import botocore.session
from botocore import UNSIGNED
from botocore.config import Config
from botocore.compat import urlsplit
import awscrt.http
from awscrt.s3 import S3Client, S3RequestType
from awscrt.io import ClientBootstrap, DefaultHostResolver, EventLoopGroup
from awscrt.auth import AwsCredentialsProvider, AwsCredentials

from s3transfer.futures import BaseTransferFuture, BaseTransferMeta
from s3transfer.utils import CallArgs, OSUtils, get_callbacks

logger = logging.getLogger(__name__)


class CRTTransferConfig(object):
    def __init__(self,
                 max_bandwidth=None,
                 multipart_chunksize=None,
                 max_request_processes=None):
        # TODO rename max_bandwidth and max_request_processes for more
        # appropriate description
        self.max_bandwidth = max_bandwidth
        self.multipart_chunksize = multipart_chunksize
        self.max_request_processes = max_request_processes


class CRTTransferManager(object):
    """
    Transfer manager based on CRT s3 client.
    """

    def __init__(self, session, config=None, crt_s3_client=None):
        self._crt_credential_provider = \
            self._botocore_credential_provider_adaptor(session)
        if config is None:
            config = CRTTransferConfig()
        if crt_s3_client is None:
            crt_s3_client = self._create_crt_s3_client(session, config)
        self._crt_s3_client = crt_s3_client
        self._s3_args_creator = S3ClientArgsCreator(session)
        self._futures = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, *args):
        # Wait until all the transfer done, even if some fails and clean up all
        # the underlying resource
        try:
            for future in self._futures:
                future.result()
        except Exception:
            pass
        shutdown_event = self._crt_s3_client.shutdown_event
        self._crt_s3_client = None
        shutdown_event.wait()

    def download(self, bucket, key, fileobj, extra_args=None,
                 subscribers=None):
        if extra_args is None:
            extra_args = {}
        callargs = CallArgs(
            bucket=bucket, key=key, fileobj=fileobj,
            extra_args=extra_args, subscribers=subscribers)
        return self._submit_transfer("get_object", callargs)

    def upload(self, bucket, key, fileobj, extra_args=None,
               subscribers=None):
        if extra_args is None:
            extra_args = {}
        callargs = CallArgs(
            bucket=bucket, key=key, fileobj=fileobj,
            extra_args=extra_args, subscribers=subscribers)
        return self._submit_transfer("put_object", callargs)

    def delete(self, bucket, key, extra_args=None,
               subscribers=None):
        if extra_args is None:
            extra_args = {}
        callargs = CallArgs(
            bucket=bucket, key=key, extra_args=extra_args,
            subscribers=subscribers)
        return self._submit_transfer("delete_object", callargs)

    def _botocore_credential_provider_adaptor(self, session):

        def provider():
            credentials = session.get_credentials().get_frozen_credentials()
            return AwsCredentials(credentials.access_key,
                                  credentials.secret_key, credentials.token)

        return provider

    def _create_crt_s3_client(self, session, transfer_config):
        client_factory = CRTS3ClientFactory()
        return client_factory.create_client(
            region=session.get_config_variable("region"),
            client_config=session.get_default_client_config(),
            transfer_config=transfer_config,
            credential_provider=self._crt_credential_provider
        )

    def _submit_transfer(self, request_type, call_args):
        # TODO unique id may be needed for the future.
        future = CRTTransferFuture(None, CRTTransferMeta(call_args=call_args))

        # TODO Catch any exception happens during serialization and set the
        # expection for
        on_queued = self._s3_args_creator.get_crt_callback(future, 'queued')
        on_queued()
        crt_callargs = self._s3_args_creator.get_make_request_args(
            request_type, call_args, future)
        crt_s3_request = self._crt_s3_client.make_request(**crt_callargs)

        future.set_s3_request(crt_s3_request)
        self._futures.append(future)
        return future


class CRTUtil(object):
    '''
    Utilities related to CRT.
    '''
    def crt_request_from_aws_request(aws_request):
        url_parts = urlsplit(aws_request.url)
        crt_path = url_parts.path
        if url_parts.query:
            crt_path = '%s?%s' % (crt_path, url_parts.query)
        headers_list = []
        for name, value in aws_request.headers.items():
            if isinstance(value, str):
                headers_list.append((name, value))
            else:
                headers_list.append((name, str(value, 'utf-8')))

        crt_headers = awscrt.http.HttpHeaders(headers_list)
        # CRT requires body (if it exists) to be an I/O stream.
        crt_body_stream = None
        if aws_request.body:
            if hasattr(aws_request.body, 'seek'):
                crt_body_stream = aws_request.body
            else:
                crt_body_stream = BytesIO(aws_request.body)

        crt_request = awscrt.http.HttpRequest(
            method=aws_request.method,
            path=crt_path,
            headers=crt_headers,
            body_stream=crt_body_stream)
        return crt_request


class CRTTransferMeta(BaseTransferMeta):
    """Holds metadata about the CRTTransferFuture"""

    def __init__(self, transfer_id=None, call_args=None):
        self._transfer_id = transfer_id
        self._call_args = call_args
        self._user_context = {}
        self._size = 0

    @property
    def call_args(self):
        return self._call_args

    @property
    def transfer_id(self):
        return self._transfer_id

    @property
    def user_context(self):
        return self._user_context

    @property
    def size(self):
        return self._size

    def provide_transfer_size(self, size):
        # TODO the size is not related to CRT, move this to CLI
        self._size = size


class CRTTransferFuture(BaseTransferFuture):
    def __init__(self, s3_request=None, meta=None):
        """The future associated to a submitted transfer request via CRT S3 client

        :type s3_request: S3Request
        :param s3_request: The s3_request, the CRT s3 request handles cancel
            and the finish future.

        :type meta: CRTTransferMeta
        :param meta: The metadata associated to the request. This object
            is visible to the requester.
        """
        self._s3_request = s3_request
        self._crt_future = None
        if s3_request:
            self._crt_future = self._s3_request.finished_future
        self._meta = meta
        if meta is None:
            self._meta = CRTTransferMeta()

    @property
    def meta(self):
        return self._meta

    # TODO remove this later, since it's not something we want user to have
    # access to
    def set_s3_request(self, s3_request):
        self._s3_request = s3_request
        self._crt_future = self._s3_request.finished_future

    def done(self):
        return self._crt_future.done()

    def result(self):
        try:
            if self._s3_request:
                result = self._crt_future.result()
                self._s3_request = None
                return result
            return
        except KeyboardInterrupt as e:
            self.cancel()
            raise e

    def cancel(self):
        # TODO support cancel correctly for error handling
        raise NotImplementedError('cancel')


class CRTS3ClientFactory:
    def create_client(self, region, transfer_config=None,
                      client_config=None,
                      credential_provider=None):
        # TODO client config is not resolved correctly.
        event_loop_group = EventLoopGroup(
            transfer_config.max_request_processes)
        host_resolver = DefaultHostResolver(event_loop_group)
        bootstrap = ClientBootstrap(
            event_loop_group, host_resolver)
        provider = AwsCredentialsProvider.new_delegate(
            credential_provider)
        target_gbps = 0
        if transfer_config.max_bandwidth:
            # Translate bytes to gigabits
            target_gbps = transfer_config.max_bandwidth * \
                8 / (1000 * 1000 * 1000)

        return S3Client(
            bootstrap=bootstrap,
            region=region,
            credential_provider=provider,
            part_size=transfer_config.multipart_chunksize,
            throughput_target_gbps=target_gbps)


class FakeRawResponse(BytesIO):
    def stream(self, amt=1024, decode_content=None):
        while True:
            chunk = self.read(amt)
            if not chunk:
                break
            yield chunk


class S3ClientArgsCreator:
    def __init__(self, session):
        # Turn off the signing process and depends on the crt client to sign
        # the request
        client_config = Config(signature_version=UNSIGNED)
        self._client = session.create_client(
            's3', config=client_config)
        self._os_utils = OSUtils()
        self._client.meta.events.register(
            'request-created.s3.*', self._capture_http_request)
        self._client.meta.events.register(
            'after-call.s3.*',
            self._change_response_to_serialized_http_request)
        self._client.meta.events.register(
            'before-send.s3.*', self._make_fake_http_response)

    def get_make_request_args(self, request_type, call_args, future):
        recv_filepath = None
        send_filepath = None
        s3_meta_request_type = getattr(
            S3RequestType,
            request_type.upper(),
            S3RequestType.DEFAULT)
        if s3_meta_request_type == S3RequestType.GET_OBJECT:
            recv_filepath = call_args.fileobj
        elif s3_meta_request_type == S3RequestType.PUT_OBJECT:
            send_filepath = call_args.fileobj
            data_len = self._os_utils.get_file_size(send_filepath)
            call_args.extra_args["ContentLength"] = data_len

        botocore_http_request = self._get_botocore_http_request(
            request_type, call_args)
        crt_request = self._convert_to_crt_http_request(botocore_http_request)

        return {
            'request': crt_request,
            'type': s3_meta_request_type,
            'recv_filepath': recv_filepath,
            'send_filepath': send_filepath,
            'on_done': self.get_crt_callback(future, 'done'),
            'on_progress': self.get_crt_callback(future, 'progress')
        }

    def get_crt_callback(self, future, callback_type):

        def invoke_subscriber_callbacks(*args, **kwargs):
            # TODO The get_callbacks helper will set the first augment
            # by keyword, the other augments need to be set by keyword
            # as well. Consider removing the restriction later
            if callback_type == "progress":
                callback(bytes_transferred=args[0])
            else:
                callback(*args, **kwargs)

        return invoke_subscriber_callbacks

    def _convert_to_crt_http_request(self, botocore_http_request):
        # Logic that does CRTUtils.crt_request_from_aws_request
        crt_request = CRTUtil.crt_request_from_aws_request(
            botocore_http_request)
        if crt_request.headers.get("host") is None:
            # If host is not set, set it for the request before using CRT s3
            url_parts = urlsplit(botocore_http_request.url)
            crt_request.headers.set("host", url_parts.netloc)
        return crt_request

    def _capture_http_request(self, request, **kwargs):
        request.context['http_request'] = request

    def _change_response_to_serialized_http_request(
            self, context, parsed, **kwargs):
        request = context['http_request']
        parsed['HTTPRequest'] = request.prepare()

    def _make_fake_http_response(self, request, **kwargs):
        return botocore.awsrequest.AWSResponse(
            None,
            200,
            {},
            FakeRawResponse(b""),
        )

    def _get_botocore_http_request(self, client_method, call_args):
        return getattr(self._client, client_method)(
            Bucket=call_args.bucket, Key=call_args.key,
            **call_args.extra_args
        )['HTTPRequest']
