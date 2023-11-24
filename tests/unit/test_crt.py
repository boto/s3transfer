# Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import io

from botocore.credentials import CredentialResolver, ReadOnlyCredentials
from botocore.exceptions import ClientError
from botocore.session import Session

from s3transfer.exceptions import TransferNotDoneError
from s3transfer.utils import CallArgs
from tests import HAS_CRT, FileCreator, mock, requires_crt, unittest

if HAS_CRT:
    import awscrt.s3

    import s3transfer.crt


class CustomFutureException(Exception):
    pass


@requires_crt
class TestBotocoreCRTRequestSerializer(unittest.TestCase):
    def setUp(self):
        self.region = 'us-west-2'
        self.session = Session()
        self.session.set_config_variable('region', self.region)
        self.request_serializer = s3transfer.crt.BotocoreCRTRequestSerializer(
            self.session
        )
        self.bucket = "test_bucket"
        self.key = "test_key"
        self.files = FileCreator()
        self.filename = self.files.create_file('myfile', 'my content')
        self.expected_path = "/" + self.bucket + "/" + self.key
        self.expected_host = "s3.%s.amazonaws.com" % (self.region)

    def tearDown(self):
        self.files.remove_all()

    def test_upload_request(self):
        callargs = CallArgs(
            bucket=self.bucket,
            key=self.key,
            fileobj=self.filename,
            extra_args={},
            subscribers=[],
        )
        coordinator = s3transfer.crt.CRTTransferCoordinator()
        future = s3transfer.crt.CRTTransferFuture(
            s3transfer.crt.CRTTransferMeta(call_args=callargs), coordinator
        )
        crt_request = self.request_serializer.serialize_http_request(
            "put_object", future
        )
        self.assertEqual("PUT", crt_request.method)
        self.assertEqual(self.expected_path, crt_request.path)
        self.assertEqual(self.expected_host, crt_request.headers.get("host"))
        self.assertIsNone(crt_request.headers.get("Authorization"))

    def test_download_request(self):
        callargs = CallArgs(
            bucket=self.bucket,
            key=self.key,
            fileobj=self.filename,
            extra_args={},
            subscribers=[],
        )
        coordinator = s3transfer.crt.CRTTransferCoordinator()
        future = s3transfer.crt.CRTTransferFuture(
            s3transfer.crt.CRTTransferMeta(call_args=callargs), coordinator
        )
        crt_request = self.request_serializer.serialize_http_request(
            "get_object", future
        )
        self.assertEqual("GET", crt_request.method)
        self.assertEqual(self.expected_path, crt_request.path)
        self.assertEqual(self.expected_host, crt_request.headers.get("host"))
        self.assertIsNone(crt_request.headers.get("Authorization"))

    def test_delete_request(self):
        callargs = CallArgs(
            bucket=self.bucket, key=self.key, extra_args={}, subscribers=[]
        )
        coordinator = s3transfer.crt.CRTTransferCoordinator()
        future = s3transfer.crt.CRTTransferFuture(
            s3transfer.crt.CRTTransferMeta(call_args=callargs), coordinator
        )
        crt_request = self.request_serializer.serialize_http_request(
            "delete_object", future
        )
        self.assertEqual("DELETE", crt_request.method)
        self.assertEqual(self.expected_path, crt_request.path)
        self.assertEqual(self.expected_host, crt_request.headers.get("host"))
        self.assertIsNone(crt_request.headers.get("Authorization"))

    def _create_crt_response_error(
        self, status_code, body, operation_name=None
    ):
        return awscrt.s3.S3ResponseError(
            code=14343,
            name='AWS_ERROR_S3_INVALID_RESPONSE_STATUS',
            message='Invalid response status from request',
            status_code=status_code,
            headers=[
                ('x-amz-request-id', 'QSJHJJZR2EDYD4GQ'),
                (
                    'x-amz-id-2',
                    'xDbgdKdvYZTjgpOTzm7yNP2JPrOQl+eaQvUkFdOjdJoWkIC643fgHxdsHpUKvVAfjKf5F6otEYA=',
                ),
                ('Content-Type', 'application/xml'),
                ('Transfer-Encoding', 'chunked'),
                ('Date', 'Fri, 10 Nov 2023 23:22:47 GMT'),
                ('Server', 'AmazonS3'),
            ],
            body=body,
            operation_name=operation_name,
        )

    def test_translate_get_object_404(self):
        body = (
            b'<?xml version="1.0" encoding="UTF-8"?>\n<Error>'
            b'<Code>NoSuchKey</Code>'
            b'<Message>The specified key does not exist.</Message>'
            b'<Key>obviously-no-such-key.txt</Key>'
            b'<RequestId>SBJ7ZQY03N1WDW9T</RequestId>'
            b'<HostId>SomeHostId</HostId></Error>'
        )
        crt_exc = self._create_crt_response_error(404, body, 'GetObject')
        boto_err = self.request_serializer.translate_crt_exception(crt_exc)
        self.assertIsInstance(
            boto_err, self.session.create_client('s3').exceptions.NoSuchKey
        )

    def test_translate_head_object_404(self):
        # There's no body in a HEAD response, so we can't map it to a modeled S3 exception.
        # But it should still map to a botocore ClientError
        body = None
        crt_exc = self._create_crt_response_error(
            404, body, operation_name='HeadObject'
        )
        boto_err = self.request_serializer.translate_crt_exception(crt_exc)
        self.assertIsInstance(boto_err, ClientError)

    def test_translate_unknown_operation_404(self):
        body = None
        crt_exc = self._create_crt_response_error(404, body)
        boto_err = self.request_serializer.translate_crt_exception(crt_exc)
        self.assertIsInstance(boto_err, ClientError)


@requires_crt
class TestCRTCredentialProviderAdapter(unittest.TestCase):
    def setUp(self):
        self.botocore_credential_provider = mock.Mock(CredentialResolver)
        self.access_key = "access_key"
        self.secret_key = "secret_key"
        self.token = "token"
        self.botocore_credential_provider.load_credentials.return_value.get_frozen_credentials.return_value = ReadOnlyCredentials(
            self.access_key, self.secret_key, self.token
        )

    def _call_adapter_and_check(self, credentails_provider_adapter):
        credentials = credentails_provider_adapter()
        self.assertEqual(credentials.access_key_id, self.access_key)
        self.assertEqual(credentials.secret_access_key, self.secret_key)
        self.assertEqual(credentials.session_token, self.token)

    def test_fetch_crt_credentials_successfully(self):
        credentails_provider_adapter = (
            s3transfer.crt.CRTCredentialProviderAdapter(
                self.botocore_credential_provider
            )
        )
        self._call_adapter_and_check(credentails_provider_adapter)

    def test_load_credentials_once(self):
        credentails_provider_adapter = (
            s3transfer.crt.CRTCredentialProviderAdapter(
                self.botocore_credential_provider
            )
        )
        called_times = 5
        for i in range(called_times):
            self._call_adapter_and_check(credentails_provider_adapter)
        # Assert that the load_credentails of botocore credential provider
        # will only be called once
        self.assertEqual(
            self.botocore_credential_provider.load_credentials.call_count, 1
        )


@requires_crt
class TestCRTTransferFuture(unittest.TestCase):
    def setUp(self):
        self.mock_s3_request = mock.Mock(awscrt.s3.S3RequestType)
        self.mock_crt_future = mock.Mock(awscrt.s3.Future)
        self.mock_s3_request.finished_future = self.mock_crt_future
        self.coordinator = s3transfer.crt.CRTTransferCoordinator()
        self.coordinator.set_s3_request(self.mock_s3_request)
        self.future = s3transfer.crt.CRTTransferFuture(
            coordinator=self.coordinator
        )

    def test_set_exception(self):
        self.future.set_exception(CustomFutureException())
        with self.assertRaises(CustomFutureException):
            self.future.result()

    def test_set_exception_raises_error_when_not_done(self):
        self.mock_crt_future.done.return_value = False
        with self.assertRaises(TransferNotDoneError):
            self.future.set_exception(CustomFutureException())

    def test_set_exception_can_override_previous_exception(self):
        self.future.set_exception(Exception())
        self.future.set_exception(CustomFutureException())
        with self.assertRaises(CustomFutureException):
            self.future.result()


@requires_crt
class TestOnBodyFileObjWriter(unittest.TestCase):
    def test_call(self):
        fileobj = io.BytesIO()
        writer = s3transfer.crt.OnBodyFileObjWriter(fileobj)
        writer(chunk=b'content')
        self.assertEqual(fileobj.getvalue(), b'content')
