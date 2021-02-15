
import unittest
import mock

from botocore.session import Session
from awscrt.s3 import S3Client
from s3transfer.crt import BotocoreCRTRequestSerializer, create_s3_crt_client
from s3transfer.crt import CRTTransferCoordinator, CRTTransferFuture, CRTTransferMeta
from s3transfer.utils import CallArgs, OSUtils

from tests import FileCreator


class TestBotocoreCRTRequestSerializer(unittest.TestCase):
    def setUp(self):
        self.region = 'us-west-2'
        self.session = Session()
        self.session.set_config_variable('region', self.region)
        self.request_serializer = BotocoreCRTRequestSerializer(self.session)
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
            bucket=self.bucket, key=self.key, fileobj=self.filename,
            extra_args={}, subscribers=[])
        coordinator = CRTTransferCoordinator()
        future = CRTTransferFuture(
            CRTTransferMeta(call_args=callargs),
            coordinator)
        crt_request = self.request_serializer.serialize_http_request(
            "put_object", future)
        self.assertEqual("PUT", crt_request.method)
        self.assertEqual(self.expected_path, crt_request.path)
        self.assertEqual(self.expected_host, crt_request.headers.get("host"))

    def test_download_request(self):
        callargs = CallArgs(
            bucket=self.bucket, key=self.key, fileobj=self.filename,
            extra_args={}, subscribers=[])
        coordinator = CRTTransferCoordinator()
        future = CRTTransferFuture(
            CRTTransferMeta(call_args=callargs),
            coordinator)
        crt_request = self.request_serializer.serialize_http_request(
            "get_object", future)
        self.assertEqual("GET", crt_request.method)
        self.assertEqual(self.expected_path, crt_request.path)
        self.assertEqual(self.expected_host, crt_request.headers.get("host"))

    def test_delete_request(self):
        callargs = CallArgs(
            bucket=self.bucket, key=self.key,
            extra_args={}, subscribers=[])
        coordinator = CRTTransferCoordinator()
        future = CRTTransferFuture(
            CRTTransferMeta(call_args=callargs),
            coordinator)
        crt_request = self.request_serializer.serialize_http_request(
            "delete_object", future)
        self.assertEqual("DELETE", crt_request.method)
        self.assertEqual(self.expected_path, crt_request.path)
        self.assertEqual(self.expected_host, crt_request.headers.get("host"))


class TestBotocoreCRTRequestSerializer(unittest.TestCase):
    def setUp(self):
        self.region = 'us-west-2'
        self.session = Session()
        self.session.set_config_variable('region', self.region)
        self.request_serializer = BotocoreCRTRequestSerializer(self.session)
        self.bucket = "test_bucket"
        self.key = "test_key"
        self.files = FileCreator()
        self.filename = self.files.create_file('myfile', 'my content')
        self.expected_path = "/" + self.bucket + "/" + self.key
        self.expected_host = "s3.%s.amazonaws.com" % (self.region)

    def test_upload_request(self):
        callargs = CallArgs(
            bucket=self.bucket, key=self.key, fileobj=self.filename,
            extra_args={}, subscribers=[])
        coordinator = CRTTransferCoordinator()
        future = CRTTransferFuture(
            CRTTransferMeta(call_args=callargs),
            coordinator)
        crt_request = self.request_serializer.serialize_http_request(
            "put_object", future)
        self.assertEqual("PUT", crt_request.method)
        self.assertEqual(self.expected_path, crt_request.path)
        self.assertEqual(self.expected_host, crt_request.headers.get("host"))

    def test_upload_request(self):
        callargs = CallArgs(
            bucket=self.bucket, key=self.key, fileobj=self.filename,
            extra_args={}, subscribers=[])
        coordinator = CRTTransferCoordinator()
        future = CRTTransferFuture(
            CRTTransferMeta(call_args=callargs),
            coordinator)
        crt_request = self.request_serializer.serialize_http_request(
            "put_object", future)
        self.assertEqual("PUT", crt_request.method)
        self.assertEqual(self.expected_path, crt_request.path)
        self.assertEqual(self.expected_host, crt_request.headers.get("host"))

    def test_download_request(self):
        callargs = CallArgs(
            bucket=self.bucket, key=self.key, fileobj=self.filename,
            extra_args={}, subscribers=[])
        coordinator = CRTTransferCoordinator()
        future = CRTTransferFuture(
            CRTTransferMeta(call_args=callargs),
            coordinator)
        crt_request = self.request_serializer.serialize_http_request(
            "get_object", future)
        self.assertEqual("GET", crt_request.method)
        self.assertEqual(self.expected_path, crt_request.path)
        self.assertEqual(self.expected_host, crt_request.headers.get("host"))

    def test_delete_request(self):
        callargs = CallArgs(
            bucket=self.bucket, key=self.key,
            extra_args={}, subscribers=[])
        coordinator = CRTTransferCoordinator()
        future = CRTTransferFuture(
            CRTTransferMeta(call_args=callargs),
            coordinator)
        crt_request = self.request_serializer.serialize_http_request(
            "delete_object", future)
        self.assertEqual("DELETE", crt_request.method)
        self.assertEqual(self.expected_path, crt_request.path)
        self.assertEqual(self.expected_host, crt_request.headers.get("host"))
