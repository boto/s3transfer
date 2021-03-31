
import unittest

from botocore.session import Session
from s3transfer.utils import CallArgs

from tests import FileCreator
from tests import requires_crt, HAS_CRT

if HAS_CRT:
    import s3transfer.crt


@requires_crt
class TestBotocoreCRTRequestSerializer(unittest.TestCase):
    def setUp(self):
        self.region = 'us-west-2'
        self.session = Session()
        self.session.set_config_variable('region', self.region)
        self.request_serializer = s3transfer.crt.BotocoreCRTRequestSerializer(
            self.session)
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
        coordinator = s3transfer.crt.CRTTransferCoordinator()
        future = s3transfer.crt.CRTTransferFuture(
            s3transfer.crt.CRTTransferMeta(call_args=callargs),
            coordinator)
        crt_request = self.request_serializer.serialize_http_request(
            "put_object", future)
        self.assertEqual("PUT", crt_request.method)
        self.assertEqual(self.expected_path, crt_request.path)
        self.assertEqual(self.expected_host, crt_request.headers.get("host"))
        self.assertIsNone(crt_request.headers.get("Authorization"))

    def test_download_request(self):
        callargs = CallArgs(
            bucket=self.bucket, key=self.key, fileobj=self.filename,
            extra_args={}, subscribers=[])
        coordinator = s3transfer.crt.CRTTransferCoordinator()
        future = s3transfer.crt.CRTTransferFuture(
            s3transfer.crt.CRTTransferMeta(call_args=callargs),
            coordinator)
        crt_request = self.request_serializer.serialize_http_request(
            "get_object", future)
        self.assertEqual("GET", crt_request.method)
        self.assertEqual(self.expected_path, crt_request.path)
        self.assertEqual(self.expected_host, crt_request.headers.get("host"))
        self.assertIsNone(crt_request.headers.get("Authorization"))

    def test_delete_request(self):
        callargs = CallArgs(
            bucket=self.bucket, key=self.key,
            extra_args={}, subscribers=[])
        coordinator = s3transfer.crt.CRTTransferCoordinator()
        future = s3transfer.crt.CRTTransferFuture(
            s3transfer.crt.CRTTransferMeta(call_args=callargs),
            coordinator)
        crt_request = self.request_serializer.serialize_http_request(
            "delete_object", future)
        self.assertEqual("DELETE", crt_request.method)
        self.assertEqual(self.expected_path, crt_request.path)
        self.assertEqual(self.expected_host, crt_request.headers.get("host"))
        self.assertIsNone(crt_request.headers.get("Authorization"))
