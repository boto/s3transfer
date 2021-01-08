import mock
import unittest
import tempfile
import os

from awscrt.s3 import S3Client, S3RequestType
from botocore.session import Session

from s3transfer.crt import CRTTransferManager

from tests import FileCreator


class TestCRTTransferManager(unittest.TestCase):
    def setUp(self):
        self.s3_crt_client = mock.Mock(S3Client)
        self.session = Session()
        self.transfer_manager = CRTTransferManager(
            crt_s3_client=self.s3_crt_client, session=self.session)
        self.region = 'us-west-2'
        self.session = Session()
        self.session.set_config_variable('region', self.region)
        self.bucket = "test_bucket"
        self.key = "test_key"
        self.files = FileCreator()
        self.filename = self.files.create_file('myfile', 'my content')
        self.expected_path = "/" + self.bucket + "/" + self.key
        self.expected_host = "s3.%s.amazonaws.com" % (self.region)

    def tearDown(self):
        self.files.remove_all()

    def test_upload(self):
        future = self.transfer_manager.upload(
            self.bucket, self.key, self.filename, {}, [])
        future.result()

        callargs = self.s3_crt_client.make_request.call_args
        callargs_kwargs = callargs[1]
        self.assertEqual(callargs_kwargs["send_filepath"], self.filename)
        self.assertIsNone(callargs_kwargs["recv_filepath"])
        self.assertEqual(callargs_kwargs["type"], S3RequestType.PUT_OBJECT)
        crt_request = callargs_kwargs["request"]
        self.assertEqual("PUT", crt_request.method)
        self.assertEqual(self.expected_path, crt_request.path)
        self.assertEqual(self.expected_host, crt_request.headers.get("host"))

    def test_download(self):
        future = self.transfer_manager.download(
            self.bucket, self.key, self.filename, {}, [])
        future.result()

        callargs = self.s3_crt_client.make_request.call_args
        callargs_kwargs = callargs[1]
        self.assertEqual(callargs_kwargs["recv_filepath"], self.filename)
        self.assertIsNone(callargs_kwargs["send_filepath"])
        self.assertEqual(callargs_kwargs["type"], S3RequestType.GET_OBJECT)
        crt_request = callargs_kwargs["request"]
        self.assertEqual("GET", crt_request.method)
        self.assertEqual(self.expected_path, crt_request.path)
        self.assertEqual(self.expected_host, crt_request.headers.get("host"))

    def test_delete(self):
        future = self.transfer_manager.delete(
            self.bucket, self.key, {}, [])
        future.result()

        callargs = self.s3_crt_client.make_request.call_args
        callargs_kwargs = callargs[1]
        self.assertIsNone(callargs_kwargs["send_filepath"])
        self.assertIsNone(callargs_kwargs["recv_filepath"])
        self.assertEqual(callargs_kwargs["type"], S3RequestType.DEFAULT)
        crt_request = callargs_kwargs["request"]
        self.assertEqual("DELETE", crt_request.method)
        self.assertEqual(self.expected_path, crt_request.path)
        self.assertEqual(self.expected_host, crt_request.headers.get("host"))
