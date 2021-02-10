import mock
import unittest
import threading
import re
from concurrent.futures import Future

from awscrt.s3 import S3Client, S3RequestType, S3Request
import awscrt.exceptions as crtException
from botocore.session import Session

from s3transfer.crt import CRTTransferManager, BotocoreCRTRequestSerializer
from s3transfer.crt import BaseCRTRequestSerializer

from tests import FileCreator


class submitThread(threading.Thread):
    def __init__(self, transfer_manager, futures, callargs):
        threading.Thread.__init__(self)
        self._transfer_manager = transfer_manager
        self._futures = futures
        self._callargs = callargs

    def run(self):
        self._futures.append(self._transfer_manager.download(*self._callargs))


class TestCRTTransferManager(unittest.TestCase):
    def setUp(self):
        self.region = 'us-west-2'
        self.bucket = "test_bucket"
        self.key = "test_key"
        self.files = FileCreator()
        self.filename = self.files.create_file('myfile', 'my content')
        self.expected_path = "/" + self.bucket + "/" + self.key
        self.expected_host = "s3.%s.amazonaws.com" % (self.region)
        self.s3_request = mock.Mock(S3Request)
        self.s3_crt_client = mock.Mock(S3Client)
        self.s3_crt_client.make_request.return_value = self.s3_request
        self.session = Session()
        self.session.set_config_variable('region', self.region)
        self.request_serializer = BotocoreCRTRequestSerializer(self.session)
        self.transfer_manager = CRTTransferManager(
            crt_s3_client=self.s3_crt_client,
            crt_request_serializer=self.request_serializer)

    def tearDown(self):
        self.files.remove_all()

    def test_upload(self):
        future = self.transfer_manager.upload(
            self.filename, self.bucket, self.key, {}, [])
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
        # the recv_filepath will be set to a temporary file path with some
        # random suffix
        self.assertTrue(re.match(self.filename + ".*",
                                 callargs_kwargs["recv_filepath"]))
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

    def test_blocks_when_max_requests_processes_reached(self):
        futures = []
        callargs = (self.bucket, self.key, self.filename, {}, [])
        all_concurrent = 33
        max_request_processes = 32  # the hard coded max processes
        threads = []
        for i in range(0, all_concurrent):
            thread = submitThread(self.transfer_manager, futures, callargs)
            thread.start()
            threads.append(thread)
        self.assertLessEqual(
            self.s3_crt_client.make_request.call_count,
            max_request_processes)
        # Release lock
        callargs = self.s3_crt_client.make_request.call_args
        callargs_kwargs = callargs[1]
        on_done = callargs_kwargs["on_done"]
        on_done(error=None)
        for thread in threads:
            thread.join()
        self.assertEqual(
            self.s3_crt_client.make_request.call_count,
            all_concurrent)

    def _cancel_function(self):
        self.cancel_called = True
        self.s3_request.finished_future.set_exception(
            crtException.from_code(0))
        callargs = self.s3_crt_client.make_request.call_args
        callargs_kwargs = callargs[1]
        on_done = callargs_kwargs["on_done"]
        on_done(error=None)

    def test_cancel(self):
        self.s3_request.finished_future = Future()
        self.cancel_called = False
        self.s3_request.cancel = self._cancel_function
        try:
            with self.transfer_manager:
                future = self.transfer_manager.upload(
                    self.filename, self.bucket, self.key, {}, [])
                raise KeyboardInterrupt()
        except KeyboardInterrupt:
            pass

        with self.assertRaises(crtException.AwsCrtError):
            future.result()
        self.assertTrue(self.cancel_called)
