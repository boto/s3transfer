# Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the 'License'). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the 'license' file accompanying this file. This file is
# distributed on an 'AS IS' BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.
import hashlib
import math
import os
import shutil
import string
import tempfile

try:
    import unittest2 as unittest
except ImportError:
    import unittest
import botocore.session
from botocore.stub import Stubber
from concurrent import futures

from s3transfer.manager import TransferConfig
from s3transfer.futures import TransferContext
from s3transfer.utils import OSUtils


def assert_files_equal(first, second):
    if os.path.getsize(first) != os.path.getsize(second):
        raise AssertionError("Files are not equal: %s, %s" % (first, second))
    first_md5 = md5_checksum(first)
    second_md5 = md5_checksum(second)
    if first_md5 != second_md5:
        raise AssertionError(
            "Files are not equal: %s(md5=%s) != %s(md5=%s)" % (
                first, first_md5, second, second_md5))


def md5_checksum(filename):
    checksum = hashlib.md5()
    with open(filename, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            checksum.update(chunk)
    return checksum.hexdigest()


def random_bucket_name(prefix='s3transfer', num_chars=10):
    base = string.ascii_lowercase + string.digits
    random_bytes = bytearray(os.urandom(num_chars))
    return prefix + ''.join([base[b % len(base)] for b in random_bytes])


class FileCreator(object):
    def __init__(self):
        self.rootdir = tempfile.mkdtemp()

    def remove_all(self):
        shutil.rmtree(self.rootdir)

    def create_file(self, filename, contents, mode='w'):
        """Creates a file in a tmpdir
        ``filename`` should be a relative path, e.g. "foo/bar/baz.txt"
        It will be translated into a full path in a tmp dir.
        ``mode`` is the mode the file should be opened either as ``w`` or
        `wb``.
        Returns the full path to the file.
        """
        full_path = os.path.join(self.rootdir, filename)
        if not os.path.isdir(os.path.dirname(full_path)):
            os.makedirs(os.path.dirname(full_path))
        with open(full_path, mode) as f:
            f.write(contents)
        return full_path

    def create_file_with_size(self, filename, filesize):
        filename = self.create_file(filename, contents='')
        chunksize = 8192
        with open(filename, 'wb') as f:
            for i in range(int(math.ceil(filesize / float(chunksize)))):
                f.write(b'a' * chunksize)
        return filename

    def append_file(self, filename, contents):
        """Append contents to a file
        ``filename`` should be a relative path, e.g. "foo/bar/baz.txt"
        It will be translated into a full path in a tmp dir.
        Returns the full path to the file.
        """
        full_path = os.path.join(self.rootdir, filename)
        if not os.path.isdir(os.path.dirname(full_path)):
            os.makedirs(os.path.dirname(full_path))
        with open(full_path, 'a') as f:
            f.write(contents)
        return full_path

    def full_path(self, filename):
        """Translate relative path to full path in temp dir.
        f.full_path('foo/bar.txt') -> /tmp/asdfasd/foo/bar.txt
        """
        return os.path.join(self.rootdir, filename)


class NotCallableSubscriber(object):
    def __init__(self):
        self.on_done = 'foo'


class NoKwargsSubscriber(object):
    def on_done(self):
        pass


class RecordingSubscriber(object):
    def __init__(self):
        self.on_queued_calls = []
        self.on_progress_calls = []
        self.on_done_calls = []

    def on_queued(self, **kwargs):
        self.on_queued_calls.append(kwargs)

    def on_progress(self, **kwargs):
        self.on_progress_calls.append(kwargs)

    def on_done(self, **kwargs):
        self.on_done_calls.append(kwargs)

    def calculate_bytes_seen(self, **kwargs):
        amount_seen = 0
        for call in self.on_progress_calls:
            amount_seen += call['bytes_transferred']
        return amount_seen


class StubbedClientTest(unittest.TestCase):
    def setUp(self):
        self.session = botocore.session.get_session()
        self.region = 'us-west-2'
        self.client = self.session.create_client(
            's3', self.region, aws_access_key_id='foo',
            aws_secret_access_key='bar')
        self.stubber = Stubber(self.client)
        self.stubber.activate()

    def tearDown(self):
        self.stubber.deactivate()

    def reset_stubber_with_new_client(self, override_client_kwargs):
        client_kwargs = {
            'service_name': 's3',
            'region_name': self.region,
            'aws_access_key_id': 'foo',
            'aws_secret_access_key': 'bar'
        }
        client_kwargs.update(override_client_kwargs)
        self.client = self.session.create_client(**client_kwargs)
        self.stubber = Stubber(self.client)
        self.stubber.activate()


class BaseTaskTest(StubbedClientTest):
    def setUp(self):
        super(BaseTaskTest, self).setUp()
        self.transfer_context = TransferContext()

    def get_task(self, task_cls, **kwargs):
        if 'transfer_context' not in kwargs:
            kwargs['transfer_context'] = self.transfer_context
        return task_cls(**kwargs)


class BaseTaskSubmitterTest(StubbedClientTest):
    def setUp(self):
        super(BaseTaskSubmitterTest, self).setUp()
        self.config = TransferConfig()
        self.osutil = OSUtils()
        self.executor = futures.ThreadPoolExecutor(1)

    def tearDown(self):
        super(BaseTaskSubmitterTest, self).tearDown()
        self.executor.shutdown()


class BaseGeneralInterfaceTest(StubbedClientTest):
    """A general test class to ensure consistency across TransferManger methods

    This test should never be called and should be subclassed from to pick up
    the various tests that all TransferManager method must pass from a
    functionality standpoint.
    """
    __test__ = False

    def manager(self):
        """The transfer manager to use"""
        raise NotImplementedError('method is not implemented')

    @property
    def method(self):
        """The transfer manager method to invoke i.e. upload()"""
        raise NotImplementedError('method is not implemented')

    @property
    def call_kwargs(self):
        """The kwargs to be passed to the transfer manager method"""
        raise NotImplementedError('call_kwargs is not implemented')

    @property
    def invalid_extra_args(self):
        """A value for extra_args that will cause validation errors"""
        raise NotImplementedError(
            'invalid_extra_args is not implemented')

    @property
    def stubbed_responses(self):
        """A list of stubbed responses that will cause the request to succeed

        The elements of this list is a dictionary that will be used as key
        word arguments to botocore.Stubber.add_response(). For example::

            [{'method': 'put_object', 'service_response': {}}]
        """
        raise NotImplementedError(
            'stubbed_responses is not implemented')

    @property
    def expected_progress_callback_info(self):
        """A list of kwargs expected to be passed to each progress callback

        Note that the future kwargs does not need to be added to each
        dictionary provided in the list. This is injected for you. An example
        is::

            [
                {'bytes_transferred': 4},
                {'bytes_transferred': 4},
                {'bytes_transferred': 2}
            ]

        This indicates that the progress callback will be called three
        times and pass along the specified keyword arguments and corresponding
        values.
        """
        raise NotImplementedError(
            'expected_progress_callback_info is not implemented')

    def test_invalid_subscribers(self):
        with self.assertRaisesRegexp(ValueError, 'must be callable'):
            self.method(
                subscribers=[NotCallableSubscriber()],
                **self.call_kwargs
            )

        with self.assertRaisesRegexp(ValueError, 'must accept keyword'):
            self.method(
                subscribers=[NoKwargsSubscriber()], **self.call_kwargs
            )

    def test_invalid_extra_args(self):
        with self.assertRaisesRegexp(ValueError, 'Invalid extra_args'):
            self.method(
                extra_args=self.invalid_extra_args,
                **self.call_kwargs
            )

    def test_for_callback_kwargs_correctness(self):
        # Add the stubbed responses before invoking the method
        for stubbed_response in self.stubbed_responses:
            self.stubber.add_response(**stubbed_response)

        subscriber = RecordingSubscriber()
        future = self.method(
            subscribers=[subscriber], **self.call_kwargs)
        # We call shutdown instead of result on future because the future
        # could be finished but the done callback could still be going.
        # The manager's shutdown method ensures everything completes.
        self.manager.shutdown()

        # Assert the various subscribers were called with the
        # expected kwargs
        expected_progress_calls = self.expected_progress_callback_info
        for expected_progress_call in expected_progress_calls:
            expected_progress_call['future'] = future

        self.assertEqual(subscriber.on_queued_calls, [{'future': future}])
        self.assertEqual(subscriber.on_progress_calls, expected_progress_calls)
        self.assertEqual(subscriber.on_done_calls, [{'future': future}])
