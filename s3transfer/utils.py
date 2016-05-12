# Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import random
import time
import functools
import os
import string
import logging

from s3transfer.compat import rename_file


logger = logging.getLogger(__name__)


def unique_id(name):
    """
    Generate a unique ID that includes the given name,
    a timestamp and a random number.
    """
    return '{0}-{1}-{2}'.format(name, int(time.time()),
                                random.randint(0, 10000))


def random_file_extension(num_digits=8):
    return ''.join(random.choice(string.hexdigits) for _ in range(num_digits))


def disable_upload_callbacks(request, operation_name, **kwargs):
    if operation_name in ['PutObject', 'UploadPart'] and \
            hasattr(request.body, 'disable_callback'):
        request.body.disable_callback()


def enable_upload_callbacks(request, operation_name, **kwargs):
    if operation_name in ['PutObject', 'UploadPart'] and \
            hasattr(request.body, 'enable_callback'):
        request.body.enable_callback()


def get_callbacks(transfer_future, callback_type):
    """Retrieves callbacks from a subscriber

    :type transfer_future: s3transfer.futures.TransferFuture
    :param transfer_future: The transfer future the subscriber is associated
        to.

    :type callback_type: str
    :param callback_type: The type of callback to retrieve from the subscriber.
        Valid types include:
            * 'queued'
            * 'progress'
            * 'done'

    :returns: A list of callbacks for the type specified. All callbacks are
        preinjected with the transfer future.
    """
    callbacks = []
    for subscriber in transfer_future.meta.call_args.subscribers:
        callback_name = 'on_' + callback_type
        if hasattr(subscriber, callback_name):
            callbacks.append(
                functools.partial(
                    getattr(subscriber, callback_name),
                    future=transfer_future
                )
            )
    return callbacks


def invoke_progress_callbacks(callbacks, bytes_transferred):
    """Calls all progress callbacks

    :param callbacks: A list of progress callbacks to invoke
    :param bytes_transferred: The number of bytes transferred. This is passed
        to the callbacks. If no bytes were transferred the callbacks will not
        be invoked because no progress was achieved
    """
    # Only invoke the callbacks if bytes were actually transferred.
    if bytes_transferred:
        for callback in callbacks:
            callback(bytes_transferred=bytes_transferred)


class CallArgs(object):
    def __init__(self, **kwargs):
        """A class that records call arguments

        The call arguments must be passed as keyword arguments. It will set
        each keyword argument as an attribute of the object along with its
        associated value.
        """
        for arg, value in kwargs.items():
            setattr(self, arg, value)


class OSUtils(object):
    def get_file_size(self, filename):
        return os.path.getsize(filename)

    def open_file_chunk_reader(self, filename, start_byte, size, callbacks):
        return ReadFileChunk.from_filename(filename, start_byte,
                                           size, callbacks,
                                           enable_callbacks=False)

    def open(self, filename, mode):
        return open(filename, mode)

    def remove_file(self, filename):
        """Remove a file, noop if file does not exist."""
        # Unlike os.remove, if the file does not exist,
        # then this method does nothing.
        try:
            os.remove(filename)
        except OSError:
            pass

    def rename_file(self, current_filename, new_filename):
        rename_file(current_filename, new_filename)


class ReadFileChunk(object):
    def __init__(self, fileobj, start_byte, chunk_size, full_file_size,
                 callbacks=None, enable_callbacks=True):
        """

        Given a file object shown below:

            |___________________________________________________|
            0          |                 |                 full_file_size
                       |----chunk_size---|
                 start_byte

        :type fileobj: file
        :param fileobj: File like object

        :type start_byte: int
        :param start_byte: The first byte from which to start reading.

        :type chunk_size: int
        :param chunk_size: The max chunk size to read.  Trying to read
            pass the end of the chunk size will behave like you've
            reached the end of the file.

        :type full_file_size: int
        :param full_file_size: The entire content length associated
            with ``fileobj``.

        :type callbacks: A list of function(amount_read)
        :param callbacks: Called whenever data is read from this object in the
            order provided.

        """
        self._fileobj = fileobj
        self._start_byte = start_byte
        self._size = self._calculate_file_size(
            self._fileobj, requested_size=chunk_size,
            start_byte=start_byte, actual_file_size=full_file_size)
        self._fileobj.seek(self._start_byte)
        self._amount_read = 0
        self._callbacks = callbacks
        if callbacks is None:
            self._callbacks = []
        self._callbacks_enabled = enable_callbacks

    @classmethod
    def from_filename(cls, filename, start_byte, chunk_size, callbacks=None,
                      enable_callbacks=True):
        """Convenience factory function to create from a filename.

        :type start_byte: int
        :param start_byte: The first byte from which to start reading.

        :type chunk_size: int
        :param chunk_size: The max chunk size to read.  Trying to read
            pass the end of the chunk size will behave like you've
            reached the end of the file.

        :type full_file_size: int
        :param full_file_size: The entire content length associated
            with ``fileobj``.

        :type callbacks: function(amount_read)
        :param callbacks: Called whenever data is read from this object.

        :type enable_callbacks: bool
        :param enable_callbacks: Indicate whether to invoke callback
            during read() calls.

        :rtype: ``ReadFileChunk``
        :return: A new instance of ``ReadFileChunk``

        """
        f = open(filename, 'rb')
        file_size = os.fstat(f.fileno()).st_size
        return cls(f, start_byte, chunk_size, file_size, callbacks,
                   enable_callbacks)

    def _calculate_file_size(self, fileobj, requested_size, start_byte,
                             actual_file_size):
        max_chunk_size = actual_file_size - start_byte
        return min(max_chunk_size, requested_size)

    def read(self, amount=None):
        if amount is None:
            amount_to_read = self._size - self._amount_read
        else:
            amount_to_read = min(self._size - self._amount_read, amount)
        data = self._fileobj.read(amount_to_read)
        self._amount_read += len(data)
        if self._callbacks is not None and self._callbacks_enabled:
            invoke_progress_callbacks(self._callbacks, len(data))
        return data

    def enable_callback(self):
        self._callbacks_enabled = True

    def disable_callback(self):
        self._callbacks_enabled = False

    def seek(self, where):
        self._fileobj.seek(self._start_byte + where)
        if self._callbacks is not None and self._callbacks_enabled:
            # To also rewind the callback() for an accurate progress report
            invoke_progress_callbacks(
                self._callbacks, bytes_transferred=where - self._amount_read)
        self._amount_read = where

    def close(self):
        self._fileobj.close()

    def tell(self):
        return self._amount_read

    def __len__(self):
        # __len__ is defined because requests will try to determine the length
        # of the stream to set a content length.  In the normal case
        # of the file it will just stat the file, but we need to change that
        # behavior.  By providing a __len__, requests will use that instead
        # of stat'ing the file.
        return self._size

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.close()

    def __iter__(self):
        # This is a workaround for http://bugs.python.org/issue17575
        # Basically httplib will try to iterate over the contents, even
        # if its a file like object.  This wasn't noticed because we've
        # already exhausted the stream so iterating over the file immediately
        # stops, which is what we're simulating here.
        return iter([])


class StreamReaderProgress(object):
    """Wrapper for a read only stream that adds progress callbacks."""
    def __init__(self, stream, callbacks=None):
        self._stream = stream
        self._callbacks = callbacks
        if callbacks is None:
            self._callbacks = []

    def read(self, *args, **kwargs):
        value = self._stream.read(*args, **kwargs)
        invoke_progress_callbacks(self._callbacks, len(value))
        return value
