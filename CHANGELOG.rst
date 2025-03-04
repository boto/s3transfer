=========
CHANGELOG
=========

0.11.4
======

* enhancement:Dependencies: Update the floor version of botocore to 1.37.4 to match imports.
* enhancement:Tasks: Pass Botocore context from parent to child threads.


0.11.3
======

* bugfix:``awscrt``: Fix urlencoding issues for request signing with the awscrt.


0.11.2
======

* bugfix:upload: Only set a default checksum if the ``request_checksum_calculation`` config is set to ``when_supported``. Fixes `boto/s3transfer#327 <https://github.com/boto/s3transfer/issues/327>`__.


0.11.1
======

* bugfix:Dependencies: Update the floor version of botocore to 1.36.0 to match imports.


0.11.0
======

* feature:manager: Use CRC32 by default and support user provided full-object checksums.


0.10.4
======

* enhancement:``s3``: Added Multi-Region Access Points support to CRT transfers.


0.10.3
======

* enhancement:Python: Added provisional Python 3.13 support to s3transfer


0.10.2
======

* bugfix:``awscrt``: Pass operation name to awscrt.s3 to improve error handling.


0.10.1
======

* bugfix:``urllib3``: Fixed retry handling for IncompleteRead exception raised by urllib3 2.x during data transfer


0.10.0
======

* feature:``s3``: Added CRT support for S3 Express One Zone


0.9.0
=====

* feature:Python: End of support for Python 3.7


0.8.2
=====

* bugfix:Subscribers: Added caching for Subscribers to improve throughput by up to 24% in high volume transfer


0.8.1
=====

* enhancement:``s3``: Added support for defaulting checksums to CRC32 for s3express.


0.8.0
=====

* enhancement:``crt``: Automatically configure CRC32 checksums for uploads and checksum validation for downloads through the CRT transfer manager.
* feature:``crt``: S3transfer now supports a wider range of CRT functionality for uploads to improve throughput in the CLI/Boto3.
* enhancement:``Botocore``: S3Transfer now requires Botocore >=1.32.7
* enhancement:``crt``: Update ``target_throughput`` defaults. If not configured, s3transfer will use the AWS CRT to attempt to determine a recommended target throughput to use based on the system. If there is no recommended throughput, s3transfer now falls back to ten gigabits per second.
* enhancement:``crt``: Add support for uploading and downloading file-like objects using CRT transfer manager. It supports both seekable and non-seekable file-like objects.


0.7.0
=====

* feature:``SSE-C``: Pass SSECustomer* arguments to CompleteMultipartUpload for upload operations


0.6.2
=====

* enhancement:Python: Added provisional Python 3.12 support to s3transfer


0.6.1
=====

* bugfix:copy: Added support for ``ChecksumAlgorithm`` when uploading copy data in parts.


0.6.0
=====

* feature:Python: Dropped support for Python 3.6


0.5.2
=====

* enhancement:``s3``: Added support for flexible checksums when uploading or downloading objects.


0.5.1
=====

* enhancement:Python: Officially add Python 3.10 support


0.5.0
=====

* feature:Python: Dropped support for Python 2.7


0.4.2
=====

* enhancement:s3: Add support for ``ExpectedBucketOwner``. Fixes `#181 <https://github.com/boto/s3transfer/issues/181>`__.


0.4.1
=====

* enhancement:``crt``: Add ``set_exception`` to ``CRTTransferFuture`` to allow setting exceptions in subscribers.


0.4.0
=====

* feature:``crt``: Add optional AWS Common Runtime (CRT) support. The AWS CRT provides a C-based S3 transfer client that can improve transfer throughput.


0.3.7
=====

* bugfix:ReadFileChunk: Fix seek behavior in ReadFileChunk class


0.3.6
=====

* bugfix:packaging: Fix setup.py metadata for `futures` on Python 2.7


0.3.5
=====

* enhancement:``s3``: Block TransferManager methods for S3 Object Lambda resources


0.3.4
=====

* enhancement:s3: Add server side encryption context into allowed list


0.3.3
=====

* bugfix:dependency: Updated botocore version range to allow for developmental installs.


0.3.2
=====

* bugfix:s3: Fixes boto/botocore`#1916 <https://github.com/boto/botocore/issues/1916>`__


0.3.1
=====

* enhancement:``TransferManager``: Expose ``client`` and ``config`` properties
* enhancement:Tags: Add support for ``Tagging`` and ``TaggingDirective``


0.3.0
=====

* feature:Python: Dropped support for Python 2.6 and 3.3.


0.2.1
=====

* enhancment:ProcessPool: Adds user agent suffix.


0.2.0
=====

* feature:``ProcessPoolDownloader``: Add ``ProcessPoolDownloader`` class to speed up download throughput by using processes instead of threads.


0.1.13
======

* bugfix:``RequestPayer``: Plumb ``RequestPayer` argument to the ``CompleteMultipartUpload` operation (`#103 <https://github.com/boto/s3transfer/issues/103>`__).


0.1.12
======

* enhancement:``max_bandwidth``: Add ability to set maximum bandwidth consumption for streaming of S3 uploads and downloads


0.1.11
======

* bugfix:TransferManager: Properly handle unicode exceptions in the context manager. Fixes `#85 <https://github.com/boto/boto3/issues/85>`__


0.1.10
======

* feature:``TransferManager``: Expose ability to use own executor class for ``TransferManager``


0.1.9
=====

* feature:``TransferFuture``: Add support for setting exceptions on transfer future


0.1.8
=====

* feature:download: Support downloading to FIFOs.


0.1.7
=====

* bugfix:TransferManager: Fix memory leak when using same client to create multiple TransferManagers


0.1.6
=====

* bugfix:download: Fix issue where S3 Object was not downloaded to disk when empty


0.1.5
=====

* bugfix:Cntrl-C: Fix issue of hangs when Cntrl-C happens for many queued transfers
* feature:cancel: Expose messages for cancels


0.1.4
=====

* feature:chunksize: Automatically adjust the chunksize if it doesn't meet S3s requirements.
* bugfix:Download: Add support for downloading to special UNIX file by name


0.1.3
=====

* feature:delete: Add a ``.delete()`` method to the transfer manager.
* bugfix:seekable upload: Fix issue where seeked position of seekable file for a nonmultipart upload was not being taken into account.


0.1.2
=====

* bugfix:download: Patch memory leak related to unnecessarily holding onto futures for downloads.


0.1.1
=====

* bugfix:deadlock: Fix deadlock issue described here: https://bugs.python.org/issue20319 with using concurrent.futures.wait


0.1.0
=====

* feature:copy: Add support for managed copies.
* feature:download: Add support for downloading to a filename, seekable file-like object, and nonseekable file-like object.
* feature:general: Add ``TransferManager`` class. All public functionality for ``s3transfer`` is exposed through this class.
* feature:subscribers: Add subscriber interface. Currently supports on_queued, on_progress, and on_done status changes.
* feature:upload: Add support for uploading a filename, seekable file-like object, and nonseekable file-like object.


0.0.1
=====

* feature:manager: Add boto3 s3 transfer logic to package. (`issue 2 <https://github.com/boto/s3transfer/pull/2>`__)

