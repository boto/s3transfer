=========
CHANGELOG
=========

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

