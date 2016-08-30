=========
CHANGELOG
=========

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

