S3 Acceptance Tests
===================

List of all of the various scenarios that need to be handled in implementing
a S3 transfer manager.

Upload Tests
------------

General
~~~~~~~
* [x] Upload single nonmultipart file
* [x] Upload single multipart file
* [x] Upload multiple nonmultipart files
* [x] Upload multiple multipart files
* [x] Failed/cancelled multipart upload is aborted and leaves no orphaned parts especially for:

  * [x] Failure of ``UploadPart``
  * [x] Failure of ``CompleteMultipartUpload``
  * [x] Failure unrelated to making an API call during upload such as read failure

* [ ] Ctrl-C of any upload does not hang and the wait time is ``avg(transfer_time_iter_chunk) * some_margin``
* [ ] Upload empty file
* [ ] Upload nonseekable nonmultipart binary stream
* [ ] Upload nonseekable multipart binary stream


Region
~~~~~~
* [ ] Provide no or incorrect region for sig4 and be able to redirect request in fewest amount of calls as possible for multipart upload.


Validation
~~~~~~~~~~
* [ ] Before upload, validate upload size of file is less than 5 TB.
* [ ] Before upload, modify chunksize to an acceptable size when needed:

  * [ ] Make chunksize 5 MB when the provided chunksize is less
  * [ ] Make chunksize 5 GB when the provided chunksize is more
  * [ ] Increase chunksize till the maximum number of parts for multipart upload is less than or equal to 10,000 parts

* [ ] Before upload, ensure upload is nonmultipart if the file size is less than 5 MB no matter the provided multipart threshold.


Extra Parameters
~~~~~~~~~~~~~~~~
* [ ] Upload multipart and nonmultipart file with any of the following properties:

  * [x] ACL's
  * [x] CacheControl
  * [x] ContentDisposition
  * [x] ContentEncoding
  * [x] ContentLanguage
  * [x] ContentType
  * [x] Expires
  * [x] Metadata
  * [x] Grants
  * [x] StorageClass
  * [x] SSE (including KMS)
  * [ ] Website Redirect

* [x] Upload multipart and nonmultipart file with a sse-c key
* [x] Upload multipart and nonmultipart file with requester pays


Performance
~~~~~~~~~~~
* [ ] Maximum memory usage does not grow linearly with linearly increasing file size for any upload.
* [ ] Maximum memory usage does not grow linearly with linearly increasing number of uploads.


Download Tests
--------------

General
~~~~~~~
* [x] Download single nonmultipart object
* [x] Download single multipart object
* [x] Download multiple nonmultipart objects
* [x] Download multiple multipart objects
* [x] Download of any object is written to temporary file and renamed to final filename once the object is completely downloaded
* [x] Failed downloads of any object cleans up temporary file
* [x] Provide a transfer size for any download in lieu of using HeadObject
* [ ] Ctrl-C of any download does not hang and the wait time is ``avg(transfer_time_iter_chunk) * some_margin``
* [ ] Download nonmultipart object as nonseekable binary stream
* [ ] Download multipart object as nonseekable binary stream


Region
~~~~~~
* [ ] Provide no or incorrect region for sig4 and be able to redirect request in fewest amount of calls as possible for multipart download.


Retry Logic
~~~~~~~~~~~
* [x] Retry on connection related errors when downloading data
* [ ] Compare MD5 to ``ETag`` and retry for mismatches if all following scenarios are met:
      
  * If MD5 is available
  * Response does not have a ``ServerSideEncryption`` header equal to ``aws:kms``
  * Response does not have ``SSECustomerAlgorithm``
  * ``ETag`` does not have ``-`` in its value indicating a multipart transfer


Extra Parameters
~~~~~~~~~~~~~~~~
* [x] Download an object of a specific version
* [x] Download an object encrypted with sse-c
* [x] Download an object using requester pays


Performance
~~~~~~~~~~~
* [ ] Maximum memory usage does not grow linearly with linearly increasing file size for any download.
* [ ] Maximum memory usage does not grow linearly with linearly increasing number of downloads.


Copy Tests
----------

General
~~~~~~~
* [x] Copy single nonmultipart object
* [x] Copy single multipart object
* [x] Copy multiple nonmultipart objects
* [x] Copy multiple multipart objects
* [x] Provide a transfer size for any copy in lieu of using HeadObject.
* [x] Failed/cancelled multipart copy is aborted and leaves no orphaned parts
* [ ] Ctrl-C of any copy does not hang and the wait time is ``avg(transfer_time_iter_chunk) * some_margin``


Region
~~~~~~
* [ ] Provide no or incorrect region for sig4 and be able to redirect request in fewest amount of calls as possible for multipart copy.


Validation
~~~~~~~~~~
* [ ] Before copy, modify chunksize to an acceptable size when needed:

  * [ ] Make chunksize 5 MB when the provided chunksize is less
  * [ ] Make chunksize 5 GB when the provided chunksize is more
  * [ ] Increase chunksize till the maximum number of parts for multipart copy is less than or equal to 10,000 parts

* [ ] Before copy, ensure copy is nonmultipart if the file size is less than 5 MB no matter the provided multipart threshold.


Extra Parameters
~~~~~~~~~~~~~~~~
* [ ] Copy multipart and nonmultipart file with any of the following properties:
  
  * [x] ACL's
  * [x] CacheControl
  * [x] ContentDisposition
  * [x] ContentEncoding
  * [x] ContentLanguage
  * [x] ContentType
  * [x] Expires
  * [x] Metadata
  * [x] Grants
  * [x] StorageClass
  * [x] SSE (including KMS)
  * [ ] Website Redirect

* [x] Copy multipart and nonmultipart copies with copy source parameters:

  * [x] CopySourceIfMatch
  * [x] CopySourceIfModifiedSince
  * [x] CopySourceIfNoneMatch
  * [x] CopySourceIfUnmodifiedSince

* [x] Copy nonmultipart object with metadata directive and do not use metadata directive for multipart object
* [x] Copy multipart and nonmultipart objects of a specific version
* [x] Copy multipart and nonmultipart objects using requester pays
* [x] Copy multipart and nonmultipart objects using a sse-c key
* [x] Copy multipart and nonmultipart objects using a copy source sse-c key
* [x] Copy multipart and nonmultipart objects using a copy source sse-c key and sse-c key


Cross-Bucket
~~~~~~~~~~~~
* [ ] Copy single nonmultipart object across sigv4 regions
* [ ] Copy single multipart object across sigv4 regions
