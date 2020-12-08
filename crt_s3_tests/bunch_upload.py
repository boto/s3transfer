from s3transfer.crt import CRTTransferManager
import botocore.session

s = botocore.session.Session()
crt_manager = CRTTransferManager(s)

dir = "small_files/"

write_file = "get_object_test_py_10MB"
read_file = "put_object_test_py_10MB"

suffix = ".txt"


upload_future = []

for i in range(0, 100):
    key = str(i)+suffix
    file_name = dir+key
    upload_future.append(crt_manager.upload_file(bucket='aws-crt-python-s3-testing-bucket',
                                                 key=key, filename=file_name, extra_args={"data_len": 1048576}))


print("waiting for the future")

for i in upload_future:
    i.result()
