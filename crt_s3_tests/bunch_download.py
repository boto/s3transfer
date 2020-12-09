from s3transfer.crt import CRTTransferManager
import botocore.session


s = botocore.session.Session()
crt_manager = CRTTransferManager(s)

dir = "small_files/"

suffix = ".txt"

download_future = []

for i in range(0, 100):
    key = str(i)+suffix
    file_name = dir+str(i)+"_download"+suffix
    download_future.append(crt_manager.download_file(bucket='aws-crt-python-s3-testing-bucket',
                                                     key=key, filename=file_name))


for i in download_future:
    i.result()
