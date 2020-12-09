from s3transfer.crt import CRTTransferManager
import io
from botocore import args
import botocore.awsrequest
import botocore.session
from botocore.utils import CrtUtil
from botocore import UNSIGNED
from botocore.config import Config
from botocore.compat import urlsplit, six
from awscrt.s3 import S3Client, AwsS3RequestType
from awscrt.io import ClientBootstrap, ClientTlsContext, DefaultHostResolver, EventLoopGroup, TlsConnectionOptions, TlsContextOptions, init_logging, LogLevel
from awscrt.auth import AwsCredentialsProvider
from awscrt.http import HttpHeaders, HttpRequest
from urllib3.response import HTTPResponse


s = botocore.session.Session()
crt_manager = CRTTransferManager(s)

dir = "tests_files/"
dir = "small_files/"

write_file = "get_object_test_py_10MB"
read_file = "put_object_test_py_10MB"

suffix = ".txt"

filename = dir+"0.txt"
file = open(filename, "rb")
data = file.read()
print(len(data))

upload_future = crt_manager.upload_file(bucket='aws-crt-canary-bucket',
                                        key='0.txt', filename=dir+"0.txt", extra_args={"data_len": 1048576})

# download_future = crt_manager.download_file(bucket='aws-crt-canary-bucket',
#                                             key='get_object_test_10MB.txt', filename=write_file+suffix)


upload_future.result()
upload_future.result()
# download_future.result()
# s3_upload_request.finished_future.result(1000)
# uploadfile.close()
