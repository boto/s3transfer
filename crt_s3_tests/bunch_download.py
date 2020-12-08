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
# s3_upload_request.finished_future.result(1000)
# uploadfile.close()
