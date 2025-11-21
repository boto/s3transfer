#!/usr/bin/env python
"""Direct timing of batch downloads without shell wrapper."""

import argparse
import tempfile
import shutil
import time
from botocore.session import get_session
from s3transfer.manager import TransferManager


def create_file(filename, file_size):
    with open(filename, 'wb') as f:
        f.write(b'a' * file_size)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--file-count', type=int, required=True)
    parser.add_argument('--file-size', type=int, required=True)
    parser.add_argument('--s3-bucket', required=True)
    args = parser.parse_args()
    
    session = get_session()
    client = session.create_client('s3')
    
    tempdir = tempfile.mkdtemp()
    s3_keys = []
    
    try:
        # Upload files
        print(f"Uploading {args.file_count} files...")
        with TransferManager(client) as manager:
            for i in range(args.file_count):
                file_path = f"{tempdir}/upload_{i}"
                create_file(file_path, args.file_size)
                s3_key = f"perf_test_{i}"
                manager.upload(file_path, args.s3_bucket, s3_key)
                s3_keys.append(s3_key)
        
        # Download files
        print(f"Downloading {args.file_count} files...")
        start_time = time.time()
        with TransferManager(client) as manager:
            for i, s3_key in enumerate(s3_keys):
                download_path = f"{tempdir}/download_{i}"
                manager.download(args.s3_bucket, s3_key, download_path)
        duration = time.time() - start_time
        
        print(f"Download duration: {duration:.2f} seconds")
        
        # Cleanup
        for s3_key in s3_keys:
            client.delete_object(Bucket=args.s3_bucket, Key=s3_key)
    finally:
        shutil.rmtree(tempdir)


if __name__ == '__main__':
    main()
