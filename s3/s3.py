# Imorts
from concurrent.futures.process import _ExceptionWithTraceback
import os
import boto3
import logging
from botocore import UNSIGNED
from botocore.client import Config

# Setup Logging
s3_logger = logging.getLogger(__name__)
s3_logger.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s:%(name)s:%(levelname)s:%(message)s")
file_handler = logging.FileHandler("log_files/s3.log")
file_handler.setFormatter(formatter)
s3_logger.addHandler(file_handler)

class bucket():

    def __init__(self):
        self.bucket_name = os.getenv("BUCKET_NAME")
        self.signature_version = UNSIGNED
        self.s3 = boto3.client('s3', config=Config(signature_version=self.signature_version))

    def fetch(self,file_category):
        try:
            response = self.s3.list_objects(Bucket=self.bucket_name, Prefix=f"{file_category}_data")
            return response
        
        except Exception as e:
            s3_logger.critical(f"FetchBucketError: failed to fetch bucket content, {e}")


    def download(self,file_category,src_name="",dst_name=""):
        try:
            self.s3.download_file(self.bucket_name, f"{file_category}_data/{src_name}", f"{dst_name}")
        
        except Exception as e:
            s3_logger.warning(f"DownloadBucketError: failed to download file from bucket, {e}")

    def upload(self,user_id,src_folder=None,dst_name=None):
        try:
            files = os.listdir(src_folder)
            for file in files:
                file_path = f"{src_folder}/{file}"
                self.s3.upload_file(file_path,self.bucket_name,f"{dst_name}/{user_id}/{file}")
        except Exception as e:
            s3_logger.warning(f"ExportBucketError: failed to upload files to bucket, {e}")
