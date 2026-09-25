import boto3
import logging
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig
from botocore.config import Config as BotoConfig
from src.config import Config

logger = logging.getLogger("etl_pipeline")

class S3Client:
    def __init__(self):
        if Config.AWS_ACCESS_KEY_ID and Config.AWS_SECRET_ACCESS_KEY:
            self.s3_client = boto3.client(
                "s3",
                aws_access_key_id=Config.AWS_ACCESS_KEY_ID,
                aws_secret_access_key=Config.AWS_SECRET_ACCESS_KEY,
                region_name=Config.AWS_DEFAULT_REGION
            )
        else:
            # Fallback to default IAM role
            self.s3_client = boto3.client("s3", region_name=Config.AWS_DEFAULT_REGION)
        self.bucket = Config.S3_BUCKET_NAME
    
    def upload_file(self, local_path: str, s3_key: str) -> bool:
        if not self.bucket:
            logger.warning("S3 bucket name is not configured.")
            return False
        
        transfer_config = TransferConfig(
            multipart_threshold=50 * 1024 * 1024, # Only use multipart if file > 50MB (default is 8MB)
            max_concurrency=2,                    # Reduce threads (default is 10)
            use_threads=True
        )
        
        try:
            logger.info(f"Uploading file {local_path} to S3 bucket.")
            self.s3_client.upload_file(str(local_path), self.bucket, s3_key, Config=transfer_config)
            logger.info(f"File {local_path} uploaded to S3 bucket {self.bucket}.")
            return True
        except ClientError as e:
            logger.error(f"Failed to upload file {local_path} to S3: {e}")
            return False
    
    def download_file(self, s3_key: str, local_path: str) -> bool:
        if not self.bucket:
            logger.warning("S3 bucket name is not configured.")
            return False
        try:
            self.s3_client.download_file(self.bucket, s3_key, str(local_path))
            logger.info(f"File {s3_key} downloaded from S3 bucket {self.bucket} to {local_path}.")
            return True
        except ClientError as e:
            logger.error(f"Failed to download file {s3_key} from S3: {e}")
            return False