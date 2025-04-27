# Infrastructure/aws/s3/s3_manager.py

import json

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from Infrastructure.aws.s3.config import get_s3_config


class S3Manager:
    """
    A manager class for interacting with an AWS S3 bucket.

    Provides methods for uploading, downloading, listing, and deleting files.
    Credentials and bucket configuration are automatically loaded from .env.
    """

    def __init__(self):
        """
        Initializes the S3Manager with AWS credentials and bucket info
        loaded from the environment (via get_s3_config).
        """
        config = get_s3_config()
        self.bucket = config["bucket"]
        print(f"🪣 Loaded bucket: {self.bucket} (type: {type(self.bucket)})")  # 👈 Debug ici
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=config["access_key"],
            aws_secret_access_key=config["secret_key"],
            region_name=config["region"]
        )

    def upload(self, local_path: str, s3_key: str):
        """
        Uploads a local file to the S3 bucket.

        Args:
            local_path (str): Path to the file on the local filesystem.
            s3_key (str): Path (key) where the file should be stored in S3.

        Returns:
            None
        """
        try:
            self.s3.upload_file(local_path, self.bucket, s3_key)
            print(f"✅ Uploaded: {local_path} → s3://{self.bucket}/{s3_key}")
        except FileNotFoundError:
            print("❌ File not found:", local_path)
        except NoCredentialsError:
            print("❌ AWS credentials not found!")

    def download(self, s3_key: str, local_path: str):
        """
        Downloads a file from the S3 bucket to the local filesystem.

        Args:
            s3_key (str): The path (key) of the file in S3.
            local_path (str): Path to store the downloaded file locally.

        Returns:
            None
        """
        try:
            self.s3.download_file(self.bucket, s3_key, local_path)
            print(f"✅ Downloaded: s3://{self.bucket}/{s3_key} → {local_path}")
        except ClientError as e:
            print(f"❌ Error: {e}")

    def list(self, prefix: str = ""):
        """
        Lists all files (keys) in the S3 bucket under the given prefix,
        handling AWS pagination (max 1000 per API call).

        Args:
            prefix (str): (Optional) The S3 folder path to filter files.

        Returns:
            List[str]: A list of keys (file paths) in the bucket.
        """
        try:
            files = []
            continuation_token = None

            while True:
                if continuation_token:
                    response = self.s3.list_objects_v2(
                        Bucket=self.bucket,
                        Prefix=prefix,
                        ContinuationToken=continuation_token
                    )
                else:
                    response = self.s3.list_objects_v2(
                        Bucket=self.bucket,
                        Prefix=prefix
                    )

                contents = response.get("Contents", [])
                files.extend(obj["Key"] for obj in contents)

                if response.get("IsTruncated"):  # 🚨 Encore des fichiers à récupérer
                    continuation_token = response.get("NextContinuationToken")
                else:
                    break

            print(f"📁 {len(files)} files found under s3://{self.bucket}/{prefix}")
            return files

        except Exception as e:
            print(f"❌ Error listing files: {e}")
            return []

    def upload_json(self, bucket: str, key: str, data: dict):
        """Upload a JSON object to S3."""
        self.s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(data, ensure_ascii=False, indent=2).encode('utf-8'),
            ContentType='application/json'
        )

    def delete(self, s3_key: str):
        """
        Deletes a file from the S3 bucket.

        Args:
            s3_key (str): Path (key) of the file to delete in S3.

        Returns:
            None
        """
        try:
            self.s3.delete_object(Bucket=self.bucket, Key=s3_key)
            print(f"🗑️ Deleted: s3://{self.bucket}/{s3_key}")
        except ClientError as e:
            print(f"❌ Error deleting file: {e}")