# Infrastructure/aws/s3/s3_manager.py

"""
S3Manager
=========

This module provides a lightweight wrapper class (S3Manager) for interacting with AWS S3 buckets.

Features:
---------
- Upload files to S3.
- Download files from S3.
- List all files under a prefix (handles pagination).
- Upload JSON objects.
- Delete files.
- Automatic loading of AWS credentials and bucket configuration from the environment.

Use case:
---------
Facilitates common S3 operations without rewriting boilerplate boto3 code.
Designed for projects working with OpenFoodFacts or similar datasets.
"""

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
        Initialize the S3Manager.

        Loads AWS credentials (access key, secret key, region) and bucket name
        automatically using the configuration from `Infrastructure.aws.s3.config.get_s3_config()`.

        Also initializes a boto3 S3 client.
        """
        config = get_s3_config()
        self.bucket = config["bucket"]
        print(f"Loaded bucket: {self.bucket} (type: {type(self.bucket)})")  # 👈 Debug ici
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=config["access_key"],
            aws_secret_access_key=config["secret_key"],
            region_name=config["region"]
        )

    def upload(self, local_path: str, s3_key: str):
        """
        Upload a local file to the configured S3 bucket.

        Args:
            local_path (str): The local filesystem path of the file to upload.
            s3_key (str): The S3 destination key (path inside the bucket).

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
        Download a file from S3 to the local filesystem.

        Args:
            s3_key (str): The S3 key (path) of the file to download.
            local_path (str): The local destination path where the file will be saved.

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
        List all file keys under a given prefix in the S3 bucket.

        Handles AWS pagination automatically (S3 returns at most 1000 keys per call).

        Args:
            prefix (str, optional): The prefix (folder path) to list files from. Defaults to "" (root).

        Returns:
            List[str]: A list of S3 keys (file paths).
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

            print(f"{len(files)} files found under s3://{self.bucket}/{prefix}")
            return files

        except Exception as e:
            print(f"❌ Error listing files: {e}")
            return []

    def upload_json(self, bucket: str, key: str, data: dict):
        """
        Upload a JSON-serializable Python dictionary to S3.

        Args:
            bucket (str): The S3 bucket where the object should be uploaded.
            key (str): The S3 key (path) under which to store the object.
            data (dict): The JSON-serializable Python dictionary to upload.

        Returns:
            None
        """
        self.s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(data, ensure_ascii=False, indent=2).encode('utf-8'),
            ContentType='application/json'
        )

    def delete(self, s3_key: str):
        """
        Delete a file from the S3 bucket.

        Args:
            s3_key (str): The S3 key (path) of the file to delete.

        Returns:
            None
        """
        try:
            self.s3.delete_object(Bucket=self.bucket, Key=s3_key)
            print(f"Deleted: s3://{self.bucket}/{s3_key}")
        except ClientError as e:
            print(f"❌ Error deleting file: {e}")