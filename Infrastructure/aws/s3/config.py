# Infrastructure/aws/s3/config.py

import os
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv(dotenv_path=".env")

def get_s3_config():
    """
    Loads AWS credentials and S3 bucket configuration from the environment.

    Returns:
        dict: A dictionary containing S3 configuration parameters:
            - bucket (str): The name of the S3 bucket
            - region (str): AWS region name (e.g., "eu-west-3")
            - access_key (str): AWS access key ID
            - secret_key (str): AWS secret access key
    """
    return {
        "bucket": os.getenv("S3_BUCKET"),
        "region": os.getenv("AWS_REGION"),
        "access_key": os.getenv("AWS_ACCESS_KEY_ID"),
        "secret_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    }