from datetime import datetime
import os
import boto3
from botocore.exceptions import NoCredentialsError


def load_data_on_s3(**kwargs):
    """Loads a processed CSV file from local storage and uploads it to an S3 bucket.

    Args:
        **kwargs: Keyword arguments including:
            - `keyword` (str): The keyword associated with the file.
            - `bucket` (str): The S3 bucket name to which the file will be uploaded.

    Returns:
        None
    """
    keyword = kwargs["keyword"]
    bucket = kwargs["bucket"]

    date = datetime.now().strftime("%Y%m%d")
    path_csv_file = (
        f"/opt/airflow/data/cleaned_data/Youtube_data_{keyword}_{date}_cleaned"
    )

    s3_file = f"/processed-data/Youtube_data_{keyword}_{date}.csv"
    path_csv_file = f"/opt/airflow/data/extracted_data/Youtube_data_{keyword}_{date}"

    save_data_on_s3(path_csv_file, bucket, s3_file)


def save_data_on_s3(local_file, bucket, s3_file):
    """Uploads a local file to an S3 bucket.

    Args:
        local_file (str): Path to the local file to be uploaded.
        bucket (str): Name of the S3 bucket.
        s3_file (str): Target path and filename on S3.

    Returns:
        bool: True if the upload was successful, False if an error occurred.
    """
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    try:
        s3_client.upload_file(local_file, bucket, s3_file)
        print(f"Upload Successful: {local_file} to s3://{bucket}/{s3_file}")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False
