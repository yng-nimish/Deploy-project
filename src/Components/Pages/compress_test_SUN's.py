import boto3
import os
import zipfile
import tempfile
import logging
from botocore.exceptions import ClientError

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# AWS S3 configuration
s3_client = boto3.client('s3')
bucket_name = 'my-bucket-parquet-test'
test_folder_prefix = 'April 24/Test4/'
compressed_output_prefix = "Compressed test SUN's/Test4.zip"

def download_s3_folder(bucket, prefix, local_dir):
    """Download all files from an S3 folder to a local directory."""
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get('Contents', []):
                key = obj['Key']
                local_file_path = os.path.join(local_dir, os.path.relpath(key, prefix))
                os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
                logger.info(f"Downloading {key} to {local_file_path}")
                s3_client.download_file(bucket, key, local_file_path)
    except ClientError as e:
        logger.error(f"Error downloading from S3: {e}")
        raise

def create_zip_from_folder(folder_path, zip_path):
    """Create a ZIP file from a folder."""
    try:
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, _, files in os.walk(folder_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, folder_path)
                    zipf.write(file_path, arcname)
                    logger.info(f"Added {file_path} to ZIP as {arcname}")
    except Exception as e:
        logger.error(f"Error creating ZIP file: {e}")
        raise

def upload_to_s3(local_file, bucket, s3_key):
    """Upload a file to S3."""
    try:
        s3_client.upload_file(local_file, bucket, s3_key)
        logger.info(f"Uploaded {local_file} to s3://{bucket}/{s3_key}")
    except ClientError as e:
        logger.error(f"Error uploading to S3: {e}")
        raise

def generate_presigned_url(bucket, s3_key, expiration=3600):
    """Generate a pre-signed URL for an S3 object."""
    try:
        url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket, 'Key': s3_key},
            ExpiresIn=expiration
        )
        return url
    except ClientError as e:
        logger.error(f"Error generating pre-signed URL: {e}")
        return None

def main():
    try:
        # Create temporary directories for downloading and zipping
        with tempfile.TemporaryDirectory() as temp_dir:
            download_dir = os.path.join(temp_dir, 'download')
            os.makedirs(download_dir, exist_ok=True)
            zip_path = os.path.join(temp_dir, 'Test4.zip')  # Fixed zip_path

            # Step 1: Download all files from Test4 folder
            logger.info("Starting download of files from S3...")
            download_s3_folder(bucket_name, test_folder_prefix, download_dir)

            # Step 2: Create ZIP file
            logger.info("Creating ZIP file...")
            create_zip_from_folder(download_dir, zip_path)

            # Step 3: Upload ZIP file to S3
            logger.info("Uploading ZIP file to S3...")
            upload_to_s3(zip_path, bucket_name, compressed_output_prefix)

            # Step 4: Generate pre-signed URL
            logger.info("Generating pre-signed URL...")
            presigned_url = generate_presigned_url(bucket_name, compressed_output_prefix)
            if presigned_url:
                logger.info(f"Pre-signed URL: {presigned_url}")
            else:
                logger.error("Failed to generate pre-signed URL.")
    except Exception as e:
        logger.error(f"Script failed: {e}")
        raise

if __name__ == "__main__":
    main()