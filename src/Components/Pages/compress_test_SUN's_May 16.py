import boto3
import os
import zipfile
import tempfile
import logging
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError
from datetime import datetime
import sys

# Configure logging with more detailed format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f's3_zipper_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    ]
)
logger = logging.getLogger(__name__)

# AWS S3 configuration
try:
    s3_client = boto3.client('s3')
except (NoCredentialsError, PartialCredentialsError) as e:
    logger.error(f"Failed to initialize S3 client: {e}")
    sys.exit(1)

bucket_name = 'my-bucket-parquet-test'
base_prefix = 'April 24/'
compressed_output_base = "Compressed test SUN's/"

def validate_config():
    """Validate S3 configuration and bucket accessibility."""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        logger.info(f"Successfully validated access to bucket: {bucket_name}")
    except ClientError as e:
        logger.error(f"Cannot access bucket {bucket_name}: {e}")
        raise

def list_test_folders(bucket, prefix):
    """List all test folders under the given prefix."""
    try:
        folders = set()
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/'):
            for common_prefix in page.get('CommonPrefixes', []):
                folder = common_prefix['Prefix']
                folders.add(folder)
        logger.info(f"Found {len(folders)} test folders under {prefix}")
        return sorted(folders)
    except ClientError as e:
        logger.error(f"Error listing folders in {prefix}: {e}")
        raise

def download_s3_folder(bucket, prefix, local_dir):
    """Download all files from an S3 folder to a local directory."""
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        total_files = 0
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            objects = page.get('Contents', [])
            if not objects:
                logger.warning(f"No files found in {prefix}")
                continue
            for obj in objects:
                key = obj['Key']
                local_file_path = os.path.join(local_dir, os.path.relpath(key, prefix))
                os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
                logger.debug(f"Downloading {key} to {local_file_path}")
                try:
                    s3_client.download_file(bucket, key, local_file_path)
                    total_files += 1
                except ClientError as e:
                    logger.error(f"Failed to download {key}: {e}")
                    raise
        logger.info(f"Downloaded {total_files} files from {prefix}")
        return total_files
    except ClientError as e:
        logger.error(f"Error downloading from S3 folder {prefix}: {e}")
        raise

def create_zip_from_folder(folder_path, zip_path):
    """Create a ZIP file from a folder."""
    try:
        total_size = 0
        file_count = 0
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED, compresslevel=9) as zipf:
            for root, _, files in os.walk(folder_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, folder_path)
                    zipf.write(file_path, arcname)
                    file_size = os.path.getsize(file_path)
                    total_size += file_size
                    file_count += 1
                    logger.debug(f"Added {file_path} to ZIP as {arcname} ({file_size} bytes)")
        logger.info(f"Created ZIP with {file_count} files, total size: {total_size:,} bytes")
        return file_count, total_size
    except (zipfile.BadZipFile, OSError) as e:
        logger.error(f"Error creating ZIP file: {e}")
        raise

def upload_to_s3(local_file, bucket, s3_key):
    """Upload a file to S3 with progress tracking."""
    try:
        file_size = os.path.getsize(local_file)
        logger.debug(f"Uploading {local_file} ({file_size:,} bytes) to s3://{bucket}/{s3_key}")
        s3_client.upload_file(local_file, bucket, s3_key)
        logger.info(f"Successfully uploaded {local_file} to s3://{bucket}/{s3_key}")
    except (ClientError, FileNotFoundError) as e:
        logger.error(f"Error uploading {local_file} to S3: {e}")
        raise

def generate_presigned_url(bucket, s3_key, expiration=3600):
    """Generate a pre-signed URL for an S3 object."""
    try:
        url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket, 'Key': s3_key},
            ExpiresIn=expiration
        )
        logger.info(f"Generated pre-signed URL for {s3_key} (expires in {expiration} seconds)")
        return url
    except ClientError as e:
        logger.error(f"Error generating pre-signed URL for {s3_key}: {e}")
        return None

def process_folder(folder_prefix, temp_dir):
    """Process a single test folder."""
    try:
        folder_name = folder_prefix.rstrip('/').split('/')[-1]
        download_dir = os.path.join(temp_dir, 'download', folder_name)
        zip_path = os.path.join(temp_dir, f'{folder_name}.zip')
        output_key = os.path.join(compressed_output_base, f'{folder_name}.zip')

        logger.info(f"Processing folder: {folder_prefix}")

        # Download files
        os.makedirs(download_dir, exist_ok=True)
        file_count = download_s3_folder(bucket_name, folder_prefix, download_dir)
        if file_count == 0:
            logger.warning(f"Skipping empty folder: {folder_prefix}")
            return None

        # Create ZIP
        file_count, total_size = create_zip_from_folder(download_dir, zip_path)

        # Upload ZIP
        upload_to_s3(zip_path, bucket_name, output_key)

        # Generate pre-signed URL
        presigned_url = generate_presigned_url(bucket_name, output_key)
        return {
            'folder': folder_prefix,
            'file_count': file_count,
            'total_size': total_size,
            'zip_path': output_key,
            'presigned_url': presigned_url
        }
    except Exception as e:
        logger.error(f"Failed to process folder {folder_prefix}: {e}")
        raise

def main():
    """Main function to process all test folders."""
    try:
        # Validate configuration
        validate_config()

        # List all test folders
        test_folders = list_test_folders(bucket_name, base_prefix)
        if not test_folders:
            logger.warning("No test folders found")
            return

        results = []
        # Create temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            logger.info(f"Using temporary directory: {temp_dir}")
            
            # Process each folder
            for folder in test_folders:
                try:
                    result = process_folder(folder, temp_dir)
                    if result:
                        results.append(result)
                except Exception as e:
                    logger.error(f"Error processing {folder}: {e}")
                    continue

        # Log summary
        logger.info("\nProcessing Summary:")
        for result in results:
            logger.info(f"Folder: {result['folder']}")
            logger.info(f"Files: {result['file_count']}")
            logger.info(f"Size: {result['total_size']:,} bytes")
            logger.info(f"ZIP: {result['zip_path']}")
            logger.info(f"URL: {result['presigned_url'] or 'Failed'}\n")

    except KeyboardInterrupt:
        logger.warning("Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Script failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()