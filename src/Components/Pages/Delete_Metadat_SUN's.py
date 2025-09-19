import boto3
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.exceptions import ClientError
from datetime import datetime
import os

# Configure logging
log_file = f"s3_cleanup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# AWS S3 configuration
BUCKET_NAME = 'my-bucket-founder-series-sun'
PREFIX = 'Batch 1/May 2/'
REGION = 'us-east-1'
SIZE_THRESHOLD = 1_000_000  # 1 MB in bytes
F_FOLDERS = [f'F {str(i).zfill(4)}' for i in range(126)]  # F 0000 to F 0126
Z_FOLDERS = [f'Z{str(i).zfill(3)}' for i in range(1, 1001)]  # Z001 to Z1000
MAX_WORKERS = 10  # Adjust based on system and AWS rate limits


def setup_s3_client():
    """Initialize and return an S3 client."""
    try:
        s3_client = boto3.client('s3', region_name=REGION)
        logger.info("S3 client initialized successfully.")
        return s3_client
    except Exception as e:
        logger.error(f"Failed to initialize S3 client: {e}")
        raise


def process_z_folder(s3_client, f_folder, z_folder, dry_run):
    """Process a single Z folder, deleting small files and checking for existence."""
    prefix = f"{PREFIX}{f_folder}/{z_folder}/"
    try:
        # List objects in the z folder
        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
        if 'Contents' not in response:
            if z_folder == 'Z1000':
                logger.warning(f"Folder {prefix} not found.")
            return False  # Indicate folder not found for Z1000 check

        files_to_delete = []
        for obj in response.get('Contents', []):
            file_size = obj['Size']
            if file_size < SIZE_THRESHOLD:
                files_to_delete.append(obj['Key'])
                logger.info(
                    f"{'[DRY RUN] Would delete' if dry_run else 'Deleting'} {obj['Key']} (size: {file_size} bytes)")

        # Perform deletion if not dry run
        if files_to_delete and not dry_run:
            try:
                s3_client.delete_objects(
                    Bucket=BUCKET_NAME,
                    Delete={'Objects': [{'Key': key}
                                        for key in files_to_delete]}
                )
                logger.info(
                    f"Deleted {len(files_to_delete)} files from {prefix}")
            except ClientError as e:
                logger.error(f"Failed to delete files in {prefix}: {e}")

        return True  # Indicate folder was found
    except ClientError as e:
        logger.error(f"Error processing {prefix}: {e}")
        if z_folder == 'Z1000':
            logger.warning(f"Folder {prefix} not found.")
        return False


def process_f_folder(s3_client, f_folder, dry_run):
    """Process all Z folders within an F folder in parallel."""
    logger.info(f"Processing F folder: {f_folder}")
    z1000_found = False

    # Process Z folders in parallel
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_z = {executor.submit(process_z_folder, s3_client, f_folder, z_folder, dry_run): z_folder
                       for z_folder in Z_FOLDERS}
        for future in as_completed(future_to_z):
            try:
                z1000_found |= future.result()  # Update z1000_found based on result
            except Exception as e:
                z_folder = future_to_z[future]
                logger.error(f"Error processing {f_folder}/{z_folder}: {e}")

    if not z1000_found:
        logger.warning(f"Z1000 not found in {PREFIX}{f_folder}/")


def main(dry_run=True):
    """Main function to process all F folders sequentially."""
    s3_client = setup_s3_client()
    logger.info(
        f"Starting S3 cleanup {'(dry run)' if dry_run else ''} for bucket {BUCKET_NAME}")

    # Process F folders sequentially
    for f_folder in F_FOLDERS:
        try:
            process_f_folder(s3_client, f_folder, dry_run)
            logger.info(f"Completed processing {f_folder}")
        except Exception as e:
            logger.error(f"Error processing {f_folder}: {e}")

    logger.info(
        f"Completed S3 cleanup {'(dry run)' if dry_run else ''}. Logs saved to {log_file}")


if __name__ == "__main__":
    # Run with dry_run=True to list files without deleting
    # main(dry_run=True)
    # To perform actual deletions, run with dry_run=False
    main(dry_run=False)
