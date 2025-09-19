import boto3
import botocore.exceptions
import logging
import sys
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            f's3_operation_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    ]
)
logger = logging.getLogger(__name__)


def copy_and_rename_z_folders(bucket_name, source_prefix, destination_prefix, start_source_z, end_source_z, start_dest_z):
    """
    Copy and rename Z folders from source to destination in S3, then delete source folders.

    Args:
        bucket_name (str): Name of the S3 bucket.
        source_prefix (str): Source prefix (e.g., 'Batch 1/May 2/Partial 6/').
        destination_prefix (str): Destination prefix (e.g., 'Batch 1/May 2/F 0123/').
        start_source_z (int): Starting Z folder number in source (e.g., 1 for Z001).
        end_source_z (int): Ending Z folder number in source (e.g., 49 for Z049).
        start_dest_z (int): Starting Z folder number in destination (e.g., 952 for Z952).
    """
    s3_client = boto3.client('s3')

    try:
        # Verify bucket exists
        s3_client.head_bucket(Bucket=bucket_name)
        logger.info(f"Bucket {bucket_name} exists and is accessible")

        # Iterate through source Z folders (Z001 to Z049)
        for src_z_num in range(start_source_z, end_source_z + 1):
            dest_z_num = start_dest_z + (src_z_num - start_source_z)
            src_z_folder = f"Z{src_z_num:03d}"
            dest_z_folder = f"Z{dest_z_num:03d}"
            src_z_prefix = f"{source_prefix}{src_z_folder}/"
            dest_z_prefix = f"{destination_prefix}{dest_z_folder}/"

            logger.info(
                f"Processing source folder: {src_z_prefix} -> destination: {dest_z_prefix}")

            # List objects in the source Z folder
            try:
                response = s3_client.list_objects_v2(
                    Bucket=bucket_name, Prefix=src_z_prefix)

                if 'Contents' not in response or not response['Contents']:
                    logger.warning(
                        f"No objects found in {src_z_prefix}, skipping")
                    continue

                # Copy each object in the Z folder
                for obj in response['Contents']:
                    src_key = obj['Key']
                    dest_key = src_key.replace(
                        source_prefix + src_z_folder, destination_prefix + dest_z_folder)

                    logger.info(f"Copying {src_key} to {dest_key}")

                    # Check if destination object already exists
                    try:
                        s3_client.head_object(Bucket=bucket_name, Key=dest_key)
                        logger.warning(
                            f"Destination object {dest_key} already exists, skipping copy")
                        continue
                    except botocore.exceptions.ClientError as e:
                        if e.response['Error']['Code'] == '404':
                            # Object does not exist, proceed with copy
                            pass
                        else:
                            raise

                    # Perform the copy
                    s3_client.copy_object(
                        Bucket=bucket_name,
                        CopySource={'Bucket': bucket_name, 'Key': src_key},
                        Key=dest_key
                    )
                    logger.info(f"Successfully copied {src_key} to {dest_key}")

                # Delete source objects after successful copy
                for obj in response['Contents']:
                    src_key = obj['Key']
                    logger.info(f"Deleting source object {src_key}")
                    s3_client.delete_object(Bucket=bucket_name, Key=src_key)
                    logger.info(f"Successfully deleted {src_key}")

                logger.info(
                    f"Completed processing {src_z_prefix} -> {dest_z_prefix}")

            except botocore.exceptions.ClientError as e:
                logger.error(f"Failed to process {src_z_prefix}: {e}")
                continue
            except Exception as e:
                logger.error(
                    f"Unexpected error processing {src_z_prefix}: {e}")
                continue

        logger.info(
            f"Successfully processed folders Z{start_source_z:03d} to Z{end_source_z:03d} to destination Z{start_dest_z:03d} to Z{dest_z_num:03d}")

    except botocore.exceptions.ClientError as e:
        logger.error(f"Failed to access bucket {bucket_name}: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise


def main():
    bucket_name = "my-bucket-founder-series-sun"
    source_prefix = "Batch 1/May 2/Partial 10/"
    destination_prefix = "Batch 1/May 2/F 0125/"
    start_source_z = 1
    end_source_z = 329
    start_dest_z = 85

    logger.info("Starting S3 folder copy and rename operation")
    copy_and_rename_z_folders(bucket_name, source_prefix,
                              destination_prefix, start_source_z, end_source_z, start_dest_z)
    logger.info("S3 folder copy and rename operation completed")


if __name__ == "__main__":
    main()
