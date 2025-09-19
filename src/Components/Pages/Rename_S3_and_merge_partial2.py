import boto3
import botocore.exceptions
import logging
import sys
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            f's3_operation_partial2_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    ]
)
logger = logging.getLogger(__name__)


def list_prefixes_for_debugging(s3_client, bucket_name, prefix):
    """List all prefixes under the given prefix for debugging."""
    try:
        response = s3_client.list_objects_v2(
            Bucket=bucket_name, Prefix=prefix, Delimiter='/')
        prefixes = [common_prefix['Prefix']
                    for common_prefix in response.get('CommonPrefixes', [])]
        logger.debug(f"Available prefixes under {prefix}: {prefixes}")
        return prefixes
    except botocore.exceptions.ClientError as e:
        logger.error(f"Failed to list prefixes under {prefix}: {e}")
        return []


def copy_and_rename_z_folders(bucket_name, source_prefix, destination_prefix, start_source_z, end_source_z, start_dest_z):
    """
    Copy and rename Z folders from source to destination in S3, removing extra subfolder, then delete source folders.

    Args:
        bucket_name (str): Name of the S3 bucket.
        source_prefix (str): Source prefix (e.g., 'Batch 1/May 2/partial 2/').
        destination_prefix (str): Destination prefix (e.g., 'Batch 1/May 2/F 0013/').
        start_source_z (int): Starting Z folder number in source (e.g., 1 for Z001).
        end_source_z (int): Ending Z folder number in source (e.g., 14 for Z014).
        start_dest_z (int): Starting Z folder number in destination (e.g., 1 for Z001).
    """
    s3_client = boto3.client('s3', region_name='us-east-1')

    try:
        # Verify bucket exists
        logger.debug(f"Checking accessibility of bucket: {bucket_name}")
        s3_client.head_bucket(Bucket=bucket_name)
        logger.info(f"Bucket {bucket_name} exists and is accessible")

        # List prefixes under source_prefix for debugging
        list_prefixes_for_debugging(s3_client, bucket_name, source_prefix)

        # Iterate through source Z folders (Z001 to Z014)
        for src_z_num in range(start_source_z, end_source_z + 1):
            dest_z_num = start_dest_z + (src_z_num - start_source_z)
            src_z_folder = f"Z{src_z_num:03d}"
            dest_z_folder = f"Z{dest_z_num:03d}"
            src_z_prefix = f"{source_prefix}{src_z_folder}/{src_z_folder}.parquet/"
            dest_z_prefix = f"{destination_prefix}{dest_z_folder}/"

            logger.debug(
                f"Processing source folder: {src_z_prefix} -> destination: {dest_z_prefix}")

            # List objects in the source Z folder
            try:
                response = s3_client.list_objects_v2(
                    Bucket=bucket_name, Prefix=src_z_prefix)

                if 'Contents' not in response or not response['Contents']:
                    logger.warning(
                        f"No objects found in {src_z_prefix}, checking parent prefix")
                    # Fallback: Check parent prefix without ZXXX.parquet/
                    alt_src_z_prefix = f"{source_prefix}{src_z_folder}/"
                    alt_response = s3_client.list_objects_v2(
                        Bucket=bucket_name, Prefix=alt_src_z_prefix)
                    if 'Contents' not in alt_response or not alt_response['Contents']:
                        logger.warning(
                            f"No objects found in {alt_src_z_prefix}, skipping")
                        continue
                    response = alt_response
                    src_z_prefix = alt_src_z_prefix

                # Copy each object in the Z folder
                for obj in response['Contents']:
                    src_key = obj['Key']
                    # Remove the ZXXX.parquet/ subfolder if present, otherwise use direct path
                    if src_key.startswith(f"{source_prefix}{src_z_folder}/{src_z_folder}.parquet/"):
                        dest_key = src_key.replace(
                            f"{source_prefix}{src_z_folder}/{src_z_folder}.parquet/", f"{destination_prefix}{dest_z_folder}/")
                    else:
                        dest_key = src_key.replace(
                            f"{source_prefix}{src_z_folder}/", f"{destination_prefix}{dest_z_folder}/")

                    logger.debug(f"Copying {src_key} to {dest_key}")

                    # Check if destination object already exists
                    try:
                        s3_client.head_object(Bucket=bucket_name, Key=dest_key)
                        logger.warning(
                            f"Destination object {dest_key} already exists, skipping copy")
                        continue
                    except botocore.exceptions.ClientError as e:
                        if e.response['Error']['Code'] == '404':
                            logger.debug(
                                f"Destination {dest_key} does not exist, proceeding with copy")
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
                    logger.debug(f"Deleting source object {src_key}")
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
    source_prefix = "Batch 1/May 2/partial 2/"
    destination_prefix = "Batch 1/May 2/F 0013/"
    start_source_z = 1
    end_source_z = 114
    start_dest_z = 1

    logger.info("Starting S3 folder copy and rename operation for partial 2")
    copy_and_rename_z_folders(bucket_name, source_prefix,
                              destination_prefix, start_source_z, end_source_z, start_dest_z)
    logger.info("S3 folder copy and rename operation completed")


if __name__ == "__main__":
    main()
