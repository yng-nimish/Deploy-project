import boto3
import pyarrow.parquet as pq
import pandas as pd
import logging
import multiprocessing as mp
from botocore.exceptions import ClientError
from datetime import datetime
import io
from typing import List, Tuple, Optional
from botocore.config import Config

# AWS Configuration
BUCKET_NAME = "my-bucket-founder-series-sun"
PREFIX = "Batch 1/May 2/"
EXPECTED_FILE_SIZE = 1.4 * 1024 * 1024  # 1.4 MB in bytes
SIZE_TOLERANCE = 0.8 * 1024 * 1024  # 800 KB tolerance

# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('S3PatternMatcher')

# S3 Client
s3_client = boto3.client('s3', config=Config(retries={'max_attempts': 5, 'mode': 'adaptive'}))

def get_reference_string() -> Optional[str]:
    """Fetch the first 10 rows' number field from F0000/Z001 parquet file."""
    reference_s3_path = f"{PREFIX}F 0000/Z001/"
    try:
        # List objects in Z001
        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=reference_s3_path)
        if 'Contents' not in response:
            logger.error(f"No files found in {reference_s3_path}")
            return None

        # Find the parquet file (~1.35 MB)
        parquet_file = None
        for obj in response['Contents']:
            size = obj['Size']
            if (EXPECTED_FILE_SIZE - SIZE_TOLERANCE) <= size <= (EXPECTED_FILE_SIZE + SIZE_TOLERANCE):
                parquet_file = obj['Key']
                break

        if not parquet_file:
            logger.error(f"No valid parquet file found in {reference_s3_path}")
            return None

        # Read parquet file
        obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=parquet_file)
        parquet_file = pq.read_table(io.BytesIO(obj['Body'].read()))
        df = parquet_file.to_pandas()

        # Ensure number field is string
        df = df.astype({'number': str})

        # Get first 10 rows' number field
        if len(df) < 10:
            logger.error(f"Reference file {parquet_file} has fewer than 10 rows")
            return None

        reference_string = ''.join(df['number'].head(10))
        logger.info(f"Reference string from {parquet_file}: {reference_string[:30]}...")
        return reference_string

    except ClientError as e:
        logger.error(f"Error reading reference file {reference_s3_path}: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error reading reference file {reference_s3_path}: {str(e)}")
        return None

def process_file(args: Tuple[str, str]) -> List[Tuple[str, str, str, str]]:
    """Process a single parquet file, sliding a window of 10 rows to check for matching pattern."""
    file_key, reference_string = args
    matches = []

    try:
        # Check file size
        head = s3_client.head_object(Bucket=BUCKET_NAME, Key=file_key)
        size = head['ContentLength']
        if not ((EXPECTED_FILE_SIZE - SIZE_TOLERANCE) <= size <= (EXPECTED_FILE_SIZE + SIZE_TOLERANCE)):
            return matches  # Skip files not within size range

        # Read parquet file
        obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_key)
        parquet_file = pq.read_table(io.BytesIO(obj['Body'].read()))
        df = parquet_file.to_pandas()

        # Ensure data types are string
        df = df.astype({'x_coordinate': str, 'y_coordinate': str, 'number': str})

        if len(df) < 10:
            logger.warning(f"File {file_key} has fewer than 10 rows, skipping")
            return matches

        # Slide window of 10 rows
        for i in range(len(df) - 9):  # Ensure at least 10 rows remain
            window = df.iloc[i:i+10]
            test_string = ''.join(window['number'])
            if test_string == reference_string:
                first_row = window.iloc[0]
                match_info = (
                    file_key,
                    first_row['x_coordinate'],
                    first_row['y_coordinate'],
                    test_string[:30]  # Log first 30 chars for brevity
                )
                matches.append(match_info)
                logger.info(
                    f"Match found in {file_key} at x={first_row['x_coordinate']}, "
                    f"y={first_row['y_coordinate']}: {test_string[:30]}..."
                )

    except ClientError as e:
        logger.error(f"Error processing file {file_key}: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error processing file {file_key}: {str(e)}")

    return matches

def main():
    """Main function to orchestrate the pattern matching."""
    logger.info("Starting S3 parquet pattern matching job")

    # Get reference string
    reference_string = get_reference_string()
    if not reference_string:
        logger.error("Failed to obtain reference string, aborting")
        return

    # List all parquet files
    all_files = []
    paginator = s3_client.get_paginator('list_objects_v2')
    for f_folder in [f"F {str(i).zfill(4)}" for i in range(15)]:
        for z_folder in [f"Z{str(i).zfill(3)}" for i in range(1, 1001)]:
            prefix = f"{PREFIX}{f_folder}/{z_folder}/"
            for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        all_files.append(obj['Key'])

    logger.info(f"Found {len(all_files)} parquet files to process")

    # Process files in parallel
    with mp.Pool(processes=mp.cpu_count()) as pool:
        results = pool.map(
            process_file,
            [(file_key, reference_string) for file_key in all_files]
        )

    # Collect and log results
    all_matches = [match for sublist in results for match in sublist]
    logger.info(f"Total matches found: {len(all_matches)}")
    for match in all_matches:
        file_key, x, y, pattern = match
        logger.info(
            f"Summary: Match in {file_key} at x={x}, y={y}: {pattern}..."
        )

    logger.info("Pattern matching job completed")

if __name__ == "__main__":
    main()