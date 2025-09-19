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
from tqdm import tqdm

# AWS Configuration
BUCKET_NAME = "my-bucket-founder-series-sun"
PREFIX = "Batch 1/May 2/"
EXPECTED_FILE_SIZE = 1.4 * 1024 * 1024  # 1.4 MB in bytes
SIZE_TOLERANCE = 0.8 * 1024 * 1024  # 800 KB tolerance
BATCH_SIZE = 100  # Number of files to process in each batch

# Reference string broken into 3-digit groups
REFERENCE_GROUPS = [
    "665", "124", "944", "403", "213",
    "235", "098", "209", "801", "852"
]

# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('S3PatternMatcher')

# Suppress boto3 and botocore logging for credentials
logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)

# S3 Client
s3_client = boto3.client('s3', config=Config(retries={'max_attempts': 5, 'mode': 'adaptive'}))

def get_reference_string() -> Optional[str]:
    """Fetch the first 10 rows' number field from F0000/Z001 parquet file."""
    reference_s3_path = f"{PREFIX}F 0000/Z001/"
    logger.info(f"Fetching reference string from {reference_s3_path}")
    try:
        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=reference_s3_path)
        if 'Contents' not in response:
            logger.error(f"No files found in {reference_s3_path}")
            return None

        parquet_file = None
        for obj in response['Contents']:
            size = obj['Size']
            if (EXPECTED_FILE_SIZE - SIZE_TOLERANCE) <= size <= (EXPECTED_FILE_SIZE + SIZE_TOLERANCE):
                parquet_file = obj['Key']
                logger.info(f"Found valid parquet file: {parquet_file} (size: {size} bytes)")
                break

        if not parquet_file:
            logger.error(f"No valid parquet file found in {reference_s3_path}")
            return None

        logger.info(f"Reading parquet file: {parquet_file}")
        obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=parquet_file)
        parquet_file_data = pq.read_table(io.BytesIO(obj['Body'].read()))
        df = parquet_file_data.to_pandas()

        df = df.astype({'number': str})
        if len(df) < 10:
            logger.error(f"Reference file {parquet_file} has fewer than 10 rows")
            return None

        reference_string = ''.join(df['number'].head(10))
        if reference_string != ''.join(REFERENCE_GROUPS):
            logger.error(f"Reference string {reference_string[:30]}... does not match expected pattern")
            return None

        logger.info(f"Successfully retrieved reference string: {reference_string[:30]}...")
        return reference_string

    except ClientError as e:
        logger.error(f"Error reading reference file {reference_s3_path}: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error reading reference file {reference_s3_path}: {str(e)}")
        return None

def process_file(args: Tuple[str, List[str]]) -> List[Tuple[str, str, str, str]]:
    """Process a single parquet file, checking all 10-row windows for 3-digit group matches."""
    file_key, reference_groups = args
    matches = []

    logger.info(f"Processing parquet file: s3://{BUCKET_NAME}/{file_key}")
    try:
        head = s3_client.head_object(Bucket=BUCKET_NAME, Key=file_key)
        size = head['ContentLength']
        logger.info(f"File size check: {file_key} (size: {size} bytes)")
        if not ((EXPECTED_FILE_SIZE - SIZE_TOLERANCE) <= size <= (EXPECTED_FILE_SIZE + SIZE_TOLERANCE)):
            logger.warning(f"File {file_key} size {size} bytes outside tolerance, skipping")
            return matches

        logger.info(f"Reading parquet file: {file_key}")
        obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_key)
        parquet_file = pq.read_table(io.BytesIO(obj['Body'].read()))
        df = parquet_file.to_pandas()

        df = df.astype({'x_coordinate': str, 'y_coordinate': str, 'number': str})
        if len(df) < 10:
            logger.warning(f"File {file_key} has fewer than 10 rows, skipping")
            return matches

        logger.info(f"Scanning {len(df) - 9} windows in {file_key}")
        for i in range(len(df) - 9):
            window = df.iloc[i:i+10]
            match = True
            for j, (row, ref_group) in enumerate(zip(window['number'], reference_groups)):
                if row != ref_group:
                    match = False
                    break
            if match:
                first_row = window.iloc[0]
                test_string = ''.join(window['number'])
                match_info = (
                    file_key,
                    first_row['x_coordinate'],
                    first_row['y_coordinate'],
                    test_string[:30]
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

    logger.info(f"Completed processing {file_key}: {len(matches)} matches found")
    return matches

def process_folder(f_folder: str, reference_groups: List[str]) -> List[Tuple[str, str, str, str]]:
    """Process all parquet files in a single F folder, filtering by size."""
    logger.info(f"Listing files in folder: {f_folder}")
    all_files = []
    paginator = s3_client.get_paginator('list_objects_v2')
    for z_folder in [f"Z{str(i).zfill(3)}" for i in range(1, 1001)]:
        prefix = f"{PREFIX}{f_folder}/{z_folder}/"
        logger.info(f"Scanning Z folder: {prefix}")
        for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    size = obj['Size']
                    if (EXPECTED_FILE_SIZE - SIZE_TOLERANCE) <= size <= (EXPECTED_FILE_SIZE + SIZE_TOLERANCE):
                        all_files.append(obj['Key'])
                        logger.info(f"Found valid file: {obj['Key']} (size: {size} bytes)")

    logger.info(f"Found {len(all_files)} parquet files in {f_folder}")

    all_matches = []
    for i in range(0, len(all_files), BATCH_SIZE):
        batch_files = all_files[i:i + BATCH_SIZE]
        logger.info(f"Processing batch {i//BATCH_SIZE + 1} with {len(batch_files)} files")
        with mp.Pool(processes=mp.cpu_count()) as pool:
            results = pool.map(
                process_file,
                [(file_key, reference_groups) for file_key in batch_files],
                chunksize=max(1, len(batch_files) // (mp.cpu_count() * 4))
            )
        batch_matches = [match for sublist in results for match in sublist]
        all_matches.extend(batch_matches)
        logger.info(f"Completed batch {i//BATCH_SIZE + 1}: {len(batch_matches)} matches")

    logger.info(f"Completed processing folder {f_folder}: {len(all_matches)} total matches")
    return all_matches

def main():
    """Main function to orchestrate the pattern matching."""
    logger.info("Starting S3 parquet pattern matching job")

    reference_string = get_reference_string()
    if not reference_string:
        logger.error("Failed to obtain reference string, aborting")
        return

    all_matches = []
    for i in tqdm(range(86, 111), desc="Processing folders"):
        f_folder = f"F {str(i).zfill(4)}"
        logger.info(f"Processing folder {f_folder}")
        matches = process_folder(f_folder, REFERENCE_GROUPS)
        all_matches.extend(matches)
        logger.info(f"Matches found in {f_folder}: {len(matches)}")

    logger.info(f"Total matches found across all folders: {len(all_matches)}")
    for match in all_matches:
        file_key, x, y, pattern = match
        logger.info(
            f"Summary: Match in {file_key} at x={x}, y={y}: {pattern}..."
        )

    logger.info("Pattern matching job completed")

if __name__ == "__main__":
    main()