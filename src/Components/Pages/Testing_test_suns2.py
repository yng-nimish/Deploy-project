import boto3
import pyarrow.parquet as pq
import pandas as pd
import io
import json
import re
from multiprocessing import Pool
from urllib.parse import unquote
import logging
import uuid
from itertools import product

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# AWS S3 configuration
S3_BUCKET = "my-bucket-parquet-test"
S3_PREFIX = "April 24/"
MIN_SIZE_MB = 1.2  # Minimum file size in MB
MAX_SIZE_MB = 1.6  # Maximum file size in MB
MB_TO_BYTES = 1024 * 1024
SKIP_FILE_SIZE_BYTES = 473  # File size to skip (473 bytes)

# Initialize S3 client
s3_client = boto3.client("s3")

def is_valid_3_digit_string(value):
    """Check if the value is a 3-digit string."""
    return isinstance(value, str) and len(value) == 3

def verify_parquet_file(s3_uri):
    """Process a single parquet file: verify last row, check size, calculate average."""
    try:
        # Parse S3 URI
        bucket = s3_uri.split("/")[2]
        key = "/".join(s3_uri.split("/")[3:])
        key = unquote(key)  # Handle spaces in folder names

        # Get file metadata for size
        head_response = s3_client.head_object(Bucket=bucket, Key=key)
        file_size_bytes = head_response["ContentLength"]

        # Skip files that are exactly 473 bytes
        if file_size_bytes == SKIP_FILE_SIZE_BYTES:
            return None  # Return None to indicate this file should be skipped

        file_size_mb = file_size_bytes / MB_TO_BYTES

        # Check file size
        size_outlier = file_size_mb < MIN_SIZE_MB or file_size_mb > MAX_SIZE_MB
        if size_outlier:
            logger.warning(f"Size outlier: {s3_uri} ({file_size_mb:.2f} MB)")

        # Download parquet file to memory
        response = s3_client.get_object(Bucket=bucket, Key=key)
        parquet_data = response["Body"].read()
        parquet_file = pq.ParquetFile(io.BytesIO(parquet_data))

        # Read parquet file
        table = parquet_file.read()
        df = table.to_pandas()

        # Verify last row (x=1000, y=1000)
        last_row = df[(df["x_coordinate"] == 1000) & (df["y_coordinate"] == 1000)]
        last_row_valid = False
        if not last_row.empty:
            number_value = last_row.iloc[0]["number"]
            last_row_valid = is_valid_3_digit_string(number_value)
            if not last_row_valid:
                logger.warning(f"Invalid last row number: {s3_uri} ({number_value})")

        # Calculate per-file average of number column (convert to int)
        try:
            number_avg = df["number"].astype(int).mean()
        except ValueError as e:
            number_avg = None
            logger.error(f"Invalid number format in {s3_uri}: {str(e)}")

        return {
            "s3_uri": s3_uri,
            "file_size_mb": file_size_mb,
            "size_outlier": size_outlier,
            "last_row_valid": last_row_valid,
            "number_avg": number_avg,
            "data": df[["x_coordinate", "y_coordinate", "number"]] if number_avg is not None else None,
            "error": None
        }

    except Exception as e:
        logger.error(f"Error processing {s3_uri}: {str(e)}")
        return {
            "s3_uri": s3_uri,
            "file_size_mb": None,
            "size_outlier": False,
            "last_row_valid": False,
            "number_avg": None,
            "data": None,
            "error": str(e)
        }

def list_parquet_files_and_missing_z():
    """List all parquet files and identify missing Z folders."""
    parquet_files = []
    existing_z_folders = {f"Test{i}": set() for i in range(1, 13)}
    paginator = s3_client.get_paginator("list_objects_v2")

    for test_folder in [f"Test{i}" for i in range(1, 13)]:
        prefix = f"{S3_PREFIX}{test_folder}/"
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".parquet"):
                    parquet_files.append(f"s3://{S3_BUCKET}/{obj['Key']}")
                    # Extract Z folder (e.g., Z106)
                    parts = obj["Key"].split("/")
                    z_folder = parts[3]  # e.g., Z106
                    existing_z_folders[test_folder].add(z_folder)

    # Identify missing Z folders
    missing_z_folders = {}
    for test_folder in existing_z_folders:
        expected_z = {f"Z{i}" for i in range(1, 1001)}
        actual_z = existing_z_folders[test_folder]
        missing = expected_z - actual_z
        if missing:
            missing_z_folders[test_folder] = sorted(list(missing))
            logger.warning(f"Missing Z folders in {test_folder}: {missing_z_folders[test_folder]}")

    return parquet_files, missing_z_folders

def compute_grouped_averages(results):
    """Compute averages grouped by x_coordinate and y_coordinate per Test folder."""
    test_dfs = {}
    for result in results:
        if result is not None and result["data"] is not None:  # Skip None results
            test_folder = result["s3_uri"].split("/")[4]  # e.g., Test11
            if test_folder not in test_dfs:
                test_dfs[test_folder] = []
            test_dfs[test_folder].append(result["data"])

    grouped_averages = {}
    for test_folder, dfs in test_dfs.items():
        combined_df = pd.concat(dfs, ignore_index=True)
        combined_df["number"] = combined_df["number"].astype(int)
        # Group by x_coordinate
        x_avg = combined_df.groupby("x_coordinate")["number"].mean().to_dict()
        # Group by y_coordinate
        y_avg = combined_df.groupby("y_coordinate")["number"].mean().to_dict()
        grouped_averages[test_folder] = {"x_coordinate": x_avg, "y_coordinate": y_avg}

    return grouped_averages

def main():
    """Main function to process parquet files and generate report."""
    # List all parquet files and missing Z folders
    parquet_files, missing_z_folders = list_parquet_files_and_missing_z()
    logger.info(f"Found {len(parquet_files)} parquet files")

    # Process files in parallel
    with Pool() as pool:
        results = pool.map(verify_parquet_file, parquet_files)

    # Filter out None results (skipped files)
    results = [r for r in results if r is not None]

    # Compute grouped averages
    grouped_averages = compute_grouped_averages(results)

    # Summarize results
    summary = {
        "total_files": len(results),  # Only count processed files
        "missing_z_folders": missing_z_folders,
        "invalid_last_row": [r["s3_uri"] for r in results if not r["last_row_valid"]],
        "size_outliers": [r["s3_uri"] for r in results if r["size_outlier"]],
        "errors": [r["s3_uri"] for r in results if r["error"]],
        "per_file_averages": {r["s3_uri"]: r["number_avg"] for r in results if r["number_avg"] is not None},
        "grouped_averages": grouped_averages,
        "details": [{k: v for k, v in r.items() if k != "data"} for r in results]
    }

    # Save report to S3
    report_key = f"{S3_PREFIX}verification_report_{uuid.uuid4()}.json"
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=report_key,
        Body=json.dumps(summary, indent=2)
    )
    logger.info(f"Report saved to s3://{S3_BUCKET}/{report_key}")

if __name__ == "__main__":
    main()