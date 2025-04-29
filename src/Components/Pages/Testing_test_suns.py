import boto3
import pyarrow.parquet as pq
import io
import json
import re
from multiprocessing import Pool
from urllib.parse import unquote
import logging
import uuid

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# AWS S3 configuration
S3_BUCKET = "my-bucket-parquet-test"
S3_PREFIX = "April 24/"
EXPECTED_SIZE_MB = 1.4  # Expected file size in MB
SIZE_TOLERANCE = 0.1  # ±10% tolerance
MB_TO_BYTES = 1024 * 1024

# Initialize S3 client
s3_client = boto3.client("s3")

def is_valid_3_digit_string(value):
    """Check if the value is a 3-digit string containing only digits."""
    return isinstance(value, str) and bool(re.match(r"^\d{3}$", value))

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
        file_size_mb = file_size_bytes / MB_TO_BYTES

        # Check file size
        size_outlier = (
            file_size_mb < EXPECTED_SIZE_MB * (1 - SIZE_TOLERANCE) or
            file_size_mb > EXPECTED_SIZE_MB * (1 + SIZE_TOLERANCE)
        )

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

        # Calculate average of number column (convert to int)
        try:
            number_avg = df["number"].astype(int).mean()
        except ValueError:
            number_avg = None
            logger.error(f"Invalid number format in {s3_uri}")

        return {
            "s3_uri": s3_uri,
            "file_size_mb": file_size_mb,
            "size_outlier": size_outlier,
            "last_row_valid": last_row_valid,
            "number_avg": number_avg,
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
            "error": str(e)
        }

def list_parquet_files():
    """List all parquet files under April 24/Test*/Z*."""
    parquet_files = []
    paginator = s3_client.get_paginator("list_objects_v2")

    for test_folder in [f"Test{i}" for i in range(1, 13)]:
        prefix = f"{S3_PREFIX}{test_folder}/"
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".parquet"):
                    parquet_files.append(f"s3://{S3_BUCKET}/{obj['Key']}")

    return parquet_files

def main():
    """Main function to process parquet files and generate report."""
    # List all parquet files
    parquet_files = list_parquet_files()
    logger.info(f"Found {len(parquet_files)} parquet files")

    # Process files in parallel
    with Pool() as pool:
        results = pool.map(verify_parquet_file, parquet_files)

    # Summarize results
    summary = {
        "total_files": len(results),
        "invalid_last_row": [r["s3_uri"] for r in results if not r["last_row_valid"]],
        "size_outliers": [r["s3_uri"] for r in results if r["size_outlier"]],
        "errors": [r["s3_uri"] for r in results if r["error"]],
        "averages": {r["s3_uri"]: r["number_avg"] for r in results if r["number_avg"] is not None},
        "details": results
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