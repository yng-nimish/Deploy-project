import boto3
import pandas as pd
import pyarrow.parquet as pq
import logging
import os
import tempfile
from botocore.exceptions import ClientError
from botocore.config import Config
from typing import List, Optional, Tuple

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# AWS configuration
AWS_REGION = 'us-east-1'
S3_BUCKET = 'my-bucket-founder-series-sun'
DYNAMODB_TABLE = 'Security_Snippet_Table'
MAIN_FOLDER = 'Batch 1/May 2'
SUB_FOLDERS = [f'F {str(i).zfill(4)}' for i in range(0, 15)]  # F 0001 to F 0014
Z_FOLDERS = [f'Z{i}' for i in range(311, 321)]  # Z311 to Z320
X_COORDINATE = 288
Y_COORDINATE = 600
EXPECTED_FILE_SIZE = 1.4 * 1024 * 1024  # 1.4 MB in bytes
SIZE_TOLERANCE = 0.8 * 1024 * 1024  # 800 KB tolerance

# Initialize AWS clients with retry configuration
config = Config(retries={'max_attempts': 5, 'mode': 'standard'})
s3_client = boto3.client('s3', region_name=AWS_REGION, config=config)
dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
table = dynamodb.Table(DYNAMODB_TABLE)

def get_parquet_file_path(bucket: str, prefix: str) -> Optional[str]:
    """Retrieve the Parquet file path with size closest to 1.4 MB within tolerance."""
    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        parquet_files = []
        for obj in response.get('Contents', []):
            if obj['Key'].endswith('.parquet'):
                size = obj['Size']
                if abs(size - EXPECTED_FILE_SIZE) <= SIZE_TOLERANCE:
                    parquet_files.append((obj['Key'], size))
        
        if not parquet_files:
            logger.warning(f"No Parquet files within size tolerance in s3://{bucket}/{prefix}")
            return None
        
        # Select the file closest to EXPECTED_FILE_SIZE
        parquet_files.sort(key=lambda x: abs(x[1] - EXPECTED_FILE_SIZE))
        selected_key = parquet_files[0][0]
        logger.info(f"Selected Parquet file: s3://{bucket}/{selected_key} (size: {parquet_files[0][1] / 1024 / 1024:.2f} MB)")
        return selected_key
    except ClientError as e:
        logger.error(f"Error listing objects in s3://{bucket}/{prefix}: {e}")
        return None

def read_number_from_parquet(bucket: str, key: str, x: int, y: int) -> Optional[str]:
    """Read the number from a Parquet file at specific x, y coordinates."""
    with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as temp_file:
        temp_path = temp_file.name
        try:
            # Download Parquet file to temporary local file
            s3_client.download_file(bucket, key, temp_path)
            logger.debug(f"Downloaded s3://{bucket}/{key} to {temp_path}")

            # Read Parquet file
            parquet_file = pq.read_table(temp_path)
            df = parquet_file.to_pandas()

            # Filter for exact x, y coordinates
            result = df[(df['x_coordinate'] == x) & (df['y_coordinate'] == y)]
            if not result.empty:
                number = result['number'].iloc[0]
                # Ensure number is a string with 3 digits
                if isinstance(number, str) and len(number) == 3:
                    return number
                logger.warning(f"Invalid number format at s3://{bucket}/{key}: {number}")
            else:
                logger.warning(f"No data found for x={x}, y={y} in s3://{bucket}/{key}")
            return None
        except ClientError as e:
            logger.error(f"Error downloading Parquet file s3://{bucket}/{key}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error processing s3://{bucket}/{key}: {e}")
            return None
        finally:
            # Clean up temporary file
            try:
                os.unlink(temp_path)
                logger.debug(f"Deleted temporary file {temp_path}")
            except OSError as e:
                logger.warning(f"Error deleting temporary file {temp_path}: {e}")

def check_duplicate_snippet(folder: str, snippet: str) -> bool:
    """Check if the Security Snippet exists in DynamoDB, excluding the current folder."""
    try:
        response = table.scan(
            FilterExpression='Security_Snippet = :snippet AND #folder <> :folder',
            ExpressionAttributeNames={'#folder': 'Folder Name'},  # Alias for attribute with space
            ExpressionAttributeValues={
                ':snippet': snippet,
                ':folder': folder
            }
        )
        duplicates = response.get('Items', [])
        if duplicates:
            logger.info(f"Duplicate Security Snippet '{snippet}' found in folders: {[item['Folder Name'] for item in duplicates]}")
            return True
        logger.info(f"No duplicates found for Security Snippet '{snippet}'")
        return False
    except ClientError as e:
        logger.error(f"Error scanning DynamoDB for duplicates: {e}")
        return False

def save_to_dynamodb(folder: str, snippet: str) -> bool:
    """Save the Security Snippet to DynamoDB."""
    try:
        table.put_item(
            Item={
                'Folder Name': folder,
                'Security_Snippet': snippet
            }
        )
        logger.info(f"Successfully saved Security Snippet for {folder} to DynamoDB")
        return True
    except ClientError as e:
        logger.error(f"Error saving to DynamoDB for {folder}: {e}")
        return False

def process_folder(f_folder: str) -> None:
    """Process an F folder to extract numbers and save to DynamoDB."""
    numbers = []
    for z_folder in Z_FOLDERS:
        prefix = f"{MAIN_FOLDER}/{f_folder}/{z_folder}/"
        parquet_key = get_parquet_file_path(S3_BUCKET, prefix)
        if not parquet_key:
            logger.error(f"Skipping {z_folder} in {f_folder}: No suitable Parquet file found")
            continue

        number = read_number_from_parquet(S3_BUCKET, parquet_key, X_COORDINATE, Y_COORDINATE)
        if number:
            numbers.append(number)
        else:
            logger.error(f"Skipping {z_folder} in {f_folder}: No valid number found")

    if len(numbers) == len(Z_FOLDERS):
        security_snippet = ''.join(numbers)
        logger.info(f"Security Snippet for {f_folder}: {security_snippet}")
        
        # Check for duplicates
        check_duplicate_snippet(f_folder, security_snippet)
        
        # Save to DynamoDB
        save_to_dynamodb(f_folder, security_snippet)
    else:
        logger.error(f"Incomplete Security Snippet for {f_folder}: Only {len(numbers)} numbers collected")

def main():
    """Main function to process all F folders."""
    for f_folder in SUB_FOLDERS:
        logger.info(f"Processing folder: {f_folder}")
        try:
            process_folder(f_folder)
        except Exception as e:
            logger.error(f"Error processing {f_folder}: {e}. Continuing with next folder.")

if __name__ == "__main__":
    main()