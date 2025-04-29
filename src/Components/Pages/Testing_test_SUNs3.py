import boto3
import pyarrow.parquet as pq
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
import logging
import uuid
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# S3 client
s3_client = boto3.client('s3')

# Constants
BUCKET = 'my-bucket-parquet-test'
PRIMARY_FOLDER = 'April 24'
TEST_FOLDERS = [f'Test{i}' for i in range(1, 13)]
Z_FOLDERS = [f'Z{i}' for i in range(1, 1001)]
OUTPUT_PREFIX = 'today'
EXPECTED_FILE_SIZE = 1.4 * 1024 * 1024  # 1.4 MB in bytes
SIZE_TOLERANCE = 0.1 * 1024 * 1024  # 100 KB tolerance
MAX_WORKERS = 50  # Adjust for Lambda memory constraints

def validate_parquet_file(s3_uri):
    """Validate the parquet file's last row and calculate averages."""
    try:
        # Parse S3 URI
        bucket = s3_uri.split('/')[2]
        key = '/'.join(s3_uri.split('/')[3:])
        
        # Get file size
        response = s3_client.head_object(Bucket=bucket, Key=key)
        file_size = response['ContentLength']
        
        # Skip if not ~1.4MB
        if not (EXPECTED_FILE_SIZE - SIZE_TOLERANCE <= file_size <= EXPECTED_FILE_SIZE + SIZE_TOLERANCE):
            return None, None, None
        
        # Download file
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        buffer = BytesIO(obj['Body'].read())
        
        # Read parquet
        df = pq.read_table(buffer).to_pandas()
        
        # Validation: Check last row (x=1000, y=1000)
        last_row = df[(df['x_coordinate'] == 1000) & (df['y_coordinate'] == 1000)]
        validation_result = None
        if last_row.empty:
            validation_result = f"ERROR: {s3_uri} - Last row (x=1000, y=1000) missing"
        else:
            number = last_row.iloc[0]['number']
            if not (isinstance(number, str) and len(number) == 3 and number.isdigit()):
                validation_result = f"ERROR: {s3_uri} - Last row number '{number}' is not a 3-digit string"
            else:
                validation_result = f"SUCCESS: {s3_uri} - Last row number '{number}' is valid"
        
        # Calculate averages
        df['number_int'] = df['number'].astype(int)
        overall_avg = df['number_int'].mean()
        x_avg = df.groupby('x_coordinate')['number_int'].mean().to_dict()
        y_avg = df.groupby('y_coordinate')['number_int'].mean().to_dict()
        
        averages = {
            's3_uri': s3_uri,
            'overall_avg': overall_avg,
            'x_avg': x_avg,
            'y_avg': y_avg,
            'test_folder': key.split('/')[1],
            'z_folder': key.split('/')[2]
        }
        
        return validation_result, averages, df['number_int'].count()
    except Exception as e:
        return f"ERROR: {s3_uri} - Failed to process: {str(e)}", None, None

def process_z_folder(test_folder, z_folder):
    """Process all parquet files in a Z folder."""
    prefix = f"{PRIMARY_FOLDER}/{test_folder}/{z_folder}/"
    try:
        response = s3_client.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
        if 'Contents' not in response:
            return [f"ERROR: {prefix} - Z folder missing"], [], 0
        
        validation_results = []
        averages_list = []
        total_rows = 0
        
        # Process each file
        for obj in response.get('Contents', []):
            s3_uri = f"s3://{BUCKET}/{obj['Key']}"
            validation_result, averages, row_count = validate_parquet_file(s3_uri)
            if validation_result:
                validation_results.append(validation_result)
            if averages:
                averages_list.append(averages)
            if row_count:
                total_rows += row_count
        
        return validation_results, averages_list, total_rows
    except Exception as e:
        return [f"ERROR: {prefix} - Failed to process: {str(e)}"], [], 0

def main():
    validation_results = []
    all_averages = []
    total_rows_processed = 0
    
    # Parallel processing of Test and Z folders
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_folder = {
            executor.submit(process_z_folder, test_folder, z_folder): (test_folder, z_folder)
            for test_folder in TEST_FOLDERS
            for z_folder in Z_FOLDERS
        }
        
        for future in as_completed(future_to_folder):
            test_folder, z_folder = future_to_folder[future]
            try:
                val_results, avg_results, row_count = future.result()
                validation_results.extend(val_results)
                all_averages.extend(avg_results)
                total_rows_processed += row_count
                for result in val_results:
                    logger.info(result)
            except Exception as e:
                logger.error(f"ERROR: {test_folder}/{z_folder} - Failed: {str(e)}")
    
    # Write validation results to S3
    validation_key = f"{OUTPUT_PREFIX}/validation_results.txt"
    validation_content = "\n".join(validation_results)
    s3_client.put_object(
        Bucket=BUCKET,
        Key=validation_key,
        Body=validation_content.encode('utf-8')
    )
    logger.info(f"Validation results written to s3://{BUCKET}/{validation_key}")
    
    # Aggregate averages per Test folder
    averages_df = pd.DataFrame(all_averages)
    if not averages_df.empty:
        # Convert x_avg and y_avg dictionaries to JSON-compatible format
        averages_df['x_avg'] = averages_df['x_avg'].apply(lambda x: {str(k): v for k, v in x.items()})
        averages_df['y_avg'] = averages_df['y_avg'].apply(lambda x: {str(k): v for k, v in x.items()})
        
        # Calculate Test folder aggregates
        test_folder_avgs = averages_df.groupby('test_folder').agg({
            'overall_avg': 'mean',
            'x_avg': lambda x: {
                str(k): np.mean([d.get(str(k), np.nan) for d in x if str(k) in d])
                for k in range(1, 1001)
            },
            'y_avg': lambda x: {
                str(k): np.mean([d.get(str(k), np.nan) for d in x if str(k) in d])
                for k in range(1, 1001)
            }
        }).reset_index()
        
        # Combine file-level and test-folder-level averages
        output_df = pd.concat([
            averages_df[['s3_uri', 'test_folder', 'z_folder', 'overall_avg', 'x_avg', 'y_avg']],
            test_folder_avgs.rename(columns={
                'overall_avg': 'test_overall_avg',
                'x_avg': 'test_x_avg',
                'y_avg': 'test_y_avg'
            })
        ], axis=0, ignore_index=True)
        
        # Write averages to parquet
        averages_key = f"{OUTPUT_PREFIX}/averages.parquet"
        buffer = BytesIO()
        output_df.to_parquet(buffer, engine='pyarrow')
        s3_client.put_object(
            Bucket=BUCKET,
            Key=averages_key,
            Body=buffer.getvalue()
        )
        logger.info(f"Averages written to s3://{BUCKET}/{averages_key}")
        
        # Log averages
        for _, row in output_df.iterrows():
            if 's3_uri' in row and pd.notna(row['s3_uri']):
                logger.info(f"Averages for {row['s3_uri']}: Overall={row['overall_avg']:.2f}")
            else:
                logger.info(f"Test folder {row['test_folder']} aggregates: Overall={row['test_overall_avg']:.2f}")

    logger.info(f"Total rows processed: {total_rows_processed}")

if __name__ == "__main__":
    main()
