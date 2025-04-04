import sys
import logging
import boto3
import pandas as pd
from io import BytesIO
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from datetime import datetime
import openpyxl

# Set up detailed logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize AWS Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def get_s3_files(bucket, prefix):
    """Get list of files from S3 bucket with given prefix."""
    try:
        s3_client = boto3.client('s3')
        prefixes_to_try = [prefix]
        
        files = []
        for p in prefixes_to_try:
            logger.info(f"Listing objects in bucket: {bucket}, prefix: '{p}'")
            paginator = s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket, Prefix=p)
            
            for page in pages:
                if 'Contents' not in page:
                    logger.warning(f"No objects found in page for prefix: '{p}'")
                    continue
                for obj in page['Contents']:
                    if obj['Key'].endswith('.xlsx'):
                        files.append(obj['Key'])
            
            if files:
                logger.info(f"Found {len(files)} Excel files with prefix '{p}'")
                return files
        
        if not files:
            logger.error(f"No .xlsx files found with any prefix in {bucket}")
            raise ValueError(f"No .xlsx files found in {bucket} with prefixes: {prefixes_to_try}")
        
        return files
    except Exception as e:
        logger.error(f"Error listing S3 files: {str(e)}")
        raise

def read_excel_from_s3(s3_client, bucket, key):
    """Read all sheets from Excel file in S3 into a dictionary of DataFrames."""
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        excel_data = BytesIO(obj['Body'].read())
        
        # Read all sheets into a dictionary of DataFrames (key: sheet name, value: DataFrame)
        xls = pd.ExcelFile(excel_data)
        all_sheets_df = {}
        for sheet_name in xls.sheet_names:
            df = pd.read_excel(xls, sheet_name=sheet_name, dtype=str)  # Read as strings to preserve leading zeroes
            all_sheets_df[sheet_name] = df

        logger.info(f"Successfully read {key} with {len(xls.sheet_names)} sheets")
        return all_sheets_df
    except Exception as e:
        logger.error(f"Error reading Excel file {key}: {str(e)}")
        raise

def merge_and_save_batch(batch_num, file_list, s3_client, target_bucket, source_bucket):
    """Merge files for a specific batch and save the merged file to S3."""
    try:
        logger.info(f"Processing batch {batch_num} with {len(file_list)} files")
        
        # Prepare to save merged file in a new Excel file with multiple sheets
        output_key = f"merged_output_batch_{batch_num}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        output_buffer = BytesIO()

        # Use openpyxl to write Excel file with multiple sheets
        with pd.ExcelWriter(output_buffer, engine='openpyxl') as writer:
            sheet_counter = 1
            for file_key in file_list:
                try:
                    # Read all sheets of the current Excel file
                    all_sheets = read_excel_from_s3(s3_client, source_bucket, file_key)

                    # Write each sheet to the output Excel file
                    for sheet_name, df in all_sheets.items():
                        sheet_label = f"z{sheet_counter}"  # Sheet names: z1, z2, z3, ...
                        df.to_excel(writer, index=False, sheet_name=sheet_label)
                        sheet_counter += 1

                except Exception as e:
                    logger.warning(f"Skipping file {file_key} due to error: {str(e)}")
                    continue
        
        # Upload the merged Excel file to S3
        s3_client.put_object(
            Bucket=target_bucket,
            Key=output_key,
            Body=output_buffer.getvalue()
        )
        logger.info(f"Successfully saved merged batch {batch_num} to s3://{target_bucket}/{output_key}")

        # Clear the buffer to release memory
        del output_buffer

    except Exception as e:
        logger.error(f"Error processing batch {batch_num}: {str(e)}")
        raise

def merge_and_save_excel_files():
    """Main function to merge Excel files by batch and save to target bucket."""
    source_bucket = "my-bucket-sun-test"  # Define the source bucket
    source_prefix = "s3://my-bucket-sun-test/temp/ingest_year=2025/ingest_month=03/ingest_day=31/"
    target_bucket = "my-glue-checkpointing-bucket"
    s3_client = boto3.client('s3')

    try:
        # Get all Excel files
        files = get_s3_files(source_bucket, source_prefix)
        
        if not files:
            logger.error("No Excel files found to process. Job will terminate.")
            return
        
        # Group files by batch number
        batch_files = {}
        for file_key in files:
            try:
                # Extract batch number from the file key
                batch_num = int(file_key.split('batch_')[1].split('_')[0])  # Extract batch number from file name
                if batch_num not in batch_files:
                    batch_files[batch_num] = []
                batch_files[batch_num].append(file_key)
            except Exception as e:
                logger.warning(f"Could not determine batch number for file {file_key}. Skipping it. Error: {str(e)}")
        
        # Sort batches in reverse order, starting with batch 9
        sorted_batches = sorted(batch_files.keys(), reverse=True)
        
        # Process batches sequentially (one batch at a time)
        for batch_num in sorted_batches:
            merge_and_save_batch(batch_num, batch_files[batch_num], s3_client, target_bucket, source_bucket)
        
    except Exception as e:
        logger.error(f"Job failed: {str(e)}")
        raise
    finally:
        job.commit()

if __name__ == "__main__":
    try:
        logger.info("Starting Excel merge job")
        merge_and_save_excel_files()
        logger.info("Job completed successfully")
    except Exception as e:
        logger.error(f"Job failed with error: {str(e)}")
        raise
