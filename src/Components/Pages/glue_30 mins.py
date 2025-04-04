import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, lpad, explode, count
from awsglue import DynamicFrame
from awsglue.dynamicframe import DynamicFrameCollection
import datetime
import boto3
from io import BytesIO
import pandas as pd
import numpy as np
import logging
import os

# Logging Setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)

# Get job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'TempDir'])

# Initialize GlueContext
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
logger.info("Arrow optimization enabled for PySpark to Pandas conversion.")

# Global variables
batch_counter = 0
total_sheets_processed = 0
total_numbers_processed = 0
file_paths = []  # List of (path, sheet_count) tuples

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    dynamic_frame = dfc["AmazonKinesis_node"]
    df = dynamic_frame.toDF()
    record_count = df.count()
    if record_count == 0:
        return DynamicFrameCollection({}, glueContext)
    df.printSchema()
    df_unpacked = df.select(explode(col("numbers")).alias("number"))
    df_transformed = df_unpacked.withColumn("number", lpad(col("number").cast("string"), 3, "0"))
    return DynamicFrameCollection({"transformed_number": DynamicFrame.fromDF(df_transformed, glueContext, "transformed")}, glueContext)

def write_partition_to_excel(iterator, s3_base_path, partition_idx, remaining_sheets):
    s3_client = boto3.client('s3')
    numbers = [row["number"] for row in iterator]
    if not numbers:
        return []
    df = pd.DataFrame(numbers, columns=["number"])
    total_numbers = len(numbers)
    rows_per_sheet = 1000
    numbers_per_sheet = rows_per_sheet * 1000
    sheet_count = min((total_numbers + numbers_per_sheet - 1) // numbers_per_sheet, remaining_sheets)
    sheets_per_file = 10  # Max sheets per file
    results = []

    for file_start in range(0, sheet_count, sheets_per_file):
        file_idx = partition_idx * ((sheet_count + sheets_per_file - 1) // sheets_per_file) + (file_start // sheets_per_file)
        output_key = f"{s3_base_path}_partition_{partition_idx}_file_{file_idx}.xlsx"
        file_stream = BytesIO()
        with pd.ExcelWriter(file_stream, engine='openpyxl') as writer:
            sheets_in_file = min(sheets_per_file, sheet_count - file_start)
            for i in range(file_start, file_start + sheets_in_file):
                start_idx = i * numbers_per_sheet
                end_idx = min(start_idx + numbers_per_sheet, total_numbers)
                sheet_data = df.iloc[start_idx:end_idx]["number"].values
                num_elements = len(sheet_data)
                cols = min((num_elements + rows_per_sheet - 1) // rows_per_sheet, 1000)
                rows = min(num_elements, rows_per_sheet)
                if num_elements < rows * cols:
                    padding = np.array([''] * (rows * cols - num_elements))
                    sheet_data = np.concatenate((sheet_data, padding))
                elif num_elements > rows * cols:
                    sheet_data = sheet_data[:rows * cols]
                sheet_df = pd.DataFrame(sheet_data.reshape(rows, cols, order='F'))
                sheet_df.to_excel(writer, sheet_name=f"Z{i + 1}", index=False, header=False)
        file_stream.seek(0)
        s3_client.put_object(Body=file_stream, Bucket="my-bucket-sun-test", Key=output_key)
        results.append((output_key, sheets_in_file))
    return results

def save_to_excel_and_upload_to_s3(df, s3_base_path, remaining_sheets):
    global total_sheets_processed, total_numbers_processed, file_paths
    total_rows = df.count()
    logger.info(f"Total numbers to process: {total_rows}")
    results = (df.select("number")
               .rdd.mapPartitionsWithIndex(lambda idx, it: write_partition_to_excel(it, s3_base_path, idx, remaining_sheets))
               .collect())
    batch_sheets = sum(sheet_count for _, sheet_count in results)
    total_sheets_processed += batch_sheets
    total_numbers_processed += total_rows
    file_paths.extend(results)  # Store (path, sheet_count) tuples
    logger.info(f"Completed saving {total_rows} numbers across {len(results)} files with {batch_sheets} sheets")

def combine_into_10_sheet_files(partition_idx, iterator):
    s3_client = boto3.client('s3')
    files_to_process = list(iterator)  # List of (path, sheet_count)
    if not files_to_process:
        return []
    
    now = datetime.datetime.now()
    base_path = f"s3://my-bucket-sun-test/temp/ingest_year={now.year:0>4}/ingest_month={now.month:0>2}/ingest_day={now.day:0>2}/standardized_part_{partition_idx}"
    results = []
    sheet_buffer = []
    sheets_written = 0
    
    for file_path, sheet_count in files_to_process:
        obj = s3_client.get_object(Bucket="my-bucket-sun-test", Key=file_path)
        file_data = pd.ExcelFile(obj['Body'].read())
        for sheet_name in file_data.sheet_names:
            sheet_df = pd.read_excel(file_data, sheet_name=sheet_name, header=None)
            sheet_buffer.append(sheet_df)
            
            if len(sheet_buffer) == 10:  # Write when we have 10 sheets
                output_key = f"{base_path}_file_{sheets_written // 10}.xlsx"
                file_stream = BytesIO()
                with pd.ExcelWriter(file_stream, engine='openpyxl') as writer:
                    for i, df in enumerate(sheet_buffer):
                        df.to_excel(writer, sheet_name=f"Z{i + 1}", index=False, header=False)
                file_stream.seek(0)
                s3_client.put_object(Body=file_stream, Bucket="my-bucket-sun-test", Key=output_key)
                results.append((output_key, 10))
                sheet_buffer = []
                sheets_written += 10
    
    # Handle remaining sheets (last file may have <10)
    if sheet_buffer:
        output_key = f"{base_path}_file_{sheets_written // 10}.xlsx"
        file_stream = BytesIO()
        with pd.ExcelWriter(file_stream, engine='openpyxl') as writer:
            for i, df in enumerate(sheet_buffer):
                df.to_excel(writer, sheet_name=f"Z{i + 1}", index=False, header=False)
        file_stream.seek(0)
        s3_client.put_object(Body=file_stream, Bucket="my-bucket-sun-test", Key=output_key)
        results.append((output_key, len(sheet_buffer)))
    
    logger.info(f"Partition {partition_idx}: Combined into {len(results)} files, last file has {results[-1][1] if results else 0} sheets")
    return results

def combine_files_to_single_excel(partition_idx, iterator):
    s3_client = boto3.client('s3')
    files_to_process = list(iterator)
    if not files_to_process:
        return []
    now = datetime.datetime.now()
    output_key = f"s3://my-bucket-sun-test/temp/ingest_year={now.year:0>4}/ingest_month={now.month:0>2}/ingest_day={now.day:0>2}/combined_1000_sheets_part_{partition_idx}.xlsx"
    file_stream = BytesIO()
    with pd.ExcelWriter(file_stream, engine='openpyxl') as writer:
        sheets_written = 0
        target_sheets = 100
        for file_path, _ in files_to_process:
            if sheets_written >= target_sheets:
                break
            obj = s3_client.get_object(Bucket="my-bucket-sun-test", Key=file_path)
            file_data = pd.ExcelFile(obj['Body'].read())
            for sheet_name in file_data.sheet_names:
                if sheets_written >= target_sheets:
                    break
                sheet_df = pd.read_excel(file_data, sheet_name=sheet_name, header=None)
                new_idx = partition_idx * target_sheets + sheets_written + 1
                sheet_df.to_excel(writer, sheet_name=f"Z{new_idx}", index=False, header=False)
                sheets_written += 1
    file_stream.seek(0)
    s3_client.put_object(Body=file_stream, Bucket="my-bucket-sun-test", Key=output_key)
    logger.info(f"Partition {partition_idx}: Combined {sheets_written} sheets into {output_key}")
    return [(output_key, sheets_written)]

def processBatch(data_frame, batchId):
    global batch_counter, total_sheets_processed
    batch_counter += 1
    logger.info(f"Batch {batchId}: Starting processing (Batch #{batch_counter})")
    initial_count = data_frame.count()
    logger.info(f"Batch {batchId}: Fetched {initial_count} records from Kinesis")

    if initial_count > 0 and total_sheets_processed < 1000:
        dynamic_frame = DynamicFrame.fromDF(data_frame, glueContext, "from_data_frame")
        transform_result = MyTransform(glueContext, DynamicFrameCollection({"AmazonKinesis_node": dynamic_frame}, glueContext))
        if "transformed_number" not in transform_result.keys():
            return
        transformed_df = transform_result["transformed_number"].toDF()

        total_numbers = transformed_df.count()
        remaining_numbers = 1_000_000_000 - total_numbers_processed
        if total_numbers_processed + total_numbers > 1_000_000_000:
            transformed_df = transformed_df.limit(remaining_numbers)
            total_numbers = remaining_numbers
        
        logger.info(f"Batch {batchId}: Contains {total_numbers} transformed numbers")
        logger.info(f"Batch {batchId}: Checking number distribution before saving")
        transformed_df.groupBy("number").agg(count("number").alias("count")).orderBy(col("count").desc()).show(10)
        
        now = datetime.datetime.now()
        s3_base_path = f"s3://my-bucket-sun-test/temp/ingest_year={now.year:0>4}/ingest_month={now.month:0>2}/ingest_day={now.day:0>2}/output_batch_{batchId}"
        remaining_sheets = 1000 - total_sheets_processed
        save_to_excel_and_upload_to_s3(transformed_df, s3_base_path, remaining_sheets)
        logger.info(f"Processed batch {batchId} with {total_numbers} numbers")
        
        if total_sheets_processed >= 1000:
            logger.info("Reached 1,000 sheets, stopping batch processing.")
            raise StopIteration
    logger.info(f"Total batches processed so far: {batch_counter}")

# Kinesis Stream
dataframe = glueContext.create_data_frame.from_options(
    connection_type="kinesis",
    connection_options={
        "typeOfData": "kinesis",
        "streamARN": "arn:aws:kinesis:us-east-1:851725381788:stream/ExpertStream",
        "classification": "json",
        "startingPosition": "earliest",
        "inferSchema": "true"
    },
    transformation_ctx="kinesis_node"
)

# Process batches
glueContext.forEachBatch(
    frame=dataframe,
    batch_function=processBatch,
    options={"windowSize": "100 seconds", "checkpointLocation": args["TempDir"] + "/" + args["JOB_NAME"] + "/checkpoint/"}
)

# Standardize to 10-sheet files
if total_sheets_processed >= 1000:
    logger.info("Standardizing files to 10 sheets each across executors.")
    file_rdd = spark.sparkContext.parallelize(file_paths, numSlices=10)
    standardized_files = file_rdd.mapPartitionsWithIndex(combine_into_10_sheet_files).collect()
    logger.info(f"Generated {len(standardized_files)} standardized files, last may have <10 sheets.")
    
    # Combine into final file
    logger.info("Combining 1,000 sheets into a single file across executors.")
    file_rdd = spark.sparkContext.parallelize(standardized_files, numSlices=10)
    combined_results = file_rdd.mapPartitionsWithIndex(combine_files_to_single_excel).collect()
    logger.info(f"Generated {len(combined_results)} partial files, each with up to 100 sheets.")
    
    s3_client = boto3.client('s3')
    now = datetime.datetime.now()
    final_key = f"s3://my-bucket-sun-test/temp/ingest_year={now.year:0>4}/ingest_month={now.month:0>2}/ingest_day={now.day:0>2}/final_1000_sheets.xlsx"
    file_stream = BytesIO()
    with pd.ExcelWriter(file_stream, engine='openpyxl') as writer:
        sheet_idx = 1
        for part_file, _ in combined_results:
            obj = s3_client.get_object(Bucket="my-bucket-sun-test", Key=part_file)
            file_data = pd.ExcelFile(obj['Body'].read())
            for sheet_name in file_data.sheet_names:
                sheet_df = pd.read_excel(file_data, sheet_name=sheet_name, header=None)
                sheet_df.to_excel(writer, sheet_name=f"Z{sheet_idx}", index=False, header=False)
                sheet_idx += 1
    file_stream.seek(0)
    s3_client.put_object(Body=file_stream, Bucket="my-bucket-sun-test", Key=final_key)
    logger.info(f"Saved final combined file with 1,000 sheets to {final_key}")

job.commit()
logger.info(f"Glue job completed. Total batches processed: {batch_counter}")