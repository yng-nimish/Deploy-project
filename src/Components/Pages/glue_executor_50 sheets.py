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

# Global counter for batches
batch_counter = 0

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    dynamic_frame = dfc["AmazonKinesis_node"]
    df = dynamic_frame.toDF()
    record_count = df.count()
    logger.info(f"MyTransform: Received {record_count} records")

    if record_count == 0:
        logger.info("MyTransform: No data in batch.")
        return DynamicFrameCollection({}, glueContext)

    try:
        df.printSchema()
        logger.info(f"MyTransform: Input schema logged")
        df_unpacked = df.select(explode(col("numbers")).alias("number"))
        df_transformed = df_unpacked.withColumn(
            "number", lpad(col("number").cast("string"), 3, "0")
        )
        logger.info(f"MyTransform: Transformed numbers from {record_count} records")
        return DynamicFrameCollection({"transformed_number": DynamicFrame.fromDF(df_transformed, glueContext, "transformed")}, glueContext)
    except Exception as e:
        logger.error(f"MyTransform: Transform failed: {e}")
        return DynamicFrameCollection({}, glueContext)

def write_partition_to_excel(iterator, s3_base_path, partition_idx):
    """Executor function to write partitions to Excel files with 10 sheets each."""
    s3_client = boto3.client('s3')
    numbers = [row["number"] for row in iterator]
    if not numbers:
        logger.info(f"Partition {partition_idx}: No numbers to process")
        return []

    df = pd.DataFrame(numbers, columns=["number"])
    total_numbers = len(numbers)
    rows_per_sheet = 1000
    numbers_per_sheet = rows_per_sheet * 1000  # Max 1M numbers per sheet
    sheet_count = (total_numbers + numbers_per_sheet - 1) // numbers_per_sheet

    sheets_per_file = 10  # Save every 10 sheets
    results = []

    logger.info(f"Partition {partition_idx}: Processing {total_numbers} numbers into {sheet_count} sheets")

    for file_start in range(0, sheet_count, sheets_per_file):
        file_idx = partition_idx * (sheet_count // sheets_per_file + 1) + (file_start // sheets_per_file)
        output_key = f"{s3_base_path}_partition_{partition_idx}_file_{file_idx}_sheets_{file_start + 1}_to_{min(file_start + sheets_per_file, sheet_count)}.xlsx"
        file_stream = BytesIO()

        with pd.ExcelWriter(file_stream, engine='openpyxl') as writer:
            sheets_in_file = min(sheets_per_file, sheet_count - file_start)
            for i in range(file_start, file_start + sheets_in_file):
                start_idx = i * numbers_per_sheet
                end_idx = min(start_idx + numbers_per_sheet, total_numbers)
                sheet_data = df.iloc[start_idx:end_idx]["number"].values
                num_elements = len(sheet_data)
                
                # Dynamically calculate columns, ensure 1,000 rows
                cols = (num_elements + rows_per_sheet - 1) // rows_per_sheet
                cols = min(cols, 1000)  # Cap at 1,000 columns
                rows = min(num_elements, rows_per_sheet)  # Up to 1,000 rows
                
                # Pad or truncate data
                if num_elements < rows * cols:
                    padding = np.array([''] * (rows * cols - num_elements))
                    sheet_data = np.concatenate((sheet_data, padding))
                elif num_elements > rows * cols:
                    sheet_data = sheet_data[:rows * cols]
                
                # Reshape to fit 1,000 rows x dynamic columns
                sheet_df = pd.DataFrame(sheet_data.reshape(rows, cols, order='F'))
                sheet_name = f"Z{i + 1}"
                sheet_df.to_excel(writer, sheet_name=sheet_name, index=False, header=False)

        file_stream.seek(0)
        s3_client.put_object(Body=file_stream, Bucket="my-bucket-sun-test", Key=output_key)
        logger.info(f"Partition {partition_idx}: Saved {output_key} with {sheets_in_file} sheets")
        results.append((output_key, sheets_in_file))

    return results

def save_to_excel_and_upload_to_s3(df, s3_base_path):
    logger.info("Entered excel Function 1")
    try:
        total_rows = df.count()
        logger.info(f"Total numbers to process: {total_rows}")
        logger.info(f"Number of partitions: {df.rdd.getNumPartitions()}")

        # Use natural partitioning
        results = (df.select("number")
                  .rdd.mapPartitionsWithIndex(lambda idx, it: write_partition_to_excel(it, s3_base_path, idx))
                  .collect())

        total_sheets = sum(sheet_count for _, sheet_count in results)
        file_count = len(results)
        logger.info(f"Completed saving {total_rows} numbers across {file_count} files with {total_sheets} sheets")

    except Exception as e:
        logger.error(f"Excel save/upload failed: {e}")
        raise
    finally:
        logger.handlers[0].flush()

def processBatch(data_frame, batchId):
    global batch_counter
    batch_counter += 1
    try:
        logger.info(f"Batch {batchId}: Starting processing (Batch #{batch_counter})")
        initial_count = data_frame.count()
        logger.info(f"Batch {batchId}: Fetched {initial_count} records from Kinesis")
        logger.info(f"Total records fetched so far: {initial_count}")

        if initial_count > 0:
            dynamic_frame = DynamicFrame.fromDF(data_frame, glueContext, "from_data_frame")
            transform_result = MyTransform(glueContext, DynamicFrameCollection({"AmazonKinesis_node": dynamic_frame}, glueContext))
            
            if "transformed_number" not in transform_result.keys():
                logger.info(f"Batch {batchId}: No transformed data to process.")
                return

            transformed_dynamic_frame = transform_result["transformed_number"]
            transformed_df = transformed_dynamic_frame.toDF()
            transformed_df.show(7)
            
            total_numbers = transformed_df.count()
            logger.info(f"Batch {batchId}: Contains {total_numbers} transformed numbers")
            logger.info(f"Total numbers fetched so far: {total_numbers}")
            logger.info(f"Number of numbers in Batch {batchId}: {total_numbers}")

            transformed_df.show(8)

            now = datetime.datetime.now()
            s3_base_path = f"s3://my-bucket-sun-test/temp/ingest_year={now.year:0>4}/ingest_month={now.month:0>2}/ingest_day={now.day:0>2}/output_batch_{batchId}"

            logger.info(f"Batch {batchId}: Checking number distribution before saving")
            transformed_df.groupBy("number").agg(count("number").alias("count")).orderBy(col("count").desc()).show(10)

            save_to_excel_and_upload_to_s3(transformed_df, s3_base_path)
            logger.info(f"Processed batch {batchId} with {total_numbers} numbers")
        else:
            logger.info(f"Batch {batchId}: No data fetched from Kinesis")
    except Exception as e:
        logger.error(f"Batch {batchId} processing failed: {e}")
        raise
    finally:
        logger.info(f"Total batches processed so far: {batch_counter}")

dataframe_AmazonKinesis_node1743426800728 = glueContext.create_data_frame.from_options(
    connection_type="kinesis",
    connection_options={
        "typeOfData": "kinesis",
        "streamARN": "arn:aws:kinesis:us-east-1:851725381788:stream/ExpertStream",
        "classification": "json",
        "startingPosition": "earliest",
        "inferSchema": "true"
    },
    transformation_ctx="dataframe_AmazonKinesis_node1743426800728"
)

glueContext.forEachBatch(
    frame=dataframe_AmazonKinesis_node1743426800728,
    batch_function=processBatch,
    options={"windowSize": "100 seconds", "checkpointLocation": args["TempDir"] + "/" + args["JOB_NAME"] + "/checkpoint/"}
)
job.commit()
logger.info(f"Glue job completed. Total batches processed: {batch_counter}")