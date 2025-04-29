import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, explode, pandas_udf, monotonically_increasing_id, lit, struct
from pyspark.sql.types import StringType, StructType, StructField, IntegerType
from awsglue import DynamicFrame
from awsglue.dynamicframe import DynamicFrameCollection
import datetime
import boto3
from io import BytesIO
import pandas as pd
import logging
import os

# Logging Setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)

# Get job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'TempDir', 'S3_OUTPUT_BUCKET'])
s3_output_bucket = args['S3_OUTPUT_BUCKET']

# Initialize GlueContext
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

# Spark Configurations for Performance
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")
spark.conf.set("spark.default.parallelism", "200")
spark.conf.set("spark.sql.shuffle.partitions", "200")
# Consider increasing these based on your worker type and data volume
# spark.conf.set("spark.executor.memory", "6g")
# spark.conf.set("spark.driver.memory", "4g")
# spark.conf.set("spark.executor.cores", "4")
logger.info("Spark configurations set")

# Initialize Glue Job
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Vectorized UDF for padding numbers (ensuring string output)
@pandas_udf(StringType())
def vectorized_lpad(series: pd.Series) -> pd.Series:
    return series.astype(str).str.zfill(3)
spark.udf.register("vectorized_lpad", vectorized_lpad)

# Custom Transform
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    dynamic_frame = dfc["AmazonKinesis_node"]
    df = dynamic_frame.toDF()
    record_count = df.count()
    logger.info(f"MyTransform: Received {record_count} records")
    if record_count == 0:
        logger.info("MyTransform: No data in batch")
        return DynamicFrameCollection({}, glueContext)
    try:
        # Ensure numbers are treated as strings from the start
        df_unpacked = df.select(explode(col("numbers")).alias("number")).withColumn("number", col("number").cast("string"))
        # Apply padding to ensure leading zeroes are preserved
        df_transformed = df_unpacked.withColumn("padded_number", vectorized_lpad(col("number")))
        dynamic_frame_transformed = DynamicFrame.fromDF(df_transformed, glueContext, "transformed")
        logger.info(f"MyTransform: Transformed {df_transformed.count()} numbers")
        return DynamicFrameCollection({"transformed_number": dynamic_frame_transformed}, glueContext)
    except Exception as e:
        logger.error(f"MyTransform: Transform failed: {str(e)}", exc_info=True)
        return DynamicFrameCollection({}, glueContext)

# Function to write a partition of data to an Excel file with 1000 sheets
def write_excel_partition(partition_id, iterator):
    s3_client = boto3.client('s3')
    all_numbers = [row.asDict()['padded_number'] for row in iterator]

    if not all_numbers:
        return

    num_records = len(all_numbers)
    records_per_sheet = 1000 * 1000
    num_sheets = (num_records + records_per_sheet - 1) // records_per_sheet
    sheets_per_workbook = 1000
    num_workbooks = (num_sheets + sheets_per_workbook - 1) // sheets_per_workbook

    for workbook_index in range(num_workbooks):
        workbook_buffer = BytesIO()
        with pd.ExcelWriter(workbook_buffer, engine='xlsxwriter') as writer:
            start_sheet = workbook_index * sheets_per_workbook
            end_sheet = min((workbook_index + 1) * sheets_per_workbook, num_sheets)

            for sheet_index_within_workbook in range(start_sheet, end_sheet):
                global_sheet_index = sheet_index_within_workbook
                start_row = global_sheet_index * records_per_sheet
                end_row = min((global_sheet_index + 1) * records_per_sheet, num_records)
                sheet_data = all_numbers[start_row:end_row]

                if sheet_data:
                    rows = [sheet_data[i:i + 1000] for i in range(0, len(sheet_data), 1000)]
                    df_sheet = pd.DataFrame(rows)
                    df_sheet.to_excel(writer, sheet_name=f"Sheet{global_sheet_index + 1}", index=False, header=False)

        workbook_buffer.seek(0)
        now = datetime.datetime.now()
        s3_key = f"ingest_year={now.year:0>4}/ingest_month={now.month:0>2}/ingest_day={now.day:0>2}/workbook_partition_{partition_id}_wb_{workbook_index + 1}.xlsx"
        s3_client.upload_fileobj(workbook_buffer, s3_output_bucket, s3_key)
        logger.info(f"Uploaded s3://{s3_output_bucket}/{s3_key} with sheets {start_sheet + 1} to {end_sheet}")

# Batch Processing Function
def processBatch(data_frame, batchId):
    try:
        initial_count = data_frame.count()
        logger.info(f"Batch {batchId}: Fetched {initial_count} records from Kinesis")
        if initial_count > 0:
            dynamic_frame = DynamicFrame.fromDF(data_frame, glueContext, "from_data_frame")
            transform_result = MyTransform(glueContext, DynamicFrameCollection({"AmazonKinesis_node": dynamic_frame}, glueContext))
            if "transformed_number" in transform_result.keys():
                transformed_dynamic_frame = transform_result["transformed_number"]
                transformed_df = transformed_dynamic_frame.toDF()
                total_transformed = transformed_df.count()
                logger.info(f"Batch {batchId}: Contains {total_transformed} transformed numbers")

                # Process each existing partition to write Excel files
                transformed_df.rdd.mapPartitionsWithIndex(write_excel_partition).foreach(lambda _: None)

                now = datetime.datetime.now()
                parquet_path = f"s3://{s3_output_bucket}/parquet_backup/ingest_year={now.year:0>4}/ingest_month={now.month:0>2}/ingest_day={now.day:0>2}/batch_{batchId}"
                transformed_df.write.mode("overwrite").parquet(parquet_path)
                logger.info(f"Batch {batchId}: Saved parquet backup to {parquet_path}")
            else:
                logger.info(f"Batch {batchId}: No transformed data to process for Excel")
    except Exception as e:
        logger.error(f"Batch {batchId} processing failed: {str(e)}", exc_info=True)

# The rest of the script (imports, setup, Kinesis connection) remains the same.
# Kinesis Stream Setup
dataframe_AmazonKinesis_node1744136564897 = glueContext.create_data_frame.from_options(
    connection_type="kinesis",
    connection_options={
        "typeOfData": "kinesis",
        "streamARN": "arn:aws:kinesis:us-east-1:851725381788:stream/DeepStream",
        "classification": "json",
        "startingPosition": "earliest",
        "inferSchema": "true"
    },
    transformation_ctx="dataframe_AmazonKinesis_node1744136564897"
)

glueContext.forEachBatch(
    frame=dataframe_AmazonKinesis_node1744136564897,
    batch_function=processBatch,
    options={"windowSize": "100 seconds", "checkpointLocation": args["TempDir"] + "/" + args["JOB_NAME"] + "/checkpoint/"}
)

job.commit()
logger.info("Glue job completed successfully")