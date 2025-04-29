import sys
import json
import boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lpad, monotonically_increasing_id
from awsglue.dynamicframe import DynamicFrame
import logging

# Logging Setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)

# Get job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'TempDir', 'OutputS3Path'])

# Initialize Spark and Glue contexts
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Spark Configurations
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "5000")
spark.conf.set("spark.default.parallelism", "400")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "400")  # Match parallelism for shuffles
logger.info("Spark configurations applied: parallelism=400, shuffle.partitions=400, AQE enabled")

# Initialize Glue Job
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# S3 client for state management
s3_client = boto3.client('s3')
state_file = f"{args['TempDir']}/{args['JOB_NAME']}/state.json"

def read_state():
    try:
        bucket, key = state_file.replace("s3://", "").split("/", 1)
        response = s3_client.get_object(Bucket=bucket, Key=key)
        state = json.loads(response['Body'].read().decode('utf-8'))
        return state.get('folder_index', 1), state.get('file_count', 0)
    except s3_client.exceptions.NoSuchKey:
        return 1, 0
    except Exception as e:
        logger.error(f"Failed to read state: {str(e)}")
        return 1, 0

def write_state(folder_index, file_count):
    try:
        bucket, key = state_file.replace("s3://", "").split("/", 1)
        state = {'folder_index': folder_index, 'file_count': file_count}
        s3_client.put_object(Bucket=bucket, Key=key, Body=json.dumps(state))
        logger.info(f"Updated state: folder_index={folder_index}, file_count={file_count}")
    except Exception as e:
        logger.error(f"Failed to write state: {str(e)}")

def processBatch(data_frame, batchId):
    try:
        initial_count = data_frame.count()
        logger.info(f"Batch {batchId}: Fetched {initial_count} records from Kinesis")

        if initial_count == 0:
            logger.info(f"Batch {batchId}: Empty batch, skipping")
            return

        # Read current state
        folder_index, file_count = read_state()
        folder_name = f"Test{folder_index}"

        # Convert to DynamicFrame for transformation
        dynamic_frame = DynamicFrame.fromDF(data_frame, glueContext, "kinesis_data")
        df = dynamic_frame.toDF()

        # Transform: Explode numbers and pad with zeros
        df_transformed = (df.select(explode(col("numbers")).alias("number"))
                         .withColumn("number", lpad(col("number").cast("string"), 3, "0")))

        total_numbers = df_transformed.count()
        logger.info(f"Batch {batchId}: Transformed {total_numbers} numbers")

        if total_numbers == 0:
            logger.info(f"Batch {batchId}: No numbers to process after transformation")
            return

        # Assign coordinates and file indices
        cells_per_file = 1000 * 1000  # 1M cells per file (1000 rows × 1000 cols)
        df_positioned = (df_transformed
                         .withColumn("idx", monotonically_increasing_id())
                         .withColumn("cell_idx", (col("idx") % cells_per_file).cast("int"))
                         .withColumn("x_coordinate", (col("cell_idx") / 1000).cast("int") + 1)
                         .withColumn("y_coordinate", (col("cell_idx") % 1000).cast("int") + 1)
                         .withColumn("file_idx", ((col("idx") / cells_per_file).cast("int") % 1000) + 1)
                         .select("x_coordinate", "y_coordinate", "number", "file_idx"))

        # Repartition by file_idx to distribute data evenly
        df_positioned = df_positioned.repartition("file_idx")

        # Write to Parquet files
        new_files_written = 0
        for file_row in df_positioned.select("file_idx").distinct().collect():
            current_file_idx = file_count + new_files_written + 1
            if current_file_idx > 1000:
                folder_index += 1
                file_count = 0
                new_files_written = 0
                folder_name = f"Test{folder_index}"
                current_file_idx = 1

            file_name = f"z{current_file_idx}"
            output_path = f"{args['OutputS3Path']}/{folder_name}/{file_name}"
            
            df_file = df_positioned.filter(col("file_idx") == file_row["file_idx"])
            (df_file.select("x_coordinate", "y_coordinate", "number")
                    .write.mode("append")
                    .parquet(output_path))
            logger.info(f"Batch {batchId}: Wrote Parquet file {output_path}")
            new_files_written += 1

        # Update state
        file_count += new_files_written
        if file_count >= 1000:
            folder_index += 1
            file_count = file_count % 1000
        write_state(folder_index, file_count)

    except Exception as e:
        logger.error(f"Batch {batchId}: Processing failed: {str(e)}", exc_info=True)

# Script generated for node Amazon Kinesis
dataframe_AmazonKinesis_node1745502816125 = glueContext.create_data_frame.from_options(
    connection_type="kinesis",
    connection_options={
        "typeOfData": "kinesis",
        "streamARN": "arn:aws:kinesis:us-east-1:851725381788:stream/April24ParquetTest",
        "classification": "json",
        "startingPosition": "earliest",
        "inferSchema": "true"
    },
    transformation_ctx="dataframe_AmazonKinesis_node1745502816125"
)

glueContext.forEachBatch(
    frame=dataframe_AmazonKinesis_node1745502816125,
    batch_function=processBatch,
    options={"windowSize": "100 seconds", "checkpointLocation": f"{args['TempDir']}/{args['JOB_NAME']}/checkpoint/"}
)

job.commit()
logger.info("Glue job completed successfully")