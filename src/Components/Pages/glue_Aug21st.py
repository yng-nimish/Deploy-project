import sys
import json
import boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lpad, row_number, lit
from pyspark.sql.window import Window
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
spark.conf.set("spark.default.parallelism", "200")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "200")
logger.info("Spark configurations applied: parallelism=200, shuffle.partitions=200, AQE enabled")

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
        folder_index = state.get('folder_index', 99)  # Initialize to 99 if no state
        file_count = state.get('file_count', 0)
        logger.info(f"Read state: folder_index={folder_index}, file_count={file_count}")
        return folder_index, file_count
    except s3_client.exceptions.NoSuchKey:
        logger.info("No state file found, initializing with folder_index=99, file_count=0")
        return 99, 0
    except Exception as e:
        logger.error(f"Failed to read state: {str(e)}")
        return 99, 0

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
        # Format folder name with 4-digit padding starting from F 0036
        folder_name = f"F {str(folder_index).zfill(4)}"
        logger.info(f"Batch {batchId}: Processing folder {folder_name}")

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

        # Calculate number of complete files (1M numbers each)
        cells_per_file = 1000 * 1000  # 1M cells per file
        num_files = total_numbers // cells_per_file
        max_rows = num_files * cells_per_file

        if num_files == 0:
            logger.info(f"Batch {batchId}: Not enough numbers ({total_numbers}) for a complete file")
            return

        # Assign coordinates and file indices, limit to complete files
        df_positioned = (df_transformed
                         .withColumn("row_num", row_number().over(Window.orderBy(lit(1))))
                         .filter(col("row_num") <= max_rows)
                         .withColumn("cell_idx", ((col("row_num") - 1) % cells_per_file).cast("int"))
                         .withColumn("x_coordinate", (col("cell_idx") / 1000).cast("int") + 1)
                         .withColumn("y_coordinate", (col("cell_idx") % 1000).cast("int") + 1)
                         .withColumn("file_idx", ((col("row_num") - 1) / cells_per_file).cast("int") + 1)
                         .select("x_coordinate", "y_coordinate", "number", "file_idx"))

        # Log discarded numbers
        discarded_numbers = total_numbers - max_rows
        if discarded_numbers > 0:
            logger.warning(f"Batch {batchId}: Discarded {discarded_numbers} numbers")

        # Log file_idx distribution before repartitioning
        file_idx_counts = df_positioned.groupBy("file_idx").count().collect()
        logger.info(f"Batch {batchId}: file_idx distribution: {[(row['file_idx'], row['count']) for row in file_idx_counts]}")

        # Repartition by file_idx with exact number of partitions
        logger.info(f"Batch {batchId}: Repartitioning to {num_files} partitions for file_idx")
        df_positioned = df_positioned.repartition(num_files, "file_idx").cache()

        # Log distinct file_idx values for debugging
        distinct_file_idx = df_positioned.select("file_idx").distinct().count()
        logger.info(f"Batch {batchId}: Found {distinct_file_idx} distinct file_idx values")

        # Disable AQE for write operation to preserve partitions
        spark.conf.set("spark.sql.adaptive.enabled", "false")

        # Write each file_idx to a separate Parquet file
        new_files_written = 0
        for i in range(num_files):
            current_file_idx = file_count + new_files_written + 1
            if current_file_idx > 1000:
                folder_index += 1
                file_count = 0
                new_files_written = 0
                folder_name = f"F {str(folder_index).zfill(4)}"
                current_file_idx = 1
                logger.info(f"Batch {batchId}: Rolled over to new folder {folder_name}")

            # Format file name with 3-digit padding (Z001, Z002, etc.)
            file_name = f"Z{str(current_file_idx).zfill(3)}"
            output_path = f"{args['OutputS3Path']}/{folder_name}/{file_name}"
            
            # Filter for the current file_idx (i+1 since file_idx starts at 1)
            df_file = df_positioned.filter(col("file_idx") == (i + 1))
            file_row_count = df_file.count()
            if file_row_count == cells_per_file:  # Ensure exactly 1M rows
                logger.info(f"Batch {batchId}: Writing {file_row_count} rows to {output_path}")
                (df_file.select("x_coordinate", "y_coordinate", "number")
                        .write.mode("append")
                        .parquet(output_path))
                logger.info(f"Batch {batchId}: Wrote Parquet file {output_path}")
                new_files_written += 1
            else:
                logger.warning(f"Batch {batchId}: Skipped file_idx {i+1} with {file_row_count} rows (expected {cells_per_file}) for {output_path}")

        # Re-enable AQE
        spark.conf.set("spark.sql.adaptive.enabled", "true")

        df_positioned.unpersist()

        # Update state 
        file_count += new_files_written
        if file_count >= 1000:
            folder_index += 1
            file_count = file_count % 1000
            logger.info(f"Batch {batchId}: Incremented folder_index to {folder_index}, reset file_count to {file_count}")
        write_state(folder_index, file_count)
        logger.info(f"Batch {batchId}: Completed writing {new_files_written} files, total file_count={file_count}")

    except Exception as e:
        logger.error(f"Batch {batchId}: Processing failed: {str(e)}", exc_info=True)

# Script generated for node Amazon Kinesis
dataframe_AmazonKinesis_node1755779992163 = glueContext.create_data_frame.from_options(connection_type="kinesis",connection_options={"typeOfData": "kinesis", "streamARN": "arn:aws:kinesis:us-east-1:851725381788:stream/Aug21st", "classification": "json", "startingPosition": "earliest", "inferSchema": "true"}, transformation_ctx="dataframe_AmazonKinesis_node1755779992163")



glueContext.forEachBatch(frame = dataframe_AmazonKinesis_node1755779992163, batch_function = processBatch, options = {"windowSize": "80 seconds", "checkpointLocation": args["TempDir"] + "/" + args["JOB_NAME"] + "/checkpoint/"})
job.commit()
logger.info("Glue job completed successfully")