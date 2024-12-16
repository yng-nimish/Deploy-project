import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql import Row
import pandas as pd
import boto3
from io import BytesIO

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize SparkContext and GlueContext using the existing SparkContext
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)

# Kinesis streaming data frame reading
kinesis_stream_df = glueContext.create_data_frame.from_options(
    connection_type="kinesis",
    connection_options={
        "streamARN": "arn:aws:kinesis:us-east-1:851725381788:stream/bobisyouruncle",
        "starting_position": "TRIM_HORIZON"
    }
)

# Define a UDF to add leading zeroes to numerical values
def add_zeroes(value):
    try:
        return f"{int(value):03d}" if value is not None else None
    except Exception as e:
        return value  # Return the original value in case of non-numeric values

# Register the UDF to be used in Spark transformations
add_zeroes_udf = F.udf(add_zeroes, StringType())

# Process the stream: Apply transformations to the incoming stream
processed_df = kinesis_stream_df.select(
    [add_zeroes_udf(F.col(column)).alias(column) for column in kinesis_stream_df.columns]
)

# Now write the processed stream to S3 (or another output)
output_path = "s3://My-sun-test-bucket/"

# Write the output to S3 in micro-batches
query = processed_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("checkpointLocation", "s3://My-sun-test-bucket/") \
    .option("path", output_path) \
    .start()

# Wait for the streaming query to terminate
query.awaitTermination()

# Stop the SparkContext
sc.stop()
