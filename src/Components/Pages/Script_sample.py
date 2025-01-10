import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql import DataFrame
import datetime
from awsglue import DynamicFrame

# Get the job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define the UDF to pad numbers to three digits
def pad_to_three_digits(number):
    str_num = str(number).strip()
    if len(str_num) == 3:
        return str_num
    elif len(str_num) == 2:
        return '0' + str_num
    elif len(str_num) == 1:
        return '00' + str_num
    else:
        return None  # Or handle unexpected input as needed

# Register the UDF
pad_udf = udf(pad_to_three_digits, StringType())

# Script generated for node Amazon Kinesis
dataframe_AmazonKinesis_node1736267829891 = glueContext.create_data_frame.from_options(
    connection_type="kinesis",
    connection_options={
        "typeOfData": "kinesis",
        "streamARN": "arn:aws:kinesis:us-east-1:851725381788:stream/sayhellostream",
        "classification": "json",
        "startingPosition": "earliest",
        "inferSchema": "true"
    },
    transformation_ctx="dataframe_AmazonKinesis_node1736267829891"
)

# Function to process the batch and apply the UDF
def processBatch(data_frame, batchId):
    if data_frame.count() > 0:
        # Apply the UDF to pad the 'number' column
        df_transformed = data_frame.withColumn("padded_number", pad_udf(data_frame["number"]))

        # Convert the DataFrame back to a DynamicFrame
        AmazonKinesis_node1736267829891 = DynamicFrame.fromDF(df_transformed, glueContext, "from_data_frame")

        # Get the current time for partitioning
        now = datetime.datetime.now()
        year = now.year
        month = now.month
        day = now.day
        hour = now.hour

        # Script generated for node Amazon S3
        AmazonS3_node1736267833922_path = "s3://my-bucket-sun-test/Output" + "/ingest_year=" + "{:0>4}".format(str(year)) + "/ingest_month=" + "{:0>2}".format(str(month)) + "/ingest_day=" + "{:0>2}".format(str(day)) + "/ingest_hour=" + "{:0>2}".format(str(hour))  + "/"
        
        # Write the transformed data to S3
        AmazonS3_node1736267833922 = glueContext.write_dynamic_frame.from_options(
            frame=AmazonKinesis_node1736267829891,
            connection_type="s3",
            format="json",
            connection_options={"path": AmazonS3_node1736267833922_path, "partitionKeys": []},
            transformation_ctx="AmazonS3_node1736267833922"
        )

# Process the stream in batches
glueContext.forEachBatch(
    frame=dataframe_AmazonKinesis_node1736267829891,
    batch_function=processBatch,
    options={"windowSize": "10 seconds", "checkpointLocation": args["TempDir"] + "/" + args["JOB_NAME"] + "/checkpoint/"}
)

# Commit the job
job.commit()
