import sys
import datetime
import logging
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from pyspark.sql.functions import udf

# Initialize logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# UDF to pad numbers to three digits
def pad_to_three_digits(number):
    try:
        str_num = str(number).strip()
        if len(str_num) == 3:
            return str_num
        elif len(str_num) == 2:
            return '0' + str_num
        elif len(str_num) == 1:
            return '00' + str_num
        else:
            return None  # Handle unexpected input if needed
    except Exception as e:
        logger.error(f"Error in pad_to_three_digits function: {str(e)}")
        return None

# Register UDF
pad_udf = udf(pad_to_three_digits, StringType())

# AWS Glue job setup
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'TempDir'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon Kinesis
dataframe_AmazonKinesis_node1736439831674 = glueContext.create_data_frame.from_options(
    connection_type="kinesis",
    connection_options={
        "typeOfData": "kinesis",
        "streamARN": "arn:aws:kinesis:us-east-1:851725381788:stream/mystream",
        "classification": "json",
        "startingPosition": "earliest",
        "inferSchema": "true"
    },
    transformation_ctx="dataframe_AmazonKinesis_node1736439831674"
)

# Process each batch of incoming data
def processBatch(data_frame, batchId):
    try:
        if data_frame.count() > 0:
            logger.info(f"Processing batch {batchId}, data count: {data_frame.count()}")
            
            # Apply the UDF to pad the numbers
            data_frame = data_frame.withColumn("padded_column", pad_udf(data_frame["number"]))
            
            # Log the schema for code review
            logger.info("DataFrame schema:")
            data_frame.printSchema()

            # Convert to DynamicFrame
            AmazonKinesis_node1736439831674 = DynamicFrame.fromDF(data_frame, glueContext, "from_data_frame")

            # Get current date and time for partitioning
            now = datetime.datetime.now()
            year = now.year
            month = now.month
            day = now.day
            hour = now.hour

            # Generate S3 path
            AmazonS3_node1736439835438_path = f"s3://my-bucket-sun-test/Output/ingest_year={year:04}/ingest_month={month:02}/ingest_day={day:02}/ingest_hour={hour:02}/"

            # Write to S3 in JSON format
            glueContext.write_dynamic_frame.from_options(
                frame=AmazonKinesis_node1736439831674,
                connection_type="s3",
                format="json",
                connection_options={"path": AmazonS3_node1736439835438_path, "partitionKeys": []},
                transformation_ctx="AmazonS3_node1736439835438"
            )

            # Convert to Pandas DataFrame for Excel processing
            pandas_df = data_frame.toPandas()

            # Log the first few rows for debugging
            logger.info(f"Pandas DataFrame sample:\n{pandas_df.head()}")

            # Write the data to Excel in chunks
            writer = pd.ExcelWriter(f"/tmp/output_batch_{batchId}.xlsx", engine='xlsxwriter')
            rows_per_sheet = 1000
            columns_per_sheet = 1000
            total_rows = len(pandas_df)
            total_sheets = (total_rows // rows_per_sheet) + (1 if total_rows % rows_per_sheet != 0 else 0)
            
            for sheet_index in range(total_sheets):
                start_row = sheet_index * rows_per_sheet
                end_row = min((sheet_index + 1) * rows_per_sheet, total_rows)
                sheet_df = pandas_df.iloc[start_row:end_row, :columns_per_sheet]

                sheet_name = f"Sheet{sheet_index + 1}"
                sheet_df.to_excel(writer, sheet_name=sheet_name, index=False)

            writer.save()
            logger.info(f"Data written to Excel file /tmp/output_batch_{batchId}.xlsx")

    except Exception as e:
        logger.error(f"Error processing batch {batchId}: {str(e)}")

# Process batches using Glue's forEachBatch function
glueContext.forEachBatch(
    frame=dataframe_AmazonKinesis_node1736439831674,
    batch_function=processBatch,
    options={"windowSize": "10 seconds", "checkpointLocation": args["TempDir"] + "/" + args["JOB_NAME"] + "/checkpoint/"}
)

job.commit()
