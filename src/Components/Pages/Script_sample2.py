import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, lpad
from pyspark.sql import DataFrame, Row
import datetime
from awsglue import DynamicFrame
from awsglue.dynamicframe import DynamicFrameCollection

# Script generated for node Custom Transform
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    # Get the input dynamic frame from the collection
    dynamic_frame = dfc["AmazonKinesis_node1736886714459"]
    
    # Convert to DataFrame
    df = dynamic_frame.toDF()

    # Check if DataFrame is empty
    if df.count() == 0:
        # Handle empty data case
        print("No data to process in this batch.")
        return DynamicFrameCollection({}, glueContext)
    
    # Check if the 'number' column exists
    if "number" not in df.columns:
        print("Column 'number' not found. Skipping transformation.")
        return DynamicFrameCollection({}, glueContext)

    # Perform the transformation: Pad 'number' column to 3 digits
    df_transformed = df.withColumn(
        "number",
        lpad(col("number").cast("string"), 3, "0")
    )
    
    # Convert back to DynamicFrame
    dynamic_frame_transformed = DynamicFrame.fromDF(df_transformed, glueContext, "dynamic_frame_transformed")

    # Return the transformed DynamicFrameCollection
    return DynamicFrameCollection({"transformed_number": dynamic_frame_transformed}, glueContext)


# Initialize Glue context and job
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'TempDir'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon Kinesis
dataframe_AmazonKinesis_node1736886714459 = glueContext.create_data_frame.from_options(
    connection_type="kinesis",
    connection_options={"typeOfData": "kinesis", "streamARN": "arn:aws:kinesis:us-east-1:851725381788:stream/Tiger", "classification": "json", "startingPosition": "earliest", "inferSchema": "true"},
    transformation_ctx="dataframe_AmazonKinesis_node1736886714459"
)

# Batch processing function
def processBatch(data_frame, batchId):
    if data_frame.count() > 0:
        # Convert to DynamicFrame for processing
        AmazonKinesis_node1736886714459 = DynamicFrame.fromDF(data_frame, glueContext, "from_data_frame")
        
        # Apply the custom transformation
        CustomTransform_node1736886714459 = MyTransform(glueContext, {"AmazonKinesis_node1736886714459": AmazonKinesis_node1736886714459})

        # Get the current date and time for partitioning
        now = datetime.datetime.now()
        year = now.year
        month = now.month
        day = now.day
        hour = now.hour

        # Script generated for node Amazon S3
        AmazonS3_node1736886782958_path = "s3://my-bucket-sun-test/Output" + "/ingest_year=" + "{:0>4}".format(str(year)) + "/ingest_month=" + "{:0>2}".format(str(month)) + "/ingest_day=" + "{:0>2}".format(str(day)) + "/ingest_hour=" + "{:0>2}".format(str(hour))  + "/"
        
        # Write the transformed data to S3
        glueContext.write_dynamic_frame.from_options(
            frame=CustomTransform_node1736886714459,
            connection_type="s3",
            format="json",
            connection_options={"path": AmazonS3_node1736886782958_path, "partitionKeys": []},
            transformation_ctx="AmazonS3_node1736886782958"
        )

# Execute the batch processing with the specified window size and checkpoint location
glueContext.forEachBatch(
    frame=dataframe_AmazonKinesis_node1736886714459,
    batch_function=processBatch,
    options={"windowSize": "10 seconds", "checkpointLocation": args["TempDir"] + "/" + args["JOB_NAME"] + "/checkpoint/"}
)

# Commit the job
job.commit()
