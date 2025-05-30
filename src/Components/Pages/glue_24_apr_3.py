import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lpad, floor, monotonically_increasing_id
from pyspark.sql.types import IntegerType
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
logger.info("Spark configurations applied: parallelism=400, AQE enabled")

# Initialize Glue Job
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def processBatch(data_frame, batchId):
    try:
        initial_count = data_frame.count()
        logger.info(f"Batch {batchId}: Fetched {initial_count} records from Kinesis")

        if initial_count == 0:
            logger.info(f"Batch {batchId}: Empty batch, skipping")
            return

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

        # Assign x and y coordinates
        df_positioned = (df_transformed
                         .withColumn("idx", monotonically_increasing_id())
                         .withColumn("x_coordinate", (floor(col("idx") / 1000) + 1).cast(IntegerType()))
                         .withColumn("y_coordinate", ((col("idx") % 1000) + 1).cast(IntegerType()))
                         .select("x_coordinate", "y_coordinate", "number"))

        # Write to Parquet in Test1 directory
        output_path = f"{args['OutputS3Path']}/Test1"
        df_positioned.write.mode("append").parquet(output_path)
        logger.info(f"Batch {batchId}: Wrote Parquet data to {output_path}")

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
    options={"windowSize": "100 seconds", "checkpointLocation": args["TempDir"] + "/" + args["JOB_NAME"] + "/checkpoint/"}
)

job.commit()
logger.info("Glue job completed successfully")