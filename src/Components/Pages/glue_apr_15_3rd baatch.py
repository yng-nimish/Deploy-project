import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lpad, monotonically_increasing_id, floor, lit
from pyspark.sql.types import StringType, IntegerType
from awsglue.dynamicframe import DynamicFrame
import datetime
import logging

# Logging Setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)

# Get job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'TempDir'])

# Initialize Spark and Glue contexts
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Spark Configurations
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "5000")
spark.conf.set("spark.default.parallelism", "400")
spark.conf.set("spark.sql.shuffle.partitions", "400")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.executor.memory", "10g")
spark.conf.set("spark.executor.cores", "4")
spark.conf.set("spark.driver.memory", "8g")
spark.conf.set("spark.sql.streaming.microBatch.maxTriggers", "1000")
spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")  # 128 MB
spark.conf.set("spark.memory.fraction", "0.8")
spark.conf.set("spark.memory.storageFraction", "0.3")
logger.info("Spark configurations applied: parallelism=400, executor.memory=10g, AQE enabled")

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

        # Assign workbook, sheet, row, and column IDs
        cells_per_sheet = 1000 * 1000  # 1M cells per sheet
        sheets_per_workbook = 1000     # 1000 sheets per workbook
        cells_per_workbook = cells_per_sheet * sheets_per_workbook

        df_positioned = (df_transformed
                         .withColumn("idx", monotonically_increasing_id())
                         .withColumn("workbook_id", (floor(col("idx") / cells_per_workbook) + 1).cast(IntegerType()))
                         .withColumn("sheet_idx", floor((col("idx") % cells_per_workbook) / cells_per_sheet).cast(IntegerType()))
                         .withColumn("cell_idx", (col("idx") % cells_per_sheet).cast(IntegerType()))
                         .withColumn("row_id", (floor(col("cell_idx") / 1000) + 1).cast(IntegerType()))
                         .withColumn("col_id", ((col("cell_idx") % 1000) + 1).cast(IntegerType()))
                         .withColumn("sheet_name", lit("Z").cast(StringType()) + (col("sheet_idx") + 1).cast(StringType())))

        # Cache for multiple operations (no repartition to preserve randomness)
        df_positioned.cache()
        logger.info(f"Batch {batchId}: Assigned positions, using default Kinesis partitioning to preserve randomness")

        # Get unique workbook IDs (small collect, safe for a few IDs)
        workbook_ids = df_positioned.select("workbook_id").distinct().collect()
        workbook_ids = [row["workbook_id"] for row in workbook_ids]
        logger.info(f"Batch {batchId}: Processing {len(workbook_ids)} workbooks")

        # Generate timestamped S3 path
        now = datetime.datetime.now()
        s3_base_path = (f"s3://my-bucket-sun-test/output/"
                        f"ingest_year={now.year:0>4}/ingest_month={now.month:0>2}/ingest_day={now.day:0>2}/"
                        f"batch_{batchId}")

        # Process each workbook
        for workbook_id in workbook_ids:
            try:
                df_workbook = df_positioned.filter(col("workbook_id") == workbook_id)
                
                # Pivot data for each sheet within executors
                df_pivot = (df_workbook.groupBy("sheet_name", "row_id")
                            .pivot("col_id")
                            .agg(col("number"))
                            .orderBy("sheet_name", "row_id"))

                # Write directly to Excel using spark-excel
                output_path = f"{s3_base_path}/workbook_{workbook_id}.xlsx"
                (df_pivot.write
                 .format("com.crealytics.spark.excel")
                 .option("header", "false")
                 .option("sheetNameColumn", "sheet_name")
                 .mode("overwrite")
                 .save(output_path))

                logger.info(f"Batch {batchId}: Saved workbook {workbook_id} to {output_path}")
            except Exception as e:
                logger.error(f"Batch {batchId}: Failed to save workbook {workbook_id}: {str(e)}")
                continue

        # Save parquet backup for debugging
        df_positioned.write.mode("overwrite").parquet(f"{s3_base_path}_parquet")
        logger.info(f"Batch {batchId}: Saved parquet backup to {s3_base_path}_parquet")

        # Unpersist to free memory
        df_positioned.unpersist()
        
    except Exception as e:
        logger.error(f"Batch {batchId}: Processing failed: {str(e)}", exc_info=True)

# Kinesis Stream Setup
kinesis_frame = glueContext.create_data_frame.from_options(
    connection_type="kinesis",
    connection_options={
        "typeOfData": "kinesis",
        "streamARN": "arn:aws:kinesis:us-east-1:851725381788:stream/DeepStream",
        "classification": "json",
        "startingPosition": "earliest",
        "inferSchema": "true"
    },
    transformation_ctx="kinesis_stream"
)

# Process micro-batches
glueContext.forEachBatch(
    frame=kinesis_frame,
    batch_function=processBatch,
    options={
        "windowSize": "60 seconds",
        "checkpointLocation": f"{args['TempDir']}/{args['JOB_NAME']}/checkpoint/"
    }
)

# Commit job
job.commit()
logger.info("Glue job completed successfully")