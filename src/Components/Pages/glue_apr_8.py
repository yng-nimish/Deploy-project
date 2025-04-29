import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, explode, pandas_udf, monotonically_increasing_id
from pyspark.sql.types import StringType, StructType, StructField, IntegerType
from awsglue import DynamicFrame
from awsglue.dynamicframe import DynamicFrameCollection
import datetime
import boto3
from io import BytesIO
import pandas as pd
import logging
import tempfile
import os

# Logging Setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)

# Get job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'TempDir'])

# Initialize GlueContext
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

# Enable Arrow optimization and set parallelism
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")
spark.conf.set("spark.default.parallelism", "200")
spark.conf.set("spark.sql.shuffle.partitions", "200")
logger.info("Arrow optimization enabled with batch size 10000, parallelism set to 200")

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
    df = dynamic_frame.toDF().cache()
    record_count = df.count()
    logger.info(f"MyTransform: Received {record_count} records")

    if record_count == 0:
        logger.info("MyTransform: No data in batch")
        return DynamicFrameCollection({}, glueContext)

    try:
        # Ensure numbers are treated as strings from the start
        df_unpacked = df.select(explode(col("numbers")).alias("number")).withColumn("number", col("number").cast("string"))
        transformed_count = df_unpacked.count()
        
        # Apply padding to ensure leading zeroes are preserved
        df_transformed = df_unpacked.withColumn("number", vectorized_lpad(col("number")))
        dynamic_frame_transformed = DynamicFrame.fromDF(df_transformed, glueContext, "transformed")
        logger.info(f"MyTransform: Transformed {transformed_count} numbers from {record_count} records")
        return DynamicFrameCollection({"transformed_number": dynamic_frame_transformed}, glueContext)
    except Exception as e:
        logger.error(f"MyTransform: Transform failed: {str(e)}", exc_info=True)
        return DynamicFrameCollection({}, glueContext)

# Vectorized UDF to assign sheet and position
@pandas_udf(StructType([
    StructField("workbook_id", IntegerType()),
    StructField("sheet_id", IntegerType()),
    StructField("row_id", IntegerType()),
    StructField("col_id", IntegerType())
]))
def assign_excel_position(index_series: pd.Series) -> pd.DataFrame:
    total_cells = index_series.size
    cells_per_sheet = 1000 * 1000  # 1M cells per sheet (1000 rows x 1000 cols)
    sheets_per_workbook = 50       # Each workbook has 50 sheets
    
    workbook_id = (index_series // (cells_per_sheet * sheets_per_workbook)).astype(int) + 1
    sheet_id = ((index_series % (cells_per_sheet * sheets_per_workbook)) // cells_per_sheet).astype(int) + 1
    within_sheet = index_series % cells_per_sheet
    row_id = (within_sheet // 1000).astype(int) + 1
    col_id = (within_sheet % 1000).astype(int) + 1
    
    return pd.DataFrame({
        "workbook_id": workbook_id,
        "sheet_id": sheet_id,
        "row_id": row_id,
        "col_id": col_id
    })

spark.udf.register("assign_excel_position", assign_excel_position)

# Optimized Excel Save and Merge Function
def save_to_excel_and_upload_to_s3(df, s3_base_path, temp_s3_path):
    try:
        logger.info("Starting Excel save process")
        
        # Add index and assign positions, no repartition to preserve randomness
        df_with_index = df.withColumn("idx", monotonically_increasing_id())
        df_positioned = df_with_index.withColumn("position", assign_excel_position("idx")).select(
            "number",
            col("position.workbook_id").alias("workbook_id"),
            col("position.sheet_id").alias("sheet_id"),
            col("position.row_id").alias("row_id"),
            col("position.col_id").alias("col_id")
        )

        total_numbers = df_positioned.count()
        logger.info(f"Processing {total_numbers} numbers for Excel across {df_positioned.rdd.getNumPartitions()} partitions")

        # Write temporary workbooks with 50 sheets each
        def write_workbook(df_group):
            try:
                s3_client = boto3.client('s3')
                workbook_id = df_group["workbook_id"].iloc[0]
                output = BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    for sheet_id in range(1, 51):  # 50 sheets per workbook
                        sheet_data = df_group[df_group["sheet_id"] == sheet_id].pivot(
                            index="row_id", columns="col_id", values="number"
                        )
                        if not sheet_data.empty:
                            sheet_data.to_excel(writer, sheet_name=f"Z{sheet_id}", index=False, header=False)
                
                output.seek(0)
                temp_key = f"{temp_s3_path}/workbook_{workbook_id}.xlsx"
                s3_client.put_object(Body=output, Bucket="my-bucket-sun-test", Key=temp_key)
                logger.info(f"Uploaded temporary workbook {workbook_id} with 50 sheets to {temp_key}")
                return pd.DataFrame({"workbook_id": [workbook_id]})
            except Exception as e:
                logger.error(f"Failed to save workbook {workbook_id}: {str(e)}")
                raise

        output_schema = StructType([StructField("workbook_id", IntegerType())])

        logger.info("Writing temporary workbooks with 50 sheets each")
        # Write workbooks and save workbook IDs to a temporary S3 file
        temp_ids_path = f"{temp_s3_path}/workbook_ids"
        df_positioned.groupBy("workbook_id").applyInPandas(
            write_workbook, schema=output_schema
        ).select("workbook_id").write.mode("overwrite").parquet(temp_ids_path)

        # Read workbook IDs without collecting to driver
        workbook_ids_df = spark.read.parquet(temp_ids_path)
        workbook_ids = [row["workbook_id"] for row in workbook_ids_df.distinct().collect()]  # Small collect, just unique IDs
        workbook_ids.sort()  # Sort for consistency
        logger.info(f"Generated {len(workbook_ids)} temporary workbooks")

        # Merge into final workbooks with exactly 1000 sheets each
        s3_client = boto3.client('s3')
        final_workbook_count = (len(workbook_ids) + 19) // 20  # Groups of 20 workbooks = 1000 sheets

        for final_workbook_id in range(1, final_workbook_count + 1):
            try:
                with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp_file:
                    with pd.ExcelWriter(tmp_file.name, engine='xlsxwriter') as writer:
                        sheet_counter = 1
                        start_idx = (final_workbook_id - 1) * 20
                        end_idx = min(start_idx + 20, len(workbook_ids))
                        for wb_id in workbook_ids[start_idx:end_idx]:
                            temp_key = f"{temp_s3_path}/workbook_{wb_id}.xlsx"
                            try:
                                temp_file_obj = s3_client.get_object(Bucket="my-bucket-sun-test", Key=temp_key)
                                temp_workbook = pd.read_excel(BytesIO(temp_file_obj['Body'].read()), sheet_name=None, header=None)
                                for sheet_name, sheet_data in temp_workbook.items():
                                    sheet_data.to_excel(writer, sheet_name=f"Z{sheet_counter}", index=False, header=False)
                                    logger.info(f"Added sheet Z{sheet_counter} from workbook {wb_id} to final workbook {final_workbook_id}")
                                    sheet_counter += 1
                            except s3_client.exceptions.NoSuchKey:
                                logger.warning(f"Temp workbook {temp_key} not found, skipping")
                                continue

                        if sheet_counter - 1 < 1000:
                            logger.info(f"Final workbook {final_workbook_id} has {sheet_counter - 1} sheets (partial)")

                    final_s3_path = f"{s3_base_path}_final_workbook_{final_workbook_id}.xlsx"
                    s3_client.upload_file(tmp_file.name, "my-bucket-sun-test", final_s3_path)
                    logger.info(f"Uploaded final workbook {final_workbook_id} with {sheet_counter - 1} sheets to {final_s3_path}")
                    os.remove(tmp_file.name)
            except Exception as e:
                logger.error(f"Failed to merge final workbook {final_workbook_id}: {str(e)}", exc_info=True)

        # Clean up temporary S3 files
        for wb_id in workbook_ids:
            temp_key = f"{temp_s3_path}/workbook_{wb_id}.xlsx"
            try:
                s3_client.delete_object(Bucket="my-bucket-sun-test", Key=temp_key)
            except:
                logger.warning(f"Failed to delete {temp_key}, may already be removed")
        
        # Clean up workbook IDs parquet
        s3_client.delete_object(Bucket="my-bucket-sun-test", Key=f"{temp_ids_path}/")

        logger.info("Excel save and upload completed successfully")
        
    except Exception as e:
        logger.error(f"Excel save/upload failed: {str(e)}", exc_info=True)
        raise

# Batch Processing
def processBatch(data_frame, batchId):
    try:
        initial_count = data_frame.count()
        logger.info(f"Batch {batchId}: Fetched {initial_count} records from Kinesis")
        
        if initial_count > 0:
            dynamic_frame = DynamicFrame.fromDF(data_frame, glueContext, "from_data_frame")
            transform_result = MyTransform(glueContext, DynamicFrameCollection({"AmazonKinesis_node": dynamic_frame}, glueContext))
            
            if "transformed_number" not in transform_result.keys():
                logger.info(f"Batch {batchId}: No transformed data to process")
                return

            transformed_dynamic_frame = transform_result["transformed_number"]
            transformed_df = transformed_dynamic_frame.toDF()
            
            total_numbers = transformed_df.count()
            logger.info(f"Batch {batchId}: Contains {total_numbers} transformed numbers")

            now = datetime.datetime.now()
            s3_base_path = f"s3://my-bucket-sun-test/temp/ingest_year={now.year:0>4}/ingest_month={now.month:0>2}/ingest_day={now.day:0>2}/output_batch_{batchId}"
            temp_s3_path = f"{s3_base_path}/temp_workbooks"

            save_to_excel_and_upload_to_s3(transformed_df, s3_base_path, temp_s3_path)
            
            transformed_df.write.mode("overwrite").parquet(f"{s3_base_path}_parquet")
            logger.info(f"Batch {batchId}: Saved parquet backup to {s3_base_path}_parquet")
            
    except Exception as e:
        logger.error(f"Batch {batchId} processing failed: {str(e)}", exc_info=True)

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