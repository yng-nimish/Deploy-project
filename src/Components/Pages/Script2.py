import pandas as pd
import boto3
from io import BytesIO

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame

sc = SparkContext()
glueContext = GlueContext(sc)

# Reading data from Kinesis stream (as dynamic frame)
kinesis_dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="kinesis",
    connection_options={"stream_name": "My_kinesis_stream_name", "starting_position": "TRIM_HORIZON"},
    format="json" 
)

# Converting DynamicFrame to Spark DataFrame for processing
df = kinesis_dynamic_frame.toDF()


df = df.fillna(0)  # Replace missing values with zero

# Converting the DataFrame back to DynamicFrame for processing
transformed_dynamic_frame = DynamicFrame.fromDF(df, glueContext, "transformed_dynamic_frame")

# Collecting transformed data into Pandas DataFrame
pandas_df = df.toPandas()

# Generating Excel file in-memory
excel_buffer = BytesIO()
with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
    sheet_index = 0
    rows_per_sheet = 1000
    cols_per_sheet = 1000

    # Processing the data and writing it in chunks of 1000x1000 grid per sheet 
    for start_row in range(0, len(pandas_df), rows_per_sheet):
        end_row = min(start_row + rows_per_sheet, len(pandas_df))
        chunk = pandas_df.iloc[start_row:end_row, :cols_per_sheet]
        sheet_name = f"Z{sheet_index + 1}"  # Sheet is labelled as Z for index
        chunk.to_excel(writer, sheet_name=sheet_name, index=False)
        sheet_index += 1
        if sheet_index >= 1000:
            break

# Uploading the Final Excel file to S3

s3_client = boto3.client('s3') 
s3_client.put_object(Body=excel_buffer.getvalue(), Bucket="My-s3-bucket", Key="F0000.xlsx")

print("Excel file uploaded to S3.")
