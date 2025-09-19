import boto3
import time
import json
import logging
import os
from botocore.exceptions import ClientError

# === CONFIGURATION ===

REGION = "us-east-1"
STREAM_NAME = "Aug21st"
SHARD_COUNT = 60

GLUE_JOB_NAME = "FounderSeriesStreamingJob"
GLUE_ROLE = "AWSGluekinesisS3fullaccess"

S3_BUCKET_SCRIPTS = "my-bucket-scripts"
SCRIPT_TEMPLATE_KEY = "local_script_template.py"
FINAL_SCRIPT_KEY = "my-etl-script.py"
SCRIPT_S3_URI = f"s3://{S3_BUCKET_SCRIPTS}/{FINAL_SCRIPT_KEY}"

TEMP_DIR = "s3://my-temp-dir/tmp/"
OUTPUT_PATH = "s3://my-bucket-founder-series-sun/Batch 1/May 2/"

EXTRA_PY_FILES = ",".join([
    "s3://my-bucket-openpyxl/openpyxl-3.1.5-py2.py3-none-any.whl",
    "s3://my-bucket-openpyxl/et_xmlfile-2.0.0-py3-none-any.whl",
    "s3://my-bucket-openpyxl/XlsxWriter-3.2.2-py3-none-any.whl",
    "s3://my-bucket-openpyxl/pyarrow-19.0.1-cp39-cp39-manylinux_2_17_x86_64.manylinux2014_x86_64.whl"
])

# === LOGGING SETUP ===

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# === AWS CLIENTS ===

kinesis = boto3.client("kinesis", region_name=REGION)
glue = boto3.client("glue", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)

# === FUNCTIONS ===

def create_kinesis_stream():
    try:
        logger.info(f"Creating Kinesis stream '{STREAM_NAME}' with {SHARD_COUNT} shards...")
        kinesis.create_stream(StreamName=STREAM_NAME, ShardCount=SHARD_COUNT)
    except kinesis.exceptions.ResourceInUseException:
        logger.info(f"Kinesis stream '{STREAM_NAME}' already exists.")
    wait_for_kinesis_stream_active()


def wait_for_kinesis_stream_active():
    logger.info("Waiting for Kinesis stream to become ACTIVE...")
    while True:
        desc = kinesis.describe_stream(StreamName=STREAM_NAME)
        status = desc["StreamDescription"]["StreamStatus"]
        if status == "ACTIVE":
            logger.info("Stream is ACTIVE.")
            break
        time.sleep(5)


def get_stream_arn():
    desc = kinesis.describe_stream(StreamName=STREAM_NAME)
    return desc["StreamDescription"]["StreamARN"]


def generate_etl_script_with_stream_arn(stream_arn):
    logger.info("Fetching ETL script template from S3...")
    try:
        response = s3.get_object(Bucket=S3_BUCKET_SCRIPTS, Key=SCRIPT_TEMPLATE_KEY)
        template_script = response['Body'].read().decode('utf-8')
    except ClientError as e:
        logger.error(f"Failed to download template script: {e}")
        raise

    # Replace placeholder in script
    final_script = template_script.replace("REPLACE_WITH_STREAM_ARN", stream_arn)

    # Upload updated script
    logger.info("Uploading final ETL script to S3...")
    try:
        s3.put_object(Bucket=S3_BUCKET_SCRIPTS, Key=FINAL_SCRIPT_KEY, Body=final_script.encode('utf-8'))
        logger.info("ETL script uploaded successfully.")
    except ClientError as e:
        logger.error(f"Failed to upload ETL script: {e}")
        raise


def create_glue_job():
    try:
        # Check if job already exists
        glue.get_job(JobName=GLUE_JOB_NAME)
        logger.info(f"Glue job '{GLUE_JOB_NAME}' already exists.")
    except glue.exceptions.EntityNotFoundException:
        logger.info(f"Creating Glue job '{GLUE_JOB_NAME}'...")
        try:
            glue.create_job(
                Name=GLUE_JOB_NAME,
                Role=GLUE_ROLE,
                ExecutionProperty={'MaxConcurrentRuns': 1},
                Command={
                    'Name': 'gluestreaming',
                    'ScriptLocation': SCRIPT_S3_URI,
                    'PythonVersion': '3'
                },
                DefaultArguments={
                    '--TempDir': TEMP_DIR,
                    '--job-language': 'python',
                    '--extra-py-files': EXTRA_PY_FILES,
                    '--OutputS3Path': OUTPUT_PATH,
                    '--enable-continuous-logging': 'true'
                },
                GlueVersion='5.0',
                WorkerType='G.1X',
                NumberOfWorkers=10
            )
            logger.info("Glue job created successfully.")
        except ClientError as e:
            logger.error(f"Failed to create Glue job: {e}")
            raise


def start_glue_job():
    logger.info(f"Starting Glue job '{GLUE_JOB_NAME}'...")
    try:
        response = glue.start_job_run(JobName=GLUE_JOB_NAME)
        job_run_id = response['JobRunId']
        logger.info(f"Glue job started. JobRunId: {job_run_id}")
    except ClientError as e:
        logger.error(f"Failed to start Glue job: {e}")
        raise


# === MAIN EXECUTION ===

def main():
    logger.info("Starting AWS Data Pipeline Automation Script")

    create_kinesis_stream()
    stream_arn = get_stream_arn()
    logger.info(f"Stream ARN: {stream_arn}")

    generate_etl_script_with_stream_arn(stream_arn)
    create_glue_job()
    start_glue_job()

    logger.info("Pipeline setup and Glue job launch complete.")


if __name__ == "__main__":
    main()
