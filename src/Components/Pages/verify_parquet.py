import json
import boto3
import logging
import os
from verify_parquet import list_parquet_files_and_missing_z, verify_parquet_file, compute_grouped_averages

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Config via environment variables
S3_BUCKET = os.environ.get("S3_BUCKET", "my-bucket-parquet-test")
S3_PREFIX = os.environ.get("S3_PREFIX", "April 24/")
MIN_SIZE_MB = float(os.environ.get("MIN_SIZE_MB", 1.2))
MAX_SIZE_MB = float(os.environ.get("MAX_SIZE_MB", 1.6))
SKIP_SIZE_BYTES = int(os.environ.get("SKIP_SIZE_BYTES", 473))

s3_client = boto3.client("s3")

def lambda_handler(event, context):
    try:
        logger.info("Event received: %s", json.dumps(event))
        detail = event.get("detail", {})
        job_name = detail.get("jobName")
        state = detail.get("state")
        logger.info(f"Glue job '{job_name}' entered state: {state}")
        
        # Only proceed if terminal state
        if state not in ("SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT"):
            logger.info("Non-terminal state, skipping verification.")
            return {"status": "skipped", "reason": "non-terminal state"}

        # Execute verification logic
        missing_report, missing_z = list_parquet_files_and_missing_z(
            s3_client, S3_BUCKET, S3_PREFIX
        )
        logger.info(f"Found {len(missing_report)} parquet files; missing Z folders: {missing_z}")

        results = []
        for s3_uri in missing_report:
            r = verify_parquet_file(s3_client, s3_uri, MIN_SIZE_MB, MAX_SIZE_MB, SKIP_SIZE_BYTES)
            if r:
                results.append(r)

        grouped_averages = compute_grouped_averages(results)

        summary = {
            "jobName": job_name,
            "jobState": state,
            "total_processed_files": len(results),
            "missing_z_folders": missing_z,
            "grouped_averages": grouped_averages,
            "errors": [r["s3_uri"] for r in results if r.get("error")]
        }

        # Save summary to S3
        out_key = f"{S3_PREFIX}verification_report_{job_name}_{state}_{context.aws_request_id}.json"
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=out_key,
            Body=json.dumps(summary, indent=2).encode('utf-8')
        )
        logger.info(f"Report saved to s3://{S3_BUCKET}/{out_key}")

        return {"status": "success", "report_key": out_key}

    except Exception as e:
        logger.error("Error in verification: %s", str(e), exc_info=True)
        raise
