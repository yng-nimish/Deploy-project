export const handler = async (event) => {
  // TODO implement
  const response = {
    statusCode: 200,
    body: JSON.stringify('Hello from Lambda!'),
  };
  return response;
};

import json
import boto3
import logging
import os
from botocore.exceptions import ClientError
from datetime import datetime

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
eventbridge = boto3.client('events')
kinesis = boto3.client('kinesis')
glue = boto3.client('glue')

# Environment variables
STREAM_NAME = os.environ.get('KINESIS_STREAM_NAME', 'data-stream')
EVENTBRIDGE_RULE = os.environ.get('EVENTBRIDGE_RULE', 'data-pipeline-rule')
GLUE_JOB_NAME = os.environ.get('GLUE_JOB_NAME', 'data-transform-job')
S3_BUCKET = os.environ.get('S3_BUCKET', 'data-output-bucket')

def lambda_handler(event, context):
    """
    Lambda function to trigger EventBridge pipeline, send data to Kinesis, and start Glue ETL job.
    """
    try:
        logger.info("Lambda function invoked with event: %s", json.dumps(event))
        
        # Validate input event
        if not event or 'data' not in event:
            logger.error("Invalid input event: Missing 'data' key")
            raise ValueError("Event must contain 'data' key")

        # Extract data from event (assuming data comes from EC2)
        data = event['data']
        
        # Send data to Kinesis Data Stream
        try:
            kinesis_response = kinesis.put_record(
                StreamName=STREAM_NAME,
                Data=json.dumps(data),
                PartitionKey=str(datetime.now().timestamp())
            )
            logger.info("Successfully sent data to Kinesis: ShardId=%s, SequenceNumber=%s",
                       kinesis_response['ShardId'], kinesis_response['SequenceNumber'])
        except ClientError as e:
            logger.error("Failed to send data to Kinesis: %s", str(e))
            raise

        # Trigger EventBridge rule to initiate pipeline
        try:
            eventbridge_response = eventbridge.put_events(
                Entries=[
                    {
                        'Source': 'lambda.trigger',
                        'DetailType': 'DataPipelineTrigger',
                        'Detail': json.dumps({
                            'streamName': STREAM_NAME,
                            'timestamp': datetime.now().isoformat(),
                            'triggeredBy': context.function_name
                        }),
                        'EventBusName': 'default'
                    }
                ]
            )
            
            if eventbridge_response['FailedEntryCount'] > 0:
                logger.error("Failed to trigger EventBridge: %s", eventbridge_response)
                raise RuntimeError("EventBridge trigger failed")
            
            logger.info("Successfully triggered EventBridge rule: %s", EVENTBRIDGE_RULE)
        except ClientError as e:
            logger.error("EventBridge trigger error: %s", str(e))
            raise

        # Start Glue ETL job
        try:
            glue_response = glue.start_job_run(
                JobName=GLUE_JOB_NAME,
                Arguments={
                    '--kinesis_stream_name': STREAM_NAME,
                    '--s3_output_bucket': S3_BUCKET
                }
            )
            logger.info("Started Glue job: %s, RunId: %s", 
                       GLUE_JOB_NAME, glue_response['JobRunId'])
        except ClientError as e:
            logger.error("Failed to start Glue job: %s", str(e))
            raise

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Pipeline triggered successfully',
                'kinesisShardId': kinesis_response['ShardId'],
                'glueJobRunId': glue_response['JobRunId']
            })
        }

    except Exception as e:
        logger.error("Pipeline trigger failed: %s", str(e))
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': f"Pipeline trigger failed: {str(e)}"
            })
        }