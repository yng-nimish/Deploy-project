import boto3
import zipfile
import io
import json
from datetime import datetime

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    bucket_name = 'your-bucket'
    folder_prefix = 'F0000/'
    output_folder = 'zipped/'
    
    # Generate a unique ZIP file name
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    zip_key = f"{output_folder}F0000_{timestamp}.zip"
    
    # Create an in-memory ZIP file
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        # List all objects in the F0000 folder
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket_name, Prefix=folder_prefix):
            for obj in page.get('Contents', []):
                key = obj['Key']
                # Skip if it's not a file or not in F0000
                if key == folder_prefix or not key.startswith(folder_prefix):
                    continue
                
                # Download the file
                file_obj = s3_client.get_object(Bucket=bucket_name, Key=key)
                file_content = file_obj['Body'].read()
                
                # Add to ZIP, preserving folder structure
                zip_path = key[len(folder_prefix):]  # e.g., Z001/data.parquet
                zip_file.writestr(zip_path, file_content)
    
    # Upload the ZIP to S3
    buffer.seek(0)
    s3_client.put_object(Bucket=bucket_name, Key=zip_key, Body=buffer.getvalue())
    
    # Generate a pre-signed URL (e.g., valid for 1 hour)
    presigned_url = s3_client.generate_presigned_url(
        'get_object',
        Params={'Bucket': bucket_name, 'Key': zip_key},
        ExpiresIn=3600
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps({'download_url': presigned_url})
    }