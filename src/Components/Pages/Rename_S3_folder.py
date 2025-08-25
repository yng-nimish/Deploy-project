import boto3
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('s3_rename.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def copy_and_delete_object(s3_client, bucket_name, old_key, new_key):
    """Copy an object to a new key and delete the old one, with error handling."""
    try:
        # Check if new_key already exists
        head_response = s3_client.head_object(Bucket=bucket_name, Key=new_key)
        logger.warning(f"Object {new_key} already exists, skipping copy")
        return False
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            # New key doesn't exist, proceed with copy
            pass
        else:
            logger.error(f"Error checking {new_key}: {e}")
            return False
    
    try:
        # Copy object to new prefix
        s3_client.copy_object(
            Bucket=bucket_name,
            CopySource={'Bucket': bucket_name, 'Key': old_key},
            Key=new_key
        )
        logger.info(f"Copied {old_key} to {new_key}")
        
        # Delete the old object
        s3_client.delete_object(Bucket=bucket_name, Key=old_key)
        logger.info(f"Deleted {old_key}")
        
        return True
    except ClientError as e:
        logger.error(f"Error processing {old_key}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error processing {old_key}: {e}")
        return False

def rename_s3_folder(bucket_name, old_prefix, new_prefix, max_workers=20):
    """Rename an S3 folder by copying objects to a new prefix and deleting old ones."""
    s3_client = boto3.client('s3')
    try:
        # List all objects in the old prefix with pagination
        logger.info(f"Listing objects in {old_prefix}")
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=old_prefix)
        
        objects = []
        for page in pages:
            if 'Contents' in page:
                objects.extend([(obj['Key'], obj['Key'].replace(old_prefix, new_prefix, 1)) for obj in page['Contents']])
        
        total_objects = len(objects)
        if total_objects == 0:
            logger.warning(f"No objects found in {old_prefix}")
            return
        
        logger.info(f"Found {total_objects} objects to process")
        
        # Process objects in parallel
        successful = 0
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_key = {
                executor.submit(copy_and_delete_object, s3_client, bucket_name, old_key, new_key): old_key
                for old_key, new_key in objects
            }
            
            for future in as_completed(future_to_key):
                old_key = future_to_key[future]
                try:
                    if future.result():
                        successful += 1
                    else:
                        logger.error(f"Failed to process {old_key}")
                except Exception as e:
                    logger.error(f"Exception for {old_key}: {e}")
        
        logger.info(f"Completed: {successful}/{total_objects} objects processed successfully")
        if successful == total_objects:
            logger.info(f"Successfully renamed {old_prefix} to {new_prefix}")
        else:
            logger.warning(f"Partial success: {total_objects - successful} objects failed")
        
        # Validate that old_prefix is empty
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=old_prefix)
        if 'Contents' in response and len(response['Contents']) > 0:
            logger.warning(f"Old prefix {old_prefix} still contains {len(response['Contents'])} objects")
    
    except ClientError as e:
        logger.error(f"Failed to list objects in {old_prefix}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")

# Configuration
bucket_name = 'my-bucket-founder-series-sun'
old_prefix = 'Batch 1/May 2/F 0111/'
new_prefix = 'Batch 1/May 2/Partial 10/' 

# Execute the rename
rename_s3_folder(bucket_name, old_prefix, new_prefix, max_workers=10)