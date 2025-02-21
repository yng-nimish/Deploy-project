import boto3
import requests
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# Kinesis Configuration
KINESIS_STREAM_NAME = "ChristmasStream"  # Replace with your Kinesis Stream name
AWS_REGION = "us-east-1"
kinesis_client = boto3.client('kinesis', region_name=AWS_REGION)

# Data Source API Configuration
API_URL = "http://18.210.19.229:4900/num?"
API_PARAMS = {
    "floor": 0,
    "limit": 1000
}

TARGET_COUNT = 2000000000  # Target 2 billion records

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

def get_data_from_api():
    """Fetch data from API with retries and correct parsing."""
    retries = 5
    for _ in range(retries):
        try:
            response = requests.get(API_URL, params=API_PARAMS, timeout=5)
            response.raise_for_status()
            raw_data = response.text.strip()
            logger.info(f"Raw API Response: {raw_data[:500]}")  # Log first 500 chars
            
            # Convert space-separated numbers into a list of integers
            data = list(map(int, raw_data.split()))
            return data
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
        except ValueError as e:
            logger.error(f"Data parsing error: {e}")
            logger.error(f"Raw Response: {raw_data}")
        time.sleep(1)
    return []

def fetch_large_dataset():
    """Continuously fetch data until we reach the target count."""
    batch_size = 32768  # API's maximum limit
    data = []
    total_fetched = 0
    while total_fetched < TARGET_COUNT:
        API_PARAMS["count"] = batch_size  # Ensure correct parameter
        batch_data = get_data_from_api()
        if not batch_data:
            logger.warning("No more data received from API. Stopping fetch.")
            break
        data.extend(batch_data)
        total_fetched += len(batch_data)
        logger.info(f"Fetched {total_fetched}/{TARGET_COUNT} records so far.")
    return data

def send_data_to_kinesis_batch(data):
    """Batch send raw data to Kinesis asynchronously."""
    MAX_RETRIES = 3
    BATCH_SIZE = 500  # Kinesis limit
    
    def send_batch(batch):
        records = [{'Data': str(num), 'PartitionKey': str(num % 1000)} for num in batch]  # Improved partitioning
        for attempt in range(MAX_RETRIES):
            try:
                response = kinesis_client.put_records(StreamName=KINESIS_STREAM_NAME, Records=records)
                failed = sum(1 for r in response['Records'] if 'ErrorCode' in r)
                logger.info(f"Sent {len(records) - failed}/{len(records)} records successfully.")
                return
            except Exception as e:
                logger.error(f"Attempt {attempt+1}: Kinesis batch send failed: {e}")
                time.sleep(2)  # Exponential backoff can be added

    with ThreadPoolExecutor(max_workers=10) as executor:  # Increased workers for higher throughput
        executor.map(send_batch, [data[i:i + BATCH_SIZE] for i in range(0, len(data), BATCH_SIZE)])

def main():
    """Multi-threaded data ingestion loop with optimized sharding and batch sending."""
    with ThreadPoolExecutor(max_workers=10) as executor:  # Increased concurrency
        total_sent = 0
        while total_sent < TARGET_COUNT:
            data = fetch_large_dataset()
            if data:
                future_to_data = {
                    executor.submit(send_data_to_kinesis_batch, chunk): chunk 
                    for chunk in [data[i:i + 5000] for i in range(0, len(data), 5000)]
                }  # Chunking for better performance
                for future in as_completed(future_to_data):
                    future.result()  # Ensures any exceptions are caught
                total_sent += len(data)
                logger.info(f"Total records sent: {total_sent}/{TARGET_COUNT}")
            else:
                logger.warning("No data to send.")
            time.sleep(0.1)  # Reduced delay for higher throughput

if __name__ == "__main__":
    main()