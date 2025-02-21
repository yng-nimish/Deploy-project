import boto3
import requests
import time
import logging
import threading
from queue import Queue
from concurrent.futures import ThreadPoolExecutor

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

# Queue for Producer-Consumer Model
data_queue = Queue(maxsize=500000)  # Further increased buffer size

total_fetched = 0
total_sent = 0
total_stored = 0

def get_data_from_api():
    """Fetch data from API with retries and correct parsing."""
    global total_fetched
    retries = 5
    for _ in range(retries):
        try:
            response = requests.get(API_URL, params=API_PARAMS, timeout=5)
            response.raise_for_status()
            raw_data = response.text.strip()
            
            # Convert space-separated numbers into a list of integers
            data = list(map(int, raw_data.split()))
            total_fetched += len(data)
            logger.info(f"Total records fetched so far: {total_fetched}")
            return data
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
        except ValueError as e:
            logger.error(f"Data parsing error: {e}")
        time.sleep(0.1)
    return []

def fetch_data_continuously():
    """Fetch data endlessly and store in queue using multiple threads."""
    while True:
        batch_data = get_data_from_api()
        if batch_data:
            for num in batch_data:
                data_queue.put(num)
            logger.info(f"Queue size: {data_queue.qsize()}")

def send_data_to_kinesis():
    """Continuously send data from queue to Kinesis using ThreadPoolExecutor."""
    global total_sent, total_stored
    BATCH_SIZE = 500  # Kinesis limit 
    MAX_RETRIES = 3

    def send_batch(batch):
        records = [{'Data': str(num), 'PartitionKey': str(num % 1000)} for num in batch]
        for attempt in range(MAX_RETRIES):
            try:
                response = kinesis_client.put_records(StreamName=KINESIS_STREAM_NAME, Records=records)
                failed = sum(1 for r in response['Records'] if 'ErrorCode' in r)
                successful = len(records) - failed
                logger.info(f"Sent {successful}/{len(records)} records successfully.")
                return successful
            except Exception as e:
                logger.error(f"Kinesis batch send failed: {e}")
                time.sleep(0.1)
        return 0

    with ThreadPoolExecutor(max_workers=10) as executor:  # Increased sender threads
        while True:
            if data_queue.qsize() >= BATCH_SIZE:
                batch = [data_queue.get() for _ in range(BATCH_SIZE)]
                future = executor.submit(send_batch, batch)
                successful_sent = future.result() 
                total_sent += successful_sent
                total_stored += successful_sent
                logger.info(f"Total records sent to Kinesis: {total_sent}")
                logger.info(f"Total records stored in Kinesis: {total_stored}")

def main():
    """Start multiple fetch threads and sending thread."""
    fetch_threads = [threading.Thread(target=fetch_data_continuously, daemon=True) for _ in range(5)]  # Increased fetchers
    send_threads = [threading.Thread(target=send_data_to_kinesis, daemon=True) for _ in range(5)]  # Increased senders

    for t in fetch_threads + send_threads:
        t.start()
    
    for t in fetch_threads + send_threads:
        t.join()

if __name__ == "__main__":
    main()
