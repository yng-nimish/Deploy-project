import asyncio
import aiohttp
import time
import logging
import boto3

# Kinesis Configuration
KINESIS_STREAM_NAME = "canadaStream"
AWS_REGION = "us-east-1"
kinesis_client = boto3.client("kinesis", region_name=AWS_REGION)

# API Configuration
API_URL = "http://107.22.145.126:4900/num?"
API_PARAMS = {
    "floor": 100,
    "limit": 1000,
    "count": 32768 
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

# Constants
FETCH_WORKERS = 10   # Increased to match high throughput
SEND_WORKERS = 500    # Increased to parallelize Kinesis ingestion
TARGET_COUNT = 100_000_000  # 100 million records in a minute
BATCH_SIZE = 500     # Max batch size allowed by Kinesis per request
QUEUE_SIZE = 5_000_000  # Increased buffer for high throughput
BATCH_THRESHOLD = 1_000_000  # Start writing to Kinesis after 1 million records

# Track total records and bytes sent for logging
total_records_sent = 0
total_bytes_sent = 0
total_records_fetched = 0  # Track total number of records fetched

async def fetch_data(session):
    """Asynchronously fetch data from API."""
    retries = 5
    for _ in range(retries):
        try:
            async with session.get(API_URL, params=API_PARAMS, timeout=5) as response:
                response.raise_for_status()
                raw_data = await response.text()
                data = list(map(int, raw_data.split()))
                return data
        except aiohttp.ClientResponseError as e:
            logger.error(f"API request failed with status {e.status} for URL {API_URL}. Response: {e.message}")
        except asyncio.TimeoutError:
            logger.error(f"API request timed out for URL {API_URL}.")
        except Exception as e:
            logger.error(f"API request failed with exception: {e}")
        
        # Wait a bit before retrying
        await asyncio.sleep(0.01)
    
    return []

async def fetch_data_batch(session, queue):
    """Fetch and add data to queue simultaneously."""
    global total_records_fetched
    async with session.get(API_URL, params=API_PARAMS, timeout=5) as response:
        if response.status == 200:
            raw_data = await response.text()
            batch = list(map(int, raw_data.split()))
            if batch:
                total_records_fetched += len(batch)
                await queue.put(batch)
                logger.info(f"Fetched {len(batch)} records. Total fetched: {total_records_fetched}")

async def fetch_continuously(queue):
    """Fetch and put data into the queue concurrently with sending."""
    async with aiohttp.ClientSession() as session:
        while True:
            tasks = [fetch_data_batch(session, queue) for _ in range(FETCH_WORKERS)]
            await asyncio.gather(*tasks)

async def send_to_kinesis(queue):
    """Send data to Kinesis in batches of 500 records, using parallelism."""
    global total_records_sent, total_bytes_sent

    while True:
        batch = await queue.get()
        if not batch:
            continue

        # Split the batch if it exceeds the max size of 500
        for i in range(0, len(batch), 500):
            sub_batch = batch[i:i+500]
            
            # Prepare Kinesis records
            kinesis_records = [{
                "Data": str(record).encode("utf-8"),
                "PartitionKey": str(record % 1000)  # Partition key based on the record
            } for record in sub_batch]

            # Calculate the size of the batch
            batch_size_in_bytes = sum(len(str(record).encode("utf-8")) for record in sub_batch)

            try:
                # Send the batch to Kinesis concurrently
                await asyncio.to_thread(kinesis_client.put_records, StreamName=KINESIS_STREAM_NAME, Records=kinesis_records)

                total_records_sent += len(sub_batch)
                total_bytes_sent += batch_size_in_bytes

                # Log sent data
                logger.info(f"Sent {len(sub_batch)} records to Kinesis. Total: {total_records_sent} records, {total_bytes_sent} bytes")
            except Exception as e:
                logger.error(f"Failed to send records to Kinesis: {e}")

        queue.task_done()

async def main():
    queue = asyncio.Queue(maxsize=QUEUE_SIZE)
    fetch_task = asyncio.create_task(fetch_continuously(queue))
    send_tasks = [asyncio.create_task(send_to_kinesis(queue)) for _ in range(SEND_WORKERS)]
    await asyncio.gather(fetch_task, *send_tasks)

if __name__ == "__main__":
    start_time = time.time()
    asyncio.run(main())
    logger.info(f"Total runtime: {time.time() - start_time:.2f} seconds")
