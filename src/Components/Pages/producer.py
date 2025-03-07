import asyncio
import aiohttp
import time
import logging
import boto3
import json

# Kinesis Configuration
KINESIS_STREAM_NAME = "GiantStream"
AWS_REGION = "us-east-1"
kinesis_client = boto3.client("kinesis", region_name=AWS_REGION)

# API Configuration
API_URL = "http://35.173.232.184:4900/num?"
API_PARAMS = {
    "floor": 0,
    "limit": 1000,
    "count": 32768  # Max per call; adjust if API allows higher
}

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('data_pipeline.log'), logging.StreamHandler()]
)
logger = logging.getLogger()

# Constants
FETCH_WORKERS = 8        # Match vCPUs for optimal concurrency
SEND_WORKERS = 8         # Match vCPUs for Kinesis ingestion
TARGET_COUNT = 100_000_000  # 100M numbers/minute
NUMBERS_PER_RECORD = 1000   # Bundle 1000 numbers per Kinesis record
BATCH_SIZE = 500         # Max Kinesis records per put_records call
QUEUE_SIZE = 10_000      # Queue for 10K batches (~10M numbers)

# Track metrics
total_numbers_sent = 0
total_bytes_sent = 0
total_numbers_fetched = 0

async def fetch_data(session):
    """Fetch data from API."""
    retries = 3
    for attempt in range(retries):
        try:
            async with session.get(API_URL, params=API_PARAMS, timeout=5) as response:
                response.raise_for_status()
                raw_data = await response.text()
                return list(map(int, raw_data.split()))
        except Exception as e:
            logger.error(f"Fetch failed (attempt {attempt+1}/{retries}): {e}")
            await asyncio.sleep(0.1 * (attempt + 1))  # Exponential backoff
    return []

async def fetch_continuously(queue):
    """Fetch data and bundle into queue."""
    async with aiohttp.ClientSession() as session:
        while total_numbers_fetched < TARGET_COUNT:
            tasks = [fetch_data(session) for _ in range(FETCH_WORKERS)]
            results = await asyncio.gather(*tasks)
            for batch in results:
                if batch:
                    total_numbers_fetched += len(batch)
                    # Split into chunks of NUMBERS_PER_RECORD
                    for i in range(0, len(batch), NUMBERS_PER_RECORD):
                        chunk = batch[i:i + NUMBERS_PER_RECORD]
                        await queue.put(chunk)
                    logger.info(f"Fetched {len(batch)} numbers. Total: {total_numbers_fetched}")

async def send_to_kinesis(queue):
    """Send bundled data to Kinesis."""
    global total_numbers_sent, total_bytes_sent

    while total_numbers_sent < TARGET_COUNT:
        batch = []
        for _ in range(BATCH_SIZE):
            try:
                numbers = await queue.get()
                if numbers:
                    batch.append({
                        "Data": json.dumps({"numbers": numbers}).encode("utf-8"),
                        "PartitionKey": str(hash(str(numbers)) % 1000)
                    })
                queue.task_done()
            except asyncio.QueueEmpty:
                break

        if batch:
            try:
                response = await asyncio.to_thread(
                    kinesis_client.put_records, StreamName=KINESIS_STREAM_NAME, Records=batch
                )
                num_records = len(batch)
                total_numbers_sent += sum(len(json.loads(r["Data"].decode("utf-8"))["numbers"]) for r in batch)
                total_bytes_sent += sum(len(r["Data"]) for r in batch)
                logger.info(f"Sent {num_records} records ({total_numbers_sent} numbers, {total_bytes_sent} bytes)")
            except Exception as e:
                logger.error(f"Kinesis send failed: {e}")

async def main():
    queue = asyncio.Queue(maxsize=QUEUE_SIZE)
    fetch_task = asyncio.create_task(fetch_continuously(queue))
    send_tasks = [asyncio.create_task(send_to_kinesis(queue)) for _ in range(SEND_WORKERS)]
    await asyncio.gather(fetch_task, *send_tasks)

if __name__ == "__main__":
    start_time = time.time()
    asyncio.run(main())
    elapsed = time.time() - start_time
    logger.info(f"Total runtime: {elapsed:.2f} seconds")
    rate = total_numbers_sent / (elapsed / 60)
    logger.info(f"Throughput: {rate:,.0f} numbers/minute")