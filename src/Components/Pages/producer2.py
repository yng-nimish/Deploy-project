import asyncio
import aiohttp
import time
import logging
import boto3
import json
from datetime import datetime, timedelta

# Kinesis Configuration
KINESIS_STREAM_NAME = "GiantStream"
AWS_REGION = "us-east-1"
kinesis_client = boto3.client("kinesis", region_name=AWS_REGION)

# API Configuration
API_URL = "http://35.173.232.184:4900/num?"
API_PARAMS = {"floor": 0, "limit": 1000, "count": 32768}

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('data_pipeline.log'), logging.StreamHandler()]
)
logger = logging.getLogger()

# Constants
FETCH_WORKERS = 12
SEND_WORKERS = 12
NUMBERS_PER_RECORD = 2000
BATCH_SIZE = 500
QUEUE_SIZE = 20_000
RUN_DURATION = 3600  # 1 hour in seconds

# Metrics
total_numbers_sent = 0
total_bytes_sent = 0
total_numbers_fetched = 0
start_time = time.time()

async def fetch_data(session):
    """Fetch data from API with retries."""
    retries = 3
    for attempt in range(retries):
        try:
            async with session.get(API_URL, params=API_PARAMS, timeout=5) as response:
                response.raise_for_status()
                raw_data = await response.text()
                return list(map(int, raw_data.split()))
        except Exception as e:
            logger.warning(f"Fetch attempt {attempt+1}/{retries} failed: {e}")
            await asyncio.sleep(0.1 * (attempt + 1))
    logger.error("Fetch failed after retries.")
    return []

async def fetch_continuously(queue):
    """Fetch data for 1 hour."""
    async with aiohttp.ClientSession() as session:
        while time.time() - start_time < RUN_DURATION:
            tasks = [fetch_data(session) for _ in range(FETCH_WORKERS)]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"Fetch task failed: {result}")
                    continue
                if result:
                    global total_numbers_fetched
                    total_numbers_fetched += len(result)
                    for i in range(0, len(result), NUMBERS_PER_RECORD):
                        chunk = result[i:i + NUMBERS_PER_RECORD]
                        await queue.put(chunk)
                    logger.info(f"Fetched {len(result)} numbers. Total: {total_numbers_fetched}")
            await asyncio.sleep(0.01)  # Prevent tight loop

async def send_to_kinesis(queue):
    """Send data to Kinesis for 1 hour."""
    global total_numbers_sent, total_bytes_sent
    while time.time() - start_time < RUN_DURATION:
        batch = []
        for _ in range(BATCH_SIZE):
            try:
                numbers = await asyncio.wait_for(queue.get(), timeout=1.0)
                if numbers:
                    batch.append({
                        "Data": json.dumps({"numbers": numbers}).encode("utf-8"),
                        "PartitionKey": str(hash(str(numbers)) % 1000)
                    })
                queue.task_done()
            except asyncio.TimeoutError:
                break
            except asyncio.QueueEmpty:
                break

        if batch:
            try:
                response = await asyncio.to_thread(
                    kinesis_client.put_records, StreamName=KINESIS_STREAM_NAME, Records=batch
                )
                if response["FailedRecordCount"] > 0:
                    logger.error(f"Kinesis put failed for {response['FailedRecordCount']} records")
                num_records = len(batch)
                total_numbers_sent += sum(len(json.loads(r["Data"].decode("utf-8"))["numbers"]) for r in batch)
                total_bytes_sent += sum(len(r["Data"]) for r in batch)
                logger.info(f"Sent {num_records} records ({total_numbers_sent} numbers, {total_bytes_sent} bytes)")
            except Exception as e:
                logger.error(f"Kinesis send failed: {e}")
                await asyncio.sleep(1)  # Backoff on failure
        await asyncio.sleep(0.01)  # Prevent tight loop

async def main():
    queue = asyncio.Queue(maxsize=QUEUE_SIZE)
    fetch_task = asyncio.create_task(fetch_continuously(queue))
    send_tasks = [asyncio.create_task(send_to_kinesis(queue)) for _ in range(SEND_WORKERS)]
    await asyncio.wait([fetch_task] + send_tasks, timeout=RUN_DURATION)

if __name__ == "__main__":
    logger.info("Starting producer for 1 hour...")
    asyncio.run(main())
    elapsed = time.time() - start_time
    logger.info(f"Completed in {elapsed:.2f} seconds")
    rate = total_numbers_sent / (elapsed / 60)
    logger.info(f"Throughput: {rate:,.0f} numbers/minute")