import asyncio
import aiohttp
import time
import logging
import aioboto3
import json

# Kinesis Configuration
KINESIS_STREAM_NAME = "GiantStream"
AWS_REGION = "us-east-1"

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
logger.setLevel(logging.WARNING)

# Constants (Tuned for M2)
FETCH_WORKERS = 12  # 8 cores, conservative to avoid memory pressure
SEND_WORKERS = 12  # Match fetch, limited by upload speed
NUMBERS_PER_RECORD = 2000
BATCH_SIZE = 500
QUEUE_SIZE = 20_000
RUN_DURATION = 60  # Test for 1 minute first

# Metrics
total_numbers_sent = 0
total_bytes_sent = 0
total_numbers_fetched = 0
start_time = time.time()
fetch_calls = 0

async def fetch_data(session):
    """Fetch data with minimal retries."""
    global total_numbers_fetched, fetch_calls
    retries = 1
    for attempt in range(retries):
        try:
            async with session.get(API_URL, params=API_PARAMS, timeout=5) as response:
                raw_data = await response.text()
                if raw_data.strip():
                    data = [int(x) for x in raw_data.split() if x.isdigit()]
                    if data:
                        fetch_calls += 1
                        return data
            logger.warning(f"Fetch failed with status {response.status}, no data")
            return []
        except Exception as e:
            logger.warning(f"Fetch attempt {attempt+1}/{retries} failed: {e}")
            await asyncio.sleep(0.1)
    logger.error("Fetch failed after retries.")
    fetch_calls += 1
    return []

async def fetch_continuously(queue):
    """Fetch data for duration."""
    global total_numbers_fetched
    async with aiohttp.ClientSession() as session:
        while time.time() - start_time < RUN_DURATION:
            tasks = [fetch_data(session) for _ in range(FETCH_WORKERS)]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"Fetch task failed: {result}")
                    continue
                if result:
                    total_numbers_fetched += len(result)
                    for i in range(0, len(result), NUMBERS_PER_RECORD):
                        chunk = result[i:i + NUMBERS_PER_RECORD]
                        await queue.put(chunk)
                    if total_numbers_fetched % 1_000_000 < len(result):
                        logger.info(f"Fetched {total_numbers_fetched} numbers")
            await asyncio.sleep(0.001)

async def send_to_kinesis(queue):
    """Send data to Kinesis asynchronously with aioboto3."""
    global total_numbers_sent, total_bytes_sent
    async with aioboto3.Session().client("kinesis", region_name=AWS_REGION) as kinesis_client:
        while time.time() - start_time < RUN_DURATION:
            batch = []
            for _ in range(BATCH_SIZE):
                try:
                    numbers = await asyncio.wait_for(queue.get(), timeout=0.5)
                    if numbers:
                        batch.append({
                            "Data": json.dumps({"numbers": numbers}),
                            "PartitionKey": str(hash(str(numbers)) % 1000)
                        })
                    queue.task_done()
                except asyncio.TimeoutError:
                    break
                except asyncio.QueueEmpty:
                    break

            if batch:
                try:
                    response = await kinesis_client.put_records(
                        StreamName=KINESIS_STREAM_NAME,
                        Records=batch
                    )
                    if response["FailedRecordCount"] > 0:
                        logger.error(f"Kinesis put failed for {response['FailedRecordCount']} records")
                    num_records = len(batch)
                    total_numbers_sent += sum(len(json.loads(r["Data"])["numbers"]) for r in batch)
                    total_bytes_sent += sum(len(r["Data"].encode("utf-8")) for r in batch)
                    if total_numbers_sent % 1_000_000 < sum(len(json.loads(r["Data"])["numbers"]) for r in batch):
                        logger.info(f"Sent {total_numbers_sent} numbers, {total_bytes_sent} bytes")
                except Exception as e:
                    logger.error(f"Kinesis send failed: {e}")
                    await asyncio.sleep(0.5)
            await asyncio.sleep(0.001)

async def main():
    queue = asyncio.Queue(maxsize=QUEUE_SIZE)
    fetch_task = asyncio.create_task(fetch_continuously(queue))
    send_tasks = [asyncio.create_task(send_to_kinesis(queue)) for _ in range(SEND_WORKERS)]
    await asyncio.wait([fetch_task] + send_tasks, timeout=RUN_DURATION)

if __name__ == "__main__":
    logger.info("Starting producer for 1 minute on MacBook Air M2...")
    try:
        asyncio.run(main())
    except Exception as e:
        logger.error(f"Main execution failed: {e}")
    finally:
        elapsed = time.time() - start_time
        logger.setLevel(logging.INFO)
        logger.info(f"Completed in {elapsed:.2f} seconds")
        logger.info(f"Total numbers fetched: {total_numbers_fetched}")
        logger.info(f"Total numbers sent: {total_numbers_sent}")
        rate = total_numbers_sent / (elapsed / 60)
        logger.info(f"Throughput: {rate:,.0f} numbers/minute")
        if fetch_calls > 0:
            logger.info(f"Avg fetch calls per minute: {fetch_calls / (elapsed / 60):.2f}")