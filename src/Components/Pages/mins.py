import asyncio
import aiohttp
import time
import logging
import boto3
import json
from concurrent.futures import ThreadPoolExecutor
from botocore.config import Config

# Kinesis Configuration
KINESIS_STREAM_NAME = "GiantStream"
AWS_REGION = "us-east-1"
kinesis_client = boto3.client(
    "kinesis",
    region_name=AWS_REGION,
    config=Config(max_pool_connections=50)
)

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

# Constants
FETCH_WORKERS = 12  # Balanced for f1.2xlarge, reduces throttling risk
SEND_WORKERS = 12
NUMBERS_PER_RECORD = 2000
BATCH_SIZE = 500
QUEUE_SIZE = 20_000
RUN_DURATION = 60
TARGET_NUMBERS = 100_000_000

# Metrics
total_numbers_sent = 0
total_bytes_sent = 0
total_numbers_fetched = 0
start_time = time.time()
fetch_time_total = 0
send_time_total = 0
fetch_calls = 0
send_calls = 0

# Thread pool
executor = ThreadPoolExecutor(max_workers=SEND_WORKERS)

async def fetch_data(session):
    """Fetch data, accepting 400 status if data is returned."""
    global total_numbers_fetched, fetch_time_total, fetch_calls
    fetch_start = time.time()
    try:
        async with session.get(API_URL, params=API_PARAMS, timeout=10) as response:
            raw_data = await response.text()
            # Accept 200 or 400 if data is present
            if response.status in (200, 400) and raw_data.strip():
                data = list(map(int, raw_data.split()))
                fetch_time_total += time.time() - fetch_start
                fetch_calls += 1
                if response.status == 400:
                    logger.warning(f"Got 400 but received {len(data)} numbers")
                return data
            else:
                logger.warning(f"Fetch failed with status {response.status}, no data")
                fetch_time_total += time.time() - fetch_start
                fetch_calls += 1
                return []
    except aiohttp.ClientResponseError as e:
        logger.warning(f"Fetch failed with status {e.status}: {e.message}")
        fetch_time_total += time.time() - fetch_start
        fetch_calls += 1
        return []
    except Exception as e:
        logger.warning(f"Fetch failed: {str(e)}")
        fetch_time_total += time.time() - fetch_start
        fetch_calls += 1
        return []

async def fetch_continuously(queue):
    """Fetch until target or time limit."""
    global total_numbers_fetched
    async with aiohttp.ClientSession() as session:
        while total_numbers_fetched < TARGET_NUMBERS and time.time() - start_time < RUN_DURATION:
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

def put_records_wrapper(client, stream_name, records):
    """Helper for put_records."""
    return client.put_records(StreamName=stream_name, Records=records)

async def send_to_kinesis(queue, loop):
    """Send to Kinesis."""
    global total_numbers_sent, total_bytes_sent, send_time_total, send_calls
    while total_numbers_sent < TARGET_NUMBERS and time.time() - start_time < RUN_DURATION:
        batch = []
        for _ in range(BATCH_SIZE):
            try:
                numbers = await asyncio.wait_for(queue.get(), timeout=0.5)
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
            send_start = time.time()
            try:
                response = await loop.run_in_executor(
                    executor,
                    put_records_wrapper,
                    kinesis_client,
                    KINESIS_STREAM_NAME,
                    batch
                )
                if response["FailedRecordCount"] > 0:
                    logger.error(f"Kinesis put failed for {response['FailedRecordCount']} records")
                num_records = len(batch)
                total_numbers_sent += sum(len(json.loads(r["Data"].decode("utf-8"))["numbers"]) for r in batch)
                total_bytes_sent += sum(len(r["Data"]) for r in batch)
                send_time_total += time.time() - send_start
                send_calls += 1
            except Exception as e:
                logger.error(f"Kinesis send failed: {e}")
                await asyncio.sleep(0.5)

async def main():
    queue = asyncio.Queue(maxsize=QUEUE_SIZE)
    loop = asyncio.get_event_loop()
    fetch_task = asyncio.create_task(fetch_continuously(queue))
    send_tasks = [asyncio.create_task(send_to_kinesis(queue, loop)) for _ in range(SEND_WORKERS)]
    await asyncio.wait([fetch_task] + send_tasks, timeout=RUN_DURATION)

if __name__ == "__main__":
    logger.info("Starting producer for 1 minute to fetch 100M numbers...")
    try:
        asyncio.run(main())
    except Exception as e:
        logger.error(f"Main execution failed: {e}")
    finally:
        executor.shutdown()
        elapsed = time.time() - start_time
        logger.setLevel(logging.INFO)
        logger.info(f"Completed in {elapsed:.2f} seconds")
        logger.info(f"Total numbers fetched: {total_numbers_fetched}")
        logger.info(f"Total numbers sent: {total_numbers_sent}")
        rate = total_numbers_sent / (elapsed / 60)
        logger.info(f"Throughput: {rate:,.0f} numbers/minute")
        if fetch_calls > 0:
            logger.info(f"Avg fetch time: {fetch_time_total / fetch_calls:.3f} seconds ({fetch_calls} calls)")
        if send_calls > 0:
            logger.info(f"Avg send time: {send_time_total / send_calls:.3f} seconds ({send_calls} calls)")