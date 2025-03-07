import asyncio
import struct
import time
import logging
import boto3
import json
from botocore.config import Config

# Kinesis Configuration
KINESIS_STREAM_NAME = "coffeeStream"
AWS_REGION = "us-east-1"
kinesis_client = boto3.client(
    "kinesis",
    region_name=AWS_REGION,
    config=Config(max_pool_connections=100)  # Increased for more workers
)

# TCP API Configuration
TCP_HOST = "54.237.6.147"
TCP_PORT = 4902
BYTES_PER_NUMBER = 4
NUMBERS_PER_REQUEST = 4_000_000  # 4M numbers/request (2x increase)
NUMBERS_PER_RECORD = 2000  # 500 numbers/record (4x records/second)

# Logging Setup (minimized for speed)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('data_pipeline.log')]
)
logger = logging.getLogger()

# Constants
FETCH_WORKERS = 24  # 2x increase
SEND_WORKERS = 48  # 2x increase
TARGET_COUNT = 200_000_000  # 200M to ensure 100M in 30s
BATCH_SIZE = 500
QUEUE_SIZE = 100_000  # 50M numbers capacity (adjusted for smaller records)
RUN_DURATION = 30  # 30 seconds

# Metrics
total_numbers_fetched = 0
total_numbers_sent = 0
total_bytes_sent = 0
fetch_time_total = 0
send_time_total = 0
fetch_calls = 0
send_calls = 0
start_time = time.time()

async def fetch_data_tcp():
    global fetch_time_total, fetch_calls
    fetch_start = time.time()
    try:
        reader, writer = await asyncio.open_connection(TCP_HOST, TCP_PORT)
        bytes_requested = int(NUMBERS_PER_REQUEST * BYTES_PER_NUMBER * 1.01)
        writer.write(struct.pack('<I', bytes_requested))
        await writer.drain()

        data = await reader.readexactly(bytes_requested)
        numbers = []
        i = 0
        max_val = 2**32 - 1
        range_size = 1000
        threshold = max_val - (max_val % range_size)

        while len(numbers) < NUMBERS_PER_REQUEST and i < len(data) - 3:
            num = int.from_bytes(data[i:i+4], 'little')
            i += 4
            if num <= threshold:
                numbers.append(num % range_size)

        writer.close()
        await writer.wait_closed()
        fetch_time_total += time.time() - fetch_start
        fetch_calls += 1
        # Logger minimized
        if fetch_calls % 10 == 0:
            logger.info(f"Fetched {len(numbers)} numbers from TCP")
        return numbers
    except Exception as e:
        logger.error(f"TCP fetch failed: {repr(e)}")
        return []

async def fetch_continuously(queue):
    global total_numbers_fetched
    while total_numbers_fetched < TARGET_COUNT and time.time() - start_time < RUN_DURATION:
        try:
            tasks = [fetch_data_tcp() for _ in range(FETCH_WORKERS)]
            batches = await asyncio.gather(*tasks)
            for batch in batches:
                if batch:
                    total_numbers_fetched += len(batch)
                    for i in range(0, len(batch), NUMBERS_PER_RECORD):
                        chunk = batch[i:i + NUMBERS_PER_RECORD]
                        await queue.put(chunk)
                    if total_numbers_fetched % 10_000_000 == 0:
                        logger.info(f"Total fetched: {total_numbers_fetched}")
        except Exception as e:
            logger.error(f"TCP fetch failed: {repr(e)}")
            await asyncio.sleep(0.1)

async def send_to_kinesis(queue):
    global total_numbers_sent, total_bytes_sent, send_time_total, send_calls
    while total_numbers_sent < TARGET_COUNT and time.time() - start_time < RUN_DURATION:
        batch = []
        for _ in range(BATCH_SIZE):
            try:
                numbers = await asyncio.wait_for(queue.get(), timeout=0.1)  # Faster timeout
                if numbers:
                    batch.append({
                        "Data": json.dumps(numbers).encode("utf-8"),  # Skip dict wrapper
                        "PartitionKey": str(hash(str(numbers)) % 1000)
                    })
                queue.task_done()
            except asyncio.TimeoutError:
                break

        if batch:
            send_start = time.time()
            retries = 3
            for attempt in range(retries):
                try:
                    response = await asyncio.to_thread(
                        kinesis_client.put_records,
                        StreamName=KINESIS_STREAM_NAME,
                        Records=batch
                    )
                    failed_count = response["FailedRecordCount"]
                    if failed_count > 0:
                        logger.error(f"Kinesis put failed for {failed_count} records on attempt {attempt + 1}")
                        failed_records = [r for i, r in enumerate(batch) if "ErrorCode" in response["Records"][i]]
                        batch = failed_records
                        if attempt < retries - 1:
                            await asyncio.sleep(0.05 * (attempt + 1))
                            continue
                    total_numbers_sent += sum(len(json.loads(r["Data"].decode("utf-8"))) for r in batch)
                    total_bytes_sent += sum(len(r["Data"]) for r in batch)
                    send_time_total += time.time() - send_start
                    send_calls += 1
                    if send_calls % 10 == 0:
                        logger.info(f"Sent {sum(len(json.loads(r['Data'].decode('utf-8'))) for r in batch)} numbers")
                    break
                except Exception as e:
                    logger.error(f"Kinesis send failed: {repr(e)}")
                    if attempt < retries - 1:
                        await asyncio.sleep(0.05 * (attempt + 1))
                    else:
                        break

async def main():
    queue = asyncio.Queue(maxsize=QUEUE_SIZE)
    fetch_task = asyncio.create_task(fetch_continuously(queue))
    send_tasks = [asyncio.create_task(send_to_kinesis(queue)) for _ in range(SEND_WORKERS)]
    await asyncio.wait([fetch_task] + send_tasks, timeout=RUN_DURATION)
    fetch_task.cancel()
    for task in send_tasks:
        task.cancel()
    await asyncio.gather(fetch_task, *send_tasks, return_exceptions=True)

if __name__ == "__main__":
    logger.info("Starting pipeline for 30 seconds to fetch 100M numbers...")
    try:
        asyncio.run(main())
    except Exception as e:
        logger.error(f"Main execution failed: {e}")
    finally:
        elapsed = time.time() - start_time
        logger.info(f"Completed in {elapsed:.2f} seconds")
        logger.info(f"Total numbers fetched: {total_numbers_fetched}")
        logger.info(f"Total numbers sent: {total_numbers_sent}")
        rate = total_numbers_sent / (elapsed / 60) if elapsed > 0 else 0
        logger.info(f"Throughput: {rate:,.0f} numbers/minute")
        if fetch_calls > 0:
            logger.info(f"Avg fetch time: {fetch_time_total / fetch_calls:.3f} seconds ({fetch_calls} calls)")
        if send_calls > 0:
            logger.info(f"Avg send time: {send_time_total / send_calls:.3f} seconds ({send_calls} calls)")