import boto3
import json
import time

kinesis_client = boto3.client('kinesis', region_name='us-east-1')
stream_name = 'HilrayStream'

file_path = "numbers.txt"  # The file with random numbers

# Define the batch size (no more than 500 records per batch)
batch_size = 500

def send_numbers_to_kinesis_in_batches():
    with open(file_path, 'r') as file:
        batch = []
        numbers_array = []  # Accumulate numbers into an array
        
        for line in file:
            number_str = line.strip()
            if not number_str:
                continue
            number = int(number_str)
            numbers_array.append(number)
            
            # Group numbers into arrays of, say, 10 (adjustable)
            if len(numbers_array) >= 10:
                record = {
                    'Data': json.dumps({"numbers": numbers_array}).encode('utf-8'),
                    'PartitionKey': str(numbers_array[0] % 10)  # Use first number for partition key
                }
                batch.append(record)
                numbers_array = []  # Reset array
            
            # Send batch when it hits size limit
            if len(batch) == batch_size:
                send_batch_to_kinesis(batch)
                batch = []
        
        # Send remaining numbers
        if numbers_array:
            record = {
                'Data': json.dumps({"numbers": numbers_array}).encode('utf-8'),
                'PartitionKey': str(numbers_array[0] % 10)
            }
            batch.append(record)
        if batch:
            send_batch_to_kinesis(batch)

def send_batch_to_kinesis(batch):
    if len(batch) > 500:
        print(f"Error: Batch size exceeded. Contains {len(batch)} records.")
        return
    
    try:
        response = kinesis_client.put_records(
            StreamName=stream_name,
            Records=batch
        )
        failed_record_count = response.get('FailedRecordCount', 0)
        if failed_record_count > 0:
            print(f"Failed to send {failed_record_count} records in this batch.")
        for record in response['Records']:
            print(f"Sent data to Kinesis with SequenceNumber: {record['SequenceNumber']}")
    except Exception as e:
        print(f"Error sending batch to Kinesis: {e}")
    
    time.sleep(0.5)

send_numbers_to_kinesis_in_batches()