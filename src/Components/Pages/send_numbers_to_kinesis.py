import boto3
import time

kinesis_client = boto3.client('kinesis', region_name='us-east-1')  
stream_name = 'hellostream'  

file_path = "numbers.txt"  #the file with our random numbers

# Define the batch size (no more than 500 records per batch)
batch_size = 500  # Kinesis can handle up to 500 records in a single batch

def send_numbers_to_kinesis_in_batches():
    # Open the file and process the numbers in chunks
    with open(file_path, 'r') as file:
        batch = []
        
        # Read the file line by line
        for line in file:
            # Clean up any extra whitespace and convert to integer
            number_str = line.strip()
            if not number_str:
                continue
            number = int(number_str)
            
            # Add the number to the current batch
            # Convert each number to a dictionary for 'put_records'
            record = {
                'Data': str(number).encode('utf-8'),  # The data must be encoded in bytes
                'PartitionKey': str(number % 10)  # Example: Dynamic partition key based on number's last digit
            }
            batch.append(record)
            
            # Once the batch reaches the defined size, send it to Kinesis
            if len(batch) == batch_size:
                # Send the batch to Kinesis
                send_batch_to_kinesis(batch)
                batch = []  # Reset the batch for the next set of numbers
                
        # Send any remaining numbers in the last batch
        if batch:
            send_batch_to_kinesis(batch)

def send_batch_to_kinesis(batch):
    # Ensure that the batch contains no more than 500 records
    if len(batch) > 500:
        print(f"Error: Batch size exceeded. Contains {len(batch)} records.")
        return
    
    # Send the batch of records to Kinesis using `put_records`
    try:
        response = kinesis_client.put_records(
            StreamName=stream_name,
            Records=batch  # List of records id 
        )
        
        # Handle errors if any records fail
        failed_record_count = response.get('FailedRecordCount', 0)
        if failed_record_count > 0:
            print(f"Failed to send {failed_record_count} records in this batch.")
        
        # Print successful sequence numbers
        
        for record in response['Records']:
            print(f"Sent data to Kinesis with SequenceNumber: {record['SequenceNumber']}")
    
    except Exception as e:
        print(f"Error sending batch to Kinesis: {e}")
    
    # Optionally, add a small delay to avoid hitting API rate limits
    time.sleep(0.5)

# Call the function to send numbers in batches
send_numbers_to_kinesis_in_batches()
