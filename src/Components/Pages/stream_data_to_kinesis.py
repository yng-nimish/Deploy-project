import boto3
import requests
import json
import time

# Kinesis Configuration
KINESIS_STREAM_NAME = "your-kinesis-stream-name"  # Replace with your Kinesis Stream name
AWS_REGION = "us-east-1"  # Replace with your AWS region
kinesis_client = boto3.client('kinesis', region_name=AWS_REGION)

# Data Source API Configuration
API_URL = "http://your-server-address:4900/num"
API_PARAMS = {
    "floor": 100,
    "limit": 1000,
    "count": 10
}

def get_data_from_api():
    """Fetch data from the API endpoint with retries."""
    
    retries = 5
    for _ in range(retries):
        try:
            response = requests.get(API_URL, params=API_PARAMS)
            response.raise_for_status()  # Raise error for invalid responses
            return response.json()  # Assuming the API returns JSON data
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            time.sleep(1)  # Wait before retrying
    return None

def send_data_to_kinesis(data):
    """Send the fetched data to Kinesis stream."""
    try:
        for record in data:
            # Convert record to JSON string for transmission to Kinesis
            json_record = json.dumps(record)
            partition_key = "partition_key"  # You can use a more dynamic partition key
            
            # Put record into Kinesis stream
            response = kinesis_client.put_record(
                StreamName=KINESIS_STREAM_NAME,
                Data=json_record,
                PartitionKey=partition_key
            )
            print(f"Successfully sent record to Kinesis: {response['SequenceNumber']}")
    except Exception as e:
        print(f"Error sending data to Kinesis: {e}")

def main():
    """Main function to continuously fetch and send data."""
    while True:
        data = get_data_from_api()
        if data:
            send_data_to_kinesis(data)
        time.sleep(1)  # Sleep for 1 seconds before fetching data again

if __name__ == "__main__":
    main()
