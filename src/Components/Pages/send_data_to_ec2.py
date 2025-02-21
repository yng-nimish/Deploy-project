import requests
import json

# The public IP of your EC2 instance and the Flask endpoint
EC2_PUBLIC_IP = "http://44.202.114.62:5002/send_data"  # Replace with your actual EC2 IP

# Read the local data (from 'numbers.txt')
file_path = "numbers.txt"

# Open the file and read all lines (assuming one number per line)
with open(file_path, 'r') as file:
    # Read all lines and strip out extra whitespace (like newlines)
    numbers = [line.strip() for line in file.readlines()]

# Prepare the data to send as a JSON object (list of numbers)
data = {"numbers": numbers}

# Send the data to the EC2 instance
response = requests.post(EC2_PUBLIC_IP, json=data)

# Check the response from the EC2 instance
if response.status_code == 200:
    print("Data successfully sent to EC2 and Kinesis.")
else:
    print(f"Failed to send data: {response.status_code} - {response.text}")
