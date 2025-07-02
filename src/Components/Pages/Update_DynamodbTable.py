import boto3

# Initialize the DynamoDB resource
dynamodb = boto3.resource('dynamodb', region_name='ca-central-1')

# Your table name
table = dynamodb.Table('CustomerTable')

# Scan to get all items (up to 1MB)
response = table.scan()
items = response.get('Items', [])

# Update each item to add the new attribute
for item in items:
    table.update_item(
        Key={'id': item['id']},
        UpdateExpression='SET #attr = :val',
        ExpressionAttributeNames={
            '#attr': 'Downloaded Founder Series SUN'
        },
        ExpressionAttributeValues={
            ':val': False
        }
    )

print(f"Updated {len(items)} items.")
