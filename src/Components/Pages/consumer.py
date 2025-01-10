import boto3
my_stream_name ='hellostream'
kinesis_client = boto3.client('kinesis', region_name='us-east-1')

def get_records():
    records = []
    shardIterator = kinesis_client.get_shard_iterator(
        StreamName=my_stream_name,
        ShardId='shardId-000000000000',
        ShardIteratorType= 'LATEST'
    )["ShardIterator"]

    #print(shardIterator)
    while(len(records) == 0 and shardIterator != None):
        response = kinesis_client.get_records(ShardIterator=shardIterator,Limit=123)
        print(response["Records"])
        shardIterator = response["NextShardIterator"]

get_records()