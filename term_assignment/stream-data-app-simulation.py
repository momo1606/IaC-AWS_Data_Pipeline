import boto3
import csv
import json
import os
from time import sleep
from datetime import datetime

# AWS Settings from environment variables
s3_bucket = os.getenv('S3_BUCKET')
s3_key = os.getenv('S3_KEY')
kinesis_stream_name = os.getenv('KINESIS_STREAM_NAME')

s3 = boto3.client('s3', region_name='us-east-1')
s3_resource = boto3.resource('s3', region_name='us-east-1')
kinesis_client = boto3.client('kinesis', region_name='us-east-1')

def stream_data_simulator():
    # Read CSV Lines and split the file into lines
    csv_file = s3_resource.Object(s3_bucket, s3_key)
    s3_response = csv_file.get()
    lines = s3_response['Body'].read().decode('utf-8').split('\n')

    for row in csv.DictReader(lines):
        try:
            
            line_json = json.dumps(row)
            json_load = json.loads(line_json)

            
            json_load['txn_timestamp'] = datetime.now().isoformat()
            print(json_load)
            # Write to Kinesis Streams
            response = kinesis_client.put_record(StreamName=kinesis_stream_name, Data=json.dumps(json_load, indent=4), PartitionKey=str(row['category_id']))
            response['category_code'] = json_load['category_code']
            print('HttpStatusCode:', response['ResponseMetadata']['HTTPStatusCode'], ', ', json_load['category_code'])


            # Adding a temporary pause, for demo purposes
            sleep(1)

        except Exception as e:
            print('Error: {}'.format(e))

if __name__ == '__main__':
    for i in range(0, 5):
        stream_data_simulator()
