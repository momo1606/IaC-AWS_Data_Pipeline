import json
import base64
import boto3
from datetime import datetime, timedelta
from boto3.dynamodb.conditions import Key
import os
from decimal import Decimal

# Initialize DynamoDB and SNS clients
dynamodb = boto3.resource('dynamodb')
sns = boto3.client('sns')

# Use environment variables to configure the table name and SNS topic ARN
table_name = os.environ['TABLE_NAME']
sns_topic_arn = os.environ['SNS_TOPIC_ARN']

table = dynamodb.Table(table_name)

def lambda_handler(event, context):
    output = []

    for record in event['records']:
        # Decode from base64
        decoded_data = base64.b64decode(record['data']).decode('utf-8')
        payload = json.loads(decoded_data)
        print(payload)

        # Processing logic
        process_record(payload)

        # Append the original record data to the output, unchanged
        output_record = {
            'recordId': record['recordId'],
            'result': 'Ok',
            'data': record['data']
        }
        output.append(output_record)

    return {'records': output}

def process_record(payload):
    # Store record in DynamoDB
    item = {
        'user_id': payload['user_id'],
        'txn_timestamp': payload['txn_timestamp'],
        'event_type': payload['event_type'],
        'product_id': payload['product_id'],
        'category_id': payload['category_id'],
        'category_code': payload.get('category_code', ''),
        'brand': payload.get('brand', ''),
        'price': Decimal(str(payload.get('price', 0))),  
        'user_session': payload['user_session'],
        'event_time': payload['event_time']
    }
    table.put_item(Item=item)

    # Check for potential DDoS activity
    check_for_ddos(payload['user_id'], payload['txn_timestamp'])

def check_for_ddos(user_id, txn_timestamp):
    # Convert the timestamp to a datetime object
    txn_time = datetime.fromisoformat(txn_timestamp.rstrip('Z'))
    time_threshold = txn_time - timedelta(seconds=20)

    # Query DynamoDB for user actions within the last 20 seconds
    response = table.query(
        KeyConditionExpression=Key('user_id').eq(user_id) & Key('txn_timestamp').gte(time_threshold.isoformat())
    )
    print(response)

    # Flag if more than 4 actions are detected
    if response['Count'] > 4:
        print(f"Potential DDoS detected for user {user_id}")
        sns.publish(
            TopicArn=sns_topic_arn,
            Message=f"Potential DDoS detected for user {user_id}",
            Subject=f"DDoS Alert - {datetime.now().isoformat()}"
        )
