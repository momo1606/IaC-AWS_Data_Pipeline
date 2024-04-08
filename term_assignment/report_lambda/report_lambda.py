import json
import boto3
import os
from datetime import datetime


# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb')
sns = boto3.client('sns')
sns_topic_arn = os.environ['SNS_TOPIC_ARN']

def lambda_handler(event, context):
    # Get the table name from environment variables
    table_name = os.environ['TABLE_NAME']
    table = dynamodb.Table(table_name)

    # Parse the brand from the request
    try:
        brand = json.loads(event['body'])['brand']
    except KeyError:
        return {'statusCode': 400, 'body': json.dumps('Brand parameter is required')}

    # Query the DynamoDB table for the specified brand
    response = table.scan(
        FilterExpression='brand = :brand',
        ExpressionAttributeValues={':brand': brand}
    )
    
    items = response['Items']

    # Compute the metrics
    views = sum(1 for item in items if item['event_type'] == 'view')
    purchases = sum(1 for item in items if item['event_type'] == 'purchase')
    total_sales = sum(float(item['price']) for item in items if item['event_type'] == 'purchase')

    # Prepare the report
    report = {
        'brand': brand,
        'views': views,
        'purchases': purchases,
        'total_sales': total_sales,
        'timestamp': datetime.now().isoformat()
    }

    print(report)

    # Save the report to S3
    s3 = boto3.client('s3')
    s3.put_object(
        Bucket=os.environ['S3_BUCKET'],
        Key=f'reports/{brand}_report_{datetime.now().isoformat()}.json',
        Body=json.dumps(report)
    )

    sns.publish(
        TopicArn=sns_topic_arn,
        Message=f"Brand report for {brand}-\n {json.dumps(report)}",
        Subject=f"Clickstream Analysis for {brand} - {datetime.now().isoformat()}"
    )

    return {
        'statusCode': 200,
        'body': json.dumps(report)
    }
