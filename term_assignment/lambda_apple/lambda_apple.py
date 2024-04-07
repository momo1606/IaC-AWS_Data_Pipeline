import json
import boto3
import os
from datetime import datetime

sns = boto3.client('sns')
sns_topic_arn = os.environ['SNS_TOPIC_ARN']

def handler(event, context):
    brand = 'apple'  
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(os.environ['TABLE_NAME'])
    
    # Query the DynamoDB table for the brand
    response = table.scan(
        FilterExpression='brand = :brand',
        ExpressionAttributeValues={':brand': brand}
    )

    items = response['Items']
    total_views = sum(1 for item in items if item['event_type'] == 'view')
    total_purchases = sum(1 for item in items if item['event_type'] == 'purchase')
    total_price = sum(float(item['price']) for item in items if item['event_type'] == 'purchase')

    # Generate report
    report = {
        'brand': brand,
        'total_views': total_views,
        'total_purchases': total_purchases,
        'total_price': total_price
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
        Subject=f"Clickstream Analysis for {brand}"
    )

    return report
