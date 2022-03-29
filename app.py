import json
import os
import boto3
import logging
import uuid
import time

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def write_message_to_table(message):
    table_name = os.environ.get('TABLE_NAME')
    client = boto3.client('dynamodb')
    response = client.put_item(
        TableName=table_name,
        Item={
            'ID': {'S': str(uuid.uuid4())},
            'Email': {'S': message['email']},
            'Message': {'S': message['message']},
            'Name': {'S': message['name']},
            'TTL': {'N': str(int(time.time()) + (30 * 86400))},
        }
    )
    logger.info('Message written to Table: {}'.format(table_name))
    return response


def lambda_handler(event, context):
    logger.info("Received event")
    try:
        body = event['body']
        message = json.loads(body)
        write_message_to_table(message)
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'success'})
        }
    except Exception as e:
        logger.error(str(e))
        print(e)
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'error'})
        }

