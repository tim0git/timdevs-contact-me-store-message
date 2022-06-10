import json
import os
import boto3
import uuid
import time
from aws_lambda_powertools import Logger, Tracer

logger = Logger(service="store-message-staging")
tracer = Tracer(service="store-message-staging")


@tracer.capture_method
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


@tracer.capture_lambda_handler
def lambda_handler(event, context):
    logger.info("Received event")
    try:
        body = event['Records'][0]['body']
        message = json.loads(body)
        write_message_to_table(message)
        return
    except Exception as e:
        logger.error(str(e))
        raise RuntimeError(e)
