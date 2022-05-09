import logging
import os
import boto3
import pytest
import app
from moto import mock_dynamodb
import json
from testfixtures import log_capture
import time
from aws_xray_sdk import global_sdk_config

# disable xray for testing purposes
global_sdk_config.set_sdk_enabled(False)


@pytest.fixture()
def apigw_event():
    """ Generates API GW Event"""

    return {
        "Records": [
            {
                "messageId": "059f36b4-87a3-44ab-83d2-661975830a7d",
                "receiptHandle": "AQEBwJnKyrHigUMZj6rYigCgxlaS3SLy0a...",
                "body": "{\"name\":\"test\",\"email\":\"test@gmail.com\",\"message\":\"test\"}",
                "attributes": {
                    "ApproximateReceiveCount": "1",
                    "SentTimestamp": "1545082649183",
                    "SenderId": "AIDAIENQZJOLO23YVJ4VO",
                    "ApproximateFirstReceiveTimestamp": "1545082649185"
                },
                "messageAttributes": {},
                "md5OfBody": "e4e68fb7bd0e697a0ae8f1bb342846b3",
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:us-east-2:123456789012:my-queue",
                "awsRegion": "us-east-2"
            },
            {
                "messageId": "2e1424d4-f796-459a-8184-9c92662be6da",
                "receiptHandle": "AQEBzWwaftRI0KuVm4tP+/7q1rGgNqicHq...",
                "body": "Test message.",
                "attributes": {
                    "ApproximateReceiveCount": "1",
                    "SentTimestamp": "1545082650636",
                    "SenderId": "AIDAIENQZJOLO23YVJ4VO",
                    "ApproximateFirstReceiveTimestamp": "1545082650649"
                },
                "messageAttributes": {},
                "md5OfBody": "e4e68fb7bd0e697a0ae8f1bb342846b3",
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:us-east-2:123456789012:my-queue",
                "awsRegion": "us-east-2"
            }
        ]
    }


table_name = 'test-table'


def setup_dynamodb_table():
    client = boto3.resource('dynamodb')
    table = client.create_table(
        BillingMode='PAY_PER_REQUEST',
        TableName=table_name,
        KeySchema=[
            {'AttributeName': 'ID', 'KeyType': 'HASH'},
            {'AttributeName': 'Email', 'KeyType': 'RANGE'}
        ],
        AttributeDefinitions=[
            {'AttributeName': 'ID', 'AttributeType': 'S'},
            {'AttributeName': 'Email', 'AttributeType': 'S'}
        ],
    )
    os.environ['TABLE_NAME'] = table_name
    return table


def assert_table_exists(test_table):
    test_table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
    assert test_table.table_status == 'ACTIVE'


# Success
@mock_dynamodb
def test_returns_status_code_200_if_successful(apigw_event):
    table = setup_dynamodb_table()
    assert_table_exists(table)
    ret = app.lambda_handler(apigw_event, "")
    assert ret["statusCode"] == 200


@mock_dynamodb
def test_returns_success_message_if_successful(apigw_event):
    table = setup_dynamodb_table()
    assert_table_exists(table)
    ret = app.lambda_handler(apigw_event, "")
    assert ret["body"] == '{"message": "success"}'


@mock_dynamodb
def test_stores_ttl_in_dynamodb_successfully(apigw_event):
    table = setup_dynamodb_table()
    assert_table_exists(table)
    app.lambda_handler(apigw_event, "")
    result = table.scan()
    expected_ttl = int(time.time()) + (30 * 86400)
    ttl_stored_in_dynamo_db = result['Items'][0]['TTL']
    assert ttl_stored_in_dynamo_db == expected_ttl


# Error
@mock_dynamodb
def test_returns_status_code_500_if_unsuccessful(apigw_event):
    table = setup_dynamodb_table()
    assert_table_exists(table)
    apigw_event['Records'][0]['body'] = json.dumps({"name": "test", "message": "test"})
    ret = app.lambda_handler(apigw_event, "")
    assert ret["statusCode"] == 500


@mock_dynamodb
def test_returns_error_message_if_unsuccessful(apigw_event):
    table = setup_dynamodb_table()
    assert_table_exists(table)
    apigw_event['Records'][0]['body'] = json.dumps({"name": "test", "message": "test"})
    ret = app.lambda_handler(apigw_event, "")
    body = json.loads(ret["body"])
    assert body["message"] == "error"


# Logging
credential_log = ('botocore.credentials', 'INFO', 'Found credentials in environment variables.')
event_received_log = ('root', 'INFO', 'Received event')


@mock_dynamodb
@log_capture(level=logging.INFO)
def test_logs_info_request_and_success(capture, apigw_event):
    table = setup_dynamodb_table()
    assert_table_exists(table)
    app.lambda_handler(apigw_event, "")
    capture.check(
        credential_log,
        event_received_log,
        ('root', 'INFO', 'Message written to Table: {}'.format(os.environ['TABLE_NAME']))
    )


@mock_dynamodb
@log_capture(level=logging.INFO)
def test_logs_info_request_and_error_when_parameter_is_missing(capture, apigw_event):
    table = setup_dynamodb_table()
    assert_table_exists(table)
    apigw_event['Records'][0]['body'] = json.dumps({"name": "test", "message": "test"})
    app.lambda_handler(apigw_event, "")
    capture.check(
        credential_log,
        event_received_log,
        ('root', 'ERROR', "'email'"),
    )


@mock_dynamodb
@log_capture(level=logging.INFO)
def test_logs_info_request_and_error_when_parameter_is_of_wrong_type(capture, apigw_event):
    table = setup_dynamodb_table()
    assert_table_exists(table)
    apigw_event['Records'][0]['body'] = json.dumps({"name": 123456, "message": "test", "email": "test@gmail.com"})
    app.lambda_handler(apigw_event, "")
    capture.check(
        credential_log,
        event_received_log,
        ('root', 'ERROR', 'Parameter validation failed:\n'
                          "Invalid type for parameter Item.Name.S, value: 123456, type: <class 'int'>, "
                          "valid types: <class 'str'>"),
    )

