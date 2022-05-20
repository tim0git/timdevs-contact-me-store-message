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
                "body": "{\"name\":\"test-name\",\"email\":\"test@gmail.com\",\"message\":\"test-message\"}",
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


def get_message_from_event(apigw_event):
    body = apigw_event['Records'][0]['body']
    return json.loads(body)


# Success
@mock_dynamodb
def test_stores_name_in_dynamodb_successfully(apigw_event):
    table = setup_dynamodb_table()
    assert_table_exists(table)
    app.lambda_handler(apigw_event, "")
    result = table.scan()
    expected_name = get_message_from_event(apigw_event)["name"]
    name_stored_in_dynamo_db = result['Items'][0]['Name']
    assert name_stored_in_dynamo_db == expected_name


@mock_dynamodb
def test_stores_message_in_dynamodb_successfully(apigw_event):
    table = setup_dynamodb_table()
    assert_table_exists(table)
    app.lambda_handler(apigw_event, "")
    result = table.scan()
    expected_message = get_message_from_event(apigw_event)["message"]
    message_stored_in_dynamo_db = result['Items'][0]['Message']
    assert message_stored_in_dynamo_db == expected_message


@mock_dynamodb
def test_stores_email_in_dynamodb_successfully(apigw_event):
    table = setup_dynamodb_table()
    assert_table_exists(table)
    app.lambda_handler(apigw_event, "")
    result = table.scan()
    expected_email = get_message_from_event(apigw_event)["email"]
    email_stored_in_dynamo_db = result['Items'][0]['Email']
    assert email_stored_in_dynamo_db == expected_email


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
def test_handles_resource_not_found_error_when_writing_to_dynamodb_table(apigw_event):
    with pytest.raises(RuntimeError):
        table = setup_dynamodb_table()
        os.environ['TABLE_NAME'] = "wrong-test-table-name"
        assert_table_exists(table)
        app.lambda_handler(apigw_event, "")


@mock_dynamodb
def test_handles_validation_error_when_email_value_is_not_of_valid_type(apigw_event):
    with pytest.raises(RuntimeError):
        table = setup_dynamodb_table()
        assert_table_exists(table)
        apigw_event['Records'][0]['body'] = json.dumps({"name": "test", "email": "", "message": "test-message"})
        app.lambda_handler(apigw_event, "")


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
    with pytest.raises(Exception):
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
    with pytest.raises(Exception):
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
