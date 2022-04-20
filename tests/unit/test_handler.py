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
        "body": "{\"name\":\"test\",\"email\":\"test@gmail.com\",\"message\":\"test\"}",
        "resource": "/{proxy+}",
        "requestContext": {
            "resourceId": "123456",
            "apiId": "1234567890",
            "resourcePath": "/{proxy+}",
            "httpMethod": "POST",
            "requestId": "c6af9ac6-7b61-11e6-9a41-93e8deadbeef",
            "accountId": "123456789012",
            "identity": {
                "apiKey": "",
                "userArn": "",
                "cognitoAuthenticationType": "",
                "caller": "",
                "userAgent": "Custom User Agent String",
                "user": "",
                "cognitoIdentityPoolId": "",
                "cognitoIdentityId": "",
                "cognitoAuthenticationProvider": "",
                "sourceIp": "127.0.0.1",
                "accountId": "",
            },
            "stage": "prod",
        },
        "queryStringParameters": {"foo": "bar"},
        "headers": {
            "Via": "1.1 08f323deadbeefa7af34d5feb414ce27.cloudfront.net (CloudFront)",
            "Accept-Language": "en-US,en;q=0.8",
            "CloudFront-Is-Desktop-Viewer": "true",
            "CloudFront-Is-SmartTV-Viewer": "false",
            "CloudFront-Is-Mobile-Viewer": "false",
            "X-Forwarded-For": "127.0.0.1, 127.0.0.2",
            "CloudFront-Viewer-Country": "US",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Upgrade-Insecure-Requests": "1",
            "X-Forwarded-Port": "443",
            "Host": "1234567890.execute-api.us-east-1.amazonaws.com",
            "X-Forwarded-Proto": "https",
            "X-Amz-Cf-Id": "aaaaaaaaaae3VYQb9jd-nvCd-de396Uhbp027Y2JvkCPNLmGJHqlaA==",
            "CloudFront-Is-Tablet-Viewer": "false",
            "Cache-Control": "max-age=0",
            "User-Agent": "Custom User Agent String",
            "CloudFront-Forwarded-Proto": "https",
            "Accept-Encoding": "gzip, deflate, sdch",
        },
        "pathParameters": {"proxy": "/examplepath"},
        "httpMethod": "POST",
        "stageVariables": {"baz": "qux"},
        "path": "/examplepath",
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
    apigw_event['body'] = json.dumps({"name": "test", "message": "test"})
    ret = app.lambda_handler(apigw_event, "")
    assert ret["statusCode"] == 500


@mock_dynamodb
def test_returns_error_message_if_unsuccessful(apigw_event):
    table = setup_dynamodb_table()
    assert_table_exists(table)
    apigw_event['body'] = json.dumps({"name": "test", "message": "test"})
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
    apigw_event['body'] = json.dumps({"name": "test", "message": "test"})
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
    apigw_event['body'] = json.dumps({"name": 123456, "message": "test", "email": "test@gmail.com"})
    app.lambda_handler(apigw_event, "")
    capture.check(
        credential_log,
        event_received_log,
        ('root', 'ERROR', 'Parameter validation failed:\n'
                          "Invalid type for parameter Item.Name.S, value: 123456, type: <class 'int'>, "
                          "valid types: <class 'str'>"),
    )

