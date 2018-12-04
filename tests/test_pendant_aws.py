from datetime import datetime

from hypothesis import example, given
from hypothesis.strategies import integers, datetimes

import boto3

from moto import mock_s3

import pytest

from pendant.aws.s3 import S3Uri

TEST_BUCKET_NAME = 'test_bucket'
TEST_KEY_NAME = 'test_key'

@pytest.fixture
def test_bucket():
    with mock_s3():
        boto3.client('s3').create_bucket(Bucket=TEST_BUCKET_NAME)
        yield boto3.resource('s3').Bucket(TEST_BUCKET_NAME)

def test_s3_uri_object_exists(test_bucket):
    assert not S3Uri(f's3://{test_bucket.name}/{TEST_KEY_NAME}').object_exists()
    test_bucket.put_object(Key=TEST_KEY_NAME, Body='')
    assert S3Uri(f's3://{test_bucket.name}/{TEST_KEY_NAME}').object_exists()
