from datetime import datetime

import botocore
import boto3
import moto
import pytest

from hypothesis import example, given
from hypothesis.strategies import integers, datetimes

from pendant.aws.batch import BatchJob, JobDefinition
from pendant.aws.exception import BatchJobSubmissionError, S3ObjectNotFoundError
from pendant.aws.logs import AwsLogUtil, LogEvent
from pendant.aws.response import SubmitJobResponse
from pendant.aws.s3 import S3Uri
from pendant.aws.s3 import s3api_head_object, s3api_object_exists, s3_object_exists
from pendant.util import format_ISO8601

TEST_BUCKET_NAME = 'TEST_BUCKET'
TEST_KEY_NAME = 'TEST_KEY'
TEST_BODY = 'TEST_BODY'
TEST_JOB_NAME = 'TEST_JOB_NAME'

TEST_SUBMIT_JOB_RESPONSE_JSON = {
    'ResponseMetadata': {
        'RequestId': '3dd6b227-623f-4749-87cv-c3674d7asdf18',
        'HTTPStatusCode': 200,
        'HTTPHeaders': {
            'date': 'Fri, 30 Nov 2018 01:54:30 GMT',
            'content-type': 'application/json',
            'content-length': '95',
            'connection': 'keep-alive',
            'x-amzn-requestid': '3dd6b227-623f-4749-87cv-c3674d7asdf18',
            'x-amz-apigw-id': 'asdfsdaffsdfsdfsd',
            'x-amzn-trace-id': 'Root=1-asdfasdfasdfsdfsdcsdcsdcsdc;Sampled=0',
        },
        'RetryAttempts': 0,
    },
    'jobName': '2018-11-29T17-54-28_job-name',
    'jobId': '3dd6b227-623f-4749-87cv-c3674d7asdf18',
}

TEST_LOG_EVENT_RESPONSES = [
    dict(
        timestamp=1_543_809_952_329,
        message="You have started up this demo job",
        ingestionTime=1_543_809_957_080,
    ),
    dict(
        timestamp=1_543_809_955_437,
        message="Configuration, we are loading from...",
        ingestionTime=1_543_809_957_080,
    ),
    dict(
        timestamp=1_543_809_955_437,
        message="Defaulting to approximate values",
        ingestionTime=1_543_809_957_080,
    ),
    dict(
        timestamp=1_543_809_955_437,
        message="Setting up logger, nothing to see here",
        ingestionTime=1_543_809_957_080,
    ),
]


@pytest.fixture
def test_bucket():
    with moto.mock_s3():
        boto3.client('s3').create_bucket(Bucket=TEST_BUCKET_NAME)
        yield boto3.resource('s3').Bucket(TEST_BUCKET_NAME)


@pytest.fixture
def test_s3_uri():
    return S3Uri(f's3://{TEST_BUCKET_NAME}/{TEST_KEY_NAME}')


@pytest.fixture
def test_job_definition(test_s3_uri, test_bucket):
    class DemoJobDefinition(JobDefinition):
        def __init__(self, s3_uri: S3Uri):
            self.s3_uri = s3_uri

        @property
        def name(self) -> str:
            return TEST_JOB_NAME

        def validate(self) -> None:
            if not self.s3_uri.object_exists():
                raise S3ObjectNotFoundError(f'S3 object does not exist: {self.s3_uri}')

    return DemoJobDefinition(test_s3_uri)


def test_aws_batch_batch_job(test_bucket, test_job_definition, test_s3_uri):
    with pytest.raises(S3ObjectNotFoundError):
        BatchJob(test_job_definition)
    test_bucket.put_object(Key=TEST_KEY_NAME, Body=TEST_BODY)

    job = BatchJob(test_job_definition)

    assert job.definition.s3_uri.object_exists()
    assert job.job_id is None
    assert job.queue is None
    assert job.container_overrides == {}
    assert not job.is_submitted()

    with pytest.raises(BatchJobSubmissionError):
        job.status()
    with pytest.raises(BatchJobSubmissionError):
        job.log_stream_name()
    with pytest.raises(BatchJobSubmissionError):
        job.log_stream_events()
    with pytest.raises(BatchJobSubmissionError):
        assert not job.is_running()
    with pytest.raises(BatchJobSubmissionError):
        assert not job.is_runnable()

    assert repr(job)


def test_aws_batch_job_definition_validate(test_bucket, test_job_definition, test_s3_uri):
    with pytest.raises(S3ObjectNotFoundError):
        test_job_definition.validate()
    test_bucket.put_object(Key=TEST_KEY_NAME, Body=TEST_BODY)
    assert test_job_definition.validate() is None


def test_aws_batch_job_definition_default_values(test_job_definition):
    assert test_job_definition.name == TEST_JOB_NAME
    assert test_job_definition.parameters == ('s3_uri',)
    assert test_job_definition.revision == '0'
    assert str(test_job_definition) == f'{TEST_JOB_NAME}:0'
    assert repr(test_job_definition)  # TODO: Add test here


def test_aws_batch_job_definition_at_revision(test_job_definition):
    assert test_job_definition.revision == '0'
    assert str(test_job_definition) == f'{TEST_JOB_NAME}:0'
    test_job_definition.at_revision('6')
    assert test_job_definition.revision == '6'
    assert str(test_job_definition) == f'{TEST_JOB_NAME}:6'


def test_aws_batch_job_definition_make_name(test_job_definition):
    moment = datetime.now()
    formatted_date = format_ISO8601(moment)
    assert test_job_definition.make_job_name() == formatted_date + '_' + test_job_definition.name


def test_aws_batch_job_definition_to_dict(test_job_definition, test_s3_uri):
    actual = test_job_definition.to_dict()
    expected = dict(s3_uri=str(test_s3_uri))
    assert actual == expected


@moto.mock_logs
def test_aws_logs_log_util():
    AwsLogUtil()


def test_aws_logs_event_log():
    record = TEST_LOG_EVENT_RESPONSES[0]
    log = LogEvent(record)
    assert log.ingestion_time == record['ingestionTime']
    assert log.message == record['message']
    assert log.timestamp == record['timestamp']
    assert (
        repr(log)
        == 'LogEvent(timestamp=1543809952329, message=\'You have started up this demo job\', ingestion_time=1543809957080)'
    )


def test_aws_response_submit_job_response():
    response = SubmitJobResponse(TEST_SUBMIT_JOB_RESPONSE_JSON)
    assert response.http_code() == 200
    assert response.is_ok()
    assert response.job_name == '2018-11-29T17-54-28_job-name'
    assert response.job_id == '3dd6b227-623f-4749-87cv-c3674d7asdf18'


def test_aws_response_submit_job_empty_response():
    response = SubmitJobResponse({})
    assert response.http_code() == 500
    assert not response.is_ok()
    assert response.job_name is None
    assert response.job_id is None


def test_aws_s3_s3uri_object_exists(test_bucket):
    assert not S3Uri(f's3://{TEST_BUCKET_NAME}/{TEST_KEY_NAME}').object_exists()
    test_bucket.put_object(Key=TEST_KEY_NAME, Body=TEST_BODY)
    assert S3Uri(f's3://{TEST_BUCKET_NAME}/{TEST_KEY_NAME}').object_exists()


def test_aws_s3_s3uri_bad_pattern():
    with pytest.raises(AssertionError):
        S3Uri(f' s3://{TEST_BUCKET_NAME}/')
    with pytest.raises(AssertionError):
        S3Uri(f'h3://{TEST_BUCKET_NAME}/')
    with pytest.raises(AssertionError):
        S3Uri(f's3:/{TEST_BUCKET_NAME}/')


def test_aws_s3_s3uri_add_to_path():
    base = S3Uri(f's3://{TEST_BUCKET_NAME}/')
    assert f's3://{TEST_BUCKET_NAME}/{TEST_KEY_NAME}' == str(base + TEST_KEY_NAME)
    assert f's3://{TEST_BUCKET_NAME}/{TEST_KEY_NAME}' == str(base.add_suffix(TEST_KEY_NAME))
    with pytest.raises(TypeError):
        S3Uri(f's3://{TEST_BUCKET_NAME}') + 2
    with pytest.raises(TypeError):
        S3Uri(f's3://{TEST_BUCKET_NAME}').add_suffix(2)


def test_aws_s3_s3uri_fancy_division():
    assert f's3://{TEST_BUCKET_NAME}/{TEST_KEY_NAME}' == str(
        S3Uri(f's3://{TEST_BUCKET_NAME}/') / TEST_KEY_NAME
    )
    assert f's3://{TEST_BUCKET_NAME}/{TEST_KEY_NAME}' == str(
        S3Uri(f's3://{TEST_BUCKET_NAME}') / TEST_KEY_NAME
    )
    assert f's3://{TEST_BUCKET_NAME}/{TEST_KEY_NAME}' == str(
        S3Uri(f's3://{TEST_BUCKET_NAME}/') // TEST_KEY_NAME
    )
    assert f's3://{TEST_BUCKET_NAME}/{TEST_KEY_NAME}' == str(
        S3Uri(f's3://{TEST_BUCKET_NAME}') // TEST_KEY_NAME
    )

    with pytest.raises(TypeError):
        S3Uri(f's3://{TEST_BUCKET_NAME}') / 2
    with pytest.raises(TypeError):
        S3Uri(f's3://{TEST_BUCKET_NAME}') // 2
    with pytest.raises(TypeError):
        S3Uri(f's3://{TEST_BUCKET_NAME}') / 2.0
    with pytest.raises(TypeError):
        S3Uri(f's3://{TEST_BUCKET_NAME}') // 2.0


def test_aws_s3_s3uri_scheme():
    assert 's3://' == S3Uri(f's3://{TEST_BUCKET_NAME}/').scheme


def test_aws_s3_s3uri_bucket():
    assert TEST_BUCKET_NAME == S3Uri(f's3://{TEST_BUCKET_NAME}/').bucket


def test_aws_s3_s3uri_key():
    assert TEST_KEY_NAME == S3Uri(f's3://{TEST_BUCKET_NAME}/{TEST_KEY_NAME}').key
    assert '' == S3Uri(f's3://{TEST_BUCKET_NAME}/{TEST_KEY_NAME}/').key
    assert '' == S3Uri(f's3://{TEST_BUCKET_NAME}').key
    assert '' == S3Uri(f's3://{TEST_BUCKET_NAME}/').key
    assert '' == S3Uri(f's3://').key


def test_aws_s3_s3uri_str():
    base = S3Uri(f's3://{TEST_BUCKET_NAME}/{TEST_KEY_NAME}')
    assert f's3://{TEST_BUCKET_NAME}/{TEST_KEY_NAME}' == str(base)
    assert f'S3Uri(\'s3://{TEST_BUCKET_NAME}/{TEST_KEY_NAME}\')' == repr(base)


def test_aws_s3_s3api_head_object(test_bucket):
    with pytest.raises(RuntimeError):
        s3api_head_object(TEST_BUCKET_NAME, TEST_KEY_NAME)
    test_bucket.put_object(Key=TEST_KEY_NAME, Body=TEST_BODY)
    metadata = s3api_head_object(TEST_BUCKET_NAME, TEST_KEY_NAME)
    assert metadata['ContentLength'] == 9


def test_aws_s3_s3api_object_exists(test_bucket):
    assert not s3api_object_exists(TEST_BUCKET_NAME, TEST_KEY_NAME)
    test_bucket.put_object(Key=TEST_KEY_NAME, Body=TEST_BODY)
    assert s3api_object_exists(TEST_BUCKET_NAME, TEST_KEY_NAME)


def test_aws_s3_s3_object_exists(test_bucket):
    assert not s3_object_exists(TEST_BUCKET_NAME, TEST_KEY_NAME)
    test_bucket.put_object(Key=TEST_KEY_NAME, Body=TEST_BODY)
    assert s3_object_exists(TEST_BUCKET_NAME, TEST_KEY_NAME)
