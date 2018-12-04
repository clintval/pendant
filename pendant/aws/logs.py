from typing import List, Mapping

import boto3

__all__ = ['AwsLogUtil', 'LogEvent']


class LogEvent(object):
    """A AWS Cloudwatch log event.

    Args:
        record: A dictionary of log metadata.

    """

    def __init__(self, record: Mapping) -> None:
        self.timestamp = record.get('timestamp')
        self.message = record.get('message')
        self.ingestion_time = record.get('ingestionTime')

    def __repr__(self) -> str:
        return (
            f'{self.__class__.__qualname__}('
            f'timestamp={repr(self.timestamp)}, '
            f'message={repr(self.message)}, '
            f'ingestion_time={repr(self.ingestion_time)})'
        )


class AwsLogUtil(object):
    """AWS Cloudwatch cloud utility functions."""

    def __init__(self) -> None:
        self.client = boto3.client('logs')

    def get_log_events(self, group_name: str, stream_name: str) -> List[LogEvent]:
        """Get all log events from a stream within a group."""
        response = self.client.get_log_events(logGroupName=group_name, logStreamName=stream_name)
        events = [LogEvent(record) for record in response['events']]
        return events
