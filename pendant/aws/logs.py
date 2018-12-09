import time
from typing import Generator, List, Mapping, Optional

import boto3

from pendant.aws.response import AwsResponse
from pendant.util import timeout_after

__all__ = ['AwsLogUtil', 'GetLogEventsResponse', 'LogEvent']

REQUEST_SLEEP_TIME = 1
LOG_END_TIME = int(1e13 - 1)


class LogEvent(object):
    """A AWS Cloudwatch log event.

    Args:
        record: A dictionary of log metadata.

    """

    def __init__(self, record: Mapping) -> None:
        self.timestamp = record['timestamp']
        self.message = record['message']
        self.ingestion_time = record['ingestionTime']

    def __repr__(self) -> str:
        return (
            f'{self.__class__.__qualname__}('
            f'timestamp={repr(self.timestamp)}, '
            f'message={repr(self.message)}, '
            f'ingestion_time={repr(self.ingestion_time)})'
        )


class GetLogEventsResponse(AwsResponse):
    """A Cloudwatch get log events response."""

    def __init__(self, response: Mapping) -> None:
        super().__init__(response)
        self.events: List[LogEvent] = [LogEvent(e) for e in response.get('events', [])]
        self.next_forward_token: Optional[str] = response.get('nextForwardToken', None)
        self.next_backward_token: Optional[str] = response.get('nextBackwardToken', None)


class AwsLogUtil(object):
    """AWS Cloudwatch cloud utility functions."""

    def __init__(self) -> None:
        self.client = boto3.client('logs')

    def get_log_events(
        self,
        group_name: str,
        stream_name: Optional[str],
        start_time: int = 0,
        end_time: int = LOG_END_TIME,
    ) -> List[LogEvent]:
        """Get all log events from a Cloudwatch stream."""
        response = GetLogEventsResponse(
            self.client.get_log_events(
                logGroupName=group_name,
                logStreamName=stream_name,
                startTime=start_time,
                endTime=end_time,
            )
        )
        return response.events

    def yield_log_events(
        self,
        group_name: str,
        stream_name: Optional[str],
        start_time: int = 0,
        timeout: Optional[int] = None,
    ) -> Generator[LogEvent, None, None]:
        """Yield all log events from a Cloudwatch stream."""
        with timeout_after(timeout):
            while True:
                events = self.get_log_events(
                    group_name=group_name, stream_name=stream_name, start_time=start_time
                )

                if events:
                    last_event = events[-1]
                    start_time = last_event.timestamp + 1

                for event in events:
                    yield event

                time.sleep(REQUEST_SLEEP_TIME)
