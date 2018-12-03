from typing import Mapping

__all__ = ['AwsResponse', 'SubmitJobResponse']


class AwsResponse(object):
    """A generic HTTP response from AWS."""

    pass


class SubmitJobResponse(AwsResponse):
    """A Batch submit-job response."""

    def __init__(self, response: Mapping) -> None:
        self._response = response
        if not response:
            return None

        self.metadata = response['ResponseMetadata']
        self.job_name = response['jobName']
        self.job_id = response['jobId']

    def is_ok(self) -> bool:
        """Return if response was successful."""
        return self.http_code() == 200

    def http_code(self) -> int:
        """Return the HTTP status code of this response."""
        http_code: int = self.metadata['HTTPStatusCode']
        return http_code
