from typing import Mapping, Optional

__all__ = ['AwsResponse', 'SubmitJobResponse']


class AwsResponse(object):
    """A generic HTTP response from AWS."""

    pass


class SubmitJobResponse(AwsResponse):
    """A Batch submit-job response."""

    def __init__(self, response: Mapping) -> None:
        self._response: Mapping = response
        self.metadata: Mapping = response.get('ResponseMetadata', {})
        self.job_name: Optional[str] = response.get('jobName', None)
        self.job_id: Optional[str] = response.get('jobId', None)

    def is_ok(self) -> bool:
        """Return if response was successful."""
        return self.http_code() == 200

    def http_code(self) -> int:
        """Return the HTTP status code of this response."""
        http_code: int = self.metadata.get('HTTPStatusCode', 500)
        return http_code
