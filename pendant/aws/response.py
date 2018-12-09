from typing import Mapping


__all__ = ['AwsResponse']


class AwsResponse(object):
    """A generic HTTP response from AWS."""

    def __init__(self, response: Mapping) -> None:
        self.response = response
        self.metadata: Mapping = response.get('ResponseMetadata', {})

    def is_ok(self) -> bool:
        """Return if response was successful."""
        return self.http_code() == 200

    def http_code(self) -> int:
        """Return the HTTP status code of this response."""
        http_code: int = self.metadata.get('HTTPStatusCode', 500)
        return http_code
