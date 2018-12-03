__all__ = [
    'BatchJobNotFoundError',
    'BatchJobSubmissionError',
    'LogStreamNotFoundError',
    'S3ObjectNotFoundError',
]


class BatchJobNotFoundError(Exception):
    """A Batch job not found error."""

    pass


class BatchJobSubmissionError(Exception):
    """A Batch job submission error."""

    pass


class LogStreamNotFoundError(Exception):
    """A log stream not found error."""

    pass


class S3ObjectNotFoundError(FileNotFoundError):
    """A file not found error for objects on S3."""

    pass
