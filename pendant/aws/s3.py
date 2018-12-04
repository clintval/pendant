import re
from ast import literal_eval
from typing import Dict, Union

import boto3

import botocore

from pendant import aws

__all__ = ['S3Uri', 's3api_head_object', 's3api_object_exists', 's3_object_exists']


class S3Uri(object):
    """An S3 URI which conforms to RFC 3986 formatting.

    Args:
        path: The S3 URI path.

    Examples:
        >>> uri = S3Uri('s3://mybucket/prefix')
        >>> uri.scheme
        's3://'
        >>> uri.bucket
        'mybucket'
        >>> uri / 'myobject'
        S3Uri('s3://mybucket/prefix/myobject')

    """

    delimiter = r'/'

    _pattern_validate = re.compile(r'^s3://.*')
    _pattern_scheme = re.compile(r'^(s3://).*')
    _pattern_key = re.compile(r'^s3://[^/]+/([^/]*)$')
    _pattern_bucket = re.compile(r'^s3://([^/]*)')

    def __init__(self, path: Union[str, 'S3Uri']) -> None:
        path = str(path)
        assert self._pattern_validate.match(path)
        self.path = path

    def __add__(self, other: str) -> 'S3Uri':
        """Add a suffix to this S3 URI."""
        if not isinstance(other, str):
            return NotImplemented
        return S3Uri(self.path + other)

    def __floordiv__(self, other: str) -> 'S3Uri':
        """Join this URI with another part using the `/` operator."""
        if not isinstance(other, str):
            return NotImplemented
        if self.path.endswith(self.delimiter):
            return S3Uri(self.path + other)
        else:
            return S3Uri(self.delimiter.join([self.path, other]))

    def __truediv__(self, other: str) -> 'S3Uri':
        """Join this URI with another part using the `/` operator."""
        if not isinstance(other, str):
            return NotImplemented
        if self.path.endswith(self.delimiter):
            return S3Uri(self.path + other)
        else:
            return S3Uri(self.delimiter.join([self.path, other]))

    @property
    def scheme(self) -> str:
        """Return the RFC 3986 scheme of this URI.

        Example:
            >>> uri = S3Uri('s3://mybucket/myobject')
            >>> uri.scheme
            's3://'

        """
        return 's3://'

    @property
    def bucket(self) -> str:
        """Return the S3 bucket of this URI.

        Example:
            >>> uri = S3Uri('s3://mybucket/myobject')
            >>> uri.bucket
            'mybucket'

        """
        search = self._pattern_bucket.search(self.path)
        return search.groups()[0] if search else ''

    @property
    def key(self) -> str:
        """Return the S3 key of this URI.

        Example:
            >>> uri = S3Uri('s3://mybucket/myobject')
            >>> uri.key
            'myobject'

        """
        search = self._pattern_key.search(self.path)
        return search.groups()[0] if search else ''

    def add_suffix(self, suffix: str) -> 'S3Uri':
        """Add a suffix to this S3 URI.

        Args:
            suffix: Append this suffix to the URI.

        Examples:
            >>> uri = S3Uri('s3://mybucket/myobject.bam')
            >>> uri.add_suffix('.bai')
            S3Uri('s3://mybucket/myobject.bam.bai')

            This is equivalent to:

            >>> S3Uri('s3://mybucket/myobject.bam') + '.bai'
            S3Uri('s3://mybucket/myobject.bam.bai')

        """
        return self + suffix

    def object_exists(self) -> bool:
        """Test if this URI references an object that exists."""
        return s3_object_exists(self.bucket, self.key)

    def __str__(self) -> str:
        return self.path

    def __repr__(self) -> str:
        return f'{self.__class__.__qualname__}({repr(self.path)})'


def s3api_head_object(bucket: str, key: str, profile: str = 'default') -> Dict:
    """Use the :class:`awscli` to make a GET request on an S3 object's metadata.

    Args:
        bucket: The S3 bucket name.
        key: The S3 object key.
        profile: The AWS profile to use, defaults to `"default"`.

    Return:
        A dictionary of object metadata, if the object exists.

    """
    stdout: str = aws.cli(f'--profile {profile} s3api head-object --bucket {bucket} --key {key}')
    metadata: Dict = literal_eval(stdout)
    return metadata


def s3api_object_exists(bucket: str, key: str, profile: str = 'default') -> bool:
    """Use the :class:`awscli` to test if an S3 object exists.

    Args:
        bucket: The S3 bucket name.
        key: The S3 object key.
        profile: The AWS profile to use, defaults to `"default"`.

    """
    try:
        s3api_head_object(bucket, key, profile)
        return True
    except RuntimeError:
        return False


def s3_object_exists(bucket: str, key: str) -> bool:
    """Use :class:`boto3.S3.Object <S3.Object>` to test if an S3 object exists.

    Args:
        bucket: The S3 bucket name.
        key: The S3 object key.

    """
    try:
        boto3.resource('s3').Object(bucket, key).load()
        return True
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
        else:
            raise e
    else:
        return True
