# reticle

[![Testing Status](https://travis-ci.org/clintval/reticle.svg?branch=master)](https://travis-ci.org/clintval/reticle)
[![codecov](https://codecov.io/gh/clintval/reticle/branch/master/graph/badge.svg)](https://codecov.io/gh/clintval/reticle)
[![Documentation Build Status](https://readthedocs.org/projects/reticle/badge/?version=latest)](https://reticle.readthedocs.io/en/latest/?badge=latest)
[![PyPi Release](https://badge.fury.io/py/reticle.svg)](https://badge.fury.io/py/reticle)
[![Python Versions](https://img.shields.io/pypi/pyversions/reticle.svg)](https://pypi.python.org/pypi/reticle/)
[![MyPy Checked](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)

Analysis and plotting library for base substitution spectra and signatures.

```bash
❯ pip install reticle
```

Features:

- Submit Batch jobs

Read the documentation at: [reticle.readthedocs.io](https://reticle.readthedocs.io/en/latest/)


## End-to-end Example

The principle object for deploying jobs to AWS Batch is the Batch job definition.
Every Batch job definition has a name, parameters, and some form of optional parameter validation.

```python
from reticle.aws.batch import JobDefinition
from reticle.aws.s3 import S3Uri

class DemoJobDefinition(JobDefinition):
    """A Batch job definition for demonstrating our API.

    Args:
        input_object: The S3 URI to the input object.

    """
    def __init__(self, input_object: S3Uri):
        self.input_object = input_object

    @property
    def name(self) -> str:
        """Return the job definition name."""
        return 'demo-job'

    def validate(self) -> None:
        """Validate this parameterized job definition."""
        if not self.input_object.object_exists():
            raise f'S3 object does not exist: {self.input_object}'
```

We can now wrap the parameterized job definition in a Batch job and set a specific revision.

```python
from reticle.aws.batch import BatchJob

definition = DemoJobDefinition(input_object='s3://bucket/object')
definition.at_revision('6')

job = BatchJob(definition)
```

Submitting this Batch job is easy, and introspection can be performed immediately:

```python
response = job.submit(queue='prod')
```

When the job is in a `RUNNING` state we can access the job's Cloudwatch logs:

```python
for log_event in job.log_stream_events():
    print(log_event)
"""
LogEvent(timestamp="1543809952329", message="You have started up this demo job", ingestion_time="1543809957080")
LogEvent(timestamp="1543809955437", message="Configuration, we are loading from...", ingestion_time="1543809957080")
LogEvent(timestamp="1543809955437", message="Defaulting to approximate values", ingestion_time="1543809957080")
LogEvent(timestamp="1543809955437", message="Setting up logger, nothing to see here", ingestion_time="1543809957080")
"""
```

And if we must, we can cancel the job as long as we provide a reason:

```python
job.terminate(reason='I was just testing!')
```