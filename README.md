# pendant

[![Testing Status](https://travis-ci.org/clintval/pendant.svg?branch=master)](https://travis-ci.org/clintval/pendant)
[![codecov](https://codecov.io/gh/clintval/pendant/branch/master/graph/badge.svg)](https://codecov.io/gh/clintval/pendant)
[![Documentation Build Status](https://readthedocs.org/projects/pendant/badge/?version=latest)](https://pendant.readthedocs.io/en/latest/?badge=latest)
[![PyPi Release](https://badge.fury.io/py/pendant.svg)](https://badge.fury.io/py/pendant)
[![Python Versions](https://img.shields.io/pypi/pyversions/pendant.svg)](https://pypi.python.org/pypi/pendant/)
[![MyPy Checked](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)

Python 3.6+ library for submitting to AWS Batch interactively.

```bash
❯ pip install pendant
```

Features:

- Submit Batch jobs

Read the documentation at: [pendant.readthedocs.io](https://pendant.readthedocs.io/en/latest/)


## End-to-end Example

The principle object for deploying jobs to AWS Batch is the Batch job definition.
Every Batch job definition has a name, parameters, and some form of optional parameter validation.

```python
>>> from pendant.aws.batch import BatchJob, JobDefinition
>>> from pendant.aws.s3 import S3Uri
>>> from pendant.aws.exception import S3ObjectNotFoundError

>>> class DemoJobDefinition(JobDefinition):
...     def __init__(self, input_object: S3Uri) -> None:
...         self.input_object = input_object
... 
...     @property
...     def name(self) -> str:
...         return 'demo-job'
... 
...     def validate(self) -> None:
...         if not self.input_object.object_exists():
...             raise S3ObjectNotFoundError(f'S3 object does not exist: {self.input_object}')
```

Let's instantiate the definition at a specific revision and validate it.

```python
>>> definition = DemoJobDefinition(input_object=S3Uri('s3://bucket/object')).at_revision('6')
>>> definition.validate()
None
```

Validation is also performed when a job definition is wrapped by a `BatchJob` so the call to `.validate()` above was redundant.
Wrapping a job definition into a Batch job is achieved with the following, but no useful work will happen until the job is submitted.

```python
>>> job = BatchJob(definition)
```

Now we are ready to submit this job to AWS Batch!
Submitting this Batch job is easy, and introspection can be performed immediately:

```python
>>> response = job.submit(queue='prod')
>>> job.is_submitted()
True
```

When the job is in a `RUNNING` state we can access the job's Cloudwatch logs.
The log events are returned as objects which have useful properties such as `timestamp` and `message`.

```python
>>> for log_event in job.log_stream_events():
...     print(log_event)
LogEvent(timestamp="1543809952329", message="You have started up this demo job", ingestion_time="1543809957080")
LogEvent(timestamp="1543809955437", message="Configuration, we are loading from...", ingestion_time="1543809957080")
LogEvent(timestamp="1543809955437", message="Defaulting to approximate values", ingestion_time="1543809957080")
LogEvent(timestamp="1543809955437", message="Setting up logger, nothing to see here", ingestion_time="1543809957080")
```

And if we must, we can cancel the job as long as we provide a reason:

```python
>>> response = job.terminate(reason='I was just testing!')
```