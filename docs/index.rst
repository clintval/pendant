Welcome to the ``pendant`` Documentation!
=========================================

.. code-block:: bash

    ❯ pip install pendant

Features
--------

- Submit Batch jobs

.. toctree::
   :maxdepth: 2

   pendant
   CONTRIBUTING

End-to-end Examples
~~~~~~~~~~~~~~~~~~~

The principle object for deploying jobs to AWS Batch is the Batch job definition.
Every Batch job definition has a name, parameters, and some form of optional parameter validation.

.. code-block:: python

    from pendant.aws.batch import JobDefinition
    from pendant.aws.s3 import S3Uri
    from pendant.aws.exception import S3ObjectNotFoundError

    class DemoJobDefinition(JobDefinition):
        """A Batch job definition for demonstrating our API.

        Args:
            input_object: The S3 URI for the input object.

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
                raise S3ObjectNotFoundError(f'S3 object does not exist: {self.input_object}')

We can now wrap the parameterized job definition in a Batch job and set a specific revision.

.. code-block:: python

    from pendant.aws.batch import BatchJob

    definition = DemoJobDefinition(input_object='s3://bucket/object')
    definition.at_revision('6')

    job = BatchJob(definition)

Submitting this Batch job is easy, and introspection can be performed immediately:

.. code-block:: python

    response = job.submit(queue='prod')

When the job is in a ``RUNNING`` state we can access the job's Cloudwatch logs:

.. code-block:: python

    for log_event in job.log_stream_events():
        print(log_event)
    """
    LogEvent(timestamp="1543809952329", message="You have started up this demo job", ingestion_time="1543809957080")
    LogEvent(timestamp="1543809955437", message="Configuration, we are loading from...", ingestion_time="1543809957080")
    LogEvent(timestamp="1543809955437", message="Defaulting to approximate values", ingestion_time="1543809957080")
    LogEvent(timestamp="1543809955437", message="Setting up logger, nothing to see here", ingestion_time="1543809957080")
    """

And if we must, we can cancel the job as long as we provide a reason:

.. code-block:: python

    job.terminate(reason='I was just testing!')