import inspect
from abc import abstractmethod
from datetime import datetime
from typing import Dict, List, Mapping, Optional, Tuple

import boto3

from custom_inherit import DocInheritMeta

from pendant.aws.exception import BatchJobSubmissionError
from pendant.aws.logs import AwsLogUtil, LogEvent
from pendant.aws.response import SubmitJobResponse
from pendant.util import format_ISO8601

__all__ = ['BatchJob', 'JobDefinition']

CLOUDWATCH_LOG_GROUP = '/aws/batch/job'
BATCH_STATUS_SUBMITTED = 'SUBMITTED'
BATCH_STATUS_PENDING = 'PENDING'
BATCH_STATUS_RUNNABLE = 'RUNNABLE'
BATCH_STATUS_STARTING = 'STARTING'
BATCH_STATUS_RUNNING = 'RUNNING'
BATCH_STATUS_FAILED = 'FAILED'
BATCH_STATUS_NOTFOUND = 'NOTFOUND'


class JobDefinition(
    metaclass=DocInheritMeta(style="google", abstract_base_class=True)  # type: ignore
):
    """A Batch job definition."""

    def __new__(cls, *args: str, **kwargs: str) -> 'JobDefinition':
        """Create a new Batch job definition."""
        this: JobDefinition = super().__new__(cls)
        this._revision = '0'
        return this

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the name of the job definition."""

    @property
    def parameters(self) -> Tuple[str]:
        """Return the parameters of the job definition."""
        return tuple(inspect.signature(self.__init__).parameters.keys())  # type: ignore

    @property
    def revision(self) -> str:
        """Return the revision of the job definition."""
        return self._revision

    @abstractmethod
    def validate(self) -> None:
        """Validate this job definition after initialization."""

    def at_revision(self, revision: str) -> 'JobDefinition':
        """Set this job definition to a specific revision."""
        self._revision = revision
        return self

    def make_job_name(self, moment: Optional[datetime] = None) -> str:
        """Format a Batch job name from this definition."""
        moment = datetime.now() if moment is None else moment
        return format_ISO8601(moment) + '_' + self.name

    def to_dict(self) -> Dict[str, str]:
        """Return a dictionary of all parameters and their values as strings."""
        mapping: Dict[str, str] = {key: str(getattr(self, key)) for key in self.parameters}
        return mapping

    def __str__(self) -> str:
        return f'{self.name}:{self.revision}'

    def __repr__(self) -> str:
        parts = [f'{key}={repr(getattr(self, key))}' for key in self.parameters]
        signature = ', '.join(parts)
        return f'{self.__class__.__qualname__}({signature})'


class BatchJob(object):
    """An AWS Batch job.

    A Batch job can be instantiated and then submitted against the Batch service.
    After submission, the job's status can be queried, the job's logs can be
    read, and other methods can be called to understand the state of the job.

    Args:
        definition: A Batch job definition.

    """

    def __init__(self, definition: JobDefinition):
        definition.validate()
        self.definition = definition
        self._client = boto3.client('batch')

        self._is_submitted: bool = False

        self._container_overrides: Mapping = dict()
        self._job_id: Optional[str] = None
        self._queue: Optional[str] = None
        self._submit_response: Optional[SubmitJobResponse] = None

    @property
    def container_overrides(self) -> Optional[Mapping]:
        """Return container overriding parameters."""
        return self._container_overrides

    @property
    def job_id(self) -> Optional[str]:
        """Return the job ID."""
        return self._job_id

    @property
    def queue(self) -> Optional[str]:
        """Return the job queue."""
        return self._queue

    @staticmethod
    def describe_job(job_id: str) -> Dict:
        """Describe this job."""
        job, *_ = BatchJob.describe_jobs([job_id])
        return job if job else dict()

    @staticmethod
    def describe_jobs(job_ids: List[str]) -> List[Dict]:
        """Describe a Batch job by job ID."""
        jobs: List[Dict] = boto3.client('batch').describe_jobs(jobs=job_ids)['jobs']
        return jobs

    def status(self) -> str:
        """Return the job status."""
        if self.job_id is None:
            raise BatchJobSubmissionError(
                'Cannot check status of a job that has not been submitted.'
            )
        job = BatchJob.describe_job(self.job_id)
        status: str = job.get('status', BATCH_STATUS_NOTFOUND)
        return status

    def cancel(self, reason: str) -> Dict:
        """Cancel this job.

        Args:
            reason: The reason why the job must be canceled.

        Returns:
            The service response to job cancellation.

        """
        assert self.is_submitted(), 'Cannot cancel a job that has not been submitted.'
        response: Dict = self._client.cancel_job(jobId=self.job_id, reason=reason)
        return response

    def terminate(self, reason: str) -> Dict:
        """Terminate this job.

        Jobs that are in the STARTING or RUNNING state are terminated, which
        causes them to transition to FAILED. Jobs that have not progressed to
        the STARTING state are cancelled.

        Args:
            reason: The reason why the job must be terminated.

        Returns:
            The service response to job termination.

        """
        assert self.is_submitted(), 'Cannot terminate a job that has not been submitted.'
        response: Dict = self._client.terminate_job(jobId=self.job_id, reason=reason)
        return response

    def is_running(self) -> bool:
        """Return if this job's state is RUNNING or not."""
        return self.status() == BATCH_STATUS_RUNNING

    def is_runnable(self) -> bool:
        """Return if this job's state is RUNNABLE or not."""
        return self.status() == BATCH_STATUS_RUNNABLE

    def is_submitted(self) -> bool:
        """Return if this job has been submitted to Batch."""
        return self._is_submitted

    def submit(
        self, queue: str, container_overrides: Optional[Mapping] = None
    ) -> SubmitJobResponse:
        """Submit this job to Batch.

        Args:
            queue: The Batch job queue to use.
            container_overrides: The values to override in the spawned container.

        Returns:
            The service response to job submission.

        """
        assert not self.is_submitted(), 'Cannot submit already submitted job!'
        self._queue = queue
        self._container_overrides = container_overrides if container_overrides else {}
        job_name = self.definition.make_job_name()
        response: Mapping = self._client.submit_job(
            jobName=job_name,
            jobQueue=queue,
            jobDefinition=str(self.definition),
            parameters=self.definition.to_dict(),
            containerOverrides=self.container_overrides,
        )
        submit_response = SubmitJobResponse(response)

        if submit_response.is_ok():
            self._is_submitted = True
            self._job_id = submit_response.job_id
            self._submit_response = submit_response
        else:
            raise BatchJobSubmissionError(f'Batch job failed to submit!\n{response}')
        return submit_response

    def log_stream_name(self) -> str:
        """Return the Batch log stream name for this job."""
        if self.job_id is None:
            raise BatchJobSubmissionError(
                'Cannot check status of a job that has not been submitted.'
            )
        job = BatchJob.describe_job(self.job_id)
        log_stream_name: str = job['container']['logStreamName']
        return log_stream_name

    def log_stream_events(self) -> List[LogEvent]:
        """Return all log events for this job.

        Returns:
            events: All log events, to date.

        """
        log_util = AwsLogUtil()
        log_stream_name = self.log_stream_name()
        events = log_util.get_log_events(
            group_name=CLOUDWATCH_LOG_GROUP, stream_name=log_stream_name
        )
        return events

    def __repr__(self) -> str:
        return f'{self.__class__.__qualname__}(' f'definition={repr(self.definition)})'
