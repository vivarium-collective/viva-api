from dataclasses import dataclass

from viva_api.common.models import JobId, JobStatus


@dataclass
class JobStatusInfo:
    """Backend-agnostic job status information."""

    job_id: JobId
    status: JobStatus
    start_time: str | None = None
    end_time: str | None = None
    exit_code: str | None = None
    error_message: str | None = None


#: Where an ``error_message`` came from, best first. A later write with a LOWER
#: rank never overwrites an earlier one with a higher rank: the traceback a task
#: wrote (``failure_record``) or its ``.command.err`` tail (``command_err``) must
#: not be replaced by Kubernetes' generic "Job has reached the specified backoff
#: limit" (``k8s_condition``) on the next poll (observability plan D4c).
ERROR_SOURCE_RANK: dict[str, int] = {
    "failure_record": 4,
    "command_err": 3,
    "batch_status_reason": 2,
    "nextflow_trace": 2,
    "k8s_condition": 1,
    "slurm": 1,
    "local": 1,
}


def error_source_rank(source: str | None) -> int:
    """The precedence of an ``error_source`` label; unknown/None ranks lowest."""
    return ERROR_SOURCE_RANK.get(source or "", 0)


@dataclass
class JobStatusUpdate:
    """Backend-agnostic data for updating an HpcRun record's status.

    ``error_source`` labels where ``error_message`` came from (see
    :data:`ERROR_SOURCE_RANK`); ``attempt`` is how many times the work was tried
    (a Nextflow task's retry count, a Batch job's attempts) when known.
    """

    job_id: JobId
    status: JobStatus
    start_time: str | None = None
    end_time: str | None = None
    exit_code: str | None = None
    error_message: str | None = None
    error_source: str | None = None
    attempt: int | None = None
