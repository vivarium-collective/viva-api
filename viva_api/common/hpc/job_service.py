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


@dataclass
class JobStatusUpdate:
    """Backend-agnostic data for updating an HpcRun record's status."""

    job_id: JobId
    status: JobStatus
    start_time: str | None = None
    end_time: str | None = None
    exit_code: str | None = None
    error_message: str | None = None
