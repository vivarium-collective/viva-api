"""Shared helpers for orchestrating DooD image-build jobs on AWS Batch.

Both backends build their workload image the same way — submit a Docker-outside-of-
Docker job to the build queue + `build_job_definition`, then poll to completion:
  - SimulationServiceK8s → vecoli:{commit} (the Nextflow/Batch task + submit images)
  - SimulationServiceRay → v2ecoli:<sha>  (the self-contained Ray-on-Batch image)
Keeping the submit/poll here avoids duplicating the boto3 plumbing per backend.
"""

import asyncio
import logging
from dataclasses import dataclass
from typing import Any

import boto3

from viva_api.config import get_settings

logger = logging.getLogger(__name__)

# describe_jobs accepts at most 100 job ids per call (the real API limit).
_DESCRIBE_JOBS_MAX_BATCH = 100


def ray_build_job_name(commit: str) -> str:
    """The deterministic Batch job name of the v2ecoli Ray image build for ``commit``.

    Deterministic on purpose (viva-api#414): a build row that predates
    ``hpcrun.external_job_ids`` can still be resolved after the pod that owned
    its poller is gone, by looking this name up (``find_batch_job_ids_by_name``).
    """
    return f"v2ecoli-ray-build-{commit}"


def k8s_build_job_names(commit: str) -> dict[str, str]:
    """The deterministic Batch job names of the vEcoli (K8s/Nextflow backend)
    multi-arch build for ``commit``, keyed by architecture -- see
    ``ray_build_job_name`` for why they are deterministic."""
    return {"arm64": f"build-arm64-{commit}", "amd64": f"build-amd64-{commit}"}


@dataclass(frozen=True)
class BatchJobState:
    """One Batch job's raw state as ``describe_jobs`` reports it."""

    job_id: str
    job_name: str
    status: str  # raw Batch state: SUBMITTED/PENDING/RUNNABLE/STARTING/RUNNING/SUCCEEDED/FAILED
    status_reason: str | None = None
    created_at_ms: int | None = None
    stopped_at_ms: int | None = None


def _job_state(job: dict[str, Any]) -> BatchJobState:
    return BatchJobState(
        job_id=str(job.get("jobId", "")),
        job_name=str(job.get("jobName", "")),
        status=str(job.get("status", "")),
        status_reason=job.get("statusReason"),
        created_at_ms=job.get("createdAt"),
        stopped_at_ms=job.get("stoppedAt"),
    )


async def describe_batch_jobs(job_ids: list[str]) -> dict[str, BatchJobState]:
    """Batched ``describe_jobs`` for arbitrary Batch job ids, chunked at the
    API's 100-id ceiling. An id Batch does not report (eventual-consistency
    lag right after submission, or a terminal job aged out of Batch's own
    retention) is simply absent from the result rather than an error --
    callers decide what a missing id means for them.
    """
    if not job_ids:
        return {}
    settings = get_settings()
    batch = boto3.client("batch", region_name=settings.batch_region)
    states: dict[str, BatchJobState] = {}
    for i in range(0, len(job_ids), _DESCRIBE_JOBS_MAX_BATCH):
        chunk = job_ids[i : i + _DESCRIBE_JOBS_MAX_BATCH]
        response = batch.describe_jobs(jobs=chunk)
        for job in response.get("jobs", []):
            state = _job_state(job)
            if state.job_id:
                states[state.job_id] = state
    return states


async def find_batch_job_ids_by_name(queue: str, job_name: str, *, created_after_ms: int | None = None) -> list[str]:
    """Look up Batch job ids by their exact job name on ``queue`` (the
    ``JOB_NAME`` list_jobs filter, case-insensitive, paginated), newest first.

    The recovery path for a LOCAL build row with no persisted
    ``external_job_ids`` (viva-api#414): the build's job name is
    deterministic in the commit, so the work is findable even though the row
    never recorded it. ``created_after_ms`` (epoch ms) drops older builds of
    the same commit that a rebuild would otherwise be confused with.
    """
    settings = get_settings()
    batch = boto3.client("batch", region_name=settings.batch_region)
    kwargs: dict[str, Any] = {"jobQueue": queue, "filters": [{"name": "JOB_NAME", "values": [job_name]}]}
    found: list[tuple[int, str]] = []
    while True:
        response = batch.list_jobs(**kwargs)
        for job in response.get("jobSummaryList", []):
            if str(job.get("jobName", "")).lower() != job_name.lower():
                continue  # the filter is a prefix match when the value ends in '*'; be exact here
            created = int(job.get("createdAt") or 0)
            if created_after_ms is not None and created < created_after_ms:
                continue
            found.append((created, str(job["jobId"])))
        next_token = response.get("nextToken")
        if not next_token:
            break
        kwargs["nextToken"] = next_token
    found.sort(reverse=True)
    return [jid for _, jid in found]


async def submit_batch_build(
    job_name: str,
    queue: str,
    command: list[str],
    environment: list[dict[str, str]] | None = None,
) -> str:
    """Submit a DooD build job to AWS Batch; return the Batch job ID.

    The command is the build recipe (run inside the `docker:cli` DooD container); the
    job definition (`build_job_definition`) mounts the host Docker socket.
    """
    settings = get_settings()
    batch = boto3.client("batch", region_name=settings.batch_region)
    response = batch.submit_job(
        jobName=job_name,
        jobQueue=queue,
        jobDefinition=settings.build_job_definition,
        containerOverrides={"command": command, "environment": environment or []},
    )
    batch_job_id = str(response["jobId"])
    logger.info(f"Submitted Batch build job {job_name} (id={batch_job_id}) to queue {queue}")
    return batch_job_id


async def poll_batch_jobs(job_ids: list[str], interval_seconds: float = 15.0) -> None:
    """Poll Batch jobs until all SUCCEEDED. Raises RuntimeError on any FAILED."""
    settings = get_settings()
    batch = boto3.client("batch", region_name=settings.batch_region)

    while True:
        response = batch.describe_jobs(jobs=job_ids)
        statuses = {j["jobId"]: j["status"] for j in response["jobs"]}

        failed = [jid for jid, s in statuses.items() if s == "FAILED"]
        if failed:
            reasons = [
                f"{job['jobName']}: {job.get('statusReason', 'unknown')}"
                for job in response["jobs"]
                if job["jobId"] in failed
            ]
            raise RuntimeError(f"Batch build job(s) failed: {'; '.join(reasons)}")

        if statuses and all(s == "SUCCEEDED" for s in statuses.values()):
            logger.info(f"All {len(job_ids)} Batch build jobs completed successfully")
            return

        await asyncio.sleep(interval_seconds)
