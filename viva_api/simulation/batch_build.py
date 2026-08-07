"""Shared helpers for orchestrating DooD image-build jobs on AWS Batch.

Both backends build their workload image the same way — submit a Docker-outside-of-
Docker job to the build queue + `build_job_definition`, then poll to completion:
  - SimulationServiceK8s → vecoli:{commit} (the Nextflow/Batch task + submit images)
  - SimulationServiceRay → v2ecoli:<sha>  (the self-contained Ray-on-Batch image)
Keeping the submit/poll here avoids duplicating the boto3 plumbing per backend.
"""

import asyncio
import logging

import boto3

from viva_api.config import get_settings

logger = logging.getLogger(__name__)


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
