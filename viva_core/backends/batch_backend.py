"""The AWS Batch (container-type job) implementation of
:class:`~viva_core.backends.base.JobBackend` (U2g), over the engine in ``batch.py``.

One task, one container: the job definition is derived per image from a site's base definition
(``ensure_container_job_definition``), the command travels as ``CONTAINER_JOB_CMD`` for the
runtime image's entrypoint, ``env`` as the container's environment, ``labels`` as Batch tags,
``depends_on`` as ``dependsOn``. ``resources`` are the job definition's -- the engine exposes no
per-submission override yet, so a spec that asks for something else is logged and run as
defined. Logs come from CloudWatch: the job's stream (``describe_jobs``) in the definition's
group, read through the ``logs`` client the site hands in.

The engine's calls are boto3's, synchronous; they run in a thread so the Protocol's ``async`` is
honest for a caller that polls many jobs.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING

from viva_core.backends.base import BackendStatus, JobHandle, JobSpec, Resources
from viva_core.backends.batch import BatchJobClient

if TYPE_CHECKING:
    from types_boto3_logs import CloudWatchLogsClient

logger = logging.getLogger(__name__)

KIND = "batch-container"


class BatchContainerJobBackend:
    kind = KIND

    def __init__(
        self,
        engine: BatchJobClient,
        *,
        job_queue: str,
        base_job_definition: str,
        definition_suffix: str = "core",
        report_path: str = "",
        logs_client: Callable[[], CloudWatchLogsClient] | None = None,
    ) -> None:
        self._engine = engine
        self._job_queue = job_queue
        self._base_job_definition = base_job_definition
        self._definition_suffix = definition_suffix
        # Where the entrypoint uploads a report from, if the workload writes one (``CONTAINER_REPORT_PATH``).
        self._report_path = report_path
        self._logs_client = logs_client

    # ── the Protocol ───────────────────────────────────────────────────────

    async def submit(self, spec: JobSpec) -> JobHandle:
        if not spec.image:
            raise ValueError("a Batch job needs an image: there is no host to run a bare command on")
        foreign = [h for h in spec.depends_on if h.backend != self.kind]
        if foreign:
            raise ValueError(f"a Batch job can only wait on Batch jobs; got {foreign}")
        if spec.resources != Resources():
            logger.warning(
                "Batch job %s: resources %s are the job definition's, not the spec's (no per-submission override yet)",
                spec.name,
                spec.resources,
            )

        def _submit() -> str:
            definition = self._engine.ensure_container_job_definition(
                base_definition=self._base_job_definition, image=spec.image, suffix=self._definition_suffix
            )
            return self._engine.submit_container(
                job_name=spec.name,
                job_queue=self._job_queue,
                job_definition=definition,
                job_cmd=spec.command,
                report_path=self._report_path,
                stage_env=[],
                task_env=dict(spec.env) or None,
                depends_on=[h.id for h in spec.depends_on] or None,
                tags=dict(spec.labels) or None,
            )

        job_id = await asyncio.to_thread(_submit)
        return JobHandle(backend=self.kind, id=job_id, name=spec.name)

    async def status(self, handles: Sequence[JobHandle]) -> dict[str, BackendStatus]:
        ids = [h.id for h in handles]
        if not ids:
            return {}
        details = await asyncio.to_thread(self._engine.job_details, ids)
        return {
            job_id: BackendStatus(
                status=detail.status,
                exit_code=detail.exit_code,
                reason=detail.status_reason,
                attempt=detail.attempts or None,
            )
            for job_id, detail in details.items()
        }

    async def cancel(self, handle: JobHandle) -> None:
        await asyncio.to_thread(self._engine.terminate, handle.id, reason="cancelled through viva_core JobBackend")

    async def logs(self, handle: JobHandle, *, tail: int | None = None) -> list[str]:
        logs_client = self._logs_client
        if logs_client is None:
            raise RuntimeError("this Batch backend was handed no CloudWatch Logs client; logs are not readable")

        def _read() -> list[str]:
            job = self._engine.describe_job(handle.id)
            if job is None:
                return []
            stream = job.get("container", {}).get("logStreamName")
            group = self._engine.job_definition_log_group(str(job.get("jobDefinition", "")))
            if not stream or not group:
                return []  # not started yet, or a definition that logs nowhere we can read
            if tail is None:
                events = logs_client().get_log_events(logGroupName=group, logStreamName=stream, startFromHead=True)
            else:
                # the LAST ``tail`` events: from the end, and CloudWatch answers oldest-first within the page
                events = logs_client().get_log_events(
                    logGroupName=group, logStreamName=stream, startFromHead=False, limit=tail
                )
            return [str(event.get("message", "")) for event in events.get("events", [])]

        return await asyncio.to_thread(_read)
