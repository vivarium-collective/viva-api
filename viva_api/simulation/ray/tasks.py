"""Tasks: run one script in the simulator image as a standalone Batch container job
(viva-api#631), follow its status, read its logs.

``RayTaskService`` is a SERVICE, not a mixin of ``SimulationServiceRay`` (it was one for two
PRs, P2.1 cut 3; ``docs/plan-core.md`` decision log, 2026-09-20). It is handed what it
needs -- a ``TaskDispatch`` -- instead of inheriting a class to find it.

``TaskDispatch`` is the point of this file. It is the whole of what a task asks of whatever
runs container jobs for it: resolve an image, get a job definition for it, submit one
container job, ask what happened to it, find its log group. Today the thing that answers
is ``SimulationServiceRay`` (through its Batch layer), which is why the member names are
the private ones that class already has. In the target architecture tasks are a core
service (``viva_core/tasks``, plan P4b) and the same seven questions are answered by an
``EnvironmentRef`` and ``BatchJobClient``; the other thing still SMS here is the ``task``
table behind ``DatabaseService``, which P4b replaces with a ``JobStore``.
"""

import json
import logging
import re
import shlex
from pathlib import Path
from typing import Any, Protocol

from viva_api.common.models import JobStatus
from viva_api.simulation.database_service import DatabaseService
from viva_api.simulation.models import TaskDTO, TaskLogsDTO, TaskRunRequest
from viva_api.simulation.ray import _seams
from viva_api.simulation.ray.batch_layer import _rand_suffix
from viva_api.simulation.ray.image_paths import TASK_OUT_DIR, TASK_STAGE_DIR
from viva_api.simulation.tables_orm import TaskStatusDB

logger = logging.getLogger(__name__)


def _safe_task_name(raw: str) -> str:
    """Reduce a task name to the Batch jobName charset ([A-Za-z0-9_-], <=128) so
    it can't fail submit_job. A user-provided name already passed
    TaskRunRequest's validator; this also sanitizes the auto-derived script stem
    (which can carry dots and other characters)."""
    cleaned = re.sub(r"[^A-Za-z0-9_-]", "-", raw)[:128].strip("-")
    return cleaned or "task"


class TaskDispatch(Protocol):
    """What a task needs from whatever dispatches container jobs for it. Only the keyword
    arguments tasks actually pass are declared; the implementation may accept more."""

    async def get_latest_commit_hash(self) -> str: ...
    def _image_uri(self, commit: str) -> str: ...
    def _ensure_container_job_def(self, image: str, commit: str) -> str: ...
    def _results_s3_uri(self, experiment_id: str) -> str: ...
    def _submit_container(
        self,
        *,
        job_name: str,
        job_definition: str,
        job_cmd: str,
        out_s3: str,
        out_dir: str,
        stage_s3: str | None = ...,
        stage_dir: str | None = ...,
        task_env: dict[str, str] | None = ...,
        memory_class: str = ...,
    ) -> str: ...
    def get_batch_job_statuses(self, job_ids: list[str]) -> dict[str, JobStatus]: ...
    def _batch(self) -> Any: ...
    def _resolve_log_group(self, job_definition: str | None) -> str | None: ...


class RayTaskService:
    def __init__(self, dispatch: TaskDispatch) -> None:
        # Held, not copied: every call below looks the method up on ``dispatch`` when it
        # runs, so a test that swaps ``service._submit_container`` is what a task gets.
        self._dispatch = dispatch

    async def submit_task(self, request: TaskRunRequest, database_service: DatabaseService) -> TaskDTO:
        """Submit a self-contained repo-path script as a standalone AWS Batch
        container job (viva-api#631 slice 1).

        Same one-node container shape ``submit_parca_job``/the analysis DAG
        node already use (``_ensure_container_job_def`` + ``_submit_container``)
        — this method's only real job is building the ``python <script>
        <args...>`` command line and recording the result on the ``task``
        table (mirroring how ``_submit_analysis_job`` records to ``analysis``)
        so ``GET /tasks/{id}/status`` has a row to poll.

        ``request.commit`` pins the image commit the script runs in; ``None``
        resolves to the default repo/branch's latest commit, the same
        fallback every other Ray dispatch path here already uses.

        ``request.sim_data_refs``, when present, rides in as a single JSON env
        var (``TASK_SIM_DATA_REFS``) rather than individual vars — the shape
        is caller-defined (script-specific reference names -> URIs), so there
        is no fixed set of env-var names to emit.
        """
        commit = request.commit or await self._dispatch.get_latest_commit_hash()
        task_name = _safe_task_name(request.name or Path(request.script).stem)
        job_cmd = self._task_job_cmd(request.script, request.args)
        return await self._dispatch_task(
            task_name=task_name,
            script_label=request.script,
            job_cmd=job_cmd,
            request=request,
            commit=commit,
            database_service=database_service,
        )

    async def submit_uploaded_task(
        self,
        request: TaskRunRequest,
        *,
        script_bytes: bytes,
        filename: str,
        database_service: DatabaseService,
    ) -> TaskDTO:
        """Submit an UPLOADED script (viva-api#631 slice 2).

        The script bytes are staged to an S3 prefix; the container entrypoint's
        existing input-staging (``CONTAINER_STAGE_S3`` -> ``CONTAINER_STAGE_DIR``,
        an ``aws s3 sync``) pulls it into the container before the command runs,
        so the job_cmd is ``python <TASK_STAGE_DIR>/<script>``. No entrypoint
        change is needed -- this reuses the same stage-in path ParCa's cache
        staging already uses.
        """
        commit = request.commit or await self._dispatch.get_latest_commit_hash()
        safe_name = Path(filename).name  # never trust an uploaded path
        if not safe_name:
            raise ValueError("uploaded task script has no filename")
        task_name = _safe_task_name(request.name or Path(safe_name).stem)
        stage_s3 = self._dispatch._results_s3_uri(f"tasks/scripts/{task_name}-{_rand_suffix()}").rstrip("/")
        self._upload_task_script(stage_s3, safe_name, script_bytes)
        job_cmd = self._task_job_cmd(f"{TASK_STAGE_DIR}/{safe_name}", request.args)
        return await self._dispatch_task(
            task_name=task_name,
            script_label=f"{stage_s3}/{safe_name}",
            job_cmd=job_cmd,
            request=request,
            commit=commit,
            database_service=database_service,
            stage_s3=stage_s3,
            stage_dir=TASK_STAGE_DIR,
        )

    @staticmethod
    def _task_job_cmd(script: str, args: list[str]) -> str:
        cmd = "python " + shlex.quote(script)
        if args:
            cmd += " " + " ".join(shlex.quote(a) for a in args)
        return cmd

    def _upload_task_script(self, stage_s3_prefix: str, filename: str, script_bytes: bytes) -> None:
        """Put the uploaded script under ``stage_s3_prefix`` so the container's
        input staging syncs it in. Uses the instance/task S3 credentials, same as
        every other S3 write on this service."""
        from urllib.parse import urlparse

        parsed = urlparse(stage_s3_prefix)
        bucket = parsed.netloc
        key = f"{parsed.path.strip('/')}/{filename}"
        _seams.boto3.client("s3", region_name=_seams.get_settings().storage_s3_region).put_object(
            Bucket=bucket, Key=key, Body=script_bytes
        )

    async def _dispatch_task(
        self,
        *,
        task_name: str,
        script_label: str,
        job_cmd: str,
        request: TaskRunRequest,
        commit: str,
        database_service: DatabaseService,
        stage_s3: str | None = None,
        stage_dir: str | None = None,
    ) -> TaskDTO:
        """Shared submit path for repo-path and uploaded tasks: one-node container
        job (``_ensure_container_job_def`` + ``_submit_container``) recorded on the
        ``task`` table so ``GET /tasks/{id}/status`` has a row to poll."""
        job_def = self._dispatch._ensure_container_job_def(self._dispatch._image_uri(commit), commit)
        out_uri = self._dispatch._results_s3_uri(f"tasks/{task_name}-{_rand_suffix()}").rstrip("/")
        task_env: dict[str, str] | None = None
        if request.sim_data_refs:
            task_env = {"TASK_SIM_DATA_REFS": json.dumps(request.sim_data_refs)}
        batch_job_id = self._dispatch._submit_container(
            job_name=f"task-{task_name}-{_rand_suffix()}"[:128],
            job_definition=job_def,
            job_cmd=job_cmd,
            out_s3=out_uri,
            out_dir=TASK_OUT_DIR,
            stage_s3=stage_s3,
            stage_dir=stage_dir,
            task_env=task_env,
            memory_class=request.memory_class,
        )
        return await database_service.record_task(
            name=task_name,
            script=script_label,
            args=list(request.args),
            sim_data_refs=request.sim_data_refs,
            memory_class=request.memory_class,
            status=TaskStatusDB.COMPUTING,
            job_id_ext=str(batch_job_id),
            out_uri=out_uri,
        )

    async def get_task_status(self, task_id: int, database_service: DatabaseService) -> TaskDTO:
        """Poll a task's tracked Batch job (if any) and persist its mapped status.

        A task with no ``job_id_ext`` yet (shouldn't happen post-``submit_task``,
        but mirrors the defensive style elsewhere in this class) is returned
        as-is rather than raising -- there is nothing to poll.
        """
        task = await database_service.get_task(task_id)
        if task.job_id_ext is None:
            return task
        statuses = self._dispatch.get_batch_job_statuses([task.job_id_ext])
        batch_status = statuses.get(task.job_id_ext)
        if batch_status is None:
            return task
        return await database_service.update_task_status(task_id, TaskStatusDB.from_job_status(batch_status))

    async def get_task_logs(self, task_id: int, database_service: DatabaseService, *, limit: int = 1000) -> TaskLogsDTO:
        """The CloudWatch logs for a task's Batch job (viva-api#631 slice 3).

        Resolves the job's log stream (``describe_jobs`` -> container.logStreamName)
        and log group, then reads up to ``limit`` recent events. Returns an empty
        ``lines`` (not an error) when the container hasn't started yet (no stream)
        or no group can be resolved, so a caller can poll until logs appear."""
        task = await database_service.get_task(task_id)
        result = TaskLogsDTO(task_id=task_id, job_id_ext=task.job_id_ext, status=task.status)
        log_prefix = _seams.get_settings().ray_log_s3_prefix
        if log_prefix and task.job_id_ext:
            result.report_uri = f"{log_prefix.rstrip('/')}/{task.job_id_ext}/report.json"
        if not task.job_id_ext:
            return result
        jobs = self._dispatch._batch().describe_jobs(jobs=[task.job_id_ext]).get("jobs", [])
        if not jobs:
            return result
        job = jobs[0]
        stream = job.get("container", {}).get("logStreamName")
        if not stream:
            return result  # container not started yet
        group = self._dispatch._resolve_log_group(job.get("jobDefinition"))
        if not group:
            return result
        result.log_stream = stream
        try:
            logs_client = _seams.boto3.client("logs", region_name=_seams.get_settings().storage_s3_region)
            events = logs_client.get_log_events(
                logGroupName=group, logStreamName=stream, startFromHead=True, limit=limit
            ).get("events", [])
            result.lines = [str(e.get("message", "")) for e in events]
        except Exception:
            logger.warning("could not read CloudWatch logs for task %s (stream %s)", task_id, stream, exc_info=True)
        return result
