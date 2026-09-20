"""AWS Batch, as a job engine: derive a job definition for an image, submit a job, ask
what happened to it, stop it.

Two job shapes, because Batch has two and neither can stand in for the other:

* **multi-node parallel** (``submit_mnp``) -- N nodes, one node range, a head that runs
  the workload and workers that join it. What a Ray cluster needs.
* **container** (``submit_container``) -- one task, one container.

Neither shape lets a submission override the *image*, so both get a per-image job
definition derived from a base one (``ensure_*_job_definition``): the base carries roles,
resources, shared memory and log configuration, provisioned as infrastructure; the derived
revision swaps only the image.

This module takes **no settings**. Every queue, base job definition and prefix is an
argument, and the boto3 client comes from a factory the caller supplies. That is
deliberate: the engine was carved out of an application service whose tests isolate it by
replacing that service's settings and client, and an engine that looked either up for
itself would run with the real ones while those tests still passed. It also happens to be
the right shape -- the caller decides *where* a job runs, the engine knows *how* to ask
Batch for it.

The container contract the env vars below speak to (``<PREFIX>_OUT_DIR``,
``<PREFIX>_STAGE_S3``, ``RAY_JOB_CMD``, ``CONTAINER_JOB_CMD``, ...) is the image's
entrypoint's: stage inputs in, run one command, sync outputs and a report out.
"""

from __future__ import annotations

import asyncio
import copy
import logging
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from viva_core.models import JobStatus

if TYPE_CHECKING:
    # ``types-boto3`` is a DEV dependency: it exists where mypy runs and not in the image.
    # Everything it provides is used in annotations only, so nothing here may import it at
    # runtime -- ``tests/core/test_typed_boto3.py`` fails if a module does.
    from types_boto3_batch import BatchClient
    from types_boto3_batch.literals import ArrayJobDependencyType, JobStatusType
    from types_boto3_batch.type_defs import (
        JobDependencyTypeDef,
        JobDetailTypeDef,
        KeyValuePairTypeDef,
        ListJobsRequestTypeDef,
        NodeOverridesTypeDef,
        NodePropertyOverrideTypeDef,
        RetryStrategyUnionTypeDef,
        SubmitJobRequestTypeDef,
    )

logger = logging.getLogger(__name__)

# AWS Batch's SubmitJob is capped at 50 TPS per account, fixed -- not adjustable via a
# quota increase. A caller submitting thousands of jobs in a loop must stay under it.
SUBMIT_JOB_SAFE_RATE = 40.0  # jobs/sec; headroom below the 50 TPS account cap for other
#                              concurrent Batch traffic in the same account.
SUBMIT_JOB_MAX_ATTEMPTS = 5  # botocore "standard" retry attempts per submit_job call, for
#                              whatever transient/throttling errors pacing doesn't prevent.
# AWS Batch DescribeJobs accepts at most 100 job ids per call.
DESCRIBE_JOBS_MAX_BATCH = 100

# Every Batch state a job can still be stopped from.
ACTIVE_JOB_STATES: tuple[JobStatusType, ...] = ("SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING")


class SubmitJobPacer:
    """Proactive client-side pacer for AWS Batch SubmitJob.

    Caps outbound ``submit_job`` calls to ``max_per_second``, computed from
    REAL elapsed wall-clock time since the previous call (not a fixed
    per-call sleep, which either over-throttles once call latency is added on
    top, or under-throttles if the guessed interval is even slightly off).
    Deliberately proactive rather than reactive: botocore's own "adaptive"
    retry mode only starts throttling client-side AFTER it has already
    observed a real throttling response, so pacing every call up front is
    what keeps a fresh several-thousand-call burst from front-loading
    avoidable 429s in the first place. This pacer and the "standard" retry
    mode configured on the submitting client are complementary, not
    redundant: this caps the steady-state rate; retry-on-throttle is the
    backstop for whatever pacing alone doesn't prevent (concurrent campaigns,
    other Batch traffic in the same account -- the 50 TPS cap is
    account-wide, not per-caller).
    """

    def __init__(self, max_per_second: float = SUBMIT_JOB_SAFE_RATE) -> None:
        self._min_interval = 1.0 / max_per_second
        self._last_call_at: float | None = None

    async def wait(self) -> None:
        now = time.monotonic()
        if self._last_call_at is not None:
            deficit = self._min_interval - (now - self._last_call_at)
            if deficit > 0:
                await asyncio.sleep(deficit)
                now = time.monotonic()
        self._last_call_at = now


@dataclass(frozen=True)
class BatchJobDetail:
    """What ``describe_jobs`` says about one job, for reporting a failure by
    name and reason rather than by id (``BatchJobClient.job_details``)."""

    job_id: str
    job_name: str
    status: JobStatus
    status_reason: str | None = None
    exit_code: int | None = None
    attempts: int = 0

    def describe(self) -> str:
        parts = [self.job_name or self.job_id, self.status.value]
        if self.exit_code is not None:
            parts.append(f"exit {self.exit_code}")
        if self.status_reason:
            parts.append(self.status_reason)
        return ": ".join(parts[:1]) + " (" + ", ".join(parts[1:]) + ")"


def batch_exit_code(job: JobDetailTypeDef) -> str | None:
    """The container exit code from an AWS Batch ``describe_jobs`` job object,
    as a string (JobStatusInfo.exit_code is ``str | None``), or None when Batch
    has not reported one yet.

    Batch surfaces it at ``job["container"]["exitCode"]`` for a single-container
    job and at ``job["nodeProperties"]...["container"]["exitCode"]`` for a
    multi-node (MNP) job's main node; the top-level ``container`` key carries the
    main container for both shapes in ``describe_jobs`` output, so read it there.
    """
    exit_code = (job.get("container") or {}).get("exitCode")
    return str(exit_code) if exit_code is not None else None


def ecr_image_uri(*, account_id: str, region: str, repository: str, tag: str) -> str:
    """``<account>.dkr.ecr.<region>.amazonaws.com/<repository>:<tag>``."""
    return f"{account_id}.dkr.ecr.{region}.amazonaws.com/{repository}:{tag}"


def env_as_batch_list(env: Mapping[str, str] | None) -> list[KeyValuePairTypeDef]:
    """``{NAME: value}`` -> Batch's ``[{"name": ..., "value": ...}]`` shape."""
    return [{"name": k, "value": v} for k, v in (env or {}).items()]


def stage_out_env(
    *,
    prefix: str,
    out_dir: str,
    out_s3: str,
    stage_s3: str | None = None,
    stage_dir: str | None = None,
    log_s3_prefix: str | None = None,
) -> list[KeyValuePairTypeDef]:
    """The stage-in / output / log env vars of the container contract, for both job
    shapes -- same conditional logic (only emit STAGE_*/LOG_S3_PREFIX when configured), a
    different env-var prefix per shape (``RAY`` / ``CONTAINER``), since each entrypoint
    script only reads its own prefix.

    An application with more to tell its entrypoint appends its own entries to the list.
    """
    env: list[KeyValuePairTypeDef] = [
        {"name": f"{prefix}_OUT_DIR", "value": out_dir},
        {"name": f"{prefix}_OUT_S3", "value": out_s3},
    ]
    if stage_s3 and stage_dir:
        env.append({"name": f"{prefix}_STAGE_S3", "value": stage_s3})
        env.append({"name": f"{prefix}_STAGE_DIR", "value": stage_dir})
    if log_s3_prefix:
        env.append({"name": f"{prefix}_LOG_S3_PREFIX", "value": log_s3_prefix})
    return env


def _depends_on(job_ids: list[str], depends_type: str | None) -> list[JobDependencyTypeDef]:
    # ``depends_type`` arrives as a plain ``str`` from every caller; Batch accepts two values. The
    # cast states that without changing what is sent -- an invalid value is still Batch's to refuse.
    kind = cast("ArrayJobDependencyType | None", depends_type)
    return [({"jobId": jid, "type": kind} if kind else {"jobId": jid}) for jid in job_ids]


class BatchJobClient:
    """The engine. ``client_factory`` returns a boto3 Batch client; it is called per
    operation, never cached, so a caller that swaps its client (a test, a rotated
    credential) is honoured on the next call."""

    def __init__(self, client_factory: Callable[[], BatchClient]) -> None:
        self._client_factory = client_factory

    # ── job definitions ─────────────────────────────────────────────────────

    def ensure_mnp_job_definition(self, *, base_definition: str, image: str, suffix: str) -> str:
        """Return an MNP job definition (name:revision) whose image is ``image``.

        Batch MNP can't override the image per-submission, so -- symmetric with how K8s
        sets the image per-Job -- a per-image job-def revision is derived: describe the
        base job def (roles, resources, shm, log config, node count), swap ONLY every
        node range's container image to ``image``, and register it as
        ``<base>-<suffix>``. An existing active revision already pointing at this image
        is reused, so resubmits don't churn revisions.
        """
        batch = self._client_factory()
        name = f"{base_definition}-{suffix}"

        # Reuse an existing active revision that already targets this exact image.
        existing = batch.describe_job_definitions(jobDefinitionName=name, status="ACTIVE")
        for jd in existing.get("jobDefinitions", []):
            images = {
                nr.get("container", {}).get("image")
                for nr in jd.get("nodeProperties", {}).get("nodeRangeProperties", [])
            }
            if images == {image}:
                return f"{name}:{jd['revision']}"

        # Otherwise clone the base job def's node properties and swap the image.
        base = batch.describe_job_definitions(jobDefinitionName=base_definition, status="ACTIVE")
        base_defs = base.get("jobDefinitions", [])
        if not base_defs:
            raise RuntimeError(f"Base Ray MNP job definition {base_definition!r} not found")
        node_properties = copy.deepcopy(max(base_defs, key=lambda d: d["revision"])["nodeProperties"])
        for nr in node_properties.get("nodeRangeProperties", []):
            nr.setdefault("container", {})["image"] = image

        response = batch.register_job_definition(
            jobDefinitionName=name,
            type="multinode",
            nodeProperties=node_properties,
        )
        logger.info("Registered Ray MNP job def %s:%s for image %s", name, response["revision"], image)
        return f"{name}:{response['revision']}"

    def ensure_container_job_definition(self, *, base_definition: str, image: str, suffix: str) -> str:
        """Return a container job definition (name:revision) whose image is ``image``.

        Mirrors ``ensure_mnp_job_definition`` exactly, for the plain (non-MNP, non-array)
        container job shape: describe the base container job def (roles, resources, retry
        strategy, log config), swap ONLY its image, and register it as
        ``<base>-<suffix>``. An existing active revision already pointing at this image
        is reused, so resubmits don't churn revisions.
        """
        batch = self._client_factory()
        name = f"{base_definition}-{suffix}"

        # Reuse an existing active revision that already targets this exact image.
        existing = batch.describe_job_definitions(jobDefinitionName=name, status="ACTIVE")
        for jd in existing.get("jobDefinitions", []):
            if jd.get("containerProperties", {}).get("image") == image:
                return f"{name}:{jd['revision']}"

        # Otherwise clone the base job def's container properties and swap the image.
        base = batch.describe_job_definitions(jobDefinitionName=base_definition, status="ACTIVE")
        base_defs = base.get("jobDefinitions", [])
        if not base_defs:
            raise RuntimeError(f"Base container job definition {base_definition!r} not found")
        container_properties = copy.deepcopy(max(base_defs, key=lambda d: d["revision"])["containerProperties"])
        container_properties["image"] = image

        response = batch.register_job_definition(
            jobDefinitionName=name,
            type="container",
            containerProperties=container_properties,
        )
        logger.info("Registered container job def %s:%s for image %s", name, response["revision"], image)
        return f"{name}:{response['revision']}"

    # ── submission ──────────────────────────────────────────────────────────

    def submit_mnp(
        self,
        *,
        job_name: str,
        job_queue: str,
        job_definition: str,
        num_nodes: int,
        job_cmd: str,
        report_path: str,
        shared_env: list[KeyValuePairTypeDef],
        task_env: Mapping[str, str] | None = None,
        depends_on: list[str] | None = None,
        depends_type: str | None = "SEQUENTIAL",
        tags: dict[str, str] | None = None,
        retry_strategy: RetryStrategyUnionTypeDef | None = None,
        client: BatchClient | None = None,
    ) -> str:
        """Submit a multi-node parallel job. Returns the AWS Batch job id.

        Env targeting matters: the entrypoint stages inputs and syncs outputs on EVERY
        node, so ``shared_env`` (see ``stage_out_env``) must reach all nodes -- the
        workers need the staged inputs and must ship their own output. Only
        ``RAY_JOB_CMD`` (the driver) and ``RAY_REPORT_PATH`` are head-only.

        ``depends_type`` selects the ``dependsOn`` shape. The default is
        ``{"jobId": ..., "type": "SEQUENTIAL"}``. Pass ``None`` for a plain
        ``{"jobId": ...}`` wait -- required when the DEPENDENCY is an Array job, whose
        parent id AWS Batch will not accept under a SEQUENTIAL type.

        ``retry_strategy``, passed through verbatim as ``SubmitJob.retryStrategy``,
        overrides whatever the job definition itself declares.

        ``client``, when given, is used INSTEAD of the factory's for this one call --
        lets a caller submitting many jobs in a tight loop supply its own
        retry-configured client.
        """
        shared_env = list(shared_env)
        # Ray's own documented safety net (not a bespoke workaround): by default Ray
        # refuses to start its plasma object store when the container's /dev/shm is
        # smaller than the size it wants to request -- observed on a single-node job that
        # died in raylet bootstrap, before any application code ran, requesting ~10.2GB
        # against ~9.66GB available. This flag makes Ray fall back to a disk-backed object
        # store instead of erroring: zero behavioral change on every node where shm is
        # already sufficient, a graceful (slower, not silent) degradation instead of a
        # hard crash on the ones that aren't. Every node runs its own raylet, so this
        # belongs in shared_env, not head-only.
        shared_env.append({"name": "RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE", "value": "1"})
        # task_env: the request's own env, validated at the caller's boundary, reaching
        # EVERY node.
        if task_env:
            shared_env.extend(env_as_batch_list(task_env))
            logger.info("MNP job %s: task_env passthrough %s", job_name, dict(task_env))

        # The head additionally runs the workload (RAY_JOB_CMD) and writes the report.
        # Workers receive these too but never act on them -- the entrypoint branches on
        # AWS_BATCH_JOB_NODE_INDEX and only the head executes RAY_JOB_CMD/writes the report.
        head_env: list[KeyValuePairTypeDef] = [
            {"name": "RAY_JOB_CMD", "value": job_cmd},
            {"name": "RAY_REPORT_PATH", "value": report_path},
            *shared_env,
        ]

        # The base job definition declares a SINGLE node range ("0:") -- the entrypoint
        # self-branches head vs. worker -- so the submit-time override must target that same
        # range. (Splitting into "0:0"/"1:" makes Batch reject: "NodeOverride targets should
        # match job definition".) One override on "0:" with the full env reaches every node;
        # the per-node staging/output knobs in shared_env are what workers need.
        node_property_overrides: list[NodePropertyOverrideTypeDef] = [
            {"targetNodes": "0:", "containerOverrides": {"environment": head_env}},
        ]
        node_overrides: NodeOverridesTypeDef = {
            "numNodes": num_nodes,
            "nodePropertyOverrides": node_property_overrides,
        }
        kwargs: SubmitJobRequestTypeDef = {
            "jobName": job_name,
            "jobQueue": job_queue,
            "jobDefinition": job_definition,
            "nodeOverrides": node_overrides,
        }
        if depends_on:
            kwargs["dependsOn"] = _depends_on(depends_on, depends_type)
        if tags:
            # Cost-allocation tags: propagate to the underlying ECS tasks so the
            # payer account's Cost Explorer can attribute compute per run.
            kwargs["tags"] = tags
            kwargs["propagateTags"] = True
        if retry_strategy:
            kwargs["retryStrategy"] = retry_strategy

        batch = client if client is not None else self._client_factory()
        response = batch.submit_job(**kwargs)
        batch_job_id = str(response["jobId"])
        logger.info(
            "Submitted Ray MNP job %s (id=%s, nodes=%d) to %s",
            job_name,
            batch_job_id,
            num_nodes,
            job_queue,
        )
        return batch_job_id

    def submit_container(
        self,
        *,
        job_name: str,
        job_queue: str,
        job_definition: str,
        job_cmd: str,
        report_path: str,
        stage_env: list[KeyValuePairTypeDef],
        task_env: Mapping[str, str] | None = None,
        depends_on: list[str] | None = None,
        depends_type: str | None = "SEQUENTIAL",
        tags: dict[str, str] | None = None,
        retry_strategy: RetryStrategyUnionTypeDef | None = None,
        client: BatchClient | None = None,
    ) -> str:
        """Submit a plain, standalone container-type job. Returns the AWS Batch job id.

        Sibling of ``submit_mnp`` for the non-MNP, non-array job shape. One task, one
        container: no node overrides, no head/worker split -- every env var goes in a
        single ``containerOverrides.environment`` list, matching the ``CONTAINER_*``
        entrypoint contract. ``depends_on`` / ``tags`` / ``retry_strategy`` / ``client``
        as in ``submit_mnp``.
        """
        env: list[KeyValuePairTypeDef] = [
            {"name": "CONTAINER_JOB_CMD", "value": job_cmd},
            {"name": "CONTAINER_REPORT_PATH", "value": report_path},
            *stage_env,
        ]
        if task_env:
            env.extend(env_as_batch_list(task_env))
            logger.info("Container job %s: task_env passthrough %s", job_name, dict(task_env))

        kwargs: SubmitJobRequestTypeDef = {
            "jobName": job_name,
            "jobQueue": job_queue,
            "jobDefinition": job_definition,
            "containerOverrides": {"environment": env},
        }
        if depends_on:
            kwargs["dependsOn"] = _depends_on(depends_on, depends_type)
        if tags:
            kwargs["tags"] = tags
            kwargs["propagateTags"] = True
        if retry_strategy:
            kwargs["retryStrategy"] = retry_strategy

        batch = client if client is not None else self._client_factory()
        response = batch.submit_job(**kwargs)
        batch_job_id = str(response["jobId"])
        logger.info("Submitted container job %s (id=%s) to %s", job_name, batch_job_id, job_queue)
        return batch_job_id

    # ── status ──────────────────────────────────────────────────────────────

    def describe_job(self, job_id: str) -> JobDetailTypeDef | None:
        """One job's raw ``describe_jobs`` object, or None when Batch does not know it."""
        jobs = self._client_factory().describe_jobs(jobs=[job_id]).get("jobs", [])
        return copy.copy(jobs[0]) if jobs else None  # a shallow copy, as ``dict(...)`` was; the type survives

    def job_statuses(self, job_ids: list[str]) -> dict[str, JobStatus]:
        """Batched ``describe_jobs`` status lookup for arbitrary AWS Batch job ids,
        chunked by ``DESCRIBE_JOBS_MAX_BATCH`` (100/call, the real API limit). An id
        absent from the response (not yet visible -- brief eventual-consistency lag right
        after submission, or simply unknown) is simply absent from the returned mapping
        rather than raising; callers should treat a missing id as not-yet-terminal.
        """
        if not job_ids:
            return {}
        batch = self._client_factory()
        statuses: dict[str, JobStatus] = {}
        for i in range(0, len(job_ids), DESCRIBE_JOBS_MAX_BATCH):
            chunk = job_ids[i : i + DESCRIBE_JOBS_MAX_BATCH]
            response = batch.describe_jobs(jobs=chunk)
            for job in response.get("jobs", []):
                jid = job.get("jobId")
                if jid is not None:
                    statuses[str(jid)] = JobStatus.from_batch_state(str(job.get("status", "")))
        return statuses

    def job_details(self, job_ids: list[str]) -> dict[str, BatchJobDetail]:
        """``job_statuses`` plus what a failed job SAID: Batch's ``statusReason``, the
        container exit code and the attempt count. Used where a bare job id is not an
        answer. Same chunking, same missing-id semantics."""
        if not job_ids:
            return {}
        batch = self._client_factory()
        details: dict[str, BatchJobDetail] = {}
        for i in range(0, len(job_ids), DESCRIBE_JOBS_MAX_BATCH):
            chunk = job_ids[i : i + DESCRIBE_JOBS_MAX_BATCH]
            response = batch.describe_jobs(jobs=chunk)
            for job in response.get("jobs", []):
                jid = job.get("jobId")
                if jid is None:
                    continue
                exit_code = batch_exit_code(job)
                details[str(jid)] = BatchJobDetail(
                    job_id=str(jid),
                    job_name=str(job.get("jobName") or ""),
                    status=JobStatus.from_batch_state(str(job.get("status", ""))),
                    status_reason=job.get("statusReason"),
                    exit_code=int(exit_code) if exit_code is not None else None,
                    attempts=len(job.get("attempts") or []),
                )
        return details

    def job_definition_log_group(self, job_definition: str) -> str | None:
        """The CloudWatch log group a container job definition writes to (the
        ``awslogs-group`` of its logConfiguration), or None when it names none or cannot
        be described."""
        try:
            defs = (
                self._client_factory()
                .describe_job_definitions(jobDefinitions=[job_definition])
                .get("jobDefinitions", [])
            )
            options = defs[0].get("containerProperties", {}).get("logConfiguration", {}).get("options", {})
            group = options.get("awslogs-group")
            return group if isinstance(group, str) else None
        except Exception:
            logger.warning("could not resolve log group from job definition %s", job_definition, exc_info=True)
            return None

    # ── cancellation ────────────────────────────────────────────────────────

    def terminate(self, job_id: str, *, reason: str) -> None:
        """Stop one job, whatever state it is in. Batch accepts this for a job that has
        already finished, so it is safe to call without checking first."""
        self._client_factory().terminate_job(jobId=job_id, reason=reason)

    def terminate_matching(self, *, queues: list[str], matches: Callable[[JobDetailTypeDef], bool], reason: str) -> int:
        """Terminate every still-active job on ``queues`` that ``matches`` accepts, and
        return how many. For jobs the caller did not submit itself and so holds no ids
        for -- children a workflow engine launched on its behalf. ``matches`` receives
        the job's ``describe_jobs`` object."""
        batch = self._client_factory()
        terminated = 0
        for queue in queues:
            for status in ACTIVE_JOB_STATES:
                kwargs: ListJobsRequestTypeDef = {"jobQueue": queue, "jobStatus": status}
                while True:
                    response = batch.list_jobs(**kwargs)
                    ids = [j["jobId"] for j in response.get("jobSummaryList", [])]
                    for chunk in (ids[i : i + 100] for i in range(0, len(ids), 100)):
                        for job in batch.describe_jobs(jobs=chunk).get("jobs", []):
                            if not matches(job):
                                continue
                            batch.terminate_job(jobId=job["jobId"], reason=reason)
                            terminated += 1
                    next_token = response.get("nextToken")
                    if not next_token:
                        break
                    kwargs["nextToken"] = next_token
        return terminated
