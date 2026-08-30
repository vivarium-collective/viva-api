"""Env-worker router — run a simulator's own image as a workbench env worker.

Mounted at ``/env-worker/v1/``. Step 2 of vivarium-workbench#942 /
REFACTOR-PLAN §2A.8.

The workbench cannot create Jobs (§2B.2 gives it no cluster access), so it asks
here. It tells us **where to dial back and with what token** — it already knows
its own address, so viva-api discovers nothing and needs no pod-get.
"""

import functools
import logging
import os
import secrets
from typing import TYPE_CHECKING

import anyio.to_thread
from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, Field

from viva_api.api.auth import require_caller, resolve_caller
from viva_api.compose import env_worker_relay as relay
from viva_api.compose.env_worker_service import (
    EnvWorkerJobExists,
    EnvWorkerLaunchError,
    EnvWorkerService,
)
from viva_api.compose.models import ComposeJobStatus, EnvWorkerTask

if TYPE_CHECKING:
    from viva_api.compose.database_service import EnvWorkerTaskDatabaseService

logger = logging.getLogger(__name__)

router = APIRouter()

_env_worker_service: EnvWorkerService | None = None


def set_env_worker_service(service: EnvWorkerService | None) -> None:
    """Wired at app startup (dependencies.py), like the other routers."""
    global _env_worker_service
    _env_worker_service = service


_task_db: "EnvWorkerTaskDatabaseService | None" = None


def set_env_worker_task_service(db: "EnvWorkerTaskDatabaseService | None") -> None:
    """Wired at app startup (dependencies.py), like set_env_worker_service above.

    A module global rather than a FastAPI dependency because that is this
    codebase's convention for subsystem services -- see set_compose_services in
    routers/compose.py -- and a second convention would only make the wiring
    harder to find.
    """
    global _task_db
    _task_db = db


def _require_service() -> EnvWorkerService:
    if _env_worker_service is None:
        raise HTTPException(503, "env-worker service is not configured on this deployment")
    return _env_worker_service


class EnvWorkerStartRequest(BaseModel):
    """Where to dial back, and which environment to run.

    ``commit`` selects the environment: it is the tag of the prebuilt simulator
    image, which under §2A.8 *is* the execution environment rather than a recipe
    for rebuilding one.
    """

    commit: str = Field(..., description="Simulator commit; the prebuilt image tag to run")
    callback_host: str = Field(..., description="Host/IP the worker dials back to (the workbench pod IP)")
    callback_port: int = Field(..., ge=1, le=65535, description="Port the workbench is listening on")
    token: str = Field(..., description="One-time handshake token the worker must present")
    workspace: str | None = Field(
        None,
        description="Workspace path inside the worker container; defaults to the "
        "deployment's env_worker_workspace_path (the image's own checkout)",
    )
    session_key: str | None = Field(None, description="Owning session; makes the Job name unique per session")


class EnvWorkerStartResponse(BaseModel):
    job_name: str
    image: str
    namespace: str


class EnvWorkerStatusResponse(BaseModel):
    job_name: str
    status: str | None = None
    exists: bool = True
    logs: str | None = None


@router.post(
    path="/workers",
    operation_id="start-env-worker",
    response_model=EnvWorkerStartResponse,
    tags=["Env Worker"],
    summary="Run a simulator image as an env worker that dials back to the caller",
)
async def start_worker(request: EnvWorkerStartRequest) -> EnvWorkerStartResponse:
    service = _require_service()
    try:
        handle = service.start(
            commit=request.commit,
            callback_host=request.callback_host,
            callback_port=request.callback_port,
            token=request.token,
            workspace=request.workspace,
            session_key=request.session_key,
        )
    except EnvWorkerJobExists as e:
        # A knowable Kubernetes condition, not a server fault: a Job of this name
        # exists or is still terminating.
        raise HTTPException(409, str(e)) from e
    except EnvWorkerLaunchError as e:
        # A malformed launch is the caller's error and is worth saying precisely
        # — these end up in a pod spec, so vagueness here costs a round trip
        # through Kubernetes to find out what was wrong.
        raise HTTPException(422, str(e)) from e
    return EnvWorkerStartResponse(job_name=handle.job_name, image=handle.image, namespace=handle.namespace)


@router.get(
    path="/workers/{job_name}",
    operation_id="get-env-worker-status",
    response_model=EnvWorkerStatusResponse,
    tags=["Env Worker"],
    summary="Status of an env worker, with logs when it has failed",
)
async def get_worker(job_name: str, include_logs: bool = False) -> EnvWorkerStatusResponse:
    service = _require_service()
    info = service.status(job_name)
    if info is None:
        # Not an error: a worker whose Job has been reaped is a normal end state,
        # and the caller needs to distinguish that from "still starting".
        return EnvWorkerStatusResponse(job_name=job_name, exists=False)
    logs = service.logs(job_name) if include_logs else None
    return EnvWorkerStatusResponse(job_name=job_name, status=str(getattr(info, "status", None)), logs=logs)


@router.delete(
    path="/workers/{job_name}",
    operation_id="stop-env-worker",
    tags=["Env Worker"],
    summary="Delete an env worker Job (idempotent)",
)
async def stop_worker(job_name: str) -> dict[str, str]:
    service = _require_service()
    service.stop(job_name)
    return {"job_name": job_name, "status": "deleted"}


# --------------------------------------------------------------------------- #
# Relay (plan §C/§C1) — viva-api holds the worker socket
#
# The endpoints above serve the IN-CLUSTER shape: the workbench tells us where
# to dial back, because it can be dialled. A laptop cannot — its SSM tunnel is
# laptop-initiated with no inbound path — so for that case viva-api itself is
# the rendezvous: it binds the listener, starts the Job pointing at ITSELF,
# holds the socket, and forwards calls over HTTP.
#
# Both shapes coexist deliberately. The workbench selects between them with an
# env var, so a deployment can be switched either way without a cross-repo
# release window in which env workers are broken.
# --------------------------------------------------------------------------- #


class RelayStartRequest(BaseModel):
    """Start a worker that dials back to *viva-api* rather than to the caller."""

    commit: str = Field(..., description="Simulator commit; the prebuilt image tag to run")
    workspace: str | None = Field(None, description="Workspace path inside the worker container")
    session_key: str | None = Field(None, description="Owning session; makes the Job name unique per session")
    accept_timeout: float = Field(
        300.0, gt=0, le=1800, description="Seconds to wait for the worker to dial back (pod schedule + image pull)"
    )


class RelayStartResponse(BaseModel):
    job_name: str
    image: str
    namespace: str
    connected: bool


class RelayCallRequest(BaseModel):
    method: str = Field(..., description="Worker method name (JSON-RPC)")
    params: dict[str, object] | None = Field(None, description="Method params")
    timeout: float = Field(300.0, gt=0, le=3600, description="Seconds to wait for this call's reply")


class RelayCallResponse(BaseModel):
    result: object | None = None


def _relay_advertise_host() -> str:
    """The address a worker pod should dial to reach THIS viva-api pod.

    Supplied by the Downward API (``status.podIP``), the same mechanism the
    workbench uses for its own dial-back host. Absent, the relay is off — and
    that is reported as 503 rather than guessed, because a wrong host here
    fails as a Job that starts, dials nowhere, and times out three minutes
    later with nothing to point at.
    """
    host = (os.environ.get("ENV_WORKER_RELAY_ADVERTISE_HOST") or "").strip()
    if not host:
        raise HTTPException(
            503,
            "relay is not enabled on this deployment: ENV_WORKER_RELAY_ADVERTISE_HOST "
            "is unset (set it from the Downward API status.podIP)",
        )
    return host


@router.post(
    path="/relay/workers",
    operation_id="start-relayed-env-worker",
    response_model=RelayStartResponse,
    tags=["Env Worker"],
    summary="Start an env worker that dials back to viva-api, and hold its connection",
)
async def start_relayed_worker(request: RelayStartRequest) -> RelayStartResponse:
    service = _require_service()
    host = _relay_advertise_host()
    listener = relay.DialBackListener()
    try:
        handle = service.start(
            commit=request.commit,
            callback_host=host,
            callback_port=listener.port,
            token=listener.token,
            workspace=request.workspace,
            session_key=request.session_key,
        )
    except EnvWorkerJobExists as e:
        listener.close_listener()
        raise HTTPException(409, str(e)) from e
    except EnvWorkerLaunchError as e:
        listener.close_listener()
        raise HTTPException(422, str(e)) from e
    except Exception:
        listener.close_listener()
        raise

    # accept() blocks for pod scheduling + image pull. Run it off the event loop
    # so one starting worker does not stall every other request on this process.
    try:
        sock = await anyio.to_thread.run_sync(functools.partial(listener.accept, request.accept_timeout))
    except relay.DialBackError as e:
        # The Job exists but never reached us. Delete it rather than leaving a
        # pod dialling a port nobody is listening on any more.
        listener.close_listener()
        service.stop(handle.job_name)
        raise HTTPException(504, f"worker did not dial back: {e}") from e

    relay.registry.register(relay.WorkerConnection(job_name=handle.job_name, sock=sock))
    return RelayStartResponse(job_name=handle.job_name, image=handle.image, namespace=handle.namespace, connected=True)


@router.post(
    path="/relay/workers/{job_name}/call",
    operation_id="call-relayed-env-worker",
    response_model=RelayCallResponse,
    tags=["Env Worker"],
    summary="Forward one JSON-RPC call to a relayed env worker",
)
async def call_relayed_worker(job_name: str, request: RelayCallRequest) -> RelayCallResponse:
    """One request, one reply — the worker protocol is already request/response.

    Runs on a worker thread: the call holds a per-worker mutex for its whole
    duration (the worker's FIFO contract), and blocking the event loop on that
    would stall every unrelated request in this process.
    """
    try:
        conn = relay.registry.get(job_name)
    except relay.WorkerUnavailable as e:
        raise HTTPException(404, str(e)) from e
    try:
        result = await anyio.to_thread.run_sync(
            functools.partial(conn.call, request.method, request.params, timeout=request.timeout)
        )
    except relay.WorkerCallError as e:
        # The worker ran and said no. That is the caller's answer, not a viva-api
        # fault -- 502 would blame the wrong party and hide the worker's message.
        raise HTTPException(422, str(e)) from e
    except relay.WorkerUnavailable as e:
        # The socket is gone or desynced; drop it so the next caller is told to
        # start a new worker instead of inheriting a broken connection.
        relay.registry.drop(job_name)
        raise HTTPException(410, str(e)) from e
    return RelayCallResponse(result=result)


@router.delete(
    path="/relay/workers/{job_name}",
    operation_id="stop-relayed-env-worker",
    tags=["Env Worker"],
    summary="Close a relayed worker's connection and delete its Job (idempotent)",
)
async def stop_relayed_worker(request: Request, job_name: str) -> dict[str, object]:
    """Stop a worker. NOT ownership-checked, deliberately — and its tasks are
    settled with attribution instead.

    Stopping a worker is overwhelmingly an AUTOMATIC operation: the workbench's
    pool calls it on LRU eviction, idle reap, dead-worker replacement and process
    exit. The pool has no identity to present, so an ownership check here would
    either break it or have to let unidentified callers through — which is the
    very bypass such a check would exist to close, and would leave the perverse
    rule that anonymous callers may stop workers while identified ones may not.

    So the worker is shared infrastructure with an automatic lifecycle; the task
    is the unit of work that has an owner. What is owed to someone whose task
    dies with a worker is not a veto but an EXPLANATION: their task moves to a
    terminal state naming what happened and, where identity is configured, who
    did it — rather than hanging in `running` until somebody wonders.
    """
    dropped = relay.registry.drop(job_name)
    settled: list[EnvWorkerTask] = []
    if _task_db is not None:
        by = resolve_caller(request)
        settled = await _task_db.fail_unfinished_tasks(
            f"worker {job_name} was stopped" + (f" by {by}" if by else ""),
            job_name=job_name,
        )
    service = _require_service()
    service.stop(job_name)
    return {
        "job_name": job_name,
        "status": "deleted",
        "was_connected": dropped,
        "tasks_settled": len(settled),
    }


# --------------------------------------------------------------------------- #
# Tasks (plan §E option (e)) — the middle tier
#
# The relay's /call above is synchronous and stays that way: an interactive
# method answers in seconds and a round trip is the cheapest thing that works.
# But `run_study` runs a study's baseline and every variant to completion, and
# no gateway will hold a request that long — the workbench's own attempt to do
# so is what produced the double-run bug. So long work is submitted, recorded,
# and polled.
# --------------------------------------------------------------------------- #


def _require_task_db() -> "EnvWorkerTaskDatabaseService":
    if _task_db is None:
        raise HTTPException(503, "env-worker tasks are not configured on this deployment")
    return _task_db


def _require_runner() -> relay.TaskRunner:
    if relay.runner is None:
        raise HTTPException(503, "env-worker task runner is not configured on this deployment")
    return relay.runner


class TaskSubmitRequest(BaseModel):
    job_name: str = Field(..., description="Relayed worker Job to run this on")
    method: str = Field(..., description="Worker method (JSON-RPC)")
    params: dict[str, object] | None = Field(None, description="Method params")


class TaskResponse(BaseModel):
    task_id: int
    job_name: str
    method: str
    status: str
    result: object | None = None
    error_message: str | None = None
    created_by: str | None = None
    created_at: str | None = None
    started_at: str | None = None
    ended_at: str | None = None


class TaskStatusResponse(BaseModel):
    """A task WITHOUT its result — the shape the batch endpoint returns.

    Measured on dev before this existed: five tasks came back as **1.19 MB**,
    of which 1.185 MB was result payloads. The endpoint exists so a campaign can
    be polled without N round trips; inlining every result defeats exactly that,
    and the cost grows with both the number of tasks and the size of what they
    returned. Polling twenty tasks every few seconds would have moved megabytes
    per cycle over an SSM tunnel.

    So a *status* endpoint returns status. Fetch the singular
    ``GET /tasks/{id}`` for the payload, which is one request at the one moment
    a caller actually wants it.
    """

    task_id: int
    job_name: str
    method: str
    status: str
    error_message: str | None = None
    created_by: str | None = None
    created_at: str | None = None
    started_at: str | None = None
    ended_at: str | None = None
    #: Whether a result is waiting, without shipping it. Lets a poller know it is
    #: worth one GET rather than guessing from `status`.
    has_result: bool = False


def _to_status(task: EnvWorkerTask) -> TaskStatusResponse:
    return TaskStatusResponse(
        task_id=task.database_id,
        job_name=task.job_name,
        method=task.method,
        status=task.status.value,
        error_message=task.error_message,
        created_by=task.created_by,
        created_at=task.created_at,
        started_at=task.started_at,
        ended_at=task.ended_at,
        has_result=task.result is not None,
    )


def _to_response(task: EnvWorkerTask) -> TaskResponse:
    return TaskResponse(
        task_id=task.database_id,
        job_name=task.job_name,
        method=task.method,
        status=task.status.value,
        result=task.result,
        error_message=task.error_message,
        created_by=task.created_by,
        created_at=task.created_at,
        started_at=task.started_at,
        ended_at=task.ended_at,
    )


@router.post(
    path="/tasks",
    operation_id="submit-env-worker-task",
    response_model=TaskResponse,
    status_code=202,
    tags=["Env Worker"],
    summary="Submit a long-running env-worker call; poll for its result",
)
async def submit_task(request: Request, body: TaskSubmitRequest) -> TaskResponse:
    """202 with a task id. The row is written BEFORE this returns.

    That ordering is compose's contract and it matters more here: the client is
    told to poll and holds nothing else, so a status read that 404s because the
    row had not been written yet would be indistinguishable from a lost task.
    """
    db = _require_task_db()
    runner_ = _require_runner()
    # Refuse early on a worker nobody is holding, rather than accepting a task
    # that can only fail: 404 here is actionable, a queued-then-failed task is a
    # round trip and a confusing record.
    try:
        relay.registry.get(body.job_name)
    except relay.WorkerUnavailable as e:
        raise HTTPException(404, str(e)) from e
    task = await db.insert_task(
        job_name=body.job_name,
        method=body.method,
        params=body.params,
        correlation_id=f"env-worker-task-{secrets.token_hex(8)}",
        created_by=resolve_caller(request),
    )
    await runner_.submit(body.job_name, task.database_id)
    return _to_response(task)


@router.get(
    path="/tasks/{task_id}",
    operation_id="get-env-worker-task",
    response_model=TaskResponse,
    tags=["Env Worker"],
    summary="Status and result of one env-worker task",
)
async def get_task(task_id: int) -> TaskResponse:
    task = await _require_task_db().get_task(task_id)
    if task is None:
        raise HTTPException(404, f"no such task: {task_id}")
    return _to_response(task)


@router.get(
    path="/tasks/status/batch",
    operation_id="get-env-worker-tasks-batch",
    response_model=list[TaskStatusResponse],
    tags=["Env Worker"],
    summary="Status for many env-worker tasks in one call (no result payloads)",
)
async def get_tasks_batch(ids: list[int] = Query()) -> list[TaskStatusResponse]:
    """Status only — results are deliberately omitted. See TaskStatusResponse.

    Mirrors compose's /simulations/status/batch, which returns rows carrying no
    large payload. A campaign is many tasks, and polling them one at a time is
    what this endpoint exists to avoid; shipping every result inline reintroduced
    the cost in a different dimension.
    """
    return [_to_status(t) for t in await _require_task_db().get_tasks(ids)]


@router.delete(
    path="/tasks/{task_id}",
    operation_id="cancel-env-worker-task",
    response_model=TaskResponse,
    tags=["Env Worker"],
    summary="Cancel a task you started",
)
async def cancel_task(request: Request, task_id: int) -> TaskResponse:
    """The one authorization rule in this API: you cannot cancel someone else's work.

    It exists to prevent an ACCIDENT — killing a colleague's six-hour study —
    not an adversary, who can set the identity header to anything. See
    viva_api/api/auth.py.

    A task nobody claimed (created_by NULL, which is every task on a deployment
    with no identity proxy) is cancellable by anyone: there is no owner to
    protect, and refusing would make the endpoint useless exactly where identity
    is unavailable.

    Anonymity is refused only HERE, and only because the alternative is a
    decorative rule: if an unidentified caller could cancel anything, omitting
    the header would bypass ownership entirely.
    """
    db = _require_task_db()
    task = await db.get_task(task_id)
    if task is None:
        raise HTTPException(404, f"no such task: {task_id}")
    if task.created_by is not None:
        caller = require_caller(request)
        if caller != task.created_by:
            raise HTTPException(
                403,
                f"task {task_id} was started by {task.created_by}; "
                f"you are {caller}. Cancel it from the client that started it.",
            )
    if task.status in (
        ComposeJobStatus.COMPLETED,
        ComposeJobStatus.FAILED,
        ComposeJobStatus.CANCELLED,
        ComposeJobStatus.TIMEOUT,
    ):
        return _to_response(task)  # already finished; cancelling is a no-op, not an error
    await db.finish_task(
        task_id,
        ComposeJobStatus.CANCELLED,
        error_message=f"cancelled by {resolve_caller(request) or 'an unidentified caller'}",
    )
    settled = await db.get_task(task_id)
    return _to_response(settled or task)
