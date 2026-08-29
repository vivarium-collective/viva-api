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

import anyio.to_thread
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from viva_api.compose import env_worker_relay as relay
from viva_api.compose.env_worker_service import (
    EnvWorkerJobExists,
    EnvWorkerLaunchError,
    EnvWorkerService,
)

logger = logging.getLogger(__name__)

router = APIRouter()

_env_worker_service: EnvWorkerService | None = None


def set_env_worker_service(service: EnvWorkerService | None) -> None:
    """Wired at app startup (dependencies.py), like the other routers."""
    global _env_worker_service
    _env_worker_service = service


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
async def stop_relayed_worker(job_name: str) -> dict[str, object]:
    dropped = relay.registry.drop(job_name)
    service = _require_service()
    service.stop(job_name)
    return {"job_name": job_name, "status": "deleted", "was_connected": dropped}
