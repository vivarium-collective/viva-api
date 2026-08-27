"""Env-worker router — run a simulator's own image as a workbench env worker.

Mounted at ``/env-worker/v1/``. Step 2 of vivarium-workbench#942 /
REFACTOR-PLAN §2A.8.

The workbench cannot create Jobs (§2B.2 gives it no cluster access), so it asks
here. It tells us **where to dial back and with what token** — it already knows
its own address, so viva-api discovers nothing and needs no pod-get.
"""

import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

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
