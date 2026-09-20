"""
/tasks: the in-region task-run verb (viva-api#631 slice 1) -- submit a
self-contained repo-path script through the SAME Ray/Batch container path
ParCa and the analysis DAG node already use, and poll its status.
Submit-then-poll rather than a long synchronous request, matching this
repo's own EUTE convention for anything that can outlast a gateway's idle
timeout (see CLAUDE.md "Pitfall 6").
"""

import json
import logging

from fastapi import Body, File, Form, HTTPException, Query, UploadFile
from fastapi import Path as FastAPIPath

from viva_api.common.gateway.utils import get_router_config
from viva_api.config import ComputeBackend
from viva_api.dependencies import get_database_service, get_simulation_service_for_backend
from viva_api.simulation.database_service import DatabaseService
from viva_api.simulation.models import TaskDTO, TaskLogsDTO, TaskRunRequest
from viva_api.simulation.simulation_service_ray import SimulationServiceRay

logger = logging.getLogger(__name__)
config = get_router_config(prefix="api", version_major=False)

# Cap the uploaded script so an unauthenticated, network-reachable endpoint can't
# buffer an arbitrarily large body into the api pod (tight node memory budget).
# A task script is a small text file; a few MB is already generous.
_MAX_TASK_SCRIPT_BYTES = 5 * 1024 * 1024


def _require_ray_service() -> SimulationServiceRay:
    """The task verbs always run through the Ray/Batch container path -- ask for
    it by name rather than trusting the deployment's COMPUTE_BACKEND default.
    Same reasoning as run_new_gene_cache/run_variant_cache."""
    simulation_service = get_simulation_service_for_backend(ComputeBackend.RAY)
    if simulation_service is None:
        raise HTTPException(status_code=404, detail="Simulation service is not initialized")
    if not isinstance(simulation_service, SimulationServiceRay):
        raise HTTPException(status_code=501, detail="task runs require the Ray/Batch simulation service")
    return simulation_service


def _require_database_service() -> DatabaseService:
    database_service = get_database_service()
    if database_service is None:
        raise HTTPException(status_code=500, detail="Database service is not initialized")
    return database_service


@config.router.post(
    path="/tasks",
    response_model=TaskDTO,
    operation_id="run-task",
    tags=["Tasks"],
    summary="Submit a self-contained repo-path script to the in-region task compute (viva-api#631)",
)
async def run_task(request: TaskRunRequest = Body(...)) -> TaskDTO:
    simulation_service = _require_ray_service()
    database_service = _require_database_service()
    try:
        return await simulation_service.tasks.submit_task(request, database_service)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error submitting task run")
        raise HTTPException(status_code=500, detail=str(e)) from e


@config.router.post(
    path="/tasks/upload",
    response_model=TaskDTO,
    operation_id="run-uploaded-task",
    tags=["Tasks"],
    summary="Upload a script and run it on the in-region task compute (viva-api#631)",
)
async def run_uploaded_task(
    script: UploadFile = File(description="The script file to run."),
    args: list[str] = Form(default=[], description="Positional args passed to the script."),
    sim_data_refs: str | None = Form(default=None, description="JSON object of caller-defined sim-data references."),
    memory_class: str = Form(default="standard"),
    commit: str | None = Form(default=None, description="Image commit to run in; default latest."),
    name: str | None = Form(default=None, description="Optional human label; defaults to the script name."),
) -> TaskDTO:
    simulation_service = _require_ray_service()
    database_service = _require_database_service()
    filename = script.filename or "task_script.py"
    script_bytes = await script.read()
    if len(script_bytes) > _MAX_TASK_SCRIPT_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"task script is {len(script_bytes)} bytes; the limit is {_MAX_TASK_SCRIPT_BYTES}",
        )
    try:
        refs = json.loads(sim_data_refs) if sim_data_refs else None
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=422, detail=f"sim_data_refs must be a JSON object: {e}") from e
    request = TaskRunRequest(
        script=filename, args=args, sim_data_refs=refs, memory_class=memory_class, commit=commit, name=name
    )
    try:
        return await simulation_service.tasks.submit_uploaded_task(
            request, script_bytes=script_bytes, filename=filename, database_service=database_service
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error submitting uploaded task run")
        raise HTTPException(status_code=500, detail=str(e)) from e


@config.router.get(
    path="/tasks/{task_id}/status",
    response_model=TaskDTO,
    operation_id="get-task-status",
    tags=["Tasks"],
    summary="Poll a task run's status (viva-api#631)",
)
async def get_task_status(
    task_id: int = FastAPIPath(description="Database ID of a submitted task."),
) -> TaskDTO:
    simulation_service = _require_ray_service()
    database_service = _require_database_service()
    try:
        return await simulation_service.tasks.get_task_status(task_id, database_service)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error getting task status for %s", task_id)
        raise HTTPException(status_code=500, detail=str(e)) from e


@config.router.get(
    path="/tasks/{task_id}/logs",
    response_model=TaskLogsDTO,
    operation_id="get-task-logs",
    tags=["Tasks"],
    summary="Read a task run's CloudWatch logs (viva-api#631)",
)
async def get_task_logs(
    task_id: int = FastAPIPath(description="Database ID of a submitted task."),
    limit: int = Query(default=1000, ge=1, le=10000, description="Max recent log events to return."),
) -> TaskLogsDTO:
    simulation_service = _require_ray_service()
    database_service = _require_database_service()
    try:
        return await simulation_service.tasks.get_task_logs(task_id, database_service, limit=limit)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error getting task logs for %s", task_id)
        raise HTTPException(status_code=500, detail=str(e)) from e
