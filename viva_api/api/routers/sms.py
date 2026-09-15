"""
/analyses: this router is dedicated to the running and output retrieval of
    simulation analysis jobs/workflows
"""

# TODO: do we require simulation/analysis configs that are supersets of the original configs:
#   IE: where do we provide this special config: in vEcoli or API?
# TODO: what does a "configuration endpoint" actually mean (can we configure via the simulation?)
# TODO: labkey preprocessing
import datetime
import json
import logging
from collections.abc import Sequence
from typing import Any

from fastapi import BackgroundTasks, Body, Depends, HTTPException, Query
from fastapi import Path as FastAPIPath
from fastapi.requests import Request
from fastapi.responses import FileResponse, Response, StreamingResponse

from viva_api.analysis.analysis_service import AnalysisServiceSlurm
from viva_api.analysis.models import (
    AnalysisJobFailedException,
    AnalysisRun,
    DatasetListDTO,
    ExperimentAnalysisDTO,
    ExperimentAnalysisRequest,
    OutputFile,
    OutputFileMetadata,
    SimulationAnalysisFigures,
    TsvOutputFile,
)
from viva_api.api import request_examples
from viva_api.common import handlers
from viva_api.common.dispatch_validation import DispatchValidationError
from viva_api.common.gateway.utils import get_router_config
from viva_api.common.models import JobStatus
from viva_api.common.storage import data_layout
from viva_api.config import ComputeBackend, compute_backend_for_repo, get_job_backend, get_settings
from viva_api.dependencies import get_database_service, get_simulation_service
from viva_api.simulation.database_service import DatabaseService
from viva_api.simulation.github_repo import open_repo_tarball_stream
from viva_api.simulation.models import (
    AnalysisOptions,
    ChainProgress,
    CompositeEngine,
    JobType,
    NewGeneCacheJob,
    NewGeneCacheRequest,
    ObservableInfoModel,
    RepoDiscovery,
    Simulation,
    SimulationEvents,
    SimulationObservableIndex,
    SimulationObservables,
    SimulationRun,
    SimulationTask,
    VariantCacheJob,
    VariantCacheRequest,
    VecoliSource,
)
from viva_api.simulation.observable_reader import list_observables_async, read_observables_async
from viva_api.simulation.tables_orm import AnalysisStatusDB


def _validate_simulation_config_filename(simulation_config_filename: str) -> None:
    """Reject ``configs/`` prefix typos that would silently 404 on the server."""
    if simulation_config_filename.startswith("configs/"):
        raise HTTPException(
            status_code=400,
            detail=(
                f"simulation_config_filename {simulation_config_filename!r} starts "
                "with 'configs/'. The server prepends 'configs/' itself; pass the "
                "path relative to the repo's configs/ directory (e.g. "
                "'campaigns/pilot_mixed.json' instead of "
                "'configs/campaigns/pilot_mixed.json')."
            ),
        )


ENV = get_settings()

logger = logging.getLogger(__name__)
config = get_router_config(prefix="api", version_major=False)


def get_experiment_id(simulator_id: int, config_filename: str) -> str:
    return f"sim{simulator_id}-{config_filename.replace('.json', '')}"


async def _ray_seed_store_uri_or_error(db: DatabaseService, sim: Simulation, seed: int) -> str:
    """Resolve the per-seed XArray/zarr store URI for a Ray run, or fail loudly.

    Observables are a v2ecoli/Ray-only concept (an XArray/zarr store; layout owned
    by ``data_layout.ray_seed_store_uri`` and walked by ``observable_reader``). A
    vEcoli/Nextflow run emits parquet under a different layout and has no such
    store, so we return a clear 409 rather than the bare 404 (which reads as
    "results not ready yet" and was the ambiguity flagged in #152).

    ``RayLayout.seed_store_uri``'s flat ``v2ecoli_seed{NN}.zarr`` convention is
    real, but it is only what the single-generation ("phase0") and comparison-
    engine dispatch shapes actually write. A chain-dispatch campaign
    (``HpcRun.chain_final_job_ids is not None``) writes its per-seed lineage
    store under a DIFFERENT naming convention
    (``batch_baseline_runner._lineage_store_path``:
    ``{experiment_id}_v{variant}_s{seed}.zarr``) and is read back through the
    separate DuckDB/parquet analysis pipeline
    (``GET /analyses/{id}/status``), never through this endpoint. A multi-node
    composite dispatch (``HpcRun.multi_node_composite_id is not None``, e.g. a
    colony) has no per-seed zarr store at all -- only an emitter-history/
    final-state snapshot, also read through the analyses endpoint. Checking
    the backend alone (below) does not catch either case, since both are
    still Ray -- without this check, a request for either shape would fall
    through to a `seed_store_uri` that nothing ever writes, and 500 instead
    of naming the real cause (found live, 2026-08-26, alongside the phase0
    write-path bug this docstring's #152 reference already covers)."""
    simulator = await db.get_simulator(sim.simulator_id)
    backend = compute_backend_for_repo(simulator.git_repo_url) if simulator else None
    layout = data_layout.layout_for(backend) if backend is not None else None
    if layout is not data_layout.RayLayout:
        raise HTTPException(
            status_code=409,
            detail=(
                f"Observables are only available for v2ecoli (Ray) runs; simulation "
                f"{sim.database_id} ran on the {backend.value if backend else 'unknown'} backend. "
                f"Use POST /api/v1/simulations/{sim.database_id}/data for its outputs."
            ),
        )
    hpc_run = await db.get_hpcrun_by_ref(ref_id=sim.database_id, job_type=JobType.SIMULATION)
    if hpc_run is not None and hpc_run.chain_final_job_ids is not None:
        raise HTTPException(
            status_code=409,
            detail=(
                f"Simulation {sim.database_id} is a chain-dispatch campaign (multi-seed/"
                f"multi-generation) -- its per-seed lineage store uses a different S3 layout "
                f"this endpoint doesn't read. Its cd1_*/ptools_* analysis output is tracked "
                f"under a SEPARATE id space -- look it up via "
                f"GET /api/v1/simulations/{sim.database_id}/analyses, then poll "
                f"GET /api/v1/analyses/{{analysis_id}}/status with THAT id (not this "
                f"simulation's id)."
            ),
        )
    if hpc_run is not None and hpc_run.multi_node_composite_id is not None:
        raise HTTPException(
            status_code=409,
            detail=(
                f"Simulation {sim.database_id} is a multi-node composite dispatch (e.g. a "
                f"colony) -- it has no per-seed XArray/zarr store. Its analysis-flush output "
                f"is tracked under a SEPARATE id space -- look it up via "
                f"GET /api/v1/simulations/{sim.database_id}/analyses, then poll "
                f"GET /api/v1/analyses/{{analysis_id}}/status with THAT id (not this "
                f"simulation's id)."
            ),
        )
    return data_layout.RayLayout.seed_store_uri(sim.experiment_id, seed)


AnalysisOptions()


@config.router.get(
    path="/simulations/discovery",
    operation_id="discover-simulator-repo-contents",
    response_model=RepoDiscovery,
    tags=["Simulations"],
    summary="Discover available config files and analysis modules for a simulator",
)
async def discover_repo_contents(
    simulator_id: int = Query(..., description="database_id of the simulator to introspect"),
) -> RepoDiscovery:
    """Enumerate config filenames and analysis modules available in the simulator's repo."""
    sim_service = get_simulation_service()
    database_service = get_database_service()
    if sim_service is None or database_service is None:
        raise HTTPException(status_code=500, detail="Services not initialized")
    simulator = await database_service.get_simulator(simulator_id)
    if simulator is None:
        raise HTTPException(status_code=404, detail=f"Simulator {simulator_id} not found")
    return await sim_service.discover_repo_contents(simulator)


@config.router.get(
    path="/simulations/workspace",
    operation_id="export-simulator-workspace",
    tags=["Simulations"],
    summary="Export a simulator's repo@commit workspace as a gzipped tarball",
    response_model=None,
    responses={
        200: {
            "content": {"application/gzip": {}},
            "description": "A gzipped tarball of the simulator's repo at its commit",
        }
    },
)
async def export_simulator_workspace(
    simulator_id: int = Query(..., description="database_id of the simulator to export"),
) -> StreamingResponse:
    """Stream the simulator's repo@commit as a gzipped tarball (GitHub tarball).

    The repo@commit is the dashboard-loadable workspace; streamed so a large
    repo never buffers in memory or on the pod's ephemeral disk.
    """
    database_service = get_database_service()
    if database_service is None:
        raise HTTPException(status_code=500, detail="Services not initialized")
    simulator = await database_service.get_simulator(simulator_id)
    if simulator is None:
        raise HTTPException(status_code=404, detail=f"Simulator {simulator_id} not found")
    filename = f"workspace-sim{simulator_id}-{simulator.git_commit_hash}.tar.gz"
    # Validate the upstream GitHub fetch (await) BEFORE constructing the response,
    # so a 404/401/403/5xx surfaces as a real HTTPException instead of a 200 that
    # truncates mid-stream.
    body = await open_repo_tarball_stream(simulator, get_settings().github_token)
    return StreamingResponse(
        body,
        media_type="application/gzip",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@config.router.get(
    path="/simulations/tags",
    operation_id="list-simulation-tags",
    tags=["Simulations"],
    dependencies=[Depends(get_database_service)],
    summary="List the tags present in the database and their experiment IDs",
)
async def list_simulation_tags() -> dict[str, list[str]]:
    """Return every tag carried by a simulation, mapped to the experiment IDs that carry it.

    Tags are free-form data on each simulation (set at run time or via
    POST /simulations/{id}/tags), so this reflects the actual database contents
    rather than a predefined registry.
    """
    db_service = get_database_service()
    if db_service is None:
        logger.error("Database service is not initialized")
        raise HTTPException(status_code=500, detail="Database service is not initialized")
    return await db_service.list_distinct_tags()


@config.router.post(
    path="/simulations",
    operation_id="run-ecoli-simulation-new",
    response_model=Simulation,
    tags=["Simulations"],
    dependencies=[Depends(get_simulation_service), Depends(get_database_service)],
    summary="[New] Launch a vEcoli simulation workflow (engine/composite, generations, seeds, condition)",
)
async def run_simulation_workflow(
    simulator_id: int = Query(
        ..., description="`database_id` of the simulator object returned by /core/v1/simulator/upload"
    ),
    experiment_id: str | None = Query(default=None, description="Unique experiment identifier"),
    simulation_config_filename: str = Query(
        default="api_simulation_default.json",
        description="Config filename in vEcoli/configs/. Use GET /simulations/discovery to list available files.",
    ),
    num_generations: int | None = Query(default=None, description="Number of generations to simulate"),
    num_seeds: int | None = Query(default=None, description="Number of initial seeds (lineages)"),
    composite: CompositeEngine | None = Query(
        default=None,
        description="Ray two-engine comparison: 'v2ecoli' (ported) or 'vecoli' "
        "(imported via build_composite_native). When set, runs the comparison "
        "ensemble driver instead of the phase0 ensemble.",
    ),
    condition: str | None = Query(
        default=None,
        description="Growth condition/media for the comparison run (e.g. basal, acetate).",
    ),
    max_generations: int | None = Query(
        default=None,
        description="Generations per lineage for the comparison ensemble.",
    ),
    vecoli_source: VecoliSource | None = Query(
        default=None,
        description="How the genuine vEcoli side runs (composite='vecoli' only): "
        "'upstream' (default, ~50 pbg steps) or 'vivarium-process' (vEcoli as one "
        "pbg node with vivarium-core's Engine inside).",
    ),
    description: str | None = Query(default=None, description="Description of the simulation"),
    run_parca: bool | None = Query(
        default=None,
        description="If true, run the simulation parameter calculator prior "
        "to running simulation (re-parameterizes simulation "
        "workflow).",
    ),
    observables: list[str] | None = Query(
        default=None,
        description="Dot-separated vEcoli output paths to observe. "
        "E.g. ['bulk', 'listeners.mass.cell_mass']. "
        "Maps to engine_process_reports in the vEcoli config. "
        "If omitted, all outputs are emitted.",
    ),
    ecoli_sources_uri: str | None = Query(
        default=None,
        description="S3 URI for the ECOLI_SOURCES env var on the simulation container. "
        "Set automatically when ecoli_sources_repo_url is provided, or manually via the CLI's --sources flag.",
    ),
    ecoli_sources_overlays: str | None = Query(
        default=None,
        description="Semicolon-separated overlay manifest URIs for ECOLI_SOURCES_OVERLAYS.",
    ),
    ecoli_sources_repo_url: str | None = Query(
        default=None,
        description="GitHub repo URL for ecoli-sources data. The server downloads and syncs to S3 "
        "automatically, then injects ECOLI_SOURCES on the container. No AWS CLI needed on the client.",
    ),
    ecoli_sources_ref: str | None = Query(
        default=None,
        description="Git ref (branch/tag/commit) for ecoli_sources_repo_url. Defaults to 'main'.",
    ),
    tags: list[str] | None = Query(
        default=None,
        description="Free-form tags to attach to this simulation for later filtering "
        "(e.g. 'cd1'). Repeat the param for multiple tags. Tags can also be added "
        "later via POST /simulations/{id}/tags.",
    ),
    analysis_options: AnalysisOptions | None = None,
    extra_params: dict[str, Any] | None = Body(
        default=None,
        description="Additional composite-specific parameters not covered by the named "
        "params above (e.g. a composite's own `injected_processes`/`multi_node_dispatch` "
        "knobs). Merged into the resolved config without overriding any of the named "
        "params — a key here is ignored if the same key is already set by one of them.",
    ),
) -> Simulation:
    """Run a vEcoli simulation workflow with simplified parameters.

    This endpoint reads the workflow configuration from the vEcoli repo on the HPC
    system and allows overriding specific parameters via query params.
    """
    _validate_simulation_config_filename(simulation_config_filename)
    if experiment_id is None:
        experiment_id = get_experiment_id(simulator_id, simulation_config_filename)
    sim_service = get_simulation_service()
    if sim_service is None:
        logger.error("Simulation service is not initialized")
        raise HTTPException(status_code=500, detail="Simulation service is not initialized")
    database_service = get_database_service()
    if database_service is None:
        logger.error("Database service is not initialized")
        raise HTTPException(status_code=500, detail="Database service is not initialized")
    try:
        return await handlers.simulations.run_simulation_workflow(
            database_service=database_service,
            simulation_service=sim_service,
            simulator_id=simulator_id,
            experiment_id=experiment_id,
            simulation_config_filename=simulation_config_filename,
            num_generations=num_generations,
            num_seeds=num_seeds,
            composite=composite,
            condition=condition,
            max_generations=max_generations,
            vecoli_source=vecoli_source,
            description=description,
            run_parca=run_parca,
            observables=observables,
            analysis_options=analysis_options,
            ecoli_sources_uri=ecoli_sources_uri,
            ecoli_sources_overlays=ecoli_sources_overlays,
            ecoli_sources_repo_url=ecoli_sources_repo_url,
            ecoli_sources_ref=ecoli_sources_ref,
            tags=tags,
            extra_params=extra_params,
        )
    except DispatchValidationError as e:
        # A request the caller can fix. Deliberately narrower than `ValueError`:
        # not every ValueError raised down this path is the caller's fault, and a
        # 500 is the one status worth paging on.
        logger.info("Rejected simulation dispatch: %s", e)
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.exception("Error running vEcoli simulation")
        raise HTTPException(status_code=500, detail=str(e)) from e


@config.router.get(
    path="/simulations/{id}",
    operation_id="get-ecoli-simulation",
    tags=["Simulations"],
    dependencies=[Depends(get_database_service)],
)
async def get_simulation(id: int = FastAPIPath(description="Database ID of the simulation")) -> Simulation | None:
    db_service = get_database_service()
    if db_service is None:
        logger.error("Database service is not initialized")
        raise HTTPException(status_code=500, detail="Database service is not initialized")
    try:
        # return await db_service.get_simulation(database_id=id)
        return await db_service.get_simulation(simulation_id=id)
    except Exception as e:
        logger.exception("Error uploading simulation config")
        raise HTTPException(status_code=500, detail=str(e)) from e


@config.router.get(
    path="/simulations/{id}/status",
    response_model=SimulationRun,
    operation_id="get-ecoli-simulation-status",
    tags=["Simulations"],
    dependencies=[Depends(get_database_service)],
    summary="Get the simulation status record by its ID",
)
async def get_simulation_status(id: int = FastAPIPath(...)) -> SimulationRun:
    db_service = get_database_service()
    if db_service is None:
        raise HTTPException(status_code=404, detail="Database not found")
    try:
        return await handlers.simulations.get_simulation_status(db_service=db_service, id=id)
    except Exception as e:
        logger.exception(
            """Error getting simulation status.\
                Are you sure that you've passed the experiment_tag? (not the experiment id)
            """
        )
        raise HTTPException(status_code=500, detail=str(e)) from e


@config.router.get(
    path="/simulations/{id}/chain-progress",
    response_model=ChainProgress,
    operation_id="get-ecoli-simulation-chain-progress",
    tags=["Simulations"],
    dependencies=[Depends(get_database_service)],
    summary="Get real per-seed aggregate progress for a chain-dispatch campaign",
)
async def get_simulation_chain_progress(id: int = FastAPIPath(...)) -> ChainProgress:
    """Backlog item 6: real seed-level progress (succeeded/failed/in-progress
    counts) for a chain-dispatch campaign (backlog item 33) — the SAME data
    ``/simulations/{id}/status`` already computes internally and collapses to
    one coarse phase, exposed at its real granularity. 404 when the
    simulation/HpcRun doesn't exist; 409 when it exists but isn't a
    chain-dispatch campaign (a plain single-shot run has nothing to
    aggregate — callers should use ``/status`` for those instead)."""
    db_service = get_database_service()
    if db_service is None:
        raise HTTPException(status_code=404, detail="Database not found")
    try:
        return await handlers.simulations.get_simulation_chain_progress(db_service=db_service, id=id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except RuntimeError as e:
        raise HTTPException(status_code=409, detail=str(e)) from e
    except Exception as e:
        logger.exception("Error getting simulation chain progress")
        raise HTTPException(status_code=500, detail=str(e)) from e


@config.router.get(
    path="/simulations/{id}/events",
    response_model=SimulationEvents,
    operation_id="get-ecoli-simulation-events",
    tags=["Simulations"],
    dependencies=[Depends(get_database_service)],
    summary="Structured events of a simulation run (engine, runner and dispatch components), flat or as the span tree",
)
async def get_simulation_events(
    id: int = FastAPIPath(...),
    level: str | None = Query(default=None, description="debug | info | warning | error"),
    event: str | None = Query(
        default=None, description="exact event name, e.g. process.exception, lineage.generation.end"
    ),
    generation: int | None = Query(default=None),
    span_id: str | None = Query(default=None),
    after: int | None = Query(default=None, description="cursor of the last event seen (paging)"),
    limit: int = Query(default=1000, ge=1, le=1000),
    tree: bool = Query(default=False, description="also return the span tree with events attached"),
) -> SimulationEvents:
    """Observability plan D4d: what the run's tasks reported, without AWS
    credentials. Events arrive through the scheduler's ingester from the tasks'
    ``events.jsonl`` objects plus the API's own dispatcher-layer events; ``tick``
    heartbeats are folded into ``/status`` (``last_event_at``) and never stored.
    404 when the simulation or its run row does not exist."""
    db_service = get_database_service()
    if db_service is None:
        raise HTTPException(status_code=404, detail="Database not found")
    try:
        return await handlers.simulations.get_simulation_events(
            db_service=db_service,
            id=id,
            level=level,
            event=event,
            generation=generation,
            span_id=span_id,
            after=after,
            limit=limit,
            tree=tree,
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except Exception as e:
        logger.exception("Error getting simulation events")
        raise HTTPException(status_code=500, detail=str(e)) from e


@config.router.get(
    path="/simulations/{id}/tasks",
    response_model=list[SimulationTask],
    operation_id="get-ecoli-simulation-tasks",
    tags=["Simulations"],
    dependencies=[Depends(get_database_service)],
    summary="The run's units of work as the backend saw them (Nextflow trace rows / chain seed jobs)",
)
async def get_simulation_tasks(id: int = FastAPIPath(...)) -> list[SimulationTask]:
    """Observability plan D4d. Nextflow: one row per ``trace.csv`` task (its
    ``job_id`` is the Batch job id); chain dispatch: one row per seed job with
    Batch's status reason; other backends: an empty list. 404 when the
    simulation or its run row does not exist."""
    db_service = get_database_service()
    if db_service is None:
        raise HTTPException(status_code=404, detail="Database not found")
    try:
        return await handlers.simulations.get_simulation_tasks(db_service=db_service, id=id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except Exception as e:
        logger.exception("Error getting simulation tasks")
        raise HTTPException(status_code=500, detail=str(e)) from e


@config.router.delete(
    path="/simulations/{id}/cancel",
    response_model=SimulationRun,
    operation_id="cancel-ecoli-simulation",
    tags=["Simulations"],
    dependencies=[Depends(get_simulation_service), Depends(get_database_service)],
    summary="Cancel a running simulation",
)
async def cancel_simulation(id: int = FastAPIPath(description="Database ID of the simulation")) -> SimulationRun:
    """Cancel a running simulation by killing its backend job."""
    sim_service = get_simulation_service()
    if sim_service is None:
        raise HTTPException(status_code=500, detail="Simulation service is not initialized")
    db_service = get_database_service()
    if db_service is None:
        raise HTTPException(status_code=500, detail="Database service is not initialized")
    try:
        return await handlers.simulations.cancel_simulation(
            db_service=db_service,
            simulation_service=sim_service,
            simulation_id=id,
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except Exception as e:
        logger.exception("Error cancelling simulation")
        raise HTTPException(status_code=500, detail=str(e)) from e


@config.router.get(
    path="/simulations/{id}/log",
    operation_id="get-ecoli-simulation-log",
    tags=["Simulations"],
    dependencies=[Depends(get_database_service)],
    summary="Get the structured output of a given simulation workflow log.",
)
async def get_simulation_log(
    id: int = FastAPIPath(...),
    truncate: bool = Query(
        default=True,
        description="If true, return only the Nextflow header + final status block "
        "(separated by '... truncated ...'). Set to false for the full log.",
    ),
) -> Response:
    db_service = get_database_service()
    if db_service is None:
        raise HTTPException(status_code=404, detail="Database not found")
    try:
        return await handlers.simulations.get_simulation_log(db_service=db_service, simulation_id=id, truncate=truncate)
    except Exception as e:
        logger.exception(
            """Error getting simulation status.\
                Are you sure that you've passed the experiment_tag? (not the experiment id)
            """
        )
        raise HTTPException(status_code=500, detail=str(e)) from e


@config.router.post(
    path="/simulations/{id}/analysis",
    operation_id="run-ecoli-simulation-analysis",
    tags=["Simulations"],
    dependencies=[Depends(get_database_service)],
    summary="Run standalone analysis on existing simulation output",
)
async def run_simulation_analysis(
    id: int = FastAPIPath(description="Database ID of a completed simulation."),
    modules: str | None = Query(
        default=None,
        description="JSON object mapping analysis domains to module configs. "
        'E.g. \'{"single": {"ptools_rna": {"n_tp": 10}}}\'.'
        " If omitted, runs default ptools modules.",
    ),
) -> dict:  # type: ignore[type-arg]
    db_service = get_database_service()
    if db_service is None:
        raise HTTPException(status_code=500, detail="Database service is not initialized")
    try:
        parsed_modules = json.loads(modules) if modules else None
        return await handlers.simulations.run_standalone_analysis(
            database_service=db_service,
            simulation_id=id,
            modules=parsed_modules,
        )
    except Exception as e:
        logger.exception("Error running standalone analysis")
        raise HTTPException(status_code=500, detail=str(e)) from e


@config.router.post(
    path="/parca/new-gene-cache",
    response_model=NewGeneCacheJob,
    operation_id="run-new-gene-cache",
    tags=["EcoliSim"],
    summary="Stamp an induction level onto a completed ParCa dataset's cache (backlog item 105)",
)
async def run_new_gene_cache(request: NewGeneCacheRequest = Body(...)) -> NewGeneCacheJob:
    try:
        return await handlers.simulations.run_new_gene_cache(request=request)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error running new-gene-cache job")
        raise HTTPException(status_code=500, detail=str(e)) from e


@config.router.post(
    path="/parca/variant-cache",
    response_model=VariantCacheJob,
    operation_id="run-variant-cache",
    tags=["EcoliSim"],
    summary="Stamp native-gene perturbations onto a completed ParCa dataset's cache (backlog item 451)",
)
async def run_variant_cache(request: VariantCacheRequest = Body(...)) -> VariantCacheJob:
    try:
        return await handlers.simulations.run_variant_cache(request=request)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error running variant-cache job")
        raise HTTPException(status_code=500, detail=str(e)) from e


@config.router.get(
    path="/simulations/{id}/analyses",
    operation_id="list-simulation-analyses",
    tags=["Analyses"],
    dependencies=[Depends(get_database_service)],
    summary="List the existing (pre-run) analyses for a simulation",
)
async def list_simulation_analyses(
    id: int = FastAPIPath(description="Database ID of the simulation"),
) -> list[ExperimentAnalysisDTO]:
    db_service = get_database_service()
    if db_service is None:
        raise HTTPException(status_code=500, detail="Database service is not initialized")
    try:
        return await handlers.analyses.list_simulation_analyses(db_service=db_service, simulation_id=id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except Exception as e:
        logger.exception("Error listing simulation analyses")
        raise HTTPException(status_code=500, detail=str(e)) from e


@config.router.get(
    path="/simulations/{id}/datasets",
    response_model=DatasetListDTO,
    operation_id="list-simulation-datasets",
    tags=["Datasets"],
    dependencies=[Depends(get_database_service)],
    summary="Datasets attributed to a simulation (optionally with those of its analyses)",
)
async def list_simulation_datasets(
    id: int = FastAPIPath(description="Database ID of the simulation"),
    include_analyses: bool = Query(
        default=False,
        description="Also datasets attributed to analyses OF this simulation (matched on the dataset's source).",
    ),
    params: handlers.datasets.DatasetListParams = Depends(handlers.datasets.dataset_list_params),
) -> DatasetListDTO:
    """docs/plan-data-provenance.md §7. The simulation is the producer of what its run wrote and of
    bundles the S3 walk found under its output that no analysis run claims (``origin = walk``).
    Rows lag ingestion. 404 for an unknown simulation."""
    db_service = get_database_service()
    if db_service is None:
        raise HTTPException(status_code=500, detail="Database service is not initialized")
    if await db_service.get_simulation(simulation_id=id) is None:
        raise HTTPException(status_code=404, detail=f"Simulation {id} not found")
    scope: dict[str, Any] = (
        {"source": {"kind": "simulation", "ref": str(id)}} if include_analyses else {"simulation_id": id}
    )
    try:
        return await handlers.datasets.list_page(db_service, params, **scope)
    except handlers.datasets.DatasetQueryError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@config.router.get(
    path="/simulations",
    operation_id="list-ecoli-simulations",
    tags=["Simulations"],
    summary="List all simulation specs uploaded to the database",
    dependencies=[Depends(get_database_service)],
)
async def list_simulations(
    experiment_id: str | None = Query(
        default=None,
        description="Comma-separated list of experiment IDs to filter by. "
        "Example: 'sim31-baseline-60bb,sim33-violacien-seeds1000-generations10-9617'",
    ),
    tag: str | None = Query(
        default=None,
        description="Comma-separated list of tags to filter by (e.g. 'cd1'). "
        "Tags are free-form data on each simulation; an unknown tag simply matches "
        "nothing. Use GET /api/v1/simulations/tags to list tags present in the database.",
    ),
    limit: int | None = Query(
        default=None,
        ge=1,
        description="Maximum number of simulations to return, most-recent first (by id). "
        "Bounds the database query itself, not just the response; omit to return all. "
        "Applies to the unfiltered listing (not combined with experiment_id/tag).",
    ),
    offset: int = Query(
        default=0,
        ge=0,
        description="Number of simulations to skip before returning results (pagination, with limit).",
    ),
) -> list[Simulation]:
    db_service = get_database_service()
    if db_service is None:
        logger.error("Database service is not initialized")
        raise HTTPException(status_code=500, detail="Database service is not initialized")
    try:
        if experiment_id is not None or tag is not None:
            return await handlers.simulations.list_simulations_filtered(
                db_service=db_service,
                experiment_id=experiment_id,
                tag=tag,
            )
        return await handlers.simulations.list_simulations(db_service=db_service, limit=limit, offset=offset)
    except Exception as e:
        logger.exception("Error fetching the uploaded analyses")
        raise HTTPException(status_code=500, detail=str(e)) from e


@config.router.post(
    path="/simulations/{id}/tags",
    operation_id="add-simulation-tags",
    response_model=Simulation,
    tags=["Simulations"],
    dependencies=[Depends(get_database_service)],
    summary="Attach one or more free-form tags to an existing simulation",
)
async def add_simulation_tags(
    id: int = FastAPIPath(description="Database ID of the simulation"),
    tags: list[str] = Body(
        ...,
        embed=True,
        description="Tags to add (union-merged with existing tags). Example: ['cd1'].",
    ),
) -> Simulation:
    db_service = get_database_service()
    if db_service is None:
        logger.error("Database service is not initialized")
        raise HTTPException(status_code=500, detail="Database service is not initialized")
    try:
        return await db_service.add_tags(simulation_id=id, tags=tags)
    except Exception as e:
        logger.exception("Error adding tags to simulation")
        raise HTTPException(status_code=404, detail=str(e)) from e


@config.router.post(
    path="/simulations/{id}/data",
    operation_id="get-ecoli-simulation-data",
    tags=["Simulations"],
    dependencies=[Depends(get_database_service)],
    summary="Get simulation omics data as a downloadable tar.gz archive",
    response_model=None,
    responses={
        200: {
            "content": {"application/gzip": {}},
            "description": "A tar.gz archive containing simulation output files",
        }
    },
)
async def get_simulation_data(
    bg_tasks: BackgroundTasks,
    id: int = FastAPIPath(description="Database ID of the simulation."),
    response_type: handlers.simulations.SimulationAnalysisDataResponseType = Query(
        default=handlers.simulations.SimulationAnalysisDataResponseType.FILE,
        description="Response type: 'file' for direct download (recommended for browsers/Swagger UI), "
        "'streaming' for chunked streaming response (better for large files or programmatic access)",
    ),
) -> StreamingResponse | FileResponse:
    """Get simulation outputs as a tar.gz archive.

    Choose response_type based on your use case:
    - **file**: Creates the archive and returns it as a downloadable file.
      Best for browser downloads and Swagger UI - shows a "Download" button.
    - **streaming**: Streams the archive in chunks as it's created.
      Better for very large files or when you want to start processing before download completes.
    """
    db_service = get_database_service()
    if db_service is None:
        logger.error("Database service is not initialized")
        raise HTTPException(status_code=500, detail="Database service is not initialized")
    try:
        return await handlers.simulations.get_simulation_outputs(
            db_service=db_service,
            simulation_id=id,
            hpc_sim_base_path=ENV.hpc_sim_base_path,
            data_response_type=response_type,
            bg_tasks=bg_tasks,
        )
    except Exception as e:
        logger.exception("Error retrieving simulation data")
        raise HTTPException(status_code=500, detail=str(e)) from e


@config.router.get(
    path="/simulations/{id}/observables/index",
    response_model=SimulationObservableIndex,
    operation_id="get-simulation-observables-index",
    tags=["Simulations"],
    summary="List observables available in a simulation's emitter store (S3)",
)
async def get_simulation_observables_index(
    id: int = FastAPIPath(description="Database ID of the simulation"),
    seed: int = Query(0, ge=0),
) -> SimulationObservableIndex:
    db = get_database_service()
    if db is None:
        raise HTTPException(503, "database service unavailable")
    sim = await db.get_simulation(simulation_id=id)
    if sim is None:
        raise HTTPException(404, f"Simulation {id} not found")
    store_uri = await _ray_seed_store_uri_or_error(db, sim, seed)
    try:
        idx = await list_observables_async(store_uri)
    except FileNotFoundError:
        raise HTTPException(404, f"No emitter store for simulation {id} (seed {seed})") from None
    return SimulationObservableIndex(
        simulation_id=id,
        experiment_id=sim.experiment_id,
        seed=seed,
        store=idx.store,
        observables=[ObservableInfoModel(name=o.name, dims=o.dims, shape=o.shape) for o in idx.observables],
    )


@config.router.get(
    path="/simulations/{id}/observables",
    response_model=SimulationObservables,
    operation_id="get-simulation-observables",
    tags=["Simulations"],
    summary="Read observable timeseries from a simulation's emitter store (S3)",
)
async def get_simulation_observables(
    id: int = FastAPIPath(description="Database ID of the simulation"),
    names: str = "",
    seed: int = Query(0, ge=0),
    stride: int = Query(1, ge=1, description="Return every Nth point (decimation). 1 = full resolution."),
    max_points: int | None = Query(
        None, ge=1, description="Cap the number of points returned; overrides `stride` if it implies a coarser step."
    ),
) -> SimulationObservables:
    db = get_database_service()
    if db is None:
        raise HTTPException(503, "database service unavailable")
    sim = await db.get_simulation(simulation_id=id)
    if sim is None:
        raise HTTPException(404, f"Simulation {id} not found")
    requested = [n.strip() for n in names.split(",") if n.strip()]
    store_uri = await _ray_seed_store_uri_or_error(db, sim, seed)
    try:
        store_kind, time, series = await read_observables_async(
            store_uri, requested, stride=stride, max_points=max_points
        )
    except FileNotFoundError:
        raise HTTPException(404, f"No emitter store for simulation {id} (seed {seed})") from None
    except KeyError as e:
        raise HTTPException(400, str(e)) from e
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    return SimulationObservables(
        simulation_id=id,
        experiment_id=sim.experiment_id,
        seed=seed,
        store=store_kind,
        time=time,
        series=series,
    )


@config.router.post(
    path="/analyses",
    operation_id="run-ecoli-simulation-analysis",
    tags=["Analyses"],
    summary="Run an analysis",
    dependencies=[
        Depends(get_database_service),
    ],
)
async def run_analysis(
    _request: Request,
    request: ExperimentAnalysisRequest = request_examples.analysis_ptools,
) -> Sequence[TsvOutputFile | OutputFileMetadata]:
    if get_job_backend() != ComputeBackend.SLURM:
        raise HTTPException(
            status_code=501,
            detail="Legacy analysis not supported for K8s backend. Use POST /api/v1/simulations/{id}/analysis instead.",
        )
    db_service = get_database_service()
    if db_service is None:
        raise HTTPException(status_code=404, detail="Database not found")
    analysis_service = AnalysisServiceSlurm(env=ENV)

    # Look up the simulation by experiment_id to get the correct simulator
    simulation = await db_service.get_simulation_by_experiment_id(request.experiment_id)
    if simulation is None:
        raise HTTPException(status_code=404, detail=f"No simulation found with experiment_id '{request.experiment_id}'")

    simulator = await db_service.get_simulator(simulation.simulator_id)
    if simulator is None:
        raise HTTPException(status_code=404, detail=f"Simulator with id {simulation.simulator_id} not found")

    try:
        return await handlers.analyses.handle_run_analysis(
            request=request,
            simulator=simulator,
            analysis_service=analysis_service,
            logger=logger,
            _request=_request,
            db_service=db_service,
        )
    except AnalysisJobFailedException as e:
        # Return detailed error for failed analysis jobs
        logger.warning(f"Analysis job failed: {e.message}")
        raise HTTPException(status_code=422, detail=e.to_dict()) from e
    except Exception as e:
        logger.exception("Error running analysis.")
        raise HTTPException(status_code=500, detail=str(e)) from e


@config.router.get(
    path="/analyses",
    operation_id="list-analyses",
    tags=["Analyses"],
    dependencies=[Depends(get_database_service)],
    summary="List analyses across all simulations, newest change first, with filters and paging",
)
async def list_analyses(
    experiment_id: str | None = Query(default=None, description="Optional: filter by experiment_id."),
    simulation_id: int | None = Query(default=None, description="Optional: filter by simulation database id."),
    status: JobStatus | None = Query(
        default=None,
        description="Filter by reported status: 'completed' (ready), 'failed' (or 'cancelled'); "
        "any other value means still computing.",
    ),
    backend: str | None = Query(default=None, description="Filter by backend, e.g. 'ray', 'k8s', 'batch'."),
    source: str | None = Query(
        default=None, description="What the analysis is OF: 'sim:1002', or a JSON ProvenanceRef fragment."
    ),
    tag: str | None = Query(default=None, description="Comma-separated tags; an analysis must carry all of them."),
    since: datetime.datetime | None = Query(default=None, description="Only analyses changed at or after this time."),
    limit: int | None = Query(default=None, ge=1, description="Page size; omit to return every match."),
    offset: int = Query(default=0, ge=0, description="Rows to skip (pagination, with limit)."),
) -> list[ExperimentAnalysisDTO]:
    db_service = get_database_service()
    if db_service is None:
        raise HTTPException(status_code=500, detail="Database service is not initialized")
    try:
        source_filter = handlers.datasets.parse_source_filter(source)
    except handlers.datasets.DatasetQueryError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    try:
        return await db_service.list_analyses(
            experiment_id=experiment_id,
            simulation_id=simulation_id,
            status=AnalysisStatusDB.from_job_status(status) if status is not None else None,
            backend=backend,
            tags=handlers.datasets.parse_tags(tag),
            source=source_filter,
            since=handlers.datasets.naive_utc(since),
            limit=limit,
            offset=offset,
            newest_first=True,
        )
    except Exception as e:
        logger.exception("Error listing analyses")
        raise HTTPException(status_code=500, detail=str(e)) from e


@config.router.get(
    path="/analyses/{id}",
    operation_id="get-analysis",
    tags=["Analyses"],
    dependencies=[Depends(get_database_service)],
    summary="Retrieve an experiment analysis spec from the database",
)
async def get_analysis_spec(id: int) -> ExperimentAnalysisDTO:
    db_service = get_database_service()
    if db_service is None:
        raise HTTPException(status_code=404, detail="Database not found")
    try:
        analysis = await handlers.analyses.handle_get_analysis(db_service=db_service, id=id)
        analysis.n_datasets = await db_service.count_datasets(analysis_id=id, available=True)
        return analysis
    except Exception as e:
        logger.exception("Error fetching the simulation analysis file.")
        raise HTTPException(status_code=500, detail=str(e)) from e


@config.router.get(
    path="/analyses/{id}/datasets",
    response_model=DatasetListDTO,
    operation_id="list-analysis-datasets",
    tags=["Datasets"],
    dependencies=[Depends(get_database_service)],
    summary="Datasets an analysis run wrote",
)
async def list_analysis_datasets(
    id: int = FastAPIPath(description="Database ID of the analysis"),
    params: handlers.datasets.DatasetListParams = Depends(handlers.datasets.dataset_list_params),
) -> DatasetListDTO:
    """docs/plan-data-provenance.md §7. Rows lag ingestion; 404 for an unknown analysis."""
    db_service = get_database_service()
    if db_service is None:
        raise HTTPException(status_code=500, detail="Database service is not initialized")
    try:
        await db_service.get_analysis(database_id=id)
    except RuntimeError as e:
        raise HTTPException(status_code=404, detail=f"Analysis {id} not found") from e
    try:
        return await handlers.datasets.list_page(db_service, params, analysis_id=id)
    except handlers.datasets.DatasetQueryError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e


@config.router.get(
    path="/analyses/{id}/status",
    tags=["Analyses"],
    operation_id="get-analysis-status",
    dependencies=[Depends(get_database_service)],
    summary="Get the status of an existing experiment analysis run",
)
async def get_analysis_status(id: int = FastAPIPath(..., description="Database ID of the analysis")) -> AnalysisRun:
    db_service = get_database_service()
    if db_service is None:
        raise HTTPException(status_code=404, detail="Database not found")

    try:
        record = await db_service.get_analysis(database_id=id)
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Analysis {id} not found") from e

    if record.backend == "ray":
        try:
            return await handlers.analyses.handle_get_ray_analysis_status(db_service=db_service, record=record)
        except Exception as e:
            logger.exception("Error resolving Ray-native analysis status.")
            raise HTTPException(status_code=500, detail=str(e)) from e

    if get_job_backend() != ComputeBackend.SLURM:
        raise HTTPException(status_code=501, detail="Legacy analysis status not supported for K8s backend")
    aservice = AnalysisServiceSlurm(env=ENV)
    try:
        return await handlers.analyses.handle_get_analysis_status(
            db_service=db_service, analysis_service=aservice, ref=record
        )
    except Exception as e:
        logger.exception(
            """Error getting simulation status.\
                Are you sure that you've passed the experiment_tag? (not the experiment id)
            """
        )
        raise HTTPException(status_code=500, detail=str(e)) from e


@config.router.get(
    path="/analyses/{id}/log",
    tags=["Analyses"],
    operation_id="get-analysis-log",
    dependencies=[Depends(get_database_service)],
    summary="Get the log of an existing experiment analysis run",
)
async def get_analysis_log(id: int = FastAPIPath(..., description="Database ID of the analysis")) -> str:
    db_service = get_database_service()
    if db_service is None:
        raise HTTPException(status_code=404, detail="Database not found")
    try:
        return await handlers.analyses.handle_get_analysis_log(db_service=db_service, id=id)
    except Exception as e:
        logger.exception(
            """Error getting simulation status.\
                Are you sure that you've passed the experiment_tag? (not the experiment id)
            """
        )
        raise HTTPException(status_code=500, detail=str(e)) from e


@config.router.get(
    path="/analyses/{id}/plots",
    tags=["Analyses"],
    operation_id="get-analysis-plots",
    dependencies=[Depends(get_database_service)],
    summary="Get an array of HTML files representing all plot outputs of a given analysis.",
)
async def get_analysis_plots(
    id: int = FastAPIPath(..., description="Database ID of the analysis"),
) -> list[OutputFile]:
    db_service = get_database_service()
    if db_service is None:
        raise HTTPException(status_code=404, detail="Database not found")

    try:
        record = await db_service.get_analysis(database_id=id)
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Analysis {id} not found") from e

    if record.backend == "ray":
        try:
            return await handlers.analyses.handle_get_ray_analysis_plots(db_service=db_service, record=record)
        except handlers.analyses.AnalysisNotReadyError as e:
            raise HTTPException(status_code=409, detail=str(e)) from e
        except Exception as e:
            logger.exception("Error getting Ray-native analysis plots.")
            raise HTTPException(status_code=500, detail=str(e)) from e

    try:
        return await handlers.analyses.handle_get_analysis_plots(db_service=db_service, id=id)
    except Exception as e:
        logger.exception("Error getting analysis data")
        raise HTTPException(status_code=500, detail=str(e)) from e


@config.router.get(
    path="/analyses/{id}/data",
    tags=["Analyses"],
    operation_id="get-analysis-data",
    dependencies=[Depends(get_database_service)],
    summary="Retrieve the output files (TSV/CSV/TXT/HTML) of an existing analysis by id",
)
async def get_analysis_data(
    id: int = FastAPIPath(..., description="Database ID of the analysis"),
    view: str | None = Query(default=None, description="Only files of this view, e.g. 'ptools_rna'."),
    protocol: str | None = Query(
        default=None, description="Only files of this protocol: single, multigeneration, multiseed, multidaughter, all."
    ),
    variant: int | None = Query(default=None, description="Only files of this variant."),
    seed: int | None = Query(default=None, description="Only files of this lineage seed."),
    generation: int | None = Query(default=None, description="Only files of this generation."),
) -> list[TsvOutputFile]:
    """Pure retrieval of a pre-computed analysis's files by id (never computes).

    Returns the same ``list[TsvOutputFile]`` shape as the legacy ``POST /analyses``. The
    coordinate filters select files before any is downloaded. When the selection holds one file
    per view, ``filename`` is aliased to ``<view>.tsv`` (what an unpatched ptools page expects)
    and ``path`` carries the real name. 409 if the analysis is not READY; 404 if the id is unknown.
    """
    db_service = get_database_service()
    if db_service is None:
        raise HTTPException(status_code=404, detail="Database not found")
    selection = handlers.analyses.AnalysisFileSelection(
        view=view, protocol=protocol, variant=variant, seed=seed, generation=generation
    )
    try:
        return await handlers.analyses.fetch_analysis_data(db_service=db_service, analysis_id=id, selection=selection)
    except handlers.analyses.AnalysisNotReadyError as e:
        raise HTTPException(status_code=409, detail=str(e)) from e
    except RuntimeError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except Exception as e:
        logger.exception("Error retrieving analysis data")
        raise HTTPException(status_code=500, detail=str(e)) from e


@config.router.get(
    path="/simulations/{id}/analysis-figures",
    tags=["Analyses"],
    operation_id="list-simulation-analysis-figures",
    dependencies=[Depends(get_database_service)],
    summary="List every analysis's rendered figures + ptools for a simulation (metadata only).",
)
async def list_simulation_analysis_figures(
    id: int = FastAPIPath(..., description="Database ID of the simulation"),
) -> SimulationAnalysisFigures:
    """List (never inline) the rendered ``viz/`` figures and ``ptools/`` tables of
    every analysis on a simulation, unioning DB analysis records with a direct S3
    walk so hand-dispatched "fill" analyses that created no DB record still surface
    (viva-api#648). The API reads S3 with its own credentials, so a credential-less
    client can then fetch each artifact via ``/simulations/{id}/analysis-figure``.
    """
    db_service = get_database_service()
    if db_service is None:
        raise HTTPException(status_code=404, detail="Database not found")
    try:
        return await handlers.analyses.list_simulation_analysis_figures(db_service=db_service, simulation_id=id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except Exception as e:
        logger.exception("Error listing simulation analysis figures")
        raise HTTPException(status_code=500, detail=str(e)) from e


@config.router.get(
    path="/simulations/{id}/analysis-figure",
    tags=["Analyses"],
    operation_id="get-simulation-analysis-figure",
    dependencies=[Depends(get_database_service)],
    summary="Fetch one rendered analysis artifact (a viz/ figure or ptools/ table) by relative path.",
)
async def get_simulation_analysis_figure(
    id: int = FastAPIPath(..., description="Database ID of the simulation"),
    analysis: str = Query(..., description="Analysis directory name (e.g. analysis-ptools-multiseed)"),
    path: str = Query(..., description="Artifact path relative to the analysis dir, e.g. viz/foo.html"),
) -> Response:
    """Return a single rendered artifact's bytes with a content-type by extension.

    ``path`` is confined to the analysis prefix (must start with ``viz/`` or
    ``ptools/``; no ``..``); its S3 location is resolved server-side, so the caller
    never handles a raw S3 uri. 400 on a bad path, 404 on an unknown
    simulation/analysis or missing object.
    """
    db_service = get_database_service()
    if db_service is None:
        raise HTTPException(status_code=404, detail="Database not found")
    try:
        content, content_type = await handlers.analyses.fetch_simulation_analysis_figure(
            db_service=db_service, simulation_id=id, analysis_name=analysis, relpath=path
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except Exception as e:
        logger.exception("Error fetching simulation analysis figure")
        raise HTTPException(status_code=500, detail=str(e)) from e
    return Response(content=content, media_type=content_type)
