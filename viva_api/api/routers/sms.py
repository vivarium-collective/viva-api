"""
/analyses: this router is dedicated to the running and output retrieval of
    simulation analysis jobs/workflows
"""

# TODO: do we require simulation/analysis configs that are supersets of the original configs:
#   IE: where do we provide this special config: in vEcoli or API?
# TODO: what does a "configuration endpoint" actually mean (can we configure via the simulation?)
# TODO: labkey preprocessing
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
    ExperimentAnalysisDTO,
    ExperimentAnalysisRequest,
    OutputFile,
    OutputFileMetadata,
    TsvOutputFile,
)
from viva_api.api import request_examples
from viva_api.common import handlers
from viva_api.common.gateway.utils import get_router_config
from viva_api.common.storage import data_layout
from viva_api.config import ComputeBackend, compute_backend_for_repo, get_job_backend, get_settings
from viva_api.dependencies import get_database_service, get_simulation_service
from viva_api.simulation.database_service import DatabaseService
from viva_api.simulation.github_repo import open_repo_tarball_stream
from viva_api.simulation.models import (
    AnalysisOptions,
    ChainProgress,
    CompositeEngine,
    ObservableInfoModel,
    RepoDiscovery,
    Simulation,
    SimulationObservableIndex,
    SimulationObservables,
    SimulationRun,
    VecoliSource,
)
from viva_api.simulation.observable_reader import list_observables_async, read_observables_async


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
    """
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
        return await handlers.simulations.list_simulations(db_service=db_service)
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
    summary="List all analyses across all simulations (exhaustive; filtering/paging to come)",
)
async def list_analyses(
    experiment_id: str | None = Query(default=None, description="Optional: filter by experiment_id."),
    simulation_id: int | None = Query(default=None, description="Optional: filter by simulation database id."),
) -> list[ExperimentAnalysisDTO]:
    db_service = get_database_service()
    if db_service is None:
        raise HTTPException(status_code=500, detail="Database service is not initialized")
    try:
        return await db_service.list_analyses(experiment_id=experiment_id, simulation_id=simulation_id)
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
        return await handlers.analyses.handle_get_analysis(db_service=db_service, id=id)
    except Exception as e:
        logger.exception("Error fetching the simulation analysis file.")
        raise HTTPException(status_code=500, detail=str(e)) from e


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
) -> list[TsvOutputFile]:
    """Pure retrieval of a pre-computed analysis's files by id (never computes).

    Returns the same ``list[TsvOutputFile]`` shape as the legacy ``POST /analyses``.
    409 if the analysis is not READY; 404 if the analysis id is unknown.
    """
    db_service = get_database_service()
    if db_service is None:
        raise HTTPException(status_code=404, detail="Database not found")
    try:
        return await handlers.analyses.fetch_analysis_data(db_service=db_service, analysis_id=id)
    except handlers.analyses.AnalysisNotReadyError as e:
        raise HTTPException(status_code=409, detail=str(e)) from e
    except RuntimeError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except Exception as e:
        logger.exception("Error retrieving analysis data")
        raise HTTPException(status_code=500, detail=str(e)) from e
