"""Compose (process-bigraph) simulation router — mounted at /compose/v1/."""

import json
import logging
import os
import tempfile
from pathlib import Path

from fastapi import APIRouter, BackgroundTasks, Form, HTTPException, Query, UploadFile
from jinja2 import Template
from starlette.responses import FileResponse, StreamingResponse

from viva_api.compose.database_service import ComposeDatabaseService
from viva_api.compose.handlers import (
    get_compose_simulator_versions,
    run_compose_curated,
    run_compose_simulation,
)
from viva_api.compose.job_monitor import ComposeJobMonitor
from viva_api.compose.models import (
    DEFAULT_COMPOSE_ALLOW_LIST,
    BiGraphComputeType,
    BiGraphProcess,
    BiGraphStep,
    BiomodelInfo,
    BiomodelSimulator,
    BiomodelsRunRequest,
    BiomodelsRunResult,
    ComposeDocumentSubmission,
    ComposeHpcRun,
    ComposeJobType,
    ComposeRegisteredSimulators,
    ComposeSimulationExperiment,
    ComposeSimulationRequest,
    PBAllowList,
    SimulationFileType,
)
from viva_api.compose.simulation_service import ComposeSimulationService
from viva_api.config import ComputeBackend

logger = logging.getLogger(__name__)

router = APIRouter()

# Upper bound on interval_time (== ComposeSimulationRequest.end_time_point), in
# simulated seconds. A single-cell run needs only a few thousand, but a
# multi-generation lineage document (v2ecoli lineage_ray_batch: LineageProcess
# advances TOTAL simulated time, one division per generation) needs
# n_generations * max_duration_per_gen -- e.g. 8 generations * 3600 s = 28,800 s.
# 1000 rejected every such document. Still bounded to keep a runaway run from
# being submitted.
MAX_INTERVAL_TIME: float = 100_000.0

# ---------------------------------------------------------------------------
# Dependency helpers (lazy — populated at app startup via dependencies.py)
# ---------------------------------------------------------------------------

_compose_db_service: ComposeDatabaseService | None = None
_compose_sim_service: ComposeSimulationService | None = None
_compose_job_monitor: ComposeJobMonitor | None = None


def set_compose_services(
    db: ComposeDatabaseService,
    sim: ComposeSimulationService,
    monitor: ComposeJobMonitor,
) -> None:
    global _compose_db_service, _compose_sim_service, _compose_job_monitor
    _compose_db_service = db
    _compose_sim_service = sim
    _compose_job_monitor = monitor


def _require_db() -> ComposeDatabaseService:
    if _compose_db_service is None:
        raise HTTPException(500, "Compose database service not initialized")
    return _compose_db_service


def _require_sim(compute_backend: ComputeBackend | None = None) -> ComposeSimulationService:
    if _compose_sim_service is None:
        raise HTTPException(500, "Compose simulation service not initialized")
    if compute_backend is None:
        return _compose_sim_service
    # Explicit per-request backend (item 98) -- fail loud when it isn't registered
    # rather than silently substitute the default, unlike the ensemble path's own
    # get_simulation_service_for_backend (an internal repo-inference helper, not a
    # caller-facing request param): a caller who explicitly asked for one backend
    # and silently got another is exactly the "looked successful, ran the wrong
    # thing" class of bug viva-api#353 flagged as costing real debugging time.
    registry = _compose_job_monitor.sim_registry if _compose_job_monitor is not None else {}
    service: ComposeSimulationService | None = registry.get(compute_backend)
    if service is None:
        raise HTTPException(
            400,
            f"compute_backend={compute_backend.value!r} is not available on this deployment "
            f"(registered: {sorted(b.value for b in registry)})",
        )
    return service


def _require_monitor() -> ComposeJobMonitor:
    if _compose_job_monitor is None:
        raise HTTPException(500, "Compose job monitor not initialized")
    return _compose_job_monitor


# ---------------------------------------------------------------------------
# Upload helper
# ---------------------------------------------------------------------------


async def _parse_upload(uploaded_file: UploadFile, batch_submission: bool = False) -> ComposeSimulationRequest:
    if uploaded_file is None or uploaded_file.filename is None or uploaded_file.size == 0:
        raise HTTPException(400, "Empty uploaded file")
    suffix = Path(uploaded_file.filename).suffix
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp_file:
        contents = await uploaded_file.read()
        tmp_file.write(contents)
    return ComposeSimulationRequest(
        request_file_path=Path(tmp_file.name),
        simulation_file_type=SimulationFileType.get_file_type(suffix),
        is_batch=batch_submission,
    )


def _parse_analysis_options(analysis_options: str | None) -> dict | None:  # type: ignore[type-arg]
    """The upload transport is multipart, so ``analysis_options`` arrives as a
    plain Form field carrying a JSON string (mirroring ``run_v2ecoli``'s own
    ``features`` field) rather than a native dict body -- parse it here, same
    as that existing pattern, defaulting to None when absent/empty."""
    if not analysis_options:
        return None
    try:
        parsed = json.loads(analysis_options)
    except (json.JSONDecodeError, ValueError):
        raise HTTPException(400, f"Invalid analysis_options JSON: {analysis_options}")
    if not isinstance(parsed, dict):
        # Syntactically-valid JSON that isn't an object (e.g. `42`, `[1,2]`) is just
        # as unusable downstream (build_analysis_config/AnalysisConfigOptions both
        # expect a mapping) -- reject it the same way as unparseable JSON rather than
        # letting a non-dict silently ride through onto the request.
        raise HTTPException(400, f"Invalid analysis_options JSON: {analysis_options}")
    return parsed


def _from_document(document: dict[str, object], batch_submission: bool = False) -> ComposeSimulationRequest:
    """The JSON-body sibling of ``_parse_upload``: write the inline document to
    a temp ``.pbg`` file (plain JSON — ``run_pbg.py`` reads it via
    ``json.loads``) so both transports converge on the identical downstream
    ``ComposeSimulationRequest`` shape and dispatch path.
    """
    if not document:
        raise HTTPException(400, "Empty document")
    with tempfile.NamedTemporaryFile("w", delete=False, suffix=".pbg") as tmp_file:
        json.dump(document, tmp_file)
    return ComposeSimulationRequest(
        request_file_path=Path(tmp_file.name),
        simulation_file_type=SimulationFileType.PBG,
        is_batch=batch_submission,
    )


async def _dispatch_submission(
    simulation_request: ComposeSimulationRequest,
    background_tasks: BackgroundTasks,
    extra_pip_deps: list[str] | None,
) -> ComposeSimulationExperiment:
    db = _require_db()
    allow_list = await db.get_allow_list_db().list_allow_list() or DEFAULT_COMPOSE_ALLOW_LIST
    return await run_compose_simulation(
        simulation_request=simulation_request,
        database_service=db,
        simulation_service=_require_sim(simulation_request.compute_backend),
        job_monitor=_require_monitor(),
        pb_allow_list=PBAllowList(allow_list=allow_list),
        background_tasks=background_tasks,
        extra_pip_deps=extra_pip_deps,
    )


# ---------------------------------------------------------------------------
# Simulation endpoints
# ---------------------------------------------------------------------------


@router.post(
    path="/simulation/run",
    operation_id="compose-run-simulation",
    response_model=ComposeSimulationExperiment,
    tags=["Compose Simulation"],
    summary="Run a process-bigraph simulation (OMEX/PBG/SBML upload)",
)
async def submit_simulation(
    background_tasks: BackgroundTasks,
    uploaded_file: UploadFile,
    interval_time: float = 1.0,
    batch_submission: bool = False,
    simulator_id: int | None = None,
    compute_backend: ComputeBackend | None = None,
    extra_pip_deps: list[str] | None = Query(default=None),
    analysis_options: str | None = Form(default=None),
) -> ComposeSimulationExperiment:
    if interval_time < 0 or interval_time > MAX_INTERVAL_TIME:
        raise HTTPException(400, f"interval_time must be between 0 and {MAX_INTERVAL_TIME:g}")
    simulation_request = await _parse_upload(uploaded_file, batch_submission)
    simulation_request.end_time_point = interval_time
    simulation_request.simulator_id = simulator_id
    simulation_request.compute_backend = compute_backend
    simulation_request.analysis_options = _parse_analysis_options(analysis_options)
    return await _dispatch_submission(simulation_request, background_tasks, extra_pip_deps)


@router.post(
    path="/simulation/run-document",
    operation_id="compose-run-simulation-document",
    response_model=ComposeSimulationExperiment,
    tags=["Compose Simulation"],
    summary="Run a process-bigraph simulation (document as inline JSON)",
)
async def submit_simulation_document(
    background_tasks: BackgroundTasks,
    body: ComposeDocumentSubmission,
) -> ComposeSimulationExperiment:
    """The JSON-body sibling of ``submit_simulation`` — same dispatch, no
    multipart file needed. For a programmatic caller (a CLI, a generated
    client, or a future workbench path) that already holds the document as a
    Python/JSON value, this skips the upload-file round-trip entirely."""
    if body.interval_time < 0 or body.interval_time > MAX_INTERVAL_TIME:
        raise HTTPException(400, f"interval_time must be between 0 and {MAX_INTERVAL_TIME:g}")
    simulation_request = _from_document(body.document, body.batch_submission)
    simulation_request.end_time_point = body.interval_time
    simulation_request.simulator_id = body.simulator_id
    simulation_request.compute_backend = body.compute_backend
    simulation_request.num_nodes = body.num_nodes
    simulation_request.analysis_options = body.analysis_options
    return await _dispatch_submission(simulation_request, background_tasks, body.extra_pip_deps)


# ---------------------------------------------------------------------------
# Results endpoints
# ---------------------------------------------------------------------------


@router.get(
    path="/simulation/{simulation_id}/status",
    operation_id="compose-get-simulation-status",
    response_model=ComposeHpcRun,
    tags=["Compose Results"],
    summary="Get compose simulation job status",
)
async def get_simulation_status(simulation_id: int) -> ComposeHpcRun:
    db = _require_db()
    hpc_run = await db.get_hpc_db().get_hpcrun_by_ref(ref_id=simulation_id, job_type=ComposeJobType.SIMULATION)
    if hpc_run is None:
        raise HTTPException(404, f"Compose simulation {simulation_id} not found")
    return hpc_run


@router.get(
    path="/simulations/status/batch",
    operation_id="compose-get-simulations-status-batch",
    response_model=list[ComposeHpcRun],
    tags=["Compose Results"],
    summary="Batch status lookup for compose simulations",
)
async def get_simulations_status_batch(ids: list[int] = Query()) -> list[ComposeHpcRun]:
    return await _require_db().get_hpc_db().get_hpcruns_by_refs(ref_ids=ids, job_type=ComposeJobType.SIMULATION)


@router.get(
    path="/simulation/{simulation_id}/results",
    operation_id="compose-get-simulation-results",
    tags=["Compose Results"],
    response_model=None,
    summary="Download compose simulation results (zip on SLURM, tar.gz on Ray/Batch)",
    responses={
        200: {
            "content": {"application/zip": {}, "application/gzip": {}},
            "description": "Results archive -- a zip on SLURM (SSH/SCP off the HPC filesystem), "
            "a tar.gz on Ray/Batch (streamed from S3)",
        }
    },
)
async def get_results(simulation_id: int) -> FileResponse | StreamingResponse:
    from viva_api.common.models import JobBackend, SSHTarget
    from viva_api.common.s3_streaming import stream_s3_tar_gz_ray
    from viva_api.common.storage.file_paths import HPCFilePath
    from viva_api.compose.hpc_utils import get_compose_sim_results_path
    from viva_api.dependencies import get_ssh_session_service

    db = _require_db()
    try:
        experiment_id = await db.get_simulator_db().get_simulations_experiment_id(simulation_id=simulation_id)
    except LookupError:
        raise HTTPException(404, f"Compose simulation {simulation_id} not found")

    # Same accessor get_simulation_status uses to resolve the compose sim's HpcRun.
    hpc_run = await db.get_hpc_db().get_hpcrun_by_ref(ref_id=simulation_id, job_type=ComposeJobType.SIMULATION)

    if hpc_run is not None and hpc_run.job_backend != JobBackend.SLURM.value:
        # Ray/Batch: the sim never zips anything -- it writes straight to S3, and a
        # chained analysis job's results (analyses/<name>/analysis.json) land
        # under the SAME experiment prefix (see simulation_service_ray.py's
        # analysis-chaining branch), so streaming every object under
        # RayLayout.experiment_prefix(experiment_id) captures both with a single
        # dispatch -- mirroring get_simulation_outputs's proven Ray dispatch
        # (common/handlers/simulations.py).
        archive_name = f"{experiment_id}.tar.gz"
        return StreamingResponse(
            stream_s3_tar_gz_ray(experiment_id),
            media_type="application/gzip",
            headers={
                "Content-Disposition": f'attachment; filename="{archive_name}"',
                "X-Content-Type-Options": "nosniff",
            },
        )

    # SLURM (or no HpcRun on record, matching the pre-existing default): unchanged
    # SSH/SCP download of the zip the HPC-side job runner built.
    remote_path = get_compose_sim_results_path(experiment_id)

    # Download results from HPC via SCP to a local cache dir
    cache_dir = Path("/app/.results_cache/compose")
    cache_dir.mkdir(parents=True, exist_ok=True)
    local_path = cache_dir / f"{experiment_id}_results.zip"

    if not local_path.exists():
        try:
            async with get_ssh_session_service(SSHTarget.SLURM).session() as ssh:
                await ssh.scp_download(local_file=local_path, remote_path=HPCFilePath(remote_path=remote_path))
        except Exception:
            logger.exception("Failed to download compose results for simulation %s", simulation_id)
            raise HTTPException(502, f"Failed to download results from HPC for simulation {simulation_id}")

    if not local_path.exists():
        raise HTTPException(404, f"Results not available for simulation {simulation_id}")

    return FileResponse(path=str(local_path), filename=f"{experiment_id}_results.zip", media_type="application/zip")


@router.get(
    path="/simulator/{simulator_id}/build/status",
    operation_id="compose-get-simulator-build-status",
    response_model=ComposeHpcRun,
    tags=["Compose Results"],
    summary="Get compose container build status",
)
async def get_simulator_build_status(simulator_id: int) -> ComposeHpcRun:
    hpc_run = (
        await _require_db().get_hpc_db().get_hpcrun_by_ref(ref_id=simulator_id, job_type=ComposeJobType.BUILD_CONTAINER)
    )
    if hpc_run is None:
        raise HTTPException(404, f"Build for simulator {simulator_id} not found")
    return hpc_run


@router.get(
    path="/simulation/{simulation_id}/document",
    operation_id="compose-get-simulation-document",
    tags=["Compose Results"],
    summary="Retrieve the process-bigraph document used for a compose simulation",
    responses={
        200: {"content": {"application/json": {}}, "description": "PBG/SBML document content"},
        404: {"description": "Simulation not found or no document stored"},
    },
)
async def get_simulation_document(simulation_id: int) -> dict:  # type: ignore[type-arg]
    """Return the process-bigraph document (PBG JSON, SBML XML, or OMEX manifest)
    that was uploaded when this simulation was submitted."""
    import json

    db = _require_db()
    doc = await db.get_simulator_db().get_simulation_document(simulation_id)
    if doc is None:
        raise HTTPException(404, f"No document stored for compose simulation {simulation_id}")
    # Try to parse as JSON (PBG files are JSON); fall back to wrapping raw content
    try:
        return json.loads(doc)  # type: ignore[no-any-return]
    except (json.JSONDecodeError, ValueError):
        return {"format": "raw", "content": doc}


# ---------------------------------------------------------------------------
# Compute registry endpoints
# ---------------------------------------------------------------------------


@router.get(
    path="/simulators",
    operation_id="compose-list-simulators",
    response_model=ComposeRegisteredSimulators,
    tags=["Compose Compute"],
    summary="List registered compose simulators",
)
async def list_simulators() -> ComposeRegisteredSimulators:
    return await get_compose_simulator_versions(_require_db())


@router.get(
    path="/processes",
    operation_id="compose-list-processes",
    response_model=list[BiGraphProcess],
    tags=["Compose Compute"],
    summary="List registered process-bigraph processes",
)
async def list_processes() -> list[BiGraphProcess]:
    result: list[BiGraphProcess] = await _require_db().get_package_db().list_all_computes(BiGraphComputeType.PROCESS)
    return result


@router.get(
    path="/steps",
    operation_id="compose-list-steps",
    response_model=list[BiGraphStep],
    tags=["Compose Compute"],
    summary="List registered process-bigraph steps",
)
async def list_steps() -> list[BiGraphStep]:
    result: list[BiGraphStep] = await _require_db().get_package_db().list_all_computes(BiGraphComputeType.STEP)
    return result


# ---------------------------------------------------------------------------
# Curated simulator endpoints
# ---------------------------------------------------------------------------

_TEMPLATES_DIR = os.path.join(os.path.dirname(__file__), "templates")


@router.post(
    path="/curated/copasi",
    operation_id="compose-run-copasi",
    response_model=ComposeSimulationExperiment,
    tags=["Compose Curated"],
    summary="Run COPASI simulation from SBML",
)
async def run_copasi(
    background_tasks: BackgroundTasks, sbml: UploadFile, start_time: float, duration: float, num_data_points: float
) -> ComposeSimulationExperiment:
    from viva_api.config import get_settings

    with open(os.path.join(_TEMPLATES_DIR, "copasi.jinja")) as f:
        render = Template(f.read()).render(
            start_time=start_time,
            duration=duration,
            num_data_points=num_data_points,
            output_dir=get_settings().compose_containers_output_dir,
        )
    request = await _parse_upload(sbml)
    if request.simulation_file_type is not SimulationFileType.SBML:
        raise HTTPException(400, "Expected a SBML file.")
    return await run_compose_curated(
        templated_pbif=render,
        simulator_name="Copasi",
        loaded_sbml=request.request_file_path,
        background_tasks=background_tasks,
        db_service=_require_db(),
        sim_service=_require_sim(),
        job_monitor=_require_monitor(),
    )


@router.post(
    path="/curated/tellurium",
    operation_id="compose-run-tellurium",
    response_model=ComposeSimulationExperiment,
    tags=["Compose Curated"],
    summary="Run Tellurium simulation from SBML",
)
async def run_tellurium(
    background_tasks: BackgroundTasks, sbml: UploadFile, start_time: float, end_time: float, num_data_points: float
) -> ComposeSimulationExperiment:
    from viva_api.config import get_settings

    with open(os.path.join(_TEMPLATES_DIR, "tellurium.jinja")) as f:
        render = Template(f.read()).render(
            start_time=start_time,
            end_time=end_time,
            num_data_points=num_data_points,
            output_dir=get_settings().compose_containers_output_dir,
        )
    request = await _parse_upload(sbml)
    if request.simulation_file_type is not SimulationFileType.SBML:
        raise HTTPException(400, "Expected a SBML file.")
    return await run_compose_curated(
        templated_pbif=render,
        simulator_name="Tellurium",
        loaded_sbml=request.request_file_path,
        background_tasks=background_tasks,
        db_service=_require_db(),
        sim_service=_require_sim(),
        job_monitor=_require_monitor(),
    )


@router.post(
    path="/curated/ecoli",
    operation_id="compose-run-v2ecoli",
    response_model=ComposeSimulationExperiment,
    tags=["Compose Curated"],
    summary="Run a v2ecoli whole-cell simulation via process-bigraph",
)
async def run_v2ecoli(
    background_tasks: BackgroundTasks,
    duration: float = Query(default=60.0, description="Simulation duration in seconds."),
    seed: int = Query(default=0, description="Random seed for stochastic processes."),
    interval: float = Query(default=1.0, description="Execution interval (timestep) in seconds."),
    features: str = Query(default="[]", description="JSON list of feature modules, e.g. '[\"ppgpp_regulation\"]'"),
    cache_dir: str = Query(
        default="/out/cache", description="Absolute path to pre-computed ParCa cache inside container."
    ),
) -> ComposeSimulationExperiment:
    """Run a v2ecoli whole-cell E. coli simulation.

    Unlike Copasi/Tellurium, v2ecoli does not require an SBML upload.
    The biological model is pre-computed in the ParCa cache and the
    55 biological processes are composed at runtime via process-bigraph.
    """
    import json as _json

    from viva_api.compose.handlers import run_compose_v2ecoli
    from viva_api.config import get_settings

    # Parse features list
    try:
        features_list = _json.loads(features)
    except (ValueError, TypeError):
        raise HTTPException(400, f"Invalid features JSON: {features}")

    with open(os.path.join(_TEMPLATES_DIR, "v2ecoli.jinja")) as f:
        render = Template(f.read()).render(
            cache_dir=cache_dir,
            seed=seed,
            interval=interval,
            features=_json.dumps(features_list),
            output_dir=get_settings().compose_containers_output_dir,
        )

    return await run_compose_v2ecoli(
        templated_pbif=render,
        duration=duration,
        background_tasks=background_tasks,
        db_service=_require_db(),
        sim_service=_require_sim(),
        job_monitor=_require_monitor(),
        cache_dir=cache_dir,
        seed=seed,
        features=features_list,
    )


# ---------------------------------------------------------------------------
# BioModels endpoints
# ---------------------------------------------------------------------------


@router.get(
    path="/biomodels/identifiers",
    operation_id="compose-list-biomodel-identifiers",
    response_model=list[str],
    tags=["Compose BioModels"],
    summary="List BioModels database identifiers",
)
async def get_biomodels_identifiers(
    n: int = Query(default=20, ge=1, le=500, description="Max number of identifiers to return."),
) -> list[str]:
    from viva_api.compose.biomodels_service import BiomodelsService

    return BiomodelsService.get_identifiers(n=n)


@router.get(
    path="/biomodels/{biomodel_id}/metadata",
    operation_id="compose-get-biomodel-metadata",
    response_model=BiomodelInfo,
    tags=["Compose BioModels"],
    summary="Get metadata for a BioModels database entry",
)
async def get_biomodel_metadata(biomodel_id: str) -> BiomodelInfo:
    from viva_api.compose.biomodels_service import BiomodelsService

    try:
        meta = BiomodelsService.get_metadata(biomodel_id)
    except Exception as exc:
        raise HTTPException(404, f"BioModel {biomodel_id} not found: {exc}")
    return BiomodelInfo(biomodel_id=biomodel_id, metadata=meta)


async def _submit_biomodel(
    biomodel_id: str,
    steps: dict[str, str],
    background_tasks: BackgroundTasks,
) -> ComposeSimulationExperiment:
    """Load one BioModel, wire the requested simulator(s) into a single PB document,
    and submit it through the curated-compose path. One simulator runs the model on
    it; several are wired into the same document for cross-validation. Raises on a
    load/submit failure so the caller can record the id as failed."""
    import json
    import tempfile

    from viva_api.compose.biomodel_documents import make_biomodel_document
    from viva_api.compose.biomodels_service import BiomodelsService

    stable_dir = Path(tempfile.mkdtemp(prefix=f"biomodel_{biomodel_id}_stable_"))
    result = BiomodelsService.load_biomodel(biomodel_id, stable_dir)
    pb_doc = make_biomodel_document(
        biomodel_id=biomodel_id,
        sbml_path=result.sbml_path,
        utc=result.utc,
        steps=steps,
    )
    label = "+".join(steps)
    return await run_compose_curated(
        templated_pbif=json.dumps(pb_doc),
        simulator_name=f"{biomodel_id}_{label}" if len(steps) > 1 else label.capitalize(),
        loaded_sbml=Path(result.sbml_path),
        background_tasks=background_tasks,
        db_service=_require_db(),
        sim_service=_require_sim(),
        job_monitor=_require_monitor(),
    )


@router.post(
    path="/biomodels/run",
    operation_id="compose-run-biomodels",
    response_model=BiomodelsRunResult,
    tags=["Compose BioModels"],
    summary="Run one or more BioModels through one or more simulators",
)
async def run_biomodels(
    request: BiomodelsRunRequest,
    background_tasks: BackgroundTasks,
) -> BiomodelsRunResult:
    """Single entry point for BioModels runs — subsumes the former
    single/batch/audit/regression endpoints.

    - ``model_ids`` (or the first ``n_models``) selects which models to run.
    - ``simulators``: one runs each model on that simulator; several wire all of
      them into one PB document per model for cross-validation.

    Each model is submitted independently; one that fails to load or submit is
    collected in ``failed`` rather than aborting the run.
    """
    from viva_api.compose.biomodel_documents import COPASI_STEP_ADDRESS, TELLURIUM_STEP_ADDRESS
    from viva_api.compose.biomodels_service import BiomodelsService

    step_addresses = {
        BiomodelSimulator.COPASI: COPASI_STEP_ADDRESS,
        BiomodelSimulator.TELLURIUM: TELLURIUM_STEP_ADDRESS,
    }
    ids = request.model_ids or BiomodelsService.get_identifiers(n=request.n_models or 10)
    steps = {sim.value: step_addresses[sim] for sim in request.simulators}

    submitted: list[ComposeSimulationExperiment] = []
    failed: list[str] = []
    for biomodel_id in ids:
        try:
            submitted.append(await _submit_biomodel(biomodel_id, steps, background_tasks))
        except Exception:
            logger.exception("Failed to submit BioModel %s", biomodel_id)
            failed.append(biomodel_id)

    return BiomodelsRunResult(submitted=submitted, failed=failed, total_requested=len(ids))
