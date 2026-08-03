"""Compose (process-bigraph) simulation router — mounted at /compose/v1/."""

import logging
import os
import tempfile
from pathlib import Path

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, UploadFile
from jinja2 import Template
from starlette.responses import FileResponse

from sms_api.compose.database_service import ComposeDatabaseService
from sms_api.compose.handlers import (
    get_compose_simulator_versions,
    run_compose_curated,
    run_compose_simulation,
)
from sms_api.compose.job_monitor import ComposeJobMonitor
from sms_api.compose.models import (
    DEFAULT_COMPOSE_ALLOW_LIST,
    BiGraphComputeType,
    BiGraphProcess,
    BiGraphStep,
    BiomodelInfo,
    BiomodelsAuditResult,
    BiomodelSimulator,
    BiomodelsRegressionRequest,
    BiomodelsRegressionResult,
    BiomodelsRunRequest,
    BiomodelsRunResult,
    ComposeHpcRun,
    ComposeJobType,
    ComposeRegisteredSimulators,
    ComposeSimulationExperiment,
    ComposeSimulationRequest,
    PBAllowList,
    SimulationFileType,
)
from sms_api.compose.simulation_service import ComposeSimulationService

logger = logging.getLogger(__name__)

router = APIRouter()

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


def _require_sim() -> ComposeSimulationService:
    if _compose_sim_service is None:
        raise HTTPException(500, "Compose simulation service not initialized")
    return _compose_sim_service


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
    extra_pip_deps: list[str] | None = Query(default=None),
) -> ComposeSimulationExperiment:
    if interval_time < 0 or interval_time > 1000:
        raise HTTPException(400, "interval_time must be between 0 and 1000")
    simulation_request = await _parse_upload(uploaded_file, batch_submission)
    simulation_request.end_time_point = interval_time
    db = _require_db()
    allow_list = await db.get_allow_list_db().list_allow_list() or DEFAULT_COMPOSE_ALLOW_LIST
    return await run_compose_simulation(
        simulation_request=simulation_request,
        database_service=db,
        simulation_service=_require_sim(),
        job_monitor=_require_monitor(),
        pb_allow_list=PBAllowList(allow_list=allow_list),
        background_tasks=background_tasks,
        extra_pip_deps=extra_pip_deps,
    )


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
    response_class=FileResponse,
    operation_id="compose-get-simulation-results",
    tags=["Compose Results"],
    summary="Download compose simulation results as zip",
    responses={200: {"content": {"application/zip": {}}, "description": "Results zip file"}},
)
async def get_results(simulation_id: int) -> FileResponse:
    from sms_api.common.models import SSHTarget
    from sms_api.common.storage.file_paths import HPCFilePath
    from sms_api.compose.hpc_utils import get_compose_sim_results_path
    from sms_api.dependencies import get_ssh_session_service

    db = _require_db()
    try:
        experiment_id = await db.get_simulator_db().get_simulations_experiment_id(simulation_id=simulation_id)
    except LookupError:
        raise HTTPException(404, f"Compose simulation {simulation_id} not found")

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
    from sms_api.config import get_settings

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
    from sms_api.config import get_settings

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

    from sms_api.compose.handlers import run_compose_v2ecoli
    from sms_api.config import get_settings

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
    from sms_api.compose.biomodels_service import BiomodelsService

    return BiomodelsService.get_identifiers(n=n)


@router.get(
    path="/biomodels/{biomodel_id}/metadata",
    operation_id="compose-get-biomodel-metadata",
    response_model=BiomodelInfo,
    tags=["Compose BioModels"],
    summary="Get metadata for a BioModels database entry",
)
async def get_biomodel_metadata(biomodel_id: str) -> BiomodelInfo:
    from sms_api.compose.biomodels_service import BiomodelsService

    try:
        meta = BiomodelsService.get_metadata(biomodel_id)
    except Exception as exc:
        raise HTTPException(404, f"BioModel {biomodel_id} not found: {exc}")
    return BiomodelInfo(biomodel_id=biomodel_id, metadata=meta)


@router.post(
    path="/biomodels/{biomodel_id}/run",
    operation_id="compose-run-biomodel",
    response_model=ComposeSimulationExperiment,
    tags=["Compose BioModels"],
    summary="Run a BioModels database model through Copasi or Tellurium",
)
async def run_biomodel(
    biomodel_id: str,
    background_tasks: BackgroundTasks,
    simulator: BiomodelSimulator = Query(default=BiomodelSimulator.COPASI, description="Simulator to use."),
) -> ComposeSimulationExperiment:
    import json
    import tempfile

    from sms_api.compose.biomodel_documents import COPASI_STEP_ADDRESS, TELLURIUM_STEP_ADDRESS, make_biomodel_document
    from sms_api.compose.biomodels_service import BiomodelsService

    stable_dir = Path(tempfile.mkdtemp(prefix=f"biomodel_{biomodel_id}_stable_"))
    try:
        result = BiomodelsService.load_biomodel(biomodel_id, stable_dir)
    except Exception as exc:
        raise HTTPException(400, f"Failed to load BioModel {biomodel_id}: {exc}")

    step_address = COPASI_STEP_ADDRESS if simulator is BiomodelSimulator.COPASI else TELLURIUM_STEP_ADDRESS
    pb_doc = make_biomodel_document(
        biomodel_id=biomodel_id,
        sbml_path=result.sbml_path,
        utc=result.utc,
        steps={simulator.value: step_address},
    )
    simulator_name = simulator.value.capitalize()
    return await run_compose_curated(
        templated_pbif=json.dumps(pb_doc),
        simulator_name=simulator_name,
        loaded_sbml=Path(result.sbml_path),
        background_tasks=background_tasks,
        db_service=_require_db(),
        sim_service=_require_sim(),
        job_monitor=_require_monitor(),
    )


@router.post(
    path="/biomodels/batch",
    operation_id="compose-run-biomodels-batch",
    response_model=BiomodelsRunResult,
    tags=["Compose BioModels"],
    summary="Run a batch of BioModels database models",
)
async def run_biomodels_batch(
    request: BiomodelsRunRequest,
    background_tasks: BackgroundTasks,
) -> BiomodelsRunResult:
    import json
    import tempfile

    from sms_api.compose.biomodel_documents import COPASI_STEP_ADDRESS, TELLURIUM_STEP_ADDRESS, make_biomodel_document
    from sms_api.compose.biomodels_service import BiomodelsService

    ids = request.model_ids or BiomodelsService.get_identifiers(n=request.n_models or 10)
    submitted: list[ComposeSimulationExperiment] = []
    failed: list[str] = []

    for biomodel_id in ids:
        stable_dir = Path(tempfile.mkdtemp(prefix=f"biomodel_{biomodel_id}_stable_"))
        try:
            result = BiomodelsService.load_biomodel(biomodel_id, stable_dir)
        except Exception:
            logger.exception("Failed to load BioModel %s", biomodel_id)
            failed.append(biomodel_id)
            continue

        sim = request.simulator
        step_address = COPASI_STEP_ADDRESS if sim is BiomodelSimulator.COPASI else TELLURIUM_STEP_ADDRESS
        try:
            pb_doc = make_biomodel_document(
                biomodel_id=biomodel_id,
                sbml_path=result.sbml_path,
                utc=result.utc,
                steps={sim.value: step_address},
            )
            simulator_name = sim.value.capitalize()
            experiment = await run_compose_curated(
                templated_pbif=json.dumps(pb_doc),
                simulator_name=simulator_name,
                loaded_sbml=Path(result.sbml_path),
                background_tasks=background_tasks,
                db_service=_require_db(),
                sim_service=_require_sim(),
                job_monitor=_require_monitor(),
            )
            submitted.append(experiment)
        except Exception:
            logger.exception("Failed to submit BioModel %s", biomodel_id)
            failed.append(biomodel_id)

    return BiomodelsRunResult(submitted=submitted, failed=failed)


@router.post(
    path="/biomodels/{biomodel_id}/audit",
    operation_id="compose-audit-biomodel",
    response_model=BiomodelsAuditResult,
    tags=["Compose BioModels"],
    summary="Run a BioModel on multiple simulators for cross-validation",
)
async def audit_biomodel(
    biomodel_id: str,
    background_tasks: BackgroundTasks,
    simulators: list[BiomodelSimulator] = Query(
        default=[BiomodelSimulator.COPASI, BiomodelSimulator.TELLURIUM],
        description="Simulators to run. Both are wired into a single PB document.",
    ),
) -> BiomodelsAuditResult:
    import json
    import tempfile

    from sms_api.compose.biomodel_documents import COPASI_STEP_ADDRESS, TELLURIUM_STEP_ADDRESS, make_biomodel_document
    from sms_api.compose.biomodels_service import BiomodelsService

    stable_dir = Path(tempfile.mkdtemp(prefix=f"biomodel_{biomodel_id}_audit_stable_"))
    try:
        result = BiomodelsService.load_biomodel(biomodel_id, stable_dir)
    except Exception as exc:
        raise HTTPException(400, f"Failed to load BioModel {biomodel_id}: {exc}")

    _STEP_ADDRESSES = {
        BiomodelSimulator.COPASI: COPASI_STEP_ADDRESS,
        BiomodelSimulator.TELLURIUM: TELLURIUM_STEP_ADDRESS,
    }
    steps = {sim.value: _STEP_ADDRESSES[sim] for sim in simulators}
    pb_doc = make_biomodel_document(
        biomodel_id=biomodel_id,
        sbml_path=result.sbml_path,
        utc=result.utc,
        steps=steps,
    )
    experiment = await run_compose_curated(
        templated_pbif=json.dumps(pb_doc),
        simulator_name=f"{biomodel_id}_audit",
        loaded_sbml=Path(result.sbml_path),
        background_tasks=background_tasks,
        db_service=_require_db(),
        sim_service=_require_sim(),
        job_monitor=_require_monitor(),
    )
    return BiomodelsAuditResult(experiment=experiment, simulators_used=simulators)


@router.post(
    path="/biomodels/regression",
    operation_id="compose-biomodels-regression",
    response_model=BiomodelsRegressionResult,
    tags=["Compose BioModels"],
    summary="Run a BioModels regression suite — submit N models, collect results",
)
async def run_biomodels_regression(
    request: BiomodelsRegressionRequest,
    background_tasks: BackgroundTasks,
) -> BiomodelsRegressionResult:
    import json
    import tempfile

    from sms_api.compose.biomodel_documents import COPASI_STEP_ADDRESS, TELLURIUM_STEP_ADDRESS, make_biomodel_document
    from sms_api.compose.biomodels_service import BiomodelsService

    ids = request.model_ids or BiomodelsService.get_identifiers(n=request.n_models)
    total_requested = len(ids)
    submitted: list[ComposeSimulationExperiment] = []
    failed: list[str] = []

    _STEP_ADDRESSES = {
        BiomodelSimulator.COPASI: COPASI_STEP_ADDRESS,
        BiomodelSimulator.TELLURIUM: TELLURIUM_STEP_ADDRESS,
    }
    steps_map = {sim.value: _STEP_ADDRESSES[sim] for sim in request.simulators}

    for biomodel_id in ids:
        stable_dir = Path(tempfile.mkdtemp(prefix=f"biomodel_{biomodel_id}_reg_stable_"))
        try:
            result = BiomodelsService.load_biomodel(biomodel_id, stable_dir)
        except Exception:
            logger.exception("Failed to load BioModel %s in regression run", biomodel_id)
            failed.append(biomodel_id)
            continue

        try:
            pb_doc = make_biomodel_document(
                biomodel_id=biomodel_id,
                sbml_path=result.sbml_path,
                utc=result.utc,
                steps=steps_map,
            )
            sim_names = "+".join(s.value for s in request.simulators)
            experiment = await run_compose_curated(
                templated_pbif=json.dumps(pb_doc),
                simulator_name=f"{biomodel_id}_{sim_names}",
                loaded_sbml=Path(result.sbml_path),
                background_tasks=background_tasks,
                db_service=_require_db(),
                sim_service=_require_sim(),
                job_monitor=_require_monitor(),
            )
            submitted.append(experiment)
        except Exception:
            logger.exception("Failed to submit BioModel %s in regression run", biomodel_id)
            failed.append(biomodel_id)

    return BiomodelsRegressionResult(submitted=submitted, failed=failed, total_requested=total_requested)
