import asyncio
import gzip
import io
import json
import logging
import os
import random
import string
import tarfile
import uuid
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

from fastapi import BackgroundTasks, HTTPException
from fastapi.responses import FileResponse, Response, StreamingResponse

from viva_api.analysis.models import TsvOutputFile
from viva_api.common import StrEnumBase
from viva_api.common.handlers.simulators import upload_simulator
from viva_api.common.hpc.job_service import JobStatusUpdate
from viva_api.common.models import JobBackend, JobStatus, SSHTarget
from viva_api.common.simulator_defaults import DEFAULT_OBSERVABLES, RepoUrl
from viva_api.common.storage import data_layout
from viva_api.common.storage.file_paths import HPCFilePath, S3FilePath
from viva_api.config import ComputeBackend, compute_backend_for_repo, get_job_backend, get_settings
from viva_api.dependencies import (
    get_database_service,
    get_file_service,
    get_simulation_service,
    get_simulation_service_for_job,
    get_simulation_service_for_repo,
    get_ssh_session_service,
)
from viva_api.simulation.database_service import DatabaseService
from viva_api.simulation.hpc_utils import get_correlation_id
from viva_api.simulation.models import (
    AnalysisOptions,
    ChainProgress,
    CompositeEngine,
    HpcRun,
    JobType,
    ParcaDataset,
    ParcaDatasetRequest,
    ParcaOptions,
    Simulation,
    SimulationConfig,
    SimulationRequest,
    SimulationRun,
    SimulatorVersion,
    VecoliSource,
)
from viva_api.simulation.simulation_service import SimulationService
from viva_api.simulation.simulation_service_ray import SimulationServiceRay
from viva_api.simulation.tables_orm import AnalysisStatusDB

logger = logging.getLogger(__name__)


REPO_DIR = Path(__file__).parent.parent.parent.parent.absolute()
DEBUG_ARTIFACTS_DIR = REPO_DIR / "artifacts"
DEFAULT_SIMDATA_PATH = get_settings().hpc_parca_base_path / "default" / "kb" / "simData.cPickle"
# DEFAULT_SIMDATA_PATH = REPO_DIR / "assets" / "simData.cPickle"  # or, keep a remote copy (run parca along with
# repo/image build)


ANALYSIS_CATEGORIES = {"single", "multiseed", "multigeneration", "multidaughter", "multivariant", "multiexperiment"}


# -- ecoli-sources server-side sync --

# Only GitHub repos under these orgs are allowed for server-side source sync.
_ALLOWED_SOURCE_ORGS = {"vivarium-collective", "CovertLab", "CovertLabEcoli"}

# Required columns in data/manifest.tsv (ecoli-sources convention).
_MANIFEST_REQUIRED_COLUMNS = {"dataset_id", "file_path"}

# Safety limits
_MAX_TARBALL_BYTES = 500 * 1024 * 1024  # 500 MB
_MAX_FILE_COUNT = 10_000
_SKIP_DIRS = {".git", "__pycache__", ".venv", "node_modules", ".tox"}
_SKIP_EXTENSIONS = {".pyc", ".pyo", ".so", ".dylib", ".exe"}


def _parse_github_owner_repo(repo_url: str) -> tuple[str, str, str]:
    """Parse a GitHub URL into (owner, repo, owner/repo). Validates against allowed orgs."""
    parts = repo_url.rstrip("/").rstrip(".git").split("/")
    if len(parts) < 2 or "github.com" not in repo_url:
        raise HTTPException(status_code=400, detail=f"Invalid GitHub repo URL: {repo_url}")
    owner, repo = parts[-2], parts[-1]
    if owner not in _ALLOWED_SOURCE_ORGS:
        raise HTTPException(
            status_code=403,
            detail=f"Source repo org '{owner}' is not in the allowed list: {sorted(_ALLOWED_SOURCE_ORGS)}. "
            f"Only repos from trusted organizations can be synced server-side.",
        )
    return owner, repo, f"{owner}/{repo}"


def _validate_manifest(source_root: str) -> None:
    """Validate that data/manifest.tsv exists and has the required columns."""
    import csv

    manifest_path = os.path.join(source_root, "data", "manifest.tsv")
    if not os.path.isfile(manifest_path):
        raise HTTPException(
            status_code=400,
            detail="Source repo is missing data/manifest.tsv. "
            "The ecoli-sources format requires a TSV manifest at data/manifest.tsv "
            "with at least columns: dataset_id, file_path.",
        )
    with open(manifest_path) as f:
        reader = csv.reader(f, delimiter="\t")
        try:
            header = next(reader)
        except StopIteration:
            raise HTTPException(status_code=400, detail="data/manifest.tsv is empty.") from None
    header_set = {col.strip() for col in header}
    missing = _MANIFEST_REQUIRED_COLUMNS - header_set
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"data/manifest.tsv is missing required columns: {sorted(missing)}. Found: {sorted(header_set)}.",
        )


def _safe_s3_key(prefix: str, relpath: str) -> str:
    """Build an S3 key from a prefix and relative path, rejecting traversal attempts."""
    # Normalize and reject any '..' components
    normalized = os.path.normpath(relpath)
    if ".." in normalized.split(os.sep):
        raise HTTPException(status_code=400, detail=f"Path traversal detected in source file: {relpath}")
    return f"{prefix}/{normalized}"


def _upload_source_tree(s3: Any, bucket: str, s3_prefix: str, source_root: str) -> int:
    """Walk source_root and upload files to S3, skipping unwanted dirs/extensions."""
    upload_count = 0
    for dirpath, _dirnames, filenames in os.walk(source_root):
        rel_dir = os.path.relpath(dirpath, source_root)
        if any(skip in rel_dir.split(os.sep) for skip in _SKIP_DIRS):
            continue
        for filename in filenames:
            if any(filename.endswith(ext) for ext in _SKIP_EXTENSIONS):
                continue
            if upload_count >= _MAX_FILE_COUNT:
                raise HTTPException(
                    status_code=413,
                    detail=f"Source repo exceeds {_MAX_FILE_COUNT} files. Is this the right repo?",
                )
            local_path = os.path.join(dirpath, filename)
            s3_key = _safe_s3_key(s3_prefix, os.path.relpath(local_path, source_root))
            s3.upload_file(local_path, bucket, s3_key)
            upload_count += 1
    return upload_count


async def _sync_ecoli_sources_from_github(
    repo_url: str,
    ref: str,
    settings: Any,
) -> str:
    """Download an ecoli-sources repo from GitHub and upload to S3.

    Validates:
    - Repo org is in the allowed list
    - Tarball size is within limits
    - data/manifest.tsv exists with required columns
    - No path traversal in uploaded keys
    - File count within limits

    Only available on K8s/Batch backend (stanford-test).
    Returns the S3 URI to use as ECOLI_SOURCES.
    """
    import tempfile

    import boto3
    import httpx

    # Guard: only allowed on K8s/Batch backend
    backend = get_job_backend()
    if backend != ComputeBackend.BATCH:
        raise HTTPException(
            status_code=400,
            detail="Server-side ecoli-sources sync is only available on the K8s/Batch backend (stanford-test). "
            "Use --sources (local sync) for SLURM deployments.",
        )

    owner, repo_basename, owner_repo = _parse_github_owner_repo(repo_url)

    # Download tarball from GitHub
    tarball_url = f"https://api.github.com/repos/{owner_repo}/tarball/{ref}"
    headers: dict[str, str] = {"Accept": "application/vnd.github+json"}
    if settings.github_token:
        headers["Authorization"] = f"token {settings.github_token}"

    logger.info("Downloading ecoli-sources from %s (ref=%s)", owner_repo, ref)
    async with httpx.AsyncClient(follow_redirects=True, timeout=120) as client:
        resp = await client.get(tarball_url, headers=headers)
        if resp.status_code != 200:
            raise HTTPException(
                status_code=resp.status_code,
                detail=f"Failed to download tarball from {tarball_url}: HTTP {resp.status_code}",
            )

    # Validate tarball size
    if len(resp.content) > _MAX_TARBALL_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"Source repo tarball is {len(resp.content) / 1024 / 1024:.0f} MB, "
            f"exceeds limit of {_MAX_TARBALL_BYTES / 1024 / 1024:.0f} MB.",
        )

    bucket = settings.s3_work_bucket or settings.storage_s3_bucket
    if not bucket:
        raise HTTPException(status_code=500, detail="No S3 bucket configured for ecoli-sources sync")

    s3_prefix = f"sources/{repo_basename}/{ref}"
    s3 = boto3.client("s3", region_name=settings.storage_s3_region or settings.batch_region or "us-gov-west-1")

    with tempfile.TemporaryDirectory() as tmpdir:
        tar_bytes = io.BytesIO(resp.content)
        with tarfile.open(fileobj=tar_bytes, mode="r:gz") as tar:
            # Safety: reject tar members with absolute paths or traversal
            for member in tar.getmembers():
                if member.name.startswith("/") or ".." in member.name.split("/"):
                    raise HTTPException(status_code=400, detail=f"Unsafe path in tarball: {member.name}")
            tar.extractall(tmpdir)  # noqa: S202

        # GitHub tarballs have a top-level dir like "owner-repo-hash/"
        extracted_dirs = os.listdir(tmpdir)
        source_root = os.path.join(tmpdir, extracted_dirs[0]) if len(extracted_dirs) == 1 else tmpdir

        # Validate manifest
        _validate_manifest(source_root)

        # Upload files to S3
        upload_count = _upload_source_tree(s3, bucket, s3_prefix, source_root)

    s3_uri = f"s3://{bucket}/{s3_prefix}"
    logger.info("Uploaded %d files from %s to %s", upload_count, owner_repo, s3_uri)
    return s3_uri


def _validate_analysis_options(analysis_options: AnalysisOptions, available_modules: dict[str, list[str]]) -> None:
    """Validate user-specified analysis modules against what exists in the repo.

    Raises HTTPException(400) with a clear message if any module is not found.
    """
    opts = analysis_options.model_dump()
    for category, modules in opts.items():
        if category not in ANALYSIS_CATEGORIES or not isinstance(modules, dict):
            continue
        available = available_modules.get(category, [])
        if not available:
            continue  # Can't validate if discovery didn't return this category
        for module_name in modules:
            if module_name not in available:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"Analysis module '{module_name}' not found in category '{category}'. "
                        f"Available {category} modules: {', '.join(available)}. "
                        f"Use GET /api/v1/simulations/discovery?simulator_id=<id> to list all."
                    ),
                )


class SimulationAnalysisResponseType(StrEnumBase):
    FILE = "application/octet-stream"
    DATA_CONTENT = "application/octet-stream"
    STREAMING_JSON = "application/json"
    TSV = "text/tab-separated-values"
    ZIP_STREAM = "application/zip"
    GZIP_STREAM = "application/gzip"
    TAR_GZIP_STREAM = "application/gzip"


class SimulationAnalysisDataResponseType(StrEnumBase):
    """Response type for simulation data endpoint."""

    STREAMING = "streaming"
    FILE = "file"


def export_baseline_config(request: SimulationRequest) -> None:
    """Capture simulation config to disk for debugging/inspection.

    Writes the config to the artifacts/ directory at repo root.
    This directory is gitignored and used for debugging purposes only.
    """
    DEBUG_ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    config_path = DEBUG_ARTIFACTS_DIR / f"workflow_config__{get_settings().deployment_namespace}.json"
    with open(config_path, "w") as fp:
        json.dump(request.config.model_dump(), fp, indent=3)


async def run_workflow_legacy(
    request: SimulationRequest,
    simulation_service: SimulationService,
    database_service: DatabaseService,
) -> Simulation:
    """
    Legacy workflow execution that supports uploading a new simulator.

    Parameterizes and executes a "full" e2e sms-api vEcoli workflow
    (simulator -> parca ref -> Simulation(parca -> variants -> simulation -> analyses)

    For new code, prefer run_workflow() with an existing simulator_id.
    """
    export_baseline_config(request)

    # 1. upload simulator if needed
    if request.simulator_id is not None:
        simulator = await database_service.get_simulator(request.simulator_id)
    else:
        simulator = await upload_simulator(
            commit_hash=request.simulator.git_commit_hash,  # type: ignore[union-attr]
            git_branch=request.simulator.git_branch,  # type: ignore[union-attr]
            git_repo_url=request.simulator.git_repo_url,  # type: ignore[union-attr]
            database_service=database_service,
            simulation_service_slurm=simulation_service,
        )
    # 2. create parca ds reference for the simData that will be generated by this request
    # TODO: use config hash to check if exists first
    parca_config = request.config.parca_options.model_dump()
    parca_ds = await database_service.insert_parca_dataset(
        parca_dataset_request=ParcaDatasetRequest(simulator_version=simulator, parca_config=parca_config)  # type: ignore[arg-type]
    )
    request.parca_dataset_id = parca_ds.database_id

    # 3. run simulation (save to db and insert hpc run)
    simulation = await database_service.insert_simulation(sim_request=request)
    random_string_7_hex = "".join(random.choices(string.hexdigits, k=7))
    correlation_id = get_correlation_id(
        ecoli_simulation=simulation,
        random_string=random_string_7_hex,
        simulator=simulator,  # type: ignore[arg-type]
    )
    job_id = await simulation_service.submit_ecoli_simulation_job(
        ecoli_simulation=simulation, database_service=database_service, correlation_id=correlation_id
    )
    # Some dispatch shapes (chain-dispatch campaigns — backlog item 33) already
    # record their OWN HpcRun row internally, using this SAME correlation_id,
    # because that row needs fields (chain_n_generations/chain_final_job_ids) a
    # generic caller has no way to populate. Guard against inserting a SECOND,
    # generic row on top of it: get_hpcrun_by_ref (used by every real status
    # endpoint) resolves to whichever row was inserted MOST RECENTLY, so a
    # blind second insert here would permanently shadow the real, pollable
    # campaign row with a stale one nobody ever updates. For every OTHER
    # dispatch shape this is a pure no-op (a freshly-generated correlation_id
    # can never already have a row), so behavior is unchanged for them.
    if await database_service.get_hpcrun_id_by_correlation_id(correlation_id=correlation_id) is None:
        _ = await database_service.insert_hpcrun(
            job_id=job_id,
            job_type=JobType.SIMULATION,
            ref_id=simulation.database_id,
            correlation_id=correlation_id,
        )

    simulation.job_id = str(job_id)
    return simulation


async def _verify_build_complete(database_service: DatabaseService, simulator_id: int) -> None:
    """Raise ValueError if the simulator build is still running or failed."""
    build_run = await database_service.get_hpcrun_by_ref(ref_id=simulator_id, job_type=JobType.BUILD_IMAGE)
    if build_run is None:
        return
    if build_run.status in (JobStatus.RUNNING, JobStatus.PENDING):
        raise ValueError(
            f"Simulator {simulator_id} build is still in progress (status: {build_run.status.value}). "
            "Wait for the build to complete before submitting a simulation."
        )
    if build_run.status == JobStatus.FAILED:
        raise ValueError(
            f"Simulator {simulator_id} build failed: {build_run.error_message or 'unknown error'}. "
            "Re-upload the simulator to retry."
        )


# TODO: formalize config template overwrite logic to favor dataclasses over dict mutation


async def run_simulation_workflow(  # noqa: C901
    database_service: DatabaseService,
    simulation_service: SimulationService,
    simulator_id: int,
    experiment_id: str,
    simulation_config_filename: str,
    num_generations: int | None = None,
    num_seeds: int | None = None,
    composite: CompositeEngine | None = None,
    condition: str | None = None,
    max_generations: int | None = None,
    vecoli_source: VecoliSource | None = None,
    description: str | None = None,
    run_parca: bool | None = None,
    observables: list[str] | None = None,
    analysis_options: AnalysisOptions | None = None,
    ecoli_sources_uri: str | None = None,
    ecoli_sources_overlays: str | None = None,
    ecoli_sources_repo_url: str | None = None,
    ecoli_sources_ref: str | None = None,
    tags: list[str] | None = None,
) -> Simulation:
    """
    Simplified workflow execution with just the essential parameters.

    This assumes the simulator already exists in the database. The workflow
    configuration is read from the vEcoli repo on the HPC system, and parca
    execution is handled as part of the workflow.

    Args:
        database_service: Database service instance
        simulation_service: Simulation service instance
        simulator_id: Database ID of the simulator to use (must exist)
        experiment_id: Unique experiment identifier
        simulation_config_filename: Name of the config file in vEcoli/configs/ on HPC
        num_generations: Number of generations to simulate (optional, overrides config)
        num_seeds: Number of initial seeds/lineages (optional, overrides config)
        description: Description of the simulation (optional)
        run_parca: If `True`, the simulation parameter calculator is run prior to simulation execution, otherwise
            a cached "default" simulation parameter dataset is used.
        observables: a flat, list of strings representing dot-delimited hierarchical paths within the vEcoli output, for
            otherwise comma-delimited hierarchical paths to exclusively include in the output reporting.
        analysis_options: Analysis options specific to the vecoli workflow API, corresponding to specific existing
            analysis modules in the vecoli repo.
    """
    if run_parca is None:
        run_parca = True

    settings = get_settings()

    # 1. Get the simulator (must exist) and resolve its backend FROM THE REPO so one
    # deployment can serve both vecoli (Batch/Nextflow) and v2ecoli (Ray). `backend` drives
    # the config branches below; `service` is the backend's SimulationService.
    simulator = await database_service.get_simulator(simulator_id)
    if simulator is None:
        raise ValueError(f"Simulator with id {simulator_id} not found")
    backend = compute_backend_for_repo(simulator.git_repo_url) or get_job_backend()
    service = get_simulation_service_for_repo(simulator.git_repo_url) or simulation_service

    # Batch backend requires parca to run: vEcoli's workflow.py resolves sim_data_path with
    # os.path.abspath which mangles S3 URIs and passes the local kb_dir into Nextflow
    # channels Batch task containers can't reach. Force parca on Batch.
    if not run_parca and backend == ComputeBackend.BATCH:
        logger.warning("Forcing run_parca=True: --no-run-parca is not supported on the Batch backend")
        run_parca = True

    # Verify simulator build is complete before submitting simulation
    await _verify_build_complete(database_service, simulator_id)

    # 1b. Validate analysis_options against what exists in the repo (if user specified them)
    if analysis_options is not None:
        try:
            discovery = await service.discover_repo_contents(simulator)
            if discovery.analysis_modules:
                _validate_analysis_options(analysis_options, discovery.analysis_modules)
        except HTTPException:
            raise
        except Exception:
            # Discovery failure should not block the workflow — log and continue
            logger.warning("Could not validate analysis_options against repo (discovery failed), proceeding anyway")

    # 2. Read the config template via the resolved service (SSH for SLURM, GitHub API for K8s/Ray).
    # v2ecoli (RAY) has no configs/ dir and runs from CLI args, so fall back to the embedded
    # default template instead of 404-ing when the requested config file isn't in the repo.
    config_str = await service.read_config_template(
        simulator_version=simulator,
        config_filename=simulation_config_filename,
        allow_default_fallback=(backend == ComputeBackend.RAY),
    )

    # 3. Replace placeholders in the config template

    unique_experiment_id = f"sim{simulator.database_id}-{experiment_id}-{str(uuid.uuid4())[:4]}"
    config_str = config_str.replace("EXPERIMENT_ID_PLACEHOLDER", unique_experiment_id)
    config_str = config_str.replace("HPC_SIM_BASE_PATH_PLACEHOLDER", str(settings.hpc_sim_base_path))
    image_path = get_settings().hpc_image_base_path / f"vecoli-{simulator.git_commit_hash}.sif"
    config_str = config_str.replace("SIMULATOR_IMAGE_PATH_PLACEHOLDER", str(image_path))
    config_data = json.loads(config_str)

    # 3b. Ensure required fields exist (vanilla vEcoli configs may lack API placeholders)
    config_data.setdefault("experiment_id", unique_experiment_id)
    if config_data.get("experiment_id") is None:
        config_data["experiment_id"] = unique_experiment_id
    config_data.setdefault("emitter", "parquet")
    config_data.setdefault("emitter_arg", {})
    # Always ensure emitter_arg.out_dir points to the HPC output path (vanilla configs use relative "out")
    if config_data.get("emitter_arg", {}).get("out_dir") in (None, "", "out"):
        config_data["emitter_arg"]["out_dir"] = str(settings.hpc_sim_base_path)
    config_data.setdefault("analysis_options", {"multiseed": {}})
    config_data.setdefault("single_daughters", True)
    config_data.setdefault("suffix_time", False)
    # Ensure parca_options.outdir points to HPC path (vanilla configs use relative "out")
    if "parca_options" in config_data:
        parca_outdir = config_data["parca_options"].get("outdir", "")
        if not parca_outdir or parca_outdir == "out":
            config_data["parca_options"]["outdir"] = str(settings.hpc_sim_base_path)
    else:
        config_data["parca_options"] = {"outdir": str(settings.hpc_sim_base_path), "cpus": 6}

    # 4. Override config values if provided
    if num_generations is not None:
        config_data["generations"] = num_generations
    if num_seeds is not None:
        config_data["n_init_sims"] = num_seeds
    # Two-engine comparison knobs (Ray backend): when `composite` is set the Ray
    # sim job runs scripts/run_comparison_ensemble.py instead of the phase0
    # ensemble. Validated at the API boundary (Literal Query params → 422 on a
    # typo); they ride through SimulationConfig as extra passthrough keys (set only
    # when provided) so they never inject our defaults into the vEcoli solver
    # config. The Ray backend reads them via getattr.
    if composite is not None:
        config_data["composite"] = composite
    if condition is not None:
        config_data["condition"] = condition
    if max_generations is not None:
        config_data["max_generations"] = max_generations
    if vecoli_source is not None:
        config_data["vecoli_source"] = vecoli_source
    if description is not None:
        config_data["description"] = description
    effective_observables = observables if observables else DEFAULT_OBSERVABLES
    config_data["engine_process_reports"] = [obs.split(".") for obs in effective_observables]
    # For Batch backend, override HPC paths with AWS equivalents
    if backend == ComputeBackend.BATCH:
        s3_output = data_layout.NextflowLayout.output_uri(unique_experiment_id)
        config_data["emitter_arg"] = {"out_uri": s3_output}
        config_data.pop("aws_cdk", None)
        config_data.pop("ccam", None)
        config_data["progress_bar"] = False
        if "parca_options" in config_data:
            config_data["parca_options"]["outdir"] = s3_output
        # Use short image name (repo:tag) — workflow.py resolves the full ECR URI
        # via build-and-push-ecr.sh -u at runtime
        config_data["aws"] = {
            "build_image": False,
            "container_image": f"{settings.ecr_repository}:{simulator.git_commit_hash}-{settings.batch_task_arch}",
            "region": settings.batch_region,
            "batch_queue": settings.batch_arm64_queue
            if settings.batch_task_arch == "arm64"
            else settings.batch_amd64_queue,
        }
        if not run_parca:
            # Set local path for cached simData — the K8s job command will download
            # from S3 to this path before workflow.py runs (vEcoli only accepts local paths)
            config_data["sim_data_path"] = "/tmp/simData.cPickle"  # noqa: S108
        else:
            # run_parca=True: ParCa runs IN-WORKFLOW and produces sim_data. The base
            # vEcoli config.template ships sim_data_path=out/kb/simData.cPickle, and
            # workflow.py's generate_code() treats a non-None sim_data_path as
            # PRE-EXISTING and hashes it BEFORE ParCa runs → FileNotFoundError on the
            # nonexistent default (the Nextflow head pod dies immediately). POPPING the
            # key lets the config.template default win, so EXPLICITLY set None — the
            # documented "null = run parca" signal (see submit_ecoli_simulation_job) —
            # so the workflow.json override nulls it and generate_code runs ParCa first.
            config_data["sim_data_path"] = None
    elif backend == ComputeBackend.RAY:
        # Ray backend: the v2ecoli ensemble runs from CLI args on a transient Ray
        # cluster (not Nextflow), so the Nextflow/AWS config blocks are unused.
        # Keep experiment_id / generations / n_init_sims (→ --n-seeds); record the
        # S3 results prefix (xarray zarr + summary) for output retrieval.
        config_data.pop("aws_cdk", None)
        config_data.pop("ccam", None)
        config_data.pop("aws", None)
        config_data["emitter"] = "xarray"
        config_data["emitter_arg"] = {"out_uri": data_layout.NextflowLayout.output_uri(unique_experiment_id)}
    else:
        # SLURM path: replace K8s-specific sections with SLURM equivalents.
        # The ccam Nextflow profile only exists in the fork (api-support branch)
        # and the private repo — not in the public CovertLab/vEcoli repo.
        config_data.pop("aws_cdk", None)
        config_data.pop("aws", None)
        _ccam_repos = {RepoUrl.VECOLI_FORK_REPO_URL, RepoUrl.VECOLI_PRIVATE_REPO_URL}
        if simulator.git_repo_url not in _ccam_repos:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Simulator {simulator.database_id} ({simulator.git_repo_url} @ {simulator.git_branch}) "
                    f"cannot run on SLURM — the ccam Nextflow profile is only available in the fork "
                    f"(vivarium-collective/vEcoli @ api-support) or the private repo. "
                    f"Build a simulator from one of those repos, or use the stanford-test (K8s) backend."
                ),
            )
        image_path_str = str(get_settings().hpc_image_base_path / f"vecoli-{simulator.git_commit_hash}.sif")
        config_data["ccam"] = {"build_image": False, "container_image": image_path_str}
        if not run_parca:
            config_data["sim_data_path"] = DEFAULT_SIMDATA_PATH.__str__()

    # Default analysis modules depend on the simulator's source repo:
    # cd1_* modules only exist in the private vEcoli repo, so public-repo
    # simulators get an empty default that users can override via --analysis-options.
    if analysis_options is not None:
        specified_analyses = analysis_options.model_dump()
    elif simulator.git_repo_url == RepoUrl.VECOLI_PRIVATE_REPO_URL:
        specified_analyses = {
            "multiseed": {
                "cd1_metabolomics": {"generation_lower_bound": 5},
                "cd1_transcriptomics": {"generation_lower_bound": 5},
                "cd1_higher_order_properties": {"generation_lower_bound": 5},
                "cd1_fluxomics": {"generation_lower_bound": 5},
                "cd1_proteomics": {"generation_lower_bound": 5},
            }
        }
    else:
        specified_analyses = {"multiseed": {}}

    config_data["analysis_options"] = specified_analyses
    # The fork repo's workflow.py expects analysis_options.memory_gb
    if simulator.git_repo_url == RepoUrl.VECOLI_FORK_REPO_URL:
        config_data["analysis_options"].setdefault("memory_gb", 3)

    # Server-side ecoli-sources sync: download GitHub repo tarball and upload to S3.
    # This allows CLI users to pass --sources-repo without needing local AWS CLI.
    if ecoli_sources_repo_url and ecoli_sources_uri is None:
        ecoli_sources_uri = await _sync_ecoli_sources_from_github(
            repo_url=ecoli_sources_repo_url,
            ref=ecoli_sources_ref or "main",
            settings=settings,
        )

    # Optional: data-source env var pointers for the simulation container.
    # The K8s Job picks these up to set ECOLI_SOURCES / ECOLI_SOURCES_OVERLAYS,
    # so configs referencing $ECOLI_SOURCES resolve to the synced S3 URI.
    if ecoli_sources_uri is not None:
        config_data["ecoli_sources_uri"] = ecoli_sources_uri
    if ecoli_sources_overlays is not None:
        config_data["ecoli_sources_overlays"] = ecoli_sources_overlays

    config = SimulationConfig(**config_data)

    # 5. Create placeholder parca dataset entry
    # Even though parca runs as part of the Nextflow workflow, we need a database entry
    # to satisfy the simulation's foreign key constraint and track the parca config.
    parca_config = config.parca_options.model_dump()
    parca_ds = await database_service.insert_parca_dataset(
        parca_dataset_request=ParcaDatasetRequest(simulator_version=simulator, parca_config=parca_config)  # type: ignore[arg-type]
    )

    # 6. Create SimulationRequest and insert simulation
    request = SimulationRequest(
        config=config,
        simulator_id=simulator_id,
        parca_dataset_id=parca_ds.database_id,
        simulation_config_filename=simulation_config_filename,
        experiment_id=unique_experiment_id,
        tags=tags or [],
    )
    export_baseline_config(request)
    simulation = await database_service.insert_simulation(sim_request=request)

    # 7. Generate correlation ID and submit job (via the per-simulator backend service)
    random_string_7_hex = "".join(random.choices(string.hexdigits, k=7))
    correlation_id = get_correlation_id(
        ecoli_simulation=simulation,
        random_string=random_string_7_hex,
        simulator=simulator,
    )
    job_id = await service.submit_ecoli_simulation_job(
        ecoli_simulation=simulation, database_service=database_service, correlation_id=correlation_id
    )

    # 8. Record HPC run -- unless the dispatch shape already recorded its OWN
    # row internally under this SAME correlation_id (chain-dispatch campaigns,
    # backlog item 33: that row carries chain_n_generations/chain_final_job_ids,
    # fields this generic call site has no way to populate). A blind second
    # insert here would outrank it in get_hpcrun_by_ref (every real status
    # endpoint's lookup resolves to the MOST RECENTLY inserted row for a given
    # ref_id), permanently shadowing the real, pollable campaign row with a
    # stale one nobody ever updates. For every other dispatch shape this check
    # is a pure no-op (a freshly-generated correlation_id can't already have a
    # row), so behavior is unchanged for them.
    if await database_service.get_hpcrun_id_by_correlation_id(correlation_id=correlation_id) is None:
        _ = await database_service.insert_hpcrun(
            job_id=job_id,
            job_type=JobType.SIMULATION,
            ref_id=simulation.database_id,
            correlation_id=correlation_id,
        )

    simulation.job_id = str(job_id)
    return simulation


async def run_parca(
    simulator: SimulatorVersion,
    simulation_service_slurm: SimulationService | None = None,
    database_service: DatabaseService | None = None,
    parca_config: ParcaOptions | None = None,
) -> ParcaDataset:
    if not simulation_service_slurm:
        # Route to the simulator's backend (v2ecoli→Ray, vEcoli→Batch), default otherwise.
        simulation_service_slurm = get_simulation_service_for_repo(simulator.git_repo_url)
    if simulation_service_slurm is None:
        logger.exception("Simulation service is not initialized")
        raise HTTPException(status_code=404, detail="Simulation service is not initialized")
    if not database_service:
        database_service = get_database_service()
    if database_service is None:
        logger.exception("Simulation database service is not initialized")
        raise HTTPException(status_code=404, detail="Simulation database service is not initialized")

    parca_dataset_request = ParcaDatasetRequest(
        simulator_version=simulator, parca_config=parca_config or ParcaOptions()
    )
    parca_dataset = await database_service.insert_parca_dataset(parca_dataset_request=parca_dataset_request)

    # Submit parca job
    parca_job_id = await simulation_service_slurm.submit_parca_job(parca_dataset=parca_dataset)
    _hpc_run = await database_service.insert_hpcrun(
        job_id=parca_job_id,
        job_type=JobType.PARCA,
        ref_id=parca_dataset.database_id,
        correlation_id="N/A",
    )

    return parca_dataset


async def get_parca_datasets(
    simulation_service_slurm: SimulationService | None = None,
    database_service: DatabaseService | None = None,
) -> list[ParcaDataset]:
    if not simulation_service_slurm:
        simulation_service_slurm = get_simulation_service()
    if simulation_service_slurm is None:
        logger.exception("Simulation service is not initialized")
        raise HTTPException(status_code=404, detail="Simulation service is not initialized")
    if not database_service:
        database_service = get_database_service()
    if database_service is None:
        logger.exception("Simulation database service is not initialized")
        raise HTTPException(status_code=404, detail="Simulation database service is not initialized")

    parca_datasets = await database_service.list_parca_datasets()
    return parca_datasets


async def get_simulation(db_service: DatabaseService, id: int) -> Simulation | None:
    return await db_service.get_simulation(simulation_id=id)


async def get_simulation_status(db_service: DatabaseService, id: int) -> SimulationRun:
    sim_record = await db_service.get_simulation(simulation_id=id)
    if sim_record is None:
        raise ValueError(f"Simulation with id {id} not found.")

    # Get the HpcRun record for this simulation to find the job ID
    hpc_run = await db_service.get_hpcrun_by_ref(ref_id=id, job_type=JobType.SIMULATION)
    if hpc_run is None:
        raise RuntimeError(f"No HPC run found for simulation {id}")

    # Chain-dispatch campaigns (backlog item 33, reworked by item 71 Phase 4)
    # track N per-seed job chains, not one job -- `hpc_run.job_id` is only the
    # ParCa kickoff job. The plain path below reads THAT job's status and
    # writes it onto the whole campaign row, which corrupts it: ParCa
    # finishing legitimately marks the entire campaign COMPLETED while the
    # real per-seed chains are still mid-flight. Report the campaign row's OWN
    # status directly instead: under Phase 4, `JobScheduler._advance_chain_campaign`
    # (via `DatabaseService.advance_chain_campaign`) is the ONLY writer of this
    # row's status, and it transitions it exactly once, on the same tick it
    # confirms every seed has resolved -- so `hpc_run.status` is always
    # current, the same trust the non-campaign path below places in a
    # freshly-polled job's status. Do NOT re-derive terminal-ness here by
    # calling `get_chain_campaign_result` on `chain_final_job_ids`: that list
    # is now filled INCREMENTALLY (one entry per seed, only once THAT seed's
    # own job is already known-terminal), so a describe_jobs check against
    # whatever PARTIAL subset has resolved so far would always report
    # "terminal" for that subset -- unable to distinguish "3 of 1000 seeds
    # done" from "campaign complete." Never write here; only the scheduler
    # may transition a campaign row's status.
    if hpc_run.chain_final_job_ids is not None:
        return SimulationRun(
            id=int(id), status=hpc_run.status or JobStatus.RUNNING, error_message=hpc_run.error_message
        )

    # Route to the service that owns this run (by the run's backend), not the global default.
    simulation_service = get_simulation_service_for_job(hpc_run.job_id)
    if simulation_service is None:
        raise RuntimeError("Simulation service is not initialized")

    job_status_info = await simulation_service.get_job_status(hpc_run.job_id)
    if job_status_info is None:
        logger.warning(f"Job {hpc_run.job_id} not yet visible in backend, returning UNKNOWN")
        return SimulationRun(id=int(id), status=JobStatus.UNKNOWN)

    # Persist terminal status to DB so future calls don't need to hit the backend
    if job_status_info.status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED):
        update = JobStatusUpdate(
            job_id=hpc_run.job_id,
            status=job_status_info.status,
            start_time=job_status_info.start_time,
            end_time=job_status_info.end_time,
            error_message=job_status_info.error_message,
        )
        await db_service.update_hpcrun_status(hpcrun_id=hpc_run.database_id, update=update)

    return SimulationRun(id=int(id), status=job_status_info.status, error_message=job_status_info.error_message)


async def get_simulation_chain_progress(db_service: DatabaseService, id: int) -> ChainProgress:
    """Backlog item 6: real per-seed aggregate progress for a chain-dispatch
    campaign (backlog item 33, reworked by item 71 Phase 4) — ``chain_current_job_ids``
    (which seeds are still active) plus ``chain_final_job_ids`` (which have
    resolved, and how) collapsed into real/terminal/succeeded/failed counts.

    404 (via ``ValueError``, matching ``get_simulation_status``'s own
    not-found convention) when the simulation or its HpcRun doesn't exist.
    409 (via ``RuntimeError``) when the simulation exists but isn't a
    chain-dispatch campaign at all (a plain single-shot run has no
    ``chain_final_job_ids`` to aggregate) — callers should fall back to the
    plain ``get_simulation_status`` phase for those, not treat this as a
    500. Never writes to the DB (mirrors ``get_simulation_status``'s own
    read-only handling for the chain-campaign case) — only
    ``JobScheduler._advance_chain_campaign``'s own poll loop may transition
    a campaign row's terminal status.
    """
    sim_record = await db_service.get_simulation(simulation_id=id)
    if sim_record is None:
        raise ValueError(f"Simulation with id {id} not found.")

    hpc_run = await db_service.get_hpcrun_by_ref(ref_id=id, job_type=JobType.SIMULATION)
    if hpc_run is None:
        raise ValueError(f"No HPC run found for simulation {id}.")

    if hpc_run.chain_final_job_ids is None:
        raise RuntimeError(f"Simulation {id} is not a chain-dispatch campaign (no chain_final_job_ids tracked).")

    # seeds_total is the campaign's real requested seed count -- fixed at
    # submission time (len(chain_current_job_ids) never changes length, only
    # its individual entries flip between a job id and None), NOT
    # len(chain_final_job_ids), which now grows incrementally as seeds resolve
    # and would under-report the total for a still-in-flight campaign.
    current_job_ids = hpc_run.chain_current_job_ids or []
    seeds_total = len(current_job_ids) or int(sim_record.num_seeds or 0)

    seeds_succeeded = 0
    seeds_failed = 0
    if hpc_run.chain_final_job_ids:
        chain_service = get_simulation_service_for_job(hpc_run.job_id)
        if not isinstance(chain_service, SimulationServiceRay):
            # Matches get_simulation_status's own established convention for this
            # exact check (same message, same file) -- kept as RuntimeError for
            # consistency rather than TypeError, which would diverge from it.
            raise RuntimeError("Chain-dispatch campaign requires the Ray/Batch simulation service")
        # Every entry in chain_final_job_ids is already known-terminal by
        # construction (JobScheduler only appends once it has observed a
        # seed's job SUCCEEDED/FAILED) -- this call is a fast, safe formality
        # for the succeeded-vs-failed breakdown, not a live wait.
        result = chain_service.get_chain_campaign_result(hpc_run.chain_final_job_ids)
        seeds_succeeded = len(result.succeeded_job_ids)
        seeds_failed = len(result.failed_job_ids)

    seeds_in_progress = seeds_total - seeds_succeeded - seeds_failed
    terminal = hpc_run.status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED)
    return ChainProgress(
        id=int(id),
        seeds_total=seeds_total,
        seeds_succeeded=seeds_succeeded,
        seeds_failed=seeds_failed,
        seeds_in_progress=seeds_in_progress,
        terminal=terminal,
        status=hpc_run.status or JobStatus.RUNNING,
    )


async def cancel_simulation(
    db_service: DatabaseService,
    simulation_service: SimulationService,
    simulation_id: int,
) -> SimulationRun:
    """Cancel a running simulation by killing its backend job(s).

    Chain-dispatch campaigns (backlog item 71 Phase 4, folding in backlog item
    53's cancellation design) cancel every seed's CURRENT in-flight job —
    directly readable from ``chain_current_job_ids``, at most one per seed
    under the per-seed app-level-gated model, no ``dependsOn``-chain walk
    needed the way item 53's original design (written against the superseded
    upfront-chain model) would have required. Once every AWS-side job is
    terminated, the campaign row itself is marked CANCELLED, which also stops
    ``JobScheduler`` from advancing it any further (it only polls rows still
    in a non-terminal status).
    """
    hpc_run = await db_service.get_hpcrun_by_ref(ref_id=simulation_id, job_type=JobType.SIMULATION)
    if hpc_run is None:
        raise ValueError(f"No HPC run found for simulation {simulation_id}")

    # Only cancel jobs that are still active
    if hpc_run.status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED):
        return SimulationRun(id=simulation_id, status=hpc_run.status)

    # Cancel via the service that owns this run (by its backend), not necessarily the
    # injected default.
    service = get_simulation_service_for_job(hpc_run.job_id) or simulation_service

    if hpc_run.chain_final_job_ids is not None:
        if not isinstance(service, SimulationServiceRay):
            raise RuntimeError("Chain-dispatch campaign requires the Ray/Batch simulation service")
        await service.cancel_chain_campaign(hpc_run)
    else:
        await service.cancel_job(hpc_run.job_id)

    # Update the database record
    update = JobStatusUpdate(
        job_id=hpc_run.job_id,
        status=JobStatus.CANCELLED,
    )
    await db_service.update_hpcrun_status(hpcrun_id=hpc_run.database_id, update=update)

    return SimulationRun(id=simulation_id, status=JobStatus.CANCELLED)


async def list_simulations(db_service: DatabaseService) -> list[Simulation]:
    return await db_service.list_simulations()


async def list_simulations_filtered(
    db_service: DatabaseService,
    experiment_id: str | None = None,
    tag: str | None = None,
) -> list[Simulation]:
    """List simulations filtered by experiment IDs and/or tags (union).

    Tags are free-form data stored on each simulation row (see ``add_tags`` /
    ``GET /simulations/tags``), so an unknown tag simply matches nothing and
    yields an empty list — the same behavior as an experiment_id that no
    simulation carries. Both ``experiment_id`` and ``tag`` accept comma-separated
    lists; when both are given the result is their union (deduplicated by the DB).

    Args:
        db_service: Database service instance.
        experiment_id: Comma-separated list of experiment IDs.
        tag: Comma-separated list of tag names (e.g. "cd1").

    Returns:
        A list of Simulation objects matching the filter criteria.
    """
    experiment_ids = _split_csv(experiment_id)
    tags = _split_csv(tag)
    if not experiment_ids and not tags:
        return []
    return await db_service.list_simulations_filtered(experiment_ids=experiment_ids, tags=tags)


def _split_csv(value: str | None) -> list[str]:
    """Split a comma-separated query value into a list of non-empty, stripped tokens."""
    if not value:
        return []
    return [token.strip() for token in value.split(",") if token.strip()]


async def get_omics_outputs(
    hpc_sim_base_path: HPCFilePath, experiment_id: str, output_type: SimulationAnalysisResponseType | None = None
) -> list[TsvOutputFile] | StreamingResponse:
    exp_analysis_outdir = hpc_sim_base_path / experiment_id / "analyses"
    return await fetch_omics_outputs(exp_analysis_outdir=exp_analysis_outdir, output_type=output_type)


async def fetch_omics_outputs(
    exp_analysis_outdir: HPCFilePath, output_type: SimulationAnalysisResponseType | None = None
) -> list[TsvOutputFile] | StreamingResponse:
    if output_type is None:
        # original implementation's analysis response type, so default
        output_type = SimulationAnalysisResponseType.DATA_CONTENT

    analysis_request_cache = Path(get_settings().cache_dir)
    available_paths: list[HPCFilePath] = await get_available_omics_output_paths(
        remote_analysis_outdir=exp_analysis_outdir
    )

    # download available, preserving directory structure
    results_arr: None | list[TsvOutputFile] = None if output_type == SimulationAnalysisResponseType.GZIP_STREAM else []
    for remote_path in available_paths:
        output_i: TsvOutputFile | Path = await download_analysis_output(
            local_dir=analysis_request_cache,
            remote_path=remote_path,
            response_type=output_type,
            remote_base_dir=exp_analysis_outdir,
        )
        if isinstance(output_i, TsvOutputFile) and results_arr is not None:
            results_arr.append(output_i)
            continue
    if results_arr is None:
        # indicates streaming response desired
        return await stream_analysis_output_archive(dir_path=analysis_request_cache)
    return results_arr


async def get_available_omics_output_paths(remote_analysis_outdir: HPCFilePath) -> list[HPCFilePath]:
    cmd = f'find "{remote_analysis_outdir!s}" -type f'
    try:
        async with get_ssh_session_service(SSHTarget.SLURM).session() as ssh:
            ret, out, err = await ssh.run_command(cmd)
        paths = []
        accepted_extensions = ["tsv", "html", "csv", "txt"]
        for fp in out.splitlines():
            extension = fp.split(".")[-1]
            if extension in accepted_extensions:
                paths.append(HPCFilePath(remote_path=Path(fp)))
        return paths
    except Exception:
        logger.exception("could not get the filepaths that are available")
        return []


async def stream_tar_gz(dir_path: Path, chunk_size: int = 64 * 1024) -> AsyncIterator[bytes]:
    read_fd, write_fd = os.pipe()
    read_file = os.fdopen(read_fd, "rb")
    write_file = os.fdopen(write_fd, "wb")

    loop = asyncio.get_event_loop()

    async def create_tar() -> None:
        def _create() -> None:
            try:
                with tarfile.open(fileobj=write_file, mode="w|") as tar:
                    # Use arcname to make paths relative
                    tar.add(str(dir_path), arcname=dir_path.name)
            finally:
                write_file.close()

        await loop.run_in_executor(None, _create)

    tar_task = asyncio.create_task(create_tar())

    gzip_buffer = io.BytesIO()
    gzip_file = gzip.GzipFile(fileobj=gzip_buffer, mode="wb")

    try:
        while True:
            chunk = await loop.run_in_executor(None, read_file.read, chunk_size)

            if not chunk:
                break

            gzip_file.write(chunk)

            if gzip_buffer.tell() > 0:
                gzip_buffer.seek(0)
                compressed = gzip_buffer.read()
                gzip_buffer.seek(0)
                gzip_buffer.truncate()
                yield compressed

        gzip_file.close()
        gzip_buffer.seek(0)
        final_chunk = gzip_buffer.read()
        if final_chunk:
            yield final_chunk

    finally:
        read_file.close()
        await tar_task


def validate_path(dir_path: Path, base_allowed: Path | None = None) -> Path:
    """
    Validate and sanitize the directory path.
    Prevents path traversal attacks.
    """
    path = dir_path.resolve()

    # Optional: restrict to a base directory
    if base_allowed:
        base_allowed = base_allowed.resolve()
        if not str(path).startswith(str(base_allowed)):
            raise HTTPException(status_code=403, detail="Access denied: path outside allowed directory")

    if not path.exists():
        raise HTTPException(status_code=404, detail="Directory not found")

    if not path.is_dir():
        raise HTTPException(status_code=400, detail="Path is not a directory")

    return path


async def stream_analysis_output_archive(dir_path: Path) -> StreamingResponse:
    validated_path = validate_path(dir_path, base_allowed=None)

    # Generate a safe filename for the download
    archive_name = f"{validated_path.name}.tar.gz"

    return StreamingResponse(
        stream_tar_gz(validated_path),
        media_type="application/gzip",
        headers={
            "Content-Disposition": f'attachment; filename="{archive_name}"',
            "X-Content-Type-Options": "nosniff",
        },
    )


def create_tar_gz_archive(dir_path: Path, output_path: Path) -> Path:
    """Create a tar.gz archive of a directory and save it to disk.

    Args:
        dir_path: Directory to archive
        output_path: Path where the archive will be saved

    Returns:
        Path to the created archive
    """
    validated_path = validate_path(dir_path, base_allowed=None)

    # Create parent directories if needed
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Create the tar.gz archive
    with tarfile.open(output_path, "w:gz") as tar:
        tar.add(str(validated_path), arcname=validated_path.name)

    return output_path


def cleanup_archive_file(file_path: Path) -> None:
    """Remove the archive file after it has been sent."""
    try:
        if file_path.exists():
            file_path.unlink()
            logger.debug(f"Cleaned up temporary archive: {file_path}")
    except Exception as e:
        logger.warning(f"Failed to clean up archive file {file_path}: {e}")


async def file_analysis_output_archive(dir_path: Path, bg_tasks: BackgroundTasks, experiment_id: str) -> FileResponse:
    """Create a tar.gz archive and return it as a FileResponse for direct download.

    The archive is saved to disk temporarily and cleaned up after the response is sent.
    """
    validated_path = validate_path(dir_path, base_allowed=None)

    # Generate archive filename and path
    archive_name = f"{experiment_id}.tar.gz"
    archive_path = Path(get_settings().cache_dir) / "downloads" / archive_name

    # Create the archive
    create_tar_gz_archive(validated_path, archive_path)

    # Schedule cleanup after response is sent
    bg_tasks.add_task(cleanup_archive_file, archive_path)

    return FileResponse(
        path=archive_path,
        filename=archive_name,
        media_type="application/gzip",
        headers={
            "Content-Disposition": f'attachment; filename="{archive_name}"',
            "X-Content-Type-Options": "nosniff",
        },
    )


async def download_analysis_output(
    local_dir: Path,
    remote_path: HPCFilePath,
    response_type: SimulationAnalysisResponseType,
    remote_base_dir: HPCFilePath | None = None,
) -> TsvOutputFile | Path:
    """Download a remote analysis output file to local cache.

    Args:
        local_dir: Local directory to save files to
        remote_path: Full remote path to the file
        response_type: Type of response to return
        remote_base_dir: Base remote directory for calculating relative paths.
            If provided, the directory structure relative to this base will be
            preserved locally. If None, only the filename is used (legacy behavior).
    """
    accepted_response_types = SimulationAnalysisResponseType.values()
    if response_type not in accepted_response_types:
        raise ValueError(
            f"Unexpected response_type. Got: {response_type}; Expected one of: {accepted_response_types!s}"
        )

    # Calculate relative path to preserve directory structure
    if remote_base_dir is not None:
        relative_path = remote_path.remote_path.relative_to(remote_base_dir.remote_path)
        local = local_dir / relative_path
    else:
        # Legacy behavior: just use filename
        relative_path = Path(remote_path.remote_path.parts[-1])
        local = local_dir / relative_path

    # Create parent directories if needed
    local.parent.mkdir(parents=True, exist_ok=True)

    if not local.exists():
        async with get_ssh_session_service(SSHTarget.SLURM).session() as ssh:
            await ssh.scp_download(local_file=local, remote_path=remote_path)

    if response_type == SimulationAnalysisResponseType.DATA_CONTENT:
        return TsvOutputFile(filename=str(relative_path), content=local.read_text())
    elif response_type == SimulationAnalysisResponseType.TAR_GZIP_STREAM:
        return local
    raise RuntimeError("Not sure how you got here, but here you are. Cheers!")


async def get_simulation_outputs(
    db_service: DatabaseService,
    simulation_id: int,
    hpc_sim_base_path: HPCFilePath,
    data_response_type: SimulationAnalysisDataResponseType = SimulationAnalysisDataResponseType.STREAMING,
    bg_tasks: BackgroundTasks | None = None,
) -> StreamingResponse | FileResponse:
    """Get simulation outputs as a tar.gz archive.

    Dispatches to SSH/SCP (SLURM backend) or S3 (K8s/LOCAL backend) based on the HpcRun record.
    """
    simulation = await db_service.get_simulation(simulation_id=simulation_id)
    if simulation is None:
        raise ValueError(f"Simulation with id {simulation_id} not found in database.")

    experiment_id = simulation.config.experiment_id

    # Download files to local cache, preserving directory structure
    analysis_request_cache = Path(get_settings().cache_dir) / experiment_id
    analysis_request_cache.mkdir(parents=True, exist_ok=True)

    # Dispatch based on backend
    hpc_run = await db_service.get_hpcrun_by_ref(ref_id=simulation_id, job_type=JobType.SIMULATION)
    if hpc_run and hpc_run.job_id.backend == JobBackend.RAY:
        # Ray backend: stream the xarray/zarr ensemble outputs (seed_NN/store.zarr +
        # summary.json) directly from S3. FILE mode falls back to streaming.
        archive_name = f"{experiment_id}.tar.gz"
        return StreamingResponse(
            _stream_s3_tar_gz_ray(experiment_id),
            media_type="application/gzip",
            headers={
                "Content-Disposition": f'attachment; filename="{archive_name}"',
                "X-Content-Type-Options": "nosniff",
            },
        )
    if hpc_run and hpc_run.job_id.backend in (JobBackend.K8S, JobBackend.LOCAL):
        # K8s/S3 path: stream directly from S3 into tar.gz response (no disk cache).
        # This avoids ALB 504 timeouts on large simulations by sending bytes immediately.
        if data_response_type == SimulationAnalysisDataResponseType.FILE:
            # FILE mode still needs disk — fall back to download-then-serve
            await _download_outputs_from_s3(experiment_id, analysis_request_cache)
            if bg_tasks is None:
                raise ValueError("BackgroundTasks required for FILE response type")
            return await file_analysis_output_archive(
                dir_path=analysis_request_cache, bg_tasks=bg_tasks, experiment_id=experiment_id
            )
        archive_name = f"{experiment_id}.tar.gz"
        return StreamingResponse(
            _stream_s3_tar_gz(experiment_id),
            media_type="application/gzip",
            headers={
                "Content-Disposition": f'attachment; filename="{archive_name}"',
                "X-Content-Type-Options": "nosniff",
            },
        )
    else:
        # SLURM path: download via SSH/SCP then stream
        exp_analysis_outdir = hpc_sim_base_path / experiment_id / "analyses"
        available_paths: list[HPCFilePath] = await get_available_omics_output_paths(
            remote_analysis_outdir=exp_analysis_outdir
        )
        for remote_path in available_paths:
            await download_analysis_output(
                local_dir=analysis_request_cache,
                remote_path=remote_path,
                response_type=SimulationAnalysisResponseType.TAR_GZIP_STREAM,
                remote_base_dir=exp_analysis_outdir,
            )

        if data_response_type == SimulationAnalysisDataResponseType.FILE:
            if bg_tasks is None:
                raise ValueError("BackgroundTasks required for FILE response type")
            return await file_analysis_output_archive(
                dir_path=analysis_request_cache, bg_tasks=bg_tasks, experiment_id=experiment_id
            )
        return await stream_analysis_output_archive(dir_path=analysis_request_cache)


_ACCEPTED_ANALYSES_EXTENSIONS = (".tsv", ".json")
_WORKFLOW_CONFIG_KEY = "nextflow/workflow_config.json"
_S3_DOWNLOAD_CONCURRENCY = 32


async def _download_outputs_from_s3(experiment_id: str, local_cache: Path) -> None:
    """Download simulation outputs from S3 to local cache.

    Downloads only:
    - All files under ``analyses/`` matching accepted extensions (.tsv, .json)
    - ``nextflow/workflow_config.json``

    Downloads are parallelized with a bounded semaphore to avoid overwhelming
    the event loop while still finishing fast enough that reverse-proxy
    idle timeouts (60s default on ALB/NGINX) don't trigger a 504 before the
    streaming response begins.
    """
    file_service = get_file_service()
    if file_service is None:
        raise RuntimeError("File service is not initialized")

    experiment_prefix = data_layout.NextflowLayout.experiment_prefix(experiment_id)

    # 1. Plan analyses/ downloads
    analyses_prefix = S3FilePath(s3_path=Path(f"{experiment_prefix}/analyses"))
    analyses_listing = await file_service.get_listing(analyses_prefix)

    download_plan: list[tuple[S3FilePath, Path]] = []
    for item in analyses_listing:
        if not item.Key.endswith(_ACCEPTED_ANALYSES_EXTENSIONS):
            continue
        relative = Path(item.Key).relative_to(experiment_prefix)
        local_file = local_cache / relative
        if local_file.exists():
            continue
        local_file.parent.mkdir(parents=True, exist_ok=True)
        download_plan.append((S3FilePath(s3_path=Path(item.Key)), local_file))

    logger.info(
        f"Downloading {len(download_plan)} analysis files from S3 for "
        f"experiment {experiment_id} (concurrency={_S3_DOWNLOAD_CONCURRENCY})"
    )

    # 2. Run downloads concurrently with a bounded semaphore
    sem = asyncio.Semaphore(_S3_DOWNLOAD_CONCURRENCY)

    async def _bounded_download(remote: S3FilePath, local: Path) -> None:
        async with sem:
            await file_service.download_file(remote, local)

    if download_plan:
        results = await asyncio.gather(
            *(_bounded_download(remote, local) for remote, local in download_plan),
            return_exceptions=True,
        )
        failures = [r for r in results if isinstance(r, BaseException)]
        if failures:
            logger.warning(
                f"{len(failures)}/{len(download_plan)} S3 downloads failed for {experiment_id}; "
                f"continuing with partial archive. First error: {failures[0]!r}"
            )

    # 3. Download nextflow/workflow_config.json
    workflow_config_key = f"{experiment_prefix}/{_WORKFLOW_CONFIG_KEY}"
    workflow_config_s3 = S3FilePath(s3_path=Path(workflow_config_key))
    local_workflow_config = local_cache / _WORKFLOW_CONFIG_KEY
    if not local_workflow_config.exists():
        local_workflow_config.parent.mkdir(parents=True, exist_ok=True)
        try:
            await file_service.download_file(workflow_config_s3, local_workflow_config)
        except Exception:
            logger.warning(f"workflow_config.json not found at {workflow_config_key}, skipping")


async def _fetch_s3_file_entries(
    experiment_id: str, download_keys: list[str], experiment_prefix: str
) -> list[tuple[str, bytes]]:
    """Fetch S3 objects in-memory as (arcname, content) pairs for tar creation."""
    file_service = get_file_service()
    if file_service is None:
        raise RuntimeError("File service is not initialized")

    entries: list[tuple[str, bytes]] = []
    for key in download_keys:
        try:
            content = await file_service.get_file_contents(S3FilePath(s3_path=Path(key)))
            if content is not None:
                relative = str(Path(key).relative_to(experiment_prefix))
                entries.append((f"{experiment_id}/{relative}", content))
        except Exception:
            logger.warning(f"Failed to fetch {key}, skipping")
    return entries


def _write_tar_entries(write_file: io.BufferedWriter, file_entries: list[tuple[str, bytes]]) -> None:
    """Write (arcname, content) pairs into a streaming tar archive."""
    try:
        with tarfile.open(fileobj=write_file, mode="w|") as tar:
            for arcname, data in file_entries:
                info = tarfile.TarInfo(name=arcname)
                info.size = len(data)
                tar.addfile(info, io.BytesIO(data))
    finally:
        write_file.close()


async def _stream_s3_tar_gz(experiment_id: str, chunk_size: int = 64 * 1024) -> AsyncIterator[bytes]:
    """Stream S3 simulation outputs directly into a tar.gz response.

    Fetches S3 objects in-memory and pipes them into a tar stream on-the-fly,
    so bytes flow to the client continuously — avoids ALB 504 timeouts that
    occur when the server downloads all files to disk before responding.
    """
    experiment_prefix = data_layout.NextflowLayout.experiment_prefix(experiment_id)

    file_service = get_file_service()
    if file_service is None:
        raise RuntimeError("File service is not initialized")

    analyses_prefix = S3FilePath(s3_path=Path(f"{experiment_prefix}/analyses"))
    analyses_listing = await file_service.get_listing(analyses_prefix)
    download_keys = [item.Key for item in analyses_listing if item.Key.endswith(_ACCEPTED_ANALYSES_EXTENSIONS)]
    download_keys.append(f"{experiment_prefix}/{_WORKFLOW_CONFIG_KEY}")
    logger.info(f"Streaming {len(download_keys)} files from S3 for experiment {experiment_id}")

    file_entries = await _fetch_s3_file_entries(experiment_id, download_keys, experiment_prefix)

    read_fd, write_fd = os.pipe()
    read_file = os.fdopen(read_fd, "rb")
    write_file = os.fdopen(write_fd, "wb")
    loop = asyncio.get_event_loop()

    tar_future = loop.run_in_executor(None, _write_tar_entries, write_file, file_entries)

    async for chunk in _gzip_pipe_stream(read_file, loop, chunk_size):
        yield chunk

    await tar_future


async def _stream_s3_tar_gz_ray(experiment_id: str, chunk_size: int = 64 * 1024) -> AsyncIterator[bytes]:
    """Stream a Ray ensemble's S3 outputs (zarr stores + summaries) into a tar.gz.

        The Ray entrypoint syncs the whole ``.pbg/runs/phase0-xarray`` tree to
        ``s3://{bucket}/{s3_output_prefix}/{experiment_id}/`` — for v2ecoli comparison
        runs that is ``v2ecoli_seed{NN}.zarr/`` per seed (each a hive-partitioned
        datatree of many small chunk objects; verified against ``sim61-v2c-*`` on
        smsvpctest) plus ``v2ecoli_build_config.json``. This function does not build
        those paths: unlike the Nextflow layout, we stream every object under the
        prefix as-is. (The per-seed store URI the observables reader targets is built
    by ``data_layout.RayLayout.seed_store_uri`` (the observables reader's path).)
    """
    experiment_prefix = data_layout.RayLayout.experiment_prefix(experiment_id)

    file_service = get_file_service()
    if file_service is None:
        raise RuntimeError("File service is not initialized")

    listing = await file_service.get_listing(S3FilePath(s3_path=Path(experiment_prefix)))
    download_keys = [item.Key for item in listing]
    logger.info(f"Streaming {len(download_keys)} Ray output objects from S3 for experiment {experiment_id}")

    file_entries = await _fetch_s3_file_entries(experiment_id, download_keys, experiment_prefix)

    read_fd, write_fd = os.pipe()
    read_file = os.fdopen(read_fd, "rb")
    write_file = os.fdopen(write_fd, "wb")
    loop = asyncio.get_event_loop()

    tar_future = loop.run_in_executor(None, _write_tar_entries, write_file, file_entries)

    async for chunk in _gzip_pipe_stream(read_file, loop, chunk_size):
        yield chunk

    await tar_future


async def _gzip_pipe_stream(
    read_file: io.BufferedReader, loop: asyncio.AbstractEventLoop, chunk_size: int
) -> AsyncIterator[bytes]:
    """Read raw tar data from a pipe, gzip-compress, and yield chunks."""
    gzip_buffer = io.BytesIO()
    gzip_file = gzip.GzipFile(fileobj=gzip_buffer, mode="wb")
    try:
        while True:
            raw = await loop.run_in_executor(None, read_file.read, chunk_size)
            if not raw:
                break
            gzip_file.write(raw)
            if gzip_buffer.tell() > 0:
                gzip_buffer.seek(0)
                compressed = gzip_buffer.read()
                gzip_buffer.seek(0)
                gzip_buffer.truncate()
                yield compressed
        gzip_file.close()
        gzip_buffer.seek(0)
        final = gzip_buffer.read()
        if final:
            yield final
    finally:
        read_file.close()


async def get_simulation_log(db_service: DatabaseService, simulation_id: int, truncate: bool = True) -> Response:
    """Get simulation workflow log. Dispatches to SLURM or K8s based on backend."""
    hpc_run = await db_service.get_hpcrun_by_ref(ref_id=simulation_id, job_type=JobType.SIMULATION)
    if hpc_run is None:
        raise ValueError(f"No HPC run found for simulation {simulation_id}")

    if hpc_run.job_id.backend == JobBackend.K8S:
        log_content = await _get_k8s_log(hpc_run, db_service, simulation_id)
    elif hpc_run.job_id.backend == JobBackend.RAY:
        log_content = await _get_ray_log(hpc_run, db_service, simulation_id)
    elif hpc_run.job_id.backend == JobBackend.LOCAL:
        log_content = f"Logs not available for local tasks (job {hpc_run.job_id})"
    else:
        log_content = await _get_slurm_log(hpc_run)
        # Extract Nextflow section from SLURM log
        _, _, after = log_content.partition("N E X T F L O W")
        if after:
            log_content = "N E X T F L O W" + after

    if truncate:
        log_content = _truncate_log(log_content)

    return Response(content=log_content, media_type="text/plain")


_LOG_HEAD_LINES = 20
_TRUNCATION_MARKER = "\n... truncated ...\n\n"


def _truncate_log(log: str) -> str:
    """Return the head (Nextflow header) + tail (final executor block) of a log.

    The tail starts from the last line containing ``executor`` (the Nextflow
    summary block) and continues to EOF.  If no executor line is found, the
    last 15 lines are returned as the tail.
    """
    lines = log.splitlines(keepends=True)
    if len(lines) <= _LOG_HEAD_LINES + 15:
        return log  # already small enough

    head = "".join(lines[:_LOG_HEAD_LINES])

    # Find the last line containing "executor" — marks the final summary block
    tail_start = None
    for i in range(len(lines) - 1, -1, -1):
        if "executor" in lines[i].lower():
            tail_start = i
            break

    if tail_start is not None and tail_start > _LOG_HEAD_LINES:
        tail = "".join(lines[tail_start:])
    else:
        # Fallback: last 15 lines
        tail = "".join(lines[-15:])

    return head + _TRUNCATION_MARKER + tail


def workflow_log(simulation_id: int, base_url: str = "http://localhost:8080", timeout: int = 300) -> None:
    """Fetch the workflow log tail for a simulation and print it to the console.

    This is a convenience function for quick interactive debugging — it fetches
    the full log client-side, extracts the executor summary block, and prints
    it with Rich formatting.  No deploy required.

    Usage::

        from viva_api.common.handlers.simulations import workflow_log
        workflow_log(44)
    """
    from app.app_data_service import E2EDataService
    from app.cli_theme import get_console

    console = get_console()
    svc = E2EDataService(base_url=base_url, timeout=timeout)  # type: ignore[arg-type]

    with console.status("[memphis.spinner]Fetching status..."):
        run = svc.get_workflow_status(simulation_id=simulation_id)
    status = run.status.value.upper()

    with console.status("[memphis.spinner]Fetching log..."):
        log = svc.get_workflow_log(simulation_id=simulation_id, truncate=False)

    # Extract tail starting from last "executor" line
    lines = log.splitlines()
    tail_start = None
    for i in range(len(lines) - 1, -1, -1):
        if "executor" in lines[i].lower():
            tail_start = i
            break
    tail = "\n".join(lines[tail_start:]) if tail_start is not None else "\n".join(lines[-15:])

    from rich.panel import Panel

    console.print(Panel(tail, title=f"Workflow Log — sim {simulation_id}", border_style="memphis.border.info"))
    from app.cli_theme import status_border, status_style

    error_detail = f"\n{run.error_message}" if run.error_message else ""
    console.print(
        Panel(
            f"[{status_style(status.lower())}]{status}[/]{error_detail}",
            title="Simulation Status",
            border_style=status_border(status.lower()),
        )
    )


# Caller-facing default for the LEGACY vEcoli/Nextflow analysis paths, which take an
# explicit module list and have no registry-backed resolver. The Ray/v2ecoli path
# resolves its own default instead (see _run_standalone_analysis_ray_native).
_DEFAULT_LEGACY_ANALYSIS_MODULES: dict[str, dict[str, Any]] = {
    "single": {
        "ptools_rna": {"n_tp": 10},
        "ptools_rxns": {"n_tp": 10},
        "ptools_proteins": {"n_tp": 10},
    }
}


async def _run_standalone_analysis_ray_native(
    database_service: DatabaseService,
    simulation: Simulation,
    simulator: SimulatorVersion,
    modules: dict[str, dict[str, Any]] | None,
) -> dict[str, Any]:
    """Standalone analysis for a Ray/xarray-dispatched simulation (v2ecoli/sms-ecoli).

    Submits a K8s Job running scripts/run_standalone_analysis.py inside the
    already-built v2ecoli:<commit> image, rather than the legacy
    submit_standalone_analysis path (vecoli:<commit>-amd64-submit, an image
    never produced for this pipeline). Records a COMPUTING row via the
    existing analysis-tracking DB layer (analysis-results-design.md's schema
    -- already migrated, just never wired into a K8s submission path before
    this) so GET /analyses/{id}/status can resolve real progress instead of
    callers having no way to know when the job is done.

    ``modules=None`` resolves the same way the dispatch DAG's own analysis node
    does (``analysis_modules_for``): the simulation's own configured
    analysis_options, else the composite's ``applicable`` keyword. A
    hand-triggered analysis and an auto-triggered one must cover the same
    ground -- the legacy caller-facing default (three ptools modules) would
    silently under-deliver the cd1_* suite for this pipeline.
    """
    from viva_api.simulation.simulation_service_ray import analysis_modules_for

    config = simulation.config
    if modules is None:
        modules = analysis_modules_for(config)  # type: ignore[assignment]
    out_uri = getattr(config, "emitter_arg", None)
    out_uri = (out_uri or {}).get("out_uri") if isinstance(out_uri, dict) else None
    if not out_uri:
        raise ValueError(
            f"Simulation {simulation.database_id} has no config.emitter_arg.out_uri "
            "-- not a Ray/xarray dispatch, cannot run standalone analysis"
        )
    n_seeds = simulation.num_seeds or 1
    experiment_id = simulation.experiment_id
    analysis_name = f"analysis-{experiment_id[:20]}-{str(uuid.uuid4())[:4]}"
    result_uri = f"{out_uri.rstrip('/')}/analyses/{analysis_name}"

    sim_service = get_simulation_service()
    if sim_service is None:
        raise ValueError("Simulation service not initialized")

    from viva_api.simulation.simulation_service_k8s import SimulationServiceK8s

    if not isinstance(sim_service, SimulationServiceK8s):
        raise ValueError("Standalone Ray-native analysis requires K8s backend")  # noqa: TRY004

    params: dict[str, Any] = {
        "out_uri": out_uri,
        "n_seeds": n_seeds,
        "n_generations": int(getattr(config, "generations", 1) or 1),
        "modules": modules,
        "analysis_name": analysis_name,
        # ORMAnalysis.to_dto() unconditionally reads config["analysis_options"]
        # (AnalysisConfigOptions requires experiment_id) -- the legacy Batch/SLURM
        # producers below already write this shape (experiment_id + each domain
        # spread as a top-level key); match it so to_dto() doesn't KeyError. The
        # v2ecoli-side consumer (run_standalone_analysis.py) only reads out_uri/
        # n_seeds/n_generations/modules/analysis_name and ignores unknown keys, so
        # this is inert for it -- purely for the DTO contract. ``modules`` may be
        # the "applicable" keyword rather than a mapping, which spreads to nothing.
        "analysis_options": {
            "experiment_id": [experiment_id],
            **(modules if isinstance(modules, dict) else {}),
        },
    }
    job_id = await sim_service.submit_ray_native_analysis(
        experiment_id=experiment_id,
        params=params,
        commit=simulator.git_commit_hash,
    )
    record = await database_service.record_analysis(
        experiment_id=experiment_id,
        n_tp=None,
        status=AnalysisStatusDB.COMPUTING,
        config=params,
        name=analysis_name,
        simulation_id=simulation.database_id,
        backend="ray",
        job_id_ext=str(job_id),
        result_uri=result_uri,
    )
    return {
        "job_id": str(job_id),
        "analysis_name": analysis_name,
        "config": params,
        "database_id": record.database_id,
    }


async def run_standalone_analysis(
    database_service: DatabaseService,
    simulation_id: int,
    modules: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Run standalone vEcoli analysis on existing simulation output.

    Builds an analysis config from the simulation's experiment_id and paths,
    then submits it as a K8s Job (Batch) or returns the config for SLURM.

    Args:
        database_service: DB service for looking up the simulation.
        simulation_id: Database ID of a completed simulation.
        modules: Analysis modules to run, keyed by domain.
            E.g. ``{"single": {"ptools_rna": {"n_tp": 10}, "ptools_rxns": {"n_tp": 10}}}``
            If None, uses a default set of ptools modules.
    """
    settings = get_settings()
    simulation = await database_service.get_simulation(simulation_id=simulation_id)
    if simulation is None:
        raise ValueError(f"Simulation {simulation_id} not found")

    experiment_id = simulation.experiment_id

    # Build analysis config — path patterns match vEcoli conventions
    backend = get_job_backend()
    if backend == ComputeBackend.BATCH:
        simulator = await database_service.get_simulator(simulator_id=simulation.simulator_id)
        if simulator is not None and compute_backend_for_repo(simulator.git_repo_url) == ComputeBackend.RAY:
            # This simulation was dispatched via the Ray/xarray pipeline (v2ecoli/sms-ecoli),
            # which shares no build artifacts or output shape with the legacy vEcoli-private/
            # Nextflow path below (variant_sim_data, parca/kb/*, a different ECR image entirely
            # -- confirmed live via ImagePullBackOff, see v2ecoli#426). Route to the
            # v2ecoli-native entrypoint instead.
            return await _run_standalone_analysis_ray_native(
                database_service=database_service,
                simulation=simulation,
                simulator=simulator,
                modules=modules,
            )
        # The legacy vEcoli/Nextflow path below has no "applicable" resolver, so its
        # caller-facing default stays an explicit module list.
        modules = modules or _DEFAULT_LEGACY_ANALYSIS_MODULES
        s3_output = data_layout.NextflowLayout.output_uri(experiment_id)
        analysis_name = f"analysis-{experiment_id[:20]}-{str(uuid.uuid4())[:4]}"
        analysis_config: dict[str, Any] = {
            "analysis_options": {
                "experiment_id": [experiment_id],
                "variant_data_dir": [f"{s3_output}/variant_sim_data"],
                "validation_data_path": [f"{s3_output}/parca/kb/validationData.cPickle"],
                "outdir": f"{s3_output}/analyses/{analysis_name}",
                "single": {},
                "multidaughter": {},
                "multigeneration": {},
                **{domain: domain_modules for domain, domain_modules in modules.items()},
            },
            "emitter_arg": {"out_uri": s3_output},
        }

        # Submit as K8s Job
        sim_service = get_simulation_service()
        if sim_service is None:
            raise ValueError("Simulation service not initialized")

        from viva_api.simulation.simulation_service_k8s import SimulationServiceK8s

        if not isinstance(sim_service, SimulationServiceK8s):
            raise ValueError("Standalone K8s analysis requires K8s backend")

        job_id = await sim_service.submit_standalone_analysis(
            experiment_id=experiment_id,
            analysis_config=analysis_config,
            database_service=database_service,
            simulator_id=simulation.simulator_id,
        )
        return {"job_id": str(job_id), "analysis_name": analysis_name, "config": analysis_config}
    else:
        # SLURM path
        modules = modules or _DEFAULT_LEGACY_ANALYSIS_MODULES
        sim_base = settings.hpc_sim_base_path.remote_path
        analysis_name = f"analysis-{experiment_id[:20]}-{str(uuid.uuid4())[:4]}"
        analysis_config = {
            "analysis_options": {
                "experiment_id": [experiment_id],
                "variant_data_dir": [str(sim_base / experiment_id / "variant_sim_data")],
                "validation_data_path": [
                    str(settings.hpc_parca_base_path.remote_path / "default" / "kb" / "validationData.cPickle")
                ],
                "outdir": str(settings.analysis_outdir.remote_path / analysis_name),
                "single": {},
                "multidaughter": {},
                "multigeneration": {},
                **{domain: domain_modules for domain, domain_modules in modules.items()},
            },
            "emitter_arg": {"out_dir": str(sim_base)},
        }
        # For SLURM, return the config — the caller can submit it via the analysis service
        return {"analysis_name": analysis_name, "config": analysis_config, "backend": "slurm"}


async def _get_slurm_log(hpc_run: HpcRun) -> str:
    """Read SLURM job log via SSH."""
    job_id = str(hpc_run.job_id)
    async with get_ssh_session_service(SSHTarget.SLURM).session() as ssh:
        returncode, stdout, stderr = await ssh.run_command(f"scontrol show job {job_id}")
        try:
            k = "JobName="
            job_name = next(filter(lambda v: k in v, stdout.replace("\n", "").split(" "))).replace(k, "")
            log_path = get_settings().slurm_log_base_path / f"{job_name}.out"
            _, log_stdout, _ = await ssh.run_command(f"cat {log_path!s}")
        except StopIteration:
            raise RuntimeError(f"No simulation job name available for HPC run {hpc_run.database_id}")
    return log_stdout


async def _get_ray_log(hpc_run: HpcRun, db_service: DatabaseService, simulation_id: int) -> str:
    """Surface a Ray MNP run's log.

    Ray-on-Batch runs have no SSH login node or K8s pod to read — bulk per-node Ray
    session logs go to ``RAY_LOG_S3_PREFIX`` and AWS Batch CloudWatch. The high-signal
    per-run artifact is the ensemble ``summary.json`` (per-seed step counts / errors),
    written to the run's S3 output prefix. Surface that plus pointers to the bulk logs.
    Best-effort: never raises (a missing summary just means the run hasn't produced it yet).
    """
    settings = get_settings()
    parts: list[str] = [f"Ray MNP simulation job {hpc_run.job_id.value} (state via `simulation status`)."]

    simulation = await db_service.get_simulation(simulation_id=simulation_id)
    file_service = get_file_service()
    if simulation is not None and file_service is not None:
        summary_key = data_layout.RayLayout.summary_key(simulation.config.experiment_id)
        try:
            content = await file_service.get_file_contents(S3FilePath(s3_path=Path(summary_key)))
            if content:
                parts.append("=== summary.json (per-seed results) ===\n" + content.decode("utf-8", errors="replace"))
        except Exception:
            logger.info("Ray summary.json not yet available for simulation %s", simulation_id)

    if settings.ray_log_s3_prefix:
        parts.append(f"Bulk Ray session logs: {settings.ray_log_s3_prefix} (+ AWS Batch CloudWatch).")
    return "\n\n".join(parts)


async def _get_k8s_log(hpc_run: HpcRun, db_service: DatabaseService, simulation_id: int) -> str:
    """Read K8s Job pod logs via the K8s API, falling back to S3 .nextflow.log."""
    from viva_api.simulation.simulation_service_k8s import SimulationServiceK8s

    simulation_service = get_simulation_service()
    if not isinstance(simulation_service, SimulationServiceK8s):
        raise TypeError("K8s logs requested but simulation service is not SimulationServiceK8s")

    # Try K8s pod logs first
    log_content = simulation_service._k8s.get_job_logs(hpc_run.job_id.value)
    if log_content is not None:
        return log_content

    # Fallback: download .nextflow.log from S3
    log_content = await _get_s3_nextflow_log(db_service, simulation_id)
    if log_content is not None:
        return log_content

    return f"No logs available for K8s Job {hpc_run.job_id.value} (pod cleaned up, S3 log not found)"


async def _get_s3_nextflow_log(db_service: DatabaseService, simulation_id: int) -> str | None:
    """Try to fetch .nextflow.log from S3 for a K8s simulation."""
    settings = get_settings()
    file_service = get_file_service()
    if file_service is None:
        return None

    simulation = await db_service.get_simulation(simulation_id=simulation_id)
    if simulation is None:
        return None

    experiment_id = simulation.config.experiment_id
    log_key = f"{settings.s3_work_prefix}/{experiment_id}/logs/.nextflow.log"
    log_s3 = S3FilePath(s3_path=Path(log_key))

    try:
        content = await file_service.get_file_contents(log_s3)
        if content:
            return content.decode("utf-8", errors="replace")
    except Exception:
        logger.debug(f"S3 .nextflow.log not found at {log_key}")

    return None
