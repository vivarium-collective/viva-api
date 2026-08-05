"""
Simulation analysis handler for SMS API simulation experiment outputs.

NOTE: this module is essentially "analysis_handlers_hpc". TODO: abstract this into interface
"""

import json
import logging
import re
from collections.abc import Sequence
from pathlib import Path
from textwrap import dedent

from starlette.requests import Request

from sms_api.analysis.analysis_service import AnalysisServiceSlurm, RequestPayload, parse_partition_metadata
from sms_api.analysis.models import (
    AnalysisRun,
    ExperimentAnalysisDTO,
    ExperimentAnalysisRequest,
    OutputFile,
    OutputFileMetadata,
    TsvOutputFile,
)
from sms_api.common.models import JobId, JobStatus, SSHTarget
from sms_api.common.storage import data_layout
from sms_api.common.storage.file_paths import HPCFilePath, S3FilePath
from sms_api.common.utils import get_data_id, timestamp
from sms_api.config import get_settings
from sms_api.dependencies import get_file_service, get_simulation_service, get_ssh_session_service
from sms_api.simulation.database_service import DatabaseService
from sms_api.simulation.models import SimulatorVersion
from sms_api.simulation.tables_orm import AnalysisStatusDB

# Text output extensions the analysis data endpoint returns (mirrors the legacy
# SLURM path's get_available_output_paths).
_ANALYSIS_OUTPUT_EXTENSIONS = (".tsv", ".csv", ".txt", ".html")


class AnalysisNotReadyError(Exception):
    """Raised when analysis data is requested but the result is not READY."""


async def list_simulation_analyses(db_service: DatabaseService, simulation_id: int) -> list[ExperimentAnalysisDTO]:
    """List existing analysis records for a simulation (by its experiment_id)."""
    simulation = await db_service.get_simulation(simulation_id=simulation_id)
    if simulation is None:
        raise ValueError(f"Simulation {simulation_id} not found")
    return await db_service.list_analyses(experiment_id=simulation.experiment_id)


async def fetch_analysis_data(db_service: DatabaseService, analysis_id: int) -> list[TsvOutputFile]:
    """Return all text output files of an existing analysis by id, as ``list[TsvOutputFile]``.

    Pure retrieval: reads S3 under the analysis row's ``result_uri`` and inlines each
    file's content with variant/lineage_seed/generation parsed from its partition
    path — the same shape as the legacy ``POST /analyses``. Never computes: if the
    analysis is not READY it raises ``AnalysisNotReadyError`` (mapped to 409).
    """
    analysis = await db_service.get_analysis(database_id=analysis_id)  # RuntimeError -> 404 at the router
    if analysis.status != JobStatus.COMPLETED or not analysis.result_uri:
        raise AnalysisNotReadyError(f"Analysis {analysis_id} is not ready (status={analysis.status})")

    file_service = get_file_service()
    if file_service is None:
        raise RuntimeError("File service is not initialized")

    listing = await file_service.get_listing(S3FilePath(s3_path=Path(analysis.result_uri)))
    outputs: list[TsvOutputFile] = []
    for item in listing:
        if not item.Key.lower().endswith(_ANALYSIS_OUTPUT_EXTENSIONS):
            continue
        content = await file_service.get_file_contents(S3FilePath(s3_path=Path(item.Key)))
        if content is None:
            continue
        metadata = parse_partition_metadata(Path(item.Key))
        outputs.append(
            TsvOutputFile(
                filename=Path(item.Key).name,
                content=content.decode(errors="replace"),
                variant=metadata.get("variant", 0),
                lineage_seed=metadata.get("lineage_seed"),
                generation=metadata.get("generation"),
            )
        )
    return outputs


async def handle_run_analysis(
    request: ExperimentAnalysisRequest,
    simulator: SimulatorVersion,
    analysis_service: AnalysisServiceSlurm,
    db_service: DatabaseService,
    logger: logging.Logger,
    _request: Request,
) -> Sequence[OutputFileMetadata | TsvOutputFile]:
    """
    Execute an analysis request.
    """
    return await handle_run_analysis_slurm(
        request=request,
        simulator=simulator,
        analysis_service=analysis_service,
        logger=logger,
        _request=_request,
        db_service=db_service,
    )


async def handle_run_analysis_slurm(
    request: ExperimentAnalysisRequest,
    simulator: SimulatorVersion,
    analysis_service: AnalysisServiceSlurm,
    logger: logging.Logger,
    db_service: DatabaseService,
    _request: Request | None = None,
) -> Sequence[OutputFileMetadata | TsvOutputFile]:
    # 1. check if hashed/cached payload exists
    payload_hash = RequestPayload(data=request.model_dump()).hash()
    analysis_request_cache = Path(analysis_service.env.cache_dir) / payload_hash
    analysis_name: str = get_data_id(scope="analysis")

    results: list[TsvOutputFile] = []
    # 2. if cache doesn't exist or is empty, run an analysis
    cache_has_files = analysis_request_cache.exists() and len([fp for fp in analysis_request_cache.iterdir()]) > 0
    if not cache_has_files:
        # 2a. mk local cache (use makedirs with exist_ok for empty directories)
        analysis_request_cache.mkdir(parents=True, exist_ok=True)
        # Use a single SSH session for job submission and polling
        async with get_ssh_session_service(SSHTarget.SLURM).session() as ssh:
            # 2c. dispatch job
            jobname, jobid, config = await analysis_service.dispatch_analysis(
                request=request,
                logger=logger,
                analysis_name=analysis_name,
                ssh=ssh,
                simulator_hash=simulator.git_commit_hash,
            )
            # 2d. insert analysis into db
            dto: ExperimentAnalysisDTO = await db_service.insert_analysis(
                name=analysis_name,
                config=config,
                last_updated=timestamp(),
                job_name=jobname,
                job_id=jobid,
            )
            # 2e. poll status
            _run = await analysis_service.poll_status(dto=dto, ssh=ssh)

        # Fetch available output files from the analysis output directory
        # Analysis outputs are stored at: hpc_sim_base_path / experiment_id / "analyses"
        remote_analysis_outdir = HPCFilePath(remote_path=Path(config.analysis_options.outdir))  # type: ignore[attr-defined]
        available_paths: list[HPCFilePath] = await analysis_service.get_available_output_paths(
            remote_analysis_outdir=remote_analysis_outdir
        )

        # download available
        for remote_path in available_paths:
            output_i: TsvOutputFile = await analysis_service.download_analysis_output(
                local_dir=analysis_request_cache, remote_path=remote_path
            )
            results.append(output_i)
    else:
        # Load cached results — filenames use the pattern: module_vX_sY_gZ.tsv
        for fp in analysis_request_cache.iterdir():
            filename = fp.parts[-1]
            if filename.endswith(".tsv"):
                file_content = fp.read_text()
                metadata = _parse_cached_filename_metadata(filename)
                output_i = TsvOutputFile(
                    filename=filename,
                    content=file_content,
                    variant=metadata.get("variant", 0),
                    lineage_seed=metadata.get("lineage_seed"),
                    generation=metadata.get("generation"),
                )
                results.append(output_i)

    return results


# Regex for cached filename metadata: module_vX_sY_gZ.tsv
_CACHED_META_RE = re.compile(r"_v(\d+)(?:_s(\d+))?(?:_g(\d+))?(?:\.\w+)$")


def _parse_cached_filename_metadata(filename: str) -> dict[str, int]:
    """Parse variant/lineage_seed/generation from cached filenames like ptools_rna_v0_s0_g5.tsv."""
    metadata: dict[str, int] = {}
    m = _CACHED_META_RE.search(filename)
    if m:
        metadata["variant"] = int(m.group(1))
        if m.group(2) is not None:
            metadata["lineage_seed"] = int(m.group(2))
        if m.group(3) is not None:
            metadata["generation"] = int(m.group(3))
    return metadata


async def handle_get_ray_analysis_status(
    db_service: DatabaseService,
    record: ExperimentAnalysisDTO,
) -> AnalysisRun:
    """Resolve a Ray-native standalone analysis's status (submit_ray_native_analysis).

    There is no persistent job-status API for the backing K8s Job (it has
    ttl_seconds_after_finished=86400, so get_job_status returns None once that
    expires) -- S3-exists is the durable, authoritative READY signal, matching
    analysis-results-design.md's own status-resolution plan. Falls back to a
    live K8s job-status check only to catch an early, hard failure (image pull
    error, scheduling failure) before the manifest could ever be written.
    """
    if record.status in (JobStatus.COMPLETED, JobStatus.FAILED):
        return AnalysisRun(id=record.database_id, status=record.status, error_log=record.error_message)

    file_service = get_file_service()
    manifest_bytes = None
    if file_service is not None and record.result_uri:
        manifest_key = data_layout.key_from_uri(f"{record.result_uri}/_manifest.json")
        manifest_bytes = await file_service.get_file_contents(S3FilePath(s3_path=Path(manifest_key)))
    if manifest_bytes is not None:
        manifest = json.loads(manifest_bytes)
        if manifest.get("written"):
            await db_service.update_analysis_status(
                record.database_id, AnalysisStatusDB.READY, result_uri=record.result_uri
            )
            return AnalysisRun(id=record.database_id, status=JobStatus.COMPLETED)
        error_message = "; ".join(e.get("error", "") for e in manifest.get("errors", [])) or "analysis failed"
        await db_service.update_analysis_status(
            record.database_id, AnalysisStatusDB.FAILED, error_message=error_message
        )
        return AnalysisRun(id=record.database_id, status=JobStatus.FAILED, error_log=error_message)

    sim_service = get_simulation_service()
    if sim_service is not None and record.job_id_ext:
        job_status_info = await sim_service.get_job_status(JobId.k8s(record.job_id_ext))
        if job_status_info is not None and job_status_info.status == JobStatus.FAILED:
            await db_service.update_analysis_status(
                record.database_id, AnalysisStatusDB.FAILED, error_message=job_status_info.error_message
            )
            return AnalysisRun(id=record.database_id, status=JobStatus.FAILED, error_log=job_status_info.error_message)

    return AnalysisRun(id=record.database_id, status=JobStatus.RUNNING)


async def handle_get_analysis_status(
    ref: int | ExperimentAnalysisDTO,
    analysis_service: AnalysisServiceSlurm,
    db_service: DatabaseService,
) -> AnalysisRun:
    """
    If a database_id is passed, an analysis record should NOT Be
    :param ref: (``int<ExperimentAnalysisDTO.database_id> | ExperimentAnalysisDTO>``) One of an instance of
        objects stored and read in the "analyses" db table, OR the database_id of such an aforementioned
        instance. **_NOTE: really, the <POST>/analyses endpoint should pass the DTO to this param, and the
        modular status/other endpoints should pass the db id.
    :param db_service:
    :param analysis_service:
    :return:
    """
    analysis_record: ExperimentAnalysisDTO = (
        await db_service.get_analysis(database_id=ref) if isinstance(ref, int) else ref
    )
    if analysis_record.job_id is None:
        raise ValueError("Analysis record has no job_id")
    async with get_ssh_session_service(SSHTarget.SLURM).session() as ssh:
        return await analysis_service.get_analysis_status(
            job_id=analysis_record.job_id, db_id=analysis_record.database_id, ssh=ssh
        )


async def handle_get_analysis_log(db_service: DatabaseService, id: int) -> str:
    analysis_record = await db_service.get_analysis(database_id=id)
    slurm_logfile = get_settings().slurm_log_base_path / f"{analysis_record.job_name}.out"

    async with get_ssh_session_service(SSHTarget.SLURM).session() as ssh:
        ret, stdout, stdin = await ssh.run_command(f"cat {slurm_logfile!s}")

    return stdout


async def handle_get_analysis(db_service: DatabaseService, id: int) -> ExperimentAnalysisDTO:
    return await db_service.get_analysis(database_id=id)


async def handle_list_analyses(db_service: DatabaseService) -> list[ExperimentAnalysisDTO]:
    return await db_service.list_analyses()


async def handle_get_analysis_plots(db_service: DatabaseService, id: int) -> list[OutputFile]:
    analysis_data = await db_service.get_analysis(database_id=id)
    output_id = analysis_data.name
    return await get_html_outputs_local(output_id=output_id)


async def get_html_outputs_local(output_id: str) -> list[OutputFile]:
    """Run in DEV"""
    settings = get_settings()
    slurm_base = settings.slurm_base_path
    remote_uv_executable = slurm_base / ".local" / "bin" / "uv"
    workspace_dir = slurm_base / "workspace"

    async with get_ssh_session_service(SSHTarget.SLURM).session() as ssh:
        ret, stdin, stdout = await ssh.run_command(
            dedent(f"""
                    cd {workspace_dir} \
                        && {remote_uv_executable} run scripts/html_outputs.py --output_id {output_id}
                """)
        )

    deserialized = json.loads(stdin.replace("'", '"'))
    outputs = []
    for spec in deserialized:
        output = OutputFile(name=spec["name"], content=spec["content"])
        outputs.append(output)
    return outputs
