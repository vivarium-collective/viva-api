"""Request handlers for the compose simulation subsystem."""

import asyncio
import datetime
import json
import logging
import random
import string
import tempfile
import zipfile
from pathlib import Path

from fastapi import BackgroundTasks, HTTPException

from sms_api.common.storage.data_layout import RayLayout
from sms_api.common.storage.file_paths import S3FilePath
from sms_api.common.storage.file_service import FileService
from sms_api.compose.container_def import build_pbg_def
from sms_api.compose.database_service import ComposeDatabaseService
from sms_api.compose.hpc_utils import get_compose_correlation_id, get_compose_experiment_id
from sms_api.compose.job_monitor import ComposeJobMonitor
from sms_api.compose.models import (
    BatchProgress,
    ComposeHpcRun,
    ComposeJobStatus,
    ComposeJobType,
    ComposeRegisteredSimulators,
    ComposeSimulation,
    ComposeSimulationExperiment,
    ComposeSimulationRequest,
    PBAllowList,
    SimulationFileType,
    get_singularity_hash,
)
from sms_api.compose.simulation_service import ComposeSimulationService

logger = logging.getLogger(__name__)


def _check_allow_list(extra_pip_deps: list[str] | None, pb_allow_list: PBAllowList) -> None:
    """Reject any ``extra_pip_deps`` entry not covered by ``pb_allow_list`` before
    the request is persisted or dispatched — no ``pip install`` of an unapproved
    origin ever reaches a container build (SLURM sbatch's ``%post`` or the Ray/Batch
    runner's install step), on shared infra where every job on the queue shares one
    S3 bucket (see plan §3.11/§8: the ``RayBatchJobRole`` grant).
    """
    for dep in extra_pip_deps or []:
        allowed = any(
            dep == spec or dep in spec or spec in dep
            for spec in (s.split("::", 1)[-1] for s in pb_allow_list.allow_list)
        )
        if not allowed:
            raise HTTPException(403, f"'{dep}' is not in the compose allow list")


def _extract_document_content(sim_request: ComposeSimulationRequest) -> str | None:
    """Extract the document content from the uploaded file for persistence.

    For OMEX archives: extracts the first .pbg file found inside the ZIP.
    For standalone .pbg files: reads the JSON content directly.
    For .sbml files: reads the XML content directly.
    """
    file_path = sim_request.request_file_path
    if not file_path.exists():
        return None

    match sim_request.simulation_file_type:
        case SimulationFileType.OMEX:
            try:
                with zipfile.ZipFile(file_path, "r") as zf:
                    for name in zf.namelist():
                        if name.endswith(".pbg"):
                            return zf.read(name).decode("utf-8")
                    # No .pbg found — try .sbml
                    for name in zf.namelist():
                        if name.endswith(".sbml"):
                            return zf.read(name).decode("utf-8")
                    # Fallback: return the file listing
                    return json.dumps({"omex_contents": zf.namelist()})
            except Exception:
                logger.warning("Could not extract document from OMEX archive", exc_info=True)
                return None
        case SimulationFileType.PBG:
            return file_path.read_text()
        case SimulationFileType.SBML:
            return file_path.read_text()


async def get_compose_simulator_versions(db_service: ComposeDatabaseService) -> ComposeRegisteredSimulators:
    simulators = await db_service.get_simulator_db().list_simulators()
    return ComposeRegisteredSimulators(versions=simulators)


async def run_compose_simulation(
    simulation_request: ComposeSimulationRequest,
    database_service: ComposeDatabaseService,
    simulation_service: ComposeSimulationService,
    job_monitor: ComposeJobMonitor,
    pb_allow_list: PBAllowList,
    background_tasks: BackgroundTasks,
    extra_pip_deps: list[str] | None = None,
    override_command: str | None = None,
) -> ComposeSimulationExperiment:
    _check_allow_list(extra_pip_deps, pb_allow_list)

    suffix = simulation_request.simulation_file_type.get_files_suffix()
    singularity_rep = build_pbg_def(suffix, extra_pip_deps=extra_pip_deps)

    simulator_db = database_service.get_simulator_db()
    simulator_version = await simulator_db.get_simulator_by_def_hash(get_singularity_hash(singularity_rep))
    if simulator_version is None:
        simulator_version = await simulator_db.insert_simulator(singularity_rep)

    random_string = "".join(random.choices(string.hexdigits, k=7))
    experiment_id = get_compose_experiment_id(simulator=simulator_version, random_str=random_string)

    # Extract and persist the document content for later retrieval
    document_content = _extract_document_content(simulation_request)

    simulation = await simulator_db.insert_simulation(
        sim_request=simulation_request,
        experiment_id=experiment_id,
        simulator_version=simulator_version,
        document=document_content,
    )

    # Insert the durable status row BEFORE returning 200 — not in the background task — so a
    # `GET /simulation/{id}/status` right after submit sees QUEUED instead of 404ing (which was
    # indistinguishable from "never started"), even if `perform_job` itself never runs.
    hpc_db = database_service.get_hpc_db()
    placeholder_hpcrun = await hpc_db.insert_hpcrun(
        slurmjobid=-1,
        job_type=ComposeJobType.SIMULATION,
        ref_id=simulation.database_id,
        correlation_id=get_compose_correlation_id(random_string=random_string, job_type=ComposeJobType.SIMULATION),
    )

    async def perform_job() -> None:
        try:
            await _dispatch_compose_job(
                database_service=database_service,
                job_monitor=job_monitor,
                simulation_service=simulation_service,
                simulation=simulation,
                experiment_id=experiment_id,
                hpcrun_id=placeholder_hpcrun.database_id,
                override_command=override_command,
            )
        except Exception as exc:
            logger.exception("Compose dispatch failed for simulation %s", simulation.database_id)
            await hpc_db.mark_hpcrun_failed(placeholder_hpcrun.database_id, str(exc))

    background_tasks.add_task(perform_job)

    return ComposeSimulationExperiment(
        simulation_database_id=simulation.database_id,
        simulator_database_id=simulator_version.database_id,
    )


async def run_compose_curated(
    templated_pbif: str,
    simulator_name: str,
    background_tasks: BackgroundTasks,
    loaded_sbml: Path,
    db_service: ComposeDatabaseService,
    sim_service: ComposeSimulationService,
    job_monitor: ComposeJobMonitor,
    use_interesting: bool = True,
) -> ComposeSimulationExperiment:
    with tempfile.TemporaryDirectory(delete=False) as tmp_dir:
        with zipfile.ZipFile(tmp_dir + "/input.omex", "w") as omex:
            omex.writestr(data=templated_pbif, zinfo_or_arcname=f"{simulator_name}.pbg")
            if use_interesting:
                omex.write(loaded_sbml.absolute(), arcname="interesting.sbml")
            else:
                omex.write(loaded_sbml.absolute(), arcname=loaded_sbml.name)
        if omex.filename is None:
            raise HTTPException(500, "Can't create omex file.")
        simulation_request = ComposeSimulationRequest(
            request_file_path=Path(omex.filename), simulation_file_type=SimulationFileType.OMEX, is_batch=False
        )
        return await run_compose_simulation(
            simulation_request=simulation_request,
            database_service=db_service,
            simulation_service=sim_service,
            job_monitor=job_monitor,
            pb_allow_list=PBAllowList(allow_list=[]),
            background_tasks=background_tasks,
        )


_V2ECOLI_GIT_URL = "git+https://github.com/vivarium-collective/v2ecoli.git"


async def run_compose_v2ecoli(
    templated_pbif: str,
    duration: float,
    background_tasks: BackgroundTasks,
    db_service: ComposeDatabaseService,
    sim_service: ComposeSimulationService,
    job_monitor: ComposeJobMonitor,
    cache_dir: str = "out/cache",
    seed: int = 0,
    features: list[str] | None = None,
) -> ComposeSimulationExperiment:
    """Run a v2ecoli simulation — no SBML upload needed, just PBG template + duration."""
    with tempfile.TemporaryDirectory(delete=False) as tmp_dir:
        omex_path = Path(tmp_dir) / "input.omex"
        with zipfile.ZipFile(omex_path, "w") as omex:
            omex.writestr(data=templated_pbif, zinfo_or_arcname="v2ecoli.pbg")
        simulation_request = ComposeSimulationRequest(
            request_file_path=omex_path,
            simulation_file_type=SimulationFileType.OMEX,
            end_time_point=duration,
            is_batch=False,
        )
        return await run_compose_simulation(
            simulation_request=simulation_request,
            database_service=db_service,
            simulation_service=sim_service,
            job_monitor=job_monitor,
            # curated call site: the only extra dep it ever requests is its own trusted
            # v2ecoli origin, so its allow list is exactly that (not the global DB-backed
            # one users' uploads are checked against).
            pb_allow_list=PBAllowList(allow_list=[_V2ECOLI_GIT_URL]),
            background_tasks=background_tasks,
            extra_pip_deps=[_V2ECOLI_GIT_URL],
            override_command=json.dumps({
                "mode": "v2ecoli",
                "cache_dir": cache_dir,
                "seed": seed,
                "features": features or [],
            }),
        )


async def _dispatch_compose_job(
    database_service: ComposeDatabaseService,
    job_monitor: ComposeJobMonitor,
    simulation_service: ComposeSimulationService,
    simulation: ComposeSimulation,
    experiment_id: str,
    hpcrun_id: int,
    override_command: str | None = None,
) -> None:
    simulator_version = simulation.simulator_version
    hpc_db = database_service.get_hpc_db()
    simulator_hpc_id = await hpc_db.get_hpcrun_id_by_simulator_id(simulator_id=simulator_version.database_id)
    random_string = "".join(random.choices(string.hexdigits, k=7))

    if simulation_service.requires_container_build and simulator_hpc_id is None:
        hpc_run = await simulation_service.build_container(
            simulator_version=simulator_version, random_str=random_string, db_service=database_service
        )
        job_queue: asyncio.Queue[ComposeHpcRun] = asyncio.Queue()
        job_monitor.internal_subscribe(job_queue, hpc_run.slurmjobid)
        wait_time = 0
        current_status = hpc_run.status
        while current_status != ComposeJobStatus.COMPLETED:
            wait_time += 1
            try:
                current_status = (await asyncio.wait_for(job_queue.get(), timeout=60)).status
            except TimeoutError:
                latest = await hpc_db.get_hpcrun_by_slurmjobid(hpc_run.slurmjobid)
                if latest is None:
                    raise RuntimeError(
                        f"Can't get HPC Run for container build {simulator_version.singularity_def_hash}"
                    )
                current_status = latest.status
            if current_status == ComposeJobStatus.FAILED:
                raise RuntimeError(f"Building container for simulator {simulator_version.database_id} failed.")
            elif wait_time == 30:
                raise RuntimeError(f"Container build for simulator {simulator_version.database_id} timed out.")
        job_monitor.internal_unsubscribe(hpc_run.slurmjobid)

    sim_job_id_ext = await simulation_service.submit_simulation_job(
        simulation=simulation, experiment_id=experiment_id, override_command=override_command
    )
    # Attach the real backend job id to the placeholder row `run_compose_simulation` already
    # inserted synchronously at submit time (§3.13) — not a second insert.
    await hpc_db.update_hpcrun_dispatch(
        hpcrun_id,
        job_id_ext=sim_job_id_ext,
        backend=simulation_service.backend,
        status=ComposeJobStatus.RUNNING,
    )


# ---------------------------------------------------------------------------
# Batch progress (live, computed from the S3 hive-partitioned output)
# ---------------------------------------------------------------------------

# The generation-depth probe samples this many lineages so the endpoint's S3 cost is
# constant regardless of sweep size (a 1000-seed run must not cost 1000 listings).
_PROGRESS_SAMPLE_LINEAGES = 8
# The emitter nests output a couple of levels under the experiment prefix; bound the
# descent that locates the parquet history hive so a malformed tree can't loop.
_HIVE_LOCATE_MAX_DEPTH = 8


def _leaf(prefix: str) -> str:
    """Last path segment of a bucket-relative ``.../foo/`` prefix (trailing ``/`` ignored)."""
    return prefix.rstrip("/").rsplit("/", 1)[-1]


def _find_first_int(obj: object, key: str) -> int:
    """First int-coercible value stored under ``key`` anywhere in a nested dict/list."""
    stack: list[object] = [obj]
    while stack:
        current = stack.pop()
        if isinstance(current, dict):
            if key in current:
                try:
                    return int(current[key])
                except (TypeError, ValueError):
                    pass
            stack.extend(current.values())
        elif isinstance(current, list):
            stack.extend(current)
    return 0


def _extract_batch_dims(document: str | None) -> tuple[int, int]:
    """Parse ``(n_seeds, n_generations)`` from a stored PBG document.

    Prefers the canonical composite keys ``n_seeds``/``n_generations`` and falls back
    to the vEcoli aliases ``n_init_sims``/``generations``. Returns ``(0, 0)`` when
    absent so the endpoint still reports live counts (with unknown totals) instead of
    failing on a non-batch document.
    """
    if not document:
        return 0, 0
    try:
        data = json.loads(document)
    except (json.JSONDecodeError, ValueError):
        return 0, 0
    seeds = _find_first_int(data, "n_seeds") or _find_first_int(data, "n_init_sims")
    generations = _find_first_int(data, "n_generations") or _find_first_int(data, "generations")
    return seeds, generations


async def _find_variant_prefix(file_service: FileService, root_key: str) -> str | None:
    """Descend to the parquet history hive's ``…/variant=<V>/`` level under a run.

    The emitter nests output under the composite/experiment name (e.g.
    ``{exp}/{runner}/{runner}/history/experiment_id=…/variant=…/``), so rather than
    hard-code a depth we walk toward ``history`` → ``experiment_id=`` → ``variant=``,
    stepping over per-lineage ``*.zarr`` stores. Bounded by ``_HIVE_LOCATE_MAX_DEPTH``.
    Returns ``None`` when no output has landed yet.
    """
    frontier = root_key.rstrip("/") + "/"
    for _ in range(_HIVE_LOCATE_MAX_DEPTH):
        children = await file_service.list_prefixes(S3FilePath(s3_path=Path(frontier)))
        if not children:
            return None
        variant = next((c for c in children if _leaf(c).startswith("variant=")), None)
        if variant is not None:
            return variant
        # Advance one level: prefer a 'history'/'experiment_id=' child, else the single
        # non-zarr, non-partition nesting dir (the runner name the emitter injects).
        nxt = next(
            (c for c in children if _leaf(c) == "history" or _leaf(c).startswith("experiment_id=")),
            None,
        )
        if nxt is None:
            dirs = [c for c in children if not _leaf(c).endswith(".zarr") and "=" not in _leaf(c)]
            if not dirs:
                return None
            nxt = dirs[0]
        frontier = nxt
    return None


def _generation_indices(prefixes: list[str]) -> list[int]:
    """Numeric ``G`` of every ``generation=<G>/`` entry in a prefix listing."""
    out: list[int] = []
    for prefix in prefixes:
        leaf = _leaf(prefix)
        if leaf.startswith("generation=") and leaf.split("=", 1)[1].isdigit():
            out.append(int(leaf.split("=", 1)[1]))
    return out


async def _max_generation(file_service: FileService, lineage_prefix: str) -> int:
    """Generations *reached* by one lineage = ``max(generation index) + 1`` beneath it.

    Handles both observed hive shapes: ``generation=<G>/`` sitting directly under
    ``lineage_seed=<N>/`` (this run), and the column-nested
    ``lineage_seed=<N>/<column>/generation=<G>/`` (a column carries the same
    generation partitions, so the first with any suffices). ``generation=0..k``
    present ⟹ ``k + 1`` generations done.
    """
    children = await file_service.list_prefixes(S3FilePath(s3_path=Path(lineage_prefix)))
    direct = _generation_indices(children)
    if direct:
        return max(direct) + 1
    for column in children:
        indices = _generation_indices(await file_service.list_prefixes(S3FilePath(s3_path=Path(column))))
        if indices:
            return max(indices) + 1
    return 0


async def _scan_batch_output(
    file_service: FileService, experiment_id: str, sample_size: int = _PROGRESS_SAMPLE_LINEAGES
) -> tuple[int, int, float]:
    """Return ``(started_lineages, deepest_generation, mean_generations)`` for a run.

    ``started`` (lineage seeds that have emitted) and ``deepest`` are cheap; the
    per-lineage ``mean`` used for the whole-sweep percent is estimated from an evenly
    spaced sample so the call stays O(1) in listings no matter how large the sweep.
    """
    root_key = RayLayout.experiment_prefix(experiment_id)
    variant_prefix = await _find_variant_prefix(file_service, root_key)
    if variant_prefix is None:
        return 0, 0, 0.0
    lineage_prefixes = [
        c
        for c in await file_service.list_prefixes(S3FilePath(s3_path=Path(variant_prefix)))
        if _leaf(c).startswith("lineage_seed=")
    ]
    started = len(lineage_prefixes)
    if started == 0:
        return 0, 0, 0.0
    # Evenly spaced sample so both early (deep) and late (shallow) lineages are represented.
    step = max(1, started // max(1, sample_size))
    sample = lineage_prefixes[::step][:sample_size]
    generations = [await _max_generation(file_service, lineage) for lineage in sample]
    deepest = max(generations) if generations else 0
    mean_generations = (sum(generations) / len(generations)) if generations else 0.0
    return started, deepest, mean_generations


async def get_batch_progress(
    simulation_id: int,
    db_service: ComposeDatabaseService,
    file_service: FileService,
) -> BatchProgress:
    """Compute live :class:`BatchProgress` for a compose batch from its S3 output.

    Totals (``n_seeds`` x ``n_generations``) come from the stored PBG document; live
    counts come from a bounded, delimited walk of the hive-partitioned output the
    Ray/Batch entrypoint syncs to S3 as the run proceeds. Purely additive and
    read-only, so it is safe to poll against a run in flight.
    """
    try:
        experiment_id = await db_service.get_simulator_db().get_simulations_experiment_id(simulation_id)
    except LookupError:
        raise HTTPException(404, f"Compose simulation {simulation_id} not found")

    hpc_run = await db_service.get_hpc_db().get_hpcrun_by_ref(ref_id=simulation_id, job_type=ComposeJobType.SIMULATION)
    document = await db_service.get_simulator_db().get_simulation_document(simulation_id)
    n_seeds, n_generations = _extract_batch_dims(document)

    started, deepest, mean_generations = await _scan_batch_output(file_service, experiment_id)

    total_cell_generations = n_seeds * n_generations
    overall = (
        round(min(100.0, started * mean_generations / total_cell_generations * 100.0), 2)
        if total_cell_generations
        else 0.0
    )

    elapsed = 0.0
    if hpc_run is not None and hpc_run.start_time:
        try:
            started_at = datetime.datetime.fromisoformat(hpc_run.start_time)
            elapsed = max(0.0, (datetime.datetime.now() - started_at).total_seconds())
        except ValueError:
            elapsed = 0.0

    return BatchProgress(
        lineages=f"{started}:{n_seeds}",
        generations=f"{deepest}:{n_generations}",
        overall=overall,
        time_elapsed=round(elapsed, 1),
        status=hpc_run.status if hpc_run is not None else None,
    )
