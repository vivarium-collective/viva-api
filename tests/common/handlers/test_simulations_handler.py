import asyncio
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from fastapi import HTTPException

from viva_api.analysis.models import TsvOutputFile
from viva_api.common.dispatch_validation import DispatchValidationError
from viva_api.common.handlers.simulations import (
    _S3_DOWNLOAD_CONCURRENCY,
    SimulationAnalysisResponseType,
    _download_outputs_from_s3,
    _run_standalone_analysis_ray_native,
    fetch_omics_outputs,
    get_available_omics_output_paths,
    run_simulation_workflow,
)
from viva_api.common.models import JobId, JobStatus
from viva_api.common.simulator_defaults import RepoUrl
from viva_api.common.ssh.ssh_service import SSHSessionService
from viva_api.common.storage.file_paths import HPCFilePath, S3FilePath
from viva_api.common.storage.file_service import FileService, ListingItem
from viva_api.config import ComputeBackend, get_settings
from viva_api.dependencies import get_file_service, set_file_service
from viva_api.simulation.models import (
    AnalysisOptions,
    HpcRun,
    JobType,
    Simulation,
    SimulationConfig,
    SimulatorVersion,
)
from viva_api.simulation.simulation_service_k8s import SimulationServiceK8s
from viva_api.simulation.simulation_service_ray import SimulationServiceRay
from viva_api.simulation.tables_orm import ORMAnalysis


@pytest.mark.integration
@pytest.mark.skipif(not Path(get_settings().slurm_submit_key_path).exists(), reason="slurm ssh key file not supplied")
@pytest.mark.asyncio
async def test_get_available_omics_output_paths(
    ssh_session_service: SSHSessionService, analysis_outdir: HPCFilePath
) -> None:
    results = await get_available_omics_output_paths(remote_analysis_outdir=analysis_outdir)
    assert len(results), "No files found."
    assert all([isinstance(fp, HPCFilePath) and fp.remote_path.__str__().endswith(".txt") for fp in results])


@pytest.mark.integration
@pytest.mark.skipif(not Path(get_settings().slurm_submit_key_path).exists(), reason="slurm ssh key file not supplied")
@pytest.mark.asyncio
async def test_fetch_simulation_omics_outputs(
    ssh_session_service: SSHSessionService, analysis_outdir: HPCFilePath
) -> None:
    results: list[TsvOutputFile] = await fetch_omics_outputs(  # type: ignore[assignment]
        exp_analysis_outdir=analysis_outdir, output_type=SimulationAnalysisResponseType.DATA_CONTENT
    )
    assert len(results)


# ---------------------------------------------------------------------------
# _download_outputs_from_s3 — concurrency & failure-resilience unit tests
#
# These tests verify the fix for the 504 Gateway Timeout on
# `atlantis simulation outputs` for the 10k-cell simulation.  The server-side
# download loop used to create one S3 client per file sequentially, which
# took longer than the reverse-proxy idle timeout for large archives.  The
# fix parallelizes downloads with a bounded semaphore.
# ---------------------------------------------------------------------------


class _FakeFileService(FileService):
    """Minimal in-memory FileService stub for unit testing."""

    def __init__(
        self,
        listing: list[ListingItem],
        per_download_sleep: float = 0.0,
        fail_keys: set[str] | None = None,
    ) -> None:
        self._listing = listing
        self._per_download_sleep = per_download_sleep
        self._fail_keys = fail_keys or set()
        self.downloads: list[str] = []
        self._active = 0
        self.max_active = 0
        self._lock = asyncio.Lock()

    async def download_file(self, s3_path: S3FilePath, file_path: Path | None = None) -> tuple[S3FilePath, str]:
        async with self._lock:
            self._active += 1
            self.max_active = max(self.max_active, self._active)
        try:
            if str(s3_path.s3_path) in self._fail_keys:
                raise RuntimeError(f"simulated S3 failure for {s3_path.s3_path}")
            if self._per_download_sleep:
                await asyncio.sleep(self._per_download_sleep)
            if file_path is not None:
                file_path.parent.mkdir(parents=True, exist_ok=True)
                file_path.write_bytes(b"fake-content")
            self.downloads.append(str(s3_path.s3_path))
            return s3_path, str(file_path)
        finally:
            async with self._lock:
                self._active -= 1

    async def upload_file(self, file_path: Path, s3_path: S3FilePath) -> S3FilePath:  # pragma: no cover
        raise NotImplementedError

    async def upload_bytes(self, file_contents: bytes, s3_path: S3FilePath) -> S3FilePath:  # pragma: no cover
        raise NotImplementedError

    async def get_modified_date(self, s3_path: S3FilePath) -> datetime:  # pragma: no cover
        return datetime.now(UTC)

    async def get_listing(self, s3_path: S3FilePath) -> list[ListingItem]:
        prefix = str(s3_path.s3_path)
        if not prefix.endswith("/"):
            prefix = prefix + "/"
        return [item for item in self._listing if item.Key.startswith(prefix)]

    async def get_file_contents(self, s3_path: S3FilePath) -> bytes | None:  # pragma: no cover
        return b"fake-content"

    async def delete_file(self, s3_path: S3FilePath) -> None:  # pragma: no cover
        pass

    async def close(self) -> None:
        pass


def _make_listing(experiment_prefix: str, n_files: int) -> list[ListingItem]:
    """Build a fake S3 listing with ``n_files`` .tsv entries + one workflow_config.json."""
    now = datetime.now(UTC)
    items: list[ListingItem] = []
    for i in range(n_files):
        items.append(
            ListingItem(
                Key=f"{experiment_prefix}/analyses/variant=0/plots/analysis={i}/output.tsv",
                LastModified=now,
                ETag=f"etag-{i}",
                Size=100,
            )
        )
    # A non-accepted extension should be filtered out
    items.append(
        ListingItem(
            Key=f"{experiment_prefix}/analyses/variant=0/plots/ignored.csv",
            LastModified=now,
            ETag="etag-ignored",
            Size=50,
        )
    )
    # workflow_config.json at experiment root (listed under the analyses prefix shouldn't match;
    # the real handler fetches it by exact key, so listing it here is not required)
    return items


@pytest_asyncio.fixture()
async def _swap_file_service() -> AsyncGenerator[None, Any]:
    saved = get_file_service()
    yield
    set_file_service(saved)


@pytest.mark.asyncio
async def test_download_outputs_from_s3_parallelizes(tmp_path: Path, _swap_file_service: None) -> None:
    """Downloads should run concurrently, with concurrency bounded by the semaphore."""
    experiment_id = "test-exp"
    settings = get_settings()
    experiment_prefix = f"{settings.s3_output_prefix}/{experiment_id}/{experiment_id}"
    n_files = _S3_DOWNLOAD_CONCURRENCY * 2 + 5  # enough to saturate the semaphore
    listing = _make_listing(experiment_prefix, n_files=n_files)

    fake = _FakeFileService(listing=listing, per_download_sleep=0.05)
    set_file_service(fake)

    local_cache = tmp_path / experiment_id
    local_cache.mkdir()

    await _download_outputs_from_s3(experiment_id, local_cache)

    # Only .tsv files should have been downloaded; the .csv is filtered out.
    assert len(fake.downloads) == n_files + 1  # +1 for workflow_config.json attempt
    # workflow_config.json is downloaded last (separate path); the .csv should never have been attempted
    assert all(not k.endswith(".csv") for k in fake.downloads)
    # Concurrency should have actually been exercised (more than 1 in-flight)
    assert fake.max_active > 1, "downloads did not run concurrently"
    # And must be bounded by the semaphore
    assert fake.max_active <= _S3_DOWNLOAD_CONCURRENCY


@pytest.mark.asyncio
async def test_download_outputs_from_s3_tolerates_partial_failures(tmp_path: Path, _swap_file_service: None) -> None:
    """A handful of failed files should not abort the whole batch."""
    experiment_id = "test-exp-fail"
    settings = get_settings()
    experiment_prefix = f"{settings.s3_output_prefix}/{experiment_id}/{experiment_id}"
    listing = _make_listing(experiment_prefix, n_files=10)

    # Fail 3 specific files
    fail_keys = {
        f"{experiment_prefix}/analyses/variant=0/plots/analysis=2/output.tsv",
        f"{experiment_prefix}/analyses/variant=0/plots/analysis=5/output.tsv",
        f"{experiment_prefix}/analyses/variant=0/plots/analysis=8/output.tsv",
    }
    fake = _FakeFileService(listing=listing, fail_keys=fail_keys)
    set_file_service(fake)

    local_cache = tmp_path / experiment_id
    local_cache.mkdir()

    # Should not raise — failures are logged and the handler continues
    await _download_outputs_from_s3(experiment_id, local_cache)

    # 10 tsvs were attempted; the 3 failing ones did not write files
    successful_tsvs = [k for k in fake.downloads if k.endswith(".tsv") and k not in fail_keys]
    assert len(successful_tsvs) == 10 - 3

    # Files that succeeded should exist on disk
    for i in range(10):
        key = f"{experiment_prefix}/analyses/variant=0/plots/analysis={i}/output.tsv"
        relative = Path(key).relative_to(experiment_prefix)
        local_file = local_cache / relative
        if key in fail_keys:
            assert not local_file.exists()
        else:
            assert local_file.exists(), f"expected {local_file} to exist"


@pytest.mark.asyncio
async def test_download_outputs_from_s3_skips_cached_files(tmp_path: Path, _swap_file_service: None) -> None:
    """Already-present files should not be re-downloaded."""
    experiment_id = "test-exp-cached"
    settings = get_settings()
    experiment_prefix = f"{settings.s3_output_prefix}/{experiment_id}/{experiment_id}"
    listing = _make_listing(experiment_prefix, n_files=5)

    fake = _FakeFileService(listing=listing)
    set_file_service(fake)

    local_cache = tmp_path / experiment_id
    local_cache.mkdir()

    # Pre-create 2 of the 5 files — they should be skipped on download
    cached_keys = {
        f"{experiment_prefix}/analyses/variant=0/plots/analysis=1/output.tsv",
        f"{experiment_prefix}/analyses/variant=0/plots/analysis=3/output.tsv",
    }
    for key in cached_keys:
        rel = Path(key).relative_to(experiment_prefix)
        local = local_cache / rel
        local.parent.mkdir(parents=True, exist_ok=True)
        local.write_bytes(b"already-cached")

    await _download_outputs_from_s3(experiment_id, local_cache)

    downloaded_tsvs = [k for k in fake.downloads if k.endswith(".tsv")]
    assert len(downloaded_tsvs) == 5 - len(cached_keys)
    assert all(k not in cached_keys for k in downloaded_tsvs)


def _make_ray_simulation(out_uri: str = "s3://bucket/vecoli-output/exp123", n_seeds: int = 2) -> Simulation:
    # emitter_arg/n_init_sims are extra="allow" fields, not in the strict model
    config = SimulationConfig(experiment_id="exp123", emitter_arg={"out_uri": out_uri}, n_init_sims=n_seeds)  # type: ignore[call-arg]
    return Simulation(
        database_id=115,
        simulator_id=53,
        parca_dataset_id=63,
        config=config,
        simulation_config_filename="api_simulation_default.json",
        experiment_id="exp123",
    )


@pytest.mark.asyncio
async def test_run_standalone_analysis_ray_native_routes_to_v2ecoli_job() -> None:
    """A simulator on sms-ecoli/v2ecoli must submit via submit_ray_native_analysis
    (the v2ecoli:<commit> image), never the legacy vecoli:<commit>-amd64-submit path
    that is confirmed to never work for this pipeline (ImagePullBackOff, live-verified)."""
    simulation = _make_ray_simulation()
    simulator = SimulatorVersion(
        database_id=53,
        git_commit_hash="deadbeef",
        git_repo_url=RepoUrl.SMS_ECOLI_REPO_URL,
        git_branch="main",
    )

    mock_k8s_service = AsyncMock(spec=SimulationServiceK8s)
    mock_k8s_service.submit_ray_native_analysis.return_value = "ana-exp123"
    mock_db_service = AsyncMock()
    mock_db_service.record_analysis.return_value = SimpleNamespace(database_id=42)

    with patch("viva_api.common.handlers.simulations.get_simulation_service", return_value=mock_k8s_service):
        result = await _run_standalone_analysis_ray_native(
            database_service=mock_db_service,
            simulation=simulation,
            simulator=simulator,
            modules={"multiseed": {"doubling_time_distribution": {}}},
        )

    mock_k8s_service.submit_ray_native_analysis.assert_called_once()
    call_kwargs = mock_k8s_service.submit_ray_native_analysis.call_args.kwargs
    assert call_kwargs["experiment_id"] == "exp123"
    assert call_kwargs["commit"] == "deadbeef"
    assert call_kwargs["params"]["out_uri"] == "s3://bucket/vecoli-output/exp123"
    assert call_kwargs["params"]["n_seeds"] == 2
    assert call_kwargs["params"]["modules"] == {"multiseed": {"doubling_time_distribution": {}}}
    # regression: ORMAnalysis.to_dto() unconditionally reads config["analysis_options"]
    # (AnalysisConfigOptions requires experiment_id) -- this producer must write that
    # shape too, matching the legacy Batch/SLURM producers in run_standalone_analysis(),
    # or GET /analyses/{id} 500s with a raw KeyError for every Ray-native analysis.
    assert call_kwargs["params"]["analysis_options"] == {
        "experiment_id": ["exp123"],
        "multiseed": {"doubling_time_distribution": {}},
    }
    assert result["config"] == call_kwargs["params"]
    assert result["database_id"] == 42

    mock_db_service.record_analysis.assert_called_once()
    record_kwargs = mock_db_service.record_analysis.call_args.kwargs
    assert record_kwargs["experiment_id"] == "exp123"
    assert record_kwargs["simulation_id"] == 115
    assert record_kwargs["backend"] == "ray"
    assert record_kwargs["job_id_ext"] == "ana-exp123"
    assert record_kwargs["result_uri"].startswith("s3://bucket/vecoli-output/exp123/analyses/")
    # The actual reported bug: to_dto() must not raise on this producer's config shape.
    orm_row = ORMAnalysis(
        id=42,
        name="ana-exp123",
        config=call_kwargs["params"],
        last_updated=datetime.now(UTC).isoformat(),
        experiment_id="exp123",
        simulation_id=115,
        backend="ray",
    )
    dto = orm_row.to_dto()
    assert dto.config.analysis_options.experiment_id == ["exp123"]


async def _run_default_modules_ray_native(simulation: Simulation) -> dict[str, Any]:
    """Drive the REAL default-selection path (modules=None) end to end for a
    Ray/sms-ecoli simulator, returning the submitted params."""
    from viva_api.common.handlers.simulations import run_standalone_analysis

    simulator = SimulatorVersion(
        database_id=53,
        git_commit_hash="deadbeef",
        git_repo_url=RepoUrl.SMS_ECOLI_REPO_URL,
        git_branch="main",
    )
    mock_k8s_service = AsyncMock(spec=SimulationServiceK8s)
    mock_k8s_service.submit_ray_native_analysis.return_value = "ana-exp123"
    mock_db_service = AsyncMock()
    mock_db_service.get_simulation.return_value = simulation
    mock_db_service.get_simulator.return_value = simulator
    mock_db_service.record_analysis.return_value = SimpleNamespace(database_id=42)

    with (
        patch("viva_api.common.handlers.simulations.get_job_backend", return_value=ComputeBackend.BATCH),
        patch("viva_api.common.handlers.simulations.get_simulation_service", return_value=mock_k8s_service),
    ):
        result = await run_standalone_analysis(
            database_service=mock_db_service,
            simulation_id=115,
            modules=None,
        )
    return dict(result["config"])


@pytest.mark.asyncio
async def test_ray_native_default_modules_defer_to_the_images_own_resolver() -> None:
    """Regression, twice over.

    (1) The old ptools_* default (modules=None) nested all three modules under
    "multiseed", but ptools_rna/ptools_rxns/ptools_proteins are registered
    scale="single" -- every dispatch relying on the default failed with "is
    scale='single', not 'multiseed'" (live-reproduced 2026-08-05, 5 K8s Job
    attempts over 22h, all Failed). A hardcoded name->scale list in sms-api can
    always drift from the image's registry, which is what made that bug possible.

    (2) That default also silently under-delivered: three ptools modules, none of
    the cd1_* omics suite the deliverable is defined by.

    Both are closed by deferring to the model image's own resolver -- the same
    "applicable" keyword the composite's inline flush uses -- so sms-api never
    restates a name, a scale, or a list it cannot verify. Scale correctness is
    then true by construction.
    """
    params = await _run_default_modules_ray_native(_make_ray_simulation())

    assert params["modules"] == "applicable"
    # n_generations rides along: "applicable" cannot decide whether the
    # multigeneration scales apply without it.
    assert params["n_generations"] == 1
    # The DTO contract still holds when modules is a keyword rather than a mapping.
    assert params["analysis_options"] == {"experiment_id": ["exp123"]}


@pytest.mark.asyncio
async def test_ray_native_default_modules_prefer_the_simulations_own_options() -> None:
    """A simulation dispatched WITH analysis_options (the workbench sends a study's
    spec.analyses through) must have those re-run on demand, not a generic default
    -- and must match what the dispatch DAG's own analysis node would run."""
    simulation = _make_ray_simulation()
    simulation.config.analysis_options = AnalysisOptions.model_validate({
        "multiseed": {"cd1_proteomics": {"generation_lower_bound": 5}}
    })
    simulation.config.generations = 10

    params = await _run_default_modules_ray_native(simulation)

    assert params["modules"] == {"multiseed": {"cd1_proteomics": {"generation_lower_bound": 5}}}
    assert params["n_generations"] == 10
    assert params["analysis_options"] == {
        "experiment_id": ["exp123"],
        "multiseed": {"cd1_proteomics": {"generation_lower_bound": 5}},
    }


@pytest.mark.asyncio
async def test_run_standalone_analysis_ray_native_requires_out_uri() -> None:
    """A simulation with no emitter_arg.out_uri was never dispatched via the Ray/xarray
    pipeline -- fail loudly rather than submit a job with nowhere to read data from."""
    simulation = _make_ray_simulation(out_uri="")
    simulator = SimulatorVersion(
        database_id=53,
        git_commit_hash="deadbeef",
        git_repo_url=RepoUrl.V2ECOLI_REPO_URL,
        git_branch="main",
    )

    with pytest.raises(ValueError, match="emitter_arg.out_uri"):
        await _run_standalone_analysis_ray_native(
            database_service=AsyncMock(),
            simulation=simulation,
            simulator=simulator,
            modules={},
        )


def _make_chain_campaign_hpc_run(
    status: JobStatus = JobStatus.RUNNING,
    *,
    error_message: str | None = None,
    chain_current_job_ids: list[str | None] | None = None,
    chain_final_job_ids: list[str] | None = None,
) -> HpcRun:
    return HpcRun(
        database_id=300,
        job_id=JobId.ray("parca-job-abc"),
        correlation_id="N/A",
        job_type=JobType.SIMULATION,
        ref_id=214,
        status=status,
        error_message=error_message,
        chain_n_generations=10,
        chain_final_job_ids=chain_final_job_ids
        if chain_final_job_ids is not None
        else ["seed0-gen9-job", "seed1-gen9-job"],
        chain_current_job_ids=chain_current_job_ids if chain_current_job_ids is not None else [None, None],
        chain_current_generation=[None, None],
        chain_parca_done=True,
    )


@pytest.mark.asyncio
async def test_get_simulation_status_chain_campaign_trusts_the_row_status_directly() -> None:
    """Backlog item 71 Phase 4: JobScheduler's own poll loop (via
    DatabaseService.advance_chain_campaign) is the ONLY writer of a campaign
    row's status now -- get_simulation_status must trust hpc_run.status
    directly, the same way the non-campaign path trusts a freshly-polled
    job's status, and must never write to the DB itself. This supersedes the
    ORIGINAL 2026-08-10 regression fix (which re-derived status live via
    get_chain_campaign_result on chain_final_job_ids): under Phase 4 that list
    is filled INCREMENTALLY as each seed resolves, so re-deriving from it
    would always look "terminal" for whatever partial subset has resolved so
    far -- unable to distinguish a few seeds done from the whole campaign
    done. Trusting the row's own status (written exactly once, atomically, by
    the scheduler) avoids that bug entirely."""
    from viva_api.common.handlers.simulations import get_simulation_status

    hpc_run = _make_chain_campaign_hpc_run(status=JobStatus.RUNNING)
    mock_db_service = AsyncMock()
    mock_db_service.get_simulation.return_value = SimpleNamespace(database_id=214)
    mock_db_service.get_hpcrun_by_ref.return_value = hpc_run

    result = await get_simulation_status(db_service=mock_db_service, id=214)

    assert result.status == JobStatus.RUNNING
    mock_db_service.update_hpcrun_status.assert_not_called()


@pytest.mark.asyncio
async def test_get_simulation_status_chain_campaign_terminal_success_reports_completed() -> None:
    from viva_api.common.handlers.simulations import get_simulation_status

    hpc_run = _make_chain_campaign_hpc_run(status=JobStatus.COMPLETED)
    mock_db_service = AsyncMock()
    mock_db_service.get_simulation.return_value = SimpleNamespace(database_id=214)
    mock_db_service.get_hpcrun_by_ref.return_value = hpc_run

    result = await get_simulation_status(db_service=mock_db_service, id=214)

    assert result.status == JobStatus.COMPLETED
    mock_db_service.update_hpcrun_status.assert_not_called()


@pytest.mark.asyncio
async def test_get_simulation_status_chain_campaign_terminal_zero_succeeded_reports_failed() -> None:
    """Every tracked seed chain failed -- reports the row's own FAILED status
    and explanatory message (written by the scheduler), still without writing
    the DB itself."""
    from viva_api.common.handlers.simulations import get_simulation_status

    hpc_run = _make_chain_campaign_hpc_run(
        status=JobStatus.FAILED, error_message="chain dispatch: zero seed chains succeeded"
    )
    mock_db_service = AsyncMock()
    mock_db_service.get_simulation.return_value = SimpleNamespace(database_id=214)
    mock_db_service.get_hpcrun_by_ref.return_value = hpc_run

    result = await get_simulation_status(db_service=mock_db_service, id=214)

    assert result.status == JobStatus.FAILED
    assert result.error_message == "chain dispatch: zero seed chains succeeded"
    mock_db_service.update_hpcrun_status.assert_not_called()


@pytest.mark.asyncio
async def test_get_simulation_status_non_chain_run_unaffected() -> None:
    """A plain (non-chain-campaign) simulation must keep the original behavior exactly --
    read the single job's status and persist it once terminal. Guards against the chain-aware
    branch above accidentally swallowing the normal, non-campaign path."""
    from viva_api.common.handlers.simulations import get_simulation_status
    from viva_api.common.hpc.job_service import JobStatusInfo

    hpc_run = HpcRun(
        database_id=99,
        job_id=JobId.k8s("plain-sim-job"),
        correlation_id="N/A",
        job_type=JobType.SIMULATION,
        ref_id=42,
        status=JobStatus.RUNNING,
        chain_n_generations=None,
        chain_final_job_ids=None,
    )
    mock_db_service = AsyncMock()
    mock_db_service.get_simulation.return_value = SimpleNamespace(database_id=42)
    mock_db_service.get_hpcrun_by_ref.return_value = hpc_run

    mock_service = AsyncMock()
    mock_service.get_job_status.return_value = JobStatusInfo(
        job_id=hpc_run.job_id, status=JobStatus.COMPLETED, start_time="t0", end_time="t1"
    )

    with patch(
        "viva_api.common.handlers.simulations.get_simulation_service_for_job",
        return_value=mock_service,
    ):
        result = await get_simulation_status(db_service=mock_db_service, id=42)

    assert result.status == JobStatus.COMPLETED
    mock_db_service.update_hpcrun_status.assert_called_once()


@pytest.mark.asyncio
async def test_status_does_NOT_persist_a_terminal_nextflow_head() -> None:
    """B1 (eagmon, #609): GET /status must not finalize a Nextflow head.

    The sim-749 shape: the head exits 0 with a gather task dead, so the K8s Job
    condition reads Complete. Persisting that here would write the row terminal,
    and ``finalize_nextflow_head``'s ``WHERE status IN (PENDING, RUNNING)`` means
    the trace poller could then never win -- the run would read COMPLETED forever
    with a failed task in it. Reachable by nothing more exotic than polling status
    inside the <= 30 s gap before the next scheduler tick.

    So the handler still REPORTS what the backend says; it just must not write it.
    """
    from viva_api.common.handlers.simulations import get_simulation_status
    from viva_api.common.hpc.job_service import JobStatusInfo

    hpc_run = HpcRun(
        database_id=301,
        job_id=JobId.k8s_nextflow("nf-exp-749"),
        correlation_id="N/A",
        job_type=JobType.SIMULATION,
        ref_id=749,
        status=JobStatus.RUNNING,
    )
    mock_db_service = AsyncMock()
    mock_db_service.get_simulation.return_value = SimpleNamespace(database_id=749)
    mock_db_service.get_hpcrun_by_ref.return_value = hpc_run

    mock_service = AsyncMock()
    mock_service.get_job_status.return_value = JobStatusInfo(
        job_id=hpc_run.job_id, status=JobStatus.COMPLETED, start_time="t0", end_time="t1"
    )

    with patch(
        "viva_api.common.handlers.simulations.get_simulation_service_for_job",
        return_value=mock_service,
    ):
        result = await get_simulation_status(db_service=mock_db_service, id=749)

    # reported live ...
    assert result.status == JobStatus.COMPLETED
    # ... but NOT written down: the poller stays the single writer of the outcome.
    mock_db_service.update_hpcrun_status.assert_not_called()


@pytest.mark.asyncio
async def test_status_still_persists_a_terminal_head_for_other_backends() -> None:
    """The B1 fix is scoped to K8S_NEXTFLOW. Other backends have no separate trace
    authority, so a live poll is the only source and must still be cached -- this
    is the viva-api#484 behaviour (a cancelled campaign reading "unknown" a minute
    after the cancel handler answered) that the persist exists for."""
    from viva_api.common.handlers.simulations import get_simulation_status
    from viva_api.common.hpc.job_service import JobStatusInfo

    hpc_run = HpcRun(
        database_id=302,
        job_id=JobId.ray("ray-job-xyz"),
        correlation_id="N/A",
        job_type=JobType.SIMULATION,
        ref_id=750,
        status=JobStatus.RUNNING,
    )
    mock_db_service = AsyncMock()
    mock_db_service.get_simulation.return_value = SimpleNamespace(database_id=750)
    mock_db_service.get_hpcrun_by_ref.return_value = hpc_run

    mock_service = AsyncMock()
    mock_service.get_job_status.return_value = JobStatusInfo(
        job_id=hpc_run.job_id, status=JobStatus.COMPLETED, start_time="t0", end_time="t1"
    )

    with patch(
        "viva_api.common.handlers.simulations.get_simulation_service_for_job",
        return_value=mock_service,
    ):
        result = await get_simulation_status(db_service=mock_db_service, id=750)

    assert result.status == JobStatus.COMPLETED
    mock_db_service.update_hpcrun_status.assert_called_once()


# ─── get_simulation_chain_progress (backlog item 6) ─────────────────────────


@pytest.mark.asyncio
async def test_chain_progress_not_terminal_reports_in_progress_split() -> None:
    """Mid-campaign: 6 of 10 seeds have resolved (5 succeeded, 1 failed,
    recorded in chain_final_job_ids), the other 4 still tracked as in-flight
    (chain_current_job_ids) -- all three counts must sum to seeds_total (from
    chain_current_job_ids' own fixed length), and terminal comes from the
    row's own status (backlog item 71 Phase 4), not a live re-derivation."""
    from viva_api.common.handlers.simulations import get_simulation_chain_progress
    from viva_api.simulation.simulation_service_ray import ChainCampaignPollResult

    resolved_ids = [f"seed{i}-gen9-job" for i in range(6)]
    in_flight_ids: list[str | None] = [None] * 6 + [f"seed{i}-gen3-job" for i in range(6, 10)]
    hpc_run = HpcRun(
        database_id=300,
        job_id=JobId.ray("parca-job-abc"),
        correlation_id="N/A",
        job_type=JobType.SIMULATION,
        ref_id=214,
        status=JobStatus.RUNNING,
        chain_n_generations=10,
        chain_final_job_ids=resolved_ids,
        chain_current_job_ids=in_flight_ids,
        chain_current_generation=[None] * 6 + [3] * 4,
        chain_parca_done=True,
    )
    mock_db_service = AsyncMock()
    mock_db_service.get_simulation.return_value = SimpleNamespace(database_id=214, num_seeds=10)
    mock_db_service.get_hpcrun_by_ref.return_value = hpc_run

    mock_ray_service = AsyncMock(spec=SimulationServiceRay)
    mock_ray_service.get_chain_campaign_result = (
        lambda ids: ChainCampaignPollResult(
            terminal=True,  # every id IN THIS LIST is already known-terminal by construction
            succeeded_job_ids=resolved_ids[:5],
            failed_job_ids=resolved_ids[5:6],
        )
    )

    with patch(
        "viva_api.common.handlers.simulations.get_simulation_service_for_job",
        return_value=mock_ray_service,
    ):
        result = await get_simulation_chain_progress(db_service=mock_db_service, id=214)

    assert result.seeds_total == 10
    assert result.seeds_succeeded == 5
    assert result.seeds_failed == 1
    assert result.seeds_in_progress == 4
    assert result.terminal is False  # the campaign ROW's own status is still RUNNING
    assert result.status == JobStatus.RUNNING
    mock_db_service.update_hpcrun_status.assert_not_called()


@pytest.mark.asyncio
async def test_chain_progress_terminal_all_succeeded() -> None:
    from viva_api.common.handlers.simulations import get_simulation_chain_progress
    from viva_api.simulation.simulation_service_ray import ChainCampaignPollResult

    job_ids = ["seed0-gen9-job", "seed1-gen9-job"]
    hpc_run = _make_chain_campaign_hpc_run(status=JobStatus.COMPLETED, chain_final_job_ids=job_ids)
    mock_db_service = AsyncMock()
    mock_db_service.get_simulation.return_value = SimpleNamespace(database_id=214, num_seeds=2)
    mock_db_service.get_hpcrun_by_ref.return_value = hpc_run

    mock_ray_service = AsyncMock(spec=SimulationServiceRay)
    mock_ray_service.get_chain_campaign_result = lambda ids: ChainCampaignPollResult(
        terminal=True,
        succeeded_job_ids=job_ids,
    )

    with patch(
        "viva_api.common.handlers.simulations.get_simulation_service_for_job",
        return_value=mock_ray_service,
    ):
        result = await get_simulation_chain_progress(db_service=mock_db_service, id=214)

    assert result.seeds_total == 2 and result.seeds_succeeded == 2
    assert result.seeds_failed == 0 and result.seeds_in_progress == 0
    assert result.terminal is True
    assert result.status == JobStatus.COMPLETED
    mock_db_service.update_hpcrun_status.assert_not_called()


@pytest.mark.asyncio
async def test_chain_progress_zero_tracked_is_trivially_terminal_failed() -> None:
    """Every seed failed even generation 0's submission -- nothing ever
    resolved into chain_final_job_ids at all. Skips the AWS Batch call
    entirely (nothing to classify) and reports the row's own FAILED status,
    written by the scheduler once it detected the same zero-tracked case."""
    from viva_api.common.handlers.simulations import get_simulation_chain_progress

    hpc_run = HpcRun(
        database_id=301,
        job_id=JobId.ray("parca-job-xyz"),
        correlation_id="N/A",
        job_type=JobType.SIMULATION,
        ref_id=215,
        status=JobStatus.FAILED,
        error_message="chain dispatch: zero seed chains succeeded",
        chain_n_generations=10,
        chain_final_job_ids=[],
        chain_current_job_ids=[None, None],
        chain_current_generation=[None, None],
        chain_parca_done=True,
    )
    mock_db_service = AsyncMock()
    mock_db_service.get_simulation.return_value = SimpleNamespace(database_id=215, num_seeds=2)
    mock_db_service.get_hpcrun_by_ref.return_value = hpc_run

    mock_ray_service = AsyncMock(spec=SimulationServiceRay)
    mock_ray_service.get_chain_campaign_result = MagicMock(
        side_effect=AssertionError("must not call AWS Batch for zero tracked ids")
    )

    with patch(
        "viva_api.common.handlers.simulations.get_simulation_service_for_job",
        return_value=mock_ray_service,
    ):
        result = await get_simulation_chain_progress(db_service=mock_db_service, id=215)

    assert result.seeds_total == 2
    assert result.terminal is True
    assert result.status == JobStatus.FAILED


@pytest.mark.asyncio
async def test_chain_progress_non_chain_run_raises_runtime_error() -> None:
    """A plain (non-chain-campaign) simulation has nothing to aggregate --
    the route maps this to 409, distinct from the 404 not-found case."""
    from viva_api.common.handlers.simulations import get_simulation_chain_progress

    hpc_run = HpcRun(
        database_id=99,
        job_id=JobId.k8s("plain-sim-job"),
        correlation_id="N/A",
        job_type=JobType.SIMULATION,
        ref_id=42,
        status=JobStatus.RUNNING,
        chain_n_generations=None,
        chain_final_job_ids=None,
    )
    mock_db_service = AsyncMock()
    mock_db_service.get_simulation.return_value = SimpleNamespace(database_id=42)
    mock_db_service.get_hpcrun_by_ref.return_value = hpc_run

    with pytest.raises(RuntimeError, match="not a chain-dispatch campaign"):
        await get_simulation_chain_progress(db_service=mock_db_service, id=42)


@pytest.mark.asyncio
async def test_chain_progress_unknown_simulation_raises_value_error() -> None:
    from viva_api.common.handlers.simulations import get_simulation_chain_progress

    mock_db_service = AsyncMock()
    mock_db_service.get_simulation.return_value = None

    with pytest.raises(ValueError, match="not found"):
        await get_simulation_chain_progress(db_service=mock_db_service, id=9999)


def _make_parca_dataset(database_id: int = 158, commit: str = "82e1b1e") -> Any:
    from viva_api.simulation.models import ParcaDataset, ParcaDatasetRequest, ParcaOptions, SimulatorVersion

    return ParcaDataset(
        database_id=database_id,
        parca_dataset_request=ParcaDatasetRequest(
            simulator_version=SimulatorVersion(
                database_id=104, git_commit_hash=commit, git_branch="main", git_repo_url="https://github.com/x/y"
            ),
            parca_config=ParcaOptions(),
        ),
    )


class TestRunNewGeneCache:
    """run_new_gene_cache (backlog item 105): the REST-layer handler for
    ``POST /parca/new-gene-cache``. A real, load-bearing gap slipped through
    the original PR here -- every prior test for this feature exercised only
    ``submit_new_gene_cache_job`` (the service layer, boto3-mocked), never
    THIS function, which is where the actual bug lived: a precondition check
    keyed on ``get_hpcrun_by_ref(ref_id=parca_dataset_id, job_type=PARCA)``
    that can never resolve for a real chain-dispatch-originated
    ParcaDataset (only the legacy SLURM-only ``run_parca`` handler ever
    inserts that HpcRun shape). These tests specifically cover the handler's
    own DB-facing logic, not just the mechanism underneath it."""

    @pytest.mark.asyncio
    async def test_does_not_require_a_parca_job_type_hpcrun(self) -> None:
        """The precondition this PR originally had would 409 here, since a
        real chain-dispatch ParcaDataset never has a matching JobType.PARCA
        HpcRun. Confirms get_hpcrun_by_ref is never even called."""
        from viva_api.common.handlers.simulations import run_new_gene_cache
        from viva_api.simulation.models import NewGeneCacheRequest

        mock_ray = AsyncMock(spec=SimulationServiceRay)
        mock_ray.submit_new_gene_cache_job.return_value = JobId.ray("new-gene-cache-1")
        mock_ray.cache_s3_uri.return_value = "s3://bucket/ray-parca-cache/82e1b1e/k4-induced/"
        mock_db = AsyncMock()
        mock_db.get_parca_dataset.return_value = _make_parca_dataset()

        result = await run_new_gene_cache(
            request=NewGeneCacheRequest(
                parca_dataset_id=158, variant="k4-induced", expression=1e6, translation_efficiency=1.0
            ),
            simulation_service=mock_ray,
            database_service=mock_db,
        )

        mock_db.get_hpcrun_by_ref.assert_not_called()
        assert result.job_id == "new-gene-cache-1"
        assert result.commit == "82e1b1e"

    @pytest.mark.asyncio
    async def test_resolves_ray_by_name_not_deployment_default(self) -> None:
        """Real bug, found live 2026-09-04 on this endpoint's own first-ever
        real call: with simulation_service omitted, the handler used to call
        get_simulation_service() -- the DEPLOYMENT's "default" backend, which
        on sms-api-stanford-test is COMPUTE_BACKEND=batch (Nextflow), not
        Ray -- so a deployment that dispatches real Ray/Batch MNP jobs
        successfully through every OTHER route (those resolve via
        get_simulation_service_for_repo, commit/repo-aware) 501'd on this one.
        Every other test in this class masked the bug by injecting
        simulation_service directly, never exercising resolution at all --
        this is the one test that actually calls the handler with it omitted."""
        from viva_api.common.handlers.simulations import run_new_gene_cache
        from viva_api.simulation.models import NewGeneCacheRequest

        mock_ray = AsyncMock(spec=SimulationServiceRay)
        mock_ray.submit_new_gene_cache_job.return_value = JobId.ray("new-gene-cache-2")
        mock_ray.cache_s3_uri.return_value = "s3://bucket/ray-parca-cache/82e1b1e/j3-induced/"
        mock_db = AsyncMock()
        mock_db.get_parca_dataset.return_value = _make_parca_dataset()

        with patch("viva_api.common.handlers.simulations.get_simulation_service_for_backend") as mock_resolve:
            mock_resolve.return_value = mock_ray
            result = await run_new_gene_cache(
                request=NewGeneCacheRequest(
                    parca_dataset_id=158, variant="j3-induced", expression=1e6, translation_efficiency=1.0
                ),
                database_service=mock_db,
            )

        mock_resolve.assert_called_once_with(ComputeBackend.RAY)
        assert result.job_id == "new-gene-cache-2"

    @pytest.mark.asyncio
    async def test_unknown_parca_dataset_404s(self) -> None:
        from viva_api.common.handlers.simulations import run_new_gene_cache
        from viva_api.simulation.models import NewGeneCacheRequest

        mock_ray = AsyncMock(spec=SimulationServiceRay)
        mock_db = AsyncMock()
        mock_db.get_parca_dataset.return_value = None

        with pytest.raises(HTTPException) as exc_info:
            await run_new_gene_cache(
                request=NewGeneCacheRequest(
                    parca_dataset_id=99999, variant="x", expression=1.0, translation_efficiency=1.0
                ),
                simulation_service=mock_ray,
                database_service=mock_db,
            )
        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_non_ray_backend_501s(self) -> None:
        from viva_api.common.handlers.simulations import run_new_gene_cache
        from viva_api.simulation.models import NewGeneCacheRequest

        with pytest.raises(HTTPException) as exc_info:
            await run_new_gene_cache(
                request=NewGeneCacheRequest(
                    parca_dataset_id=158, variant="k4-induced", expression=1.0, translation_efficiency=1.0
                ),
                simulation_service=AsyncMock(spec=SimulationServiceK8s),
                database_service=AsyncMock(),
            )
        assert exc_info.value.status_code == 501

    @pytest.mark.asyncio
    async def test_forwards_all_request_fields_to_the_service_layer(self) -> None:
        from viva_api.common.handlers.simulations import run_new_gene_cache
        from viva_api.simulation.models import NewGeneCacheRequest

        mock_ray = AsyncMock(spec=SimulationServiceRay)
        mock_ray.submit_new_gene_cache_job.return_value = JobId.ray("j")
        mock_ray.cache_s3_uri.return_value = "s3://x/"
        mock_db = AsyncMock()
        mock_db.get_parca_dataset.return_value = _make_parca_dataset(commit="f64994e")

        await run_new_gene_cache(
            request=NewGeneCacheRequest(
                parca_dataset_id=158,
                variant="k4-induced",
                expression=1e6,
                translation_efficiency=1.0,
                rel_exp_adj="1,2,4",
                seed=7,
            ),
            simulation_service=mock_ray,
            database_service=mock_db,
        )

        call_kwargs = mock_ray.submit_new_gene_cache_job.call_args.kwargs
        assert call_kwargs["commit"] == "f64994e"
        assert call_kwargs["variant"] == "k4-induced"
        assert call_kwargs["rel_exp_adj"] == "1,2,4"
        assert call_kwargs["seed"] == 7


class TestRunVariantCache:
    """run_variant_cache (backlog item 451): the REST-layer handler for
    ``POST /parca/variant-cache``, the native-gene sibling of
    TestRunNewGeneCache above. Same structure/gates -- mirrored here rather
    than re-explaining, since the underlying reasoning (Ray-by-name
    resolution, no HpcRun precondition) is identical."""

    @pytest.mark.asyncio
    async def test_resolves_ray_by_name_not_deployment_default(self) -> None:
        from viva_api.common.handlers.simulations import run_variant_cache
        from viva_api.simulation.models import VariantCacheRequest

        mock_ray = AsyncMock(spec=SimulationServiceRay)
        mock_ray.submit_variant_cache_job.return_value = JobId.ray("variant-cache-2")
        mock_ray.cache_s3_uri.return_value = "s3://bucket/ray-parca-cache/82e1b1e/strain-design-1/"
        mock_db = AsyncMock()
        mock_db.get_parca_dataset.return_value = _make_parca_dataset()

        with patch("viva_api.common.handlers.simulations.get_simulation_service_for_backend") as mock_resolve:
            mock_resolve.return_value = mock_ray
            result = await run_variant_cache(
                request=VariantCacheRequest(
                    parca_dataset_id=158, variant="strain-design-1", perturbations={"EG10073": 10.0}
                ),
                database_service=mock_db,
            )

        mock_resolve.assert_called_once_with(ComputeBackend.RAY)
        assert result.job_id == "variant-cache-2"
        assert result.commit == "82e1b1e"

    @pytest.mark.asyncio
    async def test_unknown_parca_dataset_404s(self) -> None:
        from viva_api.common.handlers.simulations import run_variant_cache
        from viva_api.simulation.models import VariantCacheRequest

        mock_ray = AsyncMock(spec=SimulationServiceRay)
        mock_db = AsyncMock()
        mock_db.get_parca_dataset.return_value = None

        with pytest.raises(HTTPException) as exc_info:
            await run_variant_cache(
                request=VariantCacheRequest(parca_dataset_id=99999, variant="x", perturbations={"EG10073": 1.0}),
                simulation_service=mock_ray,
                database_service=mock_db,
            )
        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_non_ray_backend_501s(self) -> None:
        from viva_api.common.handlers.simulations import run_variant_cache
        from viva_api.simulation.models import VariantCacheRequest

        with pytest.raises(HTTPException) as exc_info:
            await run_variant_cache(
                request=VariantCacheRequest(
                    parca_dataset_id=158, variant="strain-design-1", perturbations={"EG10073": 1.0}
                ),
                simulation_service=AsyncMock(spec=SimulationServiceK8s),
                database_service=AsyncMock(),
            )
        assert exc_info.value.status_code == 501

    @pytest.mark.asyncio
    async def test_forwards_all_request_fields_to_the_service_layer(self) -> None:
        from viva_api.common.handlers.simulations import run_variant_cache
        from viva_api.simulation.models import VariantCacheRequest

        mock_ray = AsyncMock(spec=SimulationServiceRay)
        mock_ray.submit_variant_cache_job.return_value = JobId.ray("j")
        mock_ray.cache_s3_uri.return_value = "s3://x/"
        mock_db = AsyncMock()
        mock_db.get_parca_dataset.return_value = _make_parca_dataset(commit="f64994e")

        await run_variant_cache(
            request=VariantCacheRequest(
                parca_dataset_id=158,
                variant="strain-design-1",
                perturbations={"EG10073": 10.0, "EG10074": 0.0},
                seed=7,
                fixed_media="minimal",
            ),
            simulation_service=mock_ray,
            database_service=mock_db,
        )

        call_kwargs = mock_ray.submit_variant_cache_job.call_args.kwargs
        assert call_kwargs["commit"] == "f64994e"
        assert call_kwargs["variant"] == "strain-design-1"
        assert call_kwargs["perturbations"] == {"EG10073": 10.0, "EG10074": 0.0}
        assert call_kwargs["seed"] == 7
        assert call_kwargs["fixed_media"] == "minimal"


@pytest.mark.asyncio
async def test_run_simulation_workflow_forces_unique_experiment_id_over_a_configs_own_baked_value() -> None:
    """Regression for backlog item 117 / sms-ecoli#235 (cplong90's own independent repro):
    Dispatch 339 was submitted from a copied Run 2 config whose own baked `experiment_id`
    was never re-pointed -- `config_data.setdefault("experiment_id", unique_experiment_id)`
    is a no-op once a key already exists, so the config's own stale value won and Run 1's
    real output landed under Run 2's S3 prefix. `unique_experiment_id` was already always
    correct on the DB's own top-level `SimulationRequest.experiment_id` (set unconditionally
    a few lines later) -- the bug was only ever that `config.experiment_id`, what a dispatch
    actually writes its S3 output under, could silently disagree with it.

    Fix: force-assign rather than setdefault. This test drives the real
    `run_simulation_workflow` end to end (mocking only DB/service I/O, no Docker needed)
    with a config template that bakes its own stale `experiment_id`, and asserts the two
    values now agree and neither is the stale one.
    """
    simulator = SimulatorVersion(
        database_id=53,
        git_commit_hash="deadbeef",
        git_repo_url=RepoUrl.SMS_ECOLI_REPO_URL,
        git_branch="main",
    )
    mock_db_service = AsyncMock()
    mock_db_service.get_simulator.return_value = simulator
    mock_db_service.insert_parca_dataset.return_value = SimpleNamespace(database_id=158)
    mock_db_service.get_hpcrun_id_by_correlation_id.return_value = None
    inserted_simulation = _make_ray_simulation()
    mock_db_service.insert_simulation.return_value = inserted_simulation

    mock_ray_service = AsyncMock(spec=SimulationServiceRay)
    # The exact real-world shape: a config file copied from another run, carrying that
    # run's own stale, baked experiment_id -- the config content sms-ecoli#235 identifies.
    mock_ray_service.read_config_template.return_value = (
        '{"experiment_id": "cd2_run2_j3", "n_init_sims": 1, "generations": 10}'
    )
    mock_ray_service.submit_ecoli_simulation_job.return_value = JobId.ray("job-abc")

    with (
        patch("viva_api.common.handlers.simulations._verify_build_complete", new=AsyncMock()),
        patch("viva_api.common.handlers.simulations.get_simulation_service_for_repo", return_value=None),
        patch("viva_api.common.handlers.simulations.export_baseline_config"),
        patch("viva_api.common.handlers.simulations.get_correlation_id", return_value="corr-1"),
    ):
        await run_simulation_workflow(
            database_service=mock_db_service,
            simulation_service=mock_ray_service,
            simulator_id=53,
            experiment_id="cd2-run1-k4-cellonly-lam050-founder-seed0",
            simulation_config_filename="configs/cd2/run1_k4_cellonly.json",
        )

    mock_db_service.insert_simulation.assert_called_once()
    request = mock_db_service.insert_simulation.call_args.kwargs["sim_request"]

    # The stale, config-baked value must never win.
    assert request.config.experiment_id != "cd2_run2_j3"
    assert request.experiment_id != "cd2_run2_j3"
    # The caller's own semantic label survives as a substring (unique_experiment_id's own
    # construction embeds it) -- forcing the fix doesn't erase caller intent, it only stops
    # a config template's own baked value from shadowing it.
    assert "cd2-run1-k4-cellonly-lam050-founder-seed0" in request.config.experiment_id
    # The one real bug: these two must now always agree. Before the fix, request.experiment_id
    # was unique_experiment_id while request.config.experiment_id was the stale "cd2_run2_j3".
    assert request.config.experiment_id == request.experiment_id


@pytest.mark.asyncio
async def test_a_malformed_dispatch_is_refused_before_anything_is_written() -> None:
    """viva-api#455: a refused dispatch must not leave rows behind.

    `run_simulation_workflow` inserts a parca-dataset row (step 5) and a
    simulation row (step 6) and only submits at step 7, so a dispatch-time raise
    used to leave two rows describing a run that never started -- observed live
    as simulation 394, `sim153-nf-resume-guard-check-0eb7`, `job_id: None`.

    `resume` without `resume_from` is answerable from the request alone, so it is
    rejected before either insert. Asserting on the INSERTS rather than on the
    raise is the point: raising was never the problem.
    """
    simulator = SimulatorVersion(
        database_id=53,
        git_commit_hash="deadbeef",
        git_repo_url=RepoUrl.SMS_ECOLI_REPO_URL,
        git_branch="main",
    )
    mock_db_service = AsyncMock()
    mock_db_service.get_simulator.return_value = simulator
    mock_db_service.insert_parca_dataset.return_value = SimpleNamespace(database_id=158)
    mock_db_service.get_hpcrun_id_by_correlation_id.return_value = None
    mock_db_service.insert_simulation.return_value = _make_ray_simulation()

    mock_ray_service = AsyncMock(spec=SimulationServiceRay)
    mock_ray_service.read_config_template.return_value = '{"n_init_sims": 1, "generations": 1}'

    with (
        patch("viva_api.common.handlers.simulations._verify_build_complete", new=AsyncMock()),
        patch("viva_api.common.handlers.simulations.get_simulation_service_for_repo", return_value=None),
        patch("viva_api.common.handlers.simulations.export_baseline_config"),
        patch("viva_api.common.handlers.simulations.get_correlation_id", return_value="corr-1"),
        pytest.raises(DispatchValidationError, match="resume_from"),
    ):
        await run_simulation_workflow(
            database_service=mock_db_service,
            simulation_service=mock_ray_service,
            simulator_id=53,
            experiment_id="nf-resume-guard-check",
            simulation_config_filename="configs/mecillinam_wellmixed.json",
            extra_params={
                "nextflow_dispatch": {
                    "composite_id": "v2ecoli.composites.workflow_nf.workflow_nf",
                    "resume": True,
                }
            },
        )

    mock_db_service.insert_simulation.assert_not_called()
    mock_db_service.insert_parca_dataset.assert_not_called()
    mock_ray_service.submit_ecoli_simulation_job.assert_not_called()


@pytest.mark.asyncio
async def test_a_well_formed_dispatch_still_reaches_the_submit() -> None:
    """The guard must not swallow the ordinary case -- a validator that rejects
    everything would pass the test above."""
    simulator = SimulatorVersion(
        database_id=53,
        git_commit_hash="deadbeef",
        git_repo_url=RepoUrl.SMS_ECOLI_REPO_URL,
        git_branch="main",
    )
    mock_db_service = AsyncMock()
    mock_db_service.get_simulator.return_value = simulator
    mock_db_service.insert_parca_dataset.return_value = SimpleNamespace(database_id=158)
    mock_db_service.get_hpcrun_id_by_correlation_id.return_value = None
    mock_db_service.insert_simulation.return_value = _make_ray_simulation()

    mock_ray_service = AsyncMock(spec=SimulationServiceRay)
    mock_ray_service.read_config_template.return_value = '{"n_init_sims": 1, "generations": 1}'
    mock_ray_service.submit_ecoli_simulation_job.return_value = JobId.ray("job-abc")

    with (
        patch("viva_api.common.handlers.simulations._verify_build_complete", new=AsyncMock()),
        patch("viva_api.common.handlers.simulations.get_simulation_service_for_repo", return_value=None),
        patch("viva_api.common.handlers.simulations.export_baseline_config"),
        patch("viva_api.common.handlers.simulations.get_correlation_id", return_value="corr-1"),
    ):
        await run_simulation_workflow(
            database_service=mock_db_service,
            simulation_service=mock_ray_service,
            simulator_id=53,
            experiment_id="nf-ok",
            simulation_config_filename="configs/mecillinam_wellmixed.json",
            extra_params={
                "nextflow_dispatch": {
                    "composite_id": "v2ecoli.composites.workflow_nf.workflow_nf",
                    "resume": True,
                    "resume_from": "sim153-prior-run-0000",
                }
            },
        )

    mock_db_service.insert_simulation.assert_called_once()
    mock_ray_service.submit_ecoli_simulation_job.assert_called_once()


class TestExtraParamsParcaOptionsDeepMerge:
    """Real, confirmed gap: config_data.setdefault("parca_options", extra_params_value)
    is a no-op whenever the config template's own JSON already declares a
    parca_options block at all -- true of essentially every real config with
    meaningful ParCa settings. extra_params.parca_options now merges PER
    SUB-FIELD instead, so a new sub-field (e.g. a field the template's own
    parca_options block never mentions) survives, while the template's own
    explicit sub-field still wins -- same "template wins" contract the
    surrounding fallback-layer docstring already promises, applied one level
    deeper. Caught live firing a real Run 4 chassis rebuild whose new_genes/
    bundle_overrides happened to coincidentally match the template's own
    baked-in values, masking that the override itself never took effect."""

    @staticmethod
    def _mocks(config_template: str) -> tuple[AsyncMock, AsyncMock]:
        simulator = SimulatorVersion(
            database_id=53,
            git_commit_hash="deadbeef",
            git_repo_url=RepoUrl.SMS_ECOLI_REPO_URL,
            git_branch="main",
        )
        mock_db_service = AsyncMock()
        mock_db_service.get_simulator.return_value = simulator
        mock_db_service.insert_parca_dataset.return_value = SimpleNamespace(database_id=158)
        mock_db_service.get_hpcrun_id_by_correlation_id.return_value = None
        mock_db_service.insert_simulation.return_value = _make_ray_simulation()

        mock_ray_service = AsyncMock(spec=SimulationServiceRay)
        mock_ray_service.read_config_template.return_value = config_template
        mock_ray_service.submit_ecoli_simulation_job.return_value = JobId.ray("job-abc")
        return mock_db_service, mock_ray_service

    @pytest.mark.asyncio
    async def test_new_sub_field_survives_against_a_template_with_its_own_parca_options(self) -> None:
        """The real Run 4 shape: the template already has its own parca_options
        block (new_genes/bundle_overrides), and extra_params supplies a field
        the template never mentions at all (deterministic_hash_seed) -- must
        survive, not be silently dropped by the old whole-object setdefault."""
        mock_db_service, mock_ray_service = self._mocks(
            '{"n_init_sims": 1, "generations": 1, "parca_options": '
            '{"cpus": 6, "new_genes": "violacein_MG1655_M5", '
            '"bundle_overrides": "models/parca/violacein_bundle_overrides.tsv"}}'
        )
        with (
            patch("viva_api.common.handlers.simulations._verify_build_complete", new=AsyncMock()),
            patch("viva_api.common.handlers.simulations.get_simulation_service_for_repo", return_value=None),
            patch("viva_api.common.handlers.simulations.export_baseline_config"),
            patch("viva_api.common.handlers.simulations.get_correlation_id", return_value="corr-1"),
        ):
            await run_simulation_workflow(
                database_service=mock_db_service,
                simulation_service=mock_ray_service,
                simulator_id=53,
                experiment_id="run4-chassis",
                simulation_config_filename="configs/pathway_expression_carina_final.json",
                extra_params={
                    "parca_options": {
                        "build_combined_bundle_manifest": True,
                        "include_violacein_bundle": True,
                        "deterministic_hash_seed": True,
                    }
                },
            )

        sim_request = mock_db_service.insert_simulation.call_args.kwargs["sim_request"]
        parca_options = sim_request.config.parca_options
        assert parca_options.build_combined_bundle_manifest is True
        assert parca_options.include_violacein_bundle is True
        assert parca_options.deterministic_hash_seed is True
        # The template's own explicit values are untouched.
        assert parca_options.new_genes == "violacein_MG1655_M5"
        assert parca_options.bundle_overrides == "models/parca/violacein_bundle_overrides.tsv"

    @pytest.mark.asyncio
    async def test_templates_own_explicit_sub_field_still_wins(self) -> None:
        """Same 'template wins' contract as every other extra_params key --
        applied per sub-field now, not lost entirely the moment the template
        happens to declare parca_options at all."""
        mock_db_service, mock_ray_service = self._mocks(
            '{"n_init_sims": 1, "generations": 1, "parca_options": {"new_genes": "violacein_MG1655_M5"}}'
        )
        with (
            patch("viva_api.common.handlers.simulations._verify_build_complete", new=AsyncMock()),
            patch("viva_api.common.handlers.simulations.get_simulation_service_for_repo", return_value=None),
            patch("viva_api.common.handlers.simulations.export_baseline_config"),
            patch("viva_api.common.handlers.simulations.get_correlation_id", return_value="corr-1"),
        ):
            await run_simulation_workflow(
                database_service=mock_db_service,
                simulation_service=mock_ray_service,
                simulator_id=53,
                experiment_id="run4-chassis-2",
                simulation_config_filename="configs/pathway_expression_carina_final.json",
                extra_params={"parca_options": {"new_genes": "some_other_strain"}},
            )

        sim_request = mock_db_service.insert_simulation.call_args.kwargs["sim_request"]
        assert sim_request.config.parca_options.new_genes == "violacein_MG1655_M5"

    @pytest.mark.asyncio
    async def test_parca_options_extra_params_survives_when_template_has_none_at_all(self) -> None:
        """A template with no parca_options block at all -- extra_params.parca_options
        becomes the whole block (the pre-existing setdefault behavior, unchanged)."""
        mock_db_service, mock_ray_service = self._mocks('{"n_init_sims": 1, "generations": 1}')
        with (
            patch("viva_api.common.handlers.simulations._verify_build_complete", new=AsyncMock()),
            patch("viva_api.common.handlers.simulations.get_simulation_service_for_repo", return_value=None),
            patch("viva_api.common.handlers.simulations.export_baseline_config"),
            patch("viva_api.common.handlers.simulations.get_correlation_id", return_value="corr-1"),
        ):
            await run_simulation_workflow(
                database_service=mock_db_service,
                simulation_service=mock_ray_service,
                simulator_id=53,
                experiment_id="no-template-parca-options",
                simulation_config_filename="configs/mecillinam_wellmixed.json",
                extra_params={"parca_options": {"new_genes": "violacein_MG1655_M5"}},
            )

        sim_request = mock_db_service.insert_simulation.call_args.kwargs["sim_request"]
        assert sim_request.config.parca_options.new_genes == "violacein_MG1655_M5"

    @pytest.mark.asyncio
    async def test_non_parca_options_keys_are_unaffected(self) -> None:
        """The generic setdefault loop for every OTHER extra_params key is
        untouched -- same byte-for-byte behavior as before this fix."""
        mock_db_service, mock_ray_service = self._mocks('{"n_init_sims": 1, "generations": 1}')
        with (
            patch("viva_api.common.handlers.simulations._verify_build_complete", new=AsyncMock()),
            patch("viva_api.common.handlers.simulations.get_simulation_service_for_repo", return_value=None),
            patch("viva_api.common.handlers.simulations.export_baseline_config"),
            patch("viva_api.common.handlers.simulations.get_correlation_id", return_value="corr-1"),
        ):
            await run_simulation_workflow(
                database_service=mock_db_service,
                simulation_service=mock_ray_service,
                simulator_id=53,
                experiment_id="cache-variant-passthrough",
                simulation_config_filename="configs/mecillinam_wellmixed.json",
                extra_params={"cache_variant": "cd2-run4-carina-genotype2"},
            )

        sim_request = mock_db_service.insert_simulation.call_args.kwargs["sim_request"]
        assert getattr(sim_request.config, "cache_variant", None) == "cd2-run4-carina-genotype2"


@pytest.mark.asyncio
async def test_a_terminal_row_is_reported_without_asking_a_backend_that_may_be_gone() -> None:
    """viva-api#484. A cancelled Nextflow campaign's head Job is deleted by the
    cancel itself, so `get_job_status` returns None and the plain path reported
    UNKNOWN -- one minute after the cancel handler had answered "cancelled".
    The DB row is the answer for any terminal status; the backend is not asked."""
    from unittest.mock import AsyncMock, MagicMock, patch

    from viva_api.common.handlers import simulations as handlers
    from viva_api.common.models import JobId, JobStatus
    from viva_api.simulation.models import HpcRun, JobType

    row = HpcRun(
        database_id=1,
        job_id=JobId.k8s_nextflow("nf-sim1-x-abcd-zzz111"),
        correlation_id="N/A",
        job_type=JobType.SIMULATION,
        ref_id=567,
        status=JobStatus.CANCELLED,
        error_message=None,
    )
    db = MagicMock()
    db.get_simulation = AsyncMock(return_value=MagicMock())  # the handler resolves the record first
    db.get_hpcrun_by_ref = AsyncMock(return_value=row)
    service = MagicMock()
    service.get_job_status = AsyncMock(return_value=None)  # the head is gone
    with patch.object(handlers, "get_simulation_service_for_job", return_value=service):
        run = await handlers.get_simulation_status(db_service=db, id=567)
    assert run.status == JobStatus.CANCELLED
    service.get_job_status.assert_not_awaited()


# --- Observability plan D4c: the log path routes by the RUN's backend -----------


@pytest.mark.asyncio
async def test_k8s_log_for_a_nextflow_head_uses_the_service_that_owns_the_run() -> None:
    """A v2ecoli Nextflow head is owned by SimulationServiceRay; the old
    `isinstance(get_simulation_service(), SimulationServiceK8s)` check raised
    TypeError for exactly that run on a deployment whose default service is not
    the vEcoli K8s one."""
    from viva_api.common.handlers.simulations import _get_k8s_log

    hpc_run = HpcRun(
        database_id=1,
        job_id=JobId.k8s_nextflow("nf-exp-abc"),
        correlation_id="c",
        job_type=JobType.SIMULATION,
        ref_id=1,
        status=JobStatus.FAILED,
    )
    ray_service = MagicMock()
    ray_service._k8s.get_job_logs.return_value = "N E X T F L O W\nexecutor > awsbatch\n"
    with patch(
        "viva_api.common.handlers.simulations.get_simulation_service_for_job", return_value=ray_service
    ) as lookup:
        log = await _get_k8s_log(hpc_run, MagicMock(), 1)
    assert log.startswith("N E X T F L O W")
    lookup.assert_called_once_with(hpc_run.job_id)
    ray_service._k8s.get_job_logs.assert_called_once_with("nf-exp-abc")


@pytest.mark.asyncio
async def test_s3_nextflow_log_falls_back_to_the_v2ecoli_results_layout() -> None:
    """A v2ecoli head stages its render dir wholesale to the RESULTS prefix, so
    `.nextflow.log` sits beside trace.csv at vecoli-output/<exp>/.nextflow.log
    (sim 749) -- not under the vEcoli `<work>/<exp>/logs/` key."""
    from viva_api.common.handlers.simulations import _get_s3_nextflow_log

    simulation = MagicMock()
    simulation.experiment_id = "sim184-run1-k4"
    simulation.config.experiment_id = "sim184-run1-k4"
    db = MagicMock()
    db.get_simulation = AsyncMock(return_value=simulation)
    fs = MagicMock()

    async def _get(s3_path: Any) -> bytes | None:
        return (
            b"Sep-10 03:15 Execution complete -- Goodbye" if str(s3_path.s3_path).startswith("vecoli-output/") else None
        )

    fs.get_file_contents = AsyncMock(side_effect=_get)
    saved = get_file_service()
    set_file_service(fs)
    try:
        with patch("viva_api.common.handlers.simulations.get_settings") as settings:
            settings.return_value = MagicMock(
                s3_work_prefix="nextflow/work", s3_work_bucket="mybucket", s3_output_prefix="vecoli-output"
            )
            log = await _get_s3_nextflow_log(db, 1)
    finally:
        set_file_service(saved)
    assert log is not None and "Goodbye" in log
    tried = [str(c.args[0].s3_path) for c in fs.get_file_contents.await_args_list]
    assert tried == ["nextflow/work/sim184-run1-k4/logs/.nextflow.log", "vecoli-output/sim184-run1-k4/.nextflow.log"]
