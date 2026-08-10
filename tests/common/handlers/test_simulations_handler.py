import asyncio
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from viva_api.analysis.models import TsvOutputFile
from viva_api.common.handlers.simulations import (
    _S3_DOWNLOAD_CONCURRENCY,
    SimulationAnalysisResponseType,
    _download_outputs_from_s3,
    _run_standalone_analysis_ray_native,
    fetch_omics_outputs,
    get_available_omics_output_paths,
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


def _make_chain_campaign_hpc_run(status: JobStatus = JobStatus.RUNNING) -> HpcRun:
    return HpcRun(
        database_id=300,
        job_id=JobId.ray("parca-job-abc"),
        correlation_id="N/A",
        job_type=JobType.SIMULATION,
        ref_id=214,
        status=status,
        chain_n_generations=10,
        chain_final_job_ids=["seed0-gen9-job", "seed1-gen9-job"],
    )


@pytest.mark.asyncio
async def test_get_simulation_status_chain_campaign_not_terminal_does_not_write_db() -> None:
    """Regression for the severe bug found 2026-08-10: `get_simulation_status` used to read
    the campaign's stored `job_id` (the ParCa kickoff job, which finishes long before the real
    per-seed chains) and write ITS status onto the whole campaign row -- corrupting it to
    COMPLETED while the real chains were still 5% done, permanently dropping the campaign out
    of JobScheduler.list_active_chain_campaigns()'s poll set and silently killing analysis
    auto-fire. A chain campaign's status must come from get_chain_campaign_result (polling the
    real tracked per-seed job ids), and this endpoint must never write to the DB -- only
    JobScheduler's own poll loop may transition a campaign row's status."""
    from viva_api.common.handlers.simulations import get_simulation_status
    from viva_api.simulation.simulation_service_ray import ChainCampaignPollResult

    hpc_run = _make_chain_campaign_hpc_run()
    mock_db_service = AsyncMock()
    mock_db_service.get_simulation.return_value = SimpleNamespace(database_id=214)
    mock_db_service.get_hpcrun_by_ref.return_value = hpc_run

    mock_ray_service = AsyncMock(spec=SimulationServiceRay)
    mock_ray_service.get_chain_campaign_result = lambda job_ids: ChainCampaignPollResult(terminal=False)

    with patch(
        "viva_api.common.handlers.simulations.get_simulation_service_for_job",
        return_value=mock_ray_service,
    ):
        result = await get_simulation_status(db_service=mock_db_service, id=214)

    assert result.status == JobStatus.RUNNING
    mock_db_service.update_hpcrun_status.assert_not_called()


@pytest.mark.asyncio
async def test_get_simulation_status_chain_campaign_terminal_success_does_not_write_db() -> None:
    """A terminal, all-succeeded chain campaign reports COMPLETED without writing the DB --
    JobScheduler.update_chain_campaigns owns that write, on its own poll interval, not this
    read path (avoids a race between a status check and the scheduler's own transition)."""
    from viva_api.common.handlers.simulations import get_simulation_status
    from viva_api.simulation.simulation_service_ray import ChainCampaignPollResult

    hpc_run = _make_chain_campaign_hpc_run()
    mock_db_service = AsyncMock()
    mock_db_service.get_simulation.return_value = SimpleNamespace(database_id=214)
    mock_db_service.get_hpcrun_by_ref.return_value = hpc_run

    mock_ray_service = AsyncMock(spec=SimulationServiceRay)
    mock_ray_service.get_chain_campaign_result = lambda job_ids: ChainCampaignPollResult(
        terminal=True, succeeded_job_ids=["seed0-gen9-job", "seed1-gen9-job"]
    )

    with patch(
        "viva_api.common.handlers.simulations.get_simulation_service_for_job",
        return_value=mock_ray_service,
    ):
        result = await get_simulation_status(db_service=mock_db_service, id=214)

    assert result.status == JobStatus.COMPLETED
    mock_db_service.update_hpcrun_status.assert_not_called()


@pytest.mark.asyncio
async def test_get_simulation_status_chain_campaign_terminal_zero_succeeded_reports_failed() -> None:
    """Every tracked seed chain failed -- reports FAILED with an explanatory message, still
    without writing the DB (same ownership rule as the success case above)."""
    from viva_api.common.handlers.simulations import get_simulation_status
    from viva_api.simulation.simulation_service_ray import ChainCampaignPollResult

    hpc_run = _make_chain_campaign_hpc_run()
    mock_db_service = AsyncMock()
    mock_db_service.get_simulation.return_value = SimpleNamespace(database_id=214)
    mock_db_service.get_hpcrun_by_ref.return_value = hpc_run

    mock_ray_service = AsyncMock(spec=SimulationServiceRay)
    mock_ray_service.get_chain_campaign_result = lambda job_ids: ChainCampaignPollResult(
        terminal=True, succeeded_job_ids=[], failed_job_ids=["seed0-gen9-job", "seed1-gen9-job"]
    )

    with patch(
        "viva_api.common.handlers.simulations.get_simulation_service_for_job",
        return_value=mock_ray_service,
    ):
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
