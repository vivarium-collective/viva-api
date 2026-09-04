import asyncio
import datetime
import os
import random
import string
import tempfile
import uuid
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio

from viva_api.common.hpc.job_service import JobStatusInfo, JobStatusUpdate
from viva_api.common.hpc.local_task_service import LocalTaskService
from viva_api.common.hpc.models import SlurmJob
from viva_api.common.hpc.slurm_service import SlurmService
from viva_api.common.messaging.messaging_service_redis import MessagingServiceRedis
from viva_api.common.models import JobId, JobStatus, SSHTarget
from viva_api.common.simulator_defaults import RepoUrl
from viva_api.common.ssh.ssh_service import SSHSessionService
from viva_api.common.storage.file_paths import S3FilePath
from viva_api.common.storage.file_service import FileService
from viva_api.common.storage.file_service_qumulo_s3 import FileServiceQumuloS3
from viva_api.common.storage.file_service_s3 import FileServiceS3
from viva_api.config import get_settings
from viva_api.dependencies import get_ssh_session_service
from viva_api.simulation import batch_build
from viva_api.simulation.database_service import DatabaseServiceSQL
from viva_api.simulation.hpc_utils import get_correlation_id
from viva_api.simulation.job_scheduler import LOCAL_ORPHAN_GRACE_SECONDS, JobScheduler
from viva_api.simulation.models import (
    HpcRun,
    JobType,
    ParcaDatasetRequest,
    ParcaOptions,
    Simulation,
    SimulationConfig,
    SimulationRequest,
    WorkerEventMessagePayload,
)
from viva_api.simulation.simulation_service_ray import ChainCampaignPollResult


def is_ci_environment() -> bool:
    """Check if running in CI/CD environment (GitHub Actions, etc.)."""
    return os.getenv("CI") == "true" or os.getenv("GITHUB_ACTIONS") == "true"


async def insert_job(database_service: DatabaseServiceSQL, slurmjobid: int) -> tuple[Simulation, SlurmJob, HpcRun]:
    latest_commit_hash = str(uuid.uuid4())
    repo_url = "https://github.com/some/repo"
    main_branch = "main"

    simulator = await database_service.insert_simulator(
        git_commit_hash=latest_commit_hash, git_repo_url=repo_url, git_branch=main_branch
    )

    parca_dataset_request = ParcaDatasetRequest(simulator_version=simulator, parca_config=ParcaOptions())
    parca_dataset = await database_service.insert_parca_dataset(parca_dataset_request=parca_dataset_request)

    experiment_id = f"test_scheduler_insert_job-{str(uuid.uuid4())[:4]!s}"
    simulation_request = SimulationRequest(
        simulation_config_filename="config_filename",
        experiment_id=experiment_id,
        parca_dataset_id=parca_dataset.database_id,
        simulator_id=simulator.database_id,
        config=SimulationConfig(experiment_id=experiment_id),
    )
    simulation = await database_service.insert_simulation(sim_request=simulation_request)
    slurm_job = SlurmJob(
        job_id=slurmjobid,
        name="name",
        account="acct",
        user_name="user",
        job_state="RUNNING",
    )

    random_string = "".join(random.choices(string.hexdigits, k=7))
    correlation_id = get_correlation_id(ecoli_simulation=simulation, random_string=random_string, simulator=simulator)
    hpcrun = await database_service.insert_hpcrun(
        job_id=JobId.slurm(slurm_job.job_id),
        job_type=JobType.SIMULATION,
        ref_id=simulation.database_id,
        correlation_id=correlation_id,
    )

    return simulation, slurm_job, hpcrun


async def insert_chain_campaign_job(
    database_service: DatabaseServiceSQL,
    *,
    job_id_ext: str,
    chain_n_generations: int,
    n_seeds: int,
    chain_parca_done: bool = False,
    chain_current_job_ids: list[str | None] | None = None,
    chain_current_generation: list[int | None] | None = None,
    chain_final_job_ids: list[str] | None = None,
    swap_processes: dict[str, str] | None = None,
    variants: dict[str, object] | None = None,
    composite_id: str | None = None,
    cache_variant: str | None = None,
) -> tuple[Simulation, HpcRun]:
    """Insert a Simulation + a chain-dispatch-campaign-shaped HpcRun row
    (backlog item 71 Phase 4 — app-level per-seed gating), the fixture shape
    JobScheduler.update_chain_campaigns/_advance_chain_campaign consumes.
    Defaults represent a freshly-submitted campaign (ParCa not yet done,
    every seed slot empty) — the same initial state
    ``SimulationServiceRay.submit_chain_dispatch_job`` itself writes; pass the
    ``chain_*`` kwargs explicitly to represent a campaign already mid-flight.

    ``swap_processes``/``variants`` (backlog item 93): both default to
    ``None`` (every existing caller keeps building a plain no-injection
    campaign, unchanged) — set either to give the inserted Simulation's own
    config real injection intent, exercising
    ``injected_processes_from_config``'s live consumer,
    ``JobScheduler._advance_parca_gate``/``_advance_seed_generations``.

    ``composite_id`` (backlog item 105): same default-None, same shape —
    set to give the inserted Simulation's own config a caller-selected
    composite (e.g. ``reactor_bird_coupled``) instead of the implicit
    ``ecoli_baseline`` default.

    ``cache_variant`` (backlog item 105): same default-None, same shape —
    set to have the campaign stage from a ``variant``-labeled ParCa cache
    (e.g. a strain-specific induced-expression build, see
    ``SimulationServiceRay.submit_new_gene_cache_job``) instead of the plain
    commit-only cache.
    """
    latest_commit_hash = str(uuid.uuid4())
    simulator = await database_service.insert_simulator(
        git_commit_hash=latest_commit_hash,
        git_repo_url="https://github.com/CovertLabEcoli/sms-ecoli",
        git_branch="main",
    )
    parca_dataset = await database_service.insert_parca_dataset(
        parca_dataset_request=ParcaDatasetRequest(simulator_version=simulator, parca_config=ParcaOptions())
    )
    experiment_id = f"test-chain-campaign-{str(uuid.uuid4())[:8]!s}"
    config = SimulationConfig(experiment_id=experiment_id, generations=chain_n_generations)
    setattr(config, "n_init_sims", n_seeds)  # noqa: B010
    if swap_processes is not None:
        setattr(config, "swap_processes", swap_processes)  # noqa: B010
    if variants is not None:
        setattr(config, "variants", variants)  # noqa: B010
    if composite_id is not None:
        setattr(config, "composite_id", composite_id)  # noqa: B010
    if cache_variant is not None:
        setattr(config, "cache_variant", cache_variant)  # noqa: B010
    simulation_request = SimulationRequest(
        simulation_config_filename="config_filename",
        experiment_id=experiment_id,
        parca_dataset_id=parca_dataset.database_id,
        simulator_id=simulator.database_id,
        config=config,
    )
    simulation = await database_service.insert_simulation(sim_request=simulation_request)
    hpcrun = await database_service.insert_hpcrun(
        job_id=JobId.ray(job_id_ext),
        job_type=JobType.SIMULATION,
        ref_id=simulation.database_id,
        correlation_id=f"chain-campaign-{experiment_id}",
        chain_n_generations=chain_n_generations,
        chain_final_job_ids=chain_final_job_ids if chain_final_job_ids is not None else [],
        chain_current_job_ids=chain_current_job_ids if chain_current_job_ids is not None else [None] * n_seeds,
        chain_current_generation=chain_current_generation if chain_current_generation is not None else [None] * n_seeds,
        chain_parca_done=chain_parca_done,
    )
    return simulation, hpcrun


def _mock_ray_service() -> MagicMock:
    """A MagicMock pre-shaped for SimulationServiceRay's chain-dispatch
    surface (backlog item 71 Phase 4), with the real async methods correctly
    set to AsyncMock (MagicMock's auto-mocked attributes default to sync) —
    tests override individual return values/side_effects as needed."""
    mock_ray = MagicMock()
    mock_ray.get_job_status = AsyncMock()
    mock_ray.stage_runner = AsyncMock(return_value="s3://mybucket/runner/run_pbg.py")
    mock_ray.submit_chain_generation_batch = AsyncMock()
    mock_ray.submit_campaign_analysis = AsyncMock(return_value="analysis-job-id")
    mock_ray.submit_multi_node_analysis = AsyncMock(return_value="mnp-analysis-job-id")
    mock_ray.cache_s3_uri = MagicMock(return_value="s3://mybucket/cache/commit")
    mock_ray.chain_base_tags = MagicMock(return_value={"Project": "v2ecoli-comparison"})
    return mock_ray


class TestAdvanceChainCampaign:
    """JobScheduler._advance_chain_campaign / update_chain_campaigns (backlog
    item 71 Phase 4 — app-level per-seed gating replacing native Batch
    dependsOn chains): the whole per-tick state machine, DatabaseService.
    advance_chain_campaign's advisory-lock write path, and the explicit
    no-double-submit regression property the plan requires.
    simulation_service_ray is mocked here (its own AWS Batch call shapes are
    proven for real in test_ray_backend.py) so these tests isolate
    JobScheduler's OWN orchestration decisions against a REAL Postgres
    database (testcontainers) -- including the real advisory-lock SQL."""

    @pytest.mark.asyncio
    async def test_update_chain_campaigns_is_a_noop_without_a_ray_service(self) -> None:
        """SLURM-only deployments wire simulation_service_ray=None -- the poll
        loop must not touch the database at all in that case, not just skip
        the AWS calls."""
        mock_database = AsyncMock()
        scheduler = JobScheduler(
            messaging_service=MagicMock(),
            database_service=mock_database,
            simulation_service_ray=None,
        )
        await scheduler.update_chain_campaigns()
        mock_database.list_active_chain_campaigns.assert_not_called()

    @pytest.mark.asyncio
    async def test_parca_still_running_leaves_campaign_untouched(self, database_service: DatabaseServiceSQL) -> None:
        _simulation, hpcrun = await insert_chain_campaign_job(
            database_service, job_id_ext="parca-running", chain_n_generations=3, n_seeds=4
        )
        mock_ray = _mock_ray_service()
        mock_ray.get_job_status.return_value = JobStatusInfo(
            job_id=JobId.ray("parca-running"), status=JobStatus.RUNNING
        )
        scheduler = JobScheduler(
            messaging_service=MagicMock(), database_service=database_service, simulation_service_ray=mock_ray
        )

        await scheduler._advance_chain_campaign(hpcrun, mock_ray)

        mock_ray.submit_chain_generation_batch.assert_not_awaited()
        refetched = await database_service.get_hpcrun(hpcrun.database_id)
        assert refetched is not None
        assert refetched.status == JobStatus.RUNNING
        assert refetched.chain_parca_done is False
        assert refetched.chain_current_job_ids == [None, None, None, None]

    @pytest.mark.asyncio
    async def test_parca_succeeded_fans_out_generation_zero_for_every_seed(
        self, database_service: DatabaseServiceSQL
    ) -> None:
        _simulation, hpcrun = await insert_chain_campaign_job(
            database_service, job_id_ext="parca-done", chain_n_generations=3, n_seeds=3
        )
        mock_ray = _mock_ray_service()
        mock_ray.get_job_status.return_value = JobStatusInfo(job_id=JobId.ray("parca-done"), status=JobStatus.COMPLETED)
        mock_ray.submit_chain_generation_batch.return_value = {0: "s0g0", 1: "s1g0", 2: "s2g0"}
        scheduler = JobScheduler(
            messaging_service=MagicMock(), database_service=database_service, simulation_service_ray=mock_ray
        )

        await scheduler._advance_chain_campaign(hpcrun, mock_ray)

        mock_ray.submit_chain_generation_batch.assert_awaited_once()
        call_kwargs = mock_ray.submit_chain_generation_batch.call_args.kwargs
        assert call_kwargs["seeds"] == [0, 1, 2]
        assert call_kwargs["generation_index"] == 0

        refetched = await database_service.get_hpcrun(hpcrun.database_id)
        assert refetched is not None
        assert refetched.chain_parca_done is True
        assert refetched.chain_current_job_ids == ["s0g0", "s1g0", "s2g0"]
        assert refetched.chain_current_generation == [0, 0, 0]
        assert refetched.status == JobStatus.RUNNING  # campaign itself not terminal yet

    @pytest.mark.asyncio
    async def test_parca_generation_zero_partial_submission_failure_leaves_that_seed_unresolved_at_zero(
        self, database_service: DatabaseServiceSQL
    ) -> None:
        """A seed whose generation-0 submission itself fails (even after
        retry-on-throttle) gets no entry in submit_chain_generation_batch's
        returned mapping -- its chain_current_job_ids slot stays None, which
        correctly makes it "already resolved, contributed nothing" once every
        OTHER seed also resolves, mirroring the pre-Phase-4 design's own
        "seed contributes nothing to chain_final_job_ids" semantics."""
        _simulation, hpcrun = await insert_chain_campaign_job(
            database_service, job_id_ext="parca-done-2", chain_n_generations=2, n_seeds=2
        )
        mock_ray = _mock_ray_service()
        mock_ray.get_job_status.return_value = JobStatusInfo(
            job_id=JobId.ray("parca-done-2"), status=JobStatus.COMPLETED
        )
        mock_ray.submit_chain_generation_batch.return_value = {0: "s0g0"}  # seed 1 failed to submit

        scheduler = JobScheduler(
            messaging_service=MagicMock(), database_service=database_service, simulation_service_ray=mock_ray
        )
        await scheduler._advance_chain_campaign(hpcrun, mock_ray)

        refetched = await database_service.get_hpcrun(hpcrun.database_id)
        assert refetched is not None
        assert refetched.chain_current_job_ids == ["s0g0", None]
        assert refetched.status == JobStatus.RUNNING  # seed 0 still in flight -- not terminal

    @pytest.mark.asyncio
    async def test_parca_failed_marks_campaign_failed_immediately(self, database_service: DatabaseServiceSQL) -> None:
        _simulation, hpcrun = await insert_chain_campaign_job(
            database_service, job_id_ext="parca-failed", chain_n_generations=3, n_seeds=4
        )
        mock_ray = _mock_ray_service()
        mock_ray.get_job_status.return_value = JobStatusInfo(job_id=JobId.ray("parca-failed"), status=JobStatus.FAILED)
        scheduler = JobScheduler(
            messaging_service=MagicMock(), database_service=database_service, simulation_service_ray=mock_ray
        )

        await scheduler._advance_chain_campaign(hpcrun, mock_ray)

        mock_ray.submit_chain_generation_batch.assert_not_awaited()
        refetched = await database_service.get_hpcrun(hpcrun.database_id)
        assert refetched is not None
        assert refetched.status == JobStatus.FAILED
        assert refetched.error_message is not None and "ParCa failed" in refetched.error_message
        assert refetched.chain_parca_done is False

    @pytest.mark.asyncio
    async def test_seed_succeeds_and_advances_to_its_next_generation(
        self, database_service: DatabaseServiceSQL
    ) -> None:
        _simulation, hpcrun = await insert_chain_campaign_job(
            database_service,
            job_id_ext="parca-1",
            chain_n_generations=3,
            n_seeds=2,
            chain_parca_done=True,
            chain_current_job_ids=["s0g0", "s1g0"],
            chain_current_generation=[0, 0],
        )
        mock_ray = _mock_ray_service()
        mock_ray.get_batch_job_statuses = MagicMock(return_value={"s0g0": JobStatus.COMPLETED})  # s1g0 not yet visible
        mock_ray.submit_chain_generation = MagicMock(return_value="s0g1")
        scheduler = JobScheduler(
            messaging_service=MagicMock(), database_service=database_service, simulation_service_ray=mock_ray
        )

        await scheduler._advance_chain_campaign(hpcrun, mock_ray)

        mock_ray.submit_chain_generation.assert_called_once()
        call_kwargs = mock_ray.submit_chain_generation.call_args.kwargs
        assert call_kwargs["seed"] == 0
        assert call_kwargs["generation_index"] == 1

        refetched = await database_service.get_hpcrun(hpcrun.database_id)
        assert refetched is not None
        assert refetched.chain_current_job_ids == ["s0g1", "s1g0"]  # seed 0 advanced, seed 1 untouched
        assert refetched.chain_current_generation == [1, 0]
        assert refetched.chain_final_job_ids == []

    @pytest.mark.asyncio
    async def test_generation_zero_fanout_forwards_injected_processes_and_variants(
        self, database_service: DatabaseServiceSQL
    ) -> None:
        """Backlog item 93: _advance_parca_gate re-derives injected_processes/
        variants from the campaign's own Simulation.config (re-read fresh by
        _tick every poll, so this is restart-safe for free) and forwards them
        to submit_chain_generation_batch's generation-0 fan-out -- the real,
        live mechanism a Run 4 dispatch depends on."""
        _simulation, hpcrun = await insert_chain_campaign_job(
            database_service,
            job_id_ext="parca-done-run4",
            chain_n_generations=3,
            n_seeds=2,
            swap_processes={"ecoli-metabolism": "ecoli-metabolism-redux"},
            variants={"strain_design": {"perturbations": {"value": [{"EG11005": 0.0}]}}},
        )
        mock_ray = _mock_ray_service()
        mock_ray.get_job_status.return_value = JobStatusInfo(
            job_id=JobId.ray("parca-done-run4"), status=JobStatus.COMPLETED
        )
        mock_ray.submit_chain_generation_batch.return_value = {0: "s0g0", 1: "s1g0"}
        scheduler = JobScheduler(
            messaging_service=MagicMock(), database_service=database_service, simulation_service_ray=mock_ray
        )

        await scheduler._advance_chain_campaign(hpcrun, mock_ray)

        mock_ray.submit_chain_generation_batch.assert_awaited_once()
        call_kwargs = mock_ray.submit_chain_generation_batch.call_args.kwargs
        assert call_kwargs["injected_processes"] == {
            "swap_processes": {"ecoli-metabolism": "ecoli-metabolism-redux"},
            "add_processes": [],
            "exclude_processes": [],
            "fork_repo": "",
        }
        assert call_kwargs["variants"] == {"strain_design": {"perturbations": {"value": [{"EG11005": 0.0}]}}}

    @pytest.mark.asyncio
    async def test_generation_zero_fanout_omits_injection_keys_when_config_has_none(
        self, database_service: DatabaseServiceSQL
    ) -> None:
        """Regression: the existing no-injection campaign shape (every test
        above this one) must keep forwarding None/None, not an empty-but-
        truthy dict, matching injected_processes_from_config's own contract."""
        _simulation, hpcrun = await insert_chain_campaign_job(
            database_service, job_id_ext="parca-done-plain", chain_n_generations=3, n_seeds=2
        )
        mock_ray = _mock_ray_service()
        mock_ray.get_job_status.return_value = JobStatusInfo(
            job_id=JobId.ray("parca-done-plain"), status=JobStatus.COMPLETED
        )
        mock_ray.submit_chain_generation_batch.return_value = {0: "s0g0", 1: "s1g0"}
        scheduler = JobScheduler(
            messaging_service=MagicMock(), database_service=database_service, simulation_service_ray=mock_ray
        )

        await scheduler._advance_chain_campaign(hpcrun, mock_ray)

        call_kwargs = mock_ray.submit_chain_generation_batch.call_args.kwargs
        assert call_kwargs["injected_processes"] is None
        assert call_kwargs["variants"] is None
        assert call_kwargs["composite_id"] is None

    @pytest.mark.asyncio
    async def test_generation_zero_fanout_forwards_composite_id(self, database_service: DatabaseServiceSQL) -> None:
        """Backlog item 105: _advance_parca_gate re-derives composite_id from
        the campaign's own Simulation.config (same restart-safe pattern as
        injected_processes/variants) and forwards it to
        submit_chain_generation_batch's generation-0 fan-out -- the real
        mechanism Run 1/K4's reactor_bird_coupled dispatch depends on."""
        _simulation, hpcrun = await insert_chain_campaign_job(
            database_service,
            job_id_ext="parca-done-run1",
            chain_n_generations=3,
            n_seeds=2,
            composite_id="v2ecoli.composites.reactor_bird_coupled.reactor_bird_coupled",
        )
        mock_ray = _mock_ray_service()
        mock_ray.get_job_status.return_value = JobStatusInfo(
            job_id=JobId.ray("parca-done-run1"), status=JobStatus.COMPLETED
        )
        mock_ray.submit_chain_generation_batch.return_value = {0: "s0g0", 1: "s1g0"}
        scheduler = JobScheduler(
            messaging_service=MagicMock(), database_service=database_service, simulation_service_ray=mock_ray
        )

        await scheduler._advance_chain_campaign(hpcrun, mock_ray)

        call_kwargs = mock_ray.submit_chain_generation_batch.call_args.kwargs
        assert call_kwargs["composite_id"] == "v2ecoli.composites.reactor_bird_coupled.reactor_bird_coupled"

    @pytest.mark.asyncio
    async def test_seed_advance_forwards_injected_processes_and_variants(
        self, database_service: DatabaseServiceSQL
    ) -> None:
        """Backlog item 93: _advance_seed_generations (phase 2 -- every
        generation AFTER generation 0) must forward the same config-derived
        injected_processes/variants on every per-seed advance, not just the
        generation-0 burst -- otherwise a swap/new-gene composite would
        regress to the stock process the moment the campaign moves past its
        first generation."""
        _simulation, hpcrun = await insert_chain_campaign_job(
            database_service,
            job_id_ext="parca-1",
            chain_n_generations=3,
            n_seeds=1,
            chain_parca_done=True,
            chain_current_job_ids=["s0g0"],
            chain_current_generation=[0],
            swap_processes={"ecoli-metabolism": "ecoli-metabolism-redux"},
        )
        mock_ray = _mock_ray_service()
        mock_ray.get_batch_job_statuses = MagicMock(return_value={"s0g0": JobStatus.COMPLETED})
        mock_ray.submit_chain_generation = MagicMock(return_value="s0g1")
        scheduler = JobScheduler(
            messaging_service=MagicMock(), database_service=database_service, simulation_service_ray=mock_ray
        )

        await scheduler._advance_chain_campaign(hpcrun, mock_ray)

        call_kwargs = mock_ray.submit_chain_generation.call_args.kwargs
        assert call_kwargs["injected_processes"] == {
            "swap_processes": {"ecoli-metabolism": "ecoli-metabolism-redux"},
            "add_processes": [],
            "exclude_processes": [],
            "fork_repo": "",
        }
        assert call_kwargs["variants"] is None

    @pytest.mark.asyncio
    async def test_seed_advance_forwards_composite_id(self, database_service: DatabaseServiceSQL) -> None:
        """Backlog item 105: _advance_seed_generations (phase 2 -- every
        generation AFTER generation 0) must forward the same config-derived
        composite_id on every per-seed advance, not just the generation-0
        burst -- otherwise a reactor_bird_coupled campaign would silently
        regress to ecoli_baseline the moment it moves past its first
        generation."""
        _simulation, hpcrun = await insert_chain_campaign_job(
            database_service,
            job_id_ext="parca-1-run1",
            chain_n_generations=3,
            n_seeds=1,
            chain_parca_done=True,
            chain_current_job_ids=["s0g0"],
            chain_current_generation=[0],
            composite_id="v2ecoli.composites.reactor_bird_coupled.reactor_bird_coupled",
        )
        mock_ray = _mock_ray_service()
        mock_ray.get_batch_job_statuses = MagicMock(return_value={"s0g0": JobStatus.COMPLETED})
        mock_ray.submit_chain_generation = MagicMock(return_value="s0g1")
        scheduler = JobScheduler(
            messaging_service=MagicMock(), database_service=database_service, simulation_service_ray=mock_ray
        )

        await scheduler._advance_chain_campaign(hpcrun, mock_ray)

        call_kwargs = mock_ray.submit_chain_generation.call_args.kwargs
        assert call_kwargs["composite_id"] == "v2ecoli.composites.reactor_bird_coupled.reactor_bird_coupled"

    @pytest.mark.asyncio
    async def test_generation_zero_fanout_forwards_cache_variant(self, database_service: DatabaseServiceSQL) -> None:
        """Backlog item 105: _advance_parca_gate re-derives cache_variant from
        the campaign's own Simulation.config and resolves cache_s3_uri with it
        for the generation-0 fan-out -- the mechanism a strain-specific
        induced-expression cache (submit_new_gene_cache_job) depends on."""
        _simulation, hpcrun = await insert_chain_campaign_job(
            database_service,
            job_id_ext="parca-done-k4",
            chain_n_generations=3,
            n_seeds=2,
            cache_variant="k4-induced",
        )
        mock_ray = _mock_ray_service()
        mock_ray.get_job_status.return_value = JobStatusInfo(
            job_id=JobId.ray("parca-done-k4"), status=JobStatus.COMPLETED
        )
        mock_ray.submit_chain_generation_batch.return_value = {0: "s0g0", 1: "s1g0"}
        scheduler = JobScheduler(
            messaging_service=MagicMock(), database_service=database_service, simulation_service_ray=mock_ray
        )

        await scheduler._advance_chain_campaign(hpcrun, mock_ray)

        cache_uri_calls = mock_ray.cache_s3_uri.call_args_list
        assert any(c.kwargs.get("variant") == "k4-induced" for c in cache_uri_calls)

    @pytest.mark.asyncio
    async def test_seed_advance_forwards_cache_variant(self, database_service: DatabaseServiceSQL) -> None:
        """Backlog item 105: _advance_seed_generations (phase 2 -- every
        generation AFTER generation 0) must forward the same config-derived
        cache_variant on every per-seed advance, not just the generation-0
        burst -- otherwise a strain-specific campaign would silently regress
        to the plain commit cache the moment it moves past its first
        generation."""
        _simulation, hpcrun = await insert_chain_campaign_job(
            database_service,
            job_id_ext="parca-1-k4",
            chain_n_generations=3,
            n_seeds=1,
            chain_parca_done=True,
            chain_current_job_ids=["s0g0"],
            chain_current_generation=[0],
            cache_variant="k4-induced",
        )
        mock_ray = _mock_ray_service()
        mock_ray.get_batch_job_statuses = MagicMock(return_value={"s0g0": JobStatus.COMPLETED})
        mock_ray.submit_chain_generation = MagicMock(return_value="s0g1")
        scheduler = JobScheduler(
            messaging_service=MagicMock(), database_service=database_service, simulation_service_ray=mock_ray
        )

        await scheduler._advance_chain_campaign(hpcrun, mock_ray)

        cache_uri_calls = mock_ray.cache_s3_uri.call_args_list
        assert any(c.kwargs.get("variant") == "k4-induced" for c in cache_uri_calls)

    @pytest.mark.asyncio
    async def test_seed_succeeds_on_its_last_generation_resolves_the_seed(
        self, database_service: DatabaseServiceSQL
    ) -> None:
        _simulation, hpcrun = await insert_chain_campaign_job(
            database_service,
            job_id_ext="parca-1",
            chain_n_generations=2,
            n_seeds=2,
            chain_parca_done=True,
            chain_current_job_ids=["s0g1", "s1g0"],
            chain_current_generation=[1, 0],  # seed 0 is already on its LAST generation (index 1 of 2)
        )
        mock_ray = _mock_ray_service()
        mock_ray.get_batch_job_statuses = MagicMock(return_value={"s0g1": JobStatus.COMPLETED})
        scheduler = JobScheduler(
            messaging_service=MagicMock(), database_service=database_service, simulation_service_ray=mock_ray
        )

        await scheduler._advance_chain_campaign(hpcrun, mock_ray)

        mock_ray.submit_chain_generation.assert_not_called()  # nothing left to submit for this seed
        refetched = await database_service.get_hpcrun(hpcrun.database_id)
        assert refetched is not None
        assert refetched.chain_current_job_ids == [None, "s1g0"]  # seed 0 resolved, seed 1 untouched
        assert refetched.chain_final_job_ids == ["s0g1"]
        assert refetched.status == JobStatus.RUNNING  # seed 1 still in flight

    @pytest.mark.asyncio
    async def test_seed_fails_resolves_the_seed_as_failed_without_orchestrator_retry(
        self, database_service: DatabaseServiceSQL
    ) -> None:
        _simulation, hpcrun = await insert_chain_campaign_job(
            database_service,
            job_id_ext="parca-1",
            chain_n_generations=5,
            n_seeds=2,
            chain_parca_done=True,
            chain_current_job_ids=["s0g2", "s1g0"],
            chain_current_generation=[2, 0],
        )
        mock_ray = _mock_ray_service()
        mock_ray.get_batch_job_statuses = MagicMock(return_value={"s0g2": JobStatus.FAILED})
        scheduler = JobScheduler(
            messaging_service=MagicMock(), database_service=database_service, simulation_service_ray=mock_ray
        )

        await scheduler._advance_chain_campaign(hpcrun, mock_ray)

        mock_ray.submit_chain_generation.assert_not_called()  # a FAILED seed is never retried by the orchestrator
        refetched = await database_service.get_hpcrun(hpcrun.database_id)
        assert refetched is not None
        assert refetched.chain_current_job_ids == [None, "s1g0"]
        assert refetched.chain_final_job_ids == ["s0g2"]  # tracked as this seed's final id, despite being FAILED

    @pytest.mark.asyncio
    async def test_all_seeds_resolved_with_at_least_one_success_submits_the_analysis(
        self, database_service: DatabaseServiceSQL
    ) -> None:
        simulation, hpcrun = await insert_chain_campaign_job(
            database_service,
            job_id_ext="parca-1",
            chain_n_generations=2,
            n_seeds=2,
            chain_parca_done=True,
            chain_current_job_ids=["s0g1", None],  # seed 1 already resolved on a prior tick
            chain_current_generation=[1, None],
            chain_final_job_ids=["s1g0"],
        )
        mock_ray = _mock_ray_service()
        mock_ray.get_batch_job_statuses = MagicMock(return_value={"s0g1": JobStatus.COMPLETED})
        mock_ray.get_chain_campaign_result = MagicMock(
            return_value=ChainCampaignPollResult(terminal=True, succeeded_job_ids=["s0g1", "s1g0"])
        )
        scheduler = JobScheduler(
            messaging_service=MagicMock(), database_service=database_service, simulation_service_ray=mock_ray
        )

        await scheduler._advance_chain_campaign(hpcrun, mock_ray)

        mock_ray.get_chain_campaign_result.assert_called_once_with(["s1g0", "s0g1"])
        mock_ray.submit_campaign_analysis.assert_awaited_once()
        call_kwargs = mock_ray.submit_campaign_analysis.call_args.kwargs
        assert call_kwargs["total_n_seeds"] == 2
        assert call_kwargs["n_generations"] == 2
        assert call_kwargs["simulation"].database_id == simulation.database_id

        refetched = await database_service.get_hpcrun(hpcrun.database_id)
        assert refetched is not None
        assert refetched.status == JobStatus.COMPLETED
        assert refetched.error_message is None
        assert refetched.chain_current_job_ids == [None, None]

    @pytest.mark.asyncio
    async def test_partial_success_does_not_mark_the_campaign_completed_and_submits_no_analysis(
        self, database_service: DatabaseServiceSQL
    ) -> None:
        """P0-7: with fewer succeeded seed chains than expected, the campaign must
        NOT be COMPLETED (that let one surviving lineage out of thousands report a
        whole sweep done, and the multivariant analysis then ran over a store full
        of undetectable holes). It ends terminal-but-not-success, names the
        missing/failed ids, and submits no analysis."""
        _simulation, hpcrun = await insert_chain_campaign_job(
            database_service,
            job_id_ext="parca-1",
            chain_n_generations=2,
            n_seeds=3,  # three seeds expected...
            chain_parca_done=True,
            chain_current_job_ids=["s0g1", None, None],
            chain_current_generation=[1, None, None],
            chain_final_job_ids=["s1g1", "s2g0"],  # seeds 1 & 2 already resolved on prior ticks
        )
        mock_ray = _mock_ray_service()
        mock_ray.get_batch_job_statuses = MagicMock(return_value={"s0g1": JobStatus.COMPLETED})
        # ...but only 2 of 3 seed chains actually succeeded (s2g0 FAILED).
        mock_ray.get_chain_campaign_result = MagicMock(
            return_value=ChainCampaignPollResult(
                terminal=True, succeeded_job_ids=["s0g1", "s1g1"], failed_job_ids=["s2g0"]
            )
        )
        scheduler = JobScheduler(
            messaging_service=MagicMock(), database_service=database_service, simulation_service_ray=mock_ray
        )

        await scheduler._advance_chain_campaign(hpcrun, mock_ray)

        mock_ray.submit_campaign_analysis.assert_not_awaited()
        refetched = await database_service.get_hpcrun(hpcrun.database_id)
        assert refetched is not None
        assert refetched.status != JobStatus.COMPLETED
        assert refetched.status == JobStatus.FAILED
        assert refetched.error_message is not None
        assert "2/3" in refetched.error_message
        assert "s2g0" in refetched.error_message  # the missing/failed final job id is named

    @pytest.mark.asyncio
    async def test_all_seeds_resolved_zero_succeeded_marks_failed_no_analysis(
        self, database_service: DatabaseServiceSQL
    ) -> None:
        _simulation, hpcrun = await insert_chain_campaign_job(
            database_service,
            job_id_ext="parca-1",
            chain_n_generations=1,
            n_seeds=2,
            chain_parca_done=True,
            chain_current_job_ids=["s0g0", None],
            chain_current_generation=[0, None],
            chain_final_job_ids=["s1g0"],
        )
        mock_ray = _mock_ray_service()
        mock_ray.get_batch_job_statuses = MagicMock(return_value={"s0g0": JobStatus.FAILED})
        mock_ray.get_chain_campaign_result = MagicMock(
            return_value=ChainCampaignPollResult(terminal=True, failed_job_ids=["s1g0", "s0g0"])
        )
        scheduler = JobScheduler(
            messaging_service=MagicMock(), database_service=database_service, simulation_service_ray=mock_ray
        )

        await scheduler._advance_chain_campaign(hpcrun, mock_ray)

        mock_ray.submit_campaign_analysis.assert_not_awaited()
        refetched = await database_service.get_hpcrun(hpcrun.database_id)
        assert refetched is not None
        assert refetched.status == JobStatus.FAILED
        assert refetched.error_message is not None and "zero seed chains succeeded" in refetched.error_message

    @pytest.mark.asyncio
    async def test_already_terminal_campaign_is_left_alone(self, database_service: DatabaseServiceSQL) -> None:
        """A campaign a concurrent tick (or a cancel request) already resolved
        must be a hard no-op -- covers the ``fresh.status in (...)`` early
        return, real evidence the FRESH re-read (not the possibly-stale
        ``campaign`` parameter) is what gates every decision."""
        _simulation, hpcrun = await insert_chain_campaign_job(
            database_service, job_id_ext="parca-1", chain_n_generations=2, n_seeds=2, chain_parca_done=True
        )
        await database_service.update_hpcrun_status(
            hpcrun_id=hpcrun.database_id, update=JobStatusUpdate(job_id=hpcrun.job_id, status=JobStatus.CANCELLED)
        )
        mock_ray = _mock_ray_service()
        scheduler = JobScheduler(
            messaging_service=MagicMock(), database_service=database_service, simulation_service_ray=mock_ray
        )

        # `hpcrun` (passed in) is stale -- still shows RUNNING, the state before
        # the CANCELLED write above -- exactly the shape a poll-loop iteration
        # would pass in if a cancel request landed between listing and advancing.
        await scheduler._advance_chain_campaign(hpcrun, mock_ray)

        mock_ray.get_job_status.assert_not_awaited()
        mock_ray.submit_chain_generation_batch.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_update_chain_campaigns_processes_every_active_campaign_row(
        self, database_service: DatabaseServiceSQL
    ) -> None:
        """End-to-end through update_chain_campaigns (not calling
        _advance_chain_campaign directly): two independent chain-dispatch
        campaigns, each gets polled and advanced correctly, using
        list_active_chain_campaigns's real WHERE clause (status IN (PENDING,
        RUNNING) AND chain_n_generations IS NOT NULL)."""
        _sim_a, hpcrun_a = await insert_chain_campaign_job(
            database_service, job_id_ext="camp-a-parca", chain_n_generations=2, n_seeds=2
        )
        _sim_b, hpcrun_b = await insert_chain_campaign_job(
            database_service, job_id_ext="camp-b-parca", chain_n_generations=2, n_seeds=3
        )

        def _parca_status(job_id: JobId) -> JobStatusInfo:
            status = JobStatus.COMPLETED if job_id.value == "camp-a-parca" else JobStatus.RUNNING
            return JobStatusInfo(job_id=job_id, status=status)

        mock_ray = _mock_ray_service()
        mock_ray.get_job_status.side_effect = _parca_status
        mock_ray.submit_chain_generation_batch.return_value = {0: "a-s0g0", 1: "a-s1g0"}
        scheduler = JobScheduler(
            messaging_service=MagicMock(), database_service=database_service, simulation_service_ray=mock_ray
        )

        await scheduler.update_chain_campaigns()

        # Campaign A: ParCa done -> generation 0 fanned out for both seeds.
        refetched_a = await database_service.get_hpcrun(hpcrun_a.database_id)
        assert refetched_a is not None
        assert refetched_a.chain_parca_done is True
        assert refetched_a.chain_current_job_ids == ["a-s0g0", "a-s1g0"]

        # Campaign B: ParCa still running -> left untouched.
        refetched_b = await database_service.get_hpcrun(hpcrun_b.database_id)
        assert refetched_b is not None
        assert refetched_b.chain_parca_done is False
        assert refetched_b.chain_current_job_ids == [None, None, None]

    @pytest.mark.asyncio
    async def test_concurrent_ticks_against_the_same_campaign_never_double_submit(
        self, database_service: DatabaseServiceSQL
    ) -> None:
        """The explicit regression property backlog item 71 Phase 4's plan
        requires: two overlapping poll ticks against the SAME campaign (e.g. a
        rolling restart briefly running two pods) must not both submit the
        same generation. Real concurrent execution (asyncio.gather, not a
        sequential double-call) against the REAL Postgres advisory lock
        (DatabaseService.advance_chain_campaign) -- a mock database could not
        exercise this property at all, since the lock is real SQL, not
        orchestration logic."""
        _simulation, hpcrun = await insert_chain_campaign_job(
            database_service,
            job_id_ext="parca-1",
            chain_n_generations=3,
            n_seeds=1,
            chain_parca_done=True,
            chain_current_job_ids=["s0g0"],
            chain_current_generation=[0],
        )
        submit_calls: list[tuple[int, int]] = []

        def _statuses(job_ids: list[str]) -> dict[str, JobStatus]:
            return dict.fromkeys(job_ids, JobStatus.COMPLETED)

        def _submit(seed: int, generation_index: int, **_kwargs: object) -> str:
            submit_calls.append((seed, generation_index))
            return f"s{seed}g{generation_index}"

        mock_ray = _mock_ray_service()
        # Real concurrent Postgres access is what actually serializes these two
        # ticks (advance_chain_campaign's pg_advisory_xact_lock blocks the
        # second task at the DB level until the first's transaction commits) --
        # no artificial delay needed here to create the race; the two
        # asyncio.gather'd coroutines already start together, and the lock
        # itself provides the mutual exclusion under test.
        mock_ray.get_batch_job_statuses = MagicMock(side_effect=_statuses)
        mock_ray.submit_chain_generation = MagicMock(side_effect=_submit)
        scheduler = JobScheduler(
            messaging_service=MagicMock(), database_service=database_service, simulation_service_ray=mock_ray
        )

        await asyncio.gather(
            scheduler._advance_chain_campaign(hpcrun, mock_ray),
            scheduler._advance_chain_campaign(hpcrun, mock_ray),
        )

        # THE regression property: generation 1 for seed 0 must have been
        # submitted exactly ONCE, never twice. Without the advisory lock, both
        # ticks would read the SAME stale ["s0g0"]/[0] state concurrently and
        # both independently decide to submit generation 1 -- a real
        # double-submit that would race on the same deterministic S3 daughter-
        # state key. With the lock, the second tick's fresh re-read (acquired
        # only after the first tick's write commits) sees the campaign has
        # ALREADY moved past generation 1, so it correctly advances the
        # campaign further instead (this mock reports every job id as
        # instantly COMPLETED, so tick B legitimately submits generation 2 —
        # real forward progress, not a stall, just never the SAME generation
        # a concurrent tick already claimed).
        assert submit_calls.count((0, 1)) == 1
        assert submit_calls.count((0, 2)) == 1

        refetched = await database_service.get_hpcrun(hpcrun.database_id)
        assert refetched is not None
        assert refetched.chain_current_job_ids == ["s0g2"]
        assert refetched.chain_current_generation == [2]


async def insert_multi_node_composite_job(
    database_service: DatabaseServiceSQL,
    *,
    job_id_ext: str,
    composite_id: str = "some_workspace.composites.some_multi_node_composite",
) -> tuple[Simulation, HpcRun]:
    """Insert a Simulation + a multi-node-composite-shaped HpcRun row (backlog
    item 88 — e.g. a colony composite spread across N Ray-cluster nodes), the
    fixture shape JobScheduler.update_multi_node_jobs/_advance_multi_node_job
    consumes. Mirrors insert_chain_campaign_job's own shape, but for the
    single-job (not N-seed-chain) discriminator: multi_node_composite_id."""
    latest_commit_hash = str(uuid.uuid4())
    simulator = await database_service.insert_simulator(
        git_commit_hash=latest_commit_hash,
        git_repo_url="https://github.com/CovertLabEcoli/sms-ecoli",
        git_branch="main",
    )
    parca_dataset = await database_service.insert_parca_dataset(
        parca_dataset_request=ParcaDatasetRequest(simulator_version=simulator, parca_config=ParcaOptions())
    )
    experiment_id = f"test-mnp-composite-{str(uuid.uuid4())[:8]!s}"
    simulation_request = SimulationRequest(
        simulation_config_filename="config_filename",
        experiment_id=experiment_id,
        parca_dataset_id=parca_dataset.database_id,
        simulator_id=simulator.database_id,
        config=SimulationConfig(experiment_id=experiment_id),
    )
    simulation = await database_service.insert_simulation(sim_request=simulation_request)
    hpcrun = await database_service.insert_hpcrun(
        job_id=JobId.ray(job_id_ext),
        job_type=JobType.SIMULATION,
        ref_id=simulation.database_id,
        correlation_id=f"mnp-composite-{experiment_id}",
        multi_node_composite_id=composite_id,
    )
    return simulation, hpcrun


class TestUpdateMultiNodeJobs:
    """JobScheduler.update_multi_node_jobs / _advance_multi_node_job (backlog
    item 88): the "Analysis flush" auto-trigger for a generic multi-node
    process-bigraph composite dispatch, mirroring update_chain_campaigns'
    role for chain-dispatch campaigns but via a deliberately SEPARATE,
    additive code path -- these tests exist specifically to prove that
    separation holds against a REAL Postgres database (testcontainers), not
    just by code inspection."""

    @pytest.mark.asyncio
    async def test_update_multi_node_jobs_is_a_noop_without_a_ray_service(self) -> None:
        mock_database = AsyncMock()
        scheduler = JobScheduler(
            messaging_service=MagicMock(),
            database_service=mock_database,
            simulation_service_ray=None,
        )
        await scheduler.update_multi_node_jobs()
        mock_database.list_active_multi_node_composites.assert_not_called()

    @pytest.mark.asyncio
    async def test_still_running_job_leaves_row_untouched_and_submits_no_analysis(
        self, database_service: DatabaseServiceSQL
    ) -> None:
        _simulation, hpcrun = await insert_multi_node_composite_job(database_service, job_id_ext="mnp-running")
        mock_ray = _mock_ray_service()
        mock_ray.get_job_status.return_value = JobStatusInfo(job_id=JobId.ray("mnp-running"), status=JobStatus.RUNNING)
        scheduler = JobScheduler(
            messaging_service=MagicMock(), database_service=database_service, simulation_service_ray=mock_ray
        )

        await scheduler._advance_multi_node_job(hpcrun, mock_ray)

        mock_ray.submit_multi_node_analysis.assert_not_awaited()
        refetched = await database_service.get_hpcrun(hpcrun.database_id)
        assert refetched is not None
        assert refetched.status == JobStatus.RUNNING

    @pytest.mark.asyncio
    async def test_completed_job_finalizes_row_and_submits_analysis_exactly_once(
        self, database_service: DatabaseServiceSQL
    ) -> None:
        _simulation, hpcrun = await insert_multi_node_composite_job(
            database_service, job_id_ext="mnp-done", composite_id="v2ecoli.composites.ecoli_colony.ecoli_colony"
        )
        mock_ray = _mock_ray_service()
        mock_ray.get_job_status.return_value = JobStatusInfo(job_id=JobId.ray("mnp-done"), status=JobStatus.COMPLETED)
        scheduler = JobScheduler(
            messaging_service=MagicMock(), database_service=database_service, simulation_service_ray=mock_ray
        )

        await scheduler._advance_multi_node_job(hpc_run=hpcrun, simulation_service_ray=mock_ray)

        mock_ray.submit_multi_node_analysis.assert_awaited_once()
        call_kwargs = mock_ray.submit_multi_node_analysis.call_args.kwargs
        assert call_kwargs["composite_id"] == "v2ecoli.composites.ecoli_colony.ecoli_colony"

        refetched = await database_service.get_hpcrun(hpcrun.database_id)
        assert refetched is not None
        assert refetched.status == JobStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_failed_job_finalizes_row_but_does_not_submit_analysis(
        self, database_service: DatabaseServiceSQL
    ) -> None:
        _simulation, hpcrun = await insert_multi_node_composite_job(database_service, job_id_ext="mnp-failed")
        mock_ray = _mock_ray_service()
        mock_ray.get_job_status.return_value = JobStatusInfo(
            job_id=JobId.ray("mnp-failed"), status=JobStatus.FAILED, error_message="Batch job failed"
        )
        scheduler = JobScheduler(
            messaging_service=MagicMock(), database_service=database_service, simulation_service_ray=mock_ray
        )

        await scheduler._advance_multi_node_job(hpc_run=hpcrun, simulation_service_ray=mock_ray)

        mock_ray.submit_multi_node_analysis.assert_not_awaited()
        refetched = await database_service.get_hpcrun(hpcrun.database_id)
        assert refetched is not None
        assert refetched.status == JobStatus.FAILED
        assert refetched.error_message == "Batch job failed"

    @pytest.mark.asyncio
    async def test_concurrent_finalize_only_one_tick_wins_the_race(self, database_service: DatabaseServiceSQL) -> None:
        """The explicit regression property finalize_multi_node_job exists to
        guarantee: two overlapping polling ticks against the SAME completed
        job (e.g. two pods briefly overlapping during a rolling restart) must
        never both submit the analysis job. Runs the real atomic conditional
        UPDATE concurrently via asyncio.gather against a real Postgres
        testcontainer -- the same methodology item 71 Phase 4's own advisory
        -lock regression test (PR #260) used for its own double-submit
        guarantee."""
        _simulation, hpcrun = await insert_multi_node_composite_job(database_service, job_id_ext="mnp-race")

        results = await asyncio.gather(
            database_service.finalize_multi_node_job(hpcrun.database_id, JobStatus.COMPLETED),
            database_service.finalize_multi_node_job(hpcrun.database_id, JobStatus.COMPLETED),
        )
        assert sorted(results) == [False, True]

    @pytest.mark.asyncio
    async def test_chain_dispatch_and_multi_node_polling_are_mutually_disjoint(
        self, database_service: DatabaseServiceSQL
    ) -> None:
        """The explicit "existing chain-dispatch path is unaffected" proof:
        with BOTH a chain-dispatch campaign row and a multi-node-composite row
        active simultaneously, each poller's own query must see ONLY its own
        row -- never the other's, and never the union. Proves
        list_active_chain_campaigns/list_active_multi_node_composites are
        structurally disjoint against a real Postgres database, not just by
        reading the two WHERE clauses."""
        _chain_sim, chain_hpcrun = await insert_chain_campaign_job(
            database_service, job_id_ext="disjoint-chain", chain_n_generations=2, n_seeds=1
        )
        _mnp_sim, mnp_hpcrun = await insert_multi_node_composite_job(database_service, job_id_ext="disjoint-mnp")

        chain_campaigns = await database_service.list_active_chain_campaigns()
        chain_ids = {c.database_id for c in chain_campaigns}
        assert chain_hpcrun.database_id in chain_ids
        assert mnp_hpcrun.database_id not in chain_ids

        mnp_jobs = await database_service.list_active_multi_node_composites()
        mnp_ids = {m.database_id for m in mnp_jobs}
        assert mnp_hpcrun.database_id in mnp_ids
        assert chain_hpcrun.database_id not in mnp_ids

        # Advancing the multi-node job must not touch the chain campaign row.
        mock_ray = _mock_ray_service()
        mock_ray.get_job_status.return_value = JobStatusInfo(
            job_id=JobId.ray("disjoint-mnp"), status=JobStatus.COMPLETED
        )
        scheduler = JobScheduler(
            messaging_service=MagicMock(), database_service=database_service, simulation_service_ray=mock_ray
        )
        await scheduler._advance_multi_node_job(hpc_run=mnp_hpcrun, simulation_service_ray=mock_ray)

        untouched_chain = await database_service.get_hpcrun(chain_hpcrun.database_id)
        assert untouched_chain is not None
        assert untouched_chain.status == JobStatus.RUNNING
        assert untouched_chain.chain_current_job_ids == [None]
        mock_ray.submit_chain_generation_batch.assert_not_awaited()


@pytest.mark.integration
@pytest.mark.skipif(not Path(get_settings().slurm_submit_key_path).exists(), reason="slurm ssh key file not supplied")
async def insert_local_build_row(
    database_service: DatabaseServiceSQL,
    *,
    task_id: str,
    repo_url: str = RepoUrl.V2ECOLI_REPO_URL.value,
    external_job_ids: list[str] | None = None,
    age_seconds: float = 0.0,
) -> HpcRun:
    """A BUILD_IMAGE HpcRun row pointing at a LOCAL task id -- the exact shape
    upload_simulator writes for a DooD build (viva-api#414). ``age_seconds``
    back-dates start_time so the reconciler's grace window can be exercised."""
    simulator = await database_service.insert_simulator(
        git_commit_hash=str(uuid.uuid4())[:7], git_repo_url=repo_url, git_branch="main"
    )
    hpcrun = await database_service.insert_hpcrun(
        job_id=JobId.local(task_id), job_type=JobType.BUILD_IMAGE, ref_id=simulator.database_id, correlation_id="N/A"
    )
    if external_job_ids is not None:
        await database_service.set_hpcrun_external_job_ids(hpcrun.database_id, external_job_ids)
    if age_seconds:
        started = datetime.datetime.now() - datetime.timedelta(seconds=age_seconds)
        await database_service.update_hpcrun_status(
            hpcrun_id=hpcrun.database_id,
            update=JobStatusUpdate(job_id=hpcrun.job_id, status=JobStatus.RUNNING, start_time=started.isoformat()),
        )
    fresh = await database_service.get_hpcrun(hpcrun.database_id)
    assert fresh is not None
    return fresh


def _batch_state(job_id: str, status: str, *, reason: str | None = None, stopped_at_ms: int | None = None) -> Any:
    return batch_build.BatchJobState(
        job_id=job_id, job_name=f"name-{job_id}", status=status, status_reason=reason, stopped_at_ms=stopped_at_ms
    )


def _reconciling_scheduler(database_service: DatabaseServiceSQL, local: LocalTaskService | None) -> JobScheduler:
    return JobScheduler(
        messaging_service=MagicMock(),
        database_service=database_service,
        slurm_service=None,
        simulation_service_ray=None,
        local_task_service=local,
    )


class TestReconcileLocalTasks:
    """JobScheduler.reconcile_local_tasks (viva-api#414): every tick, finish
    active LOCAL HpcRun rows that no live process owns from the external
    work's true state. Real Postgres (testcontainers); AWS Batch mocked at the
    batch_build helper boundary."""

    @pytest_asyncio.fixture(autouse=True)
    async def _retire_leftover_local_rows(self, database_service: DatabaseServiceSQL) -> None:
        """The Postgres fixture is shared across tests; an active LOCAL row left
        behind by another test would be reconciled here too and skew the
        call-count assertions. Retire them first so every test sees only its own."""
        for row in await database_service.list_active_local_hpcruns():
            await database_service.update_hpcrun_status(
                hpcrun_id=row.database_id,
                update=JobStatusUpdate(job_id=row.job_id, status=JobStatus.CANCELLED, error_message="test cleanup"),
            )

    @pytest.mark.asyncio
    async def test_noop_without_a_local_task_service(self) -> None:
        db = MagicMock()
        db.list_active_local_hpcruns = AsyncMock()
        scheduler = JobScheduler(messaging_service=MagicMock(), database_service=db, local_task_service=None)
        await scheduler.reconcile_local_tasks()
        db.list_active_local_hpcruns.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_row_this_process_owns_is_left_alone(self, database_service: DatabaseServiceSQL) -> None:
        local = LocalTaskService()

        async def slow() -> None:
            await asyncio.sleep(10)

        job_id = local.submit(slow(), name="build")
        row = await insert_local_build_row(database_service, task_id=job_id.value, external_job_ids=["bj-1"])
        scheduler = _reconciling_scheduler(database_service, local)
        with patch.object(batch_build, "describe_batch_jobs", new=AsyncMock()) as describe:
            await scheduler.reconcile_local_tasks()
        describe.assert_not_awaited()
        fresh = await database_service.get_hpcrun(row.database_id)
        assert fresh is not None and fresh.status == JobStatus.RUNNING
        local.cancel(job_id.value)

    @pytest.mark.asyncio
    async def test_orphaned_build_whose_batch_job_succeeded_is_completed(
        self, database_service: DatabaseServiceSQL
    ) -> None:
        """The measured case: hpcrun 506, build SUCCEEDED 6 minutes after the
        owning pod was replaced, row stuck at running until hand-edited."""
        row = await insert_local_build_row(database_service, task_id="dead0506", external_job_ids=["bj-1"])
        scheduler = _reconciling_scheduler(database_service, LocalTaskService())
        stopped_at = int(datetime.datetime(2026, 9, 4, 17, 19, 39).timestamp() * 1000)
        with patch.object(
            batch_build,
            "describe_batch_jobs",
            new=AsyncMock(return_value={"bj-1": _batch_state("bj-1", "SUCCEEDED", stopped_at_ms=stopped_at)}),
        ) as describe:
            await scheduler.reconcile_local_tasks()
        describe.assert_awaited_once_with(["bj-1"])
        fresh = await database_service.get_hpcrun(row.database_id)
        assert fresh is not None
        assert fresh.status == JobStatus.COMPLETED
        assert fresh.error_message is None
        assert fresh.end_time is not None and fresh.end_time.startswith("2026-09-04 17:19:39")

    @pytest.mark.asyncio
    async def test_orphaned_build_with_one_failed_job_is_failed_with_the_reason(
        self, database_service: DatabaseServiceSQL
    ) -> None:
        row = await insert_local_build_row(database_service, task_id="deadf41l", external_job_ids=["arm", "amd"])
        scheduler = _reconciling_scheduler(database_service, LocalTaskService())
        with patch.object(
            batch_build,
            "describe_batch_jobs",
            new=AsyncMock(
                return_value={
                    "arm": _batch_state("arm", "SUCCEEDED"),
                    "amd": _batch_state("amd", "FAILED", reason="Essential container in task exited"),
                }
            ),
        ):
            await scheduler.reconcile_local_tasks()
        fresh = await database_service.get_hpcrun(row.database_id)
        assert fresh is not None
        assert fresh.status == JobStatus.FAILED
        assert "name-amd" in (fresh.error_message or "")
        assert "Essential container" in (fresh.error_message or "")

    @pytest.mark.asyncio
    async def test_orphaned_build_still_running_on_batch_is_left_running(
        self, database_service: DatabaseServiceSQL
    ) -> None:
        row = await insert_local_build_row(
            database_service, task_id="deadrunn", external_job_ids=["bj-1"], age_seconds=LOCAL_ORPHAN_GRACE_SECONDS * 5
        )
        scheduler = _reconciling_scheduler(database_service, LocalTaskService())
        with patch.object(
            batch_build, "describe_batch_jobs", new=AsyncMock(return_value={"bj-1": _batch_state("bj-1", "RUNNING")})
        ):
            await scheduler.reconcile_local_tasks()
        fresh = await database_service.get_hpcrun(row.database_id)
        assert fresh is not None
        assert fresh.status == JobStatus.RUNNING  # far past grace, but the work is alive: derive, don't guess
        assert fresh.end_time is None

    @pytest.mark.asyncio
    async def test_orphaned_build_with_no_handle_is_failed_only_after_grace(
        self, database_service: DatabaseServiceSQL
    ) -> None:
        young = await insert_local_build_row(database_service, task_id="deadyung", age_seconds=5)
        old = await insert_local_build_row(
            database_service, task_id="dead0old", age_seconds=LOCAL_ORPHAN_GRACE_SECONDS + 60
        )
        scheduler = _reconciling_scheduler(database_service, LocalTaskService())
        settings = MagicMock(build_amd64_queue="q-amd64", build_arm64_queue="q-arm64")
        with (
            patch("viva_api.simulation.job_scheduler.get_settings", return_value=settings),
            patch.object(batch_build, "find_batch_job_ids_by_name", new=AsyncMock(return_value=[])),
            patch.object(batch_build, "describe_batch_jobs", new=AsyncMock()) as describe,
        ):
            await scheduler.reconcile_local_tasks()
        describe.assert_not_awaited()
        fresh_young = await database_service.get_hpcrun(young.database_id)
        fresh_old = await database_service.get_hpcrun(old.database_id)
        assert fresh_young is not None and fresh_young.status == JobStatus.RUNNING
        assert fresh_old is not None and fresh_old.status == JobStatus.FAILED
        assert "re-upload the simulator" in (fresh_old.error_message or "")

    @pytest.mark.asyncio
    async def test_legacy_build_row_is_resolved_by_deterministic_job_name_and_handle_persisted(
        self, database_service: DatabaseServiceSQL
    ) -> None:
        """A row written before external_job_ids existed (every orphan on a
        live site today) is still recoverable: the build's Batch job name is
        deterministic in the commit."""
        row = await insert_local_build_row(database_service, task_id="deadlegc", age_seconds=30)
        simulator = await database_service.get_simulator(simulator_id=row.ref_id)
        assert simulator is not None
        scheduler = _reconciling_scheduler(database_service, LocalTaskService())
        settings = MagicMock(build_amd64_queue="q-amd64", build_arm64_queue="q-arm64")
        with (
            patch("viva_api.simulation.job_scheduler.get_settings", return_value=settings),
            patch.object(batch_build, "find_batch_job_ids_by_name", new=AsyncMock(return_value=["found-1"])) as find,
            patch.object(
                batch_build,
                "describe_batch_jobs",
                new=AsyncMock(return_value={"found-1": _batch_state("found-1", "RUNNING")}),
            ),
        ):
            await scheduler.reconcile_local_tasks()
        find.assert_awaited_once()
        assert find.await_args is not None
        assert find.await_args.args == ("q-amd64", batch_build.ray_build_job_name(simulator.git_commit_hash))
        assert find.await_args.kwargs["created_after_ms"] is not None
        fresh = await database_service.get_hpcrun(row.database_id)
        assert fresh is not None
        assert fresh.status == JobStatus.RUNNING
        assert fresh.external_job_ids == ["found-1"]  # persisted: the next tick is a plain describe

        # next tick: no name lookup, describe by the persisted id, finish
        with (
            patch.object(batch_build, "find_batch_job_ids_by_name", new=AsyncMock()) as find_again,
            patch.object(
                batch_build,
                "describe_batch_jobs",
                new=AsyncMock(return_value={"found-1": _batch_state("found-1", "SUCCEEDED")}),
            ),
        ):
            await scheduler.reconcile_local_tasks()
        find_again.assert_not_awaited()
        fresh = await database_service.get_hpcrun(row.database_id)
        assert fresh is not None and fresh.status == JobStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_legacy_vecoli_build_looks_up_both_arch_jobs(self, database_service: DatabaseServiceSQL) -> None:
        row = await insert_local_build_row(
            database_service, task_id="deadk8s0", repo_url=RepoUrl.VECOLI_PRIVATE_REPO_URL.value
        )
        simulator = await database_service.get_simulator(simulator_id=row.ref_id)
        assert simulator is not None
        names = batch_build.k8s_build_job_names(simulator.git_commit_hash)
        scheduler = _reconciling_scheduler(database_service, LocalTaskService())
        settings = MagicMock(build_amd64_queue="q-amd64", build_arm64_queue="q-arm64")

        async def _find(queue: str, name: str, **_: Any) -> list[str]:
            return {("q-arm64", names["arm64"]): ["arm-1"], ("q-amd64", names["amd64"]): ["amd-1"]}[(queue, name)]

        with (
            patch("viva_api.simulation.job_scheduler.get_settings", return_value=settings),
            patch.object(batch_build, "find_batch_job_ids_by_name", new=AsyncMock(side_effect=_find)),
            patch.object(
                batch_build,
                "describe_batch_jobs",
                new=AsyncMock(
                    return_value={
                        "arm-1": _batch_state("arm-1", "SUCCEEDED"),
                        "amd-1": _batch_state("amd-1", "SUCCEEDED"),
                    }
                ),
            ) as describe,
        ):
            await scheduler.reconcile_local_tasks()
        describe.assert_awaited_once_with(["arm-1", "amd-1"])
        fresh = await database_service.get_hpcrun(row.database_id)
        assert fresh is not None and fresh.status == JobStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_batch_no_longer_reporting_the_job_fails_the_row_only_after_grace(
        self, database_service: DatabaseServiceSQL
    ) -> None:
        young = await insert_local_build_row(database_service, task_id="deadmis1", external_job_ids=["gone"])
        old = await insert_local_build_row(
            database_service, task_id="deadmis2", external_job_ids=["gone"], age_seconds=LOCAL_ORPHAN_GRACE_SECONDS + 60
        )
        scheduler = _reconciling_scheduler(database_service, LocalTaskService())
        with patch.object(batch_build, "describe_batch_jobs", new=AsyncMock(return_value={})):
            await scheduler.reconcile_local_tasks()
        fresh_young = await database_service.get_hpcrun(young.database_id)
        fresh_old = await database_service.get_hpcrun(old.database_id)
        assert fresh_young is not None and fresh_young.status == JobStatus.RUNNING
        assert fresh_old is not None and fresh_old.status == JobStatus.FAILED
        assert "no longer reports" in (fresh_old.error_message or "")

    @pytest.mark.asyncio
    async def test_chain_dispatch_placeholder_superseded_by_the_real_row_is_completed(
        self, database_service: DatabaseServiceSQL
    ) -> None:
        simulation, _slurm_job, _hpcrun = await insert_job(database_service, slurmjobid=0)
        placeholder = await database_service.insert_hpcrun(
            job_id=JobId.local("deadplc1"),
            job_type=JobType.SIMULATION,
            ref_id=simulation.database_id,
            correlation_id="corr-plc",
        )
        real = await database_service.insert_hpcrun(
            job_id=JobId.ray("parca-1"),
            job_type=JobType.SIMULATION,
            ref_id=simulation.database_id,
            correlation_id="corr-plc",
            chain_n_generations=3,
            chain_final_job_ids=[],
            chain_current_job_ids=[None],
            chain_current_generation=[None],
            chain_parca_done=False,
        )
        scheduler = _reconciling_scheduler(database_service, LocalTaskService())
        await scheduler.reconcile_local_tasks()
        fresh_placeholder = await database_service.get_hpcrun(placeholder.database_id)
        fresh_real = await database_service.get_hpcrun(real.database_id)
        assert fresh_placeholder is not None and fresh_placeholder.status == JobStatus.COMPLETED
        assert fresh_real is not None and fresh_real.status == JobStatus.RUNNING  # never touched

    @pytest.mark.asyncio
    async def test_chain_dispatch_placeholder_with_no_successor_is_failed_only_after_grace(
        self, database_service: DatabaseServiceSQL
    ) -> None:
        simulation, _slurm_job, hpcrun = await insert_job(database_service, slurmjobid=0)
        # insert_job leaves a SLURM row for this simulation; retire it so the
        # LOCAL placeholder is the most recent row, as in the real flow.
        await database_service.delete_hpcrun(hpcrun.database_id)
        placeholder = await database_service.insert_hpcrun(
            job_id=JobId.local("deadplc2"),
            job_type=JobType.SIMULATION,
            ref_id=simulation.database_id,
            correlation_id="corr-plc2",
        )
        scheduler = _reconciling_scheduler(database_service, LocalTaskService())
        await scheduler.reconcile_local_tasks()
        fresh = await database_service.get_hpcrun(placeholder.database_id)
        assert fresh is not None and fresh.status == JobStatus.RUNNING  # young: might be another pod's

        started = datetime.datetime.now() - datetime.timedelta(seconds=LOCAL_ORPHAN_GRACE_SECONDS + 60)
        await database_service.update_hpcrun_status(
            hpcrun_id=placeholder.database_id,
            update=JobStatusUpdate(job_id=placeholder.job_id, status=JobStatus.RUNNING, start_time=started.isoformat()),
        )
        await scheduler.reconcile_local_tasks()
        fresh = await database_service.get_hpcrun(placeholder.database_id)
        assert fresh is not None and fresh.status == JobStatus.FAILED
        assert "re-submit the simulation" in (fresh.error_message or "")

    @pytest.mark.asyncio
    async def test_polling_loop_reconciles_first_on_its_first_tick(self) -> None:
        """Startup reconciliation is the poll loop's first action, not a separate hook."""
        scheduler = JobScheduler(messaging_service=MagicMock(), database_service=MagicMock())
        order: list[str] = []

        async def _reconcile() -> None:
            order.append("reconcile")
            scheduler._stop_event.set()

        async def _record(name: str) -> None:
            order.append(name)

        with (
            patch.object(scheduler, "reconcile_local_tasks", new=_reconcile),
            patch.object(scheduler, "update_running_jobs", new=lambda: _record("running")),
            patch.object(scheduler, "update_chain_campaigns", new=lambda: _record("chain")),
            patch.object(scheduler, "update_multi_node_jobs", new=lambda: _record("mnp")),
        ):
            await scheduler._polling_loop(interval_seconds=0)
        assert order == ["reconcile", "running", "chain", "mnp"]


@pytest.mark.asyncio
async def test_messaging(
    redis_subscriber_service: MessagingServiceRedis,
    redis_producer_service: MessagingServiceRedis,
    database_service: DatabaseServiceSQL,
    slurm_service: SlurmService,
) -> None:
    scheduler = JobScheduler(
        messaging_service=redis_subscriber_service, database_service=database_service, slurm_service=slurm_service
    )
    await scheduler.subscribe()

    # Simulate a job submission and worker event handling
    simulation, slurm_job, hpc_run = await insert_job(database_service=database_service, slurmjobid=1)

    # get the initial state of a job
    sequence_number = 1
    worker_event = WorkerEventMessagePayload(
        sequence_number=sequence_number,
        correlation_id=hpc_run.correlation_id,
        time=0.1,
        mass={"water": 1.0, "glucose": 0.5},
        bulk=None,
    )

    # send worker messages to the broker
    await redis_producer_service.publish(
        subject=get_settings().redis_channel,
        data=worker_event.model_dump_json(exclude_unset=True).encode("utf-8"),
    )
    # get the updated state of the job
    await asyncio.sleep(0.1)
    _updated_worker_events = await database_service.list_worker_events(
        hpcrun_id=hpc_run.database_id, prev_sequence_number=sequence_number - 1
    )
    assert len(_updated_worker_events) == 1


@pytest.mark.integration
@pytest.mark.skipif(not Path(get_settings().slurm_submit_key_path).exists(), reason="slurm ssh key file not supplied")
@pytest.mark.asyncio
async def test_job_scheduler(
    redis_subscriber_service: MessagingServiceRedis,
    database_service: DatabaseServiceSQL,
    slurm_service: SlurmService,
    slurm_template_hello_10s: str,
) -> None:
    scheduler = JobScheduler(
        messaging_service=redis_subscriber_service, database_service=database_service, slurm_service=slurm_service
    )
    await scheduler.subscribe()
    await scheduler.start_polling(interval_seconds=1)

    # Submit a toy slurm job which takes 10 seconds to run
    settings = get_settings()
    remote_path = settings.slurm_log_base_path
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_dir = Path(tmpdir)
        # write slurm_template_hello_1s to a temp file
        local_sbatch_file = tmp_dir / f"job_{uuid.uuid4().hex}.sbatch"
        with open(local_sbatch_file, "w") as f:
            f.write(slurm_template_hello_10s)

        remote_sbatch_file = remote_path / local_sbatch_file.name
        async with get_ssh_session_service(SSHTarget.SLURM).session() as ssh:
            job_id: int = await slurm_service.submit_job(
                ssh, local_sbatch_file=local_sbatch_file, remote_sbatch_file=remote_sbatch_file
            )

    # Simulate job submission
    simulation, slurm_job, hpc_run = await insert_job(database_service=database_service, slurmjobid=job_id)
    assert hpc_run.status == JobStatus.RUNNING

    # Poll until the job receives a RUNNING status (or timeout after 30 seconds)
    max_wait = 30
    start_time = asyncio.get_event_loop().time()
    running_hpcrun: HpcRun | None = None

    while asyncio.get_event_loop().time() - start_time < max_wait:
        await asyncio.sleep(2)
        running_hpcrun = await database_service.get_hpcrun_by_job_id(job_id=JobId.slurm(job_id))
        if running_hpcrun and running_hpcrun.status == JobStatus.RUNNING:
            break

    # Check if the job is in the database with RUNNING status
    assert running_hpcrun is not None
    assert running_hpcrun.status == JobStatus.RUNNING

    # Poll until the job receives a COMPLETED status (or timeout after 30 seconds)
    max_wait_complete = 30
    start_time_complete = asyncio.get_event_loop().time()
    completed_hpcrun: HpcRun | None = None

    while asyncio.get_event_loop().time() - start_time_complete < max_wait_complete:
        await asyncio.sleep(2)
        completed_hpcrun = await database_service.get_hpcrun_by_job_id(job_id=JobId.slurm(job_id))
        if completed_hpcrun and completed_hpcrun.status == JobStatus.COMPLETED:
            break

    # Check if the job is in the database with COMPLETED status
    assert completed_hpcrun is not None
    assert completed_hpcrun.status == JobStatus.COMPLETED

    # Stop polling
    await scheduler.stop_polling()


@pytest.mark.integration
@pytest.mark.skipif(
    is_ci_environment()
    or not Path(get_settings().slurm_submit_key_path).exists()
    or (len(get_settings().storage_s3_bucket) == 0 and len(get_settings().storage_qumulo_endpoint_url) == 0),
    reason="Skipped in CI/CD or missing slurm ssh key or storage backend configuration",
)
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "storage_type",
    [
        pytest.param(
            "aws",
            marks=pytest.mark.skipif(
                is_ci_environment() or len(get_settings().storage_s3_bucket) == 0,
                reason="Skipped in CI/CD or AWS S3 not configured",
            ),
        ),
        pytest.param(
            "qumulo",
            marks=pytest.mark.skipif(
                is_ci_environment() or len(get_settings().storage_qumulo_endpoint_url) == 0,
                reason="Skipped in CI/CD or Qumulo not configured",
            ),
        ),
    ],
)
async def test_job_scheduler_with_storage(
    redis_subscriber_service: MessagingServiceRedis,
    database_service: DatabaseServiceSQL,
    slurm_service: SlurmService,
    slurm_template_with_storage: str,
    ssh_session_service: SSHSessionService,
    storage_type: str,
) -> None:
    """
    Test Slurm job that downloads from S3-compatible storage, processes data, and uploads back.
    This test validates the complete workflow with the same storage provider for both download/upload:
    1. Upload test input file to storage
    2. Submit Slurm job that downloads from storage
    3. Job processes the file
    4. Job uploads result to same storage using s3_upload function
    5. Verify the output file exists in storage
    6. Cleanup both input and output files

    Tests run with both storage_type="aws" and storage_type="qumulo" (if configured).
    """
    settings = get_settings()
    test_id = uuid.uuid4().hex[:8]
    input_key = S3FilePath(s3_path=Path(f"test/slurm/input_{test_id}.txt"))
    output_key = S3FilePath(s3_path=Path(f"test/slurm/output_{test_id}.txt"))

    # Initialize the appropriate storage service based on storage_type
    storage_service: FileService
    if storage_type == "aws":
        storage_service = FileServiceS3()
        print(f"\n=== Using AWS S3 storage: {settings.storage_s3_bucket} ===")
    else:  # qumulo
        storage_service = FileServiceQumuloS3()
        print(
            f"\n=== Using Qumulo storage: {settings.storage_qumulo_bucket} "
            f"at {settings.storage_qumulo_endpoint_url} ==="
        )

    try:
        # Step 1: Upload test input file to storage
        print(f"\n=== Step 1: Uploading test input to {storage_type}: {input_key} ===")
        test_input_content = f"Test input file created at {uuid.uuid4()}\n"
        await storage_service.upload_bytes(file_contents=test_input_content.encode("utf-8"), s3_path=input_key)
        print(f"✅ Test input uploaded to {storage_type}")

        # Step 2: Prepare Slurm job script
        print("\n=== Step 2: Preparing Slurm job ===")
        scheduler = JobScheduler(
            messaging_service=redis_subscriber_service, database_service=database_service, slurm_service=slurm_service
        )
        await scheduler.subscribe()
        await scheduler.start_polling(interval_seconds=2)

        # Upload helper script to remote host
        remote_path = settings.slurm_log_base_path
        helpers_script_path = Path(__file__).parent.parent / "fixtures" / "s3_helpers.sh"

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_dir = Path(tmpdir)

            # Copy helpers script to temp dir and upload to remote
            remote_helpers = remote_path / "s3_helpers.sh"
            async with get_ssh_session_service(SSHTarget.SLURM).session() as ssh:
                await ssh.scp_upload(local_file=helpers_script_path, remote_path=remote_helpers)
            print(f"✅ Uploaded helper script to {remote_helpers}")

            # Prepare the sbatch script with substitutions
            sbatch_content = slurm_template_with_storage
            sbatch_content = sbatch_content.replace("HELPERS_PATH", str(remote_helpers))
            sbatch_content = sbatch_content.replace("INPUT_KEY", str(input_key))
            sbatch_content = sbatch_content.replace("OUTPUT_KEY", str(output_key))

            # Add environment variables for storage access based on storage type
            if storage_type == "aws":
                env_vars = f"""
# Set environment variables for AWS S3 access
export STORAGE_TYPE="aws"
export STORAGE_BUCKET="{settings.storage_s3_bucket}"
export AWS_DEFAULT_REGION="{settings.storage_s3_region}"
export AWS_ACCESS_KEY_ID="{settings.storage_s3_access_key_id}"
export AWS_SECRET_ACCESS_KEY="{settings.storage_s3_secret_access_key}"
export AWS_SESSION_TOKEN="{settings.storage_s3_session_token}"
"""
            else:  # qumulo
                env_vars = f"""
# Set environment variables for Qumulo S3 access
export STORAGE_TYPE="qumulo"
export STORAGE_BUCKET="{settings.storage_qumulo_bucket}"
export STORAGE_ENDPOINT_URL="{settings.storage_qumulo_endpoint_url}"
export STORAGE_VERIFY_SSL="false"
export AWS_DEFAULT_REGION="us-east-1"
export AWS_ACCESS_KEY_ID="{settings.storage_qumulo_access_key_id}"
export AWS_SECRET_ACCESS_KEY="{settings.storage_qumulo_secret_access_key}"
"""
            # Insert env vars after the SBATCH directives but before the actual commands
            sbatch_lines = sbatch_content.split("\n")
            # Find where to insert (after last #SBATCH line)
            insert_idx = 0
            for i, line in enumerate(sbatch_lines):
                if line.strip().startswith("#SBATCH"):
                    insert_idx = i + 1
            sbatch_lines.insert(insert_idx, env_vars)
            sbatch_content = "\n".join(sbatch_lines)

            # Write sbatch script to temp file
            local_sbatch_file = tmp_dir / f"storage_test_{test_id}.sbatch"
            with open(local_sbatch_file, "w") as f:
                f.write(sbatch_content)

            # Submit job
            remote_sbatch_file = remote_path / local_sbatch_file.name
            print(f"✅ Submitting job with script: {remote_sbatch_file}")
            async with get_ssh_session_service(SSHTarget.SLURM).session() as ssh:
                job_id: int = await slurm_service.submit_job(
                    ssh, local_sbatch_file=local_sbatch_file, remote_sbatch_file=remote_sbatch_file
                )
            print(f"✅ Job submitted with ID: {job_id}")

        # Step 3: Insert job into database for tracking
        print("\n=== Step 3: Tracking job in database ===")
        simulation, slurm_job, hpc_run = await insert_job(database_service=database_service, slurmjobid=job_id)
        assert hpc_run.status == JobStatus.RUNNING

        # Step 4: Wait for job to complete
        print("\n=== Step 4: Waiting for job to complete (max 60s) ===")
        max_wait = 60
        check_interval = 5
        elapsed = 0

        while elapsed < max_wait:
            await asyncio.sleep(check_interval)
            elapsed += check_interval

            completed_hpcrun: HpcRun | None = await database_service.get_hpcrun_by_job_id(job_id=JobId.slurm(job_id))
            if completed_hpcrun and completed_hpcrun.status == JobStatus.COMPLETED:
                print(f"✅ Job completed after {elapsed}s")
                break
            print(f"   Waiting... ({elapsed}s / {max_wait}s)")

        # Verify job completed
        final_hpcrun: HpcRun | None = await database_service.get_hpcrun_by_job_id(job_id=JobId.slurm(job_id))
        assert final_hpcrun is not None, "Job not found in database"
        assert final_hpcrun.status == JobStatus.COMPLETED, f"Job failed with status: {final_hpcrun.status}"

        # Step 5: Verify output file exists in storage
        print(f"\n=== Step 5: Verifying output file in {storage_type} ===")
        output_contents = await storage_service.get_file_contents(output_key)
        assert output_contents is not None, f"Output file not found in {storage_type}: {output_key}"

        output_text = output_contents.decode("utf-8")
        print(f"✅ Output file found in {storage_type} ({len(output_contents)} bytes)")
        print(f"Output contents:\n{output_text}")

        # Verify output contains expected data
        assert "Processed at" in output_text, "Output missing timestamp"
        assert test_input_content.strip() in output_text, "Output missing input file contents"
        assert f"Job ID: {job_id}" in output_text, "Output missing job ID"

        print(f"\n✅ All assertions passed for {storage_type} storage!")

    finally:
        # Step 6: Cleanup
        print("\n=== Step 6: Cleaning up test files ===")
        try:
            # Clean up input file
            print(f"Cleaning up {storage_type} input file: {input_key}")
            await storage_service.delete_file(input_key)
            print(f"✅ Deleted {storage_type} file: {input_key}")

            # Clean up output file
            print(f"Cleaning up {storage_type} output file: {output_key}")
            await storage_service.delete_file(output_key)
            print(f"✅ Deleted {storage_type} file: {output_key}")

        except Exception as e:
            print(f"Warning: Cleanup failed: {e}")

        # Close services
        await storage_service.close()
        await scheduler.stop_polling()
