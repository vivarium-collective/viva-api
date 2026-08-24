import asyncio
import os
import random
import string
import tempfile
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from viva_api.common.hpc.job_service import JobStatusInfo, JobStatusUpdate
from viva_api.common.hpc.models import SlurmJob
from viva_api.common.hpc.slurm_service import SlurmService
from viva_api.common.messaging.messaging_service_redis import MessagingServiceRedis
from viva_api.common.models import JobId, JobStatus, SSHTarget
from viva_api.common.ssh.ssh_service import SSHSessionService
from viva_api.common.storage.file_paths import S3FilePath
from viva_api.common.storage.file_service import FileService
from viva_api.common.storage.file_service_qumulo_s3 import FileServiceQumuloS3
from viva_api.common.storage.file_service_s3 import FileServiceS3
from viva_api.config import get_settings
from viva_api.dependencies import get_ssh_session_service
from viva_api.simulation.database_service import DatabaseServiceSQL
from viva_api.simulation.hpc_utils import get_correlation_id
from viva_api.simulation.job_scheduler import JobScheduler
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
) -> tuple[Simulation, HpcRun]:
    """Insert a Simulation + a chain-dispatch-campaign-shaped HpcRun row
    (backlog item 71 Phase 4 — app-level per-seed gating), the fixture shape
    JobScheduler.update_chain_campaigns/_advance_chain_campaign consumes.
    Defaults represent a freshly-submitted campaign (ParCa not yet done,
    every seed slot empty) — the same initial state
    ``SimulationServiceRay.submit_chain_dispatch_job`` itself writes; pass the
    ``chain_*`` kwargs explicitly to represent a campaign already mid-flight.
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


@pytest.mark.integration
@pytest.mark.skipif(not Path(get_settings().slurm_submit_key_path).exists(), reason="slurm ssh key file not supplied")
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
