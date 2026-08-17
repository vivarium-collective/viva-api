"""Tests for the Ray-on-Batch backend: JobId.ray, Batch state mapping, ComputeBackend.RAY,
and SimulationServiceRay submission/status/cancel (boto3 mocked, Postgres via testcontainers)."""

import asyncio
import json
import shlex
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from viva_api.common.hpc.job_service import JobStatusInfo
from viva_api.common.models import JobBackend, JobId, JobStatus
from viva_api.config import ComputeBackend
from viva_api.simulation.models import JobType
from viva_api.simulation.simulation_service_ray import (
    PARCA_CACHE_DIR,
    SIM_OUT_DIR,
    SimulationServiceRay,
    analysis_memory_mib_for,
    analysis_modules_for,
)

if TYPE_CHECKING:
    from viva_api.simulation.database_service import DatabaseServiceSQL
    from viva_api.simulation.models import SimulationRequest


def _ray_settings() -> MagicMock:
    """A settings double with the ray_* / S3 fields SimulationServiceRay reads."""
    return MagicMock(
        batch_region="us-gov-west-1",
        s3_work_bucket="mybucket",
        s3_output_prefix="vecoli-output",
        ray_mnp_queue="smscdk-ray-mnp",
        ray_mnp_job_definition="smscdk-ray-mnp",
        ray_array_queue="smscdk-vecoli-task-amd64",
        ray_array_job_definition="smscdk-ray-array",
        ray_num_nodes=3,
        ray_ecr_repository="v2ecoli",
        ecr_account_id="476270107793",
        ray_parca_mode="fast",
        ray_parca_cpus=8,
        ray_n_steps=600,
        ray_chunk=60,
        ray_log_s3_prefix="s3://mybucket/ray-logs/",
        # build settings (DooD image build)
        build_amd64_queue="smscdk-vecoli-build-amd64",
        build_job_definition="smscdk-vecoli-dind-build",
        build_git_secret_arn="arn:aws-us-gov:secretsmanager:us-gov-west-1:123:secret:vecoli-github-pat",  # noqa: S106  (ARN, not a secret)
        github_token=None,
    )


def _fake_batch(submit_ids: list[str]) -> MagicMock:
    """A boto3 Batch mock that supports the per-commit MNP job-def derivation +
    submits. (The Array job-def branch this used to also support was removed
    along with _ensure_array_job_def/_submit_array/_array_sim_command --
    backlog item 33's canonical-dispatch routing made them dead code, their
    only caller having been the array branch that rework replaced.)

    describe_job_definitions returns the CDK MNP base (with properties to
    clone) for the base name, and "no existing revision" for any per-commit
    name; register returns rev 1.
    """
    b = MagicMock()
    base_node_props = {
        "numNodes": 4,
        "mainNode": 0,
        "nodeRangeProperties": [{"targetNodes": "0:", "container": {"image": "111.dkr.ecr.x/vecoli:ray", "vcpus": 16}}],
    }

    def _describe(**kwargs: Any) -> dict[str, Any]:
        name = kwargs.get("jobDefinitionName")
        if name == "smscdk-ray-mnp":  # MNP base
            return {"jobDefinitions": [{"revision": 7, "nodeProperties": base_node_props}]}
        return {"jobDefinitions": []}  # per-commit: none yet

    b.describe_job_definitions.side_effect = _describe
    b.register_job_definition.side_effect = lambda **kw: {"jobDefinitionName": kw["jobDefinitionName"], "revision": 1}
    b.submit_job.side_effect = [{"jobId": jid} for jid in submit_ids]
    return b


def _overrides(call: Any) -> list[dict[str, Any]]:
    return list(call.kwargs["nodeOverrides"]["nodePropertyOverrides"])


def _env_at(call: Any, index: int) -> dict[str, str]:
    """Env dict for the override at `index` (0 = head/`0:0`, 1 = workers/`1:`)."""
    ov = _overrides(call)[index]
    return {e["name"]: e["value"] for e in ov["containerOverrides"]["environment"]}


def _env_of(call: Any) -> dict[str, str]:
    """Head (node 0) environment dict."""
    return _env_at(call, 0)


class TestJobIdRay:
    def test_ray_factory(self) -> None:
        job_id = JobId.ray("abc-123")
        assert job_id.value == "abc-123"
        assert job_id.backend == JobBackend.RAY

    def test_ray_is_not_slurm_int(self) -> None:
        with pytest.raises(TypeError):
            _ = JobId.ray("abc-123").as_slurm_int


class TestFromBatchState:
    @pytest.mark.parametrize(
        ("batch_state", "expected"),
        [
            ("SUBMITTED", JobStatus.QUEUED),
            ("PENDING", JobStatus.QUEUED),
            ("RUNNABLE", JobStatus.QUEUED),
            ("STARTING", JobStatus.PENDING),
            ("RUNNING", JobStatus.RUNNING),
            ("SUCCEEDED", JobStatus.COMPLETED),
            ("FAILED", JobStatus.FAILED),
            ("running", JobStatus.RUNNING),  # case-insensitive
            ("", JobStatus.UNKNOWN),
            ("BOGUS", JobStatus.UNKNOWN),
        ],
    )
    def test_mapping(self, batch_state: str, expected: JobStatus) -> None:
        assert JobStatus.from_batch_state(batch_state) == expected


class TestComputeBackendRay:
    def test_enum_value(self) -> None:
        assert ComputeBackend("ray") == ComputeBackend.RAY

    def test_get_job_backend(self) -> None:
        from viva_api.config import get_job_backend

        with patch("viva_api.config.get_settings") as mock_settings:
            mock_settings.return_value = MagicMock(compute_backend="ray")
            assert get_job_backend() == ComputeBackend.RAY


@pytest.mark.asyncio
class TestSimulationServiceRaySubmit:
    """submit_ecoli_simulation_job submits ParCa (1 node) + sim (N nodes, dependsOn)."""

    async def test_submit_parca_then_sim_with_dependency(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        # Make the seed count deterministic (SimulationConfig allows extra fields).
        setattr(experiment_request.config, "n_init_sims", 2)  # noqa: B010
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        mock_batch = _fake_batch(["parca-123", "sim-456"])

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            # data_layout builds the S3 URIs (results/cache) and reads config.get_settings directly.
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
        ):
            job_id = await service.submit_ecoli_simulation_job(
                ecoli_simulation=simulation, database_service=database_service, correlation_id="corr-1"
            )

        # The tracked job is the simulation job.
        assert job_id == JobId.ray("sim-456")
        assert mock_batch.submit_job.call_count == 2

        parca_call, sim_call = mock_batch.submit_job.call_args_list
        parca_env, sim_env = _env_of(parca_call), _env_of(sim_call)

        # ParCa: 1 node, parca command, captures the cache to S3, no dependency.
        # A 1-node job has only the head override (no worker `1:` range).
        assert parca_call.kwargs["nodeOverrides"]["numNodes"] == 1
        assert len(_overrides(parca_call)) == 1
        assert "v2ecoli-parca" in parca_env["RAY_JOB_CMD"]
        # ParCa alone only emits raw parca_state.pkl; the sim loads out/cache/initial_state.json,
        # so the parca step must also hydrate the bundle via scripts/build_cache.py (into the
        # cache dir that gets synced to S3). Without this the sim seeds fail with FileNotFound.
        assert "build_cache.py" in parca_env["RAY_JOB_CMD"]
        assert f"--cache {PARCA_CACHE_DIR}" in parca_env["RAY_JOB_CMD"]
        assert parca_env["RAY_OUT_DIR"] == PARCA_CACHE_DIR
        assert "dependsOn" not in parca_call.kwargs

        # Sim: N nodes, ensemble command, gated on the parca job, stages the same cache.
        assert sim_call.kwargs["nodeOverrides"]["numNodes"] == 3
        assert sim_call.kwargs["dependsOn"] == [{"jobId": "parca-123", "type": "SEQUENTIAL"}]
        assert "run_phase0_xarray_ensemble.py" in sim_env["RAY_JOB_CMD"]
        assert "--n-seeds 2" in sim_env["RAY_JOB_CMD"]
        assert "--parallel ray" in sim_env["RAY_JOB_CMD"]
        assert sim_env["RAY_OUT_DIR"] == SIM_OUT_DIR
        assert sim_env["RAY_OUT_S3"] == "s3://mybucket/vecoli-output/" + simulation.config.experiment_id + "/"

        # Cache hand-off: sim stages exactly what parca produced.
        assert sim_env["RAY_STAGE_S3"] == parca_env["RAY_OUT_S3"]
        assert sim_env["RAY_STAGE_DIR"] == PARCA_CACHE_DIR

        # Node env targeting: the CDK base job def declares a SINGLE range ("0:"), so the
        # submit override must target that same range (Batch rejects "0:0"/"1:" splits as
        # "NodeOverride targets should match job definition"). One override on "0:" carries
        # the full env to every node — the staging + output knobs workers need to run seeds
        # and ship their zarr, plus RAY_JOB_CMD/RAY_REPORT_PATH, which workers receive but
        # never act on (the entrypoint branches on AWS_BATCH_JOB_NODE_INDEX; only the head
        # runs the driver).
        sim_overrides = _overrides(sim_call)
        assert len(sim_overrides) == 1
        assert sim_overrides[0]["targetNodes"] == "0:"
        all_node_env = _env_at(sim_call, 0)
        assert all_node_env["RAY_STAGE_S3"] == sim_env["RAY_STAGE_S3"]
        assert all_node_env["RAY_STAGE_DIR"] == PARCA_CACHE_DIR
        assert all_node_env["RAY_OUT_S3"] == sim_env["RAY_OUT_S3"]
        assert all_node_env["RAY_OUT_DIR"] == SIM_OUT_DIR

        # Queue comes from settings; both jobs run the SAME per-commit job-def revision
        # (derived from the base) so they use the simulator's TRUE commit image.
        simulator = await database_service.get_simulator(simulator_id=simulation.simulator_id)
        assert simulator is not None
        commit = simulator.git_commit_hash
        assert sim_call.kwargs["jobQueue"] == "smscdk-ray-mnp"
        assert sim_call.kwargs["jobDefinition"] == f"smscdk-ray-mnp-{commit}:1"
        assert parca_call.kwargs["jobDefinition"] == sim_call.kwargs["jobDefinition"]

        # The per-commit job def was registered cloning the base, with the image swapped
        # to v2ecoli:<commit> on every node range (never vecoli, never :latest).
        reg = mock_batch.register_job_definition.call_args
        assert reg.kwargs["type"] == "multinode"
        reg_images = {nr["container"]["image"] for nr in reg.kwargs["nodeProperties"]["nodeRangeProperties"]}
        assert reg_images == {f"476270107793.dkr.ecr.us-gov-west-1.amazonaws.com/v2ecoli:{commit}"}

    async def test_submit_routes_canonical_batch_baseline_to_chain_dispatch_when_multiseed(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """The canonical batch_baseline sweep (composite is None, generations>1)
        is now delegated ENTIRELY to submit_chain_dispatch_job (backlog item 33
        rework) -- individual per-seed AWS Batch job chains, never the array-job
        path (removed: _submit_array/_array_sim_command/_ensure_array_job_def no
        longer exist at all -- a fresh repo-wide grep confirmed their only
        caller was the array branch this replaced, before they were deleted).

        This is the test that would have caught the real wiring gap found in
        review: submit_chain_dispatch_job existed and was fully tested in
        isolation from the moment it was built, but nothing on the REAL
        submit_ecoli_simulation_job entrypoint ever called it until this
        routing landed -- a real request would have silently kept exercising
        the old array/wave-style path forever."""
        setattr(experiment_request.config, "n_init_sims", 2)  # noqa: B010
        experiment_request.config.generations = 3
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        # parca + 2 seeds x 3 generations = 7 real submissions -- not 1 array job.
        submit_ids = ["parca-1", "s0g0", "s0g1", "s0g2", "s1g0", "s1g1", "s1g2"]
        mock_batch = _fake_batch(submit_ids)
        fake_file_service = AsyncMock()
        fake_file_service.upload_file = AsyncMock()

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
            patch("viva_api.dependencies.get_file_service", return_value=fake_file_service),
            patch("viva_api.simulation.simulation_service_ray.asyncio.sleep", new=AsyncMock()),
        ):
            job_id = await service.submit_ecoli_simulation_job(
                ecoli_simulation=simulation, database_service=database_service, correlation_id="corr-real-entry"
            )

            # The entrypoint now returns a LOCAL task id the instant the campaign
            # is scheduled and issues the real N*G submissions in the background
            # (_submit_chain_dispatch_background) -- so every downstream invariant
            # below is checked AFTER awaiting that task, not synchronously on
            # return. Awaiting the asyncio.Task directly blocks until it is done
            # and re-raises anything it hit, which is exactly what a test wants;
            # it has to happen INSIDE the patch context, since the task is what
            # actually calls boto3.
            assert job_id.backend == JobBackend.LOCAL
            campaign_job_id = await service._local._tasks[job_id.value]

        # Chain-dispatch's own return convention: the ParCa job id, never a
        # single "sim" job id (there isn't one anymore). Same invariant as
        # before; it is now the background task's RESULT rather than
        # submit_ecoli_simulation_job's own return value.
        assert campaign_job_id == JobId.ray("parca-1")
        assert mock_batch.submit_job.call_count == 7
        calls = mock_batch.submit_job.call_args_list

        # Every submission is a plain MNP job -- arrayProperties never appears
        # anywhere in this call sequence.
        for call in calls:
            assert "arrayProperties" not in call.kwargs
        for call in calls[1:]:
            assert "nodeOverrides" in call.kwargs

        # Dependency chain for a representative seed (seed 0) and confirmation
        # seed 1's chain is entirely independent, both rooted at the SAME ParCa job.
        _parca_call, s0g0, s0g1, s0g2, s1g0, s1g1, s1g2 = calls
        assert s0g0.kwargs["dependsOn"] == [{"jobId": "parca-1", "type": "SEQUENTIAL"}]
        assert s0g1.kwargs["dependsOn"] == [{"jobId": "s0g0", "type": "SEQUENTIAL"}]
        assert s0g2.kwargs["dependsOn"] == [{"jobId": "s0g1", "type": "SEQUENTIAL"}]
        assert s1g0.kwargs["dependsOn"] == [{"jobId": "parca-1", "type": "SEQUENTIAL"}]
        assert s1g1.kwargs["dependsOn"] == [{"jobId": "s1g0", "type": "SEQUENTIAL"}]
        assert s1g2.kwargs["dependsOn"] == [{"jobId": "s1g1", "type": "SEQUENTIAL"}]

        # The campaign row was recorded under the CALLER's OWN correlation_id
        # (threaded through, not a fresh internally-generated one) -- this is
        # what makes the idempotent-insert guard in
        # viva_api.common.handlers.simulations fire, so the caller never adds a
        # generic row of its own on top.
        #
        # Resolved via get_hpcrun_by_ref (ORDER BY id DESC) rather than
        # get_hpcrun_id_by_correlation_id: the background dispatch now leaves TWO
        # rows under this one correlation_id -- the synchronous placeholder and,
        # once the task finishes, the real campaign row -- and
        # get_hpcrun_id_by_correlation_id is a bare LIMIT 1 with no ORDER BY, so
        # which of the two it returns is not defined. Most-recent-row-wins is the
        # rule every real status read actually uses, and it is what must resolve
        # to the campaign row.
        assert await database_service.get_hpcrun_id_by_correlation_id(correlation_id="corr-real-entry") is not None
        campaign = await database_service.get_hpcrun_by_ref(ref_id=simulation.database_id, job_type=JobType.SIMULATION)
        assert campaign is not None
        assert campaign.correlation_id == "corr-real-entry"
        assert campaign.job_id == JobId.ray("parca-1")
        assert campaign.chain_n_generations == 3
        assert campaign.chain_final_job_ids == ["s0g2", "s1g2"]

    async def test_submit_routes_canonical_batch_baseline_to_chain_dispatch_when_single_seed(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """Unlike the superseded array-job design (which required n_seeds > 1 --
        AWS Batch's own array-size floor, verified against the real API model),
        a single-seed canonical batch_baseline request is now ALSO routed to
        chain-dispatch: that floor doesn't apply at all to independent per-seed
        MNP jobs, confirmed here at the REAL entrypoint (not just in
        TestChainDispatchSubmission's own isolated coverage of the same claim)."""
        setattr(experiment_request.config, "n_init_sims", 1)  # noqa: B010
        experiment_request.config.generations = 3
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        submit_ids = ["parca-1", "s0g0", "s0g1", "s0g2"]
        mock_batch = _fake_batch(submit_ids)
        fake_file_service = AsyncMock()
        fake_file_service.upload_file = AsyncMock()

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
            patch("viva_api.dependencies.get_file_service", return_value=fake_file_service),
            patch("viva_api.simulation.simulation_service_ray.asyncio.sleep", new=AsyncMock()),
        ):
            job_id = await service.submit_ecoli_simulation_job(
                ecoli_simulation=simulation, database_service=database_service, correlation_id="corr-single-real"
            )
            # Background dispatch (see the multiseed test above for the full
            # rationale): await the spawned task before checking what it did.
            assert job_id.backend == JobBackend.LOCAL
            campaign_job_id = await service._local._tasks[job_id.value]

        assert campaign_job_id == JobId.ray("parca-1")
        assert mock_batch.submit_job.call_count == 4  # parca + 1 seed x 3 generations
        _parca_call, g0, g1, g2 = mock_batch.submit_job.call_args_list
        assert "arrayProperties" not in g0.kwargs
        assert g0.kwargs["dependsOn"] == [{"jobId": "parca-1", "type": "SEQUENTIAL"}]
        assert g1.kwargs["dependsOn"] == [{"jobId": "s0g0", "type": "SEQUENTIAL"}]
        assert g2.kwargs["dependsOn"] == [{"jobId": "s0g1", "type": "SEQUENTIAL"}]

    async def test_canonical_chain_dispatch_returns_before_submitting_anything(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """The entrypoint returns a trackable id BEFORE the campaign submits
        anything -- the property this whole change exists to create, as opposed
        to the two tests above, which re-check the (unchanged) downstream
        submission shape after the background task has run.

        The real bug (vivarium-workbench backlog item 51, found during a live
        1000x10 production dispatch on 2026-08-14): a canonical chain-dispatch
        request issued all n_seeds*n_generations AWS Batch SubmitJob calls
        INLINE, inside the single POST /api/v1/simulations the client was
        awaiting -- about 15 minutes for the real 10,000-job shape. The client's
        HTTP timeout fired long first and reported a FAILED dispatch, while
        viva-api went right on submitting the real, AWS-billed campaign. Retrying
        (the obvious response to being told it failed) would have started a
        second, duplicate, paid campaign on top of the first.

        The background task is held at a deterministic chokepoint -- the
        run_pbg.py staging upload, which submit_chain_dispatch_job awaits after
        its two DB reads and before the pacer and every submit_job call -- so the
        "nothing has happened yet" assertions below are guarantees rather than a
        race this test happens to win.
        """
        setattr(experiment_request.config, "n_init_sims", 2)  # noqa: B010
        experiment_request.config.generations = 3
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        submit_ids = ["parca-1", "s0g0", "s0g1", "s0g2", "s1g0", "s1g1", "s1g2"]
        mock_batch = _fake_batch(submit_ids)

        staging_released = asyncio.Event()

        async def _hold_at_staging(*args: Any, **kwargs: Any) -> None:
            await staging_released.wait()

        fake_file_service = AsyncMock()
        fake_file_service.upload_file = AsyncMock(side_effect=_hold_at_staging)

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
            patch("viva_api.dependencies.get_file_service", return_value=fake_file_service),
            patch("viva_api.simulation.simulation_service_ray.asyncio.sleep", new=AsyncMock()),
        ):
            # The property under test, stated directly: the entrypoint returns
            # promptly regardless of campaign size. asyncio.wait_for, rather than
            # a bare await, so that the pre-fix inline behaviour FAILS here in
            # seconds instead of deadlocking the suite -- submitting inline, it
            # would park in the submission loop at the staging chokepoint below
            # and never come back, since only this test body releases it.
            job_id = await asyncio.wait_for(
                service.submit_ecoli_simulation_job(
                    ecoli_simulation=simulation,
                    database_service=database_service,
                    correlation_id="corr-fast-return",
                ),
                timeout=10,
            )

            # 1. Returned having submitted NOTHING -- not "fewer calls", zero.
            assert mock_batch.submit_job.call_count == 0
            # 2. ...and what came back is a trackable LOCAL task id, which
            # get_job_status (and cancel_job) already resolve through the shared
            # LocalTaskService -- the same mechanism submit_build_image_job uses
            # for the other multi-minute operation this service owns.
            assert job_id.backend == JobBackend.LOCAL
            in_flight = await service.get_job_status(job_id)
            assert in_flight is not None
            assert in_flight.status == JobStatus.RUNNING

            # 3. A placeholder HpcRun row is already committed, so a status poll
            # arriving a millisecond later has something real to read -- with
            # BOTH chain fields left unset.
            placeholder = await database_service.get_hpcrun_by_ref(
                ref_id=simulation.database_id, job_type=JobType.SIMULATION
            )
            assert placeholder is not None
            assert placeholder.job_id == job_id
            assert placeholder.correlation_id == "corr-fast-return"
            assert placeholder.chain_n_generations is None
            assert placeholder.chain_final_job_ids is None

            # 4. ...and precisely BECAUSE chain_n_generations is unset, the
            # scheduler's poll set excludes the placeholder. Were it enrolled,
            # get_chain_campaign_result([]) would call the campaign trivially
            # terminal with zero successes and _advance_chain_campaign would mark
            # it FAILED on the very next tick -- recreating the same false
            # failure, this time inside viva-api itself.
            assert [
                c.database_id
                for c in await database_service.list_active_chain_campaigns()
                if c.ref_id == simulation.database_id
            ] == []

            # Release the chokepoint and let the campaign finish submitting.
            staging_released.set()
            campaign_job_id = await service._local._tasks[job_id.value]

        # 5. The real campaign row now supersedes the placeholder for every later
        # read (get_hpcrun_by_ref is ORDER BY id DESC) and IS in the scheduler's
        # poll set -- so analysis auto-fire stays wired exactly as before.
        assert campaign_job_id == JobId.ray("parca-1")
        assert mock_batch.submit_job.call_count == 7
        campaign = await database_service.get_hpcrun_by_ref(ref_id=simulation.database_id, job_type=JobType.SIMULATION)
        assert campaign is not None
        assert campaign.database_id != placeholder.database_id
        assert campaign.job_id == JobId.ray("parca-1")
        assert campaign.chain_n_generations == 3
        assert campaign.chain_final_job_ids == ["s0g2", "s1g2"]
        assert [
            c.database_id
            for c in await database_service.list_active_chain_campaigns()
            if c.ref_id == simulation.database_id
        ] == [campaign.database_id]

        # The completed background task reports COMPLETED, so the plain status
        # path stops saying RUNNING once submission is genuinely done.
        done = await service.get_job_status(job_id)
        assert done is not None
        assert done.status == JobStatus.COMPLETED

    async def test_composite_comparison_ensemble_with_multiple_generations_stays_on_mnp(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """Chain-dispatch is v2ecoli-only (backlog item 33). A composite-driven
        two-engine comparison-ensemble request must NOT be routed there even
        with generations > 1 -- guards against the real regression risk this
        rework's new routing guard could introduce (an over-broad condition
        that also swallows composite requests)."""
        setattr(experiment_request.config, "composite", "vecoli")  # noqa: B010
        experiment_request.config.generations = 3
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        mock_batch = _fake_batch(["parca-123", "sim-456"])
        fake_file_service = AsyncMock()
        fake_file_service.upload_file = AsyncMock()

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
            patch("viva_api.dependencies.get_file_service", return_value=fake_file_service),
        ):
            job_id = await service.submit_ecoli_simulation_job(
                ecoli_simulation=simulation, database_service=database_service, correlation_id="corr-composite"
            )

        # A single MNP sim job, gated on parca -- not chain-dispatch's N*G shape.
        assert job_id == JobId.ray("sim-456")
        assert mock_batch.submit_job.call_count == 2
        parca_call, sim_call = mock_batch.submit_job.call_args_list
        assert "nodeOverrides" in sim_call.kwargs
        assert "arrayProperties" not in sim_call.kwargs
        assert sim_call.kwargs["dependsOn"] == [{"jobId": "parca-123", "type": "SEQUENTIAL"}]
        assert "run_comparison_ensemble.py" in _env_of(sim_call)["RAY_JOB_CMD"]
        assert "--composite vecoli" in _env_of(sim_call)["RAY_JOB_CMD"]
        assert parca_call.kwargs["nodeOverrides"]["numNodes"] == 1


class TestAnalysisModulesFor:
    """analysis_modules_for reads the simulation's OWN configured analyses."""

    def test_real_scale_entries_are_forwarded_verbatim(self) -> None:
        from viva_api.simulation.models import AnalysisOptions, SimulationConfig

        config = SimulationConfig(
            experiment_id="exp-1",
            analysis_options=AnalysisOptions.model_validate({
                "multiseed": {"cd1_metabolomics": {"generation_lower_bound": 5}},
                "cpus": 4,
            }),
        )
        # cpus is a real AnalysisOptions field, NOT a scale — forwarding it as one
        # would ask the model image to run an analysis called "cpus".
        assert analysis_modules_for(config) == {"multiseed": {"cd1_metabolomics": {"generation_lower_bound": 5}}}

    def test_unset_or_empty_options_fall_back_to_the_applicable_keyword(self) -> None:
        """The run endpoint's own no-analysis-options default is `{"multiseed": {}}`
        — "no modules named", not "run nothing". Both it and a bare default must
        resolve to `applicable`, which the model image expands with its own
        registry (sms-api has none)."""
        from viva_api.simulation.models import AnalysisOptions, SimulationConfig

        assert analysis_modules_for(SimulationConfig(experiment_id="exp-1")) == "applicable"
        empty = SimulationConfig(
            experiment_id="exp-1", analysis_options=AnalysisOptions.model_validate({"multiseed": {}})
        )
        assert analysis_modules_for(empty) == "applicable"


class TestAnalysisCommand:
    """_analysis_command builds the analysis DAG node's workload."""

    def _cmd(self, **kw: Any) -> str:
        service = SimulationServiceRay()
        defaults: dict[str, Any] = {
            "experiment_id": "sim47-real-experiment",
            "n_seeds": 4,
            "n_generations": 3,
            "modules": "applicable",
            "analysis_name": "analysis-sim47-abc123",
            "commit": "deadbeef",
        }
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
        ):
            return service._analysis_command(**{**defaults, **kw})

    def test_runs_the_model_images_own_s3_native_analysis_entrypoint(self) -> None:
        cmd = self._cmd()
        assert "python scripts/run_standalone_analysis.py" in cmd
        # The sweep prefix is the SAME one the sim job syncs its output to, with no
        # trailing slash (run_standalone_analysis rstrips it to build result_uri).
        assert "--out-uri s3://mybucket/vecoli-output/sim47-real-experiment " in cmd + " "
        assert "--n-seeds 4" in cmd
        assert "--n-generations 3" in cmd
        assert "--analysis-name analysis-sim47-abc123" in cmd

    def test_points_sim_data_at_the_commits_parca_cache(self) -> None:
        """An s3:// sweep has no co-located sim_data pickle to glob, so the DuckDB
        analyses would raise FileNotFoundError without this pointer. Both this job
        and the ParCa job derive the URI from the commit — no hand-off plumbing."""
        cmd = self._cmd(commit="c0ffee")
        assert "V2ECOLI_SIM_DATA=s3://mybucket/ray-parca-cache/c0ffee/simData.cPickle" in cmd

    def test_explicit_modules_ride_as_json_and_survive_a_hostile_experiment_id(self) -> None:
        """experiment_id is a caller-supplied, unconstrained string, and the modules
        blob is JSON — both must reach the container as DATA, never shell syntax."""
        hostile = "exp'; touch /tmp/analysis-command-canary; echo '$(echo pwned)"
        modules = {"multiseed": {"cd1_metabolomics": {"generation_lower_bound": 5}}}
        cmd = self._cmd(experiment_id=hostile, modules=modules)
        tokens = shlex.split(cmd.split("&&", 1)[1].replace("V2ECOLI_SIM_DATA=", "", 1))
        assert json.loads(tokens[tokens.index("--modules") + 1]) == modules
        assert tokens[tokens.index("--out-uri") + 1].endswith(hostile)
        assert "touch /tmp/analysis-command-canary" not in shlex.split(cmd)

    def test_n_generations_is_emitted_only_for_the_applicable_keyword(self) -> None:
        """--n-generations exists solely to resolve `applicable`, and is the ONE
        flag a simulator image built before that keyword landed would reject
        (argparse: unrecognized argument). Emitting it only in the keyword case
        keeps an explicit module mapping runnable against ANY image that already
        ships the script, so a pre-existing build still gets its configured
        analyses instead of failing the whole node."""
        assert "--n-generations" in self._cmd(modules="applicable")
        assert "--n-generations" not in self._cmd(modules={"multiseed": {"cd1_fluxomics": {}}})


@pytest.mark.asyncio
class TestAnalysisDagNode:
    """Item 24: the analysis must fire from the pipeline DAG itself, with no
    separate manual step and no external watcher.

    Originally this covered the canonical batch_baseline shape too (composite
    is None, multiseed, multigenerational), reached through
    submit_ecoli_simulation_job's own inline array-path analysis submission.
    Backlog item 33 moved that shape's analysis trigger entirely to
    submit_campaign_analysis (fired by the poller once every seed's chain is
    terminal, not inline at submission time) -- submit_ecoli_simulation_job no
    longer submits an analysis job for ANY shape it can still reach (the
    comparison-ensemble and phase0 paths never did either). That coverage
    moved to TestSubmitCampaignAnalysis, retargeted at the real mechanism;
    only the single-generation "no analysis at all" guard still belongs here,
    since it's still a submit_ecoli_simulation_job-level property."""

    async def test_no_analysis_node_for_the_single_generation_ensemble(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """The phase0 single-generation ensemble writes no hive-parquet sweep, so
        there is nothing for the ported analyses to read — it must stay a 2-job DAG
        rather than burn a node on a guaranteed FileNotFoundError."""
        setattr(experiment_request.config, "n_init_sims", 2)  # noqa: B010
        experiment_request.config.generations = 1
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        mock_batch = _fake_batch(["parca-123", "sim-456"])
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
        ):
            await service.submit_ecoli_simulation_job(
                ecoli_simulation=simulation, database_service=database_service, correlation_id="corr-phase0"
            )

        assert mock_batch.submit_job.call_count == 2
        assert await database_service.list_analyses(simulation_id=simulation.database_id) == []


@pytest.mark.asyncio
class TestSimulationServiceRayStatusCancel:
    async def test_get_job_status_running(self) -> None:
        mock_batch = MagicMock()
        mock_batch.describe_jobs.return_value = {"jobs": [{"jobId": "sim-456", "status": "RUNNING", "startedAt": 111}]}
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
        ):
            info = await service.get_job_status(JobId.ray("sim-456"))
        assert info is not None
        assert info.status == JobStatus.RUNNING
        assert info.job_id == JobId.ray("sim-456")

    async def test_get_job_status_not_found(self) -> None:
        mock_batch = MagicMock()
        mock_batch.describe_jobs.return_value = {"jobs": []}
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
        ):
            assert await service.get_job_status(JobId.ray("missing")) is None

    async def test_get_job_status_local_dispatches_to_local(self) -> None:
        local = MagicMock()
        local.get_status.return_value = JobStatusInfo(job_id=JobId.local("t"), status=JobStatus.COMPLETED)
        service = SimulationServiceRay(local_task_service=local)
        info = await service.get_job_status(JobId.local("t"))
        assert info is not None and info.status == JobStatus.COMPLETED
        local.get_status.assert_called_once_with("t")

    async def test_get_job_status_mnp_oom_exit_code_from_attempts(self) -> None:
        """Multi-node-parallel jobs (every Ray job this backend submits) never
        populate the top-level `container` -- only `attempts[]` does, confirmed
        against a real failed item 38 MNP job's describe-jobs response (top-level
        container was None, attempts[-1].container held exitCode=137 + the OOM
        reason). This is the exit_code path item 50 Gap 6's OOM-retry-escalation
        (folded in from viva-api PR #239) depends on."""
        mock_batch = MagicMock()
        mock_batch.describe_jobs.return_value = {
            "jobs": [
                {
                    "jobId": "analysis-oom",
                    "status": "FAILED",
                    "statusReason": "Essential container in task exited",
                    "container": None,
                    "attempts": [
                        {
                            "container": {
                                "exitCode": 137,
                                "reason": "OutOfMemoryError: Container killed due to memory usage",
                            }
                        }
                    ],
                }
            ]
        }
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
        ):
            info = await service.get_job_status(JobId.ray("analysis-oom"))
        assert info is not None
        assert info.status == JobStatus.FAILED
        assert info.exit_code == "137"
        assert info.error_message == "OutOfMemoryError: Container killed due to memory usage"

    async def test_get_job_status_top_level_container_takes_precedence(self) -> None:
        """attempts[] is only a fallback for the MNP shape where the top-level
        field is empty -- a populated top-level container must win."""
        mock_batch = MagicMock()
        mock_batch.describe_jobs.return_value = {
            "jobs": [
                {
                    "jobId": "single-container",
                    "status": "FAILED",
                    "container": {"exitCode": 1, "reason": "top-level reason"},
                    "attempts": [{"container": {"exitCode": 137, "reason": "stale attempt reason"}}],
                }
            ]
        }
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
        ):
            info = await service.get_job_status(JobId.ray("single-container"))
        assert info is not None
        assert info.exit_code == "1"
        assert info.error_message == "top-level reason"

    async def test_cancel_terminates_batch_job(self) -> None:
        mock_batch = MagicMock()
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
        ):
            await service.cancel_job(JobId.ray("sim-456"))
        mock_batch.terminate_job.assert_called_once()
        assert mock_batch.terminate_job.call_args.kwargs["jobId"] == "sim-456"


class TestGetChainCampaignResult:
    """get_chain_campaign_result polls a chain-dispatch campaign's tracked
    final-generation job ids (one per seed, real independent AWS Batch jobs)
    for the analysis-fan-in condition — replacing the per-generation-array
    design's own single-array-job TestGetWaveResult. No arrayProperties/
    list_jobs involved at all: each tracked id is a genuinely independent
    job, so this is a plain per-id describe_jobs status lookup, chunked at
    AWS's real 100-id-per-call cap."""

    def test_empty_job_ids_is_trivially_terminal_with_no_aws_call(self) -> None:
        mock_batch = MagicMock()
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
        ):
            result = service.get_chain_campaign_result([])
        assert result.terminal is True
        assert result.succeeded_job_ids == []
        assert result.failed_job_ids == []
        mock_batch.describe_jobs.assert_not_called()

    def test_not_terminal_while_any_tracked_job_still_running(self) -> None:
        mock_batch = MagicMock()
        mock_batch.describe_jobs.return_value = {
            "jobs": [
                {"jobId": "seed0-final", "status": "SUCCEEDED"},
                {"jobId": "seed1-final", "status": "RUNNING"},
            ]
        }
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
        ):
            result = service.get_chain_campaign_result(["seed0-final", "seed1-final"])
        assert result.terminal is False
        assert result.succeeded_job_ids == []
        assert result.failed_job_ids == []

    def test_terminal_all_succeeded(self) -> None:
        mock_batch = MagicMock()
        mock_batch.describe_jobs.return_value = {
            "jobs": [
                {"jobId": "seed0-final", "status": "SUCCEEDED"},
                {"jobId": "seed1-final", "status": "SUCCEEDED"},
            ]
        }
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
        ):
            result = service.get_chain_campaign_result(["seed0-final", "seed1-final"])
        assert result.terminal is True
        assert result.succeeded_job_ids == ["seed0-final", "seed1-final"]
        assert result.failed_job_ids == []

    def test_terminal_with_mixed_success_and_failure(self) -> None:
        """A permanently-failed seed's own final job showing FAILED is
        expected economics, not an orchestrator error -- mirrors the
        superseded design's own 'partial failure is still terminal'
        philosophy, now evaluated per-seed instead of per-generation-wave."""
        mock_batch = MagicMock()
        mock_batch.describe_jobs.return_value = {
            "jobs": [
                {"jobId": "seed0-final", "status": "SUCCEEDED"},
                {"jobId": "seed1-final", "status": "FAILED"},
                {"jobId": "seed2-final", "status": "SUCCEEDED"},
            ]
        }
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
        ):
            result = service.get_chain_campaign_result(["seed0-final", "seed1-final", "seed2-final"])
        assert result.terminal is True
        assert result.succeeded_job_ids == ["seed0-final", "seed2-final"]
        assert result.failed_job_ids == ["seed1-final"]

    def test_all_failed_is_still_terminal(self) -> None:
        mock_batch = MagicMock()
        mock_batch.describe_jobs.return_value = {
            "jobs": [{"jobId": f"seed{i}-final", "status": "FAILED"} for i in range(3)]
        }
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
        ):
            result = service.get_chain_campaign_result([f"seed{i}-final" for i in range(3)])
        assert result.terminal is True
        assert result.succeeded_job_ids == []
        assert result.failed_job_ids == [f"seed{i}-final" for i in range(3)]

    def test_job_id_missing_from_response_is_treated_as_not_terminal(self) -> None:
        """Brief eventual-consistency lag right after submission -- a tracked
        id describe_jobs doesn't (yet) return anything for must NOT be
        mistaken for terminal; the poller just checks again next interval."""
        mock_batch = MagicMock()
        mock_batch.describe_jobs.return_value = {"jobs": [{"jobId": "seed0-final", "status": "SUCCEEDED"}]}
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
        ):
            result = service.get_chain_campaign_result(["seed0-final", "seed1-final-not-visible-yet"])
        assert result.terminal is False

    def test_chunks_describe_jobs_calls_at_the_real_100_id_cap(self) -> None:
        """AWS Batch DescribeJobs accepts at most 100 job ids per call
        (verified against the real API model this session) -- a 1000-seed
        campaign's tracked ids must be split into 10 calls of <=100, never
        one call of 1000."""
        job_ids = [f"seed{i}-final" for i in range(1000)]

        def _describe_jobs(**kwargs: Any) -> dict[str, Any]:
            assert len(kwargs["jobs"]) <= 100
            return {"jobs": [{"jobId": jid, "status": "SUCCEEDED"} for jid in kwargs["jobs"]]}

        mock_batch = MagicMock()
        mock_batch.describe_jobs.side_effect = _describe_jobs
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
        ):
            result = service.get_chain_campaign_result(job_ids)
        assert result.terminal is True
        assert len(result.succeeded_job_ids) == 1000
        assert mock_batch.describe_jobs.call_count == 10


def _v2ecoli_simulator() -> Any:
    from viva_api.simulation.models import SimulatorVersion

    return SimulatorVersion(
        database_id=1,
        git_commit_hash="abc1234",
        git_repo_url="https://github.com/vivarium-collective/v2Ecoli",
        git_branch="main",
    )


class TestSimulationServiceRayBuild:
    """submit_build_image_job builds the workload-owned v2ecoli image via a DooD Batch job."""

    def test_build_command_clones_v2ecoli_and_runs_its_recipe(self) -> None:
        service = SimulationServiceRay()
        with patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings):
            cmd = service._build_command(_v2ecoli_simulator())
        assert cmd[0] == "sh" and cmd[1] == "-c"
        script = cmd[2]
        assert "git clone --branch main --single-branch" in script
        assert "v2Ecoli" in script  # the workload repo, not vEcoli
        assert "git checkout abc1234" in script
        # runs v2ecoli's OWN recipe (symmetric with K8s running vEcoli's), not an sms-cdk script
        assert "docker/build-and-push-ecr.sh -i abc1234 -r v2ecoli -R us-gov-west-1" in script

    def test_sim_command_composite_defaults_to_single_generation(self) -> None:
        """Selecting an engine must NOT imply the 16-gen comparison default."""
        service = SimulationServiceRay()
        with patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings):
            cmd = service._sim_command(n_seeds=1, n_steps=10, chunk=4, composite="v2ecoli")
        assert "run_comparison_ensemble.py" in cmd
        assert "--max-generations 1" in cmd
        assert "--max-generations 16" not in cmd

    def test_sim_command_composite_honors_explicit_generations(self) -> None:
        service = SimulationServiceRay()
        with patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings):
            cmd = service._sim_command(n_seeds=1, n_steps=10, chunk=4, composite="v2ecoli", max_generations=5)
        assert "--max-generations 5" in cmd

    def test_sim_command_defaults_to_single_generation_phase0(self) -> None:
        """No composite, no generations requested: unchanged, verified-working
        single-generation dispatch -- must not regress by default."""
        service = SimulationServiceRay()
        with patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings):
            cmd = service._sim_command(n_seeds=2, n_steps=600, chunk=60)
        assert "run_phase0_xarray_ensemble.py" in cmd
        assert "run_batch_baseline_ray.py" not in cmd

    def test_sim_command_routes_to_batch_baseline_when_multi_generation_requested(self) -> None:
        """config.generations > 1 must route to the real multi-generation
        LineageProcess/batch_baseline_runner pipeline, dispatched as a registered
        process-bigraph composite through the generic run_pbg.py runner -- not a
        v2ecoli-specific CLI script (backlog items 26/27), and not the
        single-generation script that silently ignores generation count."""
        service = SimulationServiceRay()
        with patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings):
            cmd = service._sim_command(
                n_seeds=2,
                n_steps=600,
                chunk=60,
                n_generations=3,
                experiment_id="sim47-real-experiment",
                runner_s3_uri="s3://mybucket/vecoli-output/sim47-real-experiment/run_pbg.py",
            )
        assert "run_batch_baseline_ray.py" not in cmd
        assert "run_phase0_xarray_ensemble.py" not in cmd
        assert "aws s3 cp s3://mybucket/vecoli-output/sim47-real-experiment/run_pbg.py /tmp/run_pbg.py" in cmd
        assert "python /tmp/run_pbg.py" in cmd
        # Exact match, not a substring check. Verified directly against the deployed
        # sms-ecoli image (commit c44b69a, build 63) -- v2ecoli #373 folded the old
        # standalone batch_baseline composite into ecoli_baseline.py's baseline()
        # (backlog item 55; the old id now fails loudly instead of silently drifting,
        # by the composite registry's own deliberate design -- see the constant's
        # own comment in simulation_service_ray.py for the full incident history).
        assert "--composite-id v2ecoli.composites.ecoli_baseline.ecoli_baseline " in cmd
        assert "PBG_CORE_BUILDER=v2ecoli.core:build_core" in cmd
        assert "-n 1" in cmd

        # The overrides are a real, single-quoted JSON blob -- unpack it via shlex to
        # assert on structured content rather than substring-matching a hand-escaped string.
        tokens = shlex.split(cmd)
        overrides = json.loads(tokens[tokens.index("--overrides") + 1])
        assert overrides == {
            "n_seeds": 2,
            "n_generations": 3,
            "cache_dir": PARCA_CACHE_DIR,
            "out_dir": SIM_OUT_DIR,
            "experiment_id": "sim47-real-experiment",
            "analyses": "none",
            "parallel": "ray",
        }

    def test_sim_command_multi_generation_requires_experiment_id_and_runner_uri(self) -> None:
        """No silent placeholder default -- both must be supplied explicitly or the
        dispatch fails loudly instead of running against the wrong experiment_id."""
        service = SimulationServiceRay()
        with patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings):
            with pytest.raises(RuntimeError, match="experiment_id"):
                service._sim_command(n_seeds=2, n_steps=600, chunk=60, n_generations=3, runner_s3_uri="s3://x/y.py")
            with pytest.raises(RuntimeError, match="runner_s3_uri"):
                service._sim_command(n_seeds=2, n_steps=600, chunk=60, n_generations=3, experiment_id="exp-1")

    def test_sim_command_composite_takes_precedence_over_n_generations(self) -> None:
        """The comparison driver's own --max-generations flag is a separate knob
        from plain n_generations -- composite selection wins regardless."""
        service = SimulationServiceRay()
        with patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings):
            cmd = service._sim_command(n_seeds=1, n_steps=10, chunk=4, composite="v2ecoli", n_generations=3)
        assert "run_comparison_ensemble.py" in cmd
        assert "run_batch_baseline_ray.py" not in cmd

    def test_sim_command_vecoli_source_only_appended_for_upstream_vecoli(self) -> None:
        """--vecoli-source is meaningful only for --composite vecoli."""
        service = SimulationServiceRay()
        with patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings):
            vecoli = service._sim_command(
                n_seeds=1, n_steps=10, chunk=4, composite="vecoli", vecoli_source="vivarium-process"
            )
            v2ecoli = service._sim_command(
                n_seeds=1, n_steps=10, chunk=4, composite="v2ecoli", vecoli_source="vivarium-process"
            )
        assert "--vecoli-source vivarium-process" in vecoli
        # v2ecoli engine ignores vecoli_source (guarded by _is_upstream_vecoli)
        assert "--vecoli-source" not in v2ecoli


class TestSeedGenerationCommand:
    """_seed_generation_command builds ONE seed's ONE generation's command —
    replacing the per-generation-array design's own _wave_sim_command. Unlike
    that design, the WHOLE --overrides payload (seed, generation, carry-state
    paths) is fully static and known Python-side at SUBMISSION time -- no
    AWS_BATCH_JOB_ARRAY_INDEX, no lookup table, no container-start shell/
    python3 merge step at all, so (mirroring TestAnalysisCommand's own
    established pattern for another fully-static command) these tests parse
    the embedded JSON via shlex.split rather than executing anything."""

    @staticmethod
    def _overrides(cmd: str) -> dict[str, Any]:
        tokens = shlex.split(cmd)
        return dict(json.loads(tokens[tokens.index("--overrides") + 1]))

    def test_shape_generation_zero_has_no_carry_state(self) -> None:
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
        ):
            cmd = service._seed_generation_command(
                seed=7,
                generation_index=0,
                experiment_id="sim47-chain-experiment",
                runner_s3_uri="s3://mybucket/vecoli-output/sim47-chain-experiment/run_pbg.py",
            )
        assert "aws s3 cp s3://mybucket/vecoli-output/sim47-chain-experiment/run_pbg.py /tmp/run_pbg.py" in cmd
        assert "--composite-id v2ecoli.composites.ecoli_baseline.ecoli_baseline " in cmd
        assert "PBG_CORE_BUILDER=v2ecoli.core:build_core" in cmd
        assert "-n 1" in cmd
        # No array-index resolution left anywhere -- the whole point of the rework.
        assert "AWS_BATCH_JOB_ARRAY_INDEX" not in cmd
        assert "python3 -c" not in cmd

        overrides = self._overrides(cmd)
        # ecoli_baseline.baseline()'s own param is `seed`, not `base_seed` (backlog
        # item 55) -- a real regression: passing base_seed here is an unexpected-
        # kwarg TypeError against the composite actually registered in the deployed
        # image, exactly the failure a real dispatch (sim 152) hit on 2026-08-16.
        assert overrides["seed"] == 7
        assert "base_seed" not in overrides
        assert overrides["initial_generation_index"] == 0
        assert overrides["initial_carry_state_path"] == ""
        assert overrides["daughter_state_out_path"] == (
            "s3://mybucket/vecoli-output/sim47-chain-experiment/daughter-state/seed7/gen0.pkl"
        )
        assert overrides["n_seeds"] == 1
        assert overrides["n_generations"] == 1
        assert overrides["analyses"] == "none"
        # Per-seed S3 prefix, not the flat ensemble one (backlog item 35: every
        # job sharing the flat prefix clobbered the last job's summary.json/
        # final_state.json -- the real bug the pilot found).
        assert overrides["out_dir"] == "s3://mybucket/vecoli-output/sim47-chain-experiment/seed_07"

    def test_later_generation_carries_the_prior_generations_state(self) -> None:
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
        ):
            cmd = service._seed_generation_command(
                seed=42,
                generation_index=3,
                experiment_id="exp-b",
                runner_s3_uri="s3://mybucket/vecoli-output/exp-b/run_pbg.py",
            )
        overrides = self._overrides(cmd)
        assert overrides["seed"] == 42
        assert overrides["initial_generation_index"] == 3
        assert overrides["initial_carry_state_path"] == (
            "s3://mybucket/vecoli-output/exp-b/daughter-state/seed42/gen2.pkl"
        )
        assert overrides["daughter_state_out_path"] == (
            "s3://mybucket/vecoli-output/exp-b/daughter-state/seed42/gen3.pkl"
        )

    def test_hostile_experiment_id_stays_data_not_shell_syntax(self) -> None:
        """experiment_id is a caller-supplied, unconstrained string (no
        pattern validation at the model/API boundary) -- proves the
        shlex-quoted blob keeps arbitrary content as DATA, mirroring
        TestAnalysisCommand's own injection-canary proof for another
        fully-static command."""
        service = SimulationServiceRay()
        hostile = "exp'; touch /tmp/seed-gen-command-injection-canary; echo '$(echo pwned)"
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
        ):
            cmd = service._seed_generation_command(
                seed=3,
                generation_index=1,
                experiment_id=hostile,
                runner_s3_uri="s3://mybucket/vecoli-output/exp/run_pbg.py",
            )
        assert "touch /tmp/seed-gen-command-injection-canary" not in shlex.split(cmd)
        overrides = self._overrides(cmd)
        assert overrides["experiment_id"] == hostile
        assert hostile in overrides["daughter_state_out_path"]

    def test_out_dir_is_shared_within_a_seed_but_isolated_across_seeds(self) -> None:
        """The real regression test for the item 35 pilot bug: every
        per-generation job for the SAME seed must share one S3 prefix (so the
        parquet sweep / zarr store / summary.json accumulate correctly across
        the chain), but DIFFERENT seeds must never share a prefix (or their
        jobs clobber each other's summary.json/final_state.json exactly as
        the 4th pilot fire found -- confirmed via direct S3 reads, not
        assumed)."""
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
        ):
            seed0_gen0 = self._overrides(
                service._seed_generation_command(
                    seed=0,
                    generation_index=0,
                    experiment_id="exp-c",
                    runner_s3_uri="s3://mybucket/vecoli-output/exp-c/run_pbg.py",
                )
            )
            seed0_gen5 = self._overrides(
                service._seed_generation_command(
                    seed=0,
                    generation_index=5,
                    experiment_id="exp-c",
                    runner_s3_uri="s3://mybucket/vecoli-output/exp-c/run_pbg.py",
                )
            )
            seed1_gen0 = self._overrides(
                service._seed_generation_command(
                    seed=1,
                    generation_index=0,
                    experiment_id="exp-c",
                    runner_s3_uri="s3://mybucket/vecoli-output/exp-c/run_pbg.py",
                )
            )

        assert seed0_gen0["out_dir"] == seed0_gen5["out_dir"] == ("s3://mybucket/vecoli-output/exp-c/seed_00")
        assert seed1_gen0["out_dir"] == "s3://mybucket/vecoli-output/exp-c/seed_01"
        assert seed0_gen0["out_dir"] != seed1_gen0["out_dir"]

    def test_stays_comfortably_under_the_batch_command_size_cap(self) -> None:
        """AWS Batch caps a container override command at 8192 bytes (see
        _stage_runner's docstring). Even simpler than the design this
        superseded (no seed_indices array embedded at all -- a standalone job
        only ever needs its OWN seed), so this stays well under the cap
        regardless of experiment_id length."""
        service = SimulationServiceRay()
        long_experiment_id = "sim1000-cd1-baseline-1000x10-" + "x" * 20
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
        ):
            cmd = service._seed_generation_command(
                seed=999,
                generation_index=9,
                experiment_id=long_experiment_id,
                runner_s3_uri=f"s3://mybucket/vecoli-output/{long_experiment_id}/run_pbg.py",
            )
        assert len(cmd) < 8192, f"seed-generation command is {len(cmd)} bytes, over the real AWS Batch cap"


class TestIsUpstreamVecoli:
    """The single routing predicate shared by submit_ecoli_simulation_job and _sim_command."""

    def test_only_vecoli_is_upstream(self) -> None:
        from viva_api.simulation.simulation_service_ray import _is_upstream_vecoli

        assert _is_upstream_vecoli("vecoli") is True
        assert _is_upstream_vecoli("v2ecoli") is False
        assert _is_upstream_vecoli(None) is False


class TestSimulationServiceRayBuildSubmit:
    """Build-image submission: DooD Batch job to the amd64 queue, then poll."""

    @pytest.mark.asyncio
    async def test_run_build_submits_to_amd64_queue_and_polls(self) -> None:
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch(
                "viva_api.simulation.simulation_service_ray.batch_build.submit_batch_build",
                new=AsyncMock(return_value="build-job-1"),
            ) as mock_submit,
            patch(
                "viva_api.simulation.simulation_service_ray.batch_build.poll_batch_jobs",
                new=AsyncMock(),
            ) as mock_poll,
        ):
            await service._run_build(_v2ecoli_simulator())
        assert mock_submit.await_count == 1
        assert mock_submit.call_args.kwargs["queue"] == "smscdk-vecoli-build-amd64"
        assert "docker/build-and-push-ecr.sh" in mock_submit.call_args.kwargs["command"][2]
        mock_poll.assert_awaited_once_with(["build-job-1"])

    @pytest.mark.asyncio
    async def test_submit_build_returns_local_job(self) -> None:
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch(
                "viva_api.simulation.simulation_service_ray.batch_build.submit_batch_build",
                new=AsyncMock(return_value="bj"),
            ),
            patch("viva_api.simulation.simulation_service_ray.batch_build.poll_batch_jobs", new=AsyncMock()),
        ):
            job_id = await service.submit_build_image_job(_v2ecoli_simulator())
        assert job_id.backend == JobBackend.LOCAL


class TestEnsureMnpJobDef:
    """Per-commit MNP job-def derivation (true commit image, no per-submission override)."""

    def test_reuses_existing_revision_for_same_image(self) -> None:
        image = "476270107793.dkr.ecr.us-gov-west-1.amazonaws.com/v2ecoli:abc1234"
        mock_batch = MagicMock()
        mock_batch.describe_job_definitions.return_value = {
            "jobDefinitions": [
                {"revision": 5, "nodeProperties": {"nodeRangeProperties": [{"container": {"image": image}}]}}
            ]
        }
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
        ):
            jd = service._ensure_mnp_job_def(image, "abc1234")
        assert jd == "smscdk-ray-mnp-abc1234:5"
        mock_batch.register_job_definition.assert_not_called()

    def test_memory_mib_none_is_byte_for_byte_todays_behavior(self) -> None:
        """The default (no memory hint) call site must be completely unaffected by
        this change — same job-def name, same reuse-check, no new AWS call shape.
        Regression guard against item 50 Gap 6's fix accidentally touching the
        parca/simulation job-def paths, which never pass memory_mib."""
        image = "476270107793.dkr.ecr.us-gov-west-1.amazonaws.com/v2ecoli:abc1234"
        mock_batch = MagicMock()
        mock_batch.describe_job_definitions.return_value = {
            "jobDefinitions": [
                {"revision": 5, "nodeProperties": {"nodeRangeProperties": [{"container": {"image": image}}]}}
            ]
        }
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
        ):
            jd = service._ensure_mnp_job_def(image, "abc1234", memory_mib=None)
        assert jd == "smscdk-ray-mnp-abc1234:5"
        mock_batch.register_job_definition.assert_not_called()

    def test_memory_mib_override_registers_a_distinctly_named_revision(self) -> None:
        """A workload-declared memory hint gets its OWN job-def revision (name folds
        in the memory value), with the node range's container memory resource
        requirement patched to match — not just the image, unlike every other
        existing call site."""
        image = "476270107793.dkr.ecr.us-gov-west-1.amazonaws.com/v2ecoli:abc1234"
        mock_batch = MagicMock()
        mock_batch.describe_job_definitions.side_effect = [
            {"jobDefinitions": []},  # the -mem46080 name: nothing registered yet
            {
                "jobDefinitions": [
                    {
                        "revision": 3,
                        "nodeProperties": {
                            "nodeRangeProperties": [
                                {
                                    "container": {
                                        "image": "old:image",
                                        "resourceRequirements": [
                                            {"type": "VCPU", "value": "16"},
                                            {"type": "MEMORY", "value": "60000"},
                                        ],
                                    }
                                }
                            ]
                        },
                    }
                ]
            },  # the base job def, to clone from
        ]
        mock_batch.register_job_definition.return_value = {"revision": 1}
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
        ):
            jd = service._ensure_mnp_job_def(image, "abc1234", memory_mib=46080)
        assert jd == "smscdk-ray-mnp-abc1234-mem46080:1"
        registered = mock_batch.register_job_definition.call_args.kwargs
        assert registered["jobDefinitionName"] == "smscdk-ray-mnp-abc1234-mem46080"
        node_range = registered["nodeProperties"]["nodeRangeProperties"][0]
        assert node_range["container"]["image"] == image
        reqs = {r["type"]: r["value"] for r in node_range["container"]["resourceRequirements"]}
        assert reqs["MEMORY"] == "46080"
        assert reqs["VCPU"] == "16"  # untouched — only MEMORY is overridden

    def test_memory_mib_override_reuses_a_matching_existing_revision(self) -> None:
        """A second submission with the SAME (commit, memory_mib) must reuse the
        already-registered revision, not churn a new one every call — same caching
        discipline the image-only path already has."""
        image = "476270107793.dkr.ecr.us-gov-west-1.amazonaws.com/v2ecoli:abc1234"
        mock_batch = MagicMock()
        mock_batch.describe_job_definitions.return_value = {
            "jobDefinitions": [
                {
                    "revision": 2,
                    "nodeProperties": {
                        "nodeRangeProperties": [
                            {
                                "container": {
                                    "image": image,
                                    "resourceRequirements": [{"type": "MEMORY", "value": "46080"}],
                                }
                            }
                        ]
                    },
                }
            ]
        }
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
        ):
            jd = service._ensure_mnp_job_def(image, "abc1234", memory_mib=46080)
        assert jd == "smscdk-ray-mnp-abc1234-mem46080:2"
        mock_batch.register_job_definition.assert_not_called()

    def test_memory_mib_mismatch_against_an_existing_same_named_revision_registers_a_new_one(self) -> None:
        """Defensive: if a same-named revision exists but its actual memory value
        doesn't match (shouldn't normally happen since the value is IN the name, but
        the check must not silently trust the name alone), a new revision is
        registered rather than incorrectly reused."""
        image = "476270107793.dkr.ecr.us-gov-west-1.amazonaws.com/v2ecoli:abc1234"
        mock_batch = MagicMock()
        stale_revision = {
            "jobDefinitions": [
                {
                    "revision": 2,
                    "nodeProperties": {
                        "nodeRangeProperties": [
                            {
                                "container": {
                                    "image": image,
                                    "resourceRequirements": [{"type": "MEMORY", "value": "12345"}],
                                }
                            }
                        ]
                    },
                }
            ]
        }
        mock_batch.describe_job_definitions.side_effect = [stale_revision, stale_revision]
        mock_batch.register_job_definition.return_value = {"revision": 3}
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
        ):
            jd = service._ensure_mnp_job_def(image, "abc1234", memory_mib=46080)
        assert jd == "smscdk-ray-mnp-abc1234-mem46080:3"
        mock_batch.register_job_definition.assert_called_once()


class TestAnalysisMemoryMibFor:
    """analysis_memory_mib_for reads the workload's OWN declared memory_gb hint —
    item 50 Gap 6 / PR #239's real fix: the analysis DAG node previously had no
    dedicated, workload-declared memory knob at all. Mirrors vEcoli-private's
    analysis_options.memory_gb field-for-field."""

    def test_declared_memory_gb_converts_to_mib(self) -> None:
        from viva_api.simulation.models import AnalysisOptions, SimulationConfig

        config = SimulationConfig(
            experiment_id="exp-1", analysis_options=AnalysisOptions.model_validate({"memory_gb": 45})
        )
        assert analysis_memory_mib_for(config) == 45 * 1024

    def test_fractional_memory_gb_converts_to_mib(self) -> None:
        from viva_api.simulation.models import AnalysisOptions, SimulationConfig

        config = SimulationConfig(
            experiment_id="exp-1", analysis_options=AnalysisOptions.model_validate({"memory_gb": 2.5})
        )
        assert analysis_memory_mib_for(config) == int(2.5 * 1024)

    def test_absent_hint_is_none_not_an_error(self) -> None:
        from viva_api.simulation.models import SimulationConfig

        assert analysis_memory_mib_for(SimulationConfig(experiment_id="exp-1")) is None

    def test_non_numeric_hint_is_ignored_not_raised(self) -> None:
        """A malformed hint must never block dispatch (best-effort by design, same
        posture as the rest of the analysis DAG node)."""
        from types import SimpleNamespace

        config = SimpleNamespace(analysis_options={"memory_gb": "lots"})
        assert analysis_memory_mib_for(config) is None


@pytest.mark.asyncio
class TestSubmitJobPacer:
    """_SubmitJobPacer proactively caps AWS Batch SubmitJob calls below the
    account-wide 50 TPS ceiling, computed from REAL elapsed wall-clock time
    (not a fixed guess) -- backing the chain-dispatch campaign's upfront N*G
    submission loop (backlog item 33 rework)."""

    @pytest.mark.asyncio
    async def test_first_call_never_sleeps(self) -> None:
        from viva_api.simulation.simulation_service_ray import _SubmitJobPacer

        pacer = _SubmitJobPacer(max_per_second=40.0)
        with patch("viva_api.simulation.simulation_service_ray.asyncio.sleep", new=AsyncMock()) as mock_sleep:
            await pacer.wait()
        mock_sleep.assert_not_called()

    @pytest.mark.asyncio
    async def test_back_to_back_calls_sleep_for_the_real_deficit(self) -> None:
        """Drives a FAKE monotonic clock so the computed sleep duration is
        exactly checkable, rather than asserting on real (flaky) wall-clock
        timing."""
        from viva_api.simulation.simulation_service_ray import _SubmitJobPacer

        pacer = _SubmitJobPacer(max_per_second=10.0)  # min_interval = 0.1s
        clock = iter([100.0, 100.0, 100.02])
        with (
            patch("viva_api.simulation.simulation_service_ray.time.monotonic", side_effect=lambda: next(clock)),
            patch("viva_api.simulation.simulation_service_ray.asyncio.sleep", new=AsyncMock()) as mock_sleep,
        ):
            await pacer.wait()  # consumes 100.0 -- first call, no prior, no sleep
            await pacer.wait()  # now=100.0 again -> deficit = 0.1 -> sleeps, re-reads -> 100.02
        mock_sleep.assert_awaited_once()
        (slept_for,) = mock_sleep.call_args.args
        assert slept_for == pytest.approx(0.1)

    @pytest.mark.asyncio
    async def test_no_sleep_once_enough_real_time_has_elapsed(self) -> None:
        from viva_api.simulation.simulation_service_ray import _SubmitJobPacer

        pacer = _SubmitJobPacer(max_per_second=10.0)  # min_interval = 0.1s
        clock = iter([100.0, 100.5])  # half a second later -- comfortably past the 0.1s floor
        with (
            patch("viva_api.simulation.simulation_service_ray.time.monotonic", side_effect=lambda: next(clock)),
            patch("viva_api.simulation.simulation_service_ray.asyncio.sleep", new=AsyncMock()) as mock_sleep,
        ):
            await pacer.wait()
            await pacer.wait()
        mock_sleep.assert_not_called()


@pytest.mark.asyncio
class TestChainDispatchSubmission:
    """submit_chain_dispatch_job kicks off a chain-dispatch campaign: ParCa +
    every seed's full G-generation dependsOn chain, submitted upfront
    (backlog item 33 rework — individual per-seed job chains, replacing the
    per-generation-array "wave" design's own TestWaveDispatchSubmission /
    TestSubmitNextWave)."""

    async def test_rejects_single_generation(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        setattr(experiment_request.config, "n_init_sims", 4)  # noqa: B010
        experiment_request.config.generations = 1
        simulation = await database_service.insert_simulation(sim_request=experiment_request)
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
            pytest.raises(ValueError, match="requires generations > 1"),
        ):
            await service.submit_chain_dispatch_job(ecoli_simulation=simulation, database_service=database_service)

    async def test_single_seed_multi_generation_is_now_allowed(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """Unlike the superseded per-generation-array design, n_seeds >= 2 is
        NOT required -- that floor was AWS Batch's own array-size minimum
        (arrayProperties.size must be 2-10000, verified against the real API
        model this session), which doesn't apply here: each seed's chain is
        independent standalone MNP jobs, no array involved at all."""
        setattr(experiment_request.config, "n_init_sims", 1)  # noqa: B010
        experiment_request.config.generations = 2
        simulation = await database_service.insert_simulation(sim_request=experiment_request)
        mock_batch = _fake_batch(["parca-1", "s0g0", "s0g1"])
        fake_file_service = AsyncMock()
        fake_file_service.upload_file = AsyncMock()
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
            patch("viva_api.dependencies.get_file_service", return_value=fake_file_service),
            patch("viva_api.simulation.simulation_service_ray.asyncio.sleep", new=AsyncMock()),
        ):
            job_id = await service.submit_chain_dispatch_job(
                ecoli_simulation=simulation, database_service=database_service
            )
        assert job_id == JobId.ray("parca-1")
        assert mock_batch.submit_job.call_count == 3  # parca + 2 generations, one seed

    async def test_submits_parca_then_every_seeds_full_chain(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """2 seeds x 3 generations: locks the exact dependency structure for a
        representative seed (right job count, right dependsOn linkage, right
        deterministic daughter-state S3 paths) and the campaign HpcRun row's
        shape."""
        setattr(experiment_request.config, "n_init_sims", 2)  # noqa: B010
        experiment_request.config.generations = 3
        simulation = await database_service.insert_simulation(sim_request=experiment_request)
        submit_ids = ["parca-1", "s0g0", "s0g1", "s0g2", "s1g0", "s1g1", "s1g2"]
        mock_batch = _fake_batch(submit_ids)
        fake_file_service = AsyncMock()
        fake_file_service.upload_file = AsyncMock()

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
            patch("viva_api.dependencies.get_file_service", return_value=fake_file_service),
            patch("viva_api.simulation.simulation_service_ray.asyncio.sleep", new=AsyncMock()),
        ):
            job_id = await service.submit_chain_dispatch_job(
                ecoli_simulation=simulation, database_service=database_service
            )

        assert job_id == JobId.ray("parca-1")
        assert mock_batch.submit_job.call_count == 7  # parca + 2 seeds x 3 generations
        parca_call, s0g0, s0g1, s0g2, s1g0, s1g1, s1g2 = mock_batch.submit_job.call_args_list

        assert "dependsOn" not in parca_call.kwargs
        assert "retryStrategy" not in parca_call.kwargs  # ParCa's own behavior is unchanged

        # Representative seed (seed 0)'s full chain: right linkage, right retry,
        # right shape (MNP -- nodeOverrides, never arrayProperties at all).
        assert s0g0.kwargs["dependsOn"] == [{"jobId": "parca-1", "type": "SEQUENTIAL"}]
        assert s0g1.kwargs["dependsOn"] == [{"jobId": "s0g0", "type": "SEQUENTIAL"}]
        assert s0g2.kwargs["dependsOn"] == [{"jobId": "s0g1", "type": "SEQUENTIAL"}]
        for call in (s0g0, s0g1, s0g2, s1g0, s1g1, s1g2):
            assert call.kwargs["retryStrategy"] == {"attempts": 2}
            assert "nodeOverrides" in call.kwargs
            assert "arrayProperties" not in call.kwargs

        # Seed 1's chain is entirely independent of seed 0's, both rooted at ParCa.
        assert s1g0.kwargs["dependsOn"] == [{"jobId": "parca-1", "type": "SEQUENTIAL"}]
        assert s1g1.kwargs["dependsOn"] == [{"jobId": "s1g0", "type": "SEQUENTIAL"}]
        assert s1g2.kwargs["dependsOn"] == [{"jobId": "s1g1", "type": "SEQUENTIAL"}]

        # Deterministic daughter-state S3 paths, embedded directly -- no
        # AWS_BATCH_JOB_ARRAY_INDEX / container-start resolution at all.
        experiment_id = simulation.config.experiment_id
        env = _env_of(s0g1)
        tokens = shlex.split(env["RAY_JOB_CMD"])
        overrides = json.loads(tokens[tokens.index("--overrides") + 1])
        assert overrides["seed"] == 0
        assert overrides["initial_generation_index"] == 1
        assert overrides["initial_carry_state_path"] == (
            f"s3://mybucket/vecoli-output/{experiment_id}/daughter-state/seed0/gen0.pkl"
        )
        assert overrides["daughter_state_out_path"] == (
            f"s3://mybucket/vecoli-output/{experiment_id}/daughter-state/seed0/gen1.pkl"
        )
        assert "AWS_BATCH_JOB_ARRAY_INDEX" not in env["RAY_JOB_CMD"]

        # Tags carry seed/generation for cost-allocation granularity.
        assert s0g1.kwargs["tags"]["Seed"] == "0"
        assert s0g1.kwargs["tags"]["Generation"] == "1"
        assert s0g1.kwargs["tags"]["Phase"] == "sim"

        # ONE campaign-tracking HpcRun row, holding each seed's OWN final job id.
        active_campaigns = [
            c for c in await database_service.list_active_chain_campaigns() if c.ref_id == simulation.database_id
        ]
        assert len(active_campaigns) == 1
        campaign = active_campaigns[0]
        assert campaign.chain_n_generations == 3
        assert campaign.chain_final_job_ids == ["s0g2", "s1g2"]
        assert campaign.job_id == JobId.ray("parca-1")

    async def test_paces_every_submission_through_the_rate_limiter(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        setattr(experiment_request.config, "n_init_sims", 2)  # noqa: B010
        experiment_request.config.generations = 2
        simulation = await database_service.insert_simulation(sim_request=experiment_request)
        mock_batch = _fake_batch(["parca-1", "s0g0", "s0g1", "s1g0", "s1g1"])
        fake_file_service = AsyncMock()
        fake_file_service.upload_file = AsyncMock()

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
            patch("viva_api.dependencies.get_file_service", return_value=fake_file_service),
            patch(
                "viva_api.simulation.simulation_service_ray._SubmitJobPacer.wait", new=AsyncMock()
            ) as mock_pacer_wait,
        ):
            await service.submit_chain_dispatch_job(ecoli_simulation=simulation, database_service=database_service)
        # Every one of the 5 real SubmitJob calls (parca + 2 seeds x 2
        # generations) is paced -- not just the bulk per-seed chain jobs.
        assert mock_pacer_wait.await_count == 5

    async def test_seed_submission_failure_truncates_only_that_seeds_chain(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """seed 0's generation 1 submission fails (even after retry-on-throttle
        is exhausted): seed 0's chain is truncated right there (its
        ALREADY-submitted generation 0 job keeps running normally on Batch --
        nothing here cancels it), generation 2 is NEVER attempted for seed 0,
        and seed 0's TRACKED final job is generation 0's real id (not a
        generation-2 id that never existed). Seed 1 is entirely unaffected."""
        setattr(experiment_request.config, "n_init_sims", 2)  # noqa: B010
        experiment_request.config.generations = 3
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        base_node_props = {
            "numNodes": 4,
            "mainNode": 0,
            "nodeRangeProperties": [
                {"targetNodes": "0:", "container": {"image": "111.dkr.ecr.x/vecoli:ray", "vcpus": 16}}
            ],
        }
        mock_batch = MagicMock()

        def _describe(**kwargs: Any) -> dict[str, Any]:
            if kwargs.get("jobDefinitionName") == "smscdk-ray-mnp":
                return {"jobDefinitions": [{"revision": 7, "nodeProperties": base_node_props}]}
            return {"jobDefinitions": []}

        mock_batch.describe_job_definitions.side_effect = _describe
        mock_batch.register_job_definition.side_effect = lambda **kw: {
            "jobDefinitionName": kw["jobDefinitionName"],
            "revision": 1,
        }
        remaining_ids = iter(["parca-1", "s0g0", "s1g0", "s1g1", "s1g2"])

        def _submit_job(**kwargs: Any) -> dict[str, Any]:
            if kwargs["jobName"].startswith("chain-seed0-gen1-"):
                raise RuntimeError("submit_job: rate exceeded (simulated, retries exhausted)")
            return {"jobId": next(remaining_ids)}

        mock_batch.submit_job.side_effect = _submit_job
        fake_file_service = AsyncMock()
        fake_file_service.upload_file = AsyncMock()

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
            patch("viva_api.dependencies.get_file_service", return_value=fake_file_service),
            patch("viva_api.simulation.simulation_service_ray.asyncio.sleep", new=AsyncMock()),
        ):
            await service.submit_chain_dispatch_job(ecoli_simulation=simulation, database_service=database_service)

        submitted_names = [c.kwargs["jobName"] for c in mock_batch.submit_job.call_args_list]
        assert any(n.startswith("chain-seed0-gen0-") for n in submitted_names)
        assert any(n.startswith("chain-seed0-gen1-") for n in submitted_names)  # attempted (and failed)
        assert not any(n.startswith("chain-seed0-gen2-") for n in submitted_names)  # never attempted
        for gen in range(3):
            assert any(n.startswith(f"chain-seed1-gen{gen}-") for n in submitted_names)

        active_campaigns = [
            c for c in await database_service.list_active_chain_campaigns() if c.ref_id == simulation.database_id
        ]
        assert len(active_campaigns) == 1
        assert active_campaigns[0].chain_final_job_ids == ["s0g0", "s1g2"]

    async def test_generation_zero_failure_excludes_that_seed_entirely(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """If even generation 0 fails to submit for a seed, that seed
        contributes NOTHING to chain_final_job_ids -- there is no job of its
        own at all for the analysis-fan-in poller to track."""
        setattr(experiment_request.config, "n_init_sims", 2)  # noqa: B010
        experiment_request.config.generations = 2
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        base_node_props = {
            "numNodes": 4,
            "mainNode": 0,
            "nodeRangeProperties": [
                {"targetNodes": "0:", "container": {"image": "111.dkr.ecr.x/vecoli:ray", "vcpus": 16}}
            ],
        }
        mock_batch = MagicMock()

        def _describe(**kwargs: Any) -> dict[str, Any]:
            if kwargs.get("jobDefinitionName") == "smscdk-ray-mnp":
                return {"jobDefinitions": [{"revision": 7, "nodeProperties": base_node_props}]}
            return {"jobDefinitions": []}

        mock_batch.describe_job_definitions.side_effect = _describe
        mock_batch.register_job_definition.side_effect = lambda **kw: {
            "jobDefinitionName": kw["jobDefinitionName"],
            "revision": 1,
        }
        remaining_ids = iter(["parca-1", "s1g0", "s1g1"])

        def _submit_job(**kwargs: Any) -> dict[str, Any]:
            if kwargs["jobName"].startswith("chain-seed0-gen0-"):
                raise RuntimeError("submit_job: rate exceeded (simulated, retries exhausted)")
            return {"jobId": next(remaining_ids)}

        mock_batch.submit_job.side_effect = _submit_job
        fake_file_service = AsyncMock()
        fake_file_service.upload_file = AsyncMock()

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
            patch("viva_api.dependencies.get_file_service", return_value=fake_file_service),
            patch("viva_api.simulation.simulation_service_ray.asyncio.sleep", new=AsyncMock()),
        ):
            await service.submit_chain_dispatch_job(ecoli_simulation=simulation, database_service=database_service)

        active_campaigns = [
            c for c in await database_service.list_active_chain_campaigns() if c.ref_id == simulation.database_id
        ]
        assert len(active_campaigns) == 1
        # Only seed 1 contributed -- seed 0's generation-0 failure means it has
        # no job at all to track.
        assert active_campaigns[0].chain_final_job_ids == ["s1g1"]


@pytest.mark.asyncio
class TestSubmitCampaignAnalysis:
    """submit_campaign_analysis is what JobScheduler calls once the
    analysis-fan-in poller confirms a chain-dispatch campaign is all-terminal
    (backlog item 33 rework) -- reuses item 24's existing analysis-job
    submission code (_submit_analysis_job) completely as-is, with NO native
    dependsOn."""

    async def test_submits_analysis_with_no_dependency(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        simulation = await database_service.insert_simulation(sim_request=experiment_request)
        mock_batch = _fake_batch(["analysis-999"])
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
        ):
            job_id = await service.submit_campaign_analysis(
                simulation=simulation,
                database_service=database_service,
                commit="abc1234",
                total_n_seeds=30,
                n_generations=10,
            )
        assert job_id == "analysis-999"
        assert mock_batch.submit_job.call_count == 1
        (analysis_call,) = mock_batch.submit_job.call_args_list
        # By construction everything this depends on has ALREADY finished --
        # no native dependsOn at all (unlike item 24's single-shot-dispatch
        # shape, which depends on the sim job it rides directly behind).
        assert "dependsOn" not in analysis_call.kwargs
        assert analysis_call.kwargs["nodeOverrides"]["numNodes"] == 1  # rides on MNP, matching item 24

        records = await database_service.list_analyses(simulation_id=simulation.database_id)
        assert len(records) == 1
        assert records[0].job_id_ext == "analysis-999"
        assert records[0].backend == "ray"

    async def test_uses_the_originally_requested_seed_count_not_survivor_count(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """Matches the superseded design's own resolved semantics: analysis
        modules resolve 'applicable' against the campaign's INTENDED shape,
        not however many chains actually survived to completion."""
        simulation = await database_service.insert_simulation(sim_request=experiment_request)
        mock_batch = _fake_batch(["analysis-999"])
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
        ):
            await service.submit_campaign_analysis(
                simulation=simulation,
                database_service=database_service,
                commit="abc1234",
                total_n_seeds=1000,  # the originally requested total, even if fewer chains actually succeeded
                n_generations=10,
            )
        env = _env_of(mock_batch.submit_job.call_args_list[0])
        assert "--n-seeds 1000" in env["RAY_JOB_CMD"]

    async def test_configured_analysis_options_reach_the_analysis_job(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """REGRESSION (item 24, retargeted for backlog item 33): config.
        analysis_options (set by the run endpoint from the caller's
        --analysis-options, and by the workbench from a study's spec.analyses)
        must reach the analysis job's --modules flag. Originally verified
        through submit_ecoli_simulation_job's own inline array-path analysis
        submission (now removed -- that shape's analysis fires exclusively
        through submit_campaign_analysis, exercised directly here)."""
        from viva_api.simulation.models import AnalysisOptions

        experiment_request.config.analysis_options = AnalysisOptions.model_validate({
            "multiseed": {"cd1_fluxomics": {"generation_lower_bound": 5}}
        })
        simulation = await database_service.insert_simulation(sim_request=experiment_request)
        mock_batch = _fake_batch(["analysis-789"])
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
        ):
            await service.submit_campaign_analysis(
                simulation=simulation,
                database_service=database_service,
                commit="abc1234",
                total_n_seeds=4,
                n_generations=3,
            )
        cmd = _env_of(mock_batch.submit_job.call_args_list[0])["RAY_JOB_CMD"]
        tokens = shlex.split(cmd.split("&&", 1)[1].replace("V2ECOLI_SIM_DATA=", "", 1))
        assert json.loads(tokens[tokens.index("--modules") + 1]) == {
            "multiseed": {"cd1_fluxomics": {"generation_lower_bound": 5}}
        }

    async def test_a_failed_analysis_submission_is_recorded_not_swallowed(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """By the time this runs, every seed chain the poller was watching has
        already reached a terminal state -- raising here would just lose the
        analysis silently. It must land as a FAILED analyses-table row
        instead (item 24's guarantee, retargeted for backlog item 33: this is
        now the canonical shape's own analysis trigger, replacing
        submit_ecoli_simulation_job's removed inline path)."""
        simulation = await database_service.insert_simulation(sim_request=experiment_request)
        mock_batch = _fake_batch([])
        mock_batch.submit_job.side_effect = RuntimeError("Batch said no")

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
        ):
            result = await service.submit_campaign_analysis(
                simulation=simulation,
                database_service=database_service,
                commit="abc1234",
                total_n_seeds=4,
                n_generations=3,
            )
        assert result is None
        records = await database_service.list_analyses(simulation_id=simulation.database_id)
        assert len(records) == 1
        assert records[0].status == JobStatus.FAILED
        assert "Batch said no" in (records[0].error_message or "")
