"""Tests for the Ray-on-Batch backend: JobId.ray, Batch state mapping, ComputeBackend.RAY,
and SimulationServiceRay submission/status/cancel (boto3 mocked, Postgres via testcontainers)."""

import asyncio
import json
import shlex
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from viva_api.common.hpc.job_service import JobStatusInfo
from viva_api.common.models import JobBackend, JobId, JobStatus
from viva_api.config import ComputeBackend
from viva_api.simulation.models import AnalysisOptions, HpcRun, JobType
from viva_api.simulation.simulation_service_ray import (
    NEW_GENE_INDUCED_CACHE_DIR,
    PARCA_CACHE_DIR,
    PARCA_SIMDATA_DIR,
    SIM_OUT_DIR,
    V2ECOLI_DIR,
    SimulationServiceRay,
    analysis_modules_for,
    injected_processes_from_config,
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
        ray_mnp_standalone_queue="",  # unconfigured by default -- real fallback behavior (item 65)
        ray_array_queue="smscdk-vecoli-task-amd64",
        ray_array_job_definition="smscdk-ray-array",
        ray_container_queue="",  # unconfigured by default -- real fallback: raises (item 71)
        ray_container_job_definition="",
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


def _container_settings(**overrides: Any) -> MagicMock:
    """A settings double with the plain container-path (backlog item 71) fields
    configured, for tests that exercise _ensure_container_job_def/_submit_container."""
    s = _ray_settings()
    s.ray_container_queue = "smscdk-ray-standalone"
    s.ray_container_job_definition = "smscdk-ray-container"
    for k, v in overrides.items():
        setattr(s, k, v)
    return s


def _fake_container_batch(submit_ids: list[str]) -> MagicMock:
    """A boto3 Batch mock for the container job-def path (backlog item 71),
    mirroring _fake_batch's per-commit-revision-derivation shape but for
    containerProperties instead of nodeProperties/nodeRangeProperties."""
    b = MagicMock()
    base_container_props = {"image": "111.dkr.ecr.x/vecoli:ray", "vcpus": 16, "memory": 32000}

    def _describe(**kwargs: Any) -> dict[str, Any]:
        name = kwargs.get("jobDefinitionName")
        if name == "smscdk-ray-container":  # container base
            return {"jobDefinitions": [{"revision": 7, "containerProperties": base_container_props}]}
        return {"jobDefinitions": []}  # per-commit: none yet

    b.describe_job_definitions.side_effect = _describe
    b.register_job_definition.side_effect = lambda **kw: {"jobDefinitionName": kw["jobDefinitionName"], "revision": 1}
    b.submit_job.side_effect = [{"jobId": jid} for jid in submit_ids]
    return b


def _container_env_of(call: Any) -> dict[str, str]:
    """Env dict for a _submit_container call (single containerOverrides.environment list)."""
    return {e["name"]: e["value"] for e in call.kwargs["containerOverrides"]["environment"]}


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
        is delegated ENTIRELY to submit_chain_dispatch_job (backlog item 33
        rework), which (backlog item 71 Phase 4) now submits ONLY ParCa as a
        container job and writes the campaign's initial per-seed tracking row
        -- generation submission moves to JobScheduler's poll loop (see
        TestAdvanceChainCampaign, tests/simulation/test_scheduler.py).

        This is the test that would have caught the real wiring gap found in
        review: submit_chain_dispatch_job existed and was fully tested in
        isolation from the moment it was built, but nothing on the REAL
        submit_ecoli_simulation_job entrypoint ever called it until routing
        landed -- a real request would have silently kept exercising the old
        array/wave-style path forever."""
        setattr(experiment_request.config, "n_init_sims", 2)  # noqa: B010
        experiment_request.config.generations = 3
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        mock_batch = _fake_container_batch(["parca-1"])

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
        ):
            job_id = await service.submit_ecoli_simulation_job(
                ecoli_simulation=simulation, database_service=database_service, correlation_id="corr-real-entry"
            )

            # The entrypoint returns a LOCAL task id the instant the campaign is
            # scheduled and submits ParCa + writes the initial tracking row in
            # the background (_submit_chain_dispatch_background) -- so every
            # downstream invariant below is checked AFTER awaiting that task.
            # Awaiting the asyncio.Task directly blocks until it is done and
            # re-raises anything it hit; it has to happen INSIDE the patch
            # context, since the task is what actually calls boto3.
            assert job_id.backend == JobBackend.LOCAL
            campaign_job_id = await service._local._tasks[job_id.value]

        # Chain-dispatch's own return convention: the ParCa job id.
        assert campaign_job_id == JobId.ray("parca-1")
        # Exactly ONE submission now -- no per-seed generation jobs upfront.
        assert mock_batch.submit_job.call_count == 1
        (parca_call,) = mock_batch.submit_job.call_args_list
        assert "dependsOn" not in parca_call.kwargs
        assert "containerOverrides" in parca_call.kwargs  # container-type, not MNP

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
        assert campaign.chain_final_job_ids == []
        assert campaign.chain_current_job_ids == [None, None]
        assert campaign.chain_current_generation == [None, None]
        assert campaign.chain_parca_done is False

    async def test_submit_routes_canonical_batch_baseline_to_chain_dispatch_when_single_seed(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """Unlike the array-job design predating item 33 (which required
        n_seeds > 1 -- AWS Batch's own array-size floor), a single-seed
        canonical batch_baseline request is ALSO routed to chain-dispatch --
        confirmed here at the REAL entrypoint (not just in
        TestChainDispatchSubmission's own isolated coverage of the same
        claim), now submitting just ParCa + the initial per-seed row
        (backlog item 71 Phase 4)."""
        setattr(experiment_request.config, "n_init_sims", 1)  # noqa: B010
        experiment_request.config.generations = 3
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        mock_batch = _fake_container_batch(["parca-1"])

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
        ):
            job_id = await service.submit_ecoli_simulation_job(
                ecoli_simulation=simulation, database_service=database_service, correlation_id="corr-single-real"
            )
            # Background dispatch (see the multiseed test above for the full
            # rationale): await the spawned task before checking what it did.
            assert job_id.backend == JobBackend.LOCAL
            campaign_job_id = await service._local._tasks[job_id.value]

        assert campaign_job_id == JobId.ray("parca-1")
        assert mock_batch.submit_job.call_count == 1
        campaign = await database_service.get_hpcrun_by_ref(ref_id=simulation.database_id, job_type=JobType.SIMULATION)
        assert campaign is not None
        assert campaign.chain_current_job_ids == [None]

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
        viva-api went right on submitting the real, AWS-billed campaign.
        Backlog item 71 Phase 4 additionally shrank submit_chain_dispatch_job
        itself down to one ParCa submission + one DB insert (no more N*G loop
        at all), so the ORIGINAL pathological 15-minute case can no longer
        happen by construction -- but the backgrounding mechanism itself
        (_submit_chain_dispatch_background, unchanged by Phase 4) is still the
        real code path and still worth locking down: the chokepoint here holds
        submit_chain_dispatch_job itself (not an internal implementation
        detail of it), so this test is robust to future changes in what that
        method does internally.
        """
        setattr(experiment_request.config, "n_init_sims", 2)  # noqa: B010
        experiment_request.config.generations = 3
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        mock_batch = _fake_container_batch(["parca-1"])

        released = asyncio.Event()
        real_submit_chain_dispatch_job = SimulationServiceRay.submit_chain_dispatch_job

        async def _held_submit_chain_dispatch_job(self: "SimulationServiceRay", *args: Any, **kwargs: Any) -> JobId:
            await released.wait()
            return await real_submit_chain_dispatch_job(self, *args, **kwargs)

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
            patch.object(SimulationServiceRay, "submit_chain_dispatch_job", _held_submit_chain_dispatch_job),
        ):
            # The property under test, stated directly: the entrypoint returns
            # promptly regardless of how long the campaign's own submission
            # takes. asyncio.wait_for, rather than a bare await, so that an
            # inline (non-backgrounded) regression FAILS here in seconds
            # instead of deadlocking the suite -- submitting inline, it would
            # park at the chokepoint above and never come back, since only
            # this test body releases it.
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
            # a zero-seed campaign would look terminal with nothing to analyze,
            # recreating a false failure inside viva-api itself.
            assert [
                c.database_id
                for c in await database_service.list_active_chain_campaigns()
                if c.ref_id == simulation.database_id
            ] == []

            # Release the chokepoint and let the campaign finish submitting.
            released.set()
            campaign_job_id = await service._local._tasks[job_id.value]

        # 5. The real campaign row now supersedes the placeholder for every later
        # read (get_hpcrun_by_ref is ORDER BY id DESC) and IS in the scheduler's
        # poll set -- so analysis auto-fire stays wired exactly as before.
        assert campaign_job_id == JobId.ray("parca-1")
        assert mock_batch.submit_job.call_count == 1  # just ParCa (backlog item 71 Phase 4)
        campaign = await database_service.get_hpcrun_by_ref(ref_id=simulation.database_id, job_type=JobType.SIMULATION)
        assert campaign is not None
        assert campaign.database_id != placeholder.database_id
        assert campaign.job_id == JobId.ray("parca-1")
        assert campaign.chain_n_generations == 3
        assert campaign.chain_current_job_ids == [None, None]
        assert campaign.chain_parca_done is False
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


def _fake_multi_node_batch(submit_ids: list[str], *, per_node_vcpus: int = 16) -> MagicMock:
    """Like _fake_batch, but also answers describe_job_definitions for the
    PER-COMMIT derived name (not just the CDK base) with a real
    resourceRequirements shape (backlog item 88's _mnp_node_vcpus reads this
    -- confirmed live 2026-08-24 against the real smsvpctest-ray-mnp job def).

    Matches the per-commit name GENERICALLY (any name != the base) rather than
    a hardcoded commit string -- the real commit hash is generated fresh per
    test run by the experiment_request/database_service fixtures, not a fixed
    value this fixture can know in advance."""
    b = MagicMock()
    base_node_props = {
        "numNodes": 4,
        "mainNode": 0,
        "nodeRangeProperties": [{"targetNodes": "0:", "container": {"image": "111.dkr.ecr.x/vecoli:ray", "vcpus": 16}}],
    }

    def _per_commit_props(image: str) -> dict[str, Any]:
        return {
            "numNodes": 4,
            "mainNode": 0,
            "nodeRangeProperties": [
                {
                    "targetNodes": "0:",
                    "container": {
                        "image": image,
                        "resourceRequirements": [
                            {"type": "VCPU", "value": str(per_node_vcpus)},
                            {"type": "MEMORY", "value": "60000"},
                        ],
                    },
                }
            ],
        }

    def _describe(**kwargs: Any) -> dict[str, Any]:
        name = kwargs.get("jobDefinitionName")
        if name == "smscdk-ray-mnp":
            return {"jobDefinitions": [{"revision": 7, "nodeProperties": base_node_props}]}
        if name and name.startswith("smscdk-ray-mnp-"):
            commit = name.removeprefix("smscdk-ray-mnp-")
            image = f"476270107793.dkr.ecr.us-gov-west-1.amazonaws.com/v2ecoli:{commit}"
            return {"jobDefinitions": [{"revision": 1, "nodeProperties": _per_commit_props(image)}]}
        return {"jobDefinitions": []}

    b.describe_job_definitions.side_effect = _describe
    b.register_job_definition.side_effect = lambda **kw: {"jobDefinitionName": kw["jobDefinitionName"], "revision": 1}
    b.submit_job.side_effect = [{"jobId": jid} for jid in submit_ids]
    return b


class TestSubmitMultiNodeComposite:
    """Backlog item 88: a generic multi-node process-bigraph composite dispatch
    (e.g. a colony composite), routed via SimulationConfig's extra
    `multi_node_dispatch` field. Never references any one composite by name --
    colony is the motivating case, not a hardcoded target."""

    @pytest.mark.asyncio
    async def test_multi_node_dispatch_routes_before_chain_dispatch_even_when_generations_over_one(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """The critical ordering guard: composite is None + generations>1 would
        otherwise satisfy chain-dispatch's own routing condition. A
        multi_node_dispatch request must win that race, not silently fall
        through to chain-dispatch."""
        setattr(  # noqa: B010
            experiment_request.config,
            "multi_node_dispatch",
            {"composite_id": "some_workspace.composites.some_multi_node_composite", "num_nodes": 2, "params": {}},
        )
        experiment_request.config.generations = 5  # would satisfy chain-dispatch's own condition
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        mock_batch = _fake_multi_node_batch(["parca-1", "composite-1"])
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
                ecoli_simulation=simulation, database_service=database_service, correlation_id="corr-mnp-composite"
            )

        # NOT chain-dispatch's JobBackend.LOCAL placeholder -- a real MNP job id.
        assert job_id == JobId.ray("composite-1")
        assert mock_batch.submit_job.call_count == 2

    @pytest.mark.asyncio
    async def test_multi_node_composite_command_and_tags(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        setattr(  # noqa: B010
            experiment_request.config,
            "multi_node_dispatch",
            {
                "composite_id": "some_workspace.composites.some_multi_node_composite",
                "num_nodes": 2,
                "params": {"n_cells": 6, "env_size": 20},
                "steps": 3,
            },
        )
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        mock_batch = _fake_multi_node_batch(["parca-9", "composite-9"], per_node_vcpus=16)
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
                ecoli_simulation=simulation, database_service=database_service, correlation_id="corr-mnp-cmd"
            )

        assert job_id == JobId.ray("composite-9")
        parca_call, composite_call = mock_batch.submit_job.call_args_list

        # Real MNP job, gated on parca, N nodes -- same shape as the comparison-ensemble path.
        assert composite_call.kwargs["dependsOn"] == [{"jobId": "parca-9", "type": "SEQUENTIAL"}]
        assert composite_call.kwargs["nodeOverrides"]["numNodes"] == 2
        assert composite_call.kwargs["jobQueue"] == "smscdk-ray-mnp"  # genuine multi-node, never the standalone queue
        assert parca_call.kwargs["nodeOverrides"]["numNodes"] == 1

        cmd = _env_of(composite_call)["RAY_JOB_CMD"]
        # Reuses the existing generic run_pbg.py runner, not a new script.
        assert "run_pbg.py" in cmd
        assert "--composite-id some_workspace.composites.some_multi_node_composite" in cmd
        assert "-n 3" in cmd
        assert shlex.quote(json.dumps({"n_cells": 6, "env_size": 20})) in cmd
        # Sized from the job definition's REAL per-node vCPU declaration (16) x num_nodes (2).
        assert "RAY_SHARDS_DEFAULT=32" in cmd
        # No colony/composite-specific hardcoding anywhere in the built command.
        assert "colony" not in cmd.lower()

        assert composite_call.kwargs["tags"]["CompositeId"] == "some_workspace.composites.some_multi_node_composite"

    def test_multi_node_composite_command_sets_pythonpath_for_injection_imports(self) -> None:
        """Direct unit test of _multi_node_composite_command's own command string
        (backlog item 93): a colony/multi-node composite can carry
        injected_processes the same way ecoli_baseline's chain-dispatch path can,
        so it needs the same PYTHONPATH fix, not just chain-dispatch's own two
        call sites (see TestSimulationServiceRayBuild/TestSeedGenerationCommand's
        sibling assertions)."""
        service = SimulationServiceRay()
        with patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings):
            cmd = service._multi_node_composite_command(
                composite_id="some_workspace.composites.some_multi_node_composite",
                params={"n_cells": 6},
                steps=3,
                runner_s3_uri="s3://mybucket/vecoli-output/sim9-colony/run_pbg.py",
                n_shards_default=None,
            )
        assert "cd /app/v2ecoli" in cmd
        assert "PYTHONPATH=/app/v2ecoli" in cmd
        assert "python /tmp/run_pbg.py" in cmd

    @pytest.mark.asyncio
    async def test_multi_node_dispatch_requires_composite_id(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        setattr(experiment_request.config, "multi_node_dispatch", {"num_nodes": 2})  # noqa: B010
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
            pytest.raises(ValueError, match="composite_id"),
        ):
            await service.submit_ecoli_simulation_job(
                ecoli_simulation=simulation, database_service=database_service, correlation_id="corr-mnp-noid"
            )

    def test_mnp_node_vcpus_reads_real_resource_requirements(self) -> None:
        mock_batch = _fake_multi_node_batch(["unused"], per_node_vcpus=16)
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
        ):
            vcpus = service._mnp_node_vcpus("smscdk-ray-mnp-abc123:1")
        assert vcpus == 16

    def test_mnp_node_vcpus_returns_none_on_unknown_job_def(self) -> None:
        # A name outside _fake_multi_node_batch's own recognized prefixes (base
        # "smscdk-ray-mnp" or any "smscdk-ray-mnp-<commit>") -- genuinely
        # unmatched, unlike a per-commit-shaped name (which the fixture answers
        # generically, by design, since real per-commit revisions always exist
        # once _ensure_mnp_job_def has registered one).
        mock_batch = _fake_multi_node_batch(["unused"])
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
        ):
            assert service._mnp_node_vcpus("totally-different-job-def:1") is None

    def test_mnp_node_vcpus_retries_through_transient_empty_result_then_succeeds(self) -> None:
        """Backlog item 101: on a commit's FIRST-ever multi-node dispatch,
        describe_job_definitions can briefly return empty for a job def
        _ensure_mnp_job_def had JUST registered (AWS eventual consistency --
        confirmed live 2026-08-25, sim255). Simulates that: the first 2 calls
        return an empty jobDefinitions list (the real shape AWS returns, not
        an exception), the 3rd succeeds -- the retry loop must not give up
        early and must not treat the transient empty as a permanent unknown
        job def (contrast test_mnp_node_vcpus_returns_none_on_unknown_job_def,
        which never resolves)."""
        real_result = _fake_multi_node_batch(["unused"], per_node_vcpus=16).describe_job_definitions(
            jobDefinitionName="smscdk-ray-mnp-somecommit"
        )
        mock_batch = MagicMock()
        mock_batch.describe_job_definitions.side_effect = [
            {"jobDefinitions": []},
            {"jobDefinitions": []},
            real_result,
        ]
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch.object(SimulationServiceRay, "_VCPU_LOOKUP_BACKOFF_SECONDS", 0.0),
        ):
            vcpus = service._mnp_node_vcpus("smscdk-ray-mnp-somecommit:1")
        assert vcpus == 16
        assert mock_batch.describe_job_definitions.call_count == 3

    def test_mnp_node_vcpus_gives_up_after_exhausting_retries(self) -> None:
        """The other half of the same guard: a GENUINELY unknown/never-
        materializing job def must still return None eventually, not retry
        forever."""
        mock_batch = MagicMock()
        mock_batch.describe_job_definitions.return_value = {"jobDefinitions": []}
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch.object(SimulationServiceRay, "_VCPU_LOOKUP_BACKOFF_SECONDS", 0.0),
        ):
            assert service._mnp_node_vcpus("smscdk-ray-mnp-neverexists:1") is None
        assert mock_batch.describe_job_definitions.call_count == SimulationServiceRay._VCPU_LOOKUP_RETRIES

    @pytest.mark.asyncio
    async def test_existing_comparison_ensemble_path_unaffected_when_multi_node_dispatch_absent(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """Explicit regression proof (not just relying on the pre-existing
        comparison-ensemble/chain-dispatch tests staying green): the new
        routing check is a true no-op when multi_node_dispatch is absent."""
        assert getattr(experiment_request.config, "multi_node_dispatch", None) is None
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
                ecoli_simulation=simulation, database_service=database_service, correlation_id="corr-unaffected"
            )
        # Byte-for-byte the same outcome as test_composite_comparison_ensemble_with_multiple_generations_stays_on_mnp.
        assert job_id == JobId.ray("sim-456")
        assert mock_batch.submit_job.call_count == 2


class TestMultiNodeAnalysisCommand:
    """Item 109: _multi_node_analysis_command tries a hive-parquet read
    (matching run_standalone_analysis.py's own DuckDB mechanism) before
    falling back to the original flat-file path, when n_seeds is given."""

    def test_omits_seed_flags_entirely_when_n_seeds_not_given(self) -> None:
        """Byte-for-byte unchanged from before this item's fix -- colony's own
        real shape (item 88) never has n_seeds in the same sense."""
        service = SimulationServiceRay()
        with patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings):
            cmd = service._multi_node_analysis_command(
                experiment_id="exp1",
                composite_id="v2ecoli.composites.ecoli_colony.ecoli_colony",
                history_uri="s3://bucket/exp1",
                out_uri="s3://bucket/exp1/analyses/a1",
            )
        assert "--n-seeds" not in cmd
        assert "--n-generations" not in cmd
        assert "--modules" not in cmd
        assert "run_multi_node_analysis.py" in cmd

    def test_applicable_keyword_rides_as_a_bare_token_not_json_encoded(self) -> None:
        """Regression for the real bug caught while building this: json.dumps
        would turn "applicable" into the 12-char string '"applicable"'
        (quotes included), which the receiving script's own
        `.strip().lower() == "applicable"` check would silently miss."""
        service = SimulationServiceRay()
        with patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings):
            cmd = service._multi_node_analysis_command(
                experiment_id="exp1",
                composite_id="v2ecoli.composites.lineage_ray_batch",
                history_uri="s3://bucket/exp1",
                out_uri="s3://bucket/exp1/analyses/a1",
                n_seeds=10,
                n_generations=10,
                modules="applicable",
            )
        tokens = shlex.split(cmd.split("&&", 1)[1])
        assert tokens[tokens.index("--modules") + 1] == "applicable"
        assert "--n-seeds 10" in cmd
        assert "--n-generations 10" in cmd

    def test_explicit_module_mapping_rides_as_real_json(self) -> None:
        service = SimulationServiceRay()
        modules: dict[str, dict[str, Any]] = {"multiseed": {"doubling_time_distribution": {}}}
        with patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings):
            cmd = service._multi_node_analysis_command(
                experiment_id="exp1",
                composite_id="v2ecoli.composites.lineage_ray_batch",
                history_uri="s3://bucket/exp1",
                out_uri="s3://bucket/exp1/analyses/a1",
                n_seeds=10,
                modules=modules,
            )
        tokens = shlex.split(cmd.split("&&", 1)[1])
        assert json.loads(tokens[tokens.index("--modules") + 1]) == modules


class TestSubmitMultiNodeAnalysisExtraction:
    """submit_multi_node_analysis (item 109) extracts n_seeds/n_generations
    from the ORIGINAL dispatch's own stored multi_node_dispatch.params, and
    the module selection via analysis_modules_for (the SAME resolver
    _analysis_command already uses) -- both threaded into the command
    builder rather than left at colony's own flat-file-only default."""

    @pytest.mark.asyncio
    async def test_extracts_n_seeds_and_modules_from_the_original_dispatch_config(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        setattr(  # noqa: B010
            experiment_request.config,
            "multi_node_dispatch",
            {
                "composite_id": "v2ecoli.composites.lineage_ray_batch",
                "num_nodes": 4,
                "params": {"n_seeds": 10, "n_generations": 10},
            },
        )
        experiment_request.config.analysis_options = AnalysisOptions.model_validate({
            "multiseed": {"doubling_time_distribution": {}}
        })
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        service = SimulationServiceRay()
        captured: dict[str, Any] = {}

        def fake_submit_container(*, job_cmd: str, **kw: Any) -> str:
            captured["job_cmd"] = job_cmd
            return "mnp-analysis-job-1"

        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch.object(service, "_submit_container", side_effect=fake_submit_container),
            patch.object(service, "_ensure_container_job_def", return_value="job-def:1"),
            patch.object(service, "_image_uri", return_value="ghcr.io/example/image:abc"),
        ):
            job_id = await service.submit_multi_node_analysis(
                simulation=simulation,
                database_service=database_service,
                commit="abc123",
                composite_id="v2ecoli.composites.lineage_ray_batch",
            )

        assert job_id == "mnp-analysis-job-1"
        cmd = captured["job_cmd"]
        assert "--n-seeds 10" in cmd
        assert "--n-generations 10" in cmd
        tokens = shlex.split(cmd.split("&&", 1)[1])
        assert json.loads(tokens[tokens.index("--modules") + 1]) == {"multiseed": {"doubling_time_distribution": {}}}

    @pytest.mark.asyncio
    async def test_unset_analysis_options_and_no_n_seeds_falls_back_to_the_old_flat_file_shape(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """Colony's own real shape (item 88): no multi_node_dispatch.params at
        all (a caller who never set n_seeds) must still build byte-for-byte
        the same command as before this item's fix -- no --n-seeds/--modules
        flags at all, so an older simulator image (built before this fix)
        keeps working unchanged."""
        setattr(  # noqa: B010
            experiment_request.config,
            "multi_node_dispatch",
            {"composite_id": "v2ecoli.composites.ecoli_colony.ecoli_colony", "num_nodes": 2, "params": {}},
        )
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        service = SimulationServiceRay()
        captured: dict[str, Any] = {}

        def fake_submit_container(*, job_cmd: str, **kw: Any) -> str:
            captured["job_cmd"] = job_cmd
            return "mnp-analysis-job-2"

        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch.object(service, "_submit_container", side_effect=fake_submit_container),
            patch.object(service, "_ensure_container_job_def", return_value="job-def:1"),
            patch.object(service, "_image_uri", return_value="ghcr.io/example/image:abc"),
        ):
            await service.submit_multi_node_analysis(
                simulation=simulation,
                database_service=database_service,
                commit="abc123",
                composite_id="v2ecoli.composites.ecoli_colony.ecoli_colony",
            )

        cmd = captured["job_cmd"]
        assert "--n-seeds" not in cmd
        assert "--modules" not in cmd
        assert "run_multi_node_analysis.py" in cmd


class TestSubmitMnpStandaloneQueueRouting:
    """Backlog item 65: a genuinely standalone (numNodes=1) MNP submission has no
    inter-node traffic to protect, so it gains nothing from ray_mnp_queue's
    cluster-placement-group compute environment and pays its full concurrency
    cost for nothing. _submit_mnp routes such a job to ray_mnp_standalone_queue
    instead, when one is configured -- automatic, no call-site changes needed."""

    def _settings(self, *, standalone_queue: str) -> MagicMock:
        s = _ray_settings()
        s.ray_mnp_standalone_queue = standalone_queue
        return s

    def test_standalone_submission_routes_to_standalone_queue_when_configured(self) -> None:
        settings = self._settings(standalone_queue="smscdk-ray-standalone")
        mock_batch = MagicMock()
        mock_batch.submit_job.return_value = {"jobId": "job-1"}
        service = SimulationServiceRay()
        with patch("viva_api.simulation.simulation_service_ray.get_settings", return_value=settings):
            service._submit_mnp(
                job_name="standalone-test",
                job_definition="smscdk-ray-mnp",
                num_nodes=1,
                ray_job_cmd="echo hi",
                out_s3="s3://bucket/out/",
                out_dir="/out",
                batch_client=mock_batch,
            )
        assert mock_batch.submit_job.call_args.kwargs["jobQueue"] == "smscdk-ray-standalone"
        # Reuses the SAME job definition passed in -- no new job type needed.
        assert mock_batch.submit_job.call_args.kwargs["jobDefinition"] == "smscdk-ray-mnp"

    def test_standalone_submission_falls_back_when_not_configured(self) -> None:
        """Empty (the real default) = no behavior change -- safe to deploy before
        the standalone queue exists in the target AWS account."""
        settings = self._settings(standalone_queue="")
        mock_batch = MagicMock()
        mock_batch.submit_job.return_value = {"jobId": "job-1"}
        service = SimulationServiceRay()
        with patch("viva_api.simulation.simulation_service_ray.get_settings", return_value=settings):
            service._submit_mnp(
                job_name="standalone-test",
                job_definition="smscdk-ray-mnp",
                num_nodes=1,
                ray_job_cmd="echo hi",
                out_s3="s3://bucket/out/",
                out_dir="/out",
                batch_client=mock_batch,
            )
        assert mock_batch.submit_job.call_args.kwargs["jobQueue"] == "smscdk-ray-mnp"

    def test_genuine_multi_node_submission_stays_on_mnp_queue_even_when_standalone_configured(self) -> None:
        """A REAL multi-node request (numNodes > 1, e.g. a colony sim) must never
        be rerouted -- it's exactly the case the placement group's low-latency
        inter-node networking still matters for. Guards against an over-broad
        routing condition swallowing genuine multi-node jobs."""
        settings = self._settings(standalone_queue="smscdk-ray-standalone")
        mock_batch = MagicMock()
        mock_batch.submit_job.return_value = {"jobId": "job-1"}
        service = SimulationServiceRay()
        with patch("viva_api.simulation.simulation_service_ray.get_settings", return_value=settings):
            service._submit_mnp(
                job_name="multinode-test",
                job_definition="smscdk-ray-mnp",
                num_nodes=4,
                ray_job_cmd="echo hi",
                out_s3="s3://bucket/out/",
                out_dir="/out",
                batch_client=mock_batch,
            )
        assert mock_batch.submit_job.call_args.kwargs["jobQueue"] == "smscdk-ray-mnp"


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


class TestParcaCommandNewGenes:
    """P0-2: a config that requests a real strain (parca_options.new_genes) must
    produce a ParCa command that actually carries the --new-genes flag."""

    def test_new_genes_flows_into_the_parca_command(self) -> None:
        service = SimulationServiceRay()
        with patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings):
            cmd = service._parca_command(new_genes="violacein")
        assert f"--new-genes {shlex.quote('violacein')}" in cmd

    @pytest.mark.parametrize("new_genes", [None, "off"])
    def test_new_genes_off_or_absent_omits_the_flag(self, new_genes: str | None) -> None:
        service = SimulationServiceRay()
        with patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings):
            cmd = service._parca_command(new_genes=new_genes)
        assert "--new-genes" not in cmd

    def test_new_genes_with_a_space_is_shell_quoted(self) -> None:
        service = SimulationServiceRay()
        with patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings):
            cmd = service._parca_command(new_genes="two genes")
        assert f"--new-genes {shlex.quote('two genes')}" in cmd


@pytest.mark.asyncio
class TestBatchExitCode:
    """P1-13: get_job_status must surface the Batch container exit code, not None."""

    async def test_exit_code_is_populated_from_container_exit_code(self) -> None:
        mock_batch = MagicMock()
        mock_batch.describe_jobs.return_value = {
            "jobs": [{"jobId": "sim-1", "status": "FAILED", "container": {"exitCode": 137}}]
        }
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
        ):
            info = await service.get_job_status(JobId.ray("sim-1"))
        assert info is not None
        assert info.exit_code == "137"

    async def test_exit_code_is_none_when_batch_reports_none(self) -> None:
        mock_batch = MagicMock()
        mock_batch.describe_jobs.return_value = {"jobs": [{"jobId": "sim-2", "status": "RUNNING"}]}
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
        ):
            info = await service.get_job_status(JobId.ray("sim-2"))
        assert info is not None
        assert info.exit_code is None


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


@pytest.mark.asyncio
class TestCancelChainCampaign:
    """cancel_chain_campaign (backlog item 71 Phase 4, folding in backlog item
    53's cancellation design): terminate every seed's CURRENT in-flight job,
    directly from chain_current_job_ids -- no dependsOn-chain walk needed
    under the per-seed model (at most one in-flight job per seed at any
    time). Reuses cancel_job's existing terminate_job call unchanged."""

    def _campaign(self, chain_current_job_ids: list[str | None]) -> HpcRun:
        return HpcRun(
            database_id=1,
            job_id=JobId.ray("parca-1"),
            correlation_id="chain-campaign-exp",
            job_type=JobType.SIMULATION,
            ref_id=1,
            status=JobStatus.RUNNING,
            chain_n_generations=3,
            chain_final_job_ids=[],
            chain_current_job_ids=chain_current_job_ids,
            chain_current_generation=[0] * len(chain_current_job_ids),
            chain_parca_done=True,
        )

    async def test_terminates_every_seeds_current_job(self) -> None:
        mock_batch = MagicMock()
        service = SimulationServiceRay()
        campaign = self._campaign(["s0g1", "s1g0", "s2g2"])
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
        ):
            await service.cancel_chain_campaign(campaign)

        assert mock_batch.terminate_job.call_count == 3
        terminated_ids = {call.kwargs["jobId"] for call in mock_batch.terminate_job.call_args_list}
        assert terminated_ids == {"s0g1", "s1g0", "s2g2"}

    async def test_skips_seeds_that_already_resolved(self) -> None:
        """A seed whose chain already resolved (its own slot already None --
        either it fully succeeded/failed on a prior tick, or it never even
        started) must not be touched -- idempotent, nothing to cancel."""
        mock_batch = MagicMock()
        service = SimulationServiceRay()
        campaign = self._campaign([None, "s1g0", None])
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
        ):
            await service.cancel_chain_campaign(campaign)

        mock_batch.terminate_job.assert_called_once()
        assert mock_batch.terminate_job.call_args.kwargs["jobId"] == "s1g0"

    async def test_empty_or_all_none_is_a_safe_noop(self) -> None:
        mock_batch = MagicMock()
        service = SimulationServiceRay()
        campaign = self._campaign([None, None])
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
        ):
            await service.cancel_chain_campaign(campaign)
        mock_batch.terminate_job.assert_not_called()


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

    def test_build_command_default_never_passes_the_g_flag(self) -> None:
        """Regression: item 87 added include_new_gene_data. Every existing caller must get
        a command byte-for-byte unaffected -- the recipe line must have NOTHING after
        -R us-gov-west-1, and the PAT must still be unset right after the outer clone."""
        service = SimulationServiceRay()
        with patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings):
            script = service._build_command(_v2ecoli_simulator())[2]
        assert " -g" not in script
        assert script.count("unset GH_PAT") == 1
        # GH_PAT is unset BEFORE the build recipe runs, not after -- confirms it's not left
        # exported into that command's environment when the flag is off.
        assert script.index("unset GH_PAT") < script.index("docker/build-and-push-ecr.sh")

    def test_build_command_include_new_gene_data_passes_g_and_keeps_pat_exported(self) -> None:
        service = SimulationServiceRay()
        with patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings):
            script = service._build_command(_v2ecoli_simulator(), include_new_gene_data=True)[2]
        assert "docker/build-and-push-ecr.sh -i abc1234 -r v2ecoli -R us-gov-west-1 -g" in script
        # GH_PAT must still be exported (not unset) by the time the recipe runs, or -g's own
        # `[[ -n "${GH_PAT:-}" ]]` guard in the recipe would fail even though this method
        # believes it's supplying one.
        assert "unset GH_PAT" not in script

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
        # PYTHONPATH=V2ECOLI_DIR (backlog item 93): ecoli_baseline.baseline()'s
        # injection branch does `from scripts._compare.inject import (...)`, a bare
        # absolute import that only resolves with the repo root on sys.path --
        # `python /tmp/run_pbg.py` alone puts /tmp there instead, regardless of cwd.
        assert "PYTHONPATH=/app/v2ecoli" in cmd
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

    def test_sim_command_batch_threads_injected_processes_swap(self) -> None:
        """CD2 native seam: a config carrying swap_processes must reach the
        --composite-id batch overrides as ecoli_baseline.baseline()'s own
        injected_processes kwarg, or the composite runs plain basal despite the
        requested metabolism-redux/violacein swap (depends on v2ecoli #640)."""
        service = SimulationServiceRay()
        injected = {
            "swap_processes": {"ecoli-metabolism": "ecoli-metabolism-redux"},
            "add_processes": [],
            "exclude_processes": [],
            "fork_repo": "",
        }
        with patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings):
            cmd = service._sim_command(
                n_seeds=2,
                n_steps=600,
                chunk=60,
                n_generations=3,
                experiment_id="cd2-swap",
                runner_s3_uri="s3://b/cd2-swap/run_pbg.py",
                injected_processes=injected,
            )
        tokens = shlex.split(cmd)
        overrides = json.loads(tokens[tokens.index("--overrides") + 1])
        assert overrides["injected_processes"] == injected
        # The swap survived into the native composite's own kwarg shape.
        assert overrides["injected_processes"]["swap_processes"] == {"ecoli-metabolism": "ecoli-metabolism-redux"}

    def test_sim_command_batch_threads_all_domain_fields(self) -> None:
        """variants/config_overrides/features/exchange_fluxes(+basis) are the
        remaining ecoli_baseline batch-mode kwargs -- each must reach --overrides
        when the config carries it."""
        service = SimulationServiceRay()
        with patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings):
            cmd = service._sim_command(
                n_seeds=1,
                n_steps=600,
                chunk=60,
                n_generations=2,
                experiment_id="cd2-full",
                runner_s3_uri="s3://b/cd2-full/run_pbg.py",
                variants={"grid": {"a": {"p.k": [1.0]}}},
                config_overrides={"ecoli-metabolism-redux.foo": 1},
                features=["exchange_flux"],
                exchange_fluxes={"GLC": "EX_glc__D_e"},
                exchange_flux_basis="mmol_per_gDCW_per_hr",
            )
        overrides = json.loads(shlex.split(cmd)[shlex.split(cmd).index("--overrides") + 1])
        assert overrides["variants"] == {"grid": {"a": {"p.k": [1.0]}}}
        assert overrides["config_overrides"] == {"ecoli-metabolism-redux.foo": 1}
        assert overrides["features"] == ["exchange_flux"]
        assert overrides["exchange_fluxes"] == {"GLC": "EX_glc__D_e"}
        assert overrides["exchange_flux_basis"] == "mmol_per_gDCW_per_hr"

    def test_sim_command_batch_no_domain_fields_is_byte_for_byte_unchanged(self) -> None:
        """Regression guard: a config with no swap/variant intent produces the
        exact overrides dict this path built before threading was added -- no
        stray domain keys leak in."""
        service = SimulationServiceRay()
        with patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings):
            cmd = service._sim_command(
                n_seeds=2,
                n_steps=600,
                chunk=60,
                n_generations=3,
                experiment_id="plain",
                runner_s3_uri="s3://b/plain/run_pbg.py",
            )
        overrides = json.loads(shlex.split(cmd)[shlex.split(cmd).index("--overrides") + 1])
        assert overrides == {
            "n_seeds": 2,
            "n_generations": 3,
            "cache_dir": PARCA_CACHE_DIR,
            "out_dir": SIM_OUT_DIR,
            "experiment_id": "plain",
            "analyses": "none",
            "parallel": "ray",
        }

    def test_sim_command_batch_flux_basis_omitted_without_flux_map(self) -> None:
        """exchange_flux_basis only matters alongside a flux map -- it is omitted
        when no exchange_fluxes are supplied (composite defaults it to '')."""
        service = SimulationServiceRay()
        with patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings):
            cmd = service._sim_command(
                n_seeds=1,
                n_steps=600,
                chunk=60,
                n_generations=2,
                experiment_id="no-flux",
                runner_s3_uri="s3://b/no-flux/run_pbg.py",
                exchange_flux_basis="mmol_per_gDCW_per_hr",
            )
        overrides = json.loads(shlex.split(cmd)[shlex.split(cmd).index("--overrides") + 1])
        assert "exchange_flux_basis" not in overrides
        assert "exchange_fluxes" not in overrides

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
        # Backlog item 93's sys.path fix -- see the sibling assertion above for why.
        assert "PYTHONPATH=/app/v2ecoli" in cmd
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

    def test_injected_processes_and_variants_omitted_when_absent(self) -> None:
        """Backlog item 93 regression: a caller that doesn't pass
        injected_processes/variants (every caller before this item, and the
        single-generation phase0 path today) builds the exact same overrides
        dict as before these params existed -- no new keys leak in."""
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
        ):
            cmd = service._seed_generation_command(
                seed=0,
                generation_index=0,
                experiment_id="exp-no-injection",
                runner_s3_uri="s3://mybucket/vecoli-output/exp-no-injection/run_pbg.py",
            )
        overrides = self._overrides(cmd)
        assert "injected_processes" not in overrides
        assert "variants" not in overrides

    def test_injected_processes_and_variants_forwarded_when_present(self) -> None:
        """Backlog item 93: the actual fix -- when a caller (JobScheduler, via
        injected_processes_from_config) passes these through, they land in
        the overrides dict verbatim, using ecoli_baseline.baseline()'s own
        real kwarg names."""
        service = SimulationServiceRay()
        injected = {
            "swap_processes": {"ecoli-metabolism": "ecoli-metabolism-redux"},
            "add_processes": [],
            "exclude_processes": ["exchange_data"],
            "fork_repo": "",
        }
        variants = {"strain_design": {"perturbations": {"value": [{"EG11005": 0.0}]}}}
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
        ):
            cmd = service._seed_generation_command(
                seed=0,
                generation_index=0,
                experiment_id="exp-run4",
                runner_s3_uri="s3://mybucket/vecoli-output/exp-run4/run_pbg.py",
                injected_processes=injected,
                variants=variants,
            )
        overrides = self._overrides(cmd)
        assert overrides["injected_processes"] == injected
        assert overrides["variants"] == variants

    def test_composite_id_defaults_to_baseline_when_absent(self) -> None:
        """Backlog item 105: a caller that doesn't pass composite_id (every
        caller before this item) still gets V2ECOLI_BATCH_BASELINE_COMPOSITE_ID
        -- no behavior change for existing callers."""
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
        ):
            cmd = service._seed_generation_command(
                seed=0,
                generation_index=0,
                experiment_id="exp-default-composite",
                runner_s3_uri="s3://mybucket/vecoli-output/exp-default-composite/run_pbg.py",
            )
        assert "--composite-id v2ecoli.composites.ecoli_baseline.ecoli_baseline " in cmd

    def test_composite_id_override_replaces_default_when_present(self) -> None:
        """Backlog item 105: the actual fix -- chain-dispatch was previously
        hardcoded to ecoli_baseline only. A caller-supplied composite_id (e.g.
        reactor_bird_coupled, now that v2ecoli #648 gives it the same
        injected_processes/variants shape) replaces the default entirely."""
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
        ):
            cmd = service._seed_generation_command(
                seed=0,
                generation_index=0,
                experiment_id="exp-reactor-bird-coupled",
                runner_s3_uri="s3://mybucket/vecoli-output/exp-reactor-bird-coupled/run_pbg.py",
                composite_id="v2ecoli.composites.reactor_bird_coupled.reactor_bird_coupled",
            )
        assert "--composite-id v2ecoli.composites.reactor_bird_coupled.reactor_bird_coupled " in cmd
        assert "ecoli_baseline" not in cmd

    def test_stop_at_division_is_always_set(self) -> None:
        """Backlog item 103: without this, n_seeds=1/n_generations=1/no
        stop_at_division makes ecoli_baseline.baseline()'s own dispatch gate
        (n_seeds>1 or n_generations>1 or stop_at_division) evaluate False on
        EVERY chain-dispatch generation, routing through the plain,
        non-division-gated single-cell build (the composite's own docs call it
        "NO division-stop") -- confirmed empirically in real campaign 171
        production output: generation 0/5/9 of the same lineage were
        MD5-identical files, global_time never exceeded 1.0 across 10 chained
        "generations". Unconditional, not caller-controlled -- there is no
        legitimate chain-dispatch generation that should NOT stop at division."""
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
        ):
            cmd = service._seed_generation_command(
                seed=0,
                generation_index=0,
                experiment_id="exp-division-gate",
                runner_s3_uri="s3://mybucket/vecoli-output/exp-division-gate/run_pbg.py",
            )
        overrides = self._overrides(cmd)
        assert overrides["stop_at_division"] is True


class TestInjectedProcessesFromConfig:
    """injected_processes_from_config (backlog item 93): the shared helper
    JobScheduler uses to turn a legacy config's swap_processes/add_processes/
    exclude_processes into ecoli_baseline.baseline()'s own injected_processes
    kwarg shape -- confirmed against v2ecoli's real consumer
    (composites/ecoli_baseline.py:2019-2042, scripts/_compare/inject.py's
    resolve_injections/assert_injection_sourcing) before this was written."""

    def test_none_when_nothing_set(self) -> None:
        config = SimpleNamespace()
        assert injected_processes_from_config(config) is None

    def test_none_when_fields_present_but_empty(self) -> None:
        """swap_processes={}/add_processes=[]/exclude_processes=[] are the
        real SimulationConfig/ExperimentRequest defaults (models.py) -- an
        ordinary dispatch with no injection intent must still resolve to
        None, not an empty-but-truthy injected_processes dict."""
        config = SimpleNamespace(swap_processes={}, add_processes=[], exclude_processes=[])
        assert injected_processes_from_config(config) is None

    def test_builds_the_real_baseline_kwarg_shape_from_swap_processes_alone(self) -> None:
        """Run 4's own real config (fss_pathway_oe_native_oe_carina.json) sets
        ONLY swap_processes -- add_processes/exclude_processes[minus this one
        real field] are absent entirely, not just empty, so getattr's default
        must cover the missing-attribute case too, not just falsy-but-present."""
        config = SimpleNamespace(swap_processes={"ecoli-metabolism": "ecoli-metabolism-redux"})
        result = injected_processes_from_config(config)
        assert result == {
            "swap_processes": {"ecoli-metabolism": "ecoli-metabolism-redux"},
            "add_processes": [],
            "exclude_processes": [],
            "fork_repo": "",
        }

    def test_fork_repo_always_empty_native_path(self) -> None:
        """Every caller of this helper dispatches through ecoli_baseline (the
        native, fork-free composite) -- fork_repo must always come back
        empty-string, matching assert_injection_sourcing's own native-path
        rule (a non-empty fork_repo there is a hard caller error)."""
        config = SimpleNamespace(add_processes=["some_new_process"])
        result = injected_processes_from_config(config)
        assert result is not None
        assert result["fork_repo"] == ""

    def test_reads_nested_injected_processes_block(self) -> None:
        """viva-api#385 regression: a caller may pass the whole injected_processes
        block as an extra (extra_params={"injected_processes": {...}}) -- the shape
        run_comparison_ensemble.py --from-vecoli-config emits. The helper must read
        the nested block, not only the flat top-level fields; otherwise the swap is
        silently dropped and chain-dispatch runs wild-type while reporting success."""
        config = SimpleNamespace(
            injected_processes={
                "fork_repo": "",
                "swap_processes": {"ecoli-metabolism": "ecoli-metabolism-redux"},
                "cache_dir": "/app/v2ecoli/out/cache",
            }
        )
        result = injected_processes_from_config(config)
        assert result == {
            "swap_processes": {"ecoli-metabolism": "ecoli-metabolism-redux"},
            "add_processes": [],
            "exclude_processes": [],
            "fork_repo": "",
        }

    def test_nested_block_without_intent_falls_through_to_flat(self) -> None:
        """A nested block carrying no swap/add/exclude intent (e.g. only fork_repo)
        must not shadow flat top-level fields."""
        config = SimpleNamespace(
            injected_processes={"fork_repo": ""},
            swap_processes={"ecoli-metabolism": "ecoli-metabolism-redux"},
        )
        result = injected_processes_from_config(config)
        assert result is not None
        assert result["swap_processes"] == {"ecoli-metabolism": "ecoli-metabolism-redux"}


class TestIsUpstreamVecoli:
    """The single routing predicate shared by submit_ecoli_simulation_job and _sim_command."""

    def test_only_vecoli_is_upstream(self) -> None:
        from viva_api.simulation.simulation_service_ray import _is_upstream_vecoli

        assert _is_upstream_vecoli("vecoli") is True
        assert _is_upstream_vecoli("v2ecoli") is False
        assert _is_upstream_vecoli(None) is False


class TestUpstreamParcaCommand:
    """_upstream_parca_command / _upstream_cache_s3_uri (item 87): the vecoli
    reference-arm ParCa build/cache path gains an optional config-driven,
    non-colliding variant -- every existing caller (config_path/variant both
    None) must be provably unaffected."""

    def test_no_config_path_is_byte_identical_to_before(self) -> None:
        service = SimulationServiceRay()
        assert service._upstream_parca_command() == (
            f"cd {V2ECOLI_DIR} && python scripts/build_upstream_parca.py"
            f" --outdir {V2ECOLI_DIR}/out/upstream --cpus 1"
            f" --copy-to {PARCA_CACHE_DIR}"
        )

    def test_config_path_appends_the_config_flag(self) -> None:
        service = SimulationServiceRay()
        cmd = service._upstream_parca_command(config_path=f"{V2ECOLI_DIR}/configs/custom_strain.json")
        assert cmd.endswith(f" --config {V2ECOLI_DIR}/configs/custom_strain.json")
        # Everything before it is unchanged -- confirms this is a pure append,
        # not a differently-ordered command that happens to contain the flag.
        assert cmd.startswith(service._upstream_parca_command())

    def test_cache_uri_no_variant_is_byte_identical_to_before(self) -> None:
        service = SimulationServiceRay()
        with patch("viva_api.common.storage.data_layout.get_settings", _ray_settings):
            assert service._upstream_cache_s3_uri("abc123") == "s3://mybucket/ray-upstream-parca-cache/abc123/"

    def test_cache_uri_variant_never_collides_with_bare_commit_path(self) -> None:
        """The real hazard this whole feature guards against: a config-driven
        cache must never land where a plain baseline build/stage would read."""
        service = SimulationServiceRay()
        with patch("viva_api.common.storage.data_layout.get_settings", _ray_settings):
            bare = service._upstream_cache_s3_uri("abc123")
            variant = service._upstream_cache_s3_uri("abc123", variant="custom-strain")
        assert variant != bare
        assert variant == "s3://mybucket/ray-upstream-parca-cache/abc123/custom-strain/"


class TestParcaCommand:
    """_parca_command's own flag-assembly, in isolation -- no DB/AWS needed.

    Backlog item 104 (sms-ecoli#184 / viva-api#365, cplong90): bundle_overrides
    survived on the stored request but was never forwarded here, so ParCa built
    from defaults only. Mirrors item 93's own new_genes regression-guard shape
    (byte-identical when unset; the real flag when set)."""

    def test_no_options_is_byte_identical_to_before(self) -> None:
        service = SimulationServiceRay()
        with patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings):
            cmd = service._parca_command()
        assert "--new-genes" not in cmd
        assert "--bundle-overrides" not in cmd
        assert cmd == (
            f"cd {V2ECOLI_DIR}"
            f" && v2ecoli-parca --mode fast --cpus 8"
            f" -o {PARCA_SIMDATA_DIR} --cache-dir {PARCA_CACHE_DIR}"
            f" && gzip -f -k {PARCA_SIMDATA_DIR}/parca_state.pkl"
            f" && python scripts/build_cache.py"
            f" --fixture {PARCA_SIMDATA_DIR}/parca_state.pkl.gz --cache {PARCA_CACHE_DIR}"
            f" && cp {PARCA_SIMDATA_DIR}/parca_state.pkl.gz {PARCA_CACHE_DIR}/parca_state.pkl.gz"
        )

    def test_preserves_raw_fitted_state_for_new_gene_cache_consumption(self) -> None:
        """Backlog item 105: the raw parca_state.pkl.gz must ride along in the
        synced PARCA_CACHE_DIR -- build_new_gene_cache.py needs exactly this
        file, and PARCA_SIMDATA_DIR (where it's first produced) is never
        synced anywhere and is discarded with the job's container."""
        service = SimulationServiceRay()
        with patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings):
            cmd = service._parca_command()
        assert f"cp {PARCA_SIMDATA_DIR}/parca_state.pkl.gz {PARCA_CACHE_DIR}/parca_state.pkl.gz" in cmd
        # comes after the hydration step, not before -- gzip must exist first
        assert cmd.index("build_cache.py") < cmd.index(f"cp {PARCA_SIMDATA_DIR}")

    def test_off_new_genes_is_byte_identical_to_unset(self) -> None:
        """ "off" is v2ecoli-parca's own --new-genes default -- passing it explicitly
        must not append a redundant flag."""
        service = SimulationServiceRay()
        with patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings):
            assert service._parca_command(new_genes="off") == service._parca_command()

    def test_bundle_overrides_appends_the_flag(self) -> None:
        service = SimulationServiceRay()
        with patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings):
            cmd = service._parca_command(bundle_overrides="models/parca/composed_overlay.tsv")
        assert "--bundle-overrides models/parca/composed_overlay.tsv" in cmd
        assert "--new-genes" not in cmd

    def test_new_genes_and_bundle_overrides_both_append(self) -> None:
        service = SimulationServiceRay()
        with patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings):
            cmd = service._parca_command(
                new_genes="violacein_MG1655_M5", bundle_overrides="models/parca/composed_overlay.tsv"
            )
        assert "--new-genes violacein_MG1655_M5 --bundle-overrides models/parca/composed_overlay.tsv" in cmd


class TestCacheS3UriVariant:
    """cache_s3_uri's new `variant` kwarg (backlog item 105) -- mirrors
    _upstream_cache_s3_uri's already-shipped item-87 pattern exactly."""

    def test_omitted_variant_is_byte_identical_to_before(self) -> None:
        service = SimulationServiceRay()
        with patch("viva_api.common.storage.data_layout.get_settings", _ray_settings):
            assert service.cache_s3_uri("abc1234") == service.cache_s3_uri("abc1234", variant=None)
        assert "abc1234" in service.cache_s3_uri("abc1234")

    def test_variant_nests_under_a_non_colliding_key(self) -> None:
        service = SimulationServiceRay()
        with patch("viva_api.common.storage.data_layout.get_settings", _ray_settings):
            plain = service.cache_s3_uri("abc1234")
            variant = service.cache_s3_uri("abc1234", variant="k4-induced")
        assert variant != plain
        assert variant.startswith(plain)
        assert "k4-induced" in variant


class TestBuildNewGeneCacheCommand:
    """_build_new_gene_cache_command's own flag-assembly (backlog item 105) --
    answers Chris/cplong90's own reachability question (sms-ecoli#166) for
    scripts/build_new_gene_cache.py, the induction-level "other half" of
    ParCa's own new_genes presence/absence flag."""

    def test_required_flags_only(self) -> None:
        service = SimulationServiceRay()
        cmd = service._build_new_gene_cache_command(expression=1e6, translation_efficiency=1.0)
        assert cmd == (
            f"cd {V2ECOLI_DIR}"
            f" && python scripts/build_new_gene_cache.py"
            f" --state {PARCA_CACHE_DIR}/parca_state.pkl.gz"
            f" --cache {NEW_GENE_INDUCED_CACHE_DIR}"
            f" --expression 1000000.0 --translation-efficiency 1.0"
            f" --seed 0"
        )

    def test_optional_flags_all_append(self) -> None:
        service = SimulationServiceRay()
        cmd = service._build_new_gene_cache_command(
            expression=1e6,
            translation_efficiency=1.0,
            rel_exp_adj="1,2,4",
            rel_trl_eff_adj="1,1,1",
            seed=7,
            media_condition="basal",
            fixed_media="minimal_plus_amino_acids",
        )
        assert "--rel-exp-adj 1,2,4" in cmd
        assert "--rel-trl-eff-adj 1,1,1" in cmd
        assert "--seed 7" in cmd
        assert "--media-condition basal" in cmd
        assert "--fixed-media minimal_plus_amino_acids" in cmd

    def test_reads_the_raw_state_parca_command_preserves(self) -> None:
        """The --state path this command reads must be exactly the path
        _parca_command's own new cp step writes to -- the two are a matched
        pair across two separate job submissions with no other hand-off."""
        service = SimulationServiceRay()
        with patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings):
            parca_cmd = service._parca_command()
        cache_cmd = service._build_new_gene_cache_command(expression=1.0, translation_efficiency=1.0)
        written_path = f"{PARCA_CACHE_DIR}/parca_state.pkl.gz"
        assert written_path in parca_cmd
        assert f"--state {written_path}" in cache_cmd


@pytest.mark.asyncio
class TestSubmitNewGeneCacheJob:
    """submit_new_gene_cache_job (backlog item 105): sibling of
    submit_parca_job, composing a caller-chosen induction level on top of an
    already-built commit cache instead of producing one from scratch --
    hence the extra stage_s3/stage_dir (submit_parca_job has none)."""

    async def test_submits_via_the_container_path_with_stage_in(self) -> None:
        mock_batch = _fake_container_batch(["new-gene-cache-999"])
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
        ):
            job_id = await service.submit_new_gene_cache_job(
                commit="abc1234",
                variant="k4-induced",
                expression=1e6,
                translation_efficiency=1.0,
            )
        assert job_id == JobId.ray("new-gene-cache-999")
        call = mock_batch.submit_job.call_args
        assert "containerOverrides" in call.kwargs
        env = _container_env_of(call)
        assert "build_new_gene_cache.py" in env["CONTAINER_JOB_CMD"]
        # stages FROM the plain commit cache (source), writes TO the variant
        # cache (derived) -- must never be the same key (RayLayout.parca_cache_uri's
        # own docstring: writing the bare commit-only path would silently
        # corrupt every other concurrent dispatch on that commit).
        assert env["CONTAINER_STAGE_S3"] != env["CONTAINER_OUT_S3"]
        assert "k4-induced" in env["CONTAINER_OUT_S3"]
        assert "k4-induced" not in env["CONTAINER_STAGE_S3"]


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


class TestEnsureContainerJobDef:
    """Per-commit container job-def derivation (backlog item 71) -- mirrors
    TestEnsureMnpJobDef for the plain (non-MNP, non-array) container job shape."""

    def test_reuses_existing_revision_for_same_image(self) -> None:
        image = "476270107793.dkr.ecr.us-gov-west-1.amazonaws.com/v2ecoli:abc1234"
        mock_batch = MagicMock()
        mock_batch.describe_job_definitions.return_value = {
            "jobDefinitions": [{"revision": 5, "containerProperties": {"image": image}}]
        }
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _container_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
        ):
            jd = service._ensure_container_job_def(image, "abc1234")
        assert jd == "smscdk-ray-container-abc1234:5"
        mock_batch.register_job_definition.assert_not_called()

    def test_registers_new_revision_cloning_base_container_properties(self) -> None:
        image = "476270107793.dkr.ecr.us-gov-west-1.amazonaws.com/v2ecoli:def5678"
        mock_batch = _fake_container_batch([])
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _container_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
        ):
            jd = service._ensure_container_job_def(image, "def5678")
        assert jd == "smscdk-ray-container-def5678:1"
        registered = mock_batch.register_job_definition.call_args.kwargs
        assert registered["type"] == "container"
        assert registered["containerProperties"]["image"] == image
        assert registered["containerProperties"]["vcpus"] == 16  # cloned from the base, not dropped

    def test_raises_clearly_when_job_definition_setting_unset(self) -> None:
        """Empty (the real default) must fail loud with the setting name, not
        submit a doomed job with a blank job-def name (compose_ray_image_tag's
        own precedent in this file)."""
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            pytest.raises(RuntimeError, match="ray_container_job_definition"),
        ):
            service._ensure_container_job_def("some-image", "abc1234")


class TestSubmitContainer:
    """_submit_container: the plain container-type submission path (backlog item
    71) -- sibling of _submit_mnp with no node overrides and the CONTAINER_* env
    contract docker/batch-container-entrypoint.sh (sms-ecoli) expects."""

    def test_submits_with_container_env_and_queue_no_node_overrides(self) -> None:
        mock_batch = MagicMock()
        mock_batch.submit_job.return_value = {"jobId": "job-1"}
        service = SimulationServiceRay()
        with patch("viva_api.simulation.simulation_service_ray.get_settings", _container_settings):
            job_id = service._submit_container(
                job_name="container-test",
                job_definition="smscdk-ray-container-abc:1",
                job_cmd="echo hi",
                out_s3="s3://bucket/out/",
                out_dir="/out",
                batch_client=mock_batch,
            )
        assert job_id == "job-1"
        call = mock_batch.submit_job.call_args
        assert call.kwargs["jobQueue"] == "smscdk-ray-standalone"
        assert call.kwargs["jobDefinition"] == "smscdk-ray-container-abc:1"
        assert "nodeOverrides" not in call.kwargs
        env = _container_env_of(call)
        assert env["CONTAINER_JOB_CMD"] == "echo hi"
        assert env["CONTAINER_OUT_DIR"] == "/out"
        assert env["CONTAINER_OUT_S3"] == "s3://bucket/out/"
        assert "RAY_JOB_CMD" not in env
        assert "ARRAY_JOB_CMD" not in env

    def test_stage_vars_only_present_when_both_configured(self) -> None:
        mock_batch = MagicMock()
        mock_batch.submit_job.return_value = {"jobId": "job-1"}
        service = SimulationServiceRay()
        with patch("viva_api.simulation.simulation_service_ray.get_settings", _container_settings):
            service._submit_container(
                job_name="container-test",
                job_definition="jd:1",
                job_cmd="echo hi",
                out_s3="s3://bucket/out/",
                out_dir="/out",
                stage_s3="s3://bucket/cache/",
                stage_dir="/cache",
                batch_client=mock_batch,
            )
        env = _container_env_of(mock_batch.submit_job.call_args)
        assert env["CONTAINER_STAGE_S3"] == "s3://bucket/cache/"
        assert env["CONTAINER_STAGE_DIR"] == "/cache"

    def test_stage_vars_absent_when_only_one_of_the_pair_is_set(self) -> None:
        mock_batch = MagicMock()
        mock_batch.submit_job.return_value = {"jobId": "job-1"}
        service = SimulationServiceRay()
        with patch("viva_api.simulation.simulation_service_ray.get_settings", _container_settings):
            service._submit_container(
                job_name="container-test",
                job_definition="jd:1",
                job_cmd="echo hi",
                out_s3="s3://bucket/out/",
                out_dir="/out",
                stage_s3="s3://bucket/cache/",
                stage_dir=None,
                batch_client=mock_batch,
            )
        env = _container_env_of(mock_batch.submit_job.call_args)
        assert "CONTAINER_STAGE_S3" not in env
        assert "CONTAINER_STAGE_DIR" not in env

    def test_raises_clearly_when_queue_setting_unset(self) -> None:
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            pytest.raises(RuntimeError, match="ray_container_queue"),
        ):
            service._submit_container(
                job_name="x", job_definition="jd:1", job_cmd="echo hi", out_s3="s3://b/", out_dir="/o"
            )

    def test_depends_on_and_tags_pass_through(self) -> None:
        mock_batch = MagicMock()
        mock_batch.submit_job.return_value = {"jobId": "job-2"}
        service = SimulationServiceRay()
        with patch("viva_api.simulation.simulation_service_ray.get_settings", _container_settings):
            service._submit_container(
                job_name="container-test",
                job_definition="jd:1",
                job_cmd="echo hi",
                out_s3="s3://bucket/out/",
                out_dir="/out",
                depends_on=["parca-job-1"],
                tags={"Project": "v2ecoli"},
                batch_client=mock_batch,
            )
        call = mock_batch.submit_job.call_args
        assert call.kwargs["dependsOn"] == [{"jobId": "parca-job-1", "type": "SEQUENTIAL"}]
        assert call.kwargs["tags"] == {"Project": "v2ecoli"}
        assert call.kwargs["propagateTags"] is True


@pytest.mark.asyncio
class TestSubmitParcaJob:
    """submit_parca_job (backlog item 71): migrated from a 1-node MNP job to the
    plain container-type path -- ParCa has no real inter-node traffic to protect."""

    async def test_submits_via_the_container_path(self) -> None:
        from viva_api.simulation.models import ParcaDataset, ParcaDatasetRequest, ParcaOptions, SimulatorVersion

        parca_dataset = ParcaDataset(
            database_id=1,
            parca_dataset_request=ParcaDatasetRequest(
                simulator_version=SimulatorVersion(
                    database_id=1, git_commit_hash="abc1234", git_branch="main", git_repo_url="https://github.com/x/y"
                ),
                parca_config=ParcaOptions(),
            ),
        )
        mock_batch = _fake_container_batch(["parca-999"])
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
        ):
            job_id = await service.submit_parca_job(parca_dataset)
        assert job_id == JobId.ray("parca-999")
        call = mock_batch.submit_job.call_args
        assert "containerOverrides" in call.kwargs
        assert "nodeOverrides" not in call.kwargs
        env = _container_env_of(call)
        assert "v2ecoli-parca" in env["CONTAINER_JOB_CMD"]


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
    """submit_chain_dispatch_job (backlog item 71 Phase 4 rework): submits
    ONLY ParCa now, as a container-type job, and writes the campaign's
    initial per-seed tracking row (every slot empty, gated on ParCa) --
    generation submission moves entirely to JobScheduler's poll loop (see
    TestAdvanceChainCampaign, tests/simulation/test_scheduler.py, and
    TestSubmitChainGeneration/TestCancelChainCampaign below for those)."""

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

    async def test_submits_only_parca_as_a_container_job(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        setattr(experiment_request.config, "n_init_sims", 3)  # noqa: B010
        experiment_request.config.generations = 4
        simulation = await database_service.insert_simulation(sim_request=experiment_request)
        mock_batch = _fake_container_batch(["parca-1"])

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
        ):
            job_id = await service.submit_chain_dispatch_job(
                ecoli_simulation=simulation, database_service=database_service
            )

        assert job_id == JobId.ray("parca-1")
        # Exactly ONE submission -- no per-seed generation jobs anymore.
        assert mock_batch.submit_job.call_count == 1
        (parca_call,) = mock_batch.submit_job.call_args_list
        assert "dependsOn" not in parca_call.kwargs
        assert "containerOverrides" in parca_call.kwargs  # container-type, not MNP nodeOverrides
        env = _container_env_of(parca_call)
        assert "v2ecoli-parca" in env["CONTAINER_JOB_CMD"]
        assert "--new-genes" not in env["CONTAINER_JOB_CMD"]  # regression: absent when not set
        assert "--bundle-overrides" not in env["CONTAINER_JOB_CMD"]  # regression: absent when not set

    async def test_forwards_bundle_overrides_to_parca_when_config_sets_it(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """Backlog item 104 (sms-ecoli#184 / viva-api#365, cplong90): parca_options.bundle_overrides
        survived on the stored request but was never forwarded to the ParCa command chain-dispatch
        actually submits, so v2ecoli-parca built from defaults only and any keys the overrides
        manifest supplies were absent -- same class of gap as item 93's new_genes fix, missed in
        that pass."""
        setattr(experiment_request.config, "n_init_sims", 2)  # noqa: B010
        experiment_request.config.generations = 3
        setattr(  # noqa: B010
            experiment_request.config.parca_options, "bundle_overrides", "models/parca/composed_overlay.tsv"
        )
        simulation = await database_service.insert_simulation(sim_request=experiment_request)
        mock_batch = _fake_container_batch(["parca-1"])

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
        ):
            await service.submit_chain_dispatch_job(ecoli_simulation=simulation, database_service=database_service)

        (parca_call,) = mock_batch.submit_job.call_args_list
        env = _container_env_of(parca_call)
        assert "--bundle-overrides models/parca/composed_overlay.tsv" in env["CONTAINER_JOB_CMD"]

    async def test_forwards_new_genes_to_parca_when_config_sets_it(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """Backlog item 93: the real gap a live Run 4 smoke dispatch found --
        parca_options.new_genes (a real SimulationConfig field, matches
        v2ecoli-parca's own --new-genes SUBDIR flag) must reach the ParCa
        command chain-dispatch actually submits, not just the composite's own
        overrides -- without a violacein-aware simData, the sim can't secrete
        violacein regardless of any other fix."""
        setattr(experiment_request.config, "n_init_sims", 2)  # noqa: B010
        experiment_request.config.generations = 3
        setattr(experiment_request.config.parca_options, "new_genes", "violacein_MG1655_M5")  # noqa: B010
        simulation = await database_service.insert_simulation(sim_request=experiment_request)
        mock_batch = _fake_container_batch(["parca-1"])

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
        ):
            await service.submit_chain_dispatch_job(ecoli_simulation=simulation, database_service=database_service)

        (parca_call,) = mock_batch.submit_job.call_args_list
        env = _container_env_of(parca_call)
        assert "--new-genes violacein_MG1655_M5" in env["CONTAINER_JOB_CMD"]

    async def test_writes_initial_campaign_row_with_empty_per_seed_state(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        setattr(experiment_request.config, "n_init_sims", 3)  # noqa: B010
        experiment_request.config.generations = 4
        simulation = await database_service.insert_simulation(sim_request=experiment_request)
        mock_batch = _fake_container_batch(["parca-1"])

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
        ):
            await service.submit_chain_dispatch_job(ecoli_simulation=simulation, database_service=database_service)

        active_campaigns = [
            c for c in await database_service.list_active_chain_campaigns() if c.ref_id == simulation.database_id
        ]
        assert len(active_campaigns) == 1
        campaign = active_campaigns[0]
        assert campaign.job_id == JobId.ray("parca-1")
        assert campaign.chain_n_generations == 4
        assert campaign.chain_final_job_ids == []
        assert campaign.chain_current_job_ids == [None, None, None]
        assert campaign.chain_current_generation == [None, None, None]
        assert campaign.chain_parca_done is False

    async def test_single_seed_multi_generation_is_allowed(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """Unlike the array-job design predating item 33, n_seeds >= 2 is NOT
        required -- no AWS Batch array-size floor applies here at all (every
        seed's chain is independent standalone container jobs)."""
        setattr(experiment_request.config, "n_init_sims", 1)  # noqa: B010
        experiment_request.config.generations = 2
        simulation = await database_service.insert_simulation(sim_request=experiment_request)
        mock_batch = _fake_container_batch(["parca-1"])
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
        ):
            job_id = await service.submit_chain_dispatch_job(
                ecoli_simulation=simulation, database_service=database_service
            )
        assert job_id == JobId.ray("parca-1")
        active_campaigns = [
            c for c in await database_service.list_active_chain_campaigns() if c.ref_id == simulation.database_id
        ]
        assert active_campaigns[0].chain_current_job_ids == [None]


@pytest.mark.asyncio
class TestSubmitChainGeneration:
    """submit_chain_generation/submit_chain_generation_batch (backlog item 71
    Phase 4): submit ONE seed's ONE generation as a standalone container job,
    no depends_on -- JobScheduler decides WHEN to call these, not native Batch
    dependency resolution. Locks the exact deterministic S3 paths + tag shape
    unchanged from the superseded design, on the new container job type."""

    async def test_submit_chain_generation_builds_the_right_command_and_shape(self) -> None:
        mock_batch = _fake_container_batch(["s2g1"])
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
        ):
            job_id = service.submit_chain_generation(
                seed=2,
                generation_index=1,
                experiment_id="exp-1",
                commit="abc1234",
                cache_s3="s3://mybucket/cache/abc1234",
                runner_s3_uri="s3://mybucket/runner/run_pbg.py",
                tags={"Project": "v2ecoli-comparison", "Phase": "sim"},
            )

        assert job_id == "s2g1"
        (call,) = mock_batch.submit_job.call_args_list
        assert "dependsOn" not in call.kwargs  # app-level gating -- no native Batch dependency at all
        assert call.kwargs["tags"]["Seed"] == "2"
        assert call.kwargs["tags"]["Generation"] == "1"
        env = _container_env_of(call)
        tokens = shlex.split(env["CONTAINER_JOB_CMD"])
        overrides = json.loads(tokens[tokens.index("--overrides") + 1])
        assert overrides["seed"] == 2
        assert overrides["initial_generation_index"] == 1
        assert (
            overrides["initial_carry_state_path"] == "s3://mybucket/vecoli-output/exp-1/daughter-state/seed2/gen0.pkl"
        )
        assert overrides["daughter_state_out_path"] == "s3://mybucket/vecoli-output/exp-1/daughter-state/seed2/gen1.pkl"

    async def test_submit_chain_generation_forwards_injected_processes_and_variants(self) -> None:
        """Backlog item 93: JobScheduler passes these through on every seed's
        every generation (re-derived from Simulation.config each tick) --
        confirms submit_chain_generation threads them into the real overrides
        payload rather than dropping them at this layer."""
        mock_batch = _fake_container_batch(["s0g0"])
        injected = {
            "swap_processes": {"ecoli-metabolism": "ecoli-metabolism-redux"},
            "add_processes": [],
            "exclude_processes": ["exchange_data"],
            "fork_repo": "",
        }
        variants = {"strain_design": {"perturbations": {"value": [{"EG11005": 0.0}]}}}
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
        ):
            service.submit_chain_generation(
                seed=0,
                generation_index=0,
                experiment_id="exp-run4",
                commit="abc1234",
                cache_s3="s3://mybucket/cache/abc1234",
                runner_s3_uri="s3://mybucket/runner/run_pbg.py",
                tags={"Project": "v2ecoli-comparison"},
                injected_processes=injected,
                variants=variants,
            )
        (call,) = mock_batch.submit_job.call_args_list
        env = _container_env_of(call)
        tokens = shlex.split(env["CONTAINER_JOB_CMD"])
        overrides = json.loads(tokens[tokens.index("--overrides") + 1])
        assert overrides["injected_processes"] == injected
        assert overrides["variants"] == variants

    async def test_submit_chain_generation_forwards_composite_id(self) -> None:
        """Backlog item 105: JobScheduler passes composite_id through on every
        seed's every generation (re-derived from Simulation.config each tick,
        same pattern as injected_processes/variants) -- confirms
        submit_chain_generation threads it into the real --composite-id flag
        rather than dropping it at this layer."""
        mock_batch = _fake_container_batch(["s0g0"])
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
        ):
            service.submit_chain_generation(
                seed=0,
                generation_index=0,
                experiment_id="exp-run1-k4",
                commit="abc1234",
                cache_s3="s3://mybucket/cache/abc1234",
                runner_s3_uri="s3://mybucket/runner/run_pbg.py",
                tags={"Project": "v2ecoli-comparison"},
                composite_id="v2ecoli.composites.reactor_bird_coupled.reactor_bird_coupled",
            )
        (call,) = mock_batch.submit_job.call_args_list
        env = _container_env_of(call)
        assert (
            "--composite-id v2ecoli.composites.reactor_bird_coupled.reactor_bird_coupled " in env["CONTAINER_JOB_CMD"]
        )

    async def test_batch_forwards_injected_processes_and_variants_to_every_seed(self) -> None:
        """Backlog item 93: submit_chain_generation_batch's own fan-out loop
        (the generation-0 burst) must pass the SAME injected_processes/
        variants to every seed -- one campaign, one config."""
        mock_batch = _fake_container_batch(["s0g0", "s1g0"])
        injected = {
            "swap_processes": {"ecoli-metabolism": "ecoli-metabolism-redux"},
            "add_processes": [],
            "exclude_processes": [],
            "fork_repo": "",
        }
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
            patch("viva_api.simulation.simulation_service_ray._SubmitJobPacer.wait", new=AsyncMock()),
        ):
            submitted = await service.submit_chain_generation_batch(
                seeds=[0, 1],
                generation_index=0,
                experiment_id="exp-run4",
                commit="abc1234",
                cache_s3="s3://mybucket/cache/abc1234",
                runner_s3_uri="s3://mybucket/runner/run_pbg.py",
                tags={"Project": "v2ecoli-comparison"},
                injected_processes=injected,
                variants=None,
            )
        assert submitted == {0: "s0g0", 1: "s1g0"}
        for call in mock_batch.submit_job.call_args_list:
            env = _container_env_of(call)
            tokens = shlex.split(env["CONTAINER_JOB_CMD"])
            overrides = json.loads(tokens[tokens.index("--overrides") + 1])
            assert overrides["injected_processes"] == injected
            assert "variants" not in overrides  # None -> omitted, matches _seed_generation_command's own contract

    async def test_batch_forwards_composite_id_to_every_seed(self) -> None:
        """Backlog item 105: submit_chain_generation_batch's own fan-out loop
        (the generation-0 burst) must pass the SAME composite_id to every seed
        -- one campaign, one config, matching the injected_processes/variants
        precedent immediately above."""
        mock_batch = _fake_container_batch(["s0g0", "s1g0"])
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
            patch("viva_api.simulation.simulation_service_ray._SubmitJobPacer.wait", new=AsyncMock()),
        ):
            submitted = await service.submit_chain_generation_batch(
                seeds=[0, 1],
                generation_index=0,
                experiment_id="exp-run1-k4",
                commit="abc1234",
                cache_s3="s3://mybucket/cache/abc1234",
                runner_s3_uri="s3://mybucket/runner/run_pbg.py",
                tags={"Project": "v2ecoli-comparison"},
                composite_id="v2ecoli.composites.reactor_bird_coupled.reactor_bird_coupled",
            )
        assert submitted == {0: "s0g0", 1: "s1g0"}
        for call in mock_batch.submit_job.call_args_list:
            env = _container_env_of(call)
            assert (
                "--composite-id v2ecoli.composites.reactor_bird_coupled.reactor_bird_coupled "
                in env["CONTAINER_JOB_CMD"]
            )

    async def test_batch_fans_out_generation_zero_for_every_seed_paced_and_isolates_failures(self) -> None:
        """The one remaining genuine submission burst: every seed's
        generation 0, fanned out the instant ParCa succeeds. Paced (like the
        superseded design's own N*G burst); a per-seed failure (even after
        retry-on-throttle) is isolated -- that seed is simply omitted from the
        returned mapping, other seeds unaffected."""
        mock_batch = MagicMock()
        base_container_props = {"image": "111.dkr.ecr.x/vecoli:ray", "vcpus": 16, "memory": 32000}

        def _describe(**kwargs: Any) -> dict[str, Any]:
            if kwargs.get("jobDefinitionName") == "smscdk-ray-container":
                return {"jobDefinitions": [{"revision": 7, "containerProperties": base_container_props}]}
            return {"jobDefinitions": []}

        mock_batch.describe_job_definitions.side_effect = _describe
        mock_batch.register_job_definition.side_effect = lambda **kw: {
            "jobDefinitionName": kw["jobDefinitionName"],
            "revision": 1,
        }
        remaining_ids = iter(["s0g0", "s2g0"])

        def _submit_job(**kwargs: Any) -> dict[str, Any]:
            if kwargs["jobName"].startswith("chain-seed1-gen0-"):
                raise RuntimeError("submit_job: rate exceeded (simulated, retries exhausted)")
            return {"jobId": next(remaining_ids)}

        mock_batch.submit_job.side_effect = _submit_job

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
            patch(
                "viva_api.simulation.simulation_service_ray._SubmitJobPacer.wait", new=AsyncMock()
            ) as mock_pacer_wait,
        ):
            submitted = await service.submit_chain_generation_batch(
                seeds=[0, 1, 2],
                generation_index=0,
                experiment_id="exp-1",
                commit="abc1234",
                cache_s3="s3://mybucket/cache/abc1234",
                runner_s3_uri="s3://mybucket/runner/run_pbg.py",
                tags={"Project": "v2ecoli-comparison"},
            )

        assert submitted == {0: "s0g0", 2: "s2g0"}  # seed 1 omitted -- its submission failed
        assert mock_pacer_wait.await_count == 3  # every seed's attempt is paced, including the failed one


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
        mock_batch = _fake_container_batch(["analysis-999"])
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
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
        # Item 71: the analysis DAG node now rides the plain container path,
        # not MNP -- no node overrides at all.
        assert "containerOverrides" in analysis_call.kwargs
        assert "nodeOverrides" not in analysis_call.kwargs

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
        mock_batch = _fake_container_batch(["analysis-999"])
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
        ):
            await service.submit_campaign_analysis(
                simulation=simulation,
                database_service=database_service,
                commit="abc1234",
                total_n_seeds=1000,  # the originally requested total, even if fewer chains actually succeeded
                n_generations=10,
            )
        env = _container_env_of(mock_batch.submit_job.call_args_list[0])
        assert "--n-seeds 1000" in env["CONTAINER_JOB_CMD"]

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
        mock_batch = _fake_container_batch(["analysis-789"])
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
        ):
            await service.submit_campaign_analysis(
                simulation=simulation,
                database_service=database_service,
                commit="abc1234",
                total_n_seeds=4,
                n_generations=3,
            )
        cmd = _container_env_of(mock_batch.submit_job.call_args_list[0])["CONTAINER_JOB_CMD"]
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
        mock_batch = _fake_container_batch([])
        mock_batch.submit_job.side_effect = RuntimeError("Batch said no")

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.simulation_service_ray.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
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
