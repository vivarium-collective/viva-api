"""Tests for the Ray-on-Batch backend: JobId.ray, Batch state mapping, ComputeBackend.RAY,
and SimulationServiceRay submission/status/cancel (boto3 mocked, Postgres via testcontainers)."""

import asyncio
import json
import shlex
from collections.abc import Mapping, Sequence
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, patch

import botocore.session
import pytest
from botocore.validate import validate_parameters

from viva_api.common.hpc.job_service import JobStatusInfo
from viva_api.common.models import JobBackend, JobId, JobStatus
from viva_api.config import ComputeBackend
from viva_api.simulation.dispatch import chain, parca_spec
from viva_api.simulation.dispatch.analysis_spec import analysis_modules_for
from viva_api.simulation.dispatch.config_interpretation import (
    injected_processes_from_config,
    strain_from_config,
)
from viva_api.simulation.dispatch.image_paths import (
    NEW_GENE_INDUCED_CACHE_DIR,
    PARCA_CACHE_DIR,
    PARCA_SIMDATA_DIR,
    SIM_OUT_DIR,
    V2ECOLI_DIR,
    VARIANT_CACHE_DIR,
)
from viva_api.simulation.models import AnalysisOptions, HpcRun, JobType
from viva_api.simulation.simulation_service_ray import (
    SimulationServiceRay,
)

if TYPE_CHECKING:
    from viva_api.simulation.database_service import DatabaseServiceSQL
    from viva_api.simulation.models import SimulationRequest


def _ray_settings() -> MagicMock:
    """A settings double with the ray_* / S3 fields SimulationServiceRay reads."""
    return MagicMock(
        batch_region="us-gov-west-1",
        # a str, as the real setting is: it becomes a Batch tag, and the fakes below validate tags
        cost_team_tag="covertlab",
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
        # Nextflow awsbatch profile inputs (Phase 4)
        batch_amd64_queue="smscdk-vecoli-task-amd64",
        s3_work_prefix="nextflow/work",
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


_BATCH_SERVICE_MODEL = botocore.session.get_session().get_service_model("batch")


def validate_batch_parameters(operation: str, kwargs: dict[str, Any]) -> None:
    """Raise ``ParamValidationError`` for keywords the REAL Batch API would refuse -- offline, from
    botocore's own service model. A ``MagicMock`` client accepts anything; a fake that calls
    this first refuses what AWS refuses."""
    shape = _BATCH_SERVICE_MODEL.operation_model(operation).input_shape
    assert shape is not None
    validate_parameters(kwargs, shape)


def _validated(operation: str, answer: Any) -> Any:
    """``answer``, but only for keywords the real API would accept."""

    def call(**kwargs: Any) -> Any:
        validate_batch_parameters(operation, kwargs)
        return answer(**kwargs)

    return call


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
        validate_batch_parameters("DescribeJobDefinitions", kwargs)  # refuse what AWS refuses (#730)
        name = kwargs.get("jobDefinitionName")
        if name == "smscdk-ray-mnp":  # MNP base
            return {"jobDefinitions": [{"revision": 7, "nodeProperties": base_node_props}]}
        return {"jobDefinitions": []}  # per-commit: none yet

    b.describe_job_definitions.side_effect = _describe
    b.register_job_definition.side_effect = _validated(
        "RegisterJobDefinition", lambda **kw: {"jobDefinitionName": kw["jobDefinitionName"], "revision": 1}
    )
    _answers = iter([{"jobId": jid} for jid in submit_ids])
    b.submit_job.side_effect = _validated("SubmitJob", lambda **kw: next(_answers))
    return b


def _overrides(call: Any) -> list[dict[str, Any]]:
    return list(call.kwargs["nodeOverrides"]["nodePropertyOverrides"])


def _env_at(call: Any, index: int) -> dict[str, str]:
    """Env dict for the override at `index` (0 = head/`0:0`, 1 = workers/`1:`)."""
    ov = _overrides(call)[index]
    return {e["name"]: e["value"] for e in ov["containerOverrides"]["environment"]}


def _overrides_from_run_pbg_cmd(cmd: str) -> dict[str, Any]:
    """Parse the ``--overrides '<json>'`` payload out of a run_pbg.py command."""
    tokens = shlex.split(cmd.split("python /tmp/run_pbg.py", 1)[1])
    return dict(json.loads(tokens[tokens.index("--overrides") + 1]))


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
        validate_batch_parameters("DescribeJobDefinitions", kwargs)  # refuse what AWS refuses (#730)
        name = kwargs.get("jobDefinitionName")
        if name == "smscdk-ray-container":  # container base
            return {"jobDefinitions": [{"revision": 7, "containerProperties": base_container_props}]}
        return {"jobDefinitions": []}  # per-commit: none yet

    b.describe_job_definitions.side_effect = _describe
    b.register_job_definition.side_effect = _validated(
        "RegisterJobDefinition", lambda **kw: {"jobDefinitionName": kw["jobDefinitionName"], "revision": 1}
    )
    _answers = iter([{"jobId": jid} for jid in submit_ids])
    b.submit_job.side_effect = _validated("SubmitJob", lambda **kw: next(_answers))
    return b


def _container_env_of(call: Any) -> dict[str, str]:
    """Env dict for a _submit_container call (single containerOverrides.environment list)."""
    return {e["name"]: e["value"] for e in call.kwargs["containerOverrides"]["environment"]}


async def _raw_analysis_configs(database_service: "DatabaseServiceSQL", simulation_id: int) -> list[dict[str, Any]]:
    """Raw ``analysis.config`` JSONB for a simulation's analysis rows.

    ``ExperimentAnalysisDTO.config`` (what ``list_analyses`` returns) narrows to
    just ``analysis_options`` (``ORMAnalysis.to_dto()``), so fields like
    ``n_seeds``/``n_generations`` that ride the SAME raw ``config`` dict but
    outside ``analysis_options`` aren't visible through it -- query the ORM row
    directly to check the full recorded shape.
    """
    from sqlalchemy import select

    from viva_api.simulation.tables_orm import ORMAnalysis

    async with database_service.async_sessionmaker() as session:
        result = await session.execute(select(ORMAnalysis).where(ORMAnalysis.simulation_id == simulation_id))
        return [dict(row.config) for row in result.scalars().all()]


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
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            # data_layout builds the S3 URIs (results/cache) and reads config.get_settings directly.
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
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
            patch("viva_api.simulation.dispatch._seams.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
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
            patch("viva_api.simulation.dispatch._seams.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
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
        real_submit_chain_dispatch_job = chain.ChainStrategy.submit_chain_dispatch_job

        async def _held_submit_chain_dispatch_job(self: chain.ChainStrategy, *args: Any, **kwargs: Any) -> JobId:
            await released.wait()
            return await real_submit_chain_dispatch_job(self, *args, **kwargs)

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
            patch.object(chain.ChainStrategy, "submit_chain_dispatch_job", _held_submit_chain_dispatch_job),
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
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
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

    @pytest.mark.asyncio
    async def test_composite_comparison_cache_variant_reaches_the_staged_cache_uri(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """Real gap, found live 2026-09-05 scoping a real remote genotype-sweep
        dispatch: run_comparison_ensemble.py's own --cache-dir already defaults
        to the exact path this path stages (REPO_ROOT/out/cache ==
        PARCA_CACHE_DIR) -- confirmed identical, so no command-line change is
        needed -- but cache_s3 (what gets staged there) never respected a
        caller-supplied cache_variant, unlike every other dispatch path
        (mirrors test_cache_variant_reaches_the_staged_cache_uri's own MNP
        case). Without this, a caller pointed at a real genotype-specific
        cache would silently get the plain per-commit default instead."""
        setattr(experiment_request.config, "composite", "v2ecoli")  # noqa: B010
        setattr(experiment_request.config, "cache_variant", "cd2-run4-genotype-07")  # noqa: B010
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        mock_batch = _fake_batch(["parca-9", "sim-9"])
        fake_file_service = AsyncMock()
        fake_file_service.upload_file = AsyncMock()

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
            patch("viva_api.dependencies.get_file_service", return_value=fake_file_service),
            patch.object(parca_spec, "cache_s3_uri", wraps=parca_spec.cache_s3_uri) as mock_cache_s3_uri,
        ):
            await service.submit_ecoli_simulation_job(
                ecoli_simulation=simulation, database_service=database_service, correlation_id="corr-comparison-variant"
            )

        mock_cache_s3_uri.assert_called_once()
        assert mock_cache_s3_uri.call_args.kwargs.get("variant") == "cd2-run4-genotype-07"

    @pytest.mark.asyncio
    async def test_composite_comparison_omitted_cache_variant_is_byte_identical_to_before(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """cache_variant omitted must resolve to the plain per-commit cache,
        unchanged from every existing caller's own behavior."""
        setattr(experiment_request.config, "composite", "v2ecoli")  # noqa: B010
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        mock_batch = _fake_batch(["parca-10", "sim-10"])
        fake_file_service = AsyncMock()
        fake_file_service.upload_file = AsyncMock()

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
            patch("viva_api.dependencies.get_file_service", return_value=fake_file_service),
            patch.object(parca_spec, "cache_s3_uri", wraps=parca_spec.cache_s3_uri) as mock_cache_s3_uri,
        ):
            await service.submit_ecoli_simulation_job(
                ecoli_simulation=simulation,
                database_service=database_service,
                correlation_id="corr-comparison-no-variant",
            )

        mock_cache_s3_uri.assert_called_once()
        assert mock_cache_s3_uri.call_args.kwargs.get("variant") is None

    @pytest.mark.asyncio
    async def test_composite_comparison_upstream_vecoli_ignores_cache_variant(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """The upstream-vEcoli engine (--composite vecoli) uses its own,
        entirely separate config_path-driven cache mechanism (item 87) -- a
        cache_variant set alongside it must be ignored, not misapplied to the
        wrong cache family."""
        setattr(experiment_request.config, "composite", "vecoli")  # noqa: B010
        setattr(experiment_request.config, "cache_variant", "should-be-ignored")  # noqa: B010
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        mock_batch = _fake_batch(["parca-11", "sim-11"])
        fake_file_service = AsyncMock()
        fake_file_service.upload_file = AsyncMock()

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
            patch("viva_api.dependencies.get_file_service", return_value=fake_file_service),
            patch.object(parca_spec, "cache_s3_uri", wraps=parca_spec.cache_s3_uri) as mock_cache_s3_uri,
        ):
            await service.submit_ecoli_simulation_job(
                ecoli_simulation=simulation,
                database_service=database_service,
                correlation_id="corr-comparison-upstream",
            )

        # Upstream vEcoli never calls the v2ecoli cache_s3_uri helper at all.
        mock_cache_s3_uri.assert_not_called()


class TestNewGeneCacheSourceVariant:
    """``source_variant`` on ``submit_new_gene_cache_job`` (sms-ecoli#166, 2026-09-09):
    the induction job may stage its chassis from a variant slot instead of the
    shared bare commit slot, which any chain dispatch's ``run_parca`` rewrites
    (the 6299ba5 violacein chassis was overwritten seven hours after its 42
    inductions, and every later re-induction died with "no new-gene cistrons")."""

    @pytest.mark.asyncio
    async def test_default_stages_from_the_bare_commit_slot(self) -> None:
        service = SimulationServiceRay()
        captured: dict[str, Any] = {}

        def fake_submit_container(*, job_cmd: str, **kw: Any) -> str:
            captured.update(kw)
            captured["job_cmd"] = job_cmd
            return "ngc-1"

        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
            patch.object(service.batch, "submit_container", side_effect=fake_submit_container),
            patch.object(service.batch, "ensure_container_job_def", return_value="job-def:1"),
            patch.object(service.batch, "image_uri", return_value="ghcr.io/example/image:abc"),
        ):
            job_id = await service.parca.submit_new_gene_cache_job(
                commit="abc123",
                variant="cd2-run4-carina-lin-genotype1",
                expression=1174897.5549395303,
                translation_efficiency=0.285,
            )
            bare = service.cache_s3_uri("abc123")
            out = service.cache_s3_uri("abc123", variant="cd2-run4-carina-lin-genotype1")

        assert job_id == JobId.ray("ngc-1")
        assert captured["stage_s3"] == bare
        assert captured["out_s3"] == out
        assert "--expression 1174897.5549395303" in captured["job_cmd"]

    @pytest.mark.asyncio
    async def test_source_variant_stages_from_the_variant_slot(self) -> None:
        service = SimulationServiceRay()
        captured: dict[str, Any] = {}

        def fake_submit_container(*, job_cmd: str, **kw: Any) -> str:
            captured.update(kw)
            return "ngc-2"

        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
            patch.object(service.batch, "submit_container", side_effect=fake_submit_container),
            patch.object(service.batch, "ensure_container_job_def", return_value="job-def:1"),
            patch.object(service.batch, "image_uri", return_value="ghcr.io/example/image:abc"),
        ):
            await service.parca.submit_new_gene_cache_job(
                commit="abc123",
                variant="cd2-run4-carina-lin-genotype1",
                expression=1174897.5549395303,
                translation_efficiency=0.285,
                source_variant="cd2-run4-vio-chassis",
            )
            source = service.cache_s3_uri("abc123", variant="cd2-run4-vio-chassis")
            bare = service.cache_s3_uri("abc123")
            out = service.cache_s3_uri("abc123", variant="cd2-run4-carina-lin-genotype1")

        assert captured["stage_s3"] == source
        assert captured["stage_s3"] != bare
        # The OUTPUT slot is still the induced variant, never the source slot.
        assert captured["out_s3"] == out

    def test_request_model_accepts_and_defaults_source_variant(self) -> None:
        from viva_api.simulation.models import NewGeneCacheRequest

        base = NewGeneCacheRequest(parca_dataset_id=1, variant="v", expression=1.0, translation_efficiency=0.2)
        assert base.source_variant is None
        req = NewGeneCacheRequest.model_validate({
            "parca_dataset_id": 1,
            "variant": "v",
            "expression": 1.0,
            "translation_efficiency": 0.2,
            "source_variant": "chassis",
        })
        assert req.source_variant == "chassis"


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
        with patch("viva_api.simulation.dispatch._seams.get_settings", return_value=settings):
            service.batch.submit_mnp(
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
        with patch("viva_api.simulation.dispatch._seams.get_settings", return_value=settings):
            service.batch.submit_mnp(
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
        with patch("viva_api.simulation.dispatch._seams.get_settings", return_value=settings):
            service.batch.submit_mnp(
                job_name="multinode-test",
                job_definition="smscdk-ray-mnp",
                num_nodes=4,
                ray_job_cmd="echo hi",
                out_s3="s3://bucket/out/",
                out_dir="/out",
                batch_client=mock_batch,
            )
        assert mock_batch.submit_job.call_args.kwargs["jobQueue"] == "smscdk-ray-mnp"


class TestSubmitMnpAllowsSlowStorage:
    """Item 105/109: a single-node lineage_ray_batch diagnostic (database_id=344,
    2026-09-05) died in Ray's own raylet bootstrap -- before any application code
    ran -- because the container's /dev/shm was smaller than the plasma object
    store's default request. RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE=1 is Ray's own
    documented fallback (disk-backed instead of a hard error), applied
    unconditionally since it changes nothing on a node where shm is sufficient."""

    def test_flag_present_in_every_node_environment(self) -> None:
        settings = _ray_settings()
        mock_batch = MagicMock()
        mock_batch.submit_job.return_value = {"jobId": "job-1"}
        service = SimulationServiceRay()
        with patch("viva_api.simulation.dispatch._seams.get_settings", return_value=settings):
            service.batch.submit_mnp(
                job_name="shm-test",
                job_definition="smscdk-ray-mnp",
                num_nodes=2,
                ray_job_cmd="echo hi",
                out_s3="s3://bucket/out/",
                out_dir="/out",
                batch_client=mock_batch,
            )
        node_overrides = mock_batch.submit_job.call_args.kwargs["nodeOverrides"]
        env = node_overrides["nodePropertyOverrides"][0]["containerOverrides"]["environment"]
        env_by_name = {e["name"]: e["value"] for e in env}
        assert env_by_name["RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE"] == "1"
        # Targets "0:" (every node), not just the head -- every node runs its own raylet.
        assert node_overrides["nodePropertyOverrides"][0]["targetNodes"] == "0:"


class TestAnalysisModulesFor:
    """analysis_modules_for reads the simulation's OWN configured analyses."""

    def test_real_scale_entries_are_forwarded_verbatim(self) -> None:
        from viva_api.simulation.models import SimulationConfig

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
        from viva_api.simulation.models import SimulationConfig

        assert analysis_modules_for(SimulationConfig(experiment_id="exp-1")) == "applicable"
        empty = SimulationConfig(
            experiment_id="exp-1", analysis_options=AnalysisOptions.model_validate({"multiseed": {}})
        )
        assert analysis_modules_for(empty) == "applicable"


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
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
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
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            cmd = parca_spec.parca_command(new_genes="violacein")
        assert f"--new-genes {shlex.quote('violacein')}" in cmd

    @pytest.mark.parametrize("new_genes", [None, "off"])
    def test_new_genes_off_or_absent_omits_the_flag(self, new_genes: str | None) -> None:
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            cmd = parca_spec.parca_command(new_genes=new_genes)
        assert "--new-genes" not in cmd

    def test_new_genes_with_a_space_is_shell_quoted(self) -> None:
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            cmd = parca_spec.parca_command(new_genes="two genes")
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
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
        ):
            info = await service.get_job_status(JobId.ray("sim-1"))
        assert info is not None
        assert info.exit_code == "137"

    async def test_exit_code_is_none_when_batch_reports_none(self) -> None:
        mock_batch = MagicMock()
        mock_batch.describe_jobs.return_value = {"jobs": [{"jobId": "sim-2", "status": "RUNNING"}]}
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
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
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
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
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
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
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
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
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
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
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
        ):
            await service.cancel_chain_campaign(campaign)

        mock_batch.terminate_job.assert_called_once()
        assert mock_batch.terminate_job.call_args.kwargs["jobId"] == "s1g0"

    async def test_empty_or_all_none_is_a_safe_noop(self) -> None:
        mock_batch = MagicMock()
        service = SimulationServiceRay()
        campaign = self._campaign([None, None])
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
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
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
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
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
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
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
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
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
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
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
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
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
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
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
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
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            cmd = service._image_builder().build_command(_v2ecoli_simulator())
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
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            script = service._image_builder().build_command(_v2ecoli_simulator())[2]
        assert " -g" not in script
        assert script.count("unset GH_PAT") == 1
        # GH_PAT is unset BEFORE the build recipe runs, not after -- confirms it's not left
        # exported into that command's environment when the flag is off.
        assert script.index("unset GH_PAT") < script.index("docker/build-and-push-ecr.sh")

    def test_build_command_include_new_gene_data_passes_g_and_keeps_pat_exported(self) -> None:
        service = SimulationServiceRay()
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            script = service._image_builder().build_command(_v2ecoli_simulator(), include_new_gene_data=True)[2]
        assert "docker/build-and-push-ecr.sh -i abc1234 -r v2ecoli -R us-gov-west-1 -g" in script
        # GH_PAT must still be exported (not unset) by the time the recipe runs, or -g's own
        # `[[ -n "${GH_PAT:-}" ]]` guard in the recipe would fail even though this method
        # believes it's supplying one.
        assert "unset GH_PAT" not in script

    def test_build_command_default_never_stages_private_fork(self) -> None:
        """Regression: every existing build must stay byte-for-byte unaffected -- no -s
        flag, no inline spec heredoc, PAT still unset exactly as before this param
        existed."""
        service = SimulationServiceRay()
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            script = service._image_builder().build_command(_v2ecoli_simulator())[2]
        assert " -s " not in script
        assert "vecoli-private-fork.yaml" not in script
        assert script.count("unset GH_PAT") == 1

    def test_build_command_stage_private_fork_requires_a_commit(self) -> None:
        """No 'latest' auto-resolution: which commit gets staged must always be an
        explicit, visible choice, never a silent moving target -- fail fast in Python,
        not deep inside a generated shell script."""
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            pytest.raises(ValueError, match="vecoli_private_commit"),
        ):
            service._image_builder().build_command(_v2ecoli_simulator(), stage_private_fork=True)

    def test_build_command_stage_private_fork_passes_s_and_keeps_pat_exported(self) -> None:
        service = SimulationServiceRay()
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            script = service._image_builder().build_command(
                _v2ecoli_simulator(), stage_private_fork=True, vecoli_private_commit="deadbee"
            )[2]
        assert (
            "docker/build-and-push-ecr.sh -i abc1234 -r v2ecoli -R us-gov-west-1"
            " -s /tmp/vecoli-private-fork.yaml" in script
        )
        # The spec is generated INLINE (a heredoc) -- nothing checked into a repo to go
        # stale -- and names the real private fork + the exact requested commit.
        assert "cat > /tmp/vecoli-private-fork.yaml <<'SPEC'" in script
        assert "repo: https://github.com/CovertLabEcoli/vEcoli-private" in script
        assert "commit: deadbee" in script
        # vEcoli-private is a private repo under the same org as the outer clone -- same
        # PAT, kept exported, no second credential.
        assert "unset GH_PAT" not in script
        # The heredoc must land BEFORE the recipe invocation that consumes it via -s.
        assert script.index("cat > /tmp/vecoli-private-fork.yaml") < script.index("docker/build-and-push-ecr.sh")


def test_runner_env_carries_both_output_guards() -> None:
    """The CD2 baseline/lineage dispatch env pairs the presence guard
    (PBG_REQUIRE_OUTPUT) with the effect guard (PBG_MIN_GLOBAL_TIME, #395 / #375 §3e),
    and the floor is above the one-tick collapse (global_time ~= 1.0)."""
    from viva_api.simulation.dispatch.runner_env import PBG_MIN_GLOBAL_TIME, PBG_RUNNER_ENV

    assert "PBG_REQUIRE_OUTPUT=1" in PBG_RUNNER_ENV
    assert f"PBG_MIN_GLOBAL_TIME={PBG_MIN_GLOBAL_TIME}" in PBG_RUNNER_ENV
    assert PBG_MIN_GLOBAL_TIME > 1.0


def _env_names(env: Sequence[Mapping[str, object]]) -> dict[str, str]:
    return {str(e["name"]): str(e["value"]) for e in env}


def test_stage_out_env_emits_expect_vars_for_a_real_strain() -> None:
    """Wrong-strain guard companion (sms-ecoli#210 / #215): the staged-cache env
    carries the requested strain so the entrypoint can reject a wrong-strain cache."""
    svc = SimulationServiceRay()
    env = svc.batch._stage_out_env(
        prefix="RAY",
        out_dir="/o",
        out_s3="s3://o",
        stage_s3="s3://c",
        stage_dir="/c",
        expect_new_genes="violacein_MG1655_M5",
        expect_bundle_overrides="models/parca/composed_overlay.tsv",
    )
    d = _env_names(env)
    assert d["RAY_EXPECT_NEW_GENES"] == "violacein_MG1655_M5"
    assert d["RAY_EXPECT_BUNDLE_OVERRIDES"] == "models/parca/composed_overlay.tsv"


def test_stage_out_env_expect_bundle_overrides_accepts_a_list() -> None:
    """Regression: a caller reaching _stage_out_env directly with
    config.parca_options.bundle_overrides (str | list[str] | None as of #486),
    not through strain_from_config's own _norm-based string normalization,
    crashed with AttributeError: 'list' object has no attribute 'strip' --
    caught live firing a real K4/J3 chassis rebuild whose recipe genuinely
    stacks two --bundle-overrides files. Joined with "," matching _norm's own
    convention for the single env var."""
    svc = SimulationServiceRay()
    env = svc.batch._stage_out_env(
        prefix="RAY",
        out_dir="/o",
        out_s3="s3://o",
        expect_new_genes="violacein_gfp",
        expect_bundle_overrides=[
            "workspace/studies/cd2-pnnl-01-bundle-scenarios/bundles/vio-gfp/overrides.tsv",
            "workspace/studies/cd2-pnnl-01-bundle-scenarios/bundles/rung5-lambda-050/overrides.tsv",
        ],
    )
    d = _env_names(env)
    assert d["RAY_EXPECT_BUNDLE_OVERRIDES"] == (
        "workspace/studies/cd2-pnnl-01-bundle-scenarios/bundles/vio-gfp/overrides.tsv,"
        "workspace/studies/cd2-pnnl-01-bundle-scenarios/bundles/rung5-lambda-050/overrides.tsv"
    )


def test_stage_out_env_expect_bundle_overrides_empty_list_is_wild_type() -> None:
    """An empty list joins to "" -- same wild-type/no-expectation behavior as
    None or "", not a crash or a spurious empty EXPECT_BUNDLE_OVERRIDES var."""
    svc = SimulationServiceRay()
    env = svc.batch._stage_out_env(
        prefix="RAY",
        out_dir="/o",
        out_s3="s3://o",
        expect_bundle_overrides=[],
    )
    assert "RAY_EXPECT_BUNDLE_OVERRIDES" not in _env_names(env)


@pytest.mark.parametrize("wild", [None, "", "off", "  off  "])
def test_stage_out_env_omits_expect_vars_for_wild_type(wild: str | None) -> None:
    """off/empty/None is wild-type -> no expectation, byte-identical to before."""
    svc = SimulationServiceRay()
    env = svc.batch._stage_out_env(
        prefix="RAY",
        out_dir="/o",
        out_s3="s3://o",
        expect_new_genes=wild,
        expect_bundle_overrides=wild,
    )
    names = _env_names(env)
    assert "RAY_EXPECT_NEW_GENES" not in names
    assert "RAY_EXPECT_BUNDLE_OVERRIDES" not in names


def test_stage_out_env_expect_vars_follow_the_prefix() -> None:
    """CONTAINER path gets CONTAINER_EXPECT_* (future-proofs the chain-dispatch path)."""
    svc = SimulationServiceRay()
    env = svc.batch._stage_out_env(
        prefix="CONTAINER",
        out_dir="/o",
        out_s3="s3://o",
        expect_new_genes="violacein",
    )
    assert "CONTAINER_EXPECT_NEW_GENES" in _env_names(env)


def test_stage_out_env_omits_require_clean_chain_by_default() -> None:
    """Item 106/#166 (v2ecoli#735): default False emits nothing -- byte-identical
    to before this param existed, for both prefixes."""
    svc = SimulationServiceRay()
    for prefix in ("RAY", "CONTAINER"):
        env = svc.batch._stage_out_env(prefix=prefix, out_dir="/o", out_s3="s3://o")
        assert "V2E_REQUIRE_CLEAN_CHAIN" not in _env_names(env)


def test_stage_out_env_require_clean_chain_is_unprefixed() -> None:
    """Unlike every other var this helper emits, V2E_REQUIRE_CLEAN_CHAIN is NOT
    prefixed by RAY_/CONTAINER_ -- it's read directly by v2ecoli's own
    os.environ.get("V2E_REQUIRE_CLEAN_CHAIN"), not by the entrypoint scripts."""
    svc = SimulationServiceRay()
    for prefix in ("RAY", "CONTAINER"):
        env = svc.batch._stage_out_env(prefix=prefix, out_dir="/o", out_s3="s3://o", require_clean_chain=True)
        d = _env_names(env)
        assert d["V2E_REQUIRE_CLEAN_CHAIN"] == "1"
        assert f"{prefix}_REQUIRE_CLEAN_CHAIN" not in d


class TestStrainFromConfig:
    """strain_from_config (sms-ecoli#210 / #215): the shared helper JobScheduler
    and the sim submit use to read (new_genes, bundle_overrides) off a config's
    parca_options and thread them to the entrypoint as *_EXPECT_* so a wrong-strain
    staged cache is rejected."""

    def test_none_when_no_parca_options(self) -> None:
        assert strain_from_config(SimpleNamespace()) == (None, None)

    def test_reads_a_real_strain(self) -> None:
        cfg = SimpleNamespace(
            parca_options=SimpleNamespace(new_genes="violacein_MG1655_M5", bundle_overrides="models/parca/o.tsv")
        )
        assert strain_from_config(cfg) == ("violacein_MG1655_M5", "models/parca/o.tsv")

    @pytest.mark.parametrize("wild", [None, "", "off", "  off  "])
    def test_wild_type_sentinels_are_none(self, wild: str | None) -> None:
        cfg = SimpleNamespace(parca_options=SimpleNamespace(new_genes=wild, bundle_overrides=wild))
        assert strain_from_config(cfg) == (None, None)

    def test_partial_strain(self) -> None:
        """new_genes set, bundle_overrides absent -> only the former."""
        cfg = SimpleNamespace(parca_options=SimpleNamespace(new_genes="violacein"))
        assert strain_from_config(cfg) == ("violacein", None)

    def test_bundle_overrides_list_is_joined_not_silently_dropped(self) -> None:
        """bundle_overrides can now be a list (ParcaOptions.bundle_overrides,
        item 106). _norm used to only handle isinstance(value, str); a list
        would previously fall through to None -- silently losing the EXPECT_*
        signal for a run that legitimately stacked two overrides, the exact
        "declared but not read" failure class this whole helper exists to
        prevent for new_genes/bundle_overrides individually."""
        cfg = SimpleNamespace(
            parca_options=SimpleNamespace(
                new_genes="violacein_gfp",
                bundle_overrides=["bundles/vio-gfp/overrides.tsv", "bundles/rung5-lambda-075/overrides.tsv"],
            )
        )
        assert strain_from_config(cfg) == (
            "violacein_gfp",
            "bundles/vio-gfp/overrides.tsv,bundles/rung5-lambda-075/overrides.tsv",
        )

    def test_bundle_overrides_empty_list_is_none(self) -> None:
        cfg = SimpleNamespace(parca_options=SimpleNamespace(new_genes=None, bundle_overrides=[]))
        assert strain_from_config(cfg) == (None, None)


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
            "cache_dir": "/app/v2ecoli/out/cache",
        }

    def test_nested_block_carries_extra_keys_through(self) -> None:
        """viva-api#392 regression: #387 fixed the nested-block DROP but reconstructed
        only the four canonical keys, silently dropping everything else the caller
        put in the block -- e.g. `cache_dir`, which a fork-free swap's own
        resolve_injections() spec-building needs to load the target process's config
        from the ParCa bundle (confirmed via cplong90's own trace on PR#387, and
        jcschaff's independent live-pod confirmation). Without it the swapped-in
        process mounts with an empty config and crashes at tick 0 well away from the
        real cause. The fix must carry ARBITRARY extra keys through, not just
        special-case `cache_dir` -- assert with an unrelated marker key too."""
        config = SimpleNamespace(
            injected_processes={
                "fork_repo": "",
                "swap_processes": {"ecoli-metabolism": "ecoli-metabolism-redux"},
                "cache_dir": "/app/v2ecoli/out/cache",
                "some_future_key": "must-survive",
            }
        )
        result = injected_processes_from_config(config)
        assert result is not None
        assert result["cache_dir"] == "/app/v2ecoli/out/cache"
        assert result["some_future_key"] == "must-survive"
        # The four canonical keys are still normalized defaults layered on top,
        # not left to whatever the caller happened to send for them.
        assert result["add_processes"] == []
        assert result["exclude_processes"] == []
        assert result["fork_repo"] == ""

    def test_flat_shape_stays_byte_identical_no_carry_through(self) -> None:
        """The legacy FLAT shape has no nested block to carry extra keys from --
        the fix must not change its output at all (byte-identical regression)."""
        config = SimpleNamespace(
            swap_processes={"ecoli-metabolism": "ecoli-metabolism-redux"},
            add_processes=[],
            exclude_processes=[],
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

    def test_nested_swap_only_preserves_configs_flat_add_and_exclude(self) -> None:
        """viva-api#401 regression: a nested block carrying ONLY swap_processes must
        not silently drop the config's own flat add_processes/exclude_processes --
        the two shapes are not a whole-block either/or, each of the three fields is
        resolved independently. Observed live (sim 296, mecillinam_wellmixed.json):
        a nested metabolism swap that never mentioned add_processes silently dropped
        all 4 of the config's own add_processes, and nothing reported it."""
        config = SimpleNamespace(
            injected_processes={"swap_processes": {"ecoli-metabolism": "ecoli-metabolism-redux"}},
            add_processes=["permeability", "antibiotic-transport-odeint"],
            exclude_processes=["exchange_data"],
        )
        result = injected_processes_from_config(config)
        assert result == {
            "swap_processes": {"ecoli-metabolism": "ecoli-metabolism-redux"},
            "add_processes": ["permeability", "antibiotic-transport-odeint"],
            "exclude_processes": ["exchange_data"],
            "fork_repo": "",
        }

    def test_nested_field_wins_over_flat_on_real_conflict(self) -> None:
        """When both the nested block and the flat config set the SAME field, the
        caller's nested value wins -- an explicit override of the caller's own
        stated intent, not a silent drop of it."""
        config = SimpleNamespace(
            injected_processes={"swap_processes": {"ecoli-metabolism": "ecoli-metabolism-redux"}},
            swap_processes={"ecoli-metabolism": "some-other-swap"},
        )
        result = injected_processes_from_config(config)
        assert result is not None
        assert result["swap_processes"] == {"ecoli-metabolism": "ecoli-metabolism-redux"}

    def test_nested_add_only_preserves_configs_flat_swap(self) -> None:
        """Symmetric to the swap-only case above: a nested block setting only
        add_processes must not drop the config's own flat swap_processes."""
        config = SimpleNamespace(
            injected_processes={"add_processes": ["gillespie"]},
            swap_processes={"ecoli-metabolism": "ecoli-metabolism-redux"},
        )
        result = injected_processes_from_config(config)
        assert result == {
            "swap_processes": {"ecoli-metabolism": "ecoli-metabolism-redux"},
            "add_processes": ["gillespie"],
            "exclude_processes": [],
            "fork_repo": "",
        }


class TestIsUpstreamVecoli:
    """The single routing predicate shared by submit_ecoli_simulation_job and _sim_command."""

    def test_only_vecoli_is_upstream(self) -> None:
        from viva_api.simulation.dispatch.config_interpretation import _is_upstream_vecoli

        assert _is_upstream_vecoli("vecoli") is True
        assert _is_upstream_vecoli("v2ecoli") is False
        assert _is_upstream_vecoli(None) is False


class TestUpstreamParcaCommand:
    """_upstream_parca_command / _upstream_cache_s3_uri (item 87): the vecoli
    reference-arm ParCa build/cache path gains an optional config-driven,
    non-colliding variant -- every existing caller (config_path/variant both
    None) must be provably unaffected."""

    def test_no_config_path_is_byte_identical_to_before(self) -> None:
        assert parca_spec.upstream_parca_command() == (
            f"cd {V2ECOLI_DIR} && python scripts/build_upstream_parca.py"
            f" --outdir {V2ECOLI_DIR}/out/upstream --cpus 1"
            f" --copy-to {PARCA_CACHE_DIR}"
        )

    def test_config_path_appends_the_config_flag(self) -> None:
        cmd = parca_spec.upstream_parca_command(config_path=f"{V2ECOLI_DIR}/configs/custom_strain.json")
        assert cmd.endswith(f" --config {V2ECOLI_DIR}/configs/custom_strain.json")
        # Everything before it is unchanged -- confirms this is a pure append,
        # not a differently-ordered command that happens to contain the flag.
        assert cmd.startswith(parca_spec.upstream_parca_command())

    def test_cache_uri_no_variant_is_byte_identical_to_before(self) -> None:
        with patch("viva_api.common.storage.data_layout.get_settings", _ray_settings):
            assert parca_spec.upstream_cache_s3_uri("abc123") == "s3://mybucket/ray-upstream-parca-cache/abc123/"

    def test_cache_uri_variant_never_collides_with_bare_commit_path(self) -> None:
        """The real hazard this whole feature guards against: a config-driven
        cache must never land where a plain baseline build/stage would read."""
        with patch("viva_api.common.storage.data_layout.get_settings", _ray_settings):
            bare = parca_spec.upstream_cache_s3_uri("abc123")
            variant = parca_spec.upstream_cache_s3_uri("abc123", variant="custom-strain")
        assert variant != bare
        assert variant == "s3://mybucket/ray-upstream-parca-cache/abc123/custom-strain/"


class TestParcaCommand:
    """_parca_command's own flag-assembly, in isolation -- no DB/AWS needed.

    Backlog item 104 (sms-ecoli#184 / viva-api#365, cplong90): bundle_overrides
    survived on the stored request but was never forwarded here, so ParCa built
    from defaults only. Mirrors item 93's own new_genes regression-guard shape
    (byte-identical when unset; the real flag when set)."""

    def test_no_options_is_byte_identical_to_before(self) -> None:
        """Baseline updated for item 106/#166's chassis-provenance sidecar copy
        (v2ecoli#735) -- unconditional and non-fatal (`|| true`), unlike every
        other option here which is opt-in-only. See
        test_sidecar_copy_is_present_and_non_fatal below for that addition's own
        dedicated coverage."""
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            cmd = parca_spec.parca_command()
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
            f" && (cp {PARCA_SIMDATA_DIR}/parca_state.provenance.json"
            f" {PARCA_CACHE_DIR}/parca_state.provenance.json 2>/dev/null || true)"
        )

    def test_sidecar_copy_is_present_and_non_fatal(self) -> None:
        """The chassis-provenance sidecar copy (item 106/#166, v2ecoli#735) is
        unconditional (present with or without any other option) and non-fatal
        (`|| true`) -- a pre-#735 v2ecoli image never writes this file, so every
        dispatch must keep working unchanged until it does."""
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            cmd = parca_spec.parca_command(new_genes="violacein_MG1655_M5")
        assert f"cp {PARCA_SIMDATA_DIR}/parca_state.provenance.json" in cmd
        assert "|| true" in cmd
        # The sidecar copy must not break the leading &&-chain's own
        # short-circuit semantics for the real cache build steps before it.
        assert cmd.index("build_cache.py") < cmd.index("parca_state.provenance.json")

    def test_preserves_raw_fitted_state_for_new_gene_cache_consumption(self) -> None:
        """Backlog item 105: the raw parca_state.pkl.gz must ride along in the
        synced PARCA_CACHE_DIR -- build_new_gene_cache.py needs exactly this
        file, and PARCA_SIMDATA_DIR (where it's first produced) is never
        synced anywhere and is discarded with the job's container."""
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            cmd = parca_spec.parca_command()
        assert f"cp {PARCA_SIMDATA_DIR}/parca_state.pkl.gz {PARCA_CACHE_DIR}/parca_state.pkl.gz" in cmd
        # comes after the hydration step, not before -- gzip must exist first
        assert cmd.index("build_cache.py") < cmd.index(f"cp {PARCA_SIMDATA_DIR}")

    def test_off_new_genes_is_byte_identical_to_unset(self) -> None:
        """ "off" is v2ecoli-parca's own --new-genes default -- passing it explicitly
        must not append a redundant flag."""
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            assert parca_spec.parca_command(new_genes="off") == parca_spec.parca_command()

    def test_bundle_overrides_appends_the_flag(self) -> None:
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            cmd = parca_spec.parca_command(bundle_overrides="models/parca/composed_overlay.tsv")
        assert "--bundle-overrides models/parca/composed_overlay.tsv" in cmd
        assert "--new-genes" not in cmd

    def test_new_genes_and_bundle_overrides_both_append(self) -> None:
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            cmd = parca_spec.parca_command(
                new_genes="violacein_MG1655_M5", bundle_overrides="models/parca/composed_overlay.tsv"
            )
        assert "--new-genes violacein_MG1655_M5 --bundle-overrides models/parca/composed_overlay.tsv" in cmd

    def test_bundle_overrides_list_appends_one_flag_per_entry_in_order(self) -> None:
        """Real gap (item 106): sms-ecoli's own declared recipe for the CD2 J3
        candidate chassis (cd2-pnnl-01-bundle-scenarios/sims/run_scenarios.sh,
        scenario rung5_lam075) stacks TWO --bundle-overrides flags in one
        v2ecoli-parca invocation -- a single-string field could only ever carry
        one of the two layers, silently dropping the other."""
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            cmd = parca_spec.parca_command(
                new_genes="violacein_gfp",
                bundle_overrides=[
                    "workspace/studies/cd2-pnnl-01-bundle-scenarios/bundles/vio-gfp/overrides.tsv",
                    "workspace/studies/cd2-pnnl-01-bundle-scenarios/bundles/rung5-lambda-075/overrides.tsv",
                ],
                rnaseq_source="experimental",
            )
        assert (
            "--new-genes violacein_gfp "
            "--bundle-overrides workspace/studies/cd2-pnnl-01-bundle-scenarios/bundles/vio-gfp/overrides.tsv "
            "--bundle-overrides workspace/studies/cd2-pnnl-01-bundle-scenarios/bundles/rung5-lambda-075/overrides.tsv "
            "--rnaseq-source experimental"
        ) in cmd
        # exactly two, not deduped/collapsed
        assert cmd.count("--bundle-overrides") == 2

    def test_bundle_overrides_empty_list_is_byte_identical_to_none(self) -> None:
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            assert parca_spec.parca_command(bundle_overrides=[]) == parca_spec.parca_command()

    def test_bundle_overrides_single_element_list_matches_bare_string(self) -> None:
        """A 1-element list and the equivalent bare string must build the exact
        same command -- the list form is additive, not a parallel code path."""
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            as_list = parca_spec.parca_command(bundle_overrides=["models/parca/composed_overlay.tsv"])
            as_str = parca_spec.parca_command(bundle_overrides="models/parca/composed_overlay.tsv")
        assert as_list == as_str

    def test_omitted_rnaseq_source_is_byte_identical_to_before(self) -> None:
        """None must build byte-for-byte the same command as before this param
        existed -- same contract as new_genes/bundle_overrides above."""
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            assert parca_spec.parca_command(rnaseq_source=None) == parca_spec.parca_command()

    def test_rnaseq_source_appends_the_flag(self) -> None:
        """A bundle_overrides manifest can itself REQUIRE this flag to have any
        effect (rung5-lambda-075/overrides.tsv's own header: "READ BY NOTHING
        without that flag... the scenario silently becomes its own control") --
        confirmed real gap, item 106/#166 chassis-provenance thread."""
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            cmd = parca_spec.parca_command(rnaseq_source="experimental")
        assert "--rnaseq-source experimental" in cmd
        assert "--new-genes" not in cmd
        assert "--bundle-overrides" not in cmd

    def test_new_genes_bundle_overrides_and_rnaseq_source_all_append(self) -> None:
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            cmd = parca_spec.parca_command(
                new_genes="violacein_gfp",
                bundle_overrides="workspace/studies/cd2-pnnl-01-bundle-scenarios/bundles/rung5-lambda-075/overrides.tsv",
                rnaseq_source="experimental",
            )
        assert (
            "--new-genes violacein_gfp "
            "--bundle-overrides workspace/studies/cd2-pnnl-01-bundle-scenarios/bundles/rung5-lambda-075/overrides.tsv "
            "--rnaseq-source experimental"
        ) in cmd

    def test_bundle_manifest_path_appends_the_flag(self) -> None:
        """Item 451/#166: Run 4's own founder-chassis recipe needs
        --bundle-manifest-path -- a base-manifest-replacing flag, distinct from
        --bundle-overrides (which layers on top)."""
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            cmd = parca_spec.parca_command(bundle_manifest_path="out/combined_violacein.tsv")
        assert "--bundle-manifest-path out/combined_violacein.tsv" in cmd
        assert "--bundle-overrides" not in cmd

    def test_omitted_bundle_manifest_path_is_byte_identical_to_before(self) -> None:
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            assert parca_spec.parca_command(bundle_manifest_path=None) == parca_spec.parca_command()

    def test_build_combined_bundle_manifest_generates_and_points_at_default_output(self) -> None:
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            cmd = parca_spec.parca_command(build_combined_bundle_manifest=True)
        assert "python scripts/build_combined_bundle_manifest.py && " in cmd
        assert cmd.index("build_combined_bundle_manifest.py") < cmd.index("v2ecoli-parca")
        assert "--bundle-manifest-path out/combined_bundle_manifest.tsv" in cmd
        assert "--include-violacein" not in cmd

    def test_include_violacein_bundle_passes_the_generator_flag(self) -> None:
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            cmd = parca_spec.parca_command(build_combined_bundle_manifest=True, include_violacein_bundle=True)
        assert "build_combined_bundle_manifest.py --include-violacein && " in cmd

    def test_include_violacein_bundle_alone_is_a_no_op(self) -> None:
        """include_violacein_bundle only matters when build_combined_bundle_manifest is also set."""
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            cmd = parca_spec.parca_command(include_violacein_bundle=True)
            assert cmd == parca_spec.parca_command()

    def test_bundle_manifest_path_and_build_combined_bundle_manifest_are_mutually_exclusive(self) -> None:
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            pytest.raises(ValueError, match="mutually exclusive"),
        ):
            parca_spec.parca_command(
                bundle_manifest_path="out/combined_violacein.tsv", build_combined_bundle_manifest=True
            )

    def test_deterministic_hash_seed_prepends_pythonhashseed(self) -> None:
        """Item 451/#166: Run 4's own founder-chassis recipe explicitly requires
        PYTHONHASHSEED=0 for a deterministically re-derivable chassis."""
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            cmd = parca_spec.parca_command(deterministic_hash_seed=True)
        assert "PYTHONHASHSEED=0 v2ecoli-parca" in cmd

    def test_omitted_deterministic_hash_seed_is_byte_identical_to_before(self) -> None:
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            assert parca_spec.parca_command(deterministic_hash_seed=False) == parca_spec.parca_command()

    def test_run4_founder_chassis_recipe_end_to_end(self) -> None:
        """The exact real recipe from scripts/build_run4_founder_caches.py's own
        module docstring: build_combined_bundle_manifest(--include-violacein) +
        PYTHONHASHSEED=0 + --new-genes violacein_MG1655_M5 +
        --bundle-manifest-path out/combined_bundle_manifest.tsv."""
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            cmd = parca_spec.parca_command(
                new_genes="violacein_MG1655_M5",
                build_combined_bundle_manifest=True,
                include_violacein_bundle=True,
                deterministic_hash_seed=True,
            )
        assert "build_combined_bundle_manifest.py --include-violacein && " in cmd
        assert "PYTHONHASHSEED=0 v2ecoli-parca" in cmd
        assert "--new-genes violacein_MG1655_M5" in cmd
        assert "--bundle-manifest-path out/combined_bundle_manifest.tsv" in cmd

    def test_strain_flags_do_not_reach_the_build_cache_step(self) -> None:
        """SUPERSEDES the old test_strain_flags_reach_the_build_cache_step
        (v2ecoli#676-era design). scripts/build_cache.py's own real current CLI
        (confirmed live 2026-09-04, sms-ecoli study/cd2-pnnl-02-strain-sims) has
        ONLY --fixture/--cache/--media-condition/--fixed-media -- --new-genes/
        --bundle-overrides raise `unrecognized arguments`, confirmed via a real
        ParCa+build_cache dispatch that got exactly this far and crashed. Root
        cause, from build_cache.py's own current comment: a second, redundant
        write_cache_version() call that USED TO need these flags was removed
        because it re-derived a version with none of the real build_params and
        clobbered the correct one -- save_sim_input's own bundle-write already
        produces a complete, correct cache_version.json straight from sim_data
        (itself already correctly strain-specific, since v2ecoli-parca received
        the real --new-genes/--bundle-overrides flags one command earlier in
        this same chain). Restamping strain identity a second time at this step
        is not just unsupported now, it would be redundant even if it were."""
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            cmd = parca_spec.parca_command(
                new_genes="violacein_MG1655_M5", bundle_overrides="models/parca/composed_overlay.tsv"
            )
        # isolate just the build_cache.py invocation (between it and the trailing cp)
        build_step = cmd.split("&& python scripts/build_cache.py", 1)[1].split("&& cp", 1)[0]
        assert "--new-genes" not in build_step
        assert "--bundle-overrides" not in build_step
        # the flags still reach v2ecoli-parca, one command earlier in the chain
        parca_step = cmd.split("&& v2ecoli-parca", 1)[1].split("&& gzip", 1)[0]
        assert "--new-genes violacein_MG1655_M5" in parca_step
        assert "--bundle-overrides models/parca/composed_overlay.tsv" in parca_step


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
        cmd = parca_spec.new_gene_cache_command(expression=1e6, translation_efficiency=1.0)
        assert cmd == (
            f"cd {V2ECOLI_DIR}"
            f" && python scripts/build_new_gene_cache.py"
            f" --state {PARCA_CACHE_DIR}/parca_state.pkl.gz"
            f" --cache {NEW_GENE_INDUCED_CACHE_DIR}"
            f" --expression 1000000.0 --translation-efficiency 1.0"
            f" --seed 0"
        )

    def test_optional_flags_all_append(self) -> None:
        cmd = parca_spec.new_gene_cache_command(
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
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            parca_cmd = parca_spec.parca_command()
        cache_cmd = parca_spec.new_gene_cache_command(expression=1.0, translation_efficiency=1.0)
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
            patch("viva_api.simulation.dispatch._seams.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
        ):
            job_id = await service.parca.submit_new_gene_cache_job(
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


class TestBuildVariantCacheCommand:
    """_build_variant_cache_command's own flag-assembly (backlog item 451) --
    the native-gene sibling of TestBuildNewGeneCacheCommand above, for
    scripts/build_variant_cache.py (Run 4's second required config's own
    native-overexpression design screen)."""

    def test_required_flags_only(self) -> None:
        cmd = parca_spec.variant_cache_command(perturbations={"EG10073": 10.0, "EG10074": 1.0})
        assert cmd == (
            f"cd {V2ECOLI_DIR}"
            f" && python scripts/build_variant_cache.py"
            f" --state {PARCA_CACHE_DIR}/parca_state.pkl.gz"
            f" --cache {VARIANT_CACHE_DIR}"
            f' --perturbations \'{{"EG10073": 10.0, "EG10074": 1.0}}\''
            f" --seed 0"
        )

    def test_optional_flags_all_append(self) -> None:
        cmd = parca_spec.variant_cache_command(
            perturbations={"EG10073": 0.0},
            seed=7,
            fixed_media="minimal_plus_amino_acids",
        )
        assert "--seed 7" in cmd
        assert "--fixed-media minimal_plus_amino_acids" in cmd

    def test_reads_the_raw_state_parca_command_preserves(self) -> None:
        """Same matched-pair contract as new-gene-cache's own equivalent test --
        the --state path this command reads must be exactly the path
        _parca_command's own new cp step writes to."""
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            parca_cmd = parca_spec.parca_command()
        cache_cmd = parca_spec.variant_cache_command(perturbations={"EG10073": 1.0})
        written_path = f"{PARCA_CACHE_DIR}/parca_state.pkl.gz"
        assert written_path in parca_cmd
        assert f"--state {written_path}" in cache_cmd


@pytest.mark.asyncio
class TestSubmitVariantCacheJob:
    """submit_variant_cache_job (backlog item 451): sibling of
    submit_new_gene_cache_job, composing a caller-chosen native-gene
    perturbation set on top of an already-built commit cache."""

    async def test_submits_via_the_container_path_with_stage_in(self) -> None:
        mock_batch = _fake_container_batch(["variant-cache-999"])
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
        ):
            job_id = await service.parca.submit_variant_cache_job(
                commit="abc1234",
                variant="strain-design-1",
                perturbations={"EG10073": 10.0},
            )
        assert job_id == JobId.ray("variant-cache-999")
        call = mock_batch.submit_job.call_args
        assert "containerOverrides" in call.kwargs
        env = _container_env_of(call)
        assert "build_variant_cache.py" in env["CONTAINER_JOB_CMD"]
        # Same non-collision contract as new-gene-cache: stages FROM the plain
        # commit cache (source), writes TO the variant cache (derived).
        assert env["CONTAINER_STAGE_S3"] != env["CONTAINER_OUT_S3"]
        assert "strain-design-1" in env["CONTAINER_OUT_S3"]
        assert "strain-design-1" not in env["CONTAINER_STAGE_S3"]


class TestRaySubmitImage:
    """The Nextflow HEAD image for the Ray/v2ecoli path (plan-nextflow-dispatch.md §11.3).

    Only the process that runs ``nextflow run`` needs a JVM. On vEcoli's proven awsbatch
    profile the Batch TASKS run ``container = params.container_image`` -- the plain science
    image -- and v2ecoli's own Dockerfile already installs AWS CLI v2, which is the one thing
    Nextflow requires inside a task container to stage the S3 work dir. So this is a thin
    derived layer, and the task side is deliberately untouched.
    """

    def test_default_build_is_byte_identical_and_has_no_submit_layer(self) -> None:
        """Same guarantee item 87 established for include_new_gene_data: adding a flag must
        not perturb any existing caller. The unflagged script must be a strict PREFIX of the
        flagged one -- not merely 'similar'."""
        service = SimulationServiceRay()
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            default = service._image_builder().build_command(_v2ecoli_simulator())[2]
            flagged = service._image_builder().build_command(_v2ecoli_simulator(), include_submit_image=True)[2]
        assert "Dockerfile-submit" not in default
        assert "default-jre-headless" not in default
        assert flagged.startswith(default)

    def test_submit_image_adds_java_and_a_pinned_nextflow(self) -> None:
        service = SimulationServiceRay()
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            script = service._image_builder().build_command(_v2ecoli_simulator(), include_submit_image=True)[2]
        assert "default-jre-headless" in script
        # Pinned, not floating: an unpinned `nextflow` download would silently change the
        # renderer's runtime between two builds of the same commit.
        assert "ARG NEXTFLOW_VERSION=25.10.2" in script

    def test_submit_image_is_derived_from_this_commit_and_pushed_beside_it(self) -> None:
        """The head must be built FROM the same commit's task image, or the workflow it
        launches is not the code the simulator record names."""
        service = SimulationServiceRay()
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            script = service._image_builder().build_command(_v2ecoli_simulator(), include_submit_image=True)[2]
        assert "BASE_URI=$ECR_REGISTRY/v2ecoli:abc1234" in script
        assert 'docker push "$ECR_REGISTRY/v2ecoli:abc1234-submit"' in script

    def test_submit_image_workdir_is_the_repo_root(self) -> None:
        """WORKDIR /app/v2ecoli, not vEcoli's /vEcoli. v2ecoli bare-imports `scripts._compare`
        throughout, which resolves on cwd alone -- the same root cause as viva-api#359 and the
        reason the awsbatch profile must also export PYTHONPATH."""
        service = SimulationServiceRay()
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            script = service._image_builder().build_command(_v2ecoli_simulator(), include_submit_image=True)[2]
        assert "WORKDIR /app/v2ecoli" in script

    @pytest.mark.asyncio
    async def test_run_build_threads_the_flag(self) -> None:
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch(
                "viva_api.simulation.batch_build.submit_batch_build",
                new=AsyncMock(return_value="build-job-1"),
            ) as mock_submit,
            patch(
                "viva_api.simulation.batch_build.poll_batch_jobs",
                new=AsyncMock(),
            ),
        ):
            await service._image_builder().run(_v2ecoli_simulator(), include_submit_image=True)
        assert "Dockerfile-submit" in mock_submit.call_args.kwargs["command"][2]


class TestSimulationServiceRayBuildSubmit:
    """Build-image submission: DooD Batch job to the amd64 queue, then poll."""

    @pytest.mark.asyncio
    async def test_run_build_submits_to_amd64_queue_and_polls(self) -> None:
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch(
                "viva_api.simulation.batch_build.submit_batch_build",
                new=AsyncMock(return_value="build-job-1"),
            ) as mock_submit,
            patch(
                "viva_api.simulation.batch_build.poll_batch_jobs",
                new=AsyncMock(),
            ) as mock_poll,
        ):
            await service._image_builder().run(_v2ecoli_simulator())
        assert mock_submit.await_count == 1
        assert mock_submit.call_args.kwargs["queue"] == "smscdk-vecoli-build-amd64"
        assert "docker/build-and-push-ecr.sh" in mock_submit.call_args.kwargs["command"][2]
        mock_poll.assert_awaited_once_with(["build-job-1"])

    @pytest.mark.asyncio
    async def test_submit_build_returns_local_job(self) -> None:
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch(
                "viva_api.simulation.batch_build.submit_batch_build",
                new=AsyncMock(return_value="bj"),
            ),
            patch("viva_api.simulation.batch_build.poll_batch_jobs", new=AsyncMock()),
        ):
            job_id = await service.submit_build_image_job(_v2ecoli_simulator())
            # The build runs as a BACKGROUND task that outlives this call. Wait for it INSIDE
            # the patches: leaving the block first let it run against the real
            # batch_build.submit_batch_build -- a real Batch SubmitJob from a unit test,
            # found by tests/fixtures/aws_guard.py.
            await service._local.wait_finalized(job_id.value)
        assert job_id.backend == JobBackend.LOCAL

    @pytest.mark.asyncio
    async def test_submitted_build_records_its_batch_job_id_and_finalizes_its_row(self) -> None:
        """viva-api#414: the LOCAL build task persists the Batch job id it is
        polling onto its bound HpcRun row (so a restart can recover the
        build's outcome), and the row is finalized from the task's outcome."""
        service = SimulationServiceRay()
        db = MagicMock()
        db.set_hpcrun_external_job_ids = AsyncMock()
        db.update_hpcrun_status = AsyncMock()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch(
                "viva_api.simulation.batch_build.submit_batch_build",
                new=AsyncMock(return_value="build-job-1"),
            ) as mock_submit,
            patch("viva_api.simulation.batch_build.poll_batch_jobs", new=AsyncMock()),
        ):
            job_id = await service.submit_build_image_job(_v2ecoli_simulator())
            await service._local.bind_hpcrun(job_id.value, hpcrun_id=506, database_service=db)
            await service._local.wait_finalized(job_id.value)
        assert mock_submit.call_args.kwargs["job_name"] == "v2ecoli-ray-build-abc1234"
        db.set_hpcrun_external_job_ids.assert_awaited_once_with(506, ["build-job-1"])
        update = db.update_hpcrun_status.await_args.kwargs["update"]
        assert update.status == JobStatus.COMPLETED
        assert update.end_time is not None


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
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
        ):
            jd = service.batch.ensure_mnp_job_def(image, "abc1234")
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
            patch("viva_api.simulation.dispatch._seams.get_settings", _container_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
        ):
            jd = service.batch.ensure_container_job_def(image, "abc1234")
        assert jd == "smscdk-ray-container-abc1234:5"
        mock_batch.register_job_definition.assert_not_called()

    def test_registers_new_revision_cloning_base_container_properties(self) -> None:
        image = "476270107793.dkr.ecr.us-gov-west-1.amazonaws.com/v2ecoli:def5678"
        mock_batch = _fake_container_batch([])
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _container_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
        ):
            jd = service.batch.ensure_container_job_def(image, "def5678")
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
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            pytest.raises(RuntimeError, match="ray_container_job_definition"),
        ):
            service.batch.ensure_container_job_def("some-image", "abc1234")


class TestSubmitContainer:
    """_submit_container: the plain container-type submission path (backlog item
    71) -- sibling of _submit_mnp with no node overrides and the CONTAINER_* env
    contract docker/batch-container-entrypoint.sh (sms-ecoli) expects."""

    def test_submits_with_container_env_and_queue_no_node_overrides(self) -> None:
        mock_batch = MagicMock()
        mock_batch.submit_job.return_value = {"jobId": "job-1"}
        service = SimulationServiceRay()
        with patch("viva_api.simulation.dispatch._seams.get_settings", _container_settings):
            job_id = service.batch.submit_container(
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
        with patch("viva_api.simulation.dispatch._seams.get_settings", _container_settings):
            service.batch.submit_container(
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
        with patch("viva_api.simulation.dispatch._seams.get_settings", _container_settings):
            service.batch.submit_container(
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
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            pytest.raises(RuntimeError, match="ray_container_queue"),
        ):
            service.batch.submit_container(
                job_name="x", job_definition="jd:1", job_cmd="echo hi", out_s3="s3://b/", out_dir="/o"
            )

    def test_depends_on_and_tags_pass_through(self) -> None:
        mock_batch = MagicMock()
        mock_batch.submit_job.return_value = {"jobId": "job-2"}
        service = SimulationServiceRay()
        with patch("viva_api.simulation.dispatch._seams.get_settings", _container_settings):
            service.batch.submit_container(
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
            patch("viva_api.simulation.dispatch._seams.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
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
    """SubmitJobPacer proactively caps AWS Batch SubmitJob calls below the
    account-wide 50 TPS ceiling, computed from REAL elapsed wall-clock time
    (not a fixed guess) -- backing the chain-dispatch campaign's upfront N*G
    submission loop (backlog item 33 rework)."""

    @pytest.mark.asyncio
    async def test_first_call_never_sleeps(self) -> None:
        from viva_core.backends.batch import SubmitJobPacer

        pacer = SubmitJobPacer(max_per_second=40.0)
        with patch("viva_core.backends.batch.asyncio.sleep", new=AsyncMock()) as mock_sleep:
            await pacer.wait()
        mock_sleep.assert_not_called()

    @pytest.mark.asyncio
    async def test_back_to_back_calls_sleep_for_the_real_deficit(self) -> None:
        """Drives a FAKE monotonic clock so the computed sleep duration is
        exactly checkable, rather than asserting on real (flaky) wall-clock
        timing."""
        from viva_core.backends.batch import SubmitJobPacer

        pacer = SubmitJobPacer(max_per_second=10.0)  # min_interval = 0.1s
        clock = iter([100.0, 100.0, 100.02])
        with (
            patch("viva_core.backends.batch.time.monotonic", side_effect=lambda: next(clock)),
            patch("viva_core.backends.batch.asyncio.sleep", new=AsyncMock()) as mock_sleep,
        ):
            await pacer.wait()  # consumes 100.0 -- first call, no prior, no sleep
            await pacer.wait()  # now=100.0 again -> deficit = 0.1 -> sleeps, re-reads -> 100.02
        mock_sleep.assert_awaited_once()
        (slept_for,) = mock_sleep.call_args.args
        assert slept_for == pytest.approx(0.1)

    @pytest.mark.asyncio
    async def test_no_sleep_once_enough_real_time_has_elapsed(self) -> None:
        from viva_core.backends.batch import SubmitJobPacer

        pacer = SubmitJobPacer(max_per_second=10.0)  # min_interval = 0.1s
        clock = iter([100.0, 100.5])  # half a second later -- comfortably past the 0.1s floor
        with (
            patch("viva_core.backends.batch.time.monotonic", side_effect=lambda: next(clock)),
            patch("viva_core.backends.batch.asyncio.sleep", new=AsyncMock()) as mock_sleep,
        ):
            await pacer.wait()
            await pacer.wait()
        mock_sleep.assert_not_called()
