"""Tests for the Ray-on-Batch backend: JobId.ray, Batch state mapping, ComputeBackend.RAY,
and SimulationServiceRay submission/status/cancel (boto3 mocked, Postgres via testcontainers)."""

import json
import os
import shlex
import shutil
import subprocess
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sms_api.common.hpc.job_service import JobStatusInfo
from sms_api.common.models import JobBackend, JobId, JobStatus
from sms_api.config import ComputeBackend
from sms_api.simulation.simulation_service_ray import (
    PARCA_CACHE_DIR,
    SIM_OUT_DIR,
    SimulationServiceRay,
    analysis_modules_for,
)

if TYPE_CHECKING:
    from sms_api.simulation.database_service import DatabaseServiceSQL
    from sms_api.simulation.models import SimulationRequest


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
    """A boto3 Batch mock that supports the per-commit job-def derivation (both the
    MNP and Array shapes) + submits.

    describe_job_definitions returns the CDK base (with properties to clone) for
    either base name, and "no existing revision" for any per-commit name; register
    returns rev 1.
    """
    b = MagicMock()
    base_node_props = {
        "numNodes": 4,
        "mainNode": 0,
        "nodeRangeProperties": [{"targetNodes": "0:", "container": {"image": "111.dkr.ecr.x/vecoli:ray", "vcpus": 16}}],
    }
    base_container_props = {
        "image": "111.dkr.ecr.x/vecoli:ray",
        "resourceRequirements": [{"type": "VCPU", "value": "2"}, {"type": "MEMORY", "value": "16384"}],
    }

    def _describe(**kwargs: Any) -> dict[str, Any]:
        name = kwargs.get("jobDefinitionName")
        if name == "smscdk-ray-mnp":  # MNP base
            return {"jobDefinitions": [{"revision": 7, "nodeProperties": base_node_props}]}
        if name == "smscdk-ray-array":  # Array base
            return {
                "jobDefinitions": [
                    {
                        "revision": 3,
                        "containerProperties": base_container_props,
                        "retryStrategy": {"attempts": 2},
                        "platformCapabilities": ["EC2"],
                    }
                ]
            }
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


def _array_env(call: Any) -> dict[str, str]:
    """Environment dict for an Array job submission (containerOverrides, not nodeOverrides)."""
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
        from sms_api.config import get_job_backend

        with patch("sms_api.config.get_settings") as mock_settings:
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
            patch("sms_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            # data_layout builds the S3 URIs (results/cache) and reads config.get_settings directly.
            patch("sms_api.common.storage.data_layout.get_settings", _ray_settings),
            patch("sms_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
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

    async def test_submit_routes_batch_baseline_to_array_jobs_when_multiseed(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """The canonical batch_baseline sweep (n_seeds>1, generations>1, no composite
        override) is Array-jobs-shaped: dispatched as N independent single-seed AWS
        Batch Array children instead of an MNP Ray cluster -- ray-vs-batch-array-jobs
        decision: Array jobs for canonical, Ray-MNP stays for colonies/anything that
        genuinely needs Ray coordination. ParCa itself is unaffected (still a single
        MNP job -- one deterministic computation, no seed-parallelism to exploit).
        Also covers what the old MNP-routing test covered: the real request's own
        experiment_id reaches the overrides (previously silently defaulted), the
        generic run_pbg.py runner is used (not a v2ecoli-specific script), and the
        runner is staged to S3 exactly once before the sim job is built."""
        setattr(experiment_request.config, "n_init_sims", 4)  # noqa: B010
        experiment_request.config.generations = 3
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        mock_batch = _fake_batch(["parca-123", "sim-456", "analysis-789"])
        fake_file_service = AsyncMock()
        fake_file_service.upload_file = AsyncMock()

        service = SimulationServiceRay()
        with (
            patch("sms_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("sms_api.common.storage.data_layout.get_settings", _ray_settings),
            patch("sms_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
            patch("sms_api.dependencies.get_file_service", return_value=fake_file_service),
        ):
            job_id = await service.submit_ecoli_simulation_job(
                ecoli_simulation=simulation, database_service=database_service, correlation_id="corr-2"
            )

        fake_file_service.upload_file.assert_awaited_once()
        assert job_id == JobId.ray("sim-456")
        # parca -> sim(array) -> analysis: the analysis DAG node rides along.
        assert mock_batch.submit_job.call_count == 3
        parca_call, sim_call, _analysis_call = mock_batch.submit_job.call_args_list

        # ParCa: unchanged -- still a 1-node MNP job, no dependency.
        assert parca_call.kwargs["nodeOverrides"]["numNodes"] == 1
        assert "dependsOn" not in parca_call.kwargs

        # Sim: an Array job, NOT MNP -- no nodeOverrides at all.
        assert "nodeOverrides" not in sim_call.kwargs
        assert sim_call.kwargs["arrayProperties"] == {"size": 4}
        assert sim_call.kwargs["jobQueue"] == "smscdk-vecoli-task-amd64"
        # Plain dependency, NO "type" key -- real AWS Batch rejects {"type": "SEQUENTIAL"}
        # combined with an explicit jobId when the submitting job itself sets
        # arrayProperties (live error hit 2026-08-06: "Job Id cannot be set when
        # dependency type is SEQUENTIAL"). Do not "simplify" this back to match
        # _submit_mnp's shape -- that's the exact regression this assertion guards.
        assert sim_call.kwargs["dependsOn"] == [{"jobId": "parca-123"}]

        sim_env = _array_env(sim_call)
        cmd = sim_env["ARRAY_JOB_CMD"]
        assert "run_batch_baseline_ray.py" not in cmd
        assert "run_phase0_xarray_ensemble.py" not in cmd
        assert "aws s3 cp" in cmd and "/tmp/run_pbg.py" in cmd  # noqa: S108
        assert "AWS_BATCH_JOB_ARRAY_INDEX" in cmd
        # Exact match, not a substring check. sms-ecoli has no "ecoli_baseline" module
        # at all -- two real pilot dispatches (2026-08-06) failed chasing that name
        # before the real module (v2ecoli/composites/batch_baseline.py, decorated
        # name="batch_baseline") was confirmed directly against the deployed sms-ecoli
        # image at commit e38f742, never the separate/diverged local v2ecoli checkout.
        assert "--composite-id v2ecoli.composites.batch_baseline.batch_baseline " in cmd
        assert "PBG_CORE_BUILDER=v2ecoli.core:build_core" in cmd
        assert sim_env["ARRAY_OUT_DIR"] == SIM_OUT_DIR
        assert sim_env["ARRAY_OUT_S3"] == "s3://mybucket/vecoli-output/" + simulation.config.experiment_id + "/"

        # Cache hand-off: sim stages exactly what parca produced (same ARRAY_*
        # naming convention as the MNP path's RAY_* staging, renamed so the two
        # dispatch paths' env vars can never be cross-wired).
        parca_env = _env_of(parca_call)
        assert sim_env["ARRAY_STAGE_S3"] == parca_env["RAY_OUT_S3"]
        assert sim_env["ARRAY_STAGE_DIR"] == PARCA_CACHE_DIR

        # Per-commit Array job-def, cloned from the CDK base with the image swapped
        # (container jobs can't override the image via containerOverrides either --
        # verified against the real AWS Batch API, same limitation as MNP).
        simulator = await database_service.get_simulator(simulator_id=simulation.simulator_id)
        assert simulator is not None
        commit = simulator.git_commit_hash
        assert sim_call.kwargs["jobDefinition"] == f"smscdk-ray-array-{commit}:1"
        array_reg_calls = [
            c for c in mock_batch.register_job_definition.call_args_list if c.kwargs["type"] == "container"
        ]
        assert len(array_reg_calls) == 1
        assert array_reg_calls[0].kwargs["containerProperties"]["image"] == (
            f"476270107793.dkr.ecr.us-gov-west-1.amazonaws.com/v2ecoli:{commit}"
        )
        # The CDK base's retryStrategy must survive the clone -- register_job_definition
        # does not inherit it automatically from an existing revision.
        assert array_reg_calls[0].kwargs["retryStrategy"] == {"attempts": 2}

    async def test_submit_batch_baseline_single_seed_stays_on_mnp(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """AWS Batch array jobs require size>=2 (verified against the real API);
        a single-seed batch_baseline request also has no parallelism to gain from
        Array-izing, so it stays on the existing, already-correct MNP path."""
        setattr(experiment_request.config, "n_init_sims", 1)  # noqa: B010
        experiment_request.config.generations = 3
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        mock_batch = _fake_batch(["parca-123", "sim-456", "analysis-789"])
        fake_file_service = AsyncMock()
        fake_file_service.upload_file = AsyncMock()

        service = SimulationServiceRay()
        with (
            patch("sms_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("sms_api.common.storage.data_layout.get_settings", _ray_settings),
            patch("sms_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
            patch("sms_api.dependencies.get_file_service", return_value=fake_file_service),
        ):
            await service.submit_ecoli_simulation_job(
                ecoli_simulation=simulation, database_service=database_service, correlation_id="corr-single"
            )

        _, sim_call, analysis_call = mock_batch.submit_job.call_args_list
        assert "nodeOverrides" in sim_call.kwargs
        assert "arrayProperties" not in sim_call.kwargs
        assert sim_call.kwargs["jobQueue"] == "smscdk-ray-mnp"
        assert "--composite-id v2ecoli.composites.batch_baseline.batch_baseline " in _env_of(sim_call)["RAY_JOB_CMD"]
        # An MNP sim job is NOT array-shaped, so the analysis node keeps the
        # long-standing SEQUENTIAL dependency shape this path has always used.
        assert analysis_call.kwargs["dependsOn"] == [{"jobId": "sim-456", "type": "SEQUENTIAL"}]


class TestAnalysisModulesFor:
    """analysis_modules_for reads the simulation's OWN configured analyses."""

    def test_real_scale_entries_are_forwarded_verbatim(self) -> None:
        from sms_api.simulation.models import AnalysisOptions, SimulationConfig

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
        from sms_api.simulation.models import AnalysisOptions, SimulationConfig

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
            patch("sms_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("sms_api.common.storage.data_layout.get_settings", _ray_settings),
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


@pytest.mark.asyncio
class TestAnalysisDagNode:
    """Item 24: the analysis must fire from the pipeline DAG itself, with no
    separate manual step and no external watcher."""

    async def test_analysis_job_depends_on_the_array_sim_and_is_recorded(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """REGRESSION (backlog item 24): before this, the Ray backend never read
        `config.analysis_options` and submitted no analysis at all — a completed
        remote simulation produced zero cd1_*/ptools_* artifacts until somebody ran
        the CLI by hand. The analysis is now the DAG's third node, gated on the sim
        job, and tracked in the same `analyses` table the on-demand trigger uses."""
        setattr(experiment_request.config, "n_init_sims", 4)  # noqa: B010
        experiment_request.config.generations = 3
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        mock_batch = _fake_batch(["parca-123", "sim-456", "analysis-789"])
        fake_file_service = AsyncMock()
        fake_file_service.upload_file = AsyncMock()

        service = SimulationServiceRay()
        with (
            patch("sms_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("sms_api.common.storage.data_layout.get_settings", _ray_settings),
            patch("sms_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
            patch("sms_api.dependencies.get_file_service", return_value=fake_file_service),
        ):
            await service.submit_ecoli_simulation_job(
                ecoli_simulation=simulation, database_service=database_service, correlation_id="corr-analysis"
            )

        _parca_call, _sim_call, analysis_call = mock_batch.submit_job.call_args_list
        experiment_id = simulation.config.experiment_id

        # Gated on the SIM job (not ParCa): the sweep is only whole once every
        # array child's output has landed in S3.
        assert analysis_call.kwargs["dependsOn"] == [{"jobId": "sim-456"}]
        # An array parent id under a SEQUENTIAL type is rejected by real AWS Batch —
        # this is the shape that guard produces, and must not be "simplified" back.
        assert "type" not in analysis_call.kwargs["dependsOn"][0]
        assert analysis_call.kwargs["nodeOverrides"]["numNodes"] == 1

        env = _env_of(analysis_call)
        assert "run_standalone_analysis.py" in env["RAY_JOB_CMD"]
        assert f"--out-uri s3://mybucket/vecoli-output/{experiment_id}" in env["RAY_JOB_CMD"]
        assert "--n-seeds 4" in env["RAY_JOB_CMD"] and "--n-generations 3" in env["RAY_JOB_CMD"]
        # No ParCa staging: sim_data is named explicitly as an S3 URI instead.
        assert "RAY_STAGE_S3" not in env
        assert "V2ECOLI_SIM_DATA=s3://mybucket/ray-parca-cache/" in env["RAY_JOB_CMD"]
        assert analysis_call.kwargs["tags"]["Phase"] == "analysis"

        # Tracked: GET /simulations/{id}/analyses and GET /analyses/{id}/status must
        # both resolve an auto-triggered analysis, exactly like a hand-triggered one.
        records = await database_service.list_analyses(simulation_id=simulation.database_id)
        assert len(records) == 1
        record = records[0]
        assert record.backend == "ray"
        assert record.job_id_ext == "analysis-789"
        assert record.status == JobStatus.RUNNING
        # result_uri is where the job's own _manifest.json lands — the S3-exists probe
        # in handle_get_ray_analysis_status reads exactly this path.
        assert record.result_uri == f"s3://mybucket/vecoli-output/{experiment_id}/analyses/{record.name}"
        assert f"--analysis-name {record.name}" in env["RAY_JOB_CMD"]

    async def test_configured_analysis_options_reach_the_analysis_job(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """REGRESSION: `config.analysis_options` (set by the run endpoint from the
        caller's --analysis-options, and by the workbench from a study's
        spec.analyses) was read by no Ray code path at all."""
        from sms_api.simulation.models import AnalysisOptions

        setattr(experiment_request.config, "n_init_sims", 4)  # noqa: B010
        experiment_request.config.generations = 3
        experiment_request.config.analysis_options = AnalysisOptions.model_validate({
            "multiseed": {"cd1_fluxomics": {"generation_lower_bound": 5}}
        })
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        mock_batch = _fake_batch(["parca-123", "sim-456", "analysis-789"])
        fake_file_service = AsyncMock()
        fake_file_service.upload_file = AsyncMock()

        service = SimulationServiceRay()
        with (
            patch("sms_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("sms_api.common.storage.data_layout.get_settings", _ray_settings),
            patch("sms_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
            patch("sms_api.dependencies.get_file_service", return_value=fake_file_service),
        ):
            await service.submit_ecoli_simulation_job(
                ecoli_simulation=simulation, database_service=database_service, correlation_id="corr-opts"
            )

        cmd = _env_of(mock_batch.submit_job.call_args_list[2])["RAY_JOB_CMD"]
        tokens = shlex.split(cmd.split("&&", 1)[1].replace("V2ECOLI_SIM_DATA=", "", 1))
        assert json.loads(tokens[tokens.index("--modules") + 1]) == {
            "multiseed": {"cd1_fluxomics": {"generation_lower_bound": 5}}
        }

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
            patch("sms_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("sms_api.common.storage.data_layout.get_settings", _ray_settings),
            patch("sms_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
        ):
            await service.submit_ecoli_simulation_job(
                ecoli_simulation=simulation, database_service=database_service, correlation_id="corr-phase0"
            )

        assert mock_batch.submit_job.call_count == 2
        assert await database_service.list_analyses(simulation_id=simulation.database_id) == []

    async def test_a_failed_analysis_submission_is_recorded_not_swallowed(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """The sim job is already running by then, so raising would orphan a real,
        expensive job — but a silently-missing analysis is the exact failure mode
        item 24 exists to eliminate. It must land as a FAILED row instead."""
        setattr(experiment_request.config, "n_init_sims", 4)  # noqa: B010
        experiment_request.config.generations = 3
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        mock_batch = _fake_batch(["parca-123", "sim-456"])
        submits = [{"jobId": "parca-123"}, {"jobId": "sim-456"}]

        def _submit(**kwargs: Any) -> dict[str, str]:
            if submits:
                return submits.pop(0)
            raise RuntimeError("Batch said no")

        mock_batch.submit_job.side_effect = _submit
        fake_file_service = AsyncMock()
        fake_file_service.upload_file = AsyncMock()

        service = SimulationServiceRay()
        with (
            patch("sms_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("sms_api.common.storage.data_layout.get_settings", _ray_settings),
            patch("sms_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
            patch("sms_api.dependencies.get_file_service", return_value=fake_file_service),
        ):
            job_id = await service.submit_ecoli_simulation_job(
                ecoli_simulation=simulation, database_service=database_service, correlation_id="corr-fail"
            )

        # The simulation dispatch itself still succeeds and stays tracked.
        assert job_id == JobId.ray("sim-456")
        records = await database_service.list_analyses(simulation_id=simulation.database_id)
        assert len(records) == 1
        assert records[0].status == JobStatus.FAILED
        assert "Batch said no" in (records[0].error_message or "")


@pytest.mark.asyncio
class TestSimulationServiceRayStatusCancel:
    async def test_get_job_status_running(self) -> None:
        mock_batch = MagicMock()
        mock_batch.describe_jobs.return_value = {"jobs": [{"jobId": "sim-456", "status": "RUNNING", "startedAt": 111}]}
        service = SimulationServiceRay()
        with (
            patch("sms_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("sms_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
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
            patch("sms_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("sms_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
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
            patch("sms_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("sms_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
        ):
            await service.cancel_job(JobId.ray("sim-456"))
        mock_batch.terminate_job.assert_called_once()
        assert mock_batch.terminate_job.call_args.kwargs["jobId"] == "sim-456"


def _v2ecoli_simulator() -> Any:
    from sms_api.simulation.models import SimulatorVersion

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
        with patch("sms_api.simulation.simulation_service_ray.get_settings", _ray_settings):
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
        with patch("sms_api.simulation.simulation_service_ray.get_settings", _ray_settings):
            cmd = service._sim_command(n_seeds=1, n_steps=10, chunk=4, composite="v2ecoli")
        assert "run_comparison_ensemble.py" in cmd
        assert "--max-generations 1" in cmd
        assert "--max-generations 16" not in cmd

    def test_sim_command_composite_honors_explicit_generations(self) -> None:
        service = SimulationServiceRay()
        with patch("sms_api.simulation.simulation_service_ray.get_settings", _ray_settings):
            cmd = service._sim_command(n_seeds=1, n_steps=10, chunk=4, composite="v2ecoli", max_generations=5)
        assert "--max-generations 5" in cmd

    def test_sim_command_defaults_to_single_generation_phase0(self) -> None:
        """No composite, no generations requested: unchanged, verified-working
        single-generation dispatch -- must not regress by default."""
        service = SimulationServiceRay()
        with patch("sms_api.simulation.simulation_service_ray.get_settings", _ray_settings):
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
        with patch("sms_api.simulation.simulation_service_ray.get_settings", _ray_settings):
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
        # Exact match, not a substring check. sms-ecoli has no "ecoli_baseline" module
        # at all -- two real pilot dispatches (2026-08-06) failed chasing that name
        # before the real module (v2ecoli/composites/batch_baseline.py, decorated
        # name="batch_baseline") was confirmed directly against the deployed sms-ecoli
        # image at commit e38f742, never the separate/diverged local v2ecoli checkout.
        assert "--composite-id v2ecoli.composites.batch_baseline.batch_baseline " in cmd
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
        with patch("sms_api.simulation.simulation_service_ray.get_settings", _ray_settings):
            with pytest.raises(RuntimeError, match="experiment_id"):
                service._sim_command(n_seeds=2, n_steps=600, chunk=60, n_generations=3, runner_s3_uri="s3://x/y.py")
            with pytest.raises(RuntimeError, match="runner_s3_uri"):
                service._sim_command(n_seeds=2, n_steps=600, chunk=60, n_generations=3, experiment_id="exp-1")

    def test_sim_command_composite_takes_precedence_over_n_generations(self) -> None:
        """The comparison driver's own --max-generations flag is a separate knob
        from plain n_generations -- composite selection wins regardless."""
        service = SimulationServiceRay()
        with patch("sms_api.simulation.simulation_service_ray.get_settings", _ray_settings):
            cmd = service._sim_command(n_seeds=1, n_steps=10, chunk=4, composite="v2ecoli", n_generations=3)
        assert "run_comparison_ensemble.py" in cmd
        assert "run_batch_baseline_ray.py" not in cmd

    def test_sim_command_vecoli_source_only_appended_for_upstream_vecoli(self) -> None:
        """--vecoli-source is meaningful only for --composite vecoli."""
        service = SimulationServiceRay()
        with patch("sms_api.simulation.simulation_service_ray.get_settings", _ray_settings):
            vecoli = service._sim_command(
                n_seeds=1, n_steps=10, chunk=4, composite="vecoli", vecoli_source="vivarium-process"
            )
            v2ecoli = service._sim_command(
                n_seeds=1, n_steps=10, chunk=4, composite="v2ecoli", vecoli_source="vivarium-process"
            )
        assert "--vecoli-source vivarium-process" in vecoli
        # v2ecoli engine ignores vecoli_source (guarded by _is_upstream_vecoli)
        assert "--vecoli-source" not in v2ecoli


class TestArraySimCommand:
    """_array_sim_command builds one Array child's run_pbg.py invocation."""

    def test_shape(self) -> None:
        service = SimulationServiceRay()
        cmd = service._array_sim_command(
            n_generations=3,
            experiment_id="sim47-real-experiment",
            runner_s3_uri="s3://mybucket/vecoli-output/sim47-real-experiment/run_pbg.py",
        )
        assert "run_batch_baseline_ray.py" not in cmd
        assert "run_phase0_xarray_ensemble.py" not in cmd
        assert "aws s3 cp s3://mybucket/vecoli-output/sim47-real-experiment/run_pbg.py /tmp/run_pbg.py" in cmd
        assert "--composite-id v2ecoli.composites.batch_baseline.batch_baseline " in cmd
        assert "PBG_CORE_BUILDER=v2ecoli.core:build_core" in cmd
        assert "-n 1" in cmd
        # base_seed is resolved by the shell at container-start time (AWS_BATCH_JOB_
        # ARRAY_INDEX is only known once the container starts), not baked in here.
        assert "BASE_SEED=$((0 + AWS_BATCH_JOB_ARRAY_INDEX))" in cmd

    def test_base_seed_offset_propagates_into_the_arithmetic_expansion(self) -> None:
        service = SimulationServiceRay()
        cmd = service._array_sim_command(
            n_generations=1, experiment_id="exp-1", runner_s3_uri="s3://b/run_pbg.py", base_seed_offset=100
        )
        assert "BASE_SEED=$((100 + AWS_BATCH_JOB_ARRAY_INDEX))" in cmd

    def test_merge_produces_correct_overrides_and_is_safe_against_shell_metacharacters(self) -> None:
        """Actually run the BASE_SEED/OVERRIDES merge prefix through bash + python3
        (no mocking) with a hostile experiment_id -- experiment_id is a caller-
        supplied, unconstrained string (no pattern validation at the model/API
        boundary), so this proves the shlex-quoted static blob keeps arbitrary
        content as DATA rather than shell syntax, AND that
        AWS_BATCH_JOB_ARRAY_INDEX correctly resolves into the merged JSON --
        the actual mechanism the entrypoint runs, not a re-derivation of it."""
        service = SimulationServiceRay()
        hostile = "exp'; touch /tmp/array-sim-command-injection-canary; echo '$(echo pwned)"
        cmd = service._array_sim_command(
            n_generations=5,
            experiment_id=hostile,
            runner_s3_uri="s3://mybucket/vecoli-output/exp/run_pbg.py",
            base_seed_offset=10,
        )
        # Everything before "&& cd {V2ECOLI_DIR}" is the self-contained BASE_SEED/
        # OVERRIDES merge -- safe to actually execute (arithmetic + one python3
        # subprocess call, no aws/v2ecoli dependency).
        merge_prefix = cmd.split(" && cd ", 1)[0]
        canary = "/tmp/array-sim-command-injection-canary"  # noqa: S108
        if os.path.exists(canary):
            os.remove(canary)
        bash = shutil.which("bash")
        assert bash is not None, "bash not found on PATH"
        try:
            result = subprocess.run(  # noqa: S603 -- test-only, fixed literal args
                [bash, "-c", merge_prefix + ' && printf "%s" "$OVERRIDES"'],
                env={**os.environ, "AWS_BATCH_JOB_ARRAY_INDEX": "7"},
                capture_output=True,
                text=True,
                timeout=10,
            )
            assert result.returncode == 0, result.stderr
            assert not os.path.exists(canary), "hostile experiment_id executed as shell syntax, not data"
            merged = json.loads(result.stdout)
            assert merged == {
                "n_seeds": 1,
                "n_generations": 5,
                "cache_dir": PARCA_CACHE_DIR,
                "out_dir": SIM_OUT_DIR,
                "experiment_id": hostile,
                "analyses": "none",
                "parallel": "",
                "base_seed": 17,  # base_seed_offset(10) + AWS_BATCH_JOB_ARRAY_INDEX(7)
            }
        finally:
            if os.path.exists(canary):
                os.remove(canary)


class TestIsUpstreamVecoli:
    """The single routing predicate shared by submit_ecoli_simulation_job and _sim_command."""

    def test_only_vecoli_is_upstream(self) -> None:
        from sms_api.simulation.simulation_service_ray import _is_upstream_vecoli

        assert _is_upstream_vecoli("vecoli") is True
        assert _is_upstream_vecoli("v2ecoli") is False
        assert _is_upstream_vecoli(None) is False


class TestSimulationServiceRayBuildSubmit:
    """Build-image submission: DooD Batch job to the amd64 queue, then poll."""

    @pytest.mark.asyncio
    async def test_run_build_submits_to_amd64_queue_and_polls(self) -> None:
        service = SimulationServiceRay()
        with (
            patch("sms_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch(
                "sms_api.simulation.simulation_service_ray.batch_build.submit_batch_build",
                new=AsyncMock(return_value="build-job-1"),
            ) as mock_submit,
            patch(
                "sms_api.simulation.simulation_service_ray.batch_build.poll_batch_jobs",
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
            patch("sms_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch(
                "sms_api.simulation.simulation_service_ray.batch_build.submit_batch_build",
                new=AsyncMock(return_value="bj"),
            ),
            patch("sms_api.simulation.simulation_service_ray.batch_build.poll_batch_jobs", new=AsyncMock()),
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
            patch("sms_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("sms_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
        ):
            jd = service._ensure_mnp_job_def(image, "abc1234")
        assert jd == "smscdk-ray-mnp-abc1234:5"
        mock_batch.register_job_definition.assert_not_called()


class TestEnsureArrayJobDef:
    """Per-commit Array job-def derivation. Container jobs can't override the image
    via containerOverrides either (verified against the real AWS Batch API: only
    EKS jobs' eksPropertiesOverride has an image field) -- same limitation as MNP,
    just for a different reason, so this mirrors _ensure_mnp_job_def's shape."""

    def test_reuses_existing_revision_for_same_image(self) -> None:
        image = "476270107793.dkr.ecr.us-gov-west-1.amazonaws.com/v2ecoli:abc1234"
        mock_batch = MagicMock()
        mock_batch.describe_job_definitions.return_value = {
            "jobDefinitions": [{"revision": 5, "containerProperties": {"image": image}}]
        }
        service = SimulationServiceRay()
        with (
            patch("sms_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("sms_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
        ):
            jd = service._ensure_array_job_def(image, "abc1234")
        assert jd == "smscdk-ray-array-abc1234:5"
        mock_batch.register_job_definition.assert_not_called()

    def test_clones_base_and_carries_forward_retry_strategy_and_platform_capabilities(self) -> None:
        mock_batch = MagicMock()

        def _describe(**kwargs: Any) -> dict[str, Any]:
            if kwargs.get("jobDefinitionName") == "smscdk-ray-array":
                return {
                    "jobDefinitions": [
                        {
                            "revision": 3,
                            "containerProperties": {
                                "image": "old:tag",
                                "resourceRequirements": [{"type": "VCPU", "value": "2"}],
                            },
                            "retryStrategy": {"attempts": 2},
                            "platformCapabilities": ["EC2"],
                        }
                    ]
                }
            return {"jobDefinitions": []}  # per-commit: none yet

        mock_batch.describe_job_definitions.side_effect = _describe
        mock_batch.register_job_definition.side_effect = lambda **kw: {
            "jobDefinitionName": kw["jobDefinitionName"],
            "revision": 1,
        }
        service = SimulationServiceRay()
        new_image = "476270107793.dkr.ecr.us-gov-west-1.amazonaws.com/v2ecoli:def5678"
        with (
            patch("sms_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("sms_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
        ):
            jd = service._ensure_array_job_def(new_image, "def5678")
        assert jd == "smscdk-ray-array-def5678:1"

        reg = mock_batch.register_job_definition.call_args
        assert reg.kwargs["type"] == "container"
        assert reg.kwargs["containerProperties"]["image"] == new_image
        # Everything else the base sets survives the clone, not just the image.
        assert reg.kwargs["containerProperties"]["resourceRequirements"] == [{"type": "VCPU", "value": "2"}]
        assert reg.kwargs["retryStrategy"] == {"attempts": 2}
        assert reg.kwargs["platformCapabilities"] == ["EC2"]

    def test_raises_if_base_job_def_missing(self) -> None:
        mock_batch = MagicMock()
        mock_batch.describe_job_definitions.return_value = {"jobDefinitions": []}
        service = SimulationServiceRay()
        with (
            patch("sms_api.simulation.simulation_service_ray.get_settings", _ray_settings),
            patch("sms_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
            pytest.raises(RuntimeError, match="Base Array job definition"),
        ):
            service._ensure_array_job_def("some:image", "abc1234")
