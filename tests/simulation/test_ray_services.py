"""The build and task services on their own: no ``SimulationServiceRay`` anywhere.

That they CAN be tested like this is the reason they are services and not mixins
(``docs/plan-core.md`` decision log, 2026-09-20). ``test_ray_backend.py`` and
``test_task_run.py`` still exercise them through the real service.
"""

from collections.abc import Coroutine
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tests.simulation.test_ray_backend import _ray_settings, _v2ecoli_simulator
from viva_api.common.models import JobId, JobStatus
from viva_api.simulation import batch_build
from viva_api.simulation.models import TaskRunRequest
from viva_api.simulation.ray.build import RayImageBuilder
from viva_api.simulation.ray.image_paths import TASK_OUT_DIR
from viva_api.simulation.ray.tasks import RayTaskService

if TYPE_CHECKING:
    from viva_api.simulation.models import Simulation


class FakeLocalTasks:
    def __init__(self) -> None:
        self.submitted: list[str] = []
        self.recorded: list[list[str]] = []

    def submit(self, coro: Coroutine[Any, Any, None], name: str) -> JobId:
        coro.close()  # the build itself is exercised by ``run`` below
        self.submitted.append(name)
        return JobId.local("local-1")

    async def record_external_job_ids(self, job_ids: list[str]) -> bool:
        self.recorded.append(job_ids)
        return True


@pytest.mark.asyncio
async def test_the_builder_needs_a_local_task_service_and_nothing_else() -> None:
    local = FakeLocalTasks()
    builder = RayImageBuilder(local)  # type: ignore[arg-type]
    with patch("viva_api.simulation.ray._seams.get_settings", _ray_settings):
        job_id = await builder.submit(_v2ecoli_simulator())
    assert job_id == JobId.local("local-1")
    assert local.submitted == [f"ray-build-{_v2ecoli_simulator().git_commit_hash}"]


@pytest.mark.asyncio
async def test_a_build_records_its_batch_job_before_it_waits_on_it() -> None:
    """viva-api#414: the Batch handle must be on the row before the long poll, so the build's
    outcome survives the pod that started it."""
    local = FakeLocalTasks()
    order: list[str] = []

    async def submit(**kwargs: Any) -> str:
        order.append("submit")
        assert kwargs["queue"] == _ray_settings().build_amd64_queue
        return "build-job-1"

    async def poll(job_ids: list[str]) -> None:
        assert local.recorded == [["build-job-1"]], "polled before the job id was recorded"
        order.append("poll")

    with (
        patch("viva_api.simulation.ray._seams.get_settings", _ray_settings),
        patch.object(batch_build, "submit_batch_build", submit),
        patch.object(batch_build, "poll_batch_jobs", poll),
    ):
        await RayImageBuilder(local).run(_v2ecoli_simulator())  # type: ignore[arg-type]
    assert order == ["submit", "poll"]


async def _latest_commit() -> str:
    return "latestsha"


def _results_uri(experiment_id: str) -> str:
    return f"s3://bucket/out/{experiment_id}/"


class FakeBatch:
    """The whole of ``TaskBatch``, in twenty-five lines -- which is the point."""

    def __init__(self) -> None:
        self.submitted: list[dict[str, Any]] = []
        self.statuses: dict[str, JobStatus] = {}

    def image_uri(self, commit: str) -> str:
        return f"registry/image:{commit}"

    def ensure_container_job_def(self, image: str, commit: str) -> str:
        return f"jobdef-{commit}:1"

    def submit_container(self, **kwargs: Any) -> str:
        self.submitted.append(kwargs)
        return "batch-job-1"

    def get_batch_job_statuses(self, job_ids: list[str]) -> dict[str, JobStatus]:
        return {j: self.statuses[j] for j in job_ids if j in self.statuses}

    def client(self) -> Any:
        return MagicMock()

    def resolve_log_group(self, job_definition: str | None) -> str | None:
        return None


@pytest.mark.asyncio
async def test_a_task_is_one_container_job_recorded_on_the_task_table() -> None:
    dispatch, database = FakeBatch(), MagicMock()
    database.record_task = AsyncMock(return_value="the-task-row")
    request = TaskRunRequest(script="scripts/x.py", args=["--k", "v w"], sim_data_refs={"a": "s3://x"})

    result: Any = await RayTaskService(dispatch, latest_commit=_latest_commit, results_uri=_results_uri).submit_task(
        request, database
    )
    assert result == "the-task-row"

    (job,) = dispatch.submitted
    assert job["job_definition"] == "jobdef-latestsha:1"  # no commit given: the latest
    assert job["job_cmd"] == "python scripts/x.py --k 'v w'"
    assert job["out_dir"] == TASK_OUT_DIR
    assert job["out_s3"].startswith("s3://bucket/out/tasks/x-")
    assert job["task_env"] == {"TASK_SIM_DATA_REFS": '{"a": "s3://x"}'}
    recorded = database.record_task.await_args.kwargs
    assert (recorded["job_id_ext"], recorded["out_uri"]) == ("batch-job-1", job["out_s3"])


@pytest.mark.asyncio
async def test_a_task_status_is_what_batch_says_or_unchanged_when_batch_does_not_know() -> None:
    dispatch, database = FakeBatch(), MagicMock()
    task = SimpleNamespace(job_id_ext="batch-job-1", status=JobStatus.RUNNING)
    database.get_task = AsyncMock(return_value=task)
    database.update_task_status = AsyncMock(return_value="updated")
    service = RayTaskService(dispatch, latest_commit=_latest_commit, results_uri=_results_uri)

    unchanged: Any = await service.get_task_status(1, database)
    assert unchanged is task  # Batch has not heard of it yet
    database.update_task_status.assert_not_awaited()

    dispatch.statuses["batch-job-1"] = JobStatus.FAILED
    updated: Any = await service.get_task_status(1, database)
    assert updated == "updated"


# ------------------------------------------------------------------ one Batch layer, shared


@pytest.mark.asyncio
async def test_everything_that_submits_is_handed_the_same_batch_layer() -> None:
    """``service.batch`` is ONE object for the service's lifetime, and the composed services are
    handed that object -- not a copy, not a fresh one. That is what lets a test (or a
    deployment that wraps the layer) change one place and reach every submitter. A service
    built around its own ``RayBatchLayer()`` would behave identically against real AWS and
    silently ignore the patch; no differential run can see that, so it is pinned here."""
    from viva_api.simulation.ray.batch_layer import RayBatchLayer
    from viva_api.simulation.simulation_service_ray import SimulationServiceRay

    service = SimulationServiceRay()
    assert isinstance(service.batch, RayBatchLayer)
    assert service.batch is service.batch  # an attribute, not a property that builds one per access

    submitted: list[str] = []

    def submit(**kwargs: Any) -> str:
        submitted.append(kwargs["job_name"])
        return "batch-job-1"

    simulator = SimpleNamespace(environment_key="abc1234")
    dataset = SimpleNamespace(parca_dataset_request=SimpleNamespace(simulator_version=simulator))
    database = MagicMock()
    database.record_task = AsyncMock(return_value="row")
    with (
        patch("viva_api.simulation.ray._seams.get_settings", _ray_settings),
        patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
        patch.object(service.batch, "ensure_container_job_def", return_value="jobdef:1"),
        patch.object(service.batch, "submit_container", side_effect=submit),
    ):
        await service.submit_parca_job(dataset)  # type: ignore[arg-type]
        await service.parca.submit_variant_cache_job(commit="abc1234", variant="kd", perturbations={"EG1": 2.0})
        await service.tasks.submit_task(TaskRunRequest(script="scripts/x.py", commit="abc1234"), database)
        # ...and a dispatch STRATEGY is handed it too (P2.1 PR 7): two jobs, ParCa then the run.
        database.get_simulator = AsyncMock(return_value=simulator)
        database.insert_hpcrun = AsyncMock()
        run = SimpleNamespace(simulator_id=1, database_id=2, config=SimpleNamespace(experiment_id="exp", task_env=None))
        await service._mbp_tracked().submit(run, database, {"variant": "v"}, correlation_id="c")  # type: ignore[arg-type]

    assert [name.split("-")[0] for name in submitted] == ["ray", "variant", "task", "mbp", "mbp"], submitted


def test_a_layer_handed_to_the_constructor_is_the_one_the_service_uses() -> None:
    """The seam PR 6 needs: compose builds ONE layer and may share it, and a strategy is
    handed the service's. Defaulting to a fresh ``RayBatchLayer()`` must not override one given."""
    from viva_api.simulation.ray.batch_layer import RayBatchLayer
    from viva_api.simulation.simulation_service_ray import SimulationServiceRay

    layer = RayBatchLayer()
    assert SimulationServiceRay(batch=layer).batch is layer


# ------------------------------------------------------------------ a strategy is handed a submitter, and nothing else


class OnlyASubmitter:
    """The whole of ``ContainerSubmitter``. If ``MbpTrackedStrategy`` reached for anything else of
    the simulation service -- its database, its other backends, a sibling mechanism -- this would
    not be enough to run it."""

    def __init__(self) -> None:
        self.submitted: list[dict[str, Any]] = []

    def image_uri(self, commit: str) -> str:
        return f"registry/image:{commit}"

    def ensure_container_job_def(self, image: str, commit: str) -> str:
        return f"jobdef-{commit}:1"

    def submit_container(self, **kwargs: Any) -> str:
        self.submitted.append(kwargs)
        return f"batch-job-{len(self.submitted)}"


@pytest.mark.asyncio
async def test_the_mbp_tracked_strategy_runs_on_a_submitter_alone() -> None:
    from viva_api.simulation.ray.mbp_tracked import MbpTrackedStrategy

    batch, database = OnlyASubmitter(), MagicMock()
    database.get_simulator = AsyncMock(return_value=SimpleNamespace(environment_key="tmp-abc1234-0a1b2c"))
    database.insert_hpcrun = AsyncMock()
    run = SimpleNamespace(simulator_id=1, database_id=42, config=SimpleNamespace(experiment_id="exp-1", task_env=None))

    with (
        patch("viva_api.simulation.ray._seams.get_settings", _ray_settings),
        patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
    ):
        job_id = await MbpTrackedStrategy(batch).submit(run, database, {"variant": "v"}, correlation_id="corr")  # type: ignore[arg-type]

    parca, tracked = batch.submitted
    assert job_id == JobId.ray("batch-job-2")
    assert parca["job_name"].startswith("mbp-parca-tmp-abc1234-0a1b2c-")  # the environment key, not the commit (D11)
    assert tracked["depends_on"] == ["batch-job-1"] and tracked["stage_s3"] == parca["out_s3"]
    # the ParCa job is written down as a companion, so a cancel can stop it (viva-api#709)
    assert database.insert_hpcrun.await_args.kwargs["external_job_ids"] == ["batch-job-1"]


class OnlyNextflowBatch:
    """The whole of ``NextflowBatch``: two image names and the engine. This mechanism submits
    nothing through the layer, so it is handed no way to."""

    def __init__(self) -> None:
        self.engine_calls = 0

    def image_uri(self, commit: str) -> str:
        return f"registry/image:{commit}"

    def submit_image_uri(self, commit: str) -> str:
        return f"registry/image:{commit}-submit"

    def engine(self) -> Any:
        self.engine_calls += 1
        engine = MagicMock()
        engine.terminate_matching.return_value = 3
        return engine


@pytest.mark.asyncio
async def test_the_nextflow_strategy_runs_on_two_image_names_a_cluster_and_a_runner_stager() -> None:
    from viva_api.simulation.ray import nextflow

    batch, k8s, staged = OnlyNextflowBatch(), MagicMock(), []

    async def stage_runner(experiment_id: str) -> str:
        staged.append(experiment_id)
        return f"s3://bucket/{experiment_id}/run_pbg.py"

    database = MagicMock()
    database.get_simulator = AsyncMock(return_value=SimpleNamespace(environment_key="tmp-abc1234-0a1b2c"))
    run = SimpleNamespace(
        simulator_id=1,
        database_id=9,
        experiment_id="run-a1b2",
        config=SimpleNamespace(experiment_id="run-a1b2", task_env=None),
    )
    strategy = nextflow.NextflowStrategy(batch, k8s, stage_runner=stage_runner)
    with (
        patch("viva_api.simulation.ray._seams.get_settings", _ray_settings),
        patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
        patch("viva_api.simulation.ray.nextflow.stage_render_nf", new=AsyncMock(return_value="s3://bucket/r.py")),
    ):
        dispatch = {"composite_id": "pkg.composites.workflow_nf"}
        job_id = await strategy.submit(cast("Simulation", run), database, dispatch, correlation_id="c")

    assert job_id.backend.name == "K8S_NEXTFLOW" and staged == ["run-a1b2"]
    (head,) = [call.args[0] for call in k8s.create_job.call_args_list]
    assert (
        head.spec.template.spec.containers[0].image == "registry/image:tmp-abc1234-0a1b2c-submit"
    )  # D11: the key, not the commit

    # ...and the reap: only once the head is gone, and through the engine it was handed
    k8s.get_job_status.return_value = "still there"
    assert await strategy.reap_cancelled_campaign("nf-sim9-run-a1b2-xyz123") is None and batch.engine_calls == 0
    k8s.get_job_status.return_value = None
    with patch("viva_api.simulation.ray._seams.get_settings", _ray_settings):
        assert await strategy.reap_cancelled_campaign("nf-sim9-run-a1b2-xyz123") == 3
    assert batch.engine_calls == 1


@pytest.mark.asyncio
async def test_the_services_nextflow_strategy_is_handed_the_services_own_layer_and_cluster() -> None:
    from viva_api.simulation.simulation_service_ray import SimulationServiceRay

    k8s = MagicMock()
    k8s.get_job_status.return_value = None
    service = SimulationServiceRay(k8s_job_service=k8s)
    engine = MagicMock()
    engine.terminate_matching.return_value = 5
    with (
        patch("viva_api.simulation.ray._seams.get_settings", _ray_settings),
        patch.object(service.batch, "engine", return_value=engine),
    ):
        assert await service.reap_cancelled_campaign("nf-sim9-run-a1b2-xyz123") == 5
    k8s.get_job_status.assert_called_once_with("nf-sim9-run-a1b2-xyz123")


class OnlyMultiNodeBatch(OnlyASubmitter):
    """The whole of ``MultiNodeBatch``: an MNP submitter for the run and its ParCa, a container
    submitter for the analysis (inherited), and the client for one read of a job definition."""

    def __init__(self, vcpus: str = "16") -> None:
        super().__init__()
        self.mnp: list[dict[str, Any]] = []
        self._vcpus = vcpus

    def ensure_mnp_job_def(self, image: str, commit: str) -> str:
        return f"mnp-{commit}:1"

    def submit_mnp(self, **kwargs: Any) -> str:
        self.mnp.append(kwargs)
        return f"mnp-job-{len(self.mnp)}"

    def client(self) -> Any:
        requirement = {"type": "VCPU", "value": self._vcpus}
        ranges = [{"container": {"resourceRequirements": [requirement]}}]
        client = MagicMock()
        client.describe_job_definitions.return_value = {
            "jobDefinitions": [{"nodeProperties": {"nodeRangeProperties": ranges}}]
        }
        return client


@pytest.mark.asyncio
async def test_the_multi_node_strategy_runs_on_a_batch_and_a_runner_stager_and_sizes_its_shards() -> None:
    """...and nothing else of the service. Also the unit-level form of viva-api#730's fix: the shard
    count a run receives is the job definition's vCPUs times its nodes."""
    from viva_api.simulation.ray.multi_node import MultiNodeCompositeStrategy

    async def stage_runner(experiment_id: str) -> str:
        return f"s3://bucket/{experiment_id}/run_pbg.py"

    batch, database = OnlyMultiNodeBatch(vcpus="16"), MagicMock()
    database.get_simulator = AsyncMock(return_value=SimpleNamespace(environment_key="tmp-abc1234-0a1b2c"))
    database.insert_hpcrun = AsyncMock()
    run = SimpleNamespace(
        simulator_id=1,
        database_id=42,
        experiment_id="exp-1",
        config=SimpleNamespace(experiment_id="exp-1", task_env=None),
    )
    dispatch = {"composite_id": "pkg.composites.colony", "num_nodes": 2}
    with (
        patch("viva_api.simulation.ray._seams.get_settings", _ray_settings),
        patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
    ):
        strategy = MultiNodeCompositeStrategy(batch, stage_runner=stage_runner)
        await strategy.submit(cast("Simulation", run), database, dispatch, correlation_id="corr")

    parca, composite = batch.mnp
    assert parca["job_name"].startswith("ray-parca-tmp-abc1234-0a1b2c-")  # the environment key (D11)
    assert composite["num_nodes"] == 2 and composite["depends_on"] == ["mnp-job-1"]
    assert "RAY_SHARDS_DEFAULT=32" in composite["ray_job_cmd"]  # 16 vCPUs x 2 nodes (#730)
    assert batch.submitted == []  # no container job: the analysis comes later, from the scheduler


@pytest.mark.asyncio
async def test_the_services_multi_node_strategy_is_handed_the_services_own_layer_and_stager() -> None:
    from viva_api.simulation.simulation_service_ray import SimulationServiceRay

    service, database, submitted = SimulationServiceRay(), MagicMock(), []
    database.get_simulator = AsyncMock(return_value=SimpleNamespace(environment_key="abc1234"))
    database.insert_hpcrun = AsyncMock()
    run = SimpleNamespace(
        simulator_id=1,
        database_id=42,
        experiment_id="exp-1",
        config=SimpleNamespace(experiment_id="exp-1", task_env=None),
    )

    def submit_mnp(**kwargs: Any) -> str:
        submitted.append(kwargs["job_name"])
        return f"job-{len(submitted)}"

    with (
        patch("viva_api.simulation.ray._seams.get_settings", _ray_settings),
        patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
        patch.object(service.batch, "ensure_mnp_job_def", return_value="mnp:1"),
        patch.object(service.batch, "submit_mnp", side_effect=submit_mnp),
        patch.object(service.batch, "client", return_value=OnlyMultiNodeBatch().client()),
        patch.object(service, "stage_runner", new=AsyncMock(return_value="s3://bucket/run_pbg.py")) as staged,
    ):
        dispatch = {"composite_id": "pkg.composites.colony"}
        await service._multi_node().submit(cast("Simulation", run), database, dispatch, correlation_id="c")

    assert [name.split("-")[1] for name in submitted] == ["parca", "mnp"], submitted
    staged.assert_awaited_once_with("exp-1")  # late-bound: the swap on the SERVICE is what the strategy got
