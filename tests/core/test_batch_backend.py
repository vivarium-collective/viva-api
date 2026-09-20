"""The Batch engine on its own: no application service, no settings, a fake client.

These are the tests a second consumer of ``viva_core`` inherits. The SMS suite
(``tests/simulation/test_ray_backend.py``) still exercises the same code through
``SimulationServiceRay``'s delegating methods.
"""

import copy
from typing import TYPE_CHECKING, Any, cast

import pytest

from viva_core.backends.batch import (
    ACTIVE_JOB_STATES,
    DESCRIBE_JOBS_MAX_BATCH,
    BatchJobClient,
    SubmitJobPacer,
    batch_exit_code,
    ecr_image_uri,
    env_as_batch_list,
    stage_out_env,
)
from viva_core.models import JobStatus

if TYPE_CHECKING:
    from types_boto3_batch import BatchClient
    from types_boto3_batch.type_defs import JobDetailTypeDef, KeyValuePairTypeDef


class FakeBatch:
    """Records every call; answers from a small scripted state."""

    def __init__(
        self, *, definitions: dict[str, list[dict[str, Any]]] | None = None, jobs: list[dict[str, Any]] | None = None
    ):
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.definitions = definitions or {}
        self.jobs = jobs or []

    def _record(self, name: str, kwargs: dict[str, Any]) -> None:
        self.calls.append((name, copy.deepcopy(kwargs)))

    def named(self, name: str) -> list[dict[str, Any]]:
        return [kwargs for call, kwargs in self.calls if call == name]

    def describe_job_definitions(self, **kwargs: Any) -> dict[str, Any]:
        self._record("describe_job_definitions", kwargs)
        key = kwargs.get("jobDefinitionName") or kwargs["jobDefinitions"][0]
        return {"jobDefinitions": copy.deepcopy(self.definitions.get(key, []))}

    def register_job_definition(self, **kwargs: Any) -> dict[str, Any]:
        self._record("register_job_definition", kwargs)
        return {"revision": 7}

    def submit_job(self, **kwargs: Any) -> dict[str, Any]:
        self._record("submit_job", kwargs)
        return {"jobId": "job-1"}

    def describe_jobs(self, **kwargs: Any) -> dict[str, Any]:
        self._record("describe_jobs", kwargs)
        return {"jobs": [copy.deepcopy(j) for j in self.jobs if j["jobId"] in kwargs["jobs"]]}

    def list_jobs(self, **kwargs: Any) -> dict[str, Any]:
        self._record("list_jobs", kwargs)
        if kwargs["jobStatus"] != "RUNNING":
            return {"jobSummaryList": []}
        return {"jobSummaryList": [{"jobId": j["jobId"]} for j in self.jobs]}

    def terminate_job(self, **kwargs: Any) -> dict[str, Any]:
        self._record("terminate_job", kwargs)
        return {}


def _as_client(fake: FakeBatch) -> "BatchClient":
    """The engine is typed against the real ``BatchClient`` (``types-boto3``). A fake is a
    fake on purpose -- six operations, recorded -- so it is CAST, here and only here, rather
    than the engine's parameter being widened back to ``Any`` to let it in."""
    return cast("BatchClient", fake)


def _engine(fake: FakeBatch) -> BatchJobClient:
    return BatchJobClient(lambda: _as_client(fake))


# ── job definitions ─────────────────────────────────────────────────────────


def test_a_container_job_definition_is_derived_from_the_newest_base_revision_with_only_the_image_swapped() -> None:
    fake = FakeBatch(
        definitions={
            "base": [
                {"revision": 2, "containerProperties": {"image": "old", "vcpus": 4}},
                {"revision": 5, "containerProperties": {"image": "older", "vcpus": 8, "jobRoleArn": "role"}},
            ]
        }
    )
    assert (
        _engine(fake).ensure_container_job_definition(base_definition="base", image="IMG", suffix="abc") == "base-abc:7"
    )
    (registered,) = fake.named("register_job_definition")
    assert registered == {
        "jobDefinitionName": "base-abc",
        "type": "container",
        "containerProperties": {"image": "IMG", "vcpus": 8, "jobRoleArn": "role"},
    }


def test_an_existing_revision_for_the_same_image_is_reused_not_re_registered() -> None:
    fake = FakeBatch(definitions={"base-abc": [{"revision": 9, "containerProperties": {"image": "IMG"}}]})
    assert (
        _engine(fake).ensure_container_job_definition(base_definition="base", image="IMG", suffix="abc") == "base-abc:9"
    )
    assert fake.named("register_job_definition") == []


def test_an_mnp_job_definition_swaps_the_image_on_every_node_range() -> None:
    node_properties = {
        "numNodes": 2,
        "nodeRangeProperties": [{"targetNodes": "0:", "container": {"image": "old"}}, {"targetNodes": "1:"}],
    }
    fake = FakeBatch(definitions={"base": [{"revision": 1, "nodeProperties": node_properties}]})
    assert _engine(fake).ensure_mnp_job_definition(base_definition="base", image="IMG", suffix="abc") == "base-abc:7"
    (registered,) = fake.named("register_job_definition")
    assert registered["type"] == "multinode"
    assert [r["container"]["image"] for r in registered["nodeProperties"]["nodeRangeProperties"]] == ["IMG", "IMG"]


@pytest.mark.parametrize("method", ["ensure_mnp_job_definition", "ensure_container_job_definition"])
def test_a_missing_base_definition_is_an_error_that_names_it(method: str) -> None:
    with pytest.raises(RuntimeError, match="'nope' not found"):
        getattr(_engine(FakeBatch()), method)(base_definition="nope", image="IMG", suffix="abc")


# ── submission ──────────────────────────────────────────────────────────────


def test_stage_out_env_emits_the_optional_halves_only_when_configured() -> None:
    assert stage_out_env(prefix="CONTAINER", out_dir="/o", out_s3="s3://o") == [
        {"name": "CONTAINER_OUT_DIR", "value": "/o"},
        {"name": "CONTAINER_OUT_S3", "value": "s3://o"},
    ]
    # a stage URI without a stage dir is not a stage-in
    assert len(stage_out_env(prefix="RAY", out_dir="/o", out_s3="s3://o", stage_s3="s3://c")) == 2
    full = stage_out_env(
        prefix="RAY", out_dir="/o", out_s3="s3://o", stage_s3="s3://c", stage_dir="/c", log_s3_prefix="s3://l"
    )
    assert [e["name"] for e in full] == [
        "RAY_OUT_DIR",
        "RAY_OUT_S3",
        "RAY_STAGE_S3",
        "RAY_STAGE_DIR",
        "RAY_LOG_S3_PREFIX",
    ]


def test_submit_container_puts_the_whole_contract_in_one_environment_list() -> None:
    fake = FakeBatch()
    job_id = _engine(fake).submit_container(
        job_name="n",
        job_queue="q",
        job_definition="d:1",
        job_cmd="python x.py",
        report_path="/tmp/report.json",  # noqa: S108
        stage_env=[{"name": "CONTAINER_OUT_DIR", "value": "/o"}],
        task_env={"A": "1"},
        depends_on=["a"],
        tags={"run": "1"},
        retry_strategy={"attempts": 2},
    )
    assert job_id == "job-1"
    (submitted,) = fake.named("submit_job")
    assert submitted == {
        "jobName": "n",
        "jobQueue": "q",
        "jobDefinition": "d:1",
        "containerOverrides": {
            "environment": [
                {"name": "CONTAINER_JOB_CMD", "value": "python x.py"},
                {"name": "CONTAINER_REPORT_PATH", "value": "/tmp/report.json"},  # noqa: S108
                {"name": "CONTAINER_OUT_DIR", "value": "/o"},
                {"name": "A", "value": "1"},
            ]
        },
        "dependsOn": [{"jobId": "a", "type": "SEQUENTIAL"}],
        "tags": {"run": "1"},
        "propagateTags": True,
        "retryStrategy": {"attempts": 2},
    }


def test_submit_mnp_targets_the_single_node_range_and_does_not_mutate_the_callers_env() -> None:
    fake = FakeBatch()
    shared: list[KeyValuePairTypeDef] = [{"name": "RAY_OUT_DIR", "value": "/o"}]
    _engine(fake).submit_mnp(
        job_name="n",
        job_queue="q",
        job_definition="d:1",
        num_nodes=3,
        job_cmd="ray job",
        report_path="/tmp/report.json",  # noqa: S108
        shared_env=shared,
        depends_on=["array-parent"],
        depends_type=None,
    )
    assert shared == [{"name": "RAY_OUT_DIR", "value": "/o"}]
    (submitted,) = fake.named("submit_job")
    assert submitted["dependsOn"] == [{"jobId": "array-parent"}]  # no type: an Array parent rejects SEQUENTIAL
    assert submitted["nodeOverrides"]["numNodes"] == 3
    (override,) = submitted["nodeOverrides"]["nodePropertyOverrides"]
    assert override["targetNodes"] == "0:"
    assert [e["name"] for e in override["containerOverrides"]["environment"]] == [
        "RAY_JOB_CMD",
        "RAY_REPORT_PATH",
        "RAY_OUT_DIR",
        "RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE",
    ]


def test_an_explicit_client_is_used_instead_of_the_factorys() -> None:
    factory_client, explicit = FakeBatch(), FakeBatch()
    _engine(factory_client).submit_container(
        job_name="n",
        job_queue="q",
        job_definition="d:1",
        job_cmd="c",
        report_path="/r",
        stage_env=[],
        client=_as_client(explicit),
    )
    assert factory_client.calls == []
    assert len(explicit.named("submit_job")) == 1


def test_the_client_factory_is_called_per_operation_so_a_swapped_client_is_honoured() -> None:
    first, second = FakeBatch(), FakeBatch()
    current = [first]
    engine = BatchJobClient(lambda: _as_client(current[0]))
    engine.terminate("a", reason="r")
    current[0] = second
    engine.terminate("b", reason="r")
    assert first.named("terminate_job") == [{"jobId": "a", "reason": "r"}]
    assert second.named("terminate_job") == [{"jobId": "b", "reason": "r"}]


# ── status ──────────────────────────────────────────────────────────────────

JOBS: list[dict[str, Any]] = [
    {
        "jobId": "ok",
        "jobName": "fine",
        "status": "SUCCEEDED",
        "container": {"exitCode": 0, "command": ["run", "mine"]},
        "attempts": [{}],
    },
    {
        "jobId": "bad",
        "jobName": "oom",
        "status": "FAILED",
        "statusReason": "OutOfMemory",
        "container": {"exitCode": 137},
        "attempts": [{}, {}],
    },
    {"jobId": "live", "jobName": "", "status": "RUNNING", "container": {"command": ["run", "theirs"]}},
]


def test_statuses_are_chunked_at_the_api_limit_and_an_unknown_id_is_simply_absent() -> None:
    fake = FakeBatch(jobs=JOBS)
    ids = ["ok", "bad", *[f"unknown-{i}" for i in range(2 * DESCRIBE_JOBS_MAX_BATCH)]]
    assert _engine(fake).job_statuses(ids) == {"ok": JobStatus.from_batch_state("SUCCEEDED"), "bad": JobStatus.FAILED}
    assert [len(c["jobs"]) for c in fake.named("describe_jobs")] == [100, 100, 2]
    assert _engine(fake).job_statuses([]) == {}


def test_details_say_what_a_failed_job_said() -> None:
    detail = _engine(FakeBatch(jobs=JOBS)).job_details(["bad"])["bad"]
    assert (detail.exit_code, detail.attempts, detail.status_reason) == (137, 2, "OutOfMemory")
    assert detail.describe() == "oom (failed, exit 137, OutOfMemory)"


def test_describe_job_returns_none_for_a_job_batch_does_not_know() -> None:
    engine = _engine(FakeBatch(jobs=JOBS))
    assert engine.describe_job("nope") is None
    job = engine.describe_job("bad")
    assert job is not None and batch_exit_code(job) == "137"
    # Not a shape the stubs admit, but the function tolerates it and must keep doing so.
    assert batch_exit_code(cast("JobDetailTypeDef", {"container": None})) is None


def test_a_job_definitions_log_group_is_none_rather_than_an_error_when_it_cannot_be_read() -> None:
    options = {"logConfiguration": {"options": {"awslogs-group": "/aws/batch/x"}}}
    assert (
        _engine(FakeBatch(definitions={"d:1": [{"containerProperties": options}]})).job_definition_log_group("d:1")
        == "/aws/batch/x"
    )
    assert _engine(FakeBatch()).job_definition_log_group("missing:1") is None


# ── cancellation ────────────────────────────────────────────────────────────


def test_terminate_matching_walks_every_active_state_and_stops_only_what_matches() -> None:
    fake = FakeBatch(jobs=JOBS)
    stopped = _engine(fake).terminate_matching(
        queues=["q1"],
        matches=lambda job: "mine" in (job.get("container", {}).get("command") or []),
        reason="cancelled",
    )
    assert stopped == 1
    assert fake.named("terminate_job") == [{"jobId": "ok", "reason": "cancelled"}]
    assert [c["jobStatus"] for c in fake.named("list_jobs")] == list(ACTIVE_JOB_STATES)


# ── small pieces ────────────────────────────────────────────────────────────


def test_small_pure_helpers() -> None:
    assert ecr_image_uri(account_id="1", region="r", repository="repo", tag="t") == "1.dkr.ecr.r.amazonaws.com/repo:t"
    assert env_as_batch_list({"A": "1"}) == [{"name": "A", "value": "1"}]
    assert env_as_batch_list(None) == []


@pytest.mark.asyncio
async def test_the_pacer_never_sleeps_on_the_first_call() -> None:
    pacer = SubmitJobPacer(max_per_second=0.001)  # a 1000 s interval: a sleep here would hang the test
    await pacer.wait()
