"""``atlantis smoke`` -- each check's PASS, FAIL and SKIP path, against a fake service.

The two behaviours that matter most are pinned hardest: a job that reports COMPLETED with
nothing behind it must FAIL, and an env worker must be stopped whatever happens inside the
check. The live behaviour is proven by running the command against a deployment; these
tests keep the judgement calls from regressing.
"""

from __future__ import annotations

import io
import json
import tarfile
import zipfile
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
from typer.testing import CliRunner

from app import smoke
from app.cli import cli

HEALTH = {"version": "9.9.9", "deployment_namespace": "ns", "compute_backend": "batch"}


def _client(routes: dict[tuple[str, str], httpx.Response]) -> httpx.Client:
    def handler(request: httpx.Request) -> httpx.Response:
        found = routes.get((request.method, request.url.path))
        return found if found is not None else httpx.Response(404, json={"detail": "no such route in this fake"})

    return httpx.Client(base_url="http://fake", transport=httpx.MockTransport(handler))


def _results_zip(members: dict[str, Any]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, body in members.items():
            archive.writestr(name, json.dumps(body))
    return buffer.getvalue()


def _results_tar_gz(members: dict[str, Any], prefix: str = "exp_123/") -> bytes:
    """What the Ray path really serves: a gzipped tar, members under an experiment folder."""
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
        for name, body in members.items():
            payload = body if isinstance(body, bytes) else json.dumps(body).encode()
            info = tarfile.TarInfo(prefix + name)
            info.size = len(payload)
            tar.addfile(info, io.BytesIO(payload))
    return buffer.getvalue()


class FakeService:
    """Records what the checks did, and answers from a few knobs."""

    def __init__(self, routes: dict[tuple[str, str], httpx.Response] | None = None) -> None:
        self._client = _client(routes or {})
        self.task_statuses = ["running", "completed"]
        self.task_log_lines: list[str] | None = None  # None = echo the nonce, as a real run would
        self.worker_task_result: Any = ["gen-a"]
        self.generators: Any = ["gen-a", "gen-b"]
        self.compose_level: float | None = smoke.SMOKE_COMPOSITE_EXPECTED
        self.stopped: list[str] = []
        self.submitted_script: str = ""
        self.read_raises: Exception | None = None
        self.workflows: list[dict[str, Any]] = []
        self.simulation_status = "completed"
        self.task_refs_reach_container = True
        self.repo_tasks: list[Any] = []
        self.cancelled: list[int] = []
        self.cancel_answer = "cancelled"
        self.nextflow_task_states = ["COMPLETED", "COMPLETED"]
        self.output_seed_summaries = 2

    @property
    def client(self) -> httpx.Client:
        return self._client

    def show_simulators(self) -> list[Any]:
        return [
            SimpleNamespace(
                database_id=1, git_repo_url="https://github.com/CovertLabEcoli/vEcoli-private", git_commit_hash="aaa"
            ),
            SimpleNamespace(
                database_id=7, git_repo_url="https://github.com/CovertLabEcoli/sms-ecoli", git_commit_hash="new"
            ),
            SimpleNamespace(
                database_id=3, git_repo_url="https://github.com/vivarium-collective/v2ecoli", git_commit_hash="old"
            ),
        ]

    def run_uploaded_task(
        self,
        *,
        local_path: str,
        args: list[str],
        sim_data_refs: Any,
        memory_class: str,
        commit: str | None,
        name: str | None,
    ) -> Any:
        self.submitted_script = Path(local_path).read_text(encoding="utf-8")
        self.task_commit = commit
        self.submitted_refs = sim_data_refs
        return SimpleNamespace(database_id=42)

    def run_task(self, request: Any) -> Any:
        self.repo_tasks.append(request)
        return SimpleNamespace(database_id=43)

    def get_task_status(self, task_id: int) -> Any:
        status = self.task_statuses.pop(0) if len(self.task_statuses) > 1 else self.task_statuses[0]
        return SimpleNamespace(status=status)

    def get_task_logs(self, task_id: int, limit: int = 1000) -> Any:
        if self.task_log_lines is not None:
            return SimpleNamespace(lines=self.task_log_lines)
        nonce = self.submitted_script.split('print("')[1].split('"')[0]
        refs = json.dumps(self.submitted_refs) if self.task_refs_reach_container and self.submitted_refs else "<unset>"
        return SimpleNamespace(lines=["boot", f"{nonce} ['ok']", f"refs: {refs}"])

    def worker_start(
        self,
        commit: str,
        workspace: str | None = None,
        session_key: str | None = None,
        accept_timeout: float | None = None,
    ) -> dict[str, Any]:
        return {"job_name": "env-worker-smoke"}

    def worker_read(self, job_name: str, capability: str, params: dict[str, Any] | None = None) -> object:
        if self.read_raises:
            raise self.read_raises
        return self.generators

    def worker_submit(self, job_name: str, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        return {"task_id": 5}

    def worker_task(self, task_id: int) -> dict[str, Any]:
        return {"status": "completed", "result": self.worker_task_result}

    def worker_stop(self, job_name: str) -> dict[str, Any]:
        self.stopped.append(job_name)
        return {}

    def compose_run_simulation(
        self, file_path: Path, interval_time: float = 1.0, batch: bool = False
    ) -> dict[str, Any]:
        self.compose_document = json.loads(Path(file_path).read_text(encoding="utf-8"))
        self.compose_interval = interval_time
        return {"simulation_database_id": 11, "simulator_database_id": 1}

    def compose_get_simulation_status(self, simulation_id: int) -> dict[str, Any]:
        return {"status": "completed"}

    def compose_get_simulation_results(self, simulation_id: int, dest: Path) -> Path:
        members: dict[str, Any] = {"final_state.json": {"global_time": 5.0}}
        if self.compose_level is not None:
            members["final_state.json"]["level"] = self.compose_level
        out = dest / "results.zip"
        out.write_bytes(_results_zip(members))
        return out

    def run_workflow(self, **kwargs: Any) -> Any:
        self.workflows.append(kwargs)
        return SimpleNamespace(database_id=900 + len(self.workflows))

    def get_workflow_status(self, simulation_id: int) -> Any:
        return SimpleNamespace(status=self.simulation_status)

    def cancel_workflow(self, simulation_id: int) -> Any:
        self.cancelled.append(simulation_id)
        if self.cancel_answer == "cancelled":
            self.simulation_status = "cancelled"
        return SimpleNamespace(status=self.cancel_answer)

    def get_workflow_tasks(self, simulation_id: int) -> list[Any]:
        return [SimpleNamespace(status=state) for state in self.nextflow_task_states]

    def get_output_data_sync(self, simulation_id: int, dest: Path) -> Path:
        root = dest / "experiment"
        root.mkdir(parents=True)
        (root / "summary.json").write_text("{}", encoding="utf-8")  # the experiment-level one does not count
        for seed in range(self.output_seed_summaries):
            (root / f"seed_{seed:02d}").mkdir()
            (root / f"seed_{seed:02d}" / "summary.json").write_text("{}", encoding="utf-8")
        return root

    def run_analysis(self, simulation_id: int, modules: str | None = None) -> dict[str, Any]:
        return {"database_id": 77}

    def get_analysis_status(self, analysis_id: int) -> Any:
        return SimpleNamespace(status="completed")

    def compose_biomodels_run(
        self, model_ids: list[str] | None = None, n_models: int | None = None, simulators: list[str] | None = None
    ) -> dict[str, Any]:
        return {"submitted": [{"simulation_database_id": 12}], "failed": []}


def _opts(**overrides: Any) -> smoke.SmokeOptions:
    ticks = iter(range(10_000))
    return smoke.SmokeOptions(sleep=lambda _: None, clock=lambda: float(next(ticks)), **overrides)


def _run(name: str, svc: FakeService, **overrides: Any) -> smoke.CheckResult:
    [check] = smoke.select_checks(3, only=[name])
    [result] = smoke.run_checks(svc, [check], _opts(**overrides))
    return result


RELAY_ON = {
    ("POST", "/env-worker/v1/relay/workers/smoke-no-such-worker/call"): httpx.Response(404, json={"detail": "x"})
}


# ------------------------------------------------------------------ tier 0


def test_version_passes_when_version_and_health_agree() -> None:
    svc = FakeService({
        ("GET", "/version"): httpx.Response(200, json="9.9.9"),
        ("GET", "/health"): httpx.Response(200, json=HEALTH),
    })
    assert _run("version", svc).outcome is smoke.Outcome.PASS


def test_version_fails_when_they_disagree() -> None:
    svc = FakeService({
        ("GET", "/version"): httpx.Response(200, json="1.0.0"),
        ("GET", "/health"): httpx.Response(200, json=HEALTH),
    })
    result = _run("version", svc)
    assert result.outcome is smoke.Outcome.FAIL
    assert "9.9.9" in result.detail


def test_an_html_200_is_a_failure_not_a_pass() -> None:
    """Through the ALB an unrouted path answers 200/404 with another service's HTML."""
    html = httpx.Response(200, text="<html>ptools</html>", headers={"content-type": "text/html"})
    svc = FakeService({("GET", "/version"): html})
    result = _run("version", svc)
    assert result.outcome is smoke.Outcome.FAIL
    assert "not JSON" in result.detail


def test_routes_skips_when_client_and_server_versions_differ() -> None:
    svc = FakeService({
        ("GET", "/openapi.json"): httpx.Response(200, json={"paths": {}}),
        ("GET", "/version"): httpx.Response(200, json="0.0.1"),
    })
    result = _run("routes", svc)
    assert result.outcome is smoke.Outcome.SKIP
    assert "0.0.1" in result.detail


def test_routes_fails_when_a_spec_operation_is_not_served(monkeypatch: pytest.MonkeyPatch) -> None:
    from viva_api.version import __version__

    monkeypatch.setattr(smoke, "packaged_spec", lambda: {"paths": {"/a": {"get": {}}, "/b": {"post": {}}}})
    live: dict[str, Any] = {"paths": {"/a": {"get": {}}}}
    svc = FakeService({
        ("GET", "/openapi.json"): httpx.Response(200, json=live),
        ("GET", "/version"): httpx.Response(200, json=__version__),
    })
    result = _run("routes", svc)
    assert result.outcome is smoke.Outcome.FAIL
    assert "POST /b" in result.detail


def test_relay_outcomes() -> None:
    path = ("POST", "/env-worker/v1/relay/workers/smoke-no-such-worker/call")
    assert _run("relay", FakeService(RELAY_ON)).outcome is smoke.Outcome.PASS
    assert _run("relay", FakeService({path: httpx.Response(503, json={})})).outcome is smoke.Outcome.SKIP
    html = httpx.Response(404, text="<html/>", headers={"content-type": "text/html"})
    routed_elsewhere = _run("relay", FakeService({path: html}))
    assert routed_elsewhere.outcome is smoke.Outcome.FAIL
    assert "not routed" in routed_elsewhere.detail


def test_lists_reports_every_broken_endpoint_not_just_the_first() -> None:
    routes = {("GET", path): httpx.Response(200, json=[]) for _, path, _ in smoke._LIST_ENDPOINTS}
    routes[("GET", "/api/v1/analyses")] = httpx.Response(500, text="boom")
    routes[("GET", "/compose/v1/processes")] = httpx.Response(500, text="bang")
    result = _run("lists", FakeService(routes))
    assert result.outcome is smoke.Outcome.FAIL
    assert "analyses" in result.detail and "compose processes" in result.detail


def test_size_unwraps_a_single_list_envelope() -> None:
    assert smoke._size({"versions": [1, 2, 3]}) == 3
    assert smoke._size([1, 2]) == 2
    assert smoke._size({"a": [1], "b": [2]}) == "ok"


# ------------------------------------------------------------------ tier 1


def test_task_passes_only_when_the_nonce_comes_back() -> None:
    svc = FakeService()
    result = _run("task", svc)
    assert result.outcome is smoke.Outcome.PASS
    assert svc.task_commit == "new"  # newest container-path simulator, not the vEcoli one
    assert result.evidence["nonce"] in svc.submitted_script


def test_task_completed_without_its_output_is_a_failure() -> None:
    svc = FakeService()
    svc.task_log_lines = ["boot", "something else entirely"]
    result = _run("task", svc)
    assert result.outcome is smoke.Outcome.FAIL
    assert "does not contain the nonce" in result.detail


def test_task_that_ends_failed_fails() -> None:
    svc = FakeService()
    svc.task_statuses = ["running", "failed"]
    assert _run("task", svc).outcome is smoke.Outcome.FAIL


def test_a_job_that_never_finishes_times_out() -> None:
    svc = FakeService()
    svc.task_statuses = ["running"]
    result = _run("task", svc, timeout_seconds=5.0)
    assert result.outcome is smoke.Outcome.FAIL
    assert "still RUNNING" in result.detail


def test_explicit_commit_wins() -> None:
    svc = FakeService()
    _run("task", svc, commit="pinned")
    assert svc.task_commit == "pinned"


def test_worker_passes_and_is_stopped() -> None:
    svc = FakeService(RELAY_ON)
    result = _run("worker", svc)
    assert result.outcome is smoke.Outcome.PASS
    assert svc.stopped == ["env-worker-smoke"]


def test_worker_is_stopped_even_when_the_check_fails_inside() -> None:
    svc = FakeService(RELAY_ON)
    svc.read_raises = httpx.ReadTimeout("worker went quiet")
    result = _run("worker", svc)
    assert result.outcome is smoke.Outcome.FAIL
    assert svc.stopped == ["env-worker-smoke"], "a leaked worker is a running K8s Job"


def test_worker_task_completed_with_an_empty_result_is_a_failure() -> None:
    svc = FakeService(RELAY_ON)
    svc.worker_task_result = []
    result = _run("worker", svc)
    assert result.outcome is smoke.Outcome.FAIL
    assert svc.stopped == ["env-worker-smoke"]


def test_worker_skips_when_the_relay_is_off() -> None:
    off = {("POST", "/env-worker/v1/relay/workers/smoke-no-such-worker/call"): httpx.Response(503, json={})}
    svc = FakeService(off)
    assert _run("worker", svc).outcome is smoke.Outcome.SKIP
    assert svc.stopped == []


def test_compose_checks_the_number_not_the_status() -> None:
    svc = FakeService()
    assert _run("compose", svc).outcome is smoke.Outcome.PASS
    assert svc.compose_interval == float(smoke.SMOKE_COMPOSITE_STEPS)
    assert "IncreaseProcess" in svc.compose_document["state"]["increase"]["address"]

    svc.compose_level = 1.0  # the run "completed" but never stepped
    wrong = _run("compose", svc)
    assert wrong.outcome is smoke.Outcome.FAIL
    assert "expected 1.61051" in wrong.detail

    svc.compose_level = None
    assert "no `level`" in _run("compose", svc).detail


def test_results_archive_format_is_sniffed_not_assumed() -> None:
    """The Ray path serves a gzipped tar (the client mislabels it .zip); SLURM serves a zip."""
    members = {"final_state.json": {"level": 1.61051, "global_time": 5.0}, "run_pbg.py": b"print(1)"}
    from_tar, tar_names = smoke.level_from_results_archive(_results_tar_gz(members))
    from_zip, zip_names = smoke.level_from_results_archive(
        _results_zip({"final_state.json": members["final_state.json"]})
    )
    assert from_tar == from_zip == pytest.approx(1.61051)
    assert "exp_123/run_pbg.py" in tar_names and zip_names == ["final_state.json"]


def test_final_state_wins_over_emitter_history() -> None:
    history = {"emitter": [{"level": 1.0}, {"level": 1.1}]}
    data = _results_tar_gz({"emitter_history.json": history, "final_state.json": {"level": 1.61051}})
    level, _ = smoke.level_from_results_archive(data)
    assert level == pytest.approx(1.61051)


def test_compose_passes_on_the_real_ray_payload_shape() -> None:
    svc = FakeService()

    def tar_results(simulation_id: int, dest: Path) -> Path:
        out = dest / f"compose_results_{simulation_id}.tar.gz"
        out.write_bytes(_results_tar_gz({"final_state.json": {"level": smoke.SMOKE_COMPOSITE_EXPECTED}}))
        return out

    svc.compose_get_simulation_results = tar_results  # type: ignore[method-assign]
    assert _run("compose", svc).outcome is smoke.Outcome.PASS


def test_the_expected_level_is_what_the_composite_computes() -> None:
    assert pytest.approx(1.61051) == smoke.SMOKE_COMPOSITE_EXPECTED


def test_opt_in_checks_skip_with_a_reason() -> None:
    svc = FakeService()
    assert "--simulation-id" in _run("analysis", svc).detail
    assert "--biomodel" in _run("biomodels", svc).detail
    assert _run("analysis", svc).outcome is smoke.Outcome.SKIP


def test_biomodels_runs_when_asked() -> None:
    assert _run("biomodels", FakeService(), biomodel_id="BIOMD0000000001").outcome is smoke.Outcome.PASS


# ------------------------------------------------------------------ the runner and the CLI


def test_a_crashing_check_is_reported_and_does_not_stop_the_run() -> None:
    def boom(_: smoke.SmokeService, __: smoke.SmokeOptions) -> tuple[str, dict[str, Any]]:
        raise RuntimeError("bug in the check itself")

    checks = [smoke.Check("boom", 0, "", boom), *smoke.select_checks(1, only=["compose"])]
    results = smoke.run_checks(FakeService(), checks, _opts())
    assert [r.outcome for r in results] == [smoke.Outcome.FAIL, smoke.Outcome.PASS]
    assert "check crashed" in results[0].detail
    assert smoke.summarize(results) == {"pass": 1, "fail": 1, "skip": 0}


def test_select_checks() -> None:
    assert {c.tier for c in smoke.select_checks(0)} == {0}
    up_to_tier_1 = {c.name for c in smoke.CHECKS if c.tier <= 1}
    assert {c.name for c in smoke.select_checks(1, skip=["task"])} == up_to_tier_1 - {"task"}
    assert {c.tier for c in smoke.select_checks(2)} == {0, 1, 2}
    assert "restart" in {c.name for c in smoke.select_checks(3)}
    assert [c.name for c in smoke.select_checks(0, only=["compose"])] == ["compose"]
    with pytest.raises(ValueError, match="unknown check"):
        smoke.select_checks(0, only=["nope"])


def test_the_real_data_service_satisfies_the_protocol() -> None:
    """The checks are written against a Protocol; this is what keeps it honest."""
    from app.app_data_service import E2EDataService

    service: smoke.SmokeService = E2EDataService(base_url="http://localhost:1")
    missing = [name for name in dir(FakeService) if not name.startswith("_") and not hasattr(service, name)]
    assert not missing


def test_cli_exits_nonzero_on_failure_and_writes_json(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    svc = FakeService({
        ("GET", "/version"): httpx.Response(200, json="1.0.0"),
        ("GET", "/health"): httpx.Response(200, json=HEALTH),
    })
    monkeypatch.setattr("app.cli.E2EDataService", lambda **_: svc)
    out = tmp_path / "smoke.json"
    result = CliRunner().invoke(cli, ["smoke", "run", "--only", "version", "--url", "http://x", "--json-out", str(out)])
    assert result.exit_code == 1
    body = json.loads(out.read_text(encoding="utf-8"))
    assert body["summary"] == {"pass": 0, "fail": 1, "skip": 0}
    assert body["checks"][0]["name"] == "version"


def test_cli_rejects_an_unknown_check() -> None:
    assert CliRunner().invoke(cli, ["smoke", "run", "--only", "nope", "--url", "http://x"]).exit_code == 2


@pytest.mark.parametrize(("payload", "suffix"), [(b"PK\x03\x04rest", ".zip"), (b"\x1f\x8b\x08\x00rest", ".tar.gz")])
def test_compose_results_are_saved_under_the_extension_of_what_they_are(
    payload: bytes, suffix: str, tmp_path: Path
) -> None:
    """The Ray path serves a gzipped tar; it used to be saved as `.zip`."""
    from app.app_data_service import E2EDataService

    service = E2EDataService(base_url="http://fake")
    service.client = _client({("GET", "/compose/v1/simulation/7/results"): httpx.Response(200, content=payload)})
    saved = service.compose_get_simulation_results(7, tmp_path)
    assert saved.name == f"compose_results_7{suffix}"
    assert saved.read_bytes() == payload


# ------------------------------------------------------------------ tier 2: one real simulation per dispatch path

CHAIN_DONE = {
    "id": 1,
    "seeds_total": 2,
    "seeds_succeeded": 2,
    "seeds_failed": 0,
    "seeds_in_progress": 0,
    "terminal": True,
}


def _chain_routes(progress: dict[str, Any]) -> dict[tuple[str, str], httpx.Response]:
    return {("GET", "/api/v1/simulations/901/chain-progress"): httpx.Response(200, json=progress)}


def test_each_dispatch_path_is_selected_the_way_a_real_client_selects_it() -> None:
    svc = FakeService(_chain_routes(CHAIN_DONE))
    for name in ("sim-default",):
        assert _run(name, svc).outcome is smoke.Outcome.PASS
    default = svc.workflows[-1]
    assert (default["num_generations"], default["num_seeds"], default["extra_params"]) == (1, 1, None)
    assert default["simulator_id"] == 7 and default["experiment_id"].startswith("smoke-default-")
    assert "smoke" in default["tags"]

    svc = FakeService(_chain_routes(CHAIN_DONE))
    assert _run("sim-chain", svc).outcome is smoke.Outcome.PASS
    assert (svc.workflows[-1]["num_generations"], svc.workflows[-1]["num_seeds"]) == (2, 2)  # >1 generation = chain

    svc = FakeService()
    assert _run("sim-nextflow", svc).outcome is smoke.Outcome.PASS
    assert svc.workflows[-1]["extra_params"]["nextflow_dispatch"]["composite_id"] == smoke.NEXTFLOW_COMPOSITE_ID

    svc = FakeService()
    assert _run("sim-composite", svc).outcome is smoke.Outcome.PASS
    assert svc.workflows[-1]["extra_params"]["multi_node_dispatch"]["composite_id"] == smoke.MULTI_NODE_COMPOSITE_ID


def test_a_completed_simulation_with_no_seed_output_is_a_failure() -> None:
    svc = FakeService()
    svc.output_seed_summaries = 0
    result = _run("sim-default", svc)
    assert result.outcome is smoke.Outcome.FAIL
    assert "0 per-seed summary.json" in result.detail


def test_a_failed_simulation_fails() -> None:
    svc = FakeService()
    svc.simulation_status = "failed"
    assert _run("sim-default", svc).outcome is smoke.Outcome.FAIL


def test_chain_needs_every_seed_to_have_succeeded() -> None:
    one_failed = {**CHAIN_DONE, "seeds_succeeded": 1, "seeds_failed": 1}
    result = _run("sim-chain", FakeService(_chain_routes(one_failed)))
    assert result.outcome is smoke.Outcome.FAIL
    assert "chain-progress says" in result.detail


def test_nextflow_needs_every_traced_task_completed_and_at_least_one() -> None:
    svc = FakeService()
    svc.nextflow_task_states = ["COMPLETED", "FAILED"]
    assert "its tasks are" in _run("sim-nextflow", svc).detail
    svc.nextflow_task_states = []
    assert "lists no tasks" in _run("sim-nextflow", svc).detail


def test_an_explicit_simulator_wins() -> None:
    svc = FakeService()
    _run("sim-default", svc, simulator_id=42)
    assert svc.workflows[-1]["simulator_id"] == 42


def test_tier_2_checks_run_concurrently_and_are_all_reported() -> None:
    """Four simulations are hours if run one after another; their cost is waiting on Batch."""
    import threading

    svc = FakeService(_chain_routes(CHAIN_DONE))
    barrier = threading.Barrier(4, timeout=10)
    original = svc.run_workflow

    def all_four_submit_before_any_returns(**kwargs: Any) -> Any:
        barrier.wait()  # deadlocks (and times out) unless four submissions are in flight together
        return original(**kwargs)

    svc.run_workflow = all_four_submit_before_any_returns  # type: ignore[method-assign]
    checks = smoke.select_checks(2, only=["sim-default", "sim-chain", "sim-nextflow", "sim-composite"])
    results = smoke.run_checks(svc, checks, smoke.SmokeOptions(sleep=lambda _: None))
    assert [r.name for r in results] == ["sim-default", "sim-chain", "sim-nextflow", "sim-composite"]
    assert not barrier.broken


# ------------------------------------------------------------------ tier R: a job in flight survives a restart


def _restart_service() -> FakeService:
    return FakeService({("GET", "/version"): httpx.Response(200, json="9.9.9")})


def test_restart_skips_without_a_command() -> None:
    result = _run("restart", _restart_service())
    assert result.outcome is smoke.Outcome.SKIP and "--restart-command" in result.detail


def test_restart_passes_when_the_job_still_resolves_with_its_output() -> None:
    result = _run("restart", _restart_service(), restart_command="true")
    assert result.outcome is smoke.Outcome.PASS
    assert "still resolved" in result.detail


def test_restart_fails_when_the_command_fails() -> None:
    result = _run("restart", _restart_service(), restart_command="false")
    assert result.outcome is smoke.Outcome.FAIL and "restart command exited 1" in result.detail


def test_restart_fails_when_the_api_never_comes_back(monkeypatch: pytest.MonkeyPatch) -> None:
    svc = FakeService({("GET", "/version"): httpx.Response(200, json="9.9.9")})
    calls = {"n": 0}
    real_get = svc.client.get

    def version_then_down(path: str, **kwargs: Any) -> httpx.Response:
        calls["n"] += 1
        return real_get(path, **kwargs) if calls["n"] == 1 else httpx.Response(502, text="bad gateway")

    monkeypatch.setattr(svc.client, "get", version_then_down)
    result = _run("restart", svc, restart_command="true", restart_wait_seconds=3.0)
    assert result.outcome is smoke.Outcome.FAIL and "did not come back" in result.detail


def test_restart_fails_when_the_job_is_lost() -> None:
    svc = _restart_service()
    svc.task_log_lines = ["the pod that knew about this job is gone"]
    result = _run("restart", svc, restart_command="true")
    assert result.outcome is smoke.Outcome.FAIL and "lacks the nonce" in result.detail


# ------------------------------------------------------------------ polling rides through a blip


def test_polling_rides_through_a_connection_blip() -> None:
    """A tier-2 poll runs for an hour. A port-forward restarting is not the deployment failing."""
    answers: list[Any] = [httpx.ConnectError("refused"), httpx.ReadTimeout("slow"), "running", "completed"]

    def read() -> str:
        answer = answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return str(answer)

    assert smoke._poll(_opts(), read, "a job") == "completed"


def test_polling_gives_up_when_the_api_stays_unreachable() -> None:
    def read() -> str:
        raise httpx.ConnectError("refused")

    with pytest.raises(smoke.CheckFailed, match="API unreachable for 180 s"):
        smoke._poll(_opts(timeout_seconds=10_000.0), read, "a job")


def test_an_answer_resets_the_unreachable_clock() -> None:
    """Two outages that are each inside the grace window, with an answer between them, are
    fine -- together they exceed it, so this only passes if the answer reset the clock.
    (The fake clock ticks on every read, and the loop reads it more than once per turn.)"""
    script: list[Any] = [httpx.ConnectError("x")] * 50 + ["running"] + [httpx.ConnectError("x")] * 50 + ["completed"]

    def read() -> str:
        answer = script.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return str(answer)

    assert smoke._poll(_opts(timeout_seconds=10_000.0), read, "a job") == "completed"


# ------------------------------------------------------------------ tier 0: the database is at head


def _health(**fields: str) -> FakeService:
    return FakeService({("GET", "/health"): httpx.Response(200, json={**HEALTH, **fields})})


def test_database_check() -> None:
    at_head = _health(db_revision="r9", db_head="r9", db_at_head="true", db_create_all="false")
    assert _run("database", at_head).outcome is smoke.Outcome.PASS

    behind = _run("database", _health(db_revision="r7", db_head="r9", db_at_head="false", db_create_all="false"))
    assert behind.outcome is smoke.Outcome.FAIL
    assert "r7" in behind.detail and "r9" in behind.detail and "alembic-migrate" in behind.detail

    assert _run("database", _health()).outcome is smoke.Outcome.SKIP  # an older server
    assert _run("database", _health(db_at_head="unknown", db_revision="r7")).outcome is smoke.Outcome.SKIP


def test_database_check_says_when_create_all_is_still_on() -> None:
    result = _run("database", _health(db_revision="r9", db_head="r9", db_at_head="true", db_create_all="true"))
    assert result.outcome is smoke.Outcome.PASS and "create_all is still ON" in result.detail


# ------------------------------------------------------------------ the task paths


def test_task_fails_when_its_sim_data_refs_never_reach_the_container() -> None:
    svc = FakeService()
    svc.task_refs_reach_container = False
    result = _run("task", svc)
    assert result.outcome is smoke.Outcome.FAIL
    assert "sim_data_refs did not reach the container" in result.detail
    assert "<unset>" in result.detail


def test_task_fail_passes_only_on_failed_with_proof_that_it_ran() -> None:
    svc = FakeService()
    svc.task_statuses = ["running", "failed"]
    result = _run("task-fail", svc)
    assert result.outcome is smoke.Outcome.PASS, result.detail
    assert "sys.exit(3)" in svc.submitted_script


def test_task_fail_treats_completed_as_the_wrong_answer() -> None:
    svc = FakeService()
    svc.task_statuses = ["completed"]
    result = _run("task-fail", svc)
    assert result.outcome is smoke.Outcome.FAIL
    assert "COMPLETED" in result.detail


def test_task_fail_rejects_a_task_that_failed_without_ever_running() -> None:
    svc = FakeService()
    svc.task_statuses = ["failed"]
    svc.task_log_lines = []
    result = _run("task-fail", svc)
    assert result.outcome is smoke.Outcome.FAIL
    assert "failed without running" in result.detail


def test_task_repo_submits_a_repo_path_and_needs_the_scripts_own_output() -> None:
    svc = FakeService()
    svc.task_log_lines = ["usage: build_cache.py [-h]"]
    result = _run("task-repo", svc)
    assert result.outcome is smoke.Outcome.PASS, result.detail
    [request] = svc.repo_tasks
    assert (request.script, request.args, request.commit) == ("scripts/build_cache.py", ["--help"], "new")

    svc.task_log_lines = ["Traceback (most recent call last):"]
    assert _run("task-repo", svc).outcome is smoke.Outcome.FAIL


# ------------------------------------------------------------------ cancel


class FakeBatchJobs:
    """``active_batch_jobs``: shows work for a run until the API has been asked to cancel it,
    then for ``linger`` more looks (a task that outlives the cancel), then nothing."""

    def __init__(self, svc: FakeService, *, appear_after: int = 1, linger: int = 1, never_stops: bool = False):
        self.svc, self.appear_after, self.linger, self.never_stops = svc, appear_after, linger, never_stops
        self.looks = 0
        self.tokens: set[str] = set()

    def __call__(self, token: str) -> list[dict[str, Any]]:
        self.tokens.add(token)
        self.looks += 1
        job = {"jobId": "j-1", "jobName": f"ray-sim-{token}-abc123", "status": "RUNNING"}
        if not self.svc.cancelled:
            return [job] if self.looks > self.appear_after else []
        if self.never_stops:
            return [job]
        self.linger -= 1
        return [job] if self.linger >= 0 else []


def _cancel_service() -> FakeService:
    svc = FakeService()
    svc.simulation_status = "running"
    return svc


@pytest.mark.parametrize("name", ["sim-cancel", "nextflow-cancel"])
def test_cancel_checks_skip_without_a_view_of_batch_and_submit_nothing(name: str) -> None:
    svc = _cancel_service()
    result = _run(name, svc, active_batch_jobs_unavailable="NoCredentialsError: Unable to locate credentials")
    assert result.outcome is smoke.Outcome.SKIP
    assert "NoCredentialsError" in result.detail
    assert svc.workflows == []


def test_sim_cancel_passes_when_batch_shows_the_work_and_then_shows_none() -> None:
    svc = _cancel_service()
    batch = FakeBatchJobs(svc)
    result = _run("sim-cancel", svc, active_batch_jobs=batch)
    assert result.outcome is smoke.Outcome.PASS, result.detail
    assert svc.cancelled == [901]
    [token] = batch.tokens
    assert token == svc.workflows[0]["experiment_id"] and token.startswith("smoke-cancel-")
    assert result.evidence["active_before_cancel"] == [f"ray-sim-{token}-abc123"]


def test_sim_cancel_fails_when_the_row_says_cancelled_and_batch_still_runs_it() -> None:
    svc = _cancel_service()
    result = _run("sim-cancel", svc, active_batch_jobs=FakeBatchJobs(svc, never_stops=True), cancel_settle_seconds=30.0)
    assert result.outcome is smoke.Outcome.FAIL
    assert "still active" in result.detail
    assert svc.simulation_status == "cancelled"  # which is exactly why the status cannot be the assertion


def test_sim_cancel_fails_when_the_api_does_not_answer_cancelled() -> None:
    svc = _cancel_service()
    svc.cancel_answer = "running"
    result = _run("sim-cancel", svc, active_batch_jobs=FakeBatchJobs(svc))
    assert result.outcome is smoke.Outcome.FAIL
    assert "answered RUNNING" in result.detail


def test_sim_cancel_fails_when_the_run_ends_before_there_is_anything_to_cancel() -> None:
    svc = _cancel_service()
    svc.simulation_status = "failed"
    result = _run("sim-cancel", svc, active_batch_jobs=FakeBatchJobs(svc, appear_after=10_000))
    assert result.outcome is smoke.Outcome.FAIL
    assert "before there was anything to cancel" in result.detail


def test_a_cancel_check_that_gives_up_still_cancels_what_it_submitted() -> None:
    svc = _cancel_service()
    result = _run("sim-cancel", svc, active_batch_jobs=FakeBatchJobs(svc, appear_after=10_000), timeout_seconds=30.0)
    assert result.outcome is smoke.Outcome.FAIL
    assert "none ever appeared" in result.detail
    assert svc.cancelled == [901]


def test_sim_cancel_fails_when_the_run_does_not_stay_cancelled() -> None:
    svc = _cancel_service()

    class Resurrecting(FakeBatchJobs):
        def __call__(self, token: str) -> list[dict[str, Any]]:
            jobs = super().__call__(token)
            if svc.cancelled and not jobs:
                svc.simulation_status = "running"
            return jobs

    result = _run("sim-cancel", svc, active_batch_jobs=Resurrecting(svc))
    assert result.outcome is smoke.Outcome.FAIL
    assert "is now RUNNING" in result.detail


@pytest.mark.parametrize(
    ("reaped", "who"), [([], "shutdown hook sufficed"), ([{"event": "dispatch.reaped"}], "reaper")]
)
def test_nextflow_cancel_says_who_stopped_the_tasks(reaped: list[dict[str, Any]], who: str) -> None:
    svc = FakeService({("GET", "/api/v1/simulations/901/events"): httpx.Response(200, json={"events": reaped})})
    svc.simulation_status = "running"
    result = _run("nextflow-cancel", svc, active_batch_jobs=FakeBatchJobs(svc))
    assert result.outcome is smoke.Outcome.PASS, result.detail
    assert who in result.detail
    assert "nextflow_dispatch" in svc.workflows[0]["extra_params"]
    assert result.evidence["reaped_by_scheduler"] == len(reaped)


def test_the_batch_lister_matches_by_name_or_by_command_across_queues_and_pages() -> None:
    calls: list[tuple[str, dict[str, Any]]] = []

    class Client:
        def describe_job_queues(self, **kw: Any) -> dict[str, Any]:
            calls.append(("describe_job_queues", kw))
            if kw.get("maxResults") == 1:
                return {"jobQueues": []}
            if "nextToken" not in kw:
                return {"jobQueues": [{"jobQueueName": "q1"}], "nextToken": "more"}
            return {"jobQueues": [{"jobQueueName": "q2"}]}

        def list_jobs(self, **kw: Any) -> dict[str, Any]:
            calls.append(("list_jobs", kw))
            if kw["jobStatus"] != "RUNNING":
                return {"jobSummaryList": []}
            if kw["jobQueue"] == "q1":
                return {
                    "jobSummaryList": [{"jobId": "a", "jobName": "ray-sim-TOKEN-x"}, {"jobId": "b", "jobName": "other"}]
                }
            if "nextToken" not in kw:
                return {"jobSummaryList": [{"jobId": "c", "jobName": "sim_gen"}], "nextToken": "p2"}
            return {"jobSummaryList": [{"jobId": "d", "jobName": "sim_gen"}]}

        def describe_jobs(self, **kw: Any) -> dict[str, Any]:
            commands = {"a": None, "b": ["echo"], "c": ["bash", "s3://b/nextflow/work/TOKEN/ab/cd"], "d": None}
            return {
                "jobs": [{"jobId": j, "jobName": "sim_gen", "container": {"command": commands[j]}} for j in kw["jobs"]]
            }

    lister = smoke.AwsBatchJobLister(client=Client())
    assert sorted(job["jobId"] for job in lister("TOKEN")) == ["a", "c"]
    assert {kw["jobQueue"] for name, kw in calls if name == "list_jobs"} == {"q1", "q2"}


def test_a_failed_cancel_check_keeps_its_evidence_and_says_what_the_job_is_waiting_on() -> None:
    """Found live on dev 0.9.147: cancel terminated the simulation's job and left the ParCa
    job it depends on RUNNING, so the terminated job sat PENDING. The row said CANCELLED."""
    svc = _cancel_service()

    class WaitingOnParca(FakeBatchJobs):
        def __call__(self, token: str) -> list[dict[str, Any]]:
            jobs = super().__call__(token)
            for job in jobs:
                job.update(status="PENDING", terminated=True)
                job["waiting_on"] = [{"jobId": "p-1", "jobName": "ray-parca-abc-xyz", "status": "RUNNING"}]
            return jobs

    batch = WaitingOnParca(svc, never_stops=True)
    result = _run("sim-cancel", svc, active_batch_jobs=batch, cancel_settle_seconds=30.0)
    assert result.outcome is smoke.Outcome.FAIL
    assert "PENDING (terminate accepted), waiting on ray-parca-abc-xyz RUNNING" in result.detail
    assert result.evidence["simulation_id"] == 901
    assert result.evidence["cancel_answered"] == "cancelled"
    assert result.evidence["still_active"][0]["waiting_on"][0]["jobName"] == "ray-parca-abc-xyz"


def test_the_batch_lister_reports_what_a_matched_job_depends_on() -> None:
    class Client:
        def describe_job_queues(self, **kw: Any) -> dict[str, Any]:
            return {"jobQueues": [] if kw.get("maxResults") == 1 else [{"jobQueueName": "q"}]}

        def list_jobs(self, **kw: Any) -> dict[str, Any]:
            hit = kw["jobStatus"] == "PENDING"
            return {"jobSummaryList": [{"jobId": "sim", "jobName": "ray-sim-TOKEN-x"}] if hit else []}

        def describe_jobs(self, **kw: Any) -> dict[str, Any]:
            known = {
                "sim": {"jobId": "sim", "isTerminated": True, "dependsOn": [{"jobId": "parca", "type": "SEQUENTIAL"}]},
                "parca": {"jobId": "parca", "jobName": "ray-parca-abc-xyz", "status": "RUNNING"},
            }
            return {"jobs": [known[j] for j in kw["jobs"]]}

    [job] = smoke.AwsBatchJobLister(client=Client())("TOKEN")
    assert job["terminated"] is True
    assert job["waiting_on"] == [{"jobId": "parca", "jobName": "ray-parca-abc-xyz", "status": "RUNNING"}]
