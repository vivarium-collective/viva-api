"""``atlantis smoke`` -- each check's PASS, FAIL and SKIP path, against a fake service.

The two behaviours that matter most are pinned hardest: a job that reports COMPLETED with
nothing behind it must FAIL, and an env worker must be stopped whatever happens inside the
check. The live behaviour is proven by running the command against a deployment; these
tests keep the judgement calls from regressing.
"""

from __future__ import annotations

import io
import json
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
        return SimpleNamespace(database_id=42)

    def get_task_status(self, task_id: int) -> Any:
        status = self.task_statuses.pop(0) if len(self.task_statuses) > 1 else self.task_statuses[0]
        return SimpleNamespace(status=status)

    def get_task_logs(self, task_id: int, limit: int = 1000) -> Any:
        if self.task_log_lines is not None:
            return SimpleNamespace(lines=self.task_log_lines)
        nonce = self.submitted_script.split('print("')[1].split('"')[0]
        return SimpleNamespace(lines=["boot", f"{nonce} ['ok']"])

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
    [check] = smoke.select_checks(1, only=[name])
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
    assert {c.name for c in smoke.select_checks(1, skip=["task"])} == {c.name for c in smoke.CHECKS} - {"task"}
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
