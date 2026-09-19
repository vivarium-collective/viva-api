"""Smoke tests against a DEPLOYED API -- ``atlantis smoke run``.

Why this exists: merging is not deploying. Unit tests prove the code; only a live call
proves the image, the configuration as the pod reads it, real Batch, a real K8s Job and the
real database. Until now each deploy was verified by hand, differently every time.

Two rules shape every check here:

* **Assert the EFFECT, never just the status.** "COMPLETED" with nothing behind it is the
  failure this project has been bitten by most (the silent-success cluster). A task check
  passes only when a nonce it printed comes back in the task's own log; a compose check only
  when the result carries the number the composite must compute.
* **SKIP is not PASS.** A check that could not run says why, and the summary counts it
  separately -- a green run with five skips is not a verified deployment.

Tiers (``docs/plan-core.md`` section 8 says which a deploy checkpoint needs):

* **Tier 0** -- seconds, free, read-only: version, health, route inventory, capabilities,
  the relay diagnostic, the database-backed list endpoints, an events read.
* **Tier 1** -- minutes, cents: one tiny real dispatch per mechanism -- a container task, a
  relayed env worker (a K8s Job) plus a task on its task tier, a tiny composite; and, when
  asked for, a standalone analysis and a BioModels run.

Tier 2 (full simulations, chain dispatch, Nextflow head, image build) and Tier R (restart
resilience) are planned, not here.

The checks take a small Protocol rather than ``E2EDataService`` itself, so each is unit-tested
against a fake; ``E2EDataService`` satisfies it structurally.
"""

from __future__ import annotations

import io
import json
import secrets
import tempfile
import time
import zipfile
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from enum import StrEnum
from importlib import resources
from pathlib import Path
from typing import Any, Protocol

import httpx
import yaml

HTTP_METHODS = ("get", "post", "put", "delete", "patch")

#: The composite the compose check runs: a level that grows 10% per step, five steps.
SMOKE_COMPOSITE_STEPS = 5
SMOKE_COMPOSITE_START = 1.0
SMOKE_COMPOSITE_RATE = 0.1
SMOKE_COMPOSITE_EXPECTED = SMOKE_COMPOSITE_START * (1 + SMOKE_COMPOSITE_RATE) ** SMOKE_COMPOSITE_STEPS

TERMINAL_OK = {"completed"}
TERMINAL_BAD = {"failed", "cancelled", "out_of_memory", "timeout", "unknown", "suspended"}


class Outcome(StrEnum):
    PASS = "pass"  # noqa: S105 - an outcome label, not a credential
    FAIL = "fail"
    SKIP = "skip"


class SkipCheck(Exception):
    """The check could not run here, and that is not a failure. Carries the reason."""


class CheckFailed(Exception):
    """The deployment answered, and the answer was wrong."""


@dataclass
class CheckResult:
    name: str
    tier: int
    outcome: Outcome
    seconds: float
    detail: str
    evidence: dict[str, Any] = field(default_factory=dict)


@dataclass
class SmokeOptions:
    """Everything a check may need beyond the service. All optional: a check that needs
    something it was not given SKIPs with a reason rather than guessing."""

    commit: str | None = None
    simulation_id: int | None = None
    biomodel_id: str | None = None
    poll_seconds: float = 10.0
    timeout_seconds: float = 900.0
    sleep: Callable[[float], None] = time.sleep
    clock: Callable[[], float] = time.monotonic


class SmokeService(Protocol):
    """The slice of ``E2EDataService`` the checks use."""

    @property
    def client(self) -> httpx.Client: ...

    def show_simulators(self) -> Sequence[Any]: ...
    def run_uploaded_task(
        self,
        *,
        local_path: str,
        args: list[str],
        sim_data_refs: dict[str, str] | None,
        memory_class: str,
        commit: str | None,
        name: str | None,
    ) -> Any: ...
    def get_task_status(self, task_id: int) -> Any: ...
    def get_task_logs(self, task_id: int, limit: int = ...) -> Any: ...
    def worker_start(
        self,
        commit: str,
        workspace: str | None = ...,
        session_key: str | None = ...,
        accept_timeout: float | None = ...,
    ) -> dict[str, Any]: ...
    def worker_read(self, job_name: str, capability: str, params: dict[str, Any] | None = ...) -> object: ...
    def worker_submit(self, job_name: str, method: str, params: dict[str, Any] | None = ...) -> dict[str, Any]: ...
    def worker_task(self, task_id: int) -> dict[str, Any]: ...
    def worker_stop(self, job_name: str) -> dict[str, Any]: ...
    def compose_run_simulation(
        self, file_path: Path, interval_time: float = ..., batch: bool = ...
    ) -> dict[str, Any]: ...
    def compose_get_simulation_status(self, simulation_id: int) -> dict[str, Any]: ...
    def compose_get_simulation_results(self, simulation_id: int, dest: Path) -> Path: ...
    def run_analysis(self, simulation_id: int, modules: str | None = ...) -> dict[str, Any]: ...
    def get_analysis_status(self, analysis_id: int) -> Any: ...
    def compose_biomodels_run(
        self,
        model_ids: list[str] | None = ...,
        n_models: int | None = ...,
        simulators: list[str] | None = ...,
    ) -> dict[str, Any]: ...


CheckFn = Callable[[SmokeService, SmokeOptions], tuple[str, dict[str, Any]]]


@dataclass(frozen=True)
class Check:
    name: str
    tier: int
    summary: str
    run: CheckFn


# --------------------------------------------------------------------------- helpers


def _get_json(svc: SmokeService, path: str, **params: Any) -> Any:
    resp = svc.client.get(path, params=params or None)
    if resp.status_code != 200:
        raise CheckFailed(f"GET {path} -> {resp.status_code}: {resp.text[:200]}")
    try:
        return resp.json()
    except ValueError as e:
        # An HTML body through the ALB means the path fell through to another target.
        kind = resp.headers.get("content-type", "?")
        raise CheckFailed(f"GET {path} -> 200 but not JSON (content-type {kind}): {resp.text[:120]!r}") from e


def _status_text(value: Any) -> str:
    """A status as lower-case text, whether it arrived as an enum, a str, or ``None``."""
    raw = getattr(value, "value", value)
    return str(raw).lower() if raw is not None else "none"


def _poll(
    opts: SmokeOptions,
    read_status: Callable[[], str],
    what: str,
) -> str:
    """Poll until a terminal status. Returns it when good; raises when bad or out of time."""
    deadline = opts.clock() + opts.timeout_seconds
    last = "none"
    while True:
        last = read_status()
        if last in TERMINAL_OK:
            return last
        if last in TERMINAL_BAD:
            raise CheckFailed(f"{what} ended {last.upper()}")
        if opts.clock() >= deadline:
            raise CheckFailed(f"{what} still {last.upper()} after {opts.timeout_seconds:.0f} s")
        opts.sleep(opts.poll_seconds)


def _resolve_commit(svc: SmokeService, opts: SmokeOptions) -> str:
    """The image commit to dispatch on: ``--commit``, else the newest registered simulator
    from a repo that runs on the container/Ray path (its image lives in the same registry
    the task and worker dispatch pull from)."""
    if opts.commit:
        return opts.commit
    candidates = [
        s
        for s in svc.show_simulators()
        if any(tag in str(getattr(s, "git_repo_url", "")).lower() for tag in ("sms-ecoli", "v2ecoli"))
    ]
    if not candidates:
        raise SkipCheck("no --commit given and no container-path simulator is registered")
    newest = max(candidates, key=lambda s: int(getattr(s, "database_id", 0) or 0))
    return str(newest.git_commit_hash)


def spec_operations(spec: dict[str, Any]) -> set[tuple[str, str]]:
    return {
        (method.upper(), path)
        for path, item in spec.get("paths", {}).items()
        for method in item
        if method in HTTP_METHODS
    }


def packaged_spec() -> dict[str, Any]:
    text = resources.files("viva_api.api.spec").joinpath("openapi_3_1_0_generated.yaml").read_text(encoding="utf-8")
    loaded: dict[str, Any] = yaml.safe_load(text)
    return loaded


def smoke_composite_document() -> dict[str, Any]:
    """A composite built only from what process-bigraph itself ships, so it runs in any
    image that can run a composite at all. One process multiplies ``level`` by 1.1 per
    unit step; after five steps the level is 1.1**5. Verified through ``run_pbg.py``."""
    return {
        "state": {
            "level": SMOKE_COMPOSITE_START,
            "increase": {
                "_type": "process",
                "address": "local:process_bigraph.processes.examples.IncreaseProcess",
                "config": {"rate": SMOKE_COMPOSITE_RATE},
                "interval": 1.0,
                "inputs": {"level": ["level"]},
                "outputs": {"level": ["level"]},
            },
            "emitter": {
                "_type": "step",
                "address": "local:process_bigraph.emitter.RAMEmitter",
                "config": {"emit": {"level": "float", "global_time": "float"}},
                "inputs": {"level": ["level"], "global_time": ["global_time"]},
            },
        }
    }


def find_level(payload: Any) -> float | None:
    """The composite's ``level`` from a results payload -- a final state, or an emitter
    history whose last row carries it. Returns the LAST value found, depth-first."""
    found: float | None = None
    if isinstance(payload, dict):
        for key, value in payload.items():
            if key == "level" and isinstance(value, int | float) and not isinstance(value, bool):
                found = float(value)
            else:
                nested = find_level(value)
                found = nested if nested is not None else found
    elif isinstance(payload, list):
        for item in payload:
            nested = find_level(item)
            found = nested if nested is not None else found
    return found


def level_from_results_archive(data: bytes) -> tuple[float | None, list[str]]:
    """Every JSON member of a results archive is searched; returns the level and the names."""
    names: list[str] = []
    level: float | None = None
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        for member in archive.namelist():
            names.append(member)
            if not member.endswith(".json"):
                continue
            try:
                found = find_level(json.loads(archive.read(member)))
            except ValueError:
                continue
            if found is not None and (level is None or member.endswith("final_state.json")):
                level = found
    return level, names


# --------------------------------------------------------------------------- tier 0


def check_version(svc: SmokeService, _: SmokeOptions) -> tuple[str, dict[str, Any]]:
    version = _get_json(svc, "/version")
    health = _get_json(svc, "/health")
    if not isinstance(version, str) or not version:
        raise CheckFailed(f"/version returned {version!r}, expected a version string")
    if health.get("version") != version:
        raise CheckFailed(f"/version says {version} but /health says {health.get('version')}")
    evidence = {k: health.get(k) for k in ("version", "deployment_namespace", "compute_backend")}
    return f"{version} on {health.get('deployment_namespace')} ({health.get('compute_backend')})", evidence


def check_routes(svc: SmokeService, _: SmokeOptions) -> tuple[str, dict[str, Any]]:
    """Every operation in the spec this client was built with must be served. Only
    meaningful when client and server are the same version; otherwise it SKIPs."""
    live_spec = _get_json(svc, "/openapi.json")
    local_spec = packaged_spec()
    live_version = _get_json(svc, "/version")
    from viva_api.version import __version__ as local_version

    live, local = spec_operations(live_spec), spec_operations(local_spec)
    evidence = {"live_operations": len(live), "client_operations": len(local), "server_version": live_version}
    if str(live_version) != local_version:
        raise SkipCheck(
            f"server is {live_version}, this client is {local_version}: "
            f"route sets are only comparable at equal versions ({len(live)} live operations)"
        )
    missing = sorted(local - live)
    extra = sorted(live - local)
    evidence.update(missing=[f"{m} {p}" for m, p in missing], extra=[f"{m} {p}" for m, p in extra])
    if missing:
        shown = ", ".join(f"{m} {p}" for m, p in missing[:4])
        raise CheckFailed(f"{len(missing)} spec operation(s) not served: {shown}")
    return f"{len(live)} operations, all {len(local)} in the spec are served", evidence


def check_capabilities(svc: SmokeService, _: SmokeOptions) -> tuple[str, dict[str, Any]]:
    body = _get_json(svc, "/core/v1/capabilities")
    capabilities = body.get("capabilities")
    if not isinstance(capabilities, list) or not capabilities:
        raise CheckFailed(f"no capabilities advertised: {body!r}")
    return ", ".join(map(str, capabilities)), {"capabilities": capabilities}


def check_relay(svc: SmokeService, _: SmokeOptions) -> tuple[str, dict[str, Any]]:
    """A call to a worker that does not exist: a JSON 404 means the relay is live and the
    path reaches this API; 503 means the relay is switched off; an HTML 404 means the
    gateway sent ``/env-worker`` somewhere else entirely."""
    resp = svc.client.post("/env-worker/v1/relay/workers/smoke-no-such-worker/call", json={"method": "ping"})
    kind = resp.headers.get("content-type", "")
    evidence = {"status": resp.status_code, "content_type": kind}
    if resp.status_code == 503:
        raise SkipCheck("relay is off on this deployment (503) -- ENV_WORKER_RELAY_ADVERTISE_HOST is not set")
    if resp.status_code == 404 and "json" in kind:
        return "live (JSON 404 for an unknown worker)", evidence
    if resp.status_code == 404:
        raise CheckFailed(f"404 but not JSON ({kind}) -- /env-worker is not routed to this API")
    raise CheckFailed(f"expected 404, got {resp.status_code}: {resp.text[:160]}")


_LIST_ENDPOINTS: tuple[tuple[str, str, dict[str, Any]], ...] = (
    ("simulators", "/core/v1/simulator/versions", {}),
    ("simulations", "/api/v1/simulations", {"limit": 3}),
    ("analyses", "/api/v1/analyses", {}),
    ("parca datasets", "/core/v1/simulation/parca/versions", {}),
    ("compose simulators", "/compose/v1/simulators", {}),
    ("compose processes", "/compose/v1/processes", {}),
)


def _size(body: Any) -> int | str:
    """How many items a list endpoint returned -- unwrapping the ``{"versions": [...]}``
    style, where the list is the single list-valued field of an envelope."""
    if isinstance(body, list):
        return len(body)
    if isinstance(body, dict):
        lists = [v for v in body.values() if isinstance(v, list)]
        if len(lists) == 1:
            return len(lists[0])
    return "ok"


def check_lists(svc: SmokeService, _: SmokeOptions) -> tuple[str, dict[str, Any]]:
    """The database-backed reads, one per table family: a broken migration or ORM mapping
    shows up here as a 500 long before anyone submits a job."""
    sizes: dict[str, Any] = {}
    failures: list[str] = []
    for label, path, params in _LIST_ENDPOINTS:
        try:
            body = _get_json(svc, path, **params)
        except CheckFailed as e:
            failures.append(f"{label}: {e}")
            continue
        sizes[label] = _size(body)
    if failures:
        raise CheckFailed("; ".join(failures))
    return ", ".join(f"{k}={v}" for k, v in sizes.items()), {"sizes": sizes}


def check_events(svc: SmokeService, _: SmokeOptions) -> tuple[str, dict[str, Any]]:
    simulations = _get_json(svc, "/api/v1/simulations", limit=1)
    if not simulations:
        raise SkipCheck("no simulations exist to read events for")
    simulation_id = simulations[0].get("database_id")
    body = _get_json(svc, f"/api/v1/simulations/{simulation_id}/events", limit=5)
    count = len(body.get("events", body)) if isinstance(body, dict) else len(body)
    return f"simulation {simulation_id}: events endpoint answered ({count} returned)", {"simulation_id": simulation_id}


# --------------------------------------------------------------------------- tier 1


def check_task(svc: SmokeService, opts: SmokeOptions) -> tuple[str, dict[str, Any]]:
    """An uploaded script on the container queue. Passes only when the nonce it prints is
    read back from the task's own log -- COMPLETED alone proves nothing."""
    commit = _resolve_commit(svc, opts)
    nonce = f"smoke-{secrets.token_hex(6)}"
    with tempfile.TemporaryDirectory() as tmp:
        script = Path(tmp) / "atlantis_smoke_task.py"
        script.write_text(f'import sys\nprint("{nonce}", sys.argv[1:])\n', encoding="utf-8")
        task = svc.run_uploaded_task(
            local_path=str(script),
            args=["ok"],
            sim_data_refs=None,
            memory_class="standard",
            commit=commit,
            name=f"atlantis-{nonce}",
        )
    task_id = int(task.database_id)
    evidence: dict[str, Any] = {"task_id": task_id, "commit": commit, "nonce": nonce}
    _poll(opts, lambda: _status_text(svc.get_task_status(task_id).status), f"task {task_id}")
    lines = [str(line) for line in (getattr(svc.get_task_logs(task_id), "lines", None) or [])]
    if not any(nonce in line for line in lines):
        raise CheckFailed(f"task {task_id} COMPLETED but its log does not contain the nonce ({len(lines)} lines read)")
    return f"task {task_id} ran on {commit}; nonce read back from its log", evidence


@contextmanager
def _relayed_worker(svc: SmokeService, commit: str) -> Iterator[str]:
    started = svc.worker_start(commit, session_key=f"smoke-{secrets.token_hex(4)}")
    job_name = str(started.get("job_name") or "")
    if not job_name:
        raise CheckFailed(f"worker start returned no job_name: {started!r}")
    try:
        yield job_name
    finally:
        # Always: a leaked worker is a running K8s Job somebody pays for.
        svc.worker_stop(job_name)


def check_worker(svc: SmokeService, opts: SmokeOptions) -> tuple[str, dict[str, Any]]:
    """Start a relayed env worker (a real K8s Job), read from it, run one task on its task
    tier, stop it. Covers the Job, the dial-back, the held socket, the named read route,
    the durable task record and the runner."""
    probe = svc.client.post("/env-worker/v1/relay/workers/smoke-no-such-worker/call", json={"method": "ping"})
    if probe.status_code == 503:
        raise SkipCheck("relay is off on this deployment (503)")
    commit = _resolve_commit(svc, opts)
    evidence: dict[str, Any] = {"commit": commit}
    with _relayed_worker(svc, commit) as job_name:
        evidence["job_name"] = job_name
        generators = svc.worker_read(job_name, "generators")
        count = len(generators) if hasattr(generators, "__len__") else 0
        evidence["generators"] = count
        if not count:
            raise CheckFailed(f"worker {job_name} answered but lists no generators")

        submitted = svc.worker_submit(job_name, "list_generators")
        task_id = int(submitted["task_id"])
        evidence["task_id"] = task_id
        _poll(opts, lambda: _status_text(svc.worker_task(task_id).get("status")), f"env-worker task {task_id}")
        settled = svc.worker_task(task_id)
        if settled.get("result") in (None, [], {}):
            raise CheckFailed(f"env-worker task {task_id} COMPLETED with an empty result")
    return f"{job_name}: {count} generators read; task {task_id} completed with a result; worker stopped", evidence


def check_compose(svc: SmokeService, opts: SmokeOptions) -> tuple[str, dict[str, Any]]:
    """A five-step composite whose answer is known: level must come back as 1.1**5."""
    with tempfile.TemporaryDirectory() as tmp:
        document = Path(tmp) / "atlantis_smoke.pbg"
        document.write_text(json.dumps(smoke_composite_document()), encoding="utf-8")
        submitted = svc.compose_run_simulation(document, interval_time=float(SMOKE_COMPOSITE_STEPS))
    simulation_id = int(submitted["simulation_database_id"])
    evidence: dict[str, Any] = {"compose_simulation_id": simulation_id}
    _poll(
        opts,
        lambda: _status_text(svc.compose_get_simulation_status(simulation_id).get("status")),
        f"compose simulation {simulation_id}",
    )
    with tempfile.TemporaryDirectory() as tmp:
        archive = svc.compose_get_simulation_results(simulation_id, Path(tmp))
        level, names = level_from_results_archive(archive.read_bytes())
    evidence.update(result_files=names, level=level, expected=SMOKE_COMPOSITE_EXPECTED)
    if level is None:
        raise CheckFailed(f"compose {simulation_id} COMPLETED but no `level` in its results {names}")
    if abs(level - SMOKE_COMPOSITE_EXPECTED) > 1e-6:
        raise CheckFailed(f"compose {simulation_id}: level {level}, expected {SMOKE_COMPOSITE_EXPECTED:.5f}")
    return f"compose {simulation_id}: level {level:.5f} = 1.1^{SMOKE_COMPOSITE_STEPS}", evidence


def check_analysis(svc: SmokeService, opts: SmokeOptions) -> tuple[str, dict[str, Any]]:
    """A standalone analysis over an existing simulation's output. Opt-in: it needs a
    simulation that really has output, and only the caller knows which."""
    if opts.simulation_id is None:
        raise SkipCheck("pass --simulation-id <a completed simulation with output> to run this")
    submitted = svc.run_analysis(opts.simulation_id)
    analysis_id = submitted.get("database_id") or submitted.get("analysis_id") or submitted.get("id")
    if analysis_id is None:
        raise CheckFailed(f"analysis submit returned no id: {submitted!r}")
    evidence: dict[str, Any] = {"simulation_id": opts.simulation_id, "analysis_id": analysis_id}
    _poll(opts, lambda: _status_text(svc.get_analysis_status(int(analysis_id)).status), f"analysis {analysis_id}")
    plots = _get_json(svc, f"/api/v1/analyses/{analysis_id}/plots")
    evidence["plots"] = len(plots)
    if not plots:
        raise CheckFailed(f"analysis {analysis_id} COMPLETED but lists no output files")
    return f"analysis {analysis_id} on simulation {opts.simulation_id}: {len(plots)} output file(s)", evidence


def check_biomodels(svc: SmokeService, opts: SmokeOptions) -> tuple[str, dict[str, Any]]:
    """One BioModels model through the consolidated run endpoint. Opt-in: whether a
    deployment's compose backend can run SBML simulators is a property of the site."""
    if not opts.biomodel_id:
        raise SkipCheck("pass --biomodel <id, e.g. BIOMD0000000001> to run this")
    body = svc.compose_biomodels_run(model_ids=[opts.biomodel_id])
    submitted, failed = body.get("submitted") or [], body.get("failed") or []
    evidence = {"submitted": submitted, "failed": failed}
    if failed or not submitted:
        raise CheckFailed(f"biomodels run for {opts.biomodel_id}: submitted={submitted!r} failed={failed!r}")
    first = submitted[0]
    simulation_id = int(first["simulation_database_id"] if isinstance(first, dict) else first)
    _poll(
        opts,
        lambda: _status_text(svc.compose_get_simulation_status(simulation_id).get("status")),
        f"biomodels compose simulation {simulation_id}",
    )
    return f"{opts.biomodel_id}: compose simulation {simulation_id} completed", evidence


CHECKS: tuple[Check, ...] = (
    Check("version", 0, "/version and /health agree", check_version),
    Check("routes", 0, "every spec operation is served", check_routes),
    Check("capabilities", 0, "the capability registry answers", check_capabilities),
    Check("relay", 0, "the env-worker relay is routed and live", check_relay),
    Check("lists", 0, "database-backed list endpoints answer", check_lists),
    Check("events", 0, "a simulation's events can be read", check_events),
    Check("task", 1, "a container task runs and its output is read back", check_task),
    Check("worker", 1, "a relayed env worker starts, answers, runs a task, stops", check_worker),
    Check("compose", 1, "a composite runs and returns the right number", check_compose),
    Check("analysis", 1, "a standalone analysis produces output (needs --simulation-id)", check_analysis),
    Check("biomodels", 1, "a BioModels model runs (needs --biomodel)", check_biomodels),
)


def select_checks(tier: int, only: Sequence[str] = (), skip: Sequence[str] = ()) -> list[Check]:
    known = {c.name for c in CHECKS}
    unknown = sorted((set(only) | set(skip)) - known)
    if unknown:
        raise ValueError(f"unknown check(s): {', '.join(unknown)}; known: {', '.join(sorted(known))}")
    chosen = [c for c in CHECKS if c.tier <= tier]
    if only:
        chosen = [c for c in CHECKS if c.name in only]
    return [c for c in chosen if c.name not in skip]


def run_checks(
    svc: SmokeService,
    checks: Sequence[Check],
    opts: SmokeOptions,
    on_result: Callable[[CheckResult], None] | None = None,
) -> list[CheckResult]:
    """Run each check in order. One check failing -- or raising something unexpected -- never
    stops the rest: a smoke run's value is the whole picture."""
    results: list[CheckResult] = []
    for check in checks:
        started = opts.clock()
        evidence: dict[str, Any] = {}
        try:
            detail, evidence = check.run(svc, opts)
            outcome = Outcome.PASS
        except SkipCheck as e:
            outcome, detail = Outcome.SKIP, str(e)
        except CheckFailed as e:
            outcome, detail = Outcome.FAIL, str(e)
        except httpx.HTTPError as e:
            outcome, detail = Outcome.FAIL, f"{type(e).__name__}: {e}"
        except Exception as e:
            outcome, detail = Outcome.FAIL, f"check crashed -- {type(e).__name__}: {e}"
        result = CheckResult(check.name, check.tier, outcome, round(opts.clock() - started, 2), detail, evidence)
        results.append(result)
        if on_result:
            on_result(result)
    return results


def summarize(results: Sequence[CheckResult]) -> dict[str, int]:
    return {outcome.value: sum(1 for r in results if r.outcome is outcome) for outcome in Outcome}


def report(base_url: str, tier: int, results: Sequence[CheckResult]) -> dict[str, Any]:
    return {
        "base_url": base_url,
        "tier": tier,
        "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "summary": summarize(results),
        "checks": [asdict(r) for r in results],
    }
