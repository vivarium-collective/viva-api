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

* **Tier 2** -- tens of minutes, dollars: one real simulation per DISPATCH MECHANISM, because
  that is what a change to the dispatch code can break -- the default single-generation
  path, chain dispatch (2 seeds x 2 generations), a Nextflow head, a multi-node composite.
  They run CONCURRENTLY: their cost is waiting on AWS Batch, not on this client.
* **Tier R** (``--tier 3``) -- restart resilience: a job is put in flight, the deployment is
  restarted with the operator's own ``--restart-command``, and the job must still resolve and
  still show its output. Status that lives only in a pod's memory fails this.

Not covered: an image build, and the upstream K8s + Nextflow path -- ``scripts/qualification_test.sh``
remains the check for that one.

The checks take a small Protocol rather than ``E2EDataService`` itself, so each is unit-tested
against a fake; ``E2EDataService`` satisfies it structurally.
"""

from __future__ import annotations

import io
import json
import secrets
import shlex
import subprocess
import tarfile
import tempfile
import time
import zipfile
from collections.abc import Callable, Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, suppress
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
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
    """The deployment answered, and the answer was wrong. ``evidence`` is whatever the check
    had gathered by then -- a failure is when it is wanted most."""

    def __init__(self, message: str, evidence: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.evidence: dict[str, Any] = dict(evidence or {})


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
    simulator_id: int | None = None
    restart_command: str | None = None
    restart_wait_seconds: float = 300.0
    #: A script that EXISTS IN THE IMAGE, for the repo-path task check, and text its output
    #: must contain. The default is the script the service's own ParCa step already depends
    #: on, asked for its usage -- cheap, and present in every simulator image.
    repo_script: str = "scripts/build_cache.py"
    repo_script_args: tuple[str, ...] = ("--help",)
    repo_script_expect: str = "usage:"
    #: ``token -> the still-ACTIVE AWS Batch jobs whose name or command carries it``. The
    #: cancel checks need it: the API answers CANCELLED from its own database row whether or
    #: not anything stopped, so only Batch can say. ``None`` = no AWS access from here; those
    #: checks then SKIP rather than pass on a status.
    active_batch_jobs: Callable[[str], list[dict[str, Any]]] | None = None
    active_batch_jobs_unavailable: str = "no AWS Batch access was configured"
    #: ``sim-mbp``: which ``run_mbp_tracked.py`` variant to dispatch, and for how long. The
    #: default is the script's own reference variant, cut to one simulated minute.
    mbp_variant: str = "baseline-reference-multigen"
    mbp_duration_sec: int = 60
    #: ``build``: opt-in. Builds a MARKED-TEMPORARY simulator: a new record with its own image tag
    #: (``tmp-<commit>-<nonce>``) at the branch's HEAD, or at ``build_commit``. It can never touch
    #: an authoritative simulator, its image or its tag -- those are write-once provenance.
    build: bool = False
    build_repo_url: str | None = None
    build_branch: str | None = None
    build_commit: str | None = None
    #: Set by ``build`` when it passes: the temporary simulator tier 2 then runs on, so the
    #: simulations exercise the image that was just built.
    built_simulator_id: int | None = None
    #: ``commit -> when the registry last received that image tag``, or ``None`` if absent.
    #: The build check needs it for the same reason the cancel checks need Batch: the API
    #: would answer COMPLETED from a build that happened last week.
    image_pushed_at: Callable[[str], datetime | None] | None = None
    image_pushed_at_unavailable: str = "no registry access was configured"
    build_timeout_seconds: float = 3600.0
    now: Callable[[], datetime] = lambda: datetime.now(UTC)
    #: How long a cancelled run's Batch jobs may take to disappear. Covers the Nextflow
    #: head's termination grace period plus a few scheduler ticks of the reaper.
    cancel_settle_seconds: float = 900.0
    poll_seconds: float = 10.0
    timeout_seconds: float = 900.0
    simulation_timeout_seconds: float = 7200.0
    sleep: Callable[[float], None] = time.sleep
    clock: Callable[[], float] = time.monotonic


class SmokeService(Protocol):
    """The slice of ``E2EDataService`` the checks use."""

    @property
    def client(self) -> httpx.Client: ...

    def show_simulators(self) -> Sequence[Any]: ...
    def submit_get_latest_simulator(self, repo_url: str | None = ..., branch: str | None = ...) -> Any: ...
    def submit_upload_simulator(self, simulator: Any, force: bool = ...) -> Any: ...
    def get_simulator_status(self, simulator_id: int) -> str: ...
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
    def run_task(self, request: Any) -> Any: ...
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
    def run_workflow(
        self,
        *,
        experiment_id: str | None = ...,
        simulator_id: int | None = ...,
        num_generations: int | None = ...,
        num_seeds: int | None = ...,
        description: str | None = ...,
        tags: list[str] | None = ...,
        extra_params: dict[str, object] | None = ...,
    ) -> Any: ...
    def get_workflow_status(self, simulation_id: int) -> Any: ...
    def cancel_workflow(self, simulation_id: int) -> Any: ...
    def get_workflow_tasks(self, simulation_id: int) -> Sequence[Any]: ...
    def get_output_data_sync(self, simulation_id: int, dest: Path) -> Path: ...
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


#: How long a status poll may keep failing to REACH the API before the check gives up. A
#: tier-2 poll runs for an hour; a port-forward restarts, an SSM tunnel has a 70-minute
#: lifetime, a pod rolls. None of those is the deployment failing the check.
UNREACHABLE_GRACE_SECONDS = 180.0


def _poll(
    opts: SmokeOptions,
    read_status: Callable[[], str],
    what: str,
    timeout_seconds: float | None = None,
    terminal_ok: frozenset[str] | set[str] = TERMINAL_OK,
    terminal_bad: frozenset[str] | set[str] = TERMINAL_BAD,
) -> str:
    """Poll until a terminal status. Returns it when good; raises when bad or out of time.

    ``terminal_ok`` / ``terminal_bad`` let a check that EXPECTS a bad ending (a task made to
    fail, a run it cancelled) say so: for it, FAILED is the good answer and COMPLETED is not.

    Failing to REACH the API is tolerated for ``UNREACHABLE_GRACE_SECONDS`` at a stretch; an
    answer -- any answer, including an error status -- resets that clock.
    """
    limit = opts.timeout_seconds if timeout_seconds is None else timeout_seconds
    deadline = opts.clock() + limit
    last = "none"
    unreachable_since: float | None = None
    while True:
        try:
            last = read_status()
            unreachable_since = None
        except (httpx.TransportError, httpx.HTTPError) as e:
            now = opts.clock()
            unreachable_since = now if unreachable_since is None else unreachable_since
            if now - unreachable_since >= UNREACHABLE_GRACE_SECONDS:
                raise CheckFailed(
                    f"{what}: API unreachable for {UNREACHABLE_GRACE_SECONDS:.0f} s while polling ({type(e).__name__})"
                ) from e
        else:
            if last in terminal_ok:
                return last
            if last in terminal_bad:
                raise CheckFailed(f"{what} ended {last.upper()}")
        if opts.clock() >= deadline:
            raise CheckFailed(f"{what} still {last.upper()} after {limit:.0f} s")
        opts.sleep(opts.poll_seconds)


def _resolve_commit(svc: SmokeService, opts: SmokeOptions) -> str:
    """The image commit to dispatch on: ``--commit``, else the newest registered simulator
    from a repo that runs on the container/Ray path (its image lives in the same registry
    the task and worker dispatch pull from)."""
    if opts.commit:
        return opts.commit
    return str(_newest_ray_simulator(svc.show_simulators()).git_commit_hash)


def _newest_ray_simulator(simulators: Sequence[Any]) -> Any:
    # Never a temporary one: someone else's test artifact is not "the newest simulator".
    candidates = [
        s
        for s in simulators
        if not getattr(s, "temporary", False)
        and any(tag in str(getattr(s, "git_repo_url", "")).lower() for tag in ("sms-ecoli", "v2ecoli"))
    ]
    if not candidates:
        raise SkipCheck("no simulator was named and no authoritative container-path simulator is registered")
    return max(candidates, key=lambda s: int(getattr(s, "database_id", 0) or 0))


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


def _archive_json_members(data: bytes) -> Iterator[tuple[str, bytes | None]]:
    """``(name, content)`` for every member of a results archive; content only for ``.json``.

    The format is sniffed, not assumed: the Ray path streams a gzipped TAR
    (``application/gzip``) while the SLURM path serves a ZIP -- and the client saves both as
    ``compose_results_<id>.zip``. Found by this check on its second live run, when the job
    finally completed and the "zip" turned out not to be one.
    """
    if data[:2] == b"PK":
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            for name in archive.namelist():
                yield name, archive.read(name) if name.endswith(".json") else None
        return
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:*") as tar:
        for member in tar:
            if not member.isfile():
                continue
            handle = tar.extractfile(member) if member.name.endswith(".json") else None
            yield member.name, handle.read() if handle is not None else None


def level_from_results_archive(data: bytes) -> tuple[float | None, list[str]]:
    """Every JSON member of a results archive is searched; returns the level and the names.
    ``final_state.json`` wins when several members carry a level."""
    names: list[str] = []
    level: float | None = None
    for name, content in _archive_json_members(data):
        names.append(name)
        if content is None:
            continue
        try:
            found = find_level(json.loads(content))
        except ValueError:
            continue
        if found is not None and (level is None or name.endswith("final_state.json")):
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


def check_database(svc: SmokeService, _: SmokeOptions) -> tuple[str, dict[str, Any]]:
    """The database is at the Alembic head this image expects. A deploy whose migration Job
    did not run looks healthy until the first query that touches what changed -- and with
    ``DB_CREATE_ALL=false`` nothing will paper over it."""
    health = _get_json(svc, "/health")
    at_head = health.get("db_at_head")
    evidence = {k: health.get(k) for k in ("db_revision", "db_head", "db_at_head", "db_create_all")}
    if at_head is None:
        raise SkipCheck("this server does not report its database revision in /health (older than the check)")
    if at_head == "unknown":
        raise SkipCheck(f"the server could not read its Alembic head (database revision {health.get('db_revision')})")
    if at_head != "true":
        raise CheckFailed(
            f"database revision is {health.get('db_revision')}, this image expects {health.get('db_head')} "
            "-- the alembic-migrate Job has not run for this deploy"
        )
    note = "" if health.get("db_create_all") == "false" else "; create_all is still ON at startup"
    return f"at head {health.get('db_head')}{note}", evidence


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
    read back from the task's own log -- COMPLETED alone proves nothing -- and when the
    ``sim_data_refs`` it was submitted with reached the container's environment."""
    commit = _resolve_commit(svc, opts)
    nonce = f"smoke-{secrets.token_hex(6)}"
    ref_value = f"s3://smoke/{nonce}"
    with tempfile.TemporaryDirectory() as tmp:
        script = Path(tmp) / "atlantis_smoke_task.py"
        script.write_text(
            f'import sys\nprint("{nonce}", sys.argv[1:])\n'
            'import os\nprint("refs:", os.environ.get("TASK_SIM_DATA_REFS", "<unset>"))\n',
            encoding="utf-8",
        )
        task = svc.run_uploaded_task(
            local_path=str(script),
            args=["ok"],
            sim_data_refs={"smoke_ref": ref_value},
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
    if not any(ref_value in line for line in lines):
        seen = next((line for line in lines if line.startswith("refs:")), "no 'refs:' line")
        raise CheckFailed(f"task {task_id} ran, but its sim_data_refs did not reach the container ({seen})")
    return f"task {task_id} ran on {commit}; nonce and sim_data_refs read back from its log", evidence


def check_task_fail(svc: SmokeService, opts: SmokeOptions) -> tuple[str, dict[str, Any]]:
    """A script that exits 3. Passes only when the task is reported FAILED **and** its log
    shows it really ran: a task that fails because it never started proves nothing about
    how a failure is reported, and one reported COMPLETED is a lie."""
    commit = _resolve_commit(svc, opts)
    nonce = f"smoke-{secrets.token_hex(6)}"
    with tempfile.TemporaryDirectory() as tmp:
        script = Path(tmp) / "atlantis_smoke_task_fail.py"
        script.write_text(f'import sys\nprint("{nonce}", sys.argv[1:])\nsys.exit(3)\n', encoding="utf-8")
        task = svc.run_uploaded_task(
            local_path=str(script),
            args=["fail"],
            sim_data_refs=None,
            memory_class="standard",
            commit=commit,
            name=f"atlantis-{nonce}",
        )
    task_id = int(task.database_id)
    evidence: dict[str, Any] = {"task_id": task_id, "commit": commit, "nonce": nonce}
    ended = _poll(
        opts,
        lambda: _status_text(svc.get_task_status(task_id).status),
        f"task {task_id} (made to exit 3)",
        terminal_ok={"failed"},
        terminal_bad=(TERMINAL_BAD - {"failed"}) | TERMINAL_OK,
    )
    evidence["status"] = ended
    lines = [str(line) for line in (getattr(svc.get_task_logs(task_id), "lines", None) or [])]
    if not any(nonce in line for line in lines):
        raise CheckFailed(f"task {task_id} is FAILED, but its log lacks the nonce: it failed without running")
    return f"task {task_id} exited 3 and is reported FAILED; nonce read back from its log", evidence


def check_task_repo(svc: SmokeService, opts: SmokeOptions) -> tuple[str, dict[str, Any]]:
    """A script ALREADY IN THE IMAGE, by its repo path -- the other way in (`POST
    /api/v1/tasks`, no upload, no stage-in). Passes only on text the script itself prints."""
    from viva_api.simulation.models import TaskRunRequest

    commit = _resolve_commit(svc, opts)
    name = f"atlantis-smoke-repo-{secrets.token_hex(4)}"
    task = svc.run_task(
        TaskRunRequest(script=opts.repo_script, args=list(opts.repo_script_args), commit=commit, name=name)
    )
    task_id = int(task.database_id)
    evidence: dict[str, Any] = {"task_id": task_id, "commit": commit, "script": opts.repo_script}
    _poll(opts, lambda: _status_text(svc.get_task_status(task_id).status), f"task {task_id} ({opts.repo_script})")
    lines = [str(line) for line in (getattr(svc.get_task_logs(task_id), "lines", None) or [])]
    if not any(opts.repo_script_expect in line for line in lines):
        raise CheckFailed(
            f"task {task_id} COMPLETED but its log does not contain {opts.repo_script_expect!r} "
            f"({len(lines)} lines read)"
        )
    return f"task {task_id} ran {opts.repo_script} from the image on {commit}", evidence


def check_build(svc: SmokeService, opts: SmokeOptions) -> tuple[str, dict[str, Any]]:
    """Build a MARKED-TEMPORARY simulator and pass only when the REGISTRY shows its image arrive.

    No other tier builds anything: every simulation check reuses a simulator that already
    has an image. So the whole build path -- the LOCAL task, the Batch DooD job, the repo's
    own recipe, the push -- is otherwise unexercised by a deploy. It runs in tier 1, before
    the simulations, which then run on the image this check just built.

    **It cannot touch an authoritative simulator.** Simulator records, images and tags are
    write-once provenance (``docs/plan-core.md`` D11). What this check makes is the marked
    exception: a new record with ``temporary=True``, a label saying this check made it, and
    its own image tag ``tmp-<commit>-<nonce>`` -- never ``<commit>``, which belongs to the
    authoritative build of that commit. The server refuses ``force`` on an authoritative
    simulator; this check never sends it.
    """
    from viva_api.simulation.models import Simulator

    if not opts.build:
        raise SkipCheck("pass --build to build a marked-temporary simulator (~15 min, several GB)")
    if opts.image_pushed_at is None:
        raise SkipCheck(f"needs to see the image registry to verify a push ({opts.image_pushed_at_unavailable})")

    newest = _newest_ray_simulator(svc.show_simulators())
    repo_url = opts.build_repo_url or str(newest.git_repo_url)
    branch = opts.build_branch or str(newest.git_branch)
    commit = (opts.build_commit or str(svc.submit_get_latest_simulator(repo_url, branch).git_commit_hash))[:7]
    label = f"atlantis-smoke {secrets.token_hex(4)}"
    evidence: dict[str, Any] = {"repo_url": repo_url, "branch": branch, "commit": commit, "label": label}

    started = opts.now()
    built = svc.submit_upload_simulator(
        Simulator(git_commit_hash=commit, git_repo_url=repo_url, git_branch=branch, temporary=True, label=label),
        force=False,
    )
    simulator_id, image_tag = int(built.database_id), getattr(built, "image_tag", None)
    evidence.update(simulator_id=simulator_id, image_tag=image_tag)
    if not getattr(built, "temporary", False) or not image_tag or image_tag == commit:
        raise CheckFailed(
            f"asked for a temporary simulator and got simulator {simulator_id} (temporary="
            f"{getattr(built, 'temporary', None)}, image_tag={image_tag!r}): refusing to build under the commit's tag",
            evidence,
        )
    _poll(
        opts,
        lambda: _status_text(svc.get_simulator_status(simulator_id)),
        f"build of temporary simulator {simulator_id} ({image_tag})",
        timeout_seconds=opts.build_timeout_seconds,
    )
    pushed = opts.image_pushed_at(str(image_tag))
    evidence["pushed_at"] = pushed
    if pushed is None:
        raise CheckFailed(f"build COMPLETED but the registry has no image tagged {image_tag}", evidence)
    if pushed < started:
        raise CheckFailed(f"build COMPLETED but {image_tag} was pushed before this check began", evidence)
    opts.built_simulator_id = simulator_id
    when = pushed.astimezone(UTC)
    return f"temporary simulator {simulator_id} built as {image_tag}; image pushed {when:%H:%M:%S}Z", evidence


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


# --------------------------------------------------------------------------- tier 2

#: The composite ids the two composite-shaped dispatch mechanisms default to in the CLI.
NEXTFLOW_COMPOSITE_ID = "v2ecoli.composites.workflow_nf.workflow_nf"
MULTI_NODE_COMPOSITE_ID = "v2ecoli.composites.lineage_ray_batch"


def _resolve_simulator(svc: SmokeService, opts: SmokeOptions) -> int:
    """``--simulator-id``; else the temporary simulator ``build`` just made in this run (so the
    simulations exercise the fresh image); else the newest AUTHORITATIVE container-path one."""
    if opts.simulator_id is not None:
        return opts.simulator_id
    if opts.built_simulator_id is not None:
        return opts.built_simulator_id
    return int(_newest_ray_simulator(svc.show_simulators()).database_id)


def _run_simulation(
    svc: SmokeService,
    opts: SmokeOptions,
    kind: str,
    *,
    generations: int | None = None,
    seeds: int | None = None,
    extra_params: dict[str, object] | None = None,
) -> tuple[int, dict[str, Any]]:
    """Submit one simulation exactly as the CLI would, and wait for it. Returns its id."""
    simulator_id = _resolve_simulator(svc, opts)
    experiment_id = f"smoke-{kind}-{secrets.token_hex(4)}"
    simulation = svc.run_workflow(
        experiment_id=experiment_id,
        simulator_id=simulator_id,
        num_generations=generations,
        num_seeds=seeds,
        description=f"atlantis smoke: {kind} dispatch path",
        tags=["smoke", f"smoke-{kind}"],
        extra_params=extra_params,
    )
    simulation_id = int(simulation.database_id)
    evidence: dict[str, Any] = {
        "simulation_id": simulation_id,
        "experiment_id": experiment_id,
        "simulator_id": simulator_id,
    }
    _poll(
        opts,
        lambda: _status_text(svc.get_workflow_status(simulation_id).status),
        f"{kind} simulation {simulation_id}",
        timeout_seconds=opts.simulation_timeout_seconds,
    )
    return simulation_id, evidence


def _require_outputs(svc: SmokeService, simulation_id: int, evidence: dict[str, Any], expect_summaries: int) -> int:
    """COMPLETED is a claim; the output is the fact. Download it and count the per-seed
    ``summary.json`` files a finished lineage writes."""
    with tempfile.TemporaryDirectory() as tmp:
        root = svc.get_output_data_sync(simulation_id, Path(tmp))
        files = [p for p in root.rglob("*") if p.is_file()]
        summaries = [p for p in files if p.name == "summary.json" and p.parent != root]
    evidence.update(output_files=len(files), seed_summaries=len(summaries))
    if len(summaries) < expect_summaries:
        raise CheckFailed(
            f"simulation {simulation_id} COMPLETED but its output has {len(summaries)} per-seed summary.json "
            f"file(s), expected {expect_summaries} ({len(files)} files in all)"
        )
    return len(files)


def check_sim_default(svc: SmokeService, opts: SmokeOptions) -> tuple[str, dict[str, Any]]:
    """The default path: one seed, one generation."""
    simulation_id, evidence = _run_simulation(svc, opts, "default", generations=1, seeds=1)
    files = _require_outputs(svc, simulation_id, evidence, expect_summaries=1)
    return f"simulation {simulation_id}: completed, {files} output files, 1 seed summary", evidence


def check_sim_chain(svc: SmokeService, opts: SmokeOptions) -> tuple[str, dict[str, Any]]:
    """Chain dispatch -- selected by more than one generation: 2 seeds x 2 generations, one
    Batch job per seed per generation, gated by the scheduler."""
    simulation_id, evidence = _run_simulation(svc, opts, "chain", generations=2, seeds=2)
    progress = _get_json(svc, f"/api/v1/simulations/{simulation_id}/chain-progress")
    evidence["chain_progress"] = progress
    if not progress.get("terminal") or progress.get("seeds_succeeded") != progress.get("seeds_total") != 0:
        raise CheckFailed(f"chain {simulation_id} COMPLETED but chain-progress says {progress}")
    if progress.get("seeds_total") != 2:
        raise CheckFailed(f"chain {simulation_id}: expected 2 seeds, chain-progress says {progress}")
    files = _require_outputs(svc, simulation_id, evidence, expect_summaries=2)
    return f"simulation {simulation_id}: 2/2 seeds succeeded over 2 generations, {files} output files", evidence


def check_sim_nextflow(svc: SmokeService, opts: SmokeOptions) -> tuple[str, dict[str, Any]]:
    """A Nextflow head (a K8s Job) driving one Batch task per lineage."""
    dispatch: dict[str, object] = {
        "composite_id": NEXTFLOW_COMPOSITE_ID,
        "executor": "awsbatch",
        "launch": True,
        "params": {"n_seeds": 1, "n_generations": 1},
    }
    simulation_id, evidence = _run_simulation(svc, opts, "nextflow", extra_params={"nextflow_dispatch": dispatch})
    tasks = list(svc.get_workflow_tasks(simulation_id))
    states = sorted({_status_text(getattr(t, "status", None)) for t in tasks})
    evidence.update(tasks=len(tasks), task_states=states)
    if not tasks:
        raise CheckFailed(f"nextflow {simulation_id} COMPLETED but its trace lists no tasks")
    if states != ["completed"]:
        raise CheckFailed(f"nextflow {simulation_id} COMPLETED but its tasks are {states}")
    return f"simulation {simulation_id}: {len(tasks)} Nextflow task(s), all completed", evidence


def check_sim_composite(svc: SmokeService, opts: SmokeOptions) -> tuple[str, dict[str, Any]]:
    """A multi-node process-bigraph composite on Ray actors inside one Batch MNP job."""
    dispatch: dict[str, object] = {
        "composite_id": MULTI_NODE_COMPOSITE_ID,
        "num_nodes": 2,
        "params": {"n_seeds": 1, "n_generations": 1},
        "steps": 36000,
    }
    simulation_id, evidence = _run_simulation(svc, opts, "composite", extra_params={"multi_node_dispatch": dispatch})
    files = _require_outputs(svc, simulation_id, evidence, expect_summaries=0)
    if not files:
        raise CheckFailed(f"composite {simulation_id} COMPLETED but its output is empty")
    return f"simulation {simulation_id}: completed, {files} output files", evidence


def check_sim_mbp(svc: SmokeService, opts: SmokeOptions) -> tuple[str, dict[str, Any]]:
    """The mbp-tracked dispatch: ``run_mbp_tracked.py`` as one container job (after its own
    ParCa job), writing through the script's runtime emitter. The point of this mechanism is
    that its output SURVIVES the container -- a run that completes and leaves nothing is the
    bug it was built to fix -- so that is what is asserted."""
    dispatch: dict[str, object] = {
        "variant": opts.mbp_variant,
        "duration_sec": opts.mbp_duration_sec,
        "max_generations": 1,
    }
    simulation_id, evidence = _run_simulation(svc, opts, "mbp", extra_params={"mbp_dispatch": dispatch})
    evidence["variant"] = opts.mbp_variant
    files = _require_outputs(svc, simulation_id, evidence, expect_summaries=0)
    if not files:
        raise CheckFailed(f"mbp-tracked {simulation_id} COMPLETED but its output is empty", evidence)
    return f"simulation {simulation_id}: {opts.mbp_variant} completed, {files} output files", evidence


# ------------------------------------------------------------------- tier 2: cancel


def _wait_for_batch(
    opts: SmokeOptions,
    token: str,
    *,
    want_active: bool,
    limit: float,
    what: str,
    on_tick: Callable[[], None] | None = None,
) -> list[dict[str, Any]]:
    """Poll AWS Batch until jobs carrying ``token`` are active (``want_active``) or gone.
    Returns the last listing. ``on_tick`` runs each round and may raise to abort the wait."""
    assert opts.active_batch_jobs is not None  # noqa: S101 - callers SKIP before reaching here
    deadline = opts.clock() + limit
    while True:
        jobs = opts.active_batch_jobs(token)
        if bool(jobs) == want_active:
            return jobs
        if on_tick is not None:
            on_tick()
        if opts.clock() >= deadline:
            state = f"still active: {'; '.join(_describe_batch_job(j) for j in jobs)}" if jobs else "none ever appeared"
            raise CheckFailed(f"{what}: after {limit:.0f} s, Batch jobs for {token} -- {state}", {"still_active": jobs})
        opts.sleep(opts.poll_seconds)


def _describe_batch_job(job: dict[str, Any]) -> str:
    """``name STATUS``, plus what it is waiting on -- a job that was terminated while PENDING
    stays PENDING until its dependency finishes, and that dependency is the real leak."""
    text = f"{job.get('jobName')} {job.get('status')}"
    if job.get("terminated"):
        text += " (terminate accepted)"
    waiting = [f"{d.get('jobName')} {d.get('status')}" for d in job.get("waiting_on") or []]
    return f"{text}, waiting on {', '.join(waiting)}" if waiting else text


def _wait_until_cancellable(
    opts: SmokeOptions, ready: Callable[[int], bool] | None, simulation_id: int, kind: str
) -> None:
    """Work in Batch does not mean the API's row is the one a cancel must act on yet: a chain
    campaign is a placeholder until its background submit records the campaign."""
    deadline = opts.clock() + 120.0
    while ready is not None and not ready(simulation_id):
        if opts.clock() >= deadline:
            raise CheckFailed(f"{kind} {simulation_id} has work in Batch but never became cancellable")
        opts.sleep(opts.poll_seconds)


def _cancel_and_verify(
    svc: SmokeService,
    opts: SmokeOptions,
    kind: str,
    *,
    extra_params: dict[str, object] | None = None,
    generations: int = 1,
    seeds: int = 1,
    ready: Callable[[int], bool] | None = None,
) -> tuple[str, dict[str, Any]]:
    """Submit a run, wait until AWS Batch shows work for it, cancel it, and pass only when
    Batch shows NONE of it still active -- and the run is still CANCELLED afterwards.

    The API's answer cannot be the assertion: the cancel handler writes CANCELLED to its own
    row whether or not anything stopped, and the status endpoint reads that row back.
    """
    if opts.active_batch_jobs is None:
        raise SkipCheck(
            f"needs to see AWS Batch to verify that anything actually stopped ({opts.active_batch_jobs_unavailable})"
        )
    simulator_id = _resolve_simulator(svc, opts)
    experiment_id = f"smoke-{kind}-{secrets.token_hex(4)}"
    simulation = svc.run_workflow(
        experiment_id=experiment_id,
        simulator_id=simulator_id,
        num_generations=None if extra_params else generations,
        num_seeds=None if extra_params else seeds,
        description=f"atlantis smoke: {kind} (submitted to be cancelled)",
        tags=["smoke", f"smoke-{kind}"],
        extra_params=extra_params,
    )
    simulation_id = int(simulation.database_id)
    evidence: dict[str, Any] = {"simulation_id": simulation_id, "experiment_id": experiment_id}

    def _still_cancellable() -> None:
        status = _status_text(svc.get_workflow_status(simulation_id).status)
        if status in TERMINAL_OK | TERMINAL_BAD:
            raise CheckFailed(f"{kind} {simulation_id} ended {status.upper()} before there was anything to cancel")

    def _drive() -> None:
        before = _wait_for_batch(
            opts,
            experiment_id,
            want_active=True,
            limit=opts.timeout_seconds,
            what=f"{kind} {simulation_id} waiting for work to appear",
            on_tick=_still_cancellable,
        )
        evidence["active_before_cancel"] = sorted({str(j.get("jobName")) for j in before})
        _wait_until_cancellable(opts, ready, simulation_id, kind)
        answered = _status_text(getattr(svc.cancel_workflow(simulation_id), "status", None))
        evidence["cancel_answered"] = answered
        if answered != "cancelled":
            raise CheckFailed(f"cancel of {kind} {simulation_id} answered {answered.upper()}, not CANCELLED")
        _wait_for_batch(
            opts,
            experiment_id,
            want_active=False,
            limit=opts.cancel_settle_seconds,
            what=f"{kind} {simulation_id} was cancelled",
        )

    try:
        _drive()
    except BaseException as e:
        # Never leave a smoke run burning compute because the check itself gave up.
        with suppress(Exception):
            svc.cancel_workflow(simulation_id)
        if isinstance(e, CheckFailed):
            e.evidence = {**evidence, **e.evidence}
        raise
    after = _status_text(svc.get_workflow_status(simulation_id).status)
    evidence["status_after"] = after
    if after != "cancelled":
        raise CheckFailed(f"{kind} {simulation_id} was CANCELLED, and is now {after.upper()}")
    stopped = len(evidence["active_before_cancel"])
    return (
        f"simulation {simulation_id}: {stopped} active Batch job(s) before cancel, none after; still CANCELLED",
        evidence,
    )


def check_sim_cancel(svc: SmokeService, opts: SmokeOptions) -> tuple[str, dict[str, Any]]:
    """Cancel a default-path simulation: one ``terminate_job`` on its Batch job."""
    return _cancel_and_verify(svc, opts, "cancel")


def check_chain_cancel(svc: SmokeService, opts: SmokeOptions) -> tuple[str, dict[str, Any]]:
    """Cancel a chain campaign (2 seeds x 2 generations) as soon as Batch shows work for it --
    which is its ParCa phase, where no seed has a job yet and the only thing to stop is the
    campaign's ParCa job. A cancel that walks the per-seed jobs alone stops nothing there
    (viva-api#709)."""

    def _campaign_recorded(simulation_id: int) -> bool:
        resp = svc.client.get(f"/api/v1/simulations/{simulation_id}/chain-progress")
        return resp.status_code == 200 and resp.json().get("seeds_total") == 2

    summary, evidence = _cancel_and_verify(svc, opts, "chaincancel", generations=2, seeds=2, ready=_campaign_recorded)
    names = evidence["active_before_cancel"]
    phase = "ParCa phase" if all(name.startswith("ray-parca") for name in names) else "seeds already running"
    evidence["phase_at_cancel"] = phase
    return f"{summary} (cancelled in the {phase}: {', '.join(names)})", evidence


def check_nextflow_cancel(svc: SmokeService, opts: SmokeOptions) -> tuple[str, dict[str, Any]]:
    """Cancel a Nextflow run. The API deletes the head Job; the tasks Nextflow submitted are
    stopped by its own shutdown hook, or -- if any outlive the head -- by the scheduler's
    reaper, which finds them by campaign across every task queue. Either way: none left."""
    dispatch: dict[str, object] = {
        "composite_id": NEXTFLOW_COMPOSITE_ID,
        "executor": "awsbatch",
        "launch": True,
        "params": {"n_seeds": 1, "n_generations": 1},
    }
    summary, evidence = _cancel_and_verify(svc, opts, "nfcancel", extra_params={"nextflow_dispatch": dispatch})
    events = _get_json(svc, f"/api/v1/simulations/{evidence['simulation_id']}/events", event="dispatch.reaped")
    reaped = events.get("events", []) if isinstance(events, dict) else events
    evidence["reaped_by_scheduler"] = len(reaped)
    who = "the scheduler's reaper stepped in" if reaped else "Nextflow's own shutdown hook sufficed"
    return f"{summary} ({who})", evidence


class AwsBatchJobLister:
    """The real ``SmokeOptions.active_batch_jobs``: every still-active job, on every queue
    of the account, whose name or container command contains the token. Constructing it
    raises if Batch cannot be reached with the caller's credentials -- the CLI turns that
    into a SKIP reason.

    Read-only (Describe*/List*). A default-path job carries the experiment id in its NAME; a
    Nextflow task only in its COMMAND (the campaign's work dir); a ParCa job only in its
    ``ExperimentId`` TAG. Hence all three.
    """

    ACTIVE = ("SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING")

    def __init__(self, region: str | None = None, client: Any = None) -> None:
        if client is None:
            import boto3
            from botocore.config import Config

            # One look is (queues x 5 states) ListJobs calls -- 65 on an account with 13
            # queues -- so let botocore absorb throttling rather than fail the check on it.
            config = Config(retries={"mode": "standard", "max_attempts": 8})
            client = boto3.client("batch", region_name=region, config=config)
        self._client = client
        self._queue_names: list[str] | None = None
        self._client.describe_job_queues(maxResults=1)  # fail here, with a reason, not mid-check

    def _pages(self, call: Callable[..., dict[str, Any]], **kwargs: Any) -> Iterator[dict[str, Any]]:
        while True:
            page = call(**kwargs)
            yield page
            if not page.get("nextToken"):
                return
            kwargs["nextToken"] = page["nextToken"]

    def _queues(self) -> list[str]:
        """Every queue in the account, listed once: they do not change during a check, and
        the server's own queue settings are not something a client can read."""
        if self._queue_names is None:
            self._queue_names = self._list_queues()
        return self._queue_names

    def _list_queues(self) -> list[str]:
        return [
            q["jobQueueName"]
            for page in self._pages(self._client.describe_job_queues)
            for q in page.get("jobQueues", [])
        ]

    def _matching(self, summaries: list[dict[str, Any]], token: str) -> Iterator[dict[str, Any]]:
        by_command: list[str] = []
        for job in summaries:
            if token in str(job.get("jobName", "")):
                yield job
            else:
                by_command.append(job["jobId"])
        for i in range(0, len(by_command), 100):
            for job in self._client.describe_jobs(jobs=by_command[i : i + 100]).get("jobs", []):
                if any(token in text for text in self._identifying_text(job)):
                    yield job

    @staticmethod
    def _identifying_text(job: dict[str, Any]) -> Iterator[str]:
        """Where a run's experiment id can be found on a job that is not NAMED for it: a
        Nextflow task's command (the campaign work dir), and the cost-allocation tags every
        dispatch path sets (``ExperimentId``) -- the only place a ParCa job carries it, since
        a ParCa job is named for the COMMIT."""
        yield " ".join((job.get("container") or {}).get("command") or [])
        yield from (str(value) for value in (job.get("tags") or {}).values())

    def __call__(self, token: str) -> list[dict[str, Any]]:
        found: list[dict[str, Any]] = []
        for queue in self._queues():
            for status in self.ACTIVE:
                for page in self._pages(self._client.list_jobs, jobQueue=queue, jobStatus=status):
                    for job in self._matching(page.get("jobSummaryList", []), token):
                        found.append({"jobId": job["jobId"], "jobName": job.get("jobName"), "status": status})
        return self._with_dependencies(found) if found else found

    def _with_dependencies(self, found: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Add ``terminated`` and ``waiting_on`` (the jobs each one depends on, with their
        states). Only for what matched -- a handful -- so it costs two DescribeJobs calls."""
        detail = {j["jobId"]: j for j in self._client.describe_jobs(jobs=[f["jobId"] for f in found]).get("jobs", [])}
        wanted = sorted({d["jobId"] for j in detail.values() for d in j.get("dependsOn") or []})
        parents = (
            {j["jobId"]: j for j in self._client.describe_jobs(jobs=wanted[:100]).get("jobs", [])} if wanted else {}
        )
        for job in found:
            described = detail.get(job["jobId"], {})
            job["terminated"] = bool(described.get("isTerminated") or described.get("isCancelled"))
            job["waiting_on"] = [
                {
                    "jobId": d["jobId"],
                    "jobName": parents[d["jobId"]].get("jobName"),
                    "status": parents[d["jobId"]].get("status"),
                }
                for d in described.get("dependsOn") or []
                if d["jobId"] in parents
            ]
        return found


class AwsImagePushedAt:
    """The real ``SmokeOptions.image_pushed_at``: when ECR last received ``<repository>:<tag>``.
    Read-only (``ecr:DescribeImages``). Constructing it raises if ECR cannot be reached with
    the caller's credentials -- the CLI turns that into a SKIP reason."""

    def __init__(self, repository: str, region: str | None = None, client: Any = None) -> None:
        if client is None:
            import boto3

            client = boto3.client("ecr", region_name=region) if region else boto3.client("ecr")
        self._client, self._repository = client, repository
        self._client.describe_repositories(repositoryNames=[repository])  # fail here, with a reason

    def __call__(self, tag: str) -> datetime | None:
        try:
            images = self._client.describe_images(repositoryName=self._repository, imageIds=[{"imageTag": tag}])
        except Exception as e:
            if type(e).__name__ == "ImageNotFoundException":
                return None
            raise
        details = images.get("imageDetails") or []
        pushed = details[0].get("imagePushedAt") if details else None
        if not isinstance(pushed, datetime):
            return None
        return pushed if pushed.tzinfo else pushed.replace(tzinfo=UTC)


# --------------------------------------------------------------------------- tier R


def check_restart(svc: SmokeService, opts: SmokeOptions) -> tuple[str, dict[str, Any]]:
    """Put a job in flight, restart the deployment, and require the job to still resolve.

    The restart is the operator's own command (``kubectl rollout restart ...``, a deploy
    script): this client has no cluster access and should not. The command must return only
    once the API is reachable again AT THE SAME URL -- through a ``kubectl port-forward`` that
    means re-establishing the forward too.
    """
    if not opts.restart_command:
        raise SkipCheck('pass --restart-command "<how to restart this deployment>" to run this')
    commit = _resolve_commit(svc, opts)
    nonce = f"smoke-restart-{secrets.token_hex(6)}"
    with tempfile.TemporaryDirectory() as tmp:
        script = Path(tmp) / "atlantis_smoke_restart.py"
        script.write_text(f'print("{nonce}")\n', encoding="utf-8")
        task = svc.run_uploaded_task(
            local_path=str(script),
            args=[],
            sim_data_refs=None,
            memory_class="standard",
            commit=commit,
            name=f"atlantis-{nonce}",
        )
    task_id = int(task.database_id)
    version_before = _get_json(svc, "/version")
    evidence: dict[str, Any] = {"task_id": task_id, "commit": commit, "version": version_before}

    started = opts.clock()
    completed = subprocess.run(  # noqa: S603 - the operator's own command, given on the command line
        shlex.split(opts.restart_command), capture_output=True, text=True, check=False
    )
    evidence["restart_seconds"] = round(opts.clock() - started, 1)
    if completed.returncode != 0:
        raise CheckFailed(f"restart command exited {completed.returncode}: {completed.stderr.strip()[-300:]}")

    deadline = opts.clock() + opts.restart_wait_seconds
    while True:
        try:
            version_after = _get_json(svc, "/version")
            break
        except (CheckFailed, httpx.HTTPError) as e:
            if opts.clock() >= deadline:
                raise CheckFailed(f"API did not come back within {opts.restart_wait_seconds:.0f} s: {e}") from e
            opts.sleep(opts.poll_seconds)
    if version_after != version_before:
        raise CheckFailed(f"/version changed across the restart: {version_before} -> {version_after}")

    _poll(opts, lambda: _status_text(svc.get_task_status(task_id).status), f"task {task_id} (across a restart)")
    lines = [str(line) for line in (getattr(svc.get_task_logs(task_id), "lines", None) or [])]
    if not any(nonce in line for line in lines):
        raise CheckFailed(f"task {task_id} resolved after the restart but its log lacks the nonce")
    return (
        f"task {task_id} submitted, deployment restarted ({evidence['restart_seconds']} s), task still resolved",
        evidence,
    )


CHECKS: tuple[Check, ...] = (
    Check("version", 0, "/version and /health agree", check_version),
    Check("database", 0, "the database is at the Alembic head this image expects", check_database),
    Check("routes", 0, "every spec operation is served", check_routes),
    Check("capabilities", 0, "the capability registry answers", check_capabilities),
    Check("relay", 0, "the env-worker relay is routed and live", check_relay),
    Check("lists", 0, "database-backed list endpoints answer", check_lists),
    Check("events", 0, "a simulation's events can be read", check_events),
    Check("task", 1, "a container task runs and its output is read back", check_task),
    Check("task-fail", 1, "a task that exits 3 is reported FAILED, having really run", check_task_fail),
    Check("task-repo", 1, "a script already in the image runs by its repo path", check_task_repo),
    Check(
        "build",
        1,
        "a marked-TEMPORARY simulator is built and its image reaches the registry (needs --build)",
        check_build,
    ),
    Check("worker", 1, "a relayed env worker starts, answers, runs a task, stops", check_worker),
    Check("compose", 1, "a composite runs and returns the right number", check_compose),
    Check("analysis", 1, "a standalone analysis produces output (needs --simulation-id)", check_analysis),
    Check("biomodels", 1, "a BioModels model runs (needs --biomodel)", check_biomodels),
    Check("sim-default", 2, "default dispatch: 1 seed x 1 generation completes and writes output", check_sim_default),
    Check("sim-chain", 2, "chain dispatch: 2 seeds x 2 generations, every seed succeeds", check_sim_chain),
    Check("sim-nextflow", 2, "Nextflow head: every traced task completes", check_sim_nextflow),
    Check("sim-composite", 2, "multi-node composite on Ray completes and writes output", check_sim_composite),
    Check("sim-mbp", 2, "mbp-tracked dispatch completes and its output survives the container", check_sim_mbp),
    Check("sim-cancel", 2, "a cancelled simulation leaves no active Batch job (needs AWS access)", check_sim_cancel),
    Check(
        "chain-cancel",
        2,
        "a chain campaign cancelled in its ParCa phase leaves no active Batch job (needs AWS access)",
        check_chain_cancel,
    ),
    Check(
        "nextflow-cancel",
        2,
        "a cancelled Nextflow run leaves no active Batch task (needs AWS access)",
        check_nextflow_cancel,
    ),
    Check("restart", 3, "a job in flight survives a restart (needs --restart-command)", check_restart),
)

#: Checks that assert on AWS Batch directly and so need the operator's own AWS access.
NEEDS_BATCH_ACCESS = frozenset({"sim-cancel", "chain-cancel", "nextflow-cancel"})
#: ...and the one that asserts on the image registry.
NEEDS_REGISTRY_ACCESS = frozenset({"build"})

#: Tier 2 checks spend their time waiting on AWS Batch, so they run side by side.
CONCURRENT_TIERS = frozenset({2})


def select_checks(tier: int, only: Sequence[str] = (), skip: Sequence[str] = ()) -> list[Check]:
    known = {c.name for c in CHECKS}
    unknown = sorted((set(only) | set(skip)) - known)
    if unknown:
        raise ValueError(f"unknown check(s): {', '.join(unknown)}; known: {', '.join(sorted(known))}")
    chosen = [c for c in CHECKS if c.tier <= tier]
    if only:
        chosen = [c for c in CHECKS if c.name in only]
    return [c for c in chosen if c.name not in skip]


def _run_one(svc: SmokeService, check: Check, opts: SmokeOptions) -> CheckResult:
    started = opts.clock()
    evidence: dict[str, Any] = {}
    try:
        detail, evidence = check.run(svc, opts)
        outcome = Outcome.PASS
    except SkipCheck as e:
        outcome, detail = Outcome.SKIP, str(e)
    except CheckFailed as e:
        outcome, detail, evidence = Outcome.FAIL, str(e), e.evidence
    except httpx.HTTPError as e:
        outcome, detail = Outcome.FAIL, f"{type(e).__name__}: {e}"
    except Exception as e:
        outcome, detail = Outcome.FAIL, f"check crashed -- {type(e).__name__}: {e}"
    return CheckResult(check.name, check.tier, outcome, round(opts.clock() - started, 2), detail, evidence)


def run_checks(
    svc: SmokeService,
    checks: Sequence[Check],
    opts: SmokeOptions,
    on_result: Callable[[CheckResult], None] | None = None,
) -> list[CheckResult]:
    """Run the checks. One check failing -- or raising something unexpected -- never stops
    the rest: a smoke run's value is the whole picture.

    Tiers 0, 1 and R run in order. Tier 2 checks are submitted together and awaited together:
    each is tens of minutes of AWS Batch time and seconds of this client's.
    """
    results: list[CheckResult] = []

    def record(result: CheckResult) -> None:
        results.append(result)
        if on_result:
            on_result(result)

    serial = [c for c in checks if c.tier not in CONCURRENT_TIERS]
    concurrent = [c for c in checks if c.tier in CONCURRENT_TIERS]
    for check in (c for c in serial if c.tier < min(CONCURRENT_TIERS)):
        record(_run_one(svc, check, opts))
    if concurrent:
        with ThreadPoolExecutor(max_workers=len(concurrent)) as pool:
            for result in pool.map(lambda c: _run_one(svc, c, opts), concurrent):
                record(result)
    for check in (c for c in serial if c.tier > max(CONCURRENT_TIERS)):
        record(_run_one(svc, check, opts))
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
