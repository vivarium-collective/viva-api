"""Env-worker router — run a simulator's own image as a workbench env worker.

Mounted at ``/env-worker/v1/``. Step 2 of vivarium-workbench#942 /
REFACTOR-PLAN §2A.8.

The workbench cannot create Jobs (§2B.2 gives it no cluster access), so it asks
here. It tells us **where to dial back and with what token** — it already knows
its own address, so viva-api discovers nothing and needs no pod-get.
"""

import functools
import logging
import os
import secrets
from collections.abc import Callable
from typing import TYPE_CHECKING

import anyio.to_thread
from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, Field

from viva_api.api.auth import require_caller, resolve_caller
from viva_api.compose import env_worker_relay as relay
from viva_api.compose.env_worker_service import (
    EnvWorkerJobExists,
    EnvWorkerLaunchError,
    EnvWorkerService,
)
from viva_api.compose.models import ComposeJobStatus, EnvWorkerTask

if TYPE_CHECKING:
    from viva_api.compose.database_service import EnvWorkerTaskDatabaseService

logger = logging.getLogger(__name__)

router = APIRouter()

_env_worker_service: EnvWorkerService | None = None


def set_env_worker_service(service: EnvWorkerService | None) -> None:
    """Wired at app startup (dependencies.py), like the other routers."""
    global _env_worker_service
    _env_worker_service = service


_task_db: "EnvWorkerTaskDatabaseService | None" = None


def set_env_worker_task_service(db: "EnvWorkerTaskDatabaseService | None") -> None:
    """Wired at app startup (dependencies.py), like set_env_worker_service above.

    A module global rather than a FastAPI dependency because that is this
    codebase's convention for subsystem services -- see set_compose_services in
    routers/compose.py -- and a second convention would only make the wiring
    harder to find.
    """
    global _task_db
    _task_db = db


def _require_service() -> EnvWorkerService:
    if _env_worker_service is None:
        raise HTTPException(503, "env-worker service is not configured on this deployment")
    return _env_worker_service


class EnvWorkerStartRequest(BaseModel):
    """Where to dial back, and which environment to run.

    ``commit`` selects the environment: it is the tag of the prebuilt simulator
    image, which under §2A.8 *is* the execution environment rather than a recipe
    for rebuilding one.
    """

    commit: str = Field(..., description="Simulator commit; the prebuilt image tag to run")
    callback_host: str = Field(..., description="Host/IP the worker dials back to (the workbench pod IP)")
    callback_port: int = Field(..., ge=1, le=65535, description="Port the workbench is listening on")
    token: str = Field(..., description="One-time handshake token the worker must present")
    workspace: str | None = Field(
        None,
        description="Workspace path inside the worker container; defaults to the "
        "deployment's env_worker_workspace_path (the image's own checkout)",
    )
    session_key: str | None = Field(None, description="Owning session; makes the Job name unique per session")


class EnvWorkerStartResponse(BaseModel):
    job_name: str
    image: str
    namespace: str


class EnvWorkerStatusResponse(BaseModel):
    job_name: str
    status: str | None = None
    exists: bool = True
    logs: str | None = None


@router.post(
    path="/workers",
    operation_id="start-env-worker",
    response_model=EnvWorkerStartResponse,
    tags=["Env Worker"],
    summary="Run a simulator image as an env worker that dials back to the caller",
)
async def start_worker(request: EnvWorkerStartRequest) -> EnvWorkerStartResponse:
    service = _require_service()
    try:
        handle = service.start(
            commit=request.commit,
            callback_host=request.callback_host,
            callback_port=request.callback_port,
            token=request.token,
            workspace=request.workspace,
            session_key=request.session_key,
        )
    except EnvWorkerJobExists as e:
        # A knowable Kubernetes condition, not a server fault: a Job of this name
        # exists or is still terminating.
        raise HTTPException(409, str(e)) from e
    except EnvWorkerLaunchError as e:
        # A malformed launch is the caller's error and is worth saying precisely
        # — these end up in a pod spec, so vagueness here costs a round trip
        # through Kubernetes to find out what was wrong.
        raise HTTPException(422, str(e)) from e
    return EnvWorkerStartResponse(job_name=handle.job_name, image=handle.image, namespace=handle.namespace)


@router.get(
    path="/workers/{job_name}",
    operation_id="get-env-worker-status",
    response_model=EnvWorkerStatusResponse,
    tags=["Env Worker"],
    summary="Status of an env worker, with logs when it has failed",
)
async def get_worker(job_name: str, include_logs: bool = False) -> EnvWorkerStatusResponse:
    service = _require_service()
    info = service.status(job_name)
    if info is None:
        # Not an error: a worker whose Job has been reaped is a normal end state,
        # and the caller needs to distinguish that from "still starting".
        return EnvWorkerStatusResponse(job_name=job_name, exists=False)
    logs = service.logs(job_name) if include_logs else None
    return EnvWorkerStatusResponse(job_name=job_name, status=str(getattr(info, "status", None)), logs=logs)


@router.delete(
    path="/workers/{job_name}",
    operation_id="stop-env-worker",
    tags=["Env Worker"],
    summary="Delete an env worker Job (idempotent)",
)
async def stop_worker(job_name: str) -> dict[str, str]:
    service = _require_service()
    service.stop(job_name)
    return {"job_name": job_name, "status": "deleted"}


# --------------------------------------------------------------------------- #
# Relay (plan §C/§C1) — viva-api holds the worker socket
#
# The endpoints above serve the IN-CLUSTER shape: the workbench tells us where
# to dial back, because it can be dialled. A laptop cannot — its SSM tunnel is
# laptop-initiated with no inbound path — so for that case viva-api itself is
# the rendezvous: it binds the listener, starts the Job pointing at ITSELF,
# holds the socket, and forwards calls over HTTP.
#
# Both shapes coexist deliberately. The workbench selects between them with an
# env var, so a deployment can be switched either way without a cross-repo
# release window in which env workers are broken.
# --------------------------------------------------------------------------- #


class RelayStartRequest(BaseModel):
    """Start a worker that dials back to *viva-api* rather than to the caller."""

    commit: str = Field(..., description="Simulator commit; the prebuilt image tag to run")
    workspace: str | None = Field(None, description="Workspace path inside the worker container")
    session_key: str | None = Field(None, description="Owning session; makes the Job name unique per session")
    accept_timeout: float = Field(
        300.0, gt=0, le=1800, description="Seconds to wait for the worker to dial back (pod schedule + image pull)"
    )


class RelayStartResponse(BaseModel):
    job_name: str
    image: str
    namespace: str
    connected: bool


class RelayCallRequest(BaseModel):
    method: str = Field(..., description="Worker method name (JSON-RPC)")
    params: dict[str, object] | None = Field(None, description="Method params")
    timeout: float = Field(300.0, gt=0, le=3600, description="Seconds to wait for this call's reply")


class RelayCallResponse(BaseModel):
    result: object | None = None


def _relay_advertise_host() -> str:
    """The address a worker pod should dial to reach THIS viva-api pod.

    Supplied by the Downward API (``status.podIP``), the same mechanism the
    workbench uses for its own dial-back host. Absent, the relay is off — and
    that is reported as 503 rather than guessed, because a wrong host here
    fails as a Job that starts, dials nowhere, and times out three minutes
    later with nothing to point at.
    """
    host = (os.environ.get("ENV_WORKER_RELAY_ADVERTISE_HOST") or "").strip()
    if not host:
        raise HTTPException(
            503,
            "relay is not enabled on this deployment: ENV_WORKER_RELAY_ADVERTISE_HOST "
            "is unset (set it from the Downward API status.podIP)",
        )
    return host


@router.post(
    path="/relay/workers",
    operation_id="start-relayed-env-worker",
    response_model=RelayStartResponse,
    tags=["Env Worker"],
    summary="Start an env worker that dials back to viva-api, and hold its connection",
)
async def start_relayed_worker(request: RelayStartRequest) -> RelayStartResponse:
    service = _require_service()
    host = _relay_advertise_host()
    listener = relay.DialBackListener()
    try:
        handle = service.start(
            commit=request.commit,
            callback_host=host,
            callback_port=listener.port,
            token=listener.token,
            workspace=request.workspace,
            session_key=request.session_key,
        )
    except EnvWorkerJobExists as e:
        listener.close_listener()
        raise HTTPException(409, str(e)) from e
    except EnvWorkerLaunchError as e:
        listener.close_listener()
        raise HTTPException(422, str(e)) from e
    except Exception:
        listener.close_listener()
        raise

    # accept() blocks for pod scheduling + image pull. Run it off the event loop
    # so one starting worker does not stall every other request on this process.
    try:
        sock = await anyio.to_thread.run_sync(functools.partial(listener.accept, request.accept_timeout))
    except relay.DialBackError as e:
        # The Job exists but never reached us. Delete it rather than leaving a
        # pod dialling a port nobody is listening on any more.
        listener.close_listener()
        service.stop(handle.job_name)
        raise HTTPException(504, f"worker did not dial back: {e}") from e

    relay.registry.register(relay.WorkerConnection(job_name=handle.job_name, sock=sock))
    return RelayStartResponse(job_name=handle.job_name, image=handle.image, namespace=handle.namespace, connected=True)


async def _relay_call(job_name: str, method: str, params: dict[str, object] | None, timeout: float) -> object:
    """Forward one call down a held worker socket, mapping its failures to HTTP.

    Shared by the generic ``/call`` below and by every named capability endpoint,
    so those cannot drift on what a lost socket or a refused call means.

    Runs on a worker thread: the call holds a per-worker mutex for its whole
    duration (the worker's FIFO contract), and blocking the event loop on that
    would stall every unrelated request in this process.
    """
    try:
        conn = relay.registry.get(job_name)
    except relay.WorkerUnavailable as e:
        raise HTTPException(404, str(e)) from e
    try:
        return await anyio.to_thread.run_sync(functools.partial(conn.call, method, params, timeout=timeout))
    except relay.WorkerCallError as e:
        # The worker ran and said no. That is the caller's answer, not a viva-api
        # fault -- 502 would blame the wrong party and hide the worker's message.
        raise HTTPException(422, str(e)) from e
    except relay.WorkerUnavailable as e:
        # The socket is gone or desynced; drop it so the next caller is told to
        # start a new worker instead of inheriting a broken connection.
        relay.registry.drop(job_name)
        raise HTTPException(410, str(e)) from e


@router.post(
    path="/relay/workers/{job_name}/call",
    operation_id="call-relayed-env-worker",
    response_model=RelayCallResponse,
    tags=["Env Worker"],
    summary="Forward one JSON-RPC call to a relayed env worker",
)
async def call_relayed_worker(job_name: str, request: RelayCallRequest) -> RelayCallResponse:
    """One request, one reply — the worker protocol is already request/response.

    Deliberately RAW: whatever the worker returned is handed back untouched,
    sentinels included. This is the escape hatch, and a caller reaching for it
    has asked for the protocol rather than for an interpretation of it. The
    named endpoints below are where sentinels become status codes.
    """
    result = await _relay_call(job_name, request.method, request.params, request.timeout)
    return RelayCallResponse(result=result)


@router.delete(
    path="/relay/workers/{job_name}",
    operation_id="stop-relayed-env-worker",
    tags=["Env Worker"],
    summary="Close a relayed worker's connection and delete its Job (idempotent)",
)
async def stop_relayed_worker(request: Request, job_name: str) -> dict[str, object]:
    """Stop a worker. NOT ownership-checked, deliberately — and its tasks are
    settled with attribution instead.

    Stopping a worker is overwhelmingly an AUTOMATIC operation: the workbench's
    pool calls it on LRU eviction, idle reap, dead-worker replacement and process
    exit. The pool has no identity to present, so an ownership check here would
    either break it or have to let unidentified callers through — which is the
    very bypass such a check would exist to close, and would leave the perverse
    rule that anonymous callers may stop workers while identified ones may not.

    So the worker is shared infrastructure with an automatic lifecycle; the task
    is the unit of work that has an owner. What is owed to someone whose task
    dies with a worker is not a veto but an EXPLANATION: their task moves to a
    terminal state naming what happened and, where identity is configured, who
    did it — rather than hanging in `running` until somebody wonders.
    """
    dropped = relay.registry.drop(job_name)
    settled: list[EnvWorkerTask] = []
    if _task_db is not None:
        by = resolve_caller(request)
        settled = await _task_db.fail_unfinished_tasks(
            f"worker {job_name} was stopped" + (f" by {by}" if by else ""),
            job_name=job_name,
        )
    service = _require_service()
    service.stop(job_name)
    return {
        "job_name": job_name,
        "status": "deleted",
        "was_connected": dropped,
        "tasks_settled": len(settled),
    }


# --------------------------------------------------------------------------- #
# Named capability endpoints (plan §E option (e), step 6) — the read-shaped nine
#
# `POST /relay/workers/{job}/call` reaches all 27 worker methods, but untyped:
# the caller supplies a method name as a string, gets an untyped blob back, and
# nothing is discoverable from the OpenAPI document. These give the reads a URL,
# a summary and a place in the schema.
#
# THE SUBSTANCE IS NOT THE URL. Several worker methods answer failure IN BAND --
# `{"__unavailable__": true}`, `{"__error__": "..."}` -- returned with a JSON-RPC
# `result`, so `/call` hands them back as HTTP 200. A caller that does not know
# the sentinel vocabulary reads a successful response containing no data. Every
# endpoint below routes those to a status code instead, which is the whole
# reason a named endpoint beats a string method name.
#
# WHICH METHODS ARE HERE, and which are deliberately not:
#
#   * The 3 LIFECYCLE methods (ping, initialize, shutdown) get no endpoints;
#     start/stop already cover them.
#   * The 3 JOB-CLASS methods (run_study, run_study_analyses,
#     run_investigation_analysis) are task-tier only -- POST /tasks. A synchronous
#     endpoint for them would rebuild the request-held-open bug the task tier
#     exists to remove.
#   * `data_sources_provider` is EXCLUDED, and not for tidiness. It takes a
#     caller-supplied `module:func`, imports it and calls it -- arbitrary code
#     execution in the worker. In the workbench that string comes from the
#     workspace's own `workspace.yaml` (`dashboard.data_sources`) and never from
#     a request; a named endpoint taking it as a parameter would be a materially
#     different and worse thing on an API with no authentication (§E Q3, open).
#     Giving it one needs a worker-side change so the worker reads its own
#     workspace.yaml, and an identity boundary in front. Until then it stays
#     reachable only through the raw `/call`, unadvertised.
#   * The 12 document-shaped methods (they carry a composite, a config, a state
#     document, or a list of node paths) are POST and are step 6b.
#
# That leaves the nine reads below -- eight of them here plus
# `resolve_inner_composite_state`, which turned out to take `hops` as a LIST OF
# NODE PATHS and so is document-shaped after all. The plan's count of "9 GET,
# 13 POST" was written before the params were read; the measured split is 8 and 13.
# --------------------------------------------------------------------------- #

#: In-band failures the worker returns as a successful `result`, and the status
#: each deserves. Ordered most-specific first; `__error__` is the catch-all.
_SENTINEL_STATUS: tuple[tuple[str, int], ...] = (
    # The workspace does not provide this capability at all (e.g. no v2ecoli
    # translator installed). The request was understood; nothing here implements it.
    ("__unavailable__", 501),
    # A reference that names nothing. 404 on the referent, not on the endpoint.
    ("__not_registered__", 404),
    # The worker ran and failed: building, validating, introspecting. Same 422 as
    # a WorkerCallError, because it is the same event -- the worker said no.
    ("__build_error__", 422),
    ("__validate_error__", 422),
    ("__introspect_error__", 422),
    ("__no_validator__", 422),
    ("__error__", 422),
)


def _unwrap(result: object, *, method: str) -> object:
    """Return the worker's payload, or raise the status its sentinel means.

    Only the NAMED endpoints do this. `/call` stays raw on purpose: a caller
    asking for a method by name has asked for the protocol, and rewriting its
    answers would break the workbench, which knows the sentinels and depends on
    reading them.
    """
    if not isinstance(result, dict):
        return result
    for key, status in _SENTINEL_STATUS:
        if key not in result:
            continue
        detail = result[key]
        # `{"__unavailable__": true}` carries no message; the others carry the text.
        message = detail if isinstance(detail, str) and detail else f"{method}: {key.strip('_')}"
        raise HTTPException(status, message)
    return result


#: A per-endpoint in-band failure rule: given the worker's payload, return the
#: status it deserves, or None to let it through as a 200.
InBandRule = Callable[[dict[str, object]], int | None]


def _fails_on_ok_false(payload: dict[str, object]) -> int | None:
    """`{"ok": false, "stage": ..., "error": ...}` -- the worker ran and said no.

    NOT a global rule, and that is the point. `run_process` and
    `process_template` document this shape as a degradation from a failure.
    `viz_preview` uses the same key to mean something else entirely: its
    docstring says "every render outcome (including a raise) is a 200 body", so
    applying this to it would contradict a contract the workbench relies on.
    """
    return 422 if payload.get("ok") is False else None


def _fails_on_nested_result_status(payload: dict[str, object]) -> int | None:
    """`analysis_viewers` launch: `{"result": {"error": ..., "status": 404}}`.

    A FOURTH way of saying no, and the only one that carries the status it wants
    -- `_av_resolve_launch` returns 404 for an unknown uid, 400 for a viewer that
    is not launchable, and 500 when the contributor's own callable raised. Those
    are already the right answers; the bug was returning all three as 200.

    A launch resolves WHERE to go (the UI fetches this, then opens the returned
    `{"url": ...}`), so a caller that cannot tell "no such viewer" from "here is
    your link" will happily navigate to nothing.
    """
    result = payload.get("result")
    if not isinstance(result, dict) or "error" not in result:
        return None
    status = result.get("status")
    # Trust it only if it is a plausible HTTP error code; a contributor's dict is
    # not a trusted source of status codes.
    return status if isinstance(status, int) and 400 <= status <= 599 else 422


def _fails_on_not_registered_status(payload: dict[str, object]) -> int | None:
    """`viz_preview`'s own idiom: `{"status": "not_registered"}`, which its
    docstring says "the workbench maps to 404". Same mapping here."""
    return 404 if payload.get("status") == "not_registered" else None


async def _named_call(
    job_name: str,
    method: str,
    params: dict[str, object] | None = None,
    *,
    rules: tuple[InBandRule, ...] = (),
) -> object:
    """One named capability: forward, then turn an in-band failure into a status.

    `rules` are endpoint-specific and run AFTER the shared sentinel table, because
    the worker has more than one way of saying no and they do not agree with each
    other -- see `_fails_on_ok_false`.
    """
    result = _unwrap(await _relay_call(job_name, method, params, _NAMED_READ_TIMEOUT), method=method)
    if isinstance(result, dict):
        for rule in rules:
            status = rule(result)
            if status is not None:
                # Keep the whole payload: `stage` and `error` together are what
                # make a probe failure actionable, and a flattened string loses
                # the half that says WHERE it stopped.
                raise HTTPException(status, {"method": method, **result})
    return result


async def _named_read(job_name: str, method: str, params: dict[str, object] | None = None) -> object:
    """One named read. Kept as its own name because a GET has no endpoint-specific
    in-band rules -- the shared sentinel table is the whole story for a read."""
    return await _named_call(job_name, method, params)


#: Reads are interactive by definition -- the worker answers a question about the
#: environment rather than running science. Anything slower than this is either a
#: cold `build_core()` (which is why it is not 30s) or something that should have
#: been a task.
_NAMED_READ_TIMEOUT = 300.0

_READS = "Env Worker Reads"


@router.get(
    path="/relay/workers/{job_name}/generators",
    operation_id="list-env-worker-generators",
    tags=[_READS],
    summary="Composite generators registered in the worker's workspace",
)
async def read_generators(job_name: str) -> object:
    """The health check that actually proves the arc, and the reason this one
    earns an endpoint despite having no workbench caller.

    `ping` and `initialize` pass even when the workspace is wrong. This does not:
    a worker that could not import the workspace falls back to a GLOBAL scan of
    everything installed in the image, and the give-away is the count of
    generators from packages the workspace does not own.
    """
    return await _named_read(job_name, "list_generators")


@router.get(
    path="/relay/workers/{job_name}/registry",
    operation_id="read-env-worker-registry",
    tags=[_READS],
    summary="Process/step/type names in the worker's core",
)
async def read_registry_catalog(job_name: str) -> object:
    return await _named_read(job_name, "registry_catalog")


@router.get(
    path="/relay/workers/{job_name}/composites",
    operation_id="discover-env-worker-composites",
    tags=[_READS],
    summary="Composites discoverable in the worker's workspace",
)
async def read_composites(job_name: str) -> object:
    return await _named_read(job_name, "discover_composites")


@router.get(
    path="/relay/workers/{job_name}/composites/full",
    operation_id="read-env-worker-composites-full",
    tags=[_READS],
    summary="Discovered composites with their resolved detail",
)
async def read_composites_full(job_name: str) -> object:
    """The expensive sibling of `/composites`: it resolves each one. Separate
    URLs rather than a `?full=` flag, because the cost difference is large enough
    that a caller should have to ask for it in the path."""
    return await _named_read(job_name, "composites_full")


@router.get(
    path="/relay/workers/{job_name}/visualizations",
    operation_id="list-env-worker-visualizations",
    tags=[_READS],
    summary="Visualization classes available in the worker's workspace",
)
async def read_visualizations(job_name: str) -> object:
    return await _named_read(job_name, "viz_classes")


@router.get(
    path="/relay/workers/{job_name}/visualizations/inputs",
    operation_id="read-env-worker-visualization-inputs",
    tags=[_READS],
    summary="Declared input ports of each visualization class",
)
async def read_visualization_inputs(job_name: str) -> object:
    return await _named_read(job_name, "viz_class_inputs")


@router.get(
    path="/relay/workers/{job_name}/core-snapshot",
    operation_id="read-env-worker-core-snapshot",
    tags=[_READS],
    summary="Registry snapshot plus the workspace document, for a report render",
)
async def read_core_snapshot(job_name: str, package_path: str = Query(..., min_length=1)) -> object:
    """`package_path` is REQUIRED rather than defaulted. The worker imports
    `<package_path>.core` and `<package_path>.document`; a default here would
    guess at the caller's workspace and import whatever that guess named."""
    return await _named_read(job_name, "report_core_snapshot", {"package_path": package_path})


@router.get(
    path="/relay/workers/{job_name}/reexports",
    operation_id="read-env-worker-reexport-map",
    tags=[_READS],
    summary="Which allow-listed package re-exports each class",
)
async def read_reexport_map(job_name: str, include: list[str] = Query(default=[])) -> object:
    """`include` is the caller's allow-list of packages to scan, repeated:
    `?include=a&include=b`. The worker imports each one, so an empty list means
    "scan nothing" -- which is why it is not defaulted to something broader."""
    return await _named_read(job_name, "reexport_map", {"include": sorted(set(include))})


# --------------------------------------------------------------------------- #
# Named capability endpoints, part two (step 6b) — the document-shaped
#
# These carry a composite, a config, a state document or a list of node paths, so
# they are POST. Their bodies follow this codebase's passthrough-config
# convention: declare only the fields VIVA-API is authoritative about -- the ones
# whose absence it can reject better than the worker can -- and let everything
# else through under `extra="allow"`. The meaning of a config or a state document
# belongs to the workspace, and re-declaring it here would create a second,
# staler copy of a schema we do not own.
#
# What the boundary IS good for: refusing a request the worker could only answer
# with a confusing sentinel. `observables` accepts a `ref` OR an inline
# `{state, schema}`; sending neither returns `__not_registered__`, which reads as
# "your ref is wrong" to someone who sent no ref at all. That is a 422 here.
#
# EXCLUDED, for the same reason as `data_sources_provider` in 6a:
# `validate_generated_visualization` interpolates caller-supplied `pkg` and
# `module` into a module name, then imports it -- and RELOADS it if already
# imported, re-running module-level code. It is a write-path verify whose only
# legitimate caller is the workbench, immediately after writing the file it
# checks. There is no client-side use for it, so it does not get a documented
# endpoint on an API with no authentication. It stays reachable via `/call`.
# --------------------------------------------------------------------------- #


class _WorkerBody(BaseModel):
    """Passthrough base: declared fields are validated, unknown ones forwarded."""

    model_config = {"extra": "allow"}

    def to_params(self) -> dict[str, object]:
        """Body -> worker params, dropping keys the caller did not send.

        `exclude_none` matters: several worker handlers branch on PRESENCE
        (`if ref is not None`), so forwarding an explicit null would take a
        different path than omitting the field.

        `by_alias` matters more, and less visibly. `CompositeSelector.schema_`
        carries `alias="schema"` because a bare `schema` shadows a BaseModel
        attribute; without `by_alias` the dump emits `schema_`, the worker never
        sees the `schema` it looks for, and `observables` silently takes its
        `ref` branch on a request that supplied an inline state. Nothing raises
        -- the answer is just about a different composite.
        """
        return self.model_dump(exclude_none=True, by_alias=True)


class CompositeRef(_WorkerBody):
    """A composite named by a registered generator, optionally with overrides."""

    ref: str = Field(..., min_length=1, description="Registered @composite_generator name")
    overrides: dict[str, object] | None = Field(None, description="Generator parameter overrides")


class InnerCompositeRef(_WorkerBody):
    """`hops` is a LIST OF NODE PATHS, each itself a list of key segments -- which
    is why this is a POST and not the GET the plan first assumed."""

    ref: str = Field(..., min_length=1)
    hops: list[list[str]] = Field(..., min_length=1, description="One node path per drill level")


class ConfigDocument(_WorkerBody):
    config: dict[str, object] = Field(..., description="vEcoli-style config to translate")


class StateDocument(_WorkerBody):
    document: dict[str, object] = Field(..., description="An already-resolved composite state")
    ref: str | None = Field(None, description="Generator whose core_extensions resolve bare addresses")


class CompositeSelector(_WorkerBody):
    """A composite given EITHER by `ref` OR inline as `{state, schema}`.

    Both forms are real and the worker accepts either; sending neither is the
    mistake worth catching here, because the worker answers it with
    `__not_registered__` -- which reads as "your ref is wrong" to a caller who
    sent no ref at all.
    """

    ref: str | None = None
    state: dict[str, object] | None = None
    schema_: dict[str, object] | None = Field(None, alias="schema")

    def model_post_init(self, __context: object) -> None:
        if self.ref is None and self.state is None:
            raise ValueError("provide either 'ref' (a registered generator) or an inline 'state' (with 'schema')")


class ReadoutCheck(CompositeSelector):
    spec: dict[str, object] = Field(..., description="The study spec whose readouts are checked")


class ProcessAddress(_WorkerBody):
    address: str = Field(..., min_length=1, description="Registry address of a Process or Step")
    config: dict[str, object] | None = None


class ProcessRun(ProcessAddress):
    """One `update()` -- a probe, not a job. `env_worker._run_process` is
    deliberately NOT job-class: it builds one class, fills its ports and runs a
    single step, which is the Composite Explorer's "try this process" button."""

    inputs: dict[str, object] | None = None
    interval: float | None = None


class VizDoc(_WorkerBody):
    viz_doc: dict[str, object] = Field(..., description="A visualization composite document")


class VizPreview(_WorkerBody):
    address: str = Field(..., min_length=1, description="Visualization class address")
    config: dict[str, object] | None = None
    source: str | None = Field(None, description="demo | streaming | investigation")
    note_prefix: str | None = None
    investigation_inputs_store: dict[str, object] | None = None


class ViewerLaunch(_WorkerBody):
    """`analysis_viewers` carries two operations behind an `action` flag. They are
    split into two routes here: listing is a read, launching invokes a
    contributor's callable. One endpoint with a mode string would hide that."""

    uid: str = Field(..., min_length=1, description="Viewer uid from the listing")
    study: str | None = None
    run: str | None = None
    ctx: dict[str, object] | None = None


_DOCS = "Env Worker Documents"


@router.post(
    path="/relay/workers/{job_name}/composite-state",
    operation_id="resolve-env-worker-composite-state",
    tags=[_DOCS],
    summary="Build a registered generator's composite state",
)
async def resolve_composite_state(job_name: str, body: CompositeRef) -> object:
    return await _named_call(job_name, "resolve_composite_state", body.to_params())


@router.post(
    path="/relay/workers/{job_name}/composite-state/inner",
    operation_id="resolve-env-worker-inner-composite-state",
    tags=[_DOCS],
    summary="Drill into a Composite Process and return the inner composite's state",
)
async def resolve_inner_composite_state(job_name: str, body: InnerCompositeRef) -> object:
    """Dispatchable in the worker but absent from its `_CAPABILITIES` list, so
    `initialize`'s handshake does not advertise it. Given a URL here, it is at
    least discoverable from the OpenAPI document."""
    return await _named_call(job_name, "resolve_inner_composite_state", body.to_params())


@router.post(
    path="/relay/workers/{job_name}/composite-state/from-config",
    operation_id="convert-env-worker-config-to-composite",
    tags=[_DOCS],
    summary="Translate a vEcoli-style config into a composite document",
)
async def config_to_composite(job_name: str, body: ConfigDocument) -> object:
    """501 where the workspace ships no translator -- the worker's
    `__unavailable__`, which is a property of the workspace and not of the request."""
    return await _named_call(job_name, "config_to_composite", body.to_params())


@router.post(
    path="/relay/workers/{job_name}/composite-state/docs",
    operation_id="attach-env-worker-process-docs",
    tags=[_DOCS],
    summary="Attach per-process docstrings to a resolved composite state",
)
async def attach_process_docs(job_name: str, body: StateDocument) -> object:
    return await _named_call(job_name, "attach_process_docs", body.to_params())


@router.post(
    path="/relay/workers/{job_name}/observables",
    operation_id="read-env-worker-observables",
    tags=[_DOCS],
    summary="Observable leaves and catalogs of a composite",
)
async def read_observables(job_name: str, body: CompositeSelector) -> object:
    return await _named_call(job_name, "observables", body.to_params())


@router.post(
    path="/relay/workers/{job_name}/readout-check",
    operation_id="check-env-worker-study-readouts",
    tags=[_DOCS],
    summary="Validate a study's readouts against its composite's real structure",
)
async def check_study_readouts(job_name: str, body: ReadoutCheck) -> object:
    """The never-fabricate guard: it is what stops a study declaring a readout
    the composite cannot produce."""
    return await _named_call(job_name, "study_readout_check", body.to_params())


@router.post(
    path="/relay/workers/{job_name}/process-template",
    operation_id="read-env-worker-process-template",
    tags=[_DOCS],
    summary="Resolved default config and input-port values for a process or step",
)
async def read_process_template(job_name: str, body: ProcessAddress) -> object:
    return await _named_call(job_name, "process_template", body.to_params(), rules=(_fails_on_ok_false,))


@router.post(
    path="/relay/workers/{job_name}/process-run",
    operation_id="run-env-worker-process-probe",
    tags=[_DOCS],
    summary="Run a single update() of one process — a probe, not a job",
)
async def run_process_probe(job_name: str, body: ProcessRun) -> object:
    """ONE update. Named `probe` in the summary on purpose: `run_process` reads
    like a job-class method and is not one, and that misreading has already been
    made once in this codebase (see `env_worker_routing.JOB_CLASS_METHODS`)."""
    return await _named_call(job_name, "run_process", body.to_params(), rules=(_fails_on_ok_false,))


@router.post(
    path="/relay/workers/{job_name}/visualizations/render",
    operation_id="render-env-worker-visualization-doc",
    tags=[_DOCS],
    summary="Render one visualization composite document to HTML",
)
async def render_visualization_doc(job_name: str, body: VizDoc) -> object:
    return await _named_call(job_name, "render_viz_doc", body.to_params())


@router.post(
    path="/relay/workers/{job_name}/visualizations/preview",
    operation_id="preview-env-worker-visualization",
    tags=[_DOCS],
    summary="Render a visualization class to preview HTML",
)
async def preview_visualization(job_name: str, body: VizPreview) -> object:
    """`ok: false` here is NOT an error, deliberately. The worker's contract is
    that every render outcome including a raise comes back as a 200 body with
    notes; only an unregistered address is non-200."""
    return await _named_call(job_name, "viz_preview", body.to_params(), rules=(_fails_on_not_registered_status,))


@router.get(
    path="/relay/workers/{job_name}/analysis-viewers",
    operation_id="list-env-worker-analysis-viewers",
    tags=[_READS],
    summary="Viewer descriptors contributed by the workspace's packages",
)
async def list_analysis_viewers(job_name: str) -> object:
    """The listing half of `analysis_viewers`. A GET, because it is a read --
    the `action` flag that used to hide this behind the same name as `launch` is
    exactly the kind of thing named endpoints exist to separate."""
    return await _named_read(job_name, "analysis_viewers", {"action": "list"})


@router.post(
    path="/relay/workers/{job_name}/analysis-viewers/launch",
    operation_id="launch-env-worker-analysis-viewer",
    tags=[_DOCS],
    summary="Resolve and invoke one contributed viewer's launch",
)
async def launch_analysis_viewer(job_name: str, body: ViewerLaunch) -> object:
    """POST rather than GET: this invokes a contributor's callable, which may do
    anything the workspace's code can do. Same operation as `?action=launch`,
    with `uid` required instead of silently defaulting to the listing."""
    # The literal goes LAST. `action` is passthrough-eligible on the body model,
    # so with the spread second a caller could send `action: "list"` and turn
    # this route into the other one -- which the test for this line found.
    return await _named_call(
        job_name,
        "analysis_viewers",
        {**body.to_params(), "action": "launch"},
        rules=(_fails_on_nested_result_status,),
    )


# --------------------------------------------------------------------------- #
# Tasks (plan §E option (e)) — the middle tier
#
# The relay's /call above is synchronous and stays that way: an interactive
# method answers in seconds and a round trip is the cheapest thing that works.
# But `run_study` runs a study's baseline and every variant to completion, and
# no gateway will hold a request that long — the workbench's own attempt to do
# so is what produced the double-run bug. So long work is submitted, recorded,
# and polled.
# --------------------------------------------------------------------------- #


def _require_task_db() -> "EnvWorkerTaskDatabaseService":
    if _task_db is None:
        raise HTTPException(503, "env-worker tasks are not configured on this deployment")
    return _task_db


def _require_runner() -> relay.TaskRunner:
    if relay.runner is None:
        raise HTTPException(503, "env-worker task runner is not configured on this deployment")
    return relay.runner


class TaskSubmitRequest(BaseModel):
    job_name: str = Field(..., description="Relayed worker Job to run this on")
    method: str = Field(..., description="Worker method (JSON-RPC)")
    params: dict[str, object] | None = Field(None, description="Method params")


class TaskResponse(BaseModel):
    task_id: int
    job_name: str
    method: str
    status: str
    result: object | None = None
    error_message: str | None = None
    created_by: str | None = None
    created_at: str | None = None
    started_at: str | None = None
    ended_at: str | None = None


class TaskStatusResponse(BaseModel):
    """A task WITHOUT its result — the shape the batch endpoint returns.

    Measured on dev before this existed: five tasks came back as **1.19 MB**,
    of which 1.185 MB was result payloads. The endpoint exists so a campaign can
    be polled without N round trips; inlining every result defeats exactly that,
    and the cost grows with both the number of tasks and the size of what they
    returned. Polling twenty tasks every few seconds would have moved megabytes
    per cycle over an SSM tunnel.

    So a *status* endpoint returns status. Fetch the singular
    ``GET /tasks/{id}`` for the payload, which is one request at the one moment
    a caller actually wants it.
    """

    task_id: int
    job_name: str
    method: str
    status: str
    error_message: str | None = None
    created_by: str | None = None
    created_at: str | None = None
    started_at: str | None = None
    ended_at: str | None = None
    #: Whether a result is waiting, without shipping it. Lets a poller know it is
    #: worth one GET rather than guessing from `status`.
    has_result: bool = False


def _to_status(task: EnvWorkerTask) -> TaskStatusResponse:
    return TaskStatusResponse(
        task_id=task.database_id,
        job_name=task.job_name,
        method=task.method,
        status=task.status.value,
        error_message=task.error_message,
        created_by=task.created_by,
        created_at=task.created_at,
        started_at=task.started_at,
        ended_at=task.ended_at,
        has_result=task.result is not None,
    )


def _to_response(task: EnvWorkerTask) -> TaskResponse:
    return TaskResponse(
        task_id=task.database_id,
        job_name=task.job_name,
        method=task.method,
        status=task.status.value,
        result=task.result,
        error_message=task.error_message,
        created_by=task.created_by,
        created_at=task.created_at,
        started_at=task.started_at,
        ended_at=task.ended_at,
    )


#: Job-class methods that name a study and can therefore be scale-checked before
#: they are accepted. `run_investigation_analysis` is job-class too but takes an
#: investigation rather than a study, so there is nothing to multiply.
_SCALE_CHECKED_METHODS = ("run_study", "run_study_analyses")


async def _refuse_if_over_tier_budget(body: TaskSubmitRequest) -> None:
    """Ask the worker whether this study belongs in this tier, before accepting it.

    THE TIER DECIDES WHERE WORK GOES — that is what makes it a tier rather than a
    queue. The same declared-scale check already runs inside the worker
    (`launch_into_study`, workstream 8 step 2b), but by then the answer arrives
    as an entry in a harvest's `errors[]`, which under this tier's own semantics
    reads as *the science failed*. It did not: the caller sent 10,000 simulations
    somewhere sized for a handful. Refusing at submit says so.

    **This covers the SCALE axis only.** A study can declare one simulation and
    still be unrunnable here — `basal` declares 1 and needs a ParCa cache the
    worker has no way to stage. That is a different axis (environment
    capability), it is deliberately not inferred (`env-worker-routing.md` §4: a
    `@composite_generator` is arbitrary Python), and it is handled by failing
    fast with a legible error instead. Do not extend this function to guess at it.

    Never blocks on its own failure: an old worker without `study_precheck`, a
    dropped socket, or a malformed answer all fall through to accepting the task.
    A precheck that could refuse work by breaking would be worse than no precheck.
    """
    if body.method not in _SCALE_CHECKED_METHODS:
        return
    params = body.params or {}
    if not params.get("study_slug"):
        return
    try:
        conn = relay.registry.get(body.job_name)
        verdict = await anyio.to_thread.run_sync(
            functools.partial(conn.call, "study_precheck", dict(params), timeout=30.0)
        )
    except Exception:
        logger.info("study_precheck unavailable for %s; accepting the task unchecked", body.job_name)
        return
    if not isinstance(verdict, dict) or not verdict.get("exceeds"):
        return
    raise HTTPException(
        422,
        {
            "error": "declared run scale exceeds what an env worker may take",
            "declared_simulations": verdict.get("declared"),
            "budget": verdict.get("budget"),
            "hint": verdict.get("hint") or "dispatch this to Batch instead",
            "method": body.method,
        },
    )


@router.post(
    path="/tasks",
    operation_id="submit-env-worker-task",
    response_model=TaskResponse,
    status_code=202,
    tags=["Env Worker"],
    summary="Submit a long-running env-worker call; poll for its result",
)
async def submit_task(request: Request, body: TaskSubmitRequest) -> TaskResponse:
    """202 with a task id. The row is written BEFORE this returns.

    That ordering is compose's contract and it matters more here: the client is
    told to poll and holds nothing else, so a status read that 404s because the
    row had not been written yet would be indistinguishable from a lost task.
    """
    db = _require_task_db()
    runner_ = _require_runner()
    # Refuse early on a worker nobody is holding, rather than accepting a task
    # that can only fail: 404 here is actionable, a queued-then-failed task is a
    # round trip and a confusing record.
    try:
        relay.registry.get(body.job_name)
    except relay.WorkerUnavailable as e:
        raise HTTPException(404, str(e)) from e
    await _refuse_if_over_tier_budget(body)
    task = await db.insert_task(
        job_name=body.job_name,
        method=body.method,
        params=body.params,
        correlation_id=f"env-worker-task-{secrets.token_hex(8)}",
        created_by=resolve_caller(request),
    )
    await runner_.submit(body.job_name, task.database_id)
    return _to_response(task)


@router.get(
    path="/tasks/{task_id}",
    operation_id="get-env-worker-task",
    response_model=TaskResponse,
    tags=["Env Worker"],
    summary="Status and result of one env-worker task",
)
async def get_task(task_id: int) -> TaskResponse:
    task = await _require_task_db().get_task(task_id)
    if task is None:
        raise HTTPException(404, f"no such task: {task_id}")
    return _to_response(task)


@router.get(
    path="/tasks/status/batch",
    operation_id="get-env-worker-tasks-batch",
    response_model=list[TaskStatusResponse],
    tags=["Env Worker"],
    summary="Status for many env-worker tasks in one call (no result payloads)",
)
async def get_tasks_batch(ids: list[int] = Query()) -> list[TaskStatusResponse]:
    """Status only — results are deliberately omitted. See TaskStatusResponse.

    Mirrors compose's /simulations/status/batch, which returns rows carrying no
    large payload. A campaign is many tasks, and polling them one at a time is
    what this endpoint exists to avoid; shipping every result inline reintroduced
    the cost in a different dimension.
    """
    return [_to_status(t) for t in await _require_task_db().get_tasks(ids)]


@router.delete(
    path="/tasks/{task_id}",
    operation_id="cancel-env-worker-task",
    response_model=TaskResponse,
    tags=["Env Worker"],
    summary="Cancel a task you started",
)
async def cancel_task(request: Request, task_id: int) -> TaskResponse:
    """The one authorization rule in this API: you cannot cancel someone else's work.

    It exists to prevent an ACCIDENT — killing a colleague's six-hour study —
    not an adversary, who can set the identity header to anything. See
    viva_api/api/auth.py.

    A task nobody claimed (created_by NULL, which is every task on a deployment
    with no identity proxy) is cancellable by anyone: there is no owner to
    protect, and refusing would make the endpoint useless exactly where identity
    is unavailable.

    Anonymity is refused only HERE, and only because the alternative is a
    decorative rule: if an unidentified caller could cancel anything, omitting
    the header would bypass ownership entirely.
    """
    db = _require_task_db()
    task = await db.get_task(task_id)
    if task is None:
        raise HTTPException(404, f"no such task: {task_id}")
    if task.created_by is not None:
        caller = require_caller(request)
        if caller != task.created_by:
            raise HTTPException(
                403,
                f"task {task_id} was started by {task.created_by}; "
                f"you are {caller}. Cancel it from the client that started it.",
            )
    if task.status in (
        ComposeJobStatus.COMPLETED,
        ComposeJobStatus.FAILED,
        ComposeJobStatus.CANCELLED,
        ComposeJobStatus.TIMEOUT,
    ):
        return _to_response(task)  # already finished; cancelling is a no-op, not an error
    await db.finish_task(
        task_id,
        ComposeJobStatus.CANCELLED,
        error_message=f"cancelled by {resolve_caller(request) or 'an unidentified caller'}",
    )
    settled = await db.get_task(task_id)
    return _to_response(settled or task)
