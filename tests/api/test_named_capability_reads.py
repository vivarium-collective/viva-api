"""The read-shaped named endpoints (plan §E option (e), step 6).

`POST /relay/workers/{job}/call` already reaches every worker method. What these
add is not reachability but HONESTY: several worker methods report failure IN
BAND -- `{"__unavailable__": true}`, `{"__error__": "..."}` -- as a JSON-RPC
`result`, so `/call` returns them as HTTP 200. A caller who does not know the
sentinel vocabulary reads a successful response that contains no data.

So most of what is pinned here is the sentinel-to-status mapping, plus the two
deliberate exclusions, which are the kind of decision that gets quietly undone.
"""

from __future__ import annotations

from typing import Any

import pytest
from fastapi import FastAPI, HTTPException
from httpx import ASGITransport, AsyncClient

from viva_api.api.routers import env_worker as ew


@pytest.fixture
def app(monkeypatch: pytest.MonkeyPatch) -> tuple[FastAPI, list[dict[str, Any]]]:
    """The router with the worker socket replaced by a recorder.

    The relay itself has its own tests against a real socket and the literal wire
    format; what matters here is which method each URL asks for, and what the
    endpoint does with the answer.
    """
    calls: list[dict[str, Any]] = []
    reply: dict[str, Any] = {"value": {"ok": True}}

    async def _fake_relay_call(job_name: str, method: str, params: Any, timeout: float) -> Any:
        calls.append({"job": job_name, "method": method, "params": params, "timeout": timeout})
        if isinstance(reply["value"], Exception):
            raise reply["value"]
        return reply["value"]

    monkeypatch.setattr(ew, "_relay_call", _fake_relay_call)
    application = FastAPI()
    application.include_router(ew.router, prefix="/env-worker/v1")
    application.state.reply = reply  # tests set application.state.reply["value"]
    return application, calls


async def _get(app: FastAPI, path: str) -> Any:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        return await c.get(f"/env-worker/v1/relay/workers/job-1{path}")


# --- each URL asks for the method it claims ---------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("path", "method"),
    [
        ("/generators", "list_generators"),
        ("/registry", "registry_catalog"),
        ("/composites", "discover_composites"),
        ("/composites/full", "composites_full"),
        ("/visualizations", "viz_classes"),
        ("/visualizations/inputs", "viz_class_inputs"),
    ],
)
async def test_a_no_argument_read_maps_to_its_worker_method(
    app: tuple[FastAPI, list[dict[str, Any]]], path: str, method: str
) -> None:
    application, calls = app
    r = await _get(application, path)
    assert r.status_code == 200
    assert r.json() == {"ok": True}
    assert [c["method"] for c in calls] == [method]
    assert calls[0]["params"] is None


@pytest.mark.asyncio
async def test_composites_and_composites_full_are_different_urls(
    app: tuple[FastAPI, list[dict[str, Any]]],
) -> None:
    """`/composites/full` resolves every composite and is far more expensive. A
    `?full=true` flag would let a caller trip into that cost by editing a query
    string; the path makes them ask."""
    application, calls = app
    await _get(application, "/composites")
    await _get(application, "/composites/full")
    assert [c["method"] for c in calls] == ["discover_composites", "composites_full"]


# --- the two parameterised reads --------------------------------------------


@pytest.mark.asyncio
async def test_core_snapshot_requires_a_package_path(
    app: tuple[FastAPI, list[dict[str, Any]]],
) -> None:
    """The worker imports `<package_path>.core` and `.document`. Defaulting this
    would guess a workspace and then import whatever the guess named, so it is
    required and the request fails validation without it."""
    application, calls = app
    r = await _get(application, "/core-snapshot")
    assert r.status_code == 422
    assert not calls


@pytest.mark.asyncio
async def test_core_snapshot_passes_the_package_path_through(
    app: tuple[FastAPI, list[dict[str, Any]]],
) -> None:
    application, calls = app
    r = await _get(application, "/core-snapshot?package_path=v2ecoli")
    assert r.status_code == 200
    assert calls[0] == {
        "job": "job-1",
        "method": "report_core_snapshot",
        "params": {"package_path": "v2ecoli"},
        "timeout": ew._NAMED_READ_TIMEOUT,
    }


@pytest.mark.asyncio
async def test_reexports_takes_a_repeated_include(
    app: tuple[FastAPI, list[dict[str, Any]]],
) -> None:
    """Sorted and de-duplicated: the worker IMPORTS each name, so asking twice
    would import twice, and an unstable order would make two identical requests
    look different to anything caching or logging them."""
    application, calls = app
    r = await _get(application, "/reexports?include=b&include=a&include=b")
    assert r.status_code == 200
    assert calls[0]["params"] == {"include": ["a", "b"]}


@pytest.mark.asyncio
async def test_reexports_with_no_include_scans_nothing(
    app: tuple[FastAPI, list[dict[str, Any]]],
) -> None:
    """An empty allow-list must mean "scan nothing", not "scan everything" — the
    permissive reading would have the worker import every installed package."""
    application, calls = app
    await _get(application, "/reexports")
    assert calls[0]["params"] == {"include": []}


# --- the point: in-band failure becomes a status ----------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("sentinel", "status"),
    [
        ({"__unavailable__": True}, 501),
        ({"__not_registered__": "no such ref"}, 404),
        ({"__build_error__": "boom"}, 422),
        ({"__validate_error__": "bad"}, 422),
        ({"__introspect_error__": "nope"}, 422),
        ({"__no_validator__": "none"}, 422),
        ({"__error__": "it broke"}, 422),
    ],
)
async def test_an_in_band_sentinel_becomes_a_status_not_a_200(
    app: tuple[FastAPI, list[dict[str, Any]]], sentinel: dict[str, Any], status: int
) -> None:
    """`/call` returns these as 200 with a body the caller must know to inspect.
    That is the failure mode a named endpoint exists to remove."""
    application, _ = app
    application.state.reply["value"] = sentinel
    r = await _get(application, "/generators")
    assert r.status_code == status


@pytest.mark.asyncio
async def test_the_sentinel_message_reaches_the_caller(
    app: tuple[FastAPI, list[dict[str, Any]]],
) -> None:
    """Turning a sentinel into a bare status would trade one silence for another."""
    application, _ = app
    application.state.reply["value"] = {"__error__": "no translator in this workspace"}
    r = await _get(application, "/generators")
    assert r.json()["detail"] == "no translator in this workspace"


@pytest.mark.asyncio
async def test_a_messageless_sentinel_still_names_the_method_and_the_kind(
    app: tuple[FastAPI, list[dict[str, Any]]],
) -> None:
    """`{"__unavailable__": true}` carries no text, and "501" alone would leave
    the reader guessing which of several calls it came from."""
    application, _ = app
    application.state.reply["value"] = {"__unavailable__": True}
    r = await _get(application, "/registry")
    detail = r.json()["detail"]
    assert "registry_catalog" in detail and "unavailable" in detail


@pytest.mark.asyncio
async def test_an_ordinary_payload_is_passed_through_untouched(
    app: tuple[FastAPI, list[dict[str, Any]]],
) -> None:
    """Only the sentinel keys are special. A payload that merely CONTAINS a
    dunder-looking key of its own must not be mistaken for one."""
    application, _ = app
    application.state.reply["value"] = {"generators": ["a"], "__name__": "not a sentinel"}
    r = await _get(application, "/generators")
    assert r.status_code == 200
    assert r.json() == {"generators": ["a"], "__name__": "not a sentinel"}


@pytest.mark.asyncio
async def test_a_non_dict_payload_is_returned_as_is(
    app: tuple[FastAPI, list[dict[str, Any]]],
) -> None:
    application, _ = app
    application.state.reply["value"] = [1, 2, 3]
    r = await _get(application, "/generators")
    assert r.status_code == 200
    assert r.json() == [1, 2, 3]


# --- transport failures keep the mapping the generic /call already had -------


@pytest.mark.asyncio
async def test_a_lost_worker_is_not_flattened_into_a_sentinel(
    app: tuple[FastAPI, list[dict[str, Any]]],
) -> None:
    """`_relay_call` already maps a dead socket to 410 and an unknown job to 404;
    a named endpoint must not swallow that into its own vocabulary."""
    application, _ = app
    application.state.reply["value"] = HTTPException(410, "worker closed the connection")
    r = await _get(application, "/generators")
    assert r.status_code == 410


# --- the exclusions, which are decisions and not omissions ------------------


def _routes() -> set[str]:
    application = FastAPI()
    application.include_router(ew.router, prefix="/env-worker/v1")
    return {getattr(r, "path", "") for r in application.routes}


def test_data_sources_provider_has_no_named_endpoint() -> None:
    """EXCLUDED ON PURPOSE. It takes a caller-supplied `module:func`, imports it
    and calls it. In the workbench that string comes from the workspace's own
    workspace.yaml and never from a request; a named endpoint accepting it as a
    parameter is arbitrary code execution on an API with no authentication.

    If this test starts failing, the question to answer first is not "how do we
    fix the test" but "does the worker now read its own workspace.yaml, and is
    there an identity boundary in front of this?" (§E Q3, open)."""
    assert not any("data-source" in p or "provider" in p for p in _routes())


def test_job_class_methods_get_no_synchronous_endpoint() -> None:
    """run_study and friends are task-tier only. A synchronous endpoint for them
    would rebuild the request-held-open bug the task tier exists to remove."""
    paths = _routes()
    for banned in ("run-study", "run_study", "investigation-analysis"):
        assert not any(banned in p for p in paths), banned


def test_every_named_read_is_a_GET() -> None:
    """These answer questions; none of them changes anything. A POST here would
    make them uncacheable and unlinkable for no gain.

    Keyed on the TAG, not the path. The first version asserted that every route
    under `/relay/workers/{job_name}/` except `call` was a GET, which was true
    only until step 6b added the document-shaped POSTs beside them -- a test that
    fails because correct new work arrived is a test pinning the wrong thing.
    What is actually invariant is that anything presented to callers as a READ
    is safe to GET.
    """
    application = FastAPI()
    application.include_router(ew.router, prefix="/env-worker/v1")
    reads = [r for r in application.routes if ew._READS in getattr(r, "tags", [])]
    assert len(reads) >= 8, "the read endpoints should still be here"
    for route in reads:
        assert route.methods == {"GET"}, route.path


# --- the generic /call, which the refactor broke and nothing noticed ---------
#
# Factoring `_relay_call` out of `call_relayed_worker` inserted the new helper
# BETWEEN the `@router.post(...)` decorator and the endpoint it belonged to. The
# decorator then described the helper, so FastAPI generated `/call` from
# `_relay_call(job_name, method, params, timeout)`: `method` and `timeout`
# became QUERY parameters and `params` became the whole body. Every test above
# passed, `make check` passed, mypy passed, and the generated client quietly
# renamed `RelayCallRequest` to `call_relayed_env_worker_body_type_0` -- which is
# the only place it showed.
#
# So: pin the endpoint's shape, not just its behaviour.


def _call_route() -> Any:
    application = FastAPI()
    application.include_router(ew.router, prefix="/env-worker/v1")
    routes = [r for r in application.routes if getattr(r, "path", "").endswith("/call")]
    assert len(routes) == 1, routes
    return routes[0]


def test_call_is_a_post_bound_to_the_endpoint_not_the_helper() -> None:
    """`_relay_call` is a helper. If it is ever the registered endpoint, the
    request contract silently changes shape."""
    route = _call_route()
    assert route.methods == {"POST"}
    assert route.endpoint is ew.call_relayed_worker
    assert route.endpoint is not ew._relay_call


def test_call_takes_method_params_and_timeout_in_its_BODY() -> None:
    """The three fields a caller sends. When the decorator slipped onto the
    helper, `method` and `timeout` became query parameters and only `params`
    remained in the body -- a breaking change to the escape hatch, invisible in
    every behavioural test."""
    fields = set(ew.RelayCallRequest.model_fields)
    assert fields == {"method", "params", "timeout"}

    route = _call_route()
    body_params = [p for p in route.dependant.body_params]
    assert [p.name for p in body_params] == ["request"]
    assert body_params[0].field_info.annotation is ew.RelayCallRequest
    # job_name is the only thing that belongs in the path/query.
    assert {p.name for p in route.dependant.path_params} == {"job_name"}
    assert not route.dependant.query_params


def test_no_private_helper_is_registered_as_a_route() -> None:
    """The general form of the bug: a decorator sitting above the wrong `def`.
    An endpoint whose name starts with an underscore is a helper that escaped."""
    application = FastAPI()
    application.include_router(ew.router, prefix="/env-worker/v1")
    leaked = [
        getattr(r, "path", "")
        for r in application.routes
        if getattr(getattr(r, "endpoint", None), "__name__", "").startswith("_")
    ]
    assert not leaked, f"private helpers registered as routes: {leaked}"
