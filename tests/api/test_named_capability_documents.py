"""The document-shaped named endpoints (plan §E option (e), step 6b).

Companion to `test_named_capability_reads.py`. These carry a composite, a config,
a state document or a list of node paths, so they are POST.

Two things are pinned here that are easy to get wrong and impossible to see:

* **Field forwarding.** The bodies are passthrough (`extra="allow"`), so most of
  what a caller sends is never named in our code. What IS named must reach the
  worker under the key the worker reads -- and `schema` cannot be a plain field
  name, so it travels under an alias that has to be undone on the way out.
* **Which in-band failures become statuses.** The worker has three different
  ways of saying no and they do not agree: `__sentinel__` keys, `{"ok": false}`,
  and `{"status": "not_registered"}`. `ok: false` is a failure in `run_process`
  and a documented SUCCESS shape in `viz_preview`, so the rule cannot be global.
"""

from __future__ import annotations

from typing import Any

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from viva_api.api.routers import env_worker as ew


@pytest.fixture
def app(monkeypatch: pytest.MonkeyPatch) -> tuple[FastAPI, list[dict[str, Any]], dict[str, Any]]:
    calls: list[dict[str, Any]] = []
    reply: dict[str, Any] = {"value": {"ok": True}}

    async def _fake_relay_call(job_name: str, method: str, params: Any, timeout: float) -> Any:
        calls.append({"job": job_name, "method": method, "params": params})
        return reply["value"]

    monkeypatch.setattr(ew, "_relay_call", _fake_relay_call)
    application = FastAPI()
    application.include_router(ew.router, prefix="/env-worker/v1")
    return application, calls, reply


async def _post(app: FastAPI, path: str, body: Any) -> Any:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        return await c.post(f"/env-worker/v1/relay/workers/job-1{path}", json=body)


async def _get(app: FastAPI, path: str) -> Any:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        return await c.get(f"/env-worker/v1/relay/workers/job-1{path}")


# --- each URL asks for the method it claims ---------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("path", "body", "method"),
    [
        ("/composite-state", {"ref": "g"}, "resolve_composite_state"),
        ("/composite-state/inner", {"ref": "g", "hops": [["a"]]}, "resolve_inner_composite_state"),
        ("/composite-state/from-config", {"config": {}}, "config_to_composite"),
        ("/composite-state/docs", {"document": {}}, "attach_process_docs"),
        ("/observables", {"ref": "g"}, "observables"),
        ("/readout-check", {"spec": {}, "ref": "g"}, "study_readout_check"),
        ("/process-template", {"address": "local:P"}, "process_template"),
        ("/process-run", {"address": "local:P"}, "run_process"),
        ("/visualizations/render", {"viz_doc": {}}, "render_viz_doc"),
        ("/visualizations/preview", {"address": "local:V"}, "viz_preview"),
        ("/analysis-viewers/launch", {"uid": "u1"}, "analysis_viewers"),
    ],
)
async def test_a_document_endpoint_maps_to_its_worker_method(
    app: tuple[FastAPI, list[dict[str, Any]], dict[str, Any]], path: str, body: Any, method: str
) -> None:
    application, calls, _ = app
    r = await _post(application, path, body)
    assert r.status_code == 200, r.text
    assert [c["method"] for c in calls] == [method]


# --- field forwarding, where the invisible mistakes live --------------------


@pytest.mark.asyncio
async def test_an_inline_state_travels_under_the_key_schema_not_schema_underscore(
    app: tuple[FastAPI, list[dict[str, Any]], dict[str, Any]],
) -> None:
    """`schema` shadows a BaseModel attribute, so the field is `schema_` with an
    alias. Dumping WITHOUT `by_alias` sends the worker `schema_`, which it never
    reads -- so `observables` falls through to its `ref` branch and answers about
    a different composite. Nothing raises. This is the only place that shows."""
    application, calls, _ = app
    r = await _post(application, "/observables", {"state": {"a": 1}, "schema": {"b": 2}})
    assert r.status_code == 200
    assert calls[0]["params"] == {"state": {"a": 1}, "schema": {"b": 2}}
    assert "schema_" not in calls[0]["params"]


@pytest.mark.asyncio
async def test_unsent_fields_are_omitted_not_sent_as_null(
    app: tuple[FastAPI, list[dict[str, Any]], dict[str, Any]],
) -> None:
    """Worker handlers branch on PRESENCE (`if ref is not None`), so an explicit
    null takes a different path than an absent key."""
    application, calls, _ = app
    await _post(application, "/composite-state", {"ref": "g"})
    assert calls[0]["params"] == {"ref": "g"}


@pytest.mark.asyncio
async def test_undeclared_fields_are_forwarded_untouched(
    app: tuple[FastAPI, list[dict[str, Any]], dict[str, Any]],
) -> None:
    """Passthrough convention: the meaning of a config belongs to the workspace.
    Re-declaring it here would create a second, staler copy of a schema we do not
    own -- so a field we have never heard of must still reach the worker."""
    application, calls, _ = app
    await _post(application, "/process-run", {"address": "local:P", "some_future_knob": 7})
    assert calls[0]["params"] == {"address": "local:P", "some_future_knob": 7}


@pytest.mark.asyncio
async def test_the_two_viewer_operations_are_separate_routes(
    app: tuple[FastAPI, list[dict[str, Any]], dict[str, Any]],
) -> None:
    """One method, two operations behind an `action` flag. Listing is a read;
    launching invokes a contributor's callable. The flag is supplied here rather
    than by the caller, so neither route can be talked into the other."""
    application, calls, _ = app
    await _get(application, "/analysis-viewers")
    await _post(application, "/analysis-viewers/launch", {"uid": "u1", "study": "s"})
    assert calls[0]["params"] == {"action": "list"}
    assert calls[1]["params"] == {"action": "launch", "uid": "u1", "study": "s"}


@pytest.mark.asyncio
async def test_a_caller_cannot_override_the_viewer_action(
    app: tuple[FastAPI, list[dict[str, Any]], dict[str, Any]],
) -> None:
    """`action` is passthrough-eligible by accident of the base model, so pin it:
    posting to /launch must launch."""
    application, calls, _ = app
    await _post(application, "/analysis-viewers/launch", {"uid": "u1", "action": "list"})
    assert calls[0]["params"]["action"] == "launch"


# --- the boundary refuses what the worker would answer confusingly ----------


@pytest.mark.asyncio
async def test_a_selector_with_neither_ref_nor_state_is_refused_here(
    app: tuple[FastAPI, list[dict[str, Any]], dict[str, Any]],
) -> None:
    """The worker answers this with `__not_registered__`, which reads as "your
    ref is wrong" to someone who sent no ref at all. 422, and no worker call."""
    application, calls, _ = app
    r = await _post(application, "/observables", {})
    assert r.status_code == 422
    assert "ref" in r.text and "state" in r.text
    assert not calls


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("path", "body"),
    [
        ("/composite-state", {"ref": ""}),
        ("/composite-state/inner", {"ref": "g", "hops": []}),
        ("/composite-state/from-config", {}),
        ("/composite-state/docs", {}),
        ("/readout-check", {"ref": "g"}),
        ("/process-template", {}),
        ("/visualizations/render", {}),
        ("/visualizations/preview", {"address": ""}),
        ("/analysis-viewers/launch", {}),
    ],
)
async def test_a_missing_or_empty_required_field_never_reaches_the_worker(
    app: tuple[FastAPI, list[dict[str, Any]], dict[str, Any]], path: str, body: Any
) -> None:
    application, calls, _ = app
    r = await _post(application, path, body)
    assert r.status_code == 422, (path, r.text)
    assert not calls, path


@pytest.mark.asyncio
async def test_hops_must_be_paths_not_bare_strings(
    app: tuple[FastAPI, list[dict[str, Any]], dict[str, Any]],
) -> None:
    """`hops` is a list of node PATHS, each itself a list of key segments (what
    convert.ts emits). `["a"]` is the plausible wrong shape, and the worker would
    take it as one hop named `a` rather than rejecting it."""
    application, calls, _ = app
    r = await _post(application, "/composite-state/inner", {"ref": "g", "hops": ["a"]})
    assert r.status_code == 422
    assert not calls


# --- in-band failure, which does not use one vocabulary ---------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["/process-template", "/process-run"])
async def test_ok_false_is_a_failure_for_the_process_endpoints(
    app: tuple[FastAPI, list[dict[str, Any]], dict[str, Any]], path: str
) -> None:
    application, _, reply = app
    reply["value"] = {"ok": False, "stage": "core", "error": "build_core failed"}
    r = await _post(application, path, {"address": "local:P"})
    assert r.status_code == 422


@pytest.mark.asyncio
async def test_the_failure_keeps_its_stage_not_just_its_message(
    app: tuple[FastAPI, list[dict[str, Any]], dict[str, Any]],
) -> None:
    """`stage` says WHERE it stopped -- core, config, ports. Flattening the
    payload to a string throws away the half that makes it actionable."""
    application, _, reply = app
    reply["value"] = {"ok": False, "stage": "ports", "error": "could not fill 'x'"}
    r = await _post(application, "/process-run", {"address": "local:P"})
    detail = r.json()["detail"]
    assert detail["stage"] == "ports"
    assert detail["error"] == "could not fill 'x'"
    assert detail["method"] == "run_process"


@pytest.mark.asyncio
async def test_ok_false_is_NOT_a_failure_for_viz_preview(
    app: tuple[FastAPI, list[dict[str, Any]], dict[str, Any]],
) -> None:
    """The rule cannot be global. `viz_preview`'s contract is that every render
    outcome including a raise comes back as a 200 body with notes; only an
    unregistered address is non-200. Applying the process rule here would
    contradict a contract the workbench relies on."""
    application, _, reply = app
    reply["value"] = {"ok": False, "html": "", "notes": ["render raised"], "source_used": "demo"}
    r = await _post(application, "/visualizations/preview", {"address": "local:V"})
    assert r.status_code == 200
    assert r.json()["notes"] == ["render raised"]


@pytest.mark.asyncio
async def test_viz_preview_maps_its_own_not_registered_status_to_404(
    app: tuple[FastAPI, list[dict[str, Any]], dict[str, Any]],
) -> None:
    """A third idiom, used by exactly one method: `{"status": "not_registered"}`,
    which its docstring says the workbench maps to 404."""
    application, _, reply = app
    reply["value"] = {"status": "not_registered"}
    r = await _post(application, "/visualizations/preview", {"address": "local:nope"})
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_the_shared_sentinels_still_apply_to_document_endpoints(
    app: tuple[FastAPI, list[dict[str, Any]], dict[str, Any]],
) -> None:
    """The per-endpoint rules are additive; they do not replace the table."""
    application, _, reply = app
    reply["value"] = {"__build_error__": "generator raised"}
    r = await _post(application, "/composite-state", {"ref": "g"})
    assert r.status_code == 422
    assert r.json()["detail"] == "generator raised"


# --- the exclusion -----------------------------------------------------------


def test_validate_generated_visualization_has_no_named_endpoint() -> None:
    """EXCLUDED, same reasoning as `data_sources_provider`. It interpolates
    caller-supplied `pkg` and `module` into a module name and imports it -- and
    RELOADS it when already imported, re-running module-level code. It is a
    write-path verify whose only legitimate caller is the workbench, immediately
    after writing the file it checks; there is no client-side use for it.

    If this fails, the question is not "how do we fix the test" but "is there an
    identity boundary in front of this yet?" (§E Q3, open)."""
    application = FastAPI()
    application.include_router(ew.router, prefix="/env-worker/v1")
    paths = {getattr(r, "path", "") for r in application.routes}
    assert not any("validate" in p for p in paths)


# --- a fourth in-band idiom, found by asking what a launch actually returns ---
#
# A viewer launch does not render anything. `_av_resolve_launch` invokes the
# contributor's `launch(ws_root, study, run, ctx)` and returns its dict; the UI
# fetches this and opens the returned `{"url": ...}` in a new tab. So the payload
# is navigation instructions, and its failures arrive as
# `{"result": {"error": ..., "status": 404|400|500}}` -- carrying the status they
# want, which was being discarded into a 200.


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "why"),
    [(404, "no such uid"), (400, "viewer is not launchable"), (500, "the contributor's callable raised")],
)
async def test_a_launch_failure_uses_the_status_the_worker_already_chose(
    app: tuple[FastAPI, list[dict[str, Any]], dict[str, Any]], status: int, why: str
) -> None:
    application, _, reply = app
    reply["value"] = {"result": {"error": why, "status": status}}
    r = await _post(application, "/analysis-viewers/launch", {"uid": "u1"})
    assert r.status_code == status


@pytest.mark.asyncio
async def test_a_successful_launch_passes_its_instructions_through(
    app: tuple[FastAPI, list[dict[str, Any]], dict[str, Any]],
) -> None:
    """The success shape is a URL for the browser to open, not HTML."""
    application, _, reply = app
    reply["value"] = {"result": {"url": "/parsimony-viewer/index.html?file=x"}}
    r = await _post(application, "/analysis-viewers/launch", {"uid": "u1"})
    assert r.status_code == 200
    assert r.json()["result"]["url"].endswith("file=x")


@pytest.mark.asyncio
async def test_an_implausible_status_from_a_contributor_is_not_trusted(
    app: tuple[FastAPI, list[dict[str, Any]], dict[str, Any]],
) -> None:
    """The dict comes from a workspace package's own code. `status: 200` next to
    an `error`, or `status: "banana"`, must not become this response's status --
    it is still a failure, so 422."""
    application, _, reply = app
    for bogus in (200, 302, "banana", None):
        reply["value"] = {"result": {"error": "nope", "status": bogus}}
        r = await _post(application, "/analysis-viewers/launch", {"uid": "u1"})
        assert r.status_code == 422, bogus


@pytest.mark.asyncio
async def test_the_listing_is_unaffected_by_the_launch_rule(
    app: tuple[FastAPI, list[dict[str, Any]], dict[str, Any]],
) -> None:
    """`{"viewers": [...]}` has no nested result; the rule must not fire on it."""
    application, _, reply = app
    reply["value"] = {"viewers": [{"uid": "u1", "label": "Parsimony"}]}
    r = await _get(application, "/analysis-viewers")
    assert r.status_code == 200
    assert r.json()["viewers"][0]["uid"] == "u1"
