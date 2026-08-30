"""Every URL the CLI layer calls must still exist on the server.

`app/app_data_service.py` is the one HTTP seam all three clients share (CLI, TUI,
GUI), and it builds its URLs as string literals -- 41 of them. Nothing connects
those strings to the routes they name, so a renamed or deleted route breaks the
clients silently and only at runtime, against a deployment.

This is the cheap half of what adopting the generated client would have bought.
The expensive half -- attrs models replacing the server's own pydantic ones
across the display layer -- buys nothing here, because atlantis ships INSIDE
viva-api and imports the server models directly; and it would buy nothing for
the env-worker surface either, where 20 of 20 generated operations return bare
`Any`. So: keep httpx, and pin the drift.

WHAT THIS CATCHES: a route renamed, removed, or moved to a different method.
WHAT IT DOES NOT: a path PARAMETER renamed in the spec. That is invisible from
the client and harmless -- the caller interpolates a value positionally, so
`f"/api/v1/simulations/{simulation_id}"` works whether the server calls that
segment `simulation_id` or `sim_id`. Both sides are normalised to `{}` here for
exactly that reason; asserting on the variable's own name would be asserting on
local naming in a Python f-string.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest
import yaml

_REPO = Path(__file__).resolve().parents[3]
_SOURCE = _REPO / "app" / "app_data_service.py"
_SPEC = _REPO / "viva_api" / "api" / "spec" / "openapi_3_1_0_generated.yaml"

#: Prefixes that are viva-api routes. Anything else in the file is a local path,
#: an S3 key, or a log line, and is not this test's business.
_API_PREFIXES = ("/api/", "/core/", "/compose/", "/env-worker/", "/health", "/version")

_HTTP_METHODS = {"get", "post", "put", "patch", "delete", "head", "options", "stream", "request"}


def _normalise(path: str) -> str:
    """`/a/{anything}/b` -> `/a/{}/b`, so both sides compare on structure."""
    return re.sub(r"\{[^}]*\}", "{}", path.rstrip("/")) or "/"


def _literal_of(node: ast.expr) -> str | None:
    """The static text of a str constant or an f-string, params as `{}`."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        out = []
        for part in node.values:
            if isinstance(part, ast.Constant) and isinstance(part.value, str):
                out.append(part.value)
            elif isinstance(part, ast.FormattedValue):
                out.append("{}")
            else:  # pragma: no cover - defensive
                return None
        return "".join(out)
    return None


def _called_routes() -> set[tuple[str, str]]:
    """(method, normalised path) for every viva-api call in the CLI seam.

    Read from the AST rather than by regex so the METHOD is captured too --
    `self.client.get(...)` vs `.post(...)`. A route that survives but changes
    method is drift the URL alone would not show.
    """
    tree = ast.parse(_SOURCE.read_text(encoding="utf-8"))
    found: set[tuple[str, str]] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        attr = node.func.attr
        if attr not in _HTTP_METHODS:
            continue
        args = list(node.args)
        method = attr
        # `.stream("POST", url)` and `.request("GET", url)` carry the verb first.
        if attr in ("stream", "request") and args:
            verb = _literal_of(args[0])
            if verb is None:
                continue
            method, args = verb.lower(), args[1:]
        candidates = args + [kw.value for kw in node.keywords if kw.arg in (None, "url")]
        for candidate in candidates:
            text = _literal_of(candidate)
            if not text or not text.startswith(_API_PREFIXES):
                continue
            normalised = _normalise(text)
            # A path whose LAST segment is a parameter is composed, not named:
            # `/relay/workers/{}/{}` is `worker_read` building a URL from
            # READ_CAPABILITIES. Those are verified against the spec by their own
            # test above, from the table, which is the only way to see them.
            if normalised.endswith("/{}/{}"):
                continue
            found.add((method, normalised))
    return found


def _spec_routes() -> set[tuple[str, str]]:
    spec = yaml.safe_load(_SPEC.read_text(encoding="utf-8"))
    return {
        (method, _normalise(path)) for path, ops in spec["paths"].items() for method in ops if method in _HTTP_METHODS
    }


def test_the_extraction_actually_found_the_calls() -> None:
    """A test that silently matched nothing would pass forever. This file has
    dozens of calls; if the count collapses, the extractor broke, not the code."""
    called = _called_routes()
    assert len(called) >= 30, f"only found {len(called)} calls -- the AST walk is probably broken"


def test_every_named_read_capability_resolves_to_a_route() -> None:
    """`worker_read` composes its URL from a table, so the paths it can produce
    are enumerable. That is the point of the table: the first version pasted the
    capability straight into an f-string, and the test above could only see
    `/relay/workers/{}/{}` -- a path matching nothing, which is what a
    dynamically-built URL always looks like to static analysis.
    """
    from app.app_data_service import READ_CAPABILITIES

    spec = _spec_routes()
    missing = [
        c for c in READ_CAPABILITIES if ("get", _normalise(f"/env-worker/v1/relay/workers/{{}}/{c}")) not in spec
    ]
    assert not missing, f"READ_CAPABILITIES names endpoints that do not exist: {missing}"


def test_every_url_the_clients_call_still_exists_on_the_server() -> None:
    """The point. A renamed or deleted route fails here instead of at runtime,
    on a deployment, in front of whoever was trying to use it."""
    spec = _spec_routes()
    missing = sorted(f"{m.upper():6s} {p}" for m, p in _called_routes() - spec)
    assert not missing, "app_data_service.py calls routes that do not exist:\n  " + "\n  ".join(missing)


@pytest.mark.parametrize("path", [_SOURCE, _SPEC])
def test_both_inputs_exist(path: Path) -> None:
    """`make spec` regenerates the spec; a moved file would make the assertions
    above vacuous rather than failing."""
    assert path.is_file(), path
