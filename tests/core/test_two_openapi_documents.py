"""Two OpenAPI documents (``docs/plan-core.md`` P3f): core's own, from ``create_core_app()``, and the
application's, which stays the union. What the SMS callers see did not change; what a core caller can
be built against now exists on its own.
"""

from pathlib import Path

import yaml

from viva_api.api.main import app as sms_app
from viva_core.api.app import CORE_PREFIX
from viva_core.api.openapi_spec import SPEC_DIR, core_openapi


def _operations(document: dict[str, object]) -> dict[str, str]:
    paths = document["paths"]
    assert isinstance(paths, dict)
    return {
        str(body.get("operationId")): f"{method.upper()} {path}"
        for path, methods in paths.items()
        for method, body in methods.items()
        if isinstance(body, dict)
    }


def test_core_serves_only_under_its_prefix_and_has_its_own_document() -> None:
    document = core_openapi()
    ops = _operations(document)
    assert ops, "core serves nothing?"
    assert all(target.split(" ", 1)[1].startswith(CORE_PREFIX) for target in ops.values()), ops
    assert "core-health" in ops and "compose-run-simulation" in ops


def test_the_committed_core_document_is_current() -> None:
    committed = yaml.safe_load((SPEC_DIR / "openapi_3_1_0_generated.yaml").read_text(encoding="utf-8"))
    assert _operations(committed) == _operations(core_openapi()), "run `make spec`"


def test_the_application_document_is_the_union() -> None:
    """Every operation core serves is also served by the application -- under core's prefix for core's
    own routes, and under the application's prefixes for the routers it mounts at its callers' paths
    (``/compose/v1``, ``/env-worker/v1``). Same operation ids, so a client built on either agrees."""
    sms_ops = _operations(sms_app.openapi())
    for operation_id, target in _operations(core_openapi()).items():
        assert operation_id in sms_ops, f"{operation_id} ({target}) is core's but the application does not serve it"
    committed = yaml.safe_load((Path("viva_api/api/spec/openapi_3_1_0_generated.yaml")).read_text(encoding="utf-8"))
    assert set(_operations(committed)) == set(sms_ops), "the application's committed document is stale; run `make spec`"


def test_the_application_serves_the_interim_viva_v1_spelling_but_documents_it_once() -> None:
    """P3g (D17): the application mounts core's compose and env-worker routers under ``/viva/v1`` as
    well, so a caller can switch on ``viva-v1-surface`` before the resource families exist. Those
    routes are SERVED -- they answer by name like the same routes at ``/compose/v1`` -- but they are
    not in the application's document: the operation ids are explicit and a second copy would
    duplicate every one. Core's own document is where that spelling is described."""
    from fastapi.testclient import TestClient

    client = TestClient(sms_app)
    interim = client.get(f"{CORE_PREFIX}/compose/simulators")
    assert interim.status_code == client.get("/compose/v1/simulators").status_code != 404
    assert client.get(f"{CORE_PREFIX}/env-worker/workers/nope").status_code == 503
    core_paths = core_openapi()["paths"]
    assert isinstance(core_paths, dict)
    assert not any(p.startswith(f"{CORE_PREFIX}/compose") for p in sms_app.openapi()["paths"]), "documented twice"
    assert any(p.startswith(f"{CORE_PREFIX}/compose") for p in core_paths)
