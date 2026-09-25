"""The SMS application serves core's router under ``/viva/v1`` (``docs/plan-core.md`` P3, first step).

Core's own tests boot core ALONE (``tests/core/test_core_app_boots.py``). These check the other half:
that the application provides core's container -- its own settings, the site's resolver -- and that
what core answers is what the application itself would have said.
"""

from types import SimpleNamespace
from unittest.mock import patch

from fastapi.testclient import TestClient

from viva_api.api.main import app
from viva_api.common.site_environments import environment_image
from viva_core.api import CORE_PREFIX
from viva_core.version import __version__ as core_version

RUNTIME = "ghcr.io/vivarium-collective/viva-core-runtime:0.1.0"


def _settings(**overrides: str) -> SimpleNamespace:
    return SimpleNamespace(**{
        "ecr_account_id": "476270107793",
        "batch_region": "us-gov-west-1",
        "ray_ecr_repository": "v2ecoli",
        "core_runtime_image": RUNTIME,
        **overrides,
    })


def test_core_answers_what_the_application_itself_would_say() -> None:
    client = TestClient(app)
    with patch("viva_api.core_wiring.get_settings", _settings):
        health = client.get(f"{CORE_PREFIX}/health").json()
        assert health == {"status": "ok", "version": core_version, "services": {"environments": True}}
        for key, variant in (("d67b0a7", ""), ("tmp-d67b0a7-0a1b2c", ""), ("d67b0a7", "submit")):
            request = {"kind": "explicit", "key": key, "variant": variant}
            found = client.post(f"{CORE_PREFIX}/environments/resolve", json=request).json()
            assert found["image"] == environment_image(_settings(), key, variant=variant)
        runtime = client.post(f"{CORE_PREFIX}/environments/resolve", json={"kind": "derived"})
        assert runtime.json()["image"] == RUNTIME


def test_a_site_that_names_no_runtime_image_says_so_through_core_too() -> None:
    client = TestClient(app)
    with patch("viva_api.core_wiring.get_settings", lambda: _settings(core_runtime_image="")):
        assert client.post(f"{CORE_PREFIX}/environments/resolve", json={"kind": "derived"}).status_code == 404


def test_cores_routes_are_in_the_applications_openapi_document_under_their_own_prefix() -> None:
    paths = TestClient(app).get("/openapi.json").json()["paths"]
    core = sorted(p for p in paths if p.startswith(CORE_PREFIX))
    # Core's own three. The interim `/viva/v1/{compose,env-worker}` mounts (P3g) are served but
    # documented only in core's own document -- see tests/core/test_two_openapi_documents.py.
    assert core == [f"{CORE_PREFIX}/capabilities", f"{CORE_PREFIX}/environments/resolve", f"{CORE_PREFIX}/health"]
    assert "/core/v1/simulator/latest" in paths  # SMS's own, older `core` router: a different thing, untouched


def test_an_unset_account_is_the_deployments_fault_and_the_answer_names_the_setting() -> None:
    """Until 2026-09-21 this route handed a client ``.dkr.ecr.<region>.amazonaws.com/v2ecoli:<key>``.
    Now: 501, naming ``ECR_ACCOUNT_ID`` -- while health, and the runtime image, which need no
    registry, answer as before."""
    client = TestClient(app)
    with patch("viva_api.core_wiring.get_settings", lambda: _settings(ecr_account_id="")):
        refused = client.post(f"{CORE_PREFIX}/environments/resolve", json={"kind": "explicit", "key": "d67b0a7"})
        assert refused.status_code == 501
        assert "ECR_ACCOUNT_ID" in refused.json()["detail"] and ".dkr.ecr." not in refused.text
        assert client.get(f"{CORE_PREFIX}/health").status_code == 200
        runtime = client.post(f"{CORE_PREFIX}/environments/resolve", json={"kind": "derived"})
        assert runtime.status_code == 200 and runtime.json()["image"] == RUNTIME
