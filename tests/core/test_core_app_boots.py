"""``create_core_app()`` boots, alone (``docs/plan-core.md`` P3, first step; decision D7).

The plan's order for P3: the app factory and a test that BOOTS it come first, and only then the move
of 5,500 lines of compose and env-worker into core -- "moving 5.5k lines into a package nothing boots
is unverifiable". This is that test. It runs core as an application of its own, in an interpreter
where importing ``viva_api`` or ``app`` raises, and asks it the one thing core can answer today.
"""

import json
import subprocess
import sys

from fastapi import FastAPI
from fastapi.testclient import TestClient

from viva_core.api import CORE_PREFIX, build_core_router, create_core_app
from viva_core.container import CoreContainer
from viva_core.environments import RegistryEnvironmentResolver
from viva_core.settings import CoreSettings

RUNTIME = "registry.example.org/core-runtime:1"


def _client(*, runtime_image: str | None = RUNTIME, environments: bool = True) -> TestClient:
    resolver = (
        RegistryEnvironmentResolver(registry="registry.example.org", repository="sim", runtime_image=runtime_image)
        if environments
        else None
    )
    return TestClient(create_core_app(CoreContainer(settings=CoreSettings(), environments=resolver)))


def test_core_boots_with_the_application_unimportable() -> None:
    """The whole point of D7: a fresh interpreter in which ``viva_api`` and ``app`` cannot be imported
    builds the app from ``CoreSettings`` alone, serves a request, and writes its own OpenAPI document."""
    program = """
import json, os, sys

class _Blocked:
    def find_spec(self, name, path=None, target=None):
        if name.split(".")[0] in ("viva_api", "app", "sms_api"):
            raise ImportError(f"core reached for {name!r} -- core must be standalone")
        return None

sys.meta_path.insert(0, _Blocked())
os.environ["ENVIRONMENT_REGISTRY"] = "registry.example.org"
os.environ["ENVIRONMENT_REPOSITORY"] = "sim"
from fastapi.testclient import TestClient
from viva_core.api import create_core_app

client = TestClient(create_core_app())
health = client.get("/viva/v1/health").json()
found = client.post("/viva/v1/environments/resolve", json={"kind": "explicit", "key": "abc1234"}).json()
paths = sorted(client.get("/viva/v1/openapi.json").json()["paths"])
leaked = sorted(m for m in sys.modules if m.split(".")[0] in ("viva_api", "app", "sms_api"))
print(json.dumps({"health": health, "image": found["image"], "paths": paths, "leaked": leaked}))
"""
    done = subprocess.run([sys.executable, "-c", program], capture_output=True, text=True, check=False)  # noqa: S603
    assert done.returncode == 0, done.stderr[-2000:]
    seen = json.loads(done.stdout.strip().splitlines()[-1])
    assert seen["health"] == {"status": "ok", "services": {"environments": True}}
    assert seen["image"] == "registry.example.org/sim:abc1234"
    assert seen["paths"] == ["/viva/v1/environments/resolve", "/viva/v1/health"]
    assert seen["leaked"] == []


def test_health_says_which_services_this_deployment_provides() -> None:
    assert _client().get(f"{CORE_PREFIX}/health").json()["services"] == {"environments": True}
    assert _client(environments=False).get(f"{CORE_PREFIX}/health").json()["services"] == {"environments": False}


def test_resolve_answers_with_both_identities_and_never_with_something_close() -> None:
    client = _client()
    explicit = client.post(f"{CORE_PREFIX}/environments/resolve", json={"kind": "explicit", "key": "abc1234"})
    assert explicit.status_code == 200
    body = explicit.json()
    assert body["image"] == "registry.example.org/sim:abc1234"
    assert len(body["spec_hash"]) == 64 and body["image_digest"] is None

    submit = client.post(
        f"{CORE_PREFIX}/environments/resolve", json={"kind": "explicit", "key": "abc1234", "variant": "submit"}
    )
    assert submit.json()["image"].endswith(":abc1234-submit")

    # a composite that needs nothing: the runtime image, where there is one
    assert client.post(f"{CORE_PREFIX}/environments/resolve", json={"kind": "derived"}).json()["image"] == RUNTIME
    none_here = _client(runtime_image=None).post(f"{CORE_PREFIX}/environments/resolve", json={"kind": "derived"})
    assert none_here.status_code == 404 and "no runtime image" in none_here.json()["detail"]

    # one that needs something core cannot select or build: refused, naming it
    wants = {"kind": "derived", "dependencies": [{"name": "tellurium"}]}
    refused = client.post(f"{CORE_PREFIX}/environments/resolve", json=wants)
    assert refused.status_code == 404 and "tellurium" in refused.json()["detail"]


def test_a_bad_request_is_the_callers_and_a_missing_service_is_the_deployments() -> None:
    client = _client()
    assert client.post(f"{CORE_PREFIX}/environments/resolve", json={"kind": "explicit"}).status_code == 422
    assert client.post(f"{CORE_PREFIX}/environments/resolve", json={"kind": "explicit", "key": " x"}).status_code == 422
    assert client.post(f"{CORE_PREFIX}/environments/resolve", json={"kind": "nonsense"}).status_code == 422
    unprovided = _client(environments=False).post(
        f"{CORE_PREFIX}/environments/resolve", json={"kind": "explicit", "key": "abc1234"}
    )
    assert unprovided.status_code == 501


def test_an_application_includes_the_router_and_the_paths_are_the_same() -> None:
    """Included, not mounted: a mounted app's lifespan never runs and its routes leave the application's
    OpenAPI document. The provider is called PER REQUEST, so an application may build its container
    after the router is included -- which is when an application's services exist."""
    held: list[CoreContainer] = []
    application = FastAPI()
    application.include_router(build_core_router(lambda: held[0]))
    client = TestClient(application)

    held.append(CoreContainer(settings=CoreSettings()))  # built AFTER the router was included
    assert client.get(f"{CORE_PREFIX}/health").json()["services"] == {"environments": False}
    assert f"{CORE_PREFIX}/environments/resolve" in client.get("/openapi.json").json()["paths"]
