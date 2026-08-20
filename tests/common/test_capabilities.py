"""Capability advertisement -- the contract clients feature-detect against.

The behaviour worth protecting is not "the endpoint returns a list". It is that
a capability is advertised only when the deployment can GENUINELY serve it:
code present AND configured AND the backend wired. Advertising on any weaker
basis is the failure this endpoint exists to prevent (see the module docstring
and the 2026-08-19 production incident).
"""

from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient

from viva_api.api.main import app
from viva_api.common import capabilities
from viva_api.common.capabilities import (
    CAPABILITY_CONTAINER_JOBS,
    CAPABILITY_DUAL_ENGINE_COMPARISON,
    CAPABILITY_REGISTRY,
    ServerCapabilities,
    detect_capabilities,
    get_server_capabilities,
)
from viva_api.version import __version__


class _FakeService:
    """Stands in for SimulationServiceRay; attributes decide capability presence."""

    def __init__(self, *, container: bool = False, comparison: bool = False) -> None:
        if container:
            self._submit_container = lambda **_: "job-id"
        if comparison:
            self.submit_comparison_dispatch_job = lambda **_: "job-id"


class _FakeSettings:
    def __init__(self, queue: str = "", job_def: str = "") -> None:
        self.ray_container_queue = queue
        self.ray_container_job_definition = job_def


def _patch(monkeypatch: pytest.MonkeyPatch, *, service: Any, settings: Any = None) -> None:
    monkeypatch.setattr(capabilities, "_ray_service", lambda: service)
    monkeypatch.setattr(capabilities, "get_settings", lambda: settings or _FakeSettings())


# --- the naming contract -------------------------------------------------


def test_capability_names_are_unique_stable_slugs() -> None:
    """Names are a public API: unique, lower-case kebab-case, no surprises."""
    names = [name for name, _ in CAPABILITY_REGISTRY]
    assert len(names) == len(set(names)), "duplicate capability name"
    for name in names:
        assert name == name.lower(), f"{name!r} must be lower-case"
        assert " " not in name and "_" not in name, f"{name!r} must be kebab-case"
        assert name.strip("-") == name, f"{name!r} must not have leading/trailing dashes"


def test_detect_capabilities_is_sorted_for_stable_output() -> None:
    names = detect_capabilities()
    assert names == sorted(names)


# --- container-jobs: code AND configuration ------------------------------


def test_container_jobs_absent_when_code_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """A build predating #258 must not advertise it, however configured."""
    _patch(
        monkeypatch,
        service=_FakeService(container=False),
        settings=_FakeSettings("smscdk-ray-standalone", "smscdk-ray-container"),
    )
    assert CAPABILITY_CONTAINER_JOBS not in detect_capabilities()


def test_container_jobs_absent_when_code_present_but_unconfigured(monkeypatch: pytest.MonkeyPatch) -> None:
    """THE case this endpoint exists for.

    #258's _submit_container raises RuntimeError naming the unset setting when
    the queue/job-def are blank. A build carrying the code onto an unconfigured
    deployment cannot honour the capability, so it must not advertise it --
    otherwise a feature-detecting client is handed a capability that fails at
    dispatch, which is exactly the assumption this replaces.
    """
    _patch(monkeypatch, service=_FakeService(container=True), settings=_FakeSettings("", ""))
    assert CAPABILITY_CONTAINER_JOBS not in detect_capabilities()


@pytest.mark.parametrize(
    ("queue", "job_def"),
    [("smscdk-ray-standalone", ""), ("", "smscdk-ray-container")],
)
def test_container_jobs_requires_both_settings(monkeypatch: pytest.MonkeyPatch, queue: str, job_def: str) -> None:
    """Half-configured is unconfigured -- either blank setting fails at submit."""
    _patch(monkeypatch, service=_FakeService(container=True), settings=_FakeSettings(queue, job_def))
    assert CAPABILITY_CONTAINER_JOBS not in detect_capabilities()


def test_container_jobs_present_when_code_and_config_both_available(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch(
        monkeypatch,
        service=_FakeService(container=True),
        settings=_FakeSettings("smscdk-ray-standalone", "smscdk-ray-container"),
    )
    assert CAPABILITY_CONTAINER_JOBS in detect_capabilities()


# --- dual-engine-comparison depends on container-jobs --------------------


def test_dual_engine_comparison_requires_container_jobs(monkeypatch: pytest.MonkeyPatch) -> None:
    """Not merely alongside: the reference engine IS a container job.

    Without it there is no single reference job id known at submission time, so
    the compare job has nothing to depend_on.
    """
    _patch(
        monkeypatch,
        service=_FakeService(container=False, comparison=True),
        settings=_FakeSettings("q", "jd"),
    )
    caps = detect_capabilities()
    assert CAPABILITY_DUAL_ENGINE_COMPARISON not in caps
    assert CAPABILITY_CONTAINER_JOBS not in caps


def test_dual_engine_comparison_present_when_fully_available(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch(
        monkeypatch,
        service=_FakeService(container=True, comparison=True),
        settings=_FakeSettings("q", "jd"),
    )
    caps = detect_capabilities()
    assert CAPABILITY_DUAL_ENGINE_COMPARISON in caps
    assert CAPABILITY_CONTAINER_JOBS in caps


# --- robustness: a probe must never take the endpoint down ---------------


def test_a_raising_probe_degrades_to_unsupported(monkeypatch: pytest.MonkeyPatch) -> None:
    def _boom() -> bool:
        raise RuntimeError("probe exploded")

    monkeypatch.setattr(capabilities, "CAPABILITY_REGISTRY", [("exploding-probe", _boom)])
    assert detect_capabilities() == []


def test_no_ray_backend_yields_no_batch_capabilities(monkeypatch: pytest.MonkeyPatch) -> None:
    """A SLURM-only deployment honestly advertises none of these."""
    _patch(monkeypatch, service=None)
    caps = detect_capabilities()
    assert CAPABILITY_CONTAINER_JOBS not in caps
    assert CAPABILITY_DUAL_ENGINE_COMPARISON not in caps


# --- the payload ---------------------------------------------------------


def test_payload_reports_real_version_but_capabilities_carry_the_signal() -> None:
    payload = get_server_capabilities()
    assert isinstance(payload, ServerCapabilities)
    assert payload.version == __version__
    assert isinstance(payload.capabilities, list)
    assert all(isinstance(name, str) for name in payload.capabilities)


@pytest.mark.asyncio
async def test_endpoint_serves_the_payload() -> None:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.get("/core/v1/capabilities")
    assert response.status_code == 200
    body = response.json()
    assert body["version"] == __version__
    assert isinstance(body["capabilities"], list)
