"""Tests for the generic compose /simulation/run endpoint."""

from __future__ import annotations

import types
from contextlib import ExitStack
from typing import Any
from unittest.mock import MagicMock

import pytest
from httpx import ASGITransport, AsyncClient

import viva_api.api.routers.compose as compose_router
from viva_api.compose.models import ComposeSimulationExperiment
from viva_api.config import ComputeBackend


@pytest.mark.asyncio
async def test_submit_simulation_threads_extra_pip_deps(fastapi_app: object) -> None:
    captured: dict[str, Any] = {}

    async def _fake_run(**kwargs: Any) -> ComposeSimulationExperiment:
        captured.update(kwargs)
        return ComposeSimulationExperiment(simulation_database_id=1, simulator_database_id=1)

    from unittest.mock import AsyncMock, MagicMock, patch

    fake_db = MagicMock()
    fake_db.get_allow_list_db.return_value.list_allow_list = AsyncMock(
        return_value=["pypi::git+https://github.com/x/y.git@abc", "pypi::cobra"]
    )

    with ExitStack() as stack:
        stack.enter_context(patch("viva_api.api.routers.compose.run_compose_simulation", _fake_run))
        stack.enter_context(patch("viva_api.api.routers.compose._require_db", return_value=fake_db))
        stack.enter_context(patch("viva_api.api.routers.compose._require_sim", return_value=MagicMock()))
        stack.enter_context(patch("viva_api.api.routers.compose._require_monitor", return_value=MagicMock()))
        async with AsyncClient(transport=ASGITransport(app=fastapi_app), base_url="http://testserver") as client:  # type: ignore[arg-type]
            response = await client.post(
                "/compose/v1/simulation/run",
                params={"extra_pip_deps": ["git+https://github.com/x/y.git@abc", "cobra"]},
                files={"uploaded_file": ("m.pbg", b'{"state": {}}', "application/json")},
            )

    assert response.status_code == 200
    assert captured["extra_pip_deps"] == ["git+https://github.com/x/y.git@abc", "cobra"]


@pytest.mark.asyncio
async def test_submit_simulation_threads_simulator_id(fastapi_app: object) -> None:
    """item 98: simulator_id on the upload-transport endpoint reaches the
    ComposeSimulationRequest the compose simulation service will read."""
    captured: dict[str, Any] = {}

    async def _fake_run(**kwargs: Any) -> ComposeSimulationExperiment:
        captured.update(kwargs)
        return ComposeSimulationExperiment(simulation_database_id=1, simulator_database_id=1)

    from unittest.mock import AsyncMock, MagicMock, patch

    fake_db = MagicMock()
    fake_db.get_allow_list_db.return_value.list_allow_list = AsyncMock(return_value=["pypi::cobra"])

    with ExitStack() as stack:
        stack.enter_context(patch("viva_api.api.routers.compose.run_compose_simulation", _fake_run))
        stack.enter_context(patch("viva_api.api.routers.compose._require_db", return_value=fake_db))
        stack.enter_context(patch("viva_api.api.routers.compose._require_sim", return_value=MagicMock()))
        stack.enter_context(patch("viva_api.api.routers.compose._require_monitor", return_value=MagicMock()))
        async with AsyncClient(transport=ASGITransport(app=fastapi_app), base_url="http://testserver") as client:  # type: ignore[arg-type]
            response = await client.post(
                "/compose/v1/simulation/run",
                params={"simulator_id": 42},
                files={"uploaded_file": ("m.pbg", b'{"state": {}}', "application/json")},
            )

    assert response.status_code == 200
    assert captured["simulation_request"].simulator_id == 42


# --- item 98: document-as-JSON-body sibling of the upload transport ---


@pytest.mark.asyncio
async def test_submit_simulation_document_dispatches_the_inline_document(fastapi_app: object) -> None:
    """The JSON-body transport must reach the exact same dispatch path as the
    upload transport -- same handler, same ComposeSimulationRequest shape,
    just a different way of getting the document onto disk."""
    captured: dict[str, Any] = {}

    async def _fake_run(**kwargs: Any) -> ComposeSimulationExperiment:
        captured.update(kwargs)
        return ComposeSimulationExperiment(simulation_database_id=1, simulator_database_id=1)

    from unittest.mock import AsyncMock, MagicMock, patch

    fake_db = MagicMock()
    fake_db.get_allow_list_db.return_value.list_allow_list = AsyncMock(return_value=["pypi::cobra"])

    with ExitStack() as stack:
        stack.enter_context(patch("viva_api.api.routers.compose.run_compose_simulation", _fake_run))
        stack.enter_context(patch("viva_api.api.routers.compose._require_db", return_value=fake_db))
        stack.enter_context(patch("viva_api.api.routers.compose._require_sim", return_value=MagicMock()))
        stack.enter_context(patch("viva_api.api.routers.compose._require_monitor", return_value=MagicMock()))
        async with AsyncClient(transport=ASGITransport(app=fastapi_app), base_url="http://testserver") as client:  # type: ignore[arg-type]
            response = await client.post(
                "/compose/v1/simulation/run-document",
                json={
                    "document": {"state": {"a_process": {"_type": "process", "address": "local:Foo"}}},
                    "interval_time": 3.0,
                    "simulator_id": 7,
                    "extra_pip_deps": ["cobra"],
                },
            )

    assert response.status_code == 200
    req = captured["simulation_request"]
    assert req.simulator_id == 7
    assert req.end_time_point == 3.0
    assert req.simulation_file_type.value == "pbg"
    # written to a real temp file, readable back as the exact document sent
    import json as _json

    on_disk = _json.loads(req.request_file_path.read_text())
    assert on_disk == {"state": {"a_process": {"_type": "process", "address": "local:Foo"}}}
    assert captured["extra_pip_deps"] == ["cobra"]


@pytest.mark.asyncio
async def test_submit_simulation_document_rejects_empty_document(fastapi_app: object) -> None:
    async with AsyncClient(transport=ASGITransport(app=fastapi_app), base_url="http://testserver") as client:  # type: ignore[arg-type]
        response = await client.post("/compose/v1/simulation/run-document", json={"document": {}})
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_submit_simulation_document_rejects_bad_interval(fastapi_app: object) -> None:
    async with AsyncClient(transport=ASGITransport(app=fastapi_app), base_url="http://testserver") as client:  # type: ignore[arg-type]
        response = await client.post(
            "/compose/v1/simulation/run-document",
            json={"document": {"state": {}}, "interval_time": -1.0},
        )
    assert response.status_code == 400


# --- item 98: compute_backend -- per-request selection among the registered
# ComposeSimulationService backends (_require_sim's own resolution logic) ---


def test_require_sim_with_no_backend_returns_the_default(monkeypatch: pytest.MonkeyPatch) -> None:
    """None (the vast majority of requests -- this field is optional) preserves
    today's exact behavior: the deployment's single default service, no registry
    lookup at all."""
    default = MagicMock(name="default_service")
    monkeypatch.setattr(compose_router, "_compose_sim_service", default)
    assert compose_router._require_sim(None) is default


def test_require_sim_resolves_the_explicitly_requested_registered_backend(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    default = MagicMock(name="default_service")
    ray_service = MagicMock(name="ray_service")
    monkeypatch.setattr(compose_router, "_compose_sim_service", default)
    monkeypatch.setattr(
        compose_router,
        "_compose_job_monitor",
        types.SimpleNamespace(sim_registry={ComputeBackend.RAY: ray_service}),
    )
    # Explicitly asking for RAY must return the RAY service, not the default --
    # even when RAY also happens to BE the default, this proves the registry
    # path was actually taken rather than the None-shortcut above.
    assert compose_router._require_sim(ComputeBackend.RAY) is ray_service
    assert compose_router._require_sim(ComputeBackend.RAY) is not default


def test_require_sim_fails_loud_when_requested_backend_is_not_registered(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression target, named directly in the source comment: a caller who
    explicitly asks for a backend must never silently get a different one back
    (viva-api#353's own "looked successful, ran the wrong thing" class of bug)."""
    from fastapi import HTTPException

    monkeypatch.setattr(compose_router, "_compose_sim_service", MagicMock())
    monkeypatch.setattr(
        compose_router,
        "_compose_job_monitor",
        types.SimpleNamespace(sim_registry={ComputeBackend.RAY: MagicMock()}),
    )
    with pytest.raises(HTTPException) as exc_info:
        compose_router._require_sim(ComputeBackend.SLURM)
    assert exc_info.value.status_code == 400
    assert "slurm" in str(exc_info.value.detail)


def test_require_sim_fails_loud_with_no_monitor_at_all(monkeypatch: pytest.MonkeyPatch) -> None:
    """Same fail-loud contract when the monitor itself was never initialized --
    an empty registry, not a crash or a silent default substitution."""
    from fastapi import HTTPException

    monkeypatch.setattr(compose_router, "_compose_sim_service", MagicMock())
    monkeypatch.setattr(compose_router, "_compose_job_monitor", None)
    with pytest.raises(HTTPException) as exc_info:
        compose_router._require_sim(ComputeBackend.RAY)
    assert exc_info.value.status_code == 400


@pytest.mark.asyncio
async def test_submit_simulation_threads_compute_backend_to_require_sim(fastapi_app: object) -> None:
    """End-to-end: the upload endpoint's compute_backend query param must reach
    _require_sim as the exact ComputeBackend it resolved -- not a raw string,
    not silently dropped."""
    from unittest.mock import AsyncMock, patch

    fake_db = MagicMock()
    fake_db.get_allow_list_db.return_value.list_allow_list = AsyncMock(return_value=["pypi::cobra"])
    fake_require_sim = MagicMock(return_value=MagicMock())

    async def _fake_run(**kwargs: Any) -> ComposeSimulationExperiment:
        return ComposeSimulationExperiment(simulation_database_id=1, simulator_database_id=1)

    with ExitStack() as stack:
        stack.enter_context(patch("viva_api.api.routers.compose.run_compose_simulation", _fake_run))
        stack.enter_context(patch("viva_api.api.routers.compose._require_db", return_value=fake_db))
        stack.enter_context(patch("viva_api.api.routers.compose._require_sim", fake_require_sim))
        stack.enter_context(patch("viva_api.api.routers.compose._require_monitor", return_value=MagicMock()))
        async with AsyncClient(transport=ASGITransport(app=fastapi_app), base_url="http://testserver") as client:  # type: ignore[arg-type]
            response = await client.post(
                "/compose/v1/simulation/run",
                params={"compute_backend": "ray"},
                files={"uploaded_file": ("m.pbg", b'{"state": {}}', "application/json")},
            )

    assert response.status_code == 200
    fake_require_sim.assert_called_once_with(ComputeBackend.RAY)


@pytest.mark.asyncio
async def test_submit_simulation_document_threads_compute_backend_to_require_sim(fastapi_app: object) -> None:
    from unittest.mock import AsyncMock, patch

    fake_db = MagicMock()
    fake_db.get_allow_list_db.return_value.list_allow_list = AsyncMock(return_value=["pypi::cobra"])
    fake_require_sim = MagicMock(return_value=MagicMock())

    async def _fake_run(**kwargs: Any) -> ComposeSimulationExperiment:
        return ComposeSimulationExperiment(simulation_database_id=1, simulator_database_id=1)

    with ExitStack() as stack:
        stack.enter_context(patch("viva_api.api.routers.compose.run_compose_simulation", _fake_run))
        stack.enter_context(patch("viva_api.api.routers.compose._require_db", return_value=fake_db))
        stack.enter_context(patch("viva_api.api.routers.compose._require_sim", fake_require_sim))
        stack.enter_context(patch("viva_api.api.routers.compose._require_monitor", return_value=MagicMock()))
        async with AsyncClient(transport=ASGITransport(app=fastapi_app), base_url="http://testserver") as client:  # type: ignore[arg-type]
            response = await client.post(
                "/compose/v1/simulation/run-document",
                json={"document": {"state": {}}, "compute_backend": "slurm"},
            )

    assert response.status_code == 200
    fake_require_sim.assert_called_once_with(ComputeBackend.SLURM)


@pytest.mark.asyncio
async def test_submit_simulation_omits_compute_backend_by_default(fastapi_app: object) -> None:
    """No compute_backend sent -> _require_sim(None) -- byte-for-byte today's
    existing behavior for every caller that doesn't know this field exists yet."""
    from unittest.mock import AsyncMock, patch

    fake_db = MagicMock()
    fake_db.get_allow_list_db.return_value.list_allow_list = AsyncMock(return_value=["pypi::cobra"])
    fake_require_sim = MagicMock(return_value=MagicMock())

    async def _fake_run(**kwargs: Any) -> ComposeSimulationExperiment:
        return ComposeSimulationExperiment(simulation_database_id=1, simulator_database_id=1)

    with ExitStack() as stack:
        stack.enter_context(patch("viva_api.api.routers.compose.run_compose_simulation", _fake_run))
        stack.enter_context(patch("viva_api.api.routers.compose._require_db", return_value=fake_db))
        stack.enter_context(patch("viva_api.api.routers.compose._require_sim", fake_require_sim))
        stack.enter_context(patch("viva_api.api.routers.compose._require_monitor", return_value=MagicMock()))
        async with AsyncClient(transport=ASGITransport(app=fastapi_app), base_url="http://testserver") as client:  # type: ignore[arg-type]
            response = await client.post(
                "/compose/v1/simulation/run-document",
                json={"document": {"state": {}}},
            )

    assert response.status_code == 200
    fake_require_sim.assert_called_once_with(None)


# --- item 102: num_nodes threads onto the ComposeSimulationRequest ---


@pytest.mark.asyncio
async def test_submit_simulation_document_threads_num_nodes(fastapi_app: object) -> None:
    from unittest.mock import AsyncMock, patch

    captured: dict[str, Any] = {}

    async def _fake_run(**kwargs: Any) -> ComposeSimulationExperiment:
        captured.update(kwargs)
        return ComposeSimulationExperiment(simulation_database_id=1, simulator_database_id=1)

    fake_db = MagicMock()
    fake_db.get_allow_list_db.return_value.list_allow_list = AsyncMock(return_value=["pypi::cobra"])

    with ExitStack() as stack:
        stack.enter_context(patch("viva_api.api.routers.compose.run_compose_simulation", _fake_run))
        stack.enter_context(patch("viva_api.api.routers.compose._require_db", return_value=fake_db))
        stack.enter_context(patch("viva_api.api.routers.compose._require_sim", return_value=MagicMock()))
        stack.enter_context(patch("viva_api.api.routers.compose._require_monitor", return_value=MagicMock()))
        async with AsyncClient(transport=ASGITransport(app=fastapi_app), base_url="http://testserver") as client:  # type: ignore[arg-type]
            response = await client.post(
                "/compose/v1/simulation/run-document",
                json={"document": {"state": {}}, "num_nodes": 8},
            )

    assert response.status_code == 200
    assert captured["simulation_request"].num_nodes == 8


@pytest.mark.asyncio
async def test_submit_simulation_document_omits_num_nodes_by_default(fastapi_app: object) -> None:
    """No num_nodes sent -> None on the request, byte-for-byte today's exact
    behavior for every caller that doesn't know this field exists yet."""
    from unittest.mock import AsyncMock, patch

    captured: dict[str, Any] = {}

    async def _fake_run(**kwargs: Any) -> ComposeSimulationExperiment:
        captured.update(kwargs)
        return ComposeSimulationExperiment(simulation_database_id=1, simulator_database_id=1)

    fake_db = MagicMock()
    fake_db.get_allow_list_db.return_value.list_allow_list = AsyncMock(return_value=["pypi::cobra"])

    with ExitStack() as stack:
        stack.enter_context(patch("viva_api.api.routers.compose.run_compose_simulation", _fake_run))
        stack.enter_context(patch("viva_api.api.routers.compose._require_db", return_value=fake_db))
        stack.enter_context(patch("viva_api.api.routers.compose._require_sim", return_value=MagicMock()))
        stack.enter_context(patch("viva_api.api.routers.compose._require_monitor", return_value=MagicMock()))
        async with AsyncClient(transport=ASGITransport(app=fastapi_app), base_url="http://testserver") as client:  # type: ignore[arg-type]
            response = await client.post(
                "/compose/v1/simulation/run-document",
                json={"document": {"state": {}}},
            )

    assert response.status_code == 200
    assert captured["simulation_request"].num_nodes is None


# --- item109: MAX_INTERVAL_TIME must fit a real pbg-native lineage campaign, and the
# cap must still reject a genuinely malformed value (regression tests ported from the
# duplicate fix in #383, adapted to this branch's own MAX_INTERVAL_TIME constant) ---


def test_max_interval_time_accommodates_a_real_lineage_run() -> None:
    """8 generations * 3600s/gen (a real CD2 lineage run's total simulated time)
    must fit under the cap -- this used to be impossible at the old 1000s bound."""
    assert compose_router.MAX_INTERVAL_TIME >= 8 * 3600.0


@pytest.mark.asyncio
async def test_submit_simulation_document_rejects_interval_past_the_new_cap(fastapi_app: object) -> None:
    async with AsyncClient(transport=ASGITransport(app=fastapi_app), base_url="http://testserver") as client:  # type: ignore[arg-type]
        response = await client.post(
            "/compose/v1/simulation/run-document",
            json={"document": {"state": {}}, "interval_time": compose_router.MAX_INTERVAL_TIME + 1.0},
        )
    assert response.status_code == 400
    assert "interval_time must be between" in response.json()["detail"]
