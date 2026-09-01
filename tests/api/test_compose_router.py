"""Tests for the generic compose /simulation/run endpoint."""

from __future__ import annotations

from contextlib import ExitStack
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient

from viva_api.compose.models import ComposeSimulationExperiment


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
