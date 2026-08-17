"""Tests for the compose /analysis/* endpoints — item 50 Gap 6."""

from __future__ import annotations

from contextlib import ExitStack
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from viva_api.compose.models import ComposeAnalysis, ComposeAnalysisStatus


def _analysis(**overrides: object) -> ComposeAnalysis:
    base: dict[str, object] = {
        "database_id": 7,
        "name": "analysis-exp-1-abcdef",
        "config": {"n_seeds": 2, "n_generations": 2, "modules": "applicable"},
        "simulation_id": 42,
        "job_id_ext": "batch-job-1",
        "status": ComposeAnalysisStatus.COMPUTING,
    }
    base.update(overrides)
    return ComposeAnalysis(**base)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_submit_analysis_returns_the_created_row(fastapi_app: object) -> None:
    captured: dict[str, Any] = {}
    result = _analysis()

    async def _fake_run(**kwargs: Any) -> ComposeAnalysis:
        captured.update(kwargs)
        return result

    with ExitStack() as stack:
        stack.enter_context(patch("viva_api.api.routers.compose.run_compose_analysis", _fake_run))
        stack.enter_context(patch("viva_api.api.routers.compose._require_db", return_value=MagicMock()))
        stack.enter_context(patch("viva_api.api.routers.compose._require_sim", return_value=MagicMock()))
        async with AsyncClient(transport=ASGITransport(app=fastapi_app), base_url="http://testserver") as client:  # type: ignore[arg-type]
            response = await client.post(
                "/compose/v1/analysis/run",
                json={"simulation_id": 42, "n_seeds": 2, "n_generations": 2, "modules": "applicable"},
            )

    assert response.status_code == 200
    assert response.json()["database_id"] == 7
    assert response.json()["job_id_ext"] == "batch-job-1"
    assert captured["request"].simulation_id == 42
    assert captured["request"].n_seeds == 2
    assert captured["request"].n_generations == 2


@pytest.mark.asyncio
async def test_submit_analysis_404s_when_the_simulation_does_not_exist(fastapi_app: object) -> None:
    async def _fake_run(**kwargs: Any) -> ComposeAnalysis:
        raise LookupError("Compose simulation 999 not found")

    with ExitStack() as stack:
        stack.enter_context(patch("viva_api.api.routers.compose.run_compose_analysis", _fake_run))
        stack.enter_context(patch("viva_api.api.routers.compose._require_db", return_value=MagicMock()))
        stack.enter_context(patch("viva_api.api.routers.compose._require_sim", return_value=MagicMock()))
        async with AsyncClient(transport=ASGITransport(app=fastapi_app), base_url="http://testserver") as client:  # type: ignore[arg-type]
            response = await client.post(
                "/compose/v1/analysis/run",
                json={"simulation_id": 999, "n_seeds": 2, "n_generations": 2},
            )

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_get_analysis_status_returns_the_real_row(fastapi_app: object) -> None:
    fake_db = MagicMock()
    fake_db.get_analysis_db.return_value.get_analysis = AsyncMock(return_value=_analysis(database_id=7))

    with patch("viva_api.api.routers.compose._require_db", return_value=fake_db):
        async with AsyncClient(transport=ASGITransport(app=fastapi_app), base_url="http://testserver") as client:  # type: ignore[arg-type]
            response = await client.get("/compose/v1/analysis/7/status")

    assert response.status_code == 200
    assert response.json()["database_id"] == 7
    fake_db.get_analysis_db.return_value.get_analysis.assert_awaited_once_with(7)


@pytest.mark.asyncio
async def test_get_analysis_status_404s_when_missing(fastapi_app: object) -> None:
    fake_db = MagicMock()
    fake_db.get_analysis_db.return_value.get_analysis = AsyncMock(return_value=None)

    with patch("viva_api.api.routers.compose._require_db", return_value=fake_db):
        async with AsyncClient(transport=ASGITransport(app=fastapi_app), base_url="http://testserver") as client:  # type: ignore[arg-type]
            response = await client.get("/compose/v1/analysis/999/status")

    assert response.status_code == 404
