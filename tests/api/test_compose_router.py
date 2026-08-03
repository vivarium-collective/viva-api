"""Tests for the generic compose /simulation/run endpoint."""

from __future__ import annotations

from contextlib import ExitStack
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient

from sms_api.compose.models import ComposeSimulationExperiment


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
        stack.enter_context(patch("sms_api.api.routers.compose.run_compose_simulation", _fake_run))
        stack.enter_context(patch("sms_api.api.routers.compose._require_db", return_value=fake_db))
        stack.enter_context(patch("sms_api.api.routers.compose._require_sim", return_value=MagicMock()))
        stack.enter_context(patch("sms_api.api.routers.compose._require_monitor", return_value=MagicMock()))
        async with AsyncClient(transport=ASGITransport(app=fastapi_app), base_url="http://testserver") as client:  # type: ignore[arg-type]
            response = await client.post(
                "/compose/v1/simulation/run",
                params={"extra_pip_deps": ["git+https://github.com/x/y.git@abc", "cobra"]},
                files={"uploaded_file": ("m.pbg", b'{"state": {}}', "application/json")},
            )

    assert response.status_code == 200
    assert captured["extra_pip_deps"] == ["git+https://github.com/x/y.git@abc", "cobra"]
