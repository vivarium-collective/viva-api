"""Integration tests for BioModels REST endpoints via FastAPI TestClient."""

from __future__ import annotations

from contextlib import AbstractContextManager
from types import TracebackType
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from viva_api.compose.models import (
    ComposeSimulationExperiment,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _fake_experiment(sim_id: int = 1, sim_ver_id: int = 1) -> ComposeSimulationExperiment:
    return ComposeSimulationExperiment(simulation_database_id=sim_id, simulator_database_id=sim_ver_id)


def _patch_biomodels_run_compose(experiment: ComposeSimulationExperiment) -> MagicMock:
    """Return a patch context manager that stubs run_compose_curated and the three _require_* guards."""
    from contextlib import ExitStack
    from unittest.mock import MagicMock as _MM
    from unittest.mock import patch as _patch

    mock_run = AsyncMock(return_value=experiment)
    mock_db = _MM()
    mock_sim = _MM()
    mock_monitor = _MM()

    class _MultiPatch:
        def __enter__(self) -> _MultiPatch:
            self._stack = ExitStack()
            self._stack.enter_context(_patch("viva_api.api.routers.compose.run_compose_curated", mock_run))
            self._stack.enter_context(_patch("viva_api.api.routers.compose._require_db", return_value=mock_db))
            self._stack.enter_context(_patch("viva_api.api.routers.compose._require_sim", return_value=mock_sim))
            self._stack.enter_context(
                _patch("viva_api.api.routers.compose._require_monitor", return_value=mock_monitor)
            )
            return self

        def __exit__(
            self,
            exc_type: type[BaseException] | None,
            exc_val: BaseException | None,
            exc_tb: TracebackType | None,
        ) -> None:
            self._stack.__exit__(exc_type, exc_val, exc_tb)

    return _MultiPatch()  # type: ignore[return-value]


def _patch_biomodels_identifiers(ids: list[str]) -> AbstractContextManager[Any]:
    return patch("viva_api.compose.biomodels_service.BiomodelsService.get_identifiers", return_value=ids)


def _patch_biomodels_metadata(meta: dict[str, Any]) -> AbstractContextManager[Any]:
    return patch("viva_api.compose.biomodels_service.BiomodelsService.get_metadata", return_value=meta)


def _patch_biomodels_metadata_error(exc: Exception) -> AbstractContextManager[Any]:
    return patch("viva_api.compose.biomodels_service.BiomodelsService.get_metadata", side_effect=exc)


def _patch_load_biomodel(biomodel_id: str = "BIOMD001") -> AbstractContextManager[Any]:
    from viva_api.compose.biomodels_service import BiomodelLoadResult, UniformTimeCourseSpec

    utc = UniformTimeCourseSpec(0.0, 0.0, 10.0, 100)
    result = BiomodelLoadResult(
        biomodel_id=biomodel_id,
        sbml_path="/tmp/test.sbml",  # noqa: S108
        sedml_path="/tmp/test.sedml",  # noqa: S108
        utc=utc,
    )
    return patch("viva_api.compose.biomodels_service.BiomodelsService.load_biomodel", return_value=result)


def _patch_load_biomodel_error(exc: Exception) -> AbstractContextManager[Any]:
    return patch("viva_api.compose.biomodels_service.BiomodelsService.load_biomodel", side_effect=exc)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_biomodels_identifiers(fastapi_app: object) -> None:
    mock_ids = ["BIOMD001", "BIOMD002"]
    with _patch_biomodels_identifiers(mock_ids):
        async with AsyncClient(transport=ASGITransport(app=fastapi_app), base_url="http://testserver") as client:  # type: ignore[arg-type]
            response = await client.get("/compose/v1/biomodels/identifiers", params={"n": 2})
    assert response.status_code == 200
    assert response.json() == mock_ids


@pytest.mark.asyncio
async def test_get_biomodel_metadata(fastapi_app: object) -> None:
    meta = {"name": "Hodgkin-Huxley", "format": "SBML"}
    with _patch_biomodels_metadata(meta):
        async with AsyncClient(transport=ASGITransport(app=fastapi_app), base_url="http://testserver") as client:  # type: ignore[arg-type]
            response = await client.get("/compose/v1/biomodels/BIOMD001/metadata")
    assert response.status_code == 200
    data = response.json()
    assert data["biomodel_id"] == "BIOMD001"
    assert data["metadata"]["name"] == "Hodgkin-Huxley"


@pytest.mark.asyncio
async def test_get_biomodel_metadata_not_found(fastapi_app: object) -> None:
    with _patch_biomodels_metadata_error(ValueError("not found")):
        async with AsyncClient(transport=ASGITransport(app=fastapi_app), base_url="http://testserver") as client:  # type: ignore[arg-type]
            response = await client.get("/compose/v1/biomodels/INVALID/metadata")
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_run_biomodel_copasi(fastapi_app: object) -> None:
    experiment = _fake_experiment(sim_id=10)
    with _patch_load_biomodel(), _patch_biomodels_run_compose(experiment):
        async with AsyncClient(transport=ASGITransport(app=fastapi_app), base_url="http://testserver") as client:  # type: ignore[arg-type]
            response = await client.post("/compose/v1/biomodels/BIOMD001/run", params={"simulator": "copasi"})
    assert response.status_code == 200
    assert response.json()["simulation_database_id"] == 10


@pytest.mark.asyncio
async def test_run_biomodel_tellurium(fastapi_app: object) -> None:
    experiment = _fake_experiment(sim_id=20)
    with _patch_load_biomodel(), _patch_biomodels_run_compose(experiment):
        async with AsyncClient(transport=ASGITransport(app=fastapi_app), base_url="http://testserver") as client:  # type: ignore[arg-type]
            response = await client.post("/compose/v1/biomodels/BIOMD001/run", params={"simulator": "tellurium"})
    assert response.status_code == 200
    assert response.json()["simulation_database_id"] == 20


@pytest.mark.asyncio
async def test_run_biomodel_load_failure(fastapi_app: object) -> None:
    with _patch_load_biomodel_error(RuntimeError("EBI down")):
        async with AsyncClient(transport=ASGITransport(app=fastapi_app), base_url="http://testserver") as client:  # type: ignore[arg-type]
            response = await client.post("/compose/v1/biomodels/BIOMD001/run")
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_run_biomodels_batch(fastapi_app: object) -> None:
    experiment = _fake_experiment(sim_id=5)
    mock_ids = ["BIOMD001", "BIOMD002"]

    with _patch_load_biomodel(), _patch_biomodels_run_compose(experiment), _patch_biomodels_identifiers(mock_ids):
        async with AsyncClient(transport=ASGITransport(app=fastapi_app), base_url="http://testserver") as client:  # type: ignore[arg-type]
            response = await client.post(
                "/compose/v1/biomodels/batch",
                json={"n_models": 2, "simulator": "copasi"},
            )
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data["submitted"], list)
    assert isinstance(data["failed"], list)


@pytest.mark.asyncio
async def test_audit_biomodel(fastapi_app: object) -> None:
    experiment = _fake_experiment(sim_id=99)
    with _patch_load_biomodel(), _patch_biomodels_run_compose(experiment):
        async with AsyncClient(transport=ASGITransport(app=fastapi_app), base_url="http://testserver") as client:  # type: ignore[arg-type]
            response = await client.post(
                "/compose/v1/biomodels/BIOMD001/audit",
                params={"simulators": ["copasi", "tellurium"]},
            )
    assert response.status_code == 200
    data = response.json()
    assert data["experiment"]["simulation_database_id"] == 99
    assert "copasi" in data["simulators_used"]
    assert "tellurium" in data["simulators_used"]


@pytest.mark.asyncio
async def test_audit_biomodel_load_failure(fastapi_app: object) -> None:
    with _patch_load_biomodel_error(ValueError("bad")):
        async with AsyncClient(transport=ASGITransport(app=fastapi_app), base_url="http://testserver") as client:  # type: ignore[arg-type]
            response = await client.post("/compose/v1/biomodels/BIOMD001/audit")
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_regression_endpoint(fastapi_app: object) -> None:
    experiment = _fake_experiment(sim_id=7)
    mock_ids = ["BIOMD001", "BIOMD002"]

    with _patch_load_biomodel(), _patch_biomodels_run_compose(experiment), _patch_biomodels_identifiers(mock_ids):
        async with AsyncClient(transport=ASGITransport(app=fastapi_app), base_url="http://testserver") as client:  # type: ignore[arg-type]
            response = await client.post(
                "/compose/v1/biomodels/regression",
                json={"n_models": 2, "simulators": ["copasi", "tellurium"]},
            )
    assert response.status_code == 200
    data = response.json()
    assert data["total_requested"] == 2
    assert isinstance(data["submitted"], list)
    assert isinstance(data["failed"], list)
