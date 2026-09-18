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


# The four former endpoints (single run / batch / audit / regression) are now one
# POST /compose/v1/biomodels/run — cardinality via model_ids/n_models, and one vs
# many `simulators` selects single-run vs cross-validation.


@pytest.mark.asyncio
async def test_run_single_copasi(fastapi_app: object) -> None:
    experiment = _fake_experiment(sim_id=10)
    with _patch_load_biomodel(), _patch_biomodels_run_compose(experiment):
        async with AsyncClient(transport=ASGITransport(app=fastapi_app), base_url="http://testserver") as client:  # type: ignore[arg-type]
            response = await client.post(
                "/compose/v1/biomodels/run",
                json={"model_ids": ["BIOMD001"], "simulators": ["copasi"]},
            )
    assert response.status_code == 200
    data = response.json()
    assert data["total_requested"] == 1
    assert data["submitted"][0]["simulation_database_id"] == 10
    assert data["failed"] == []


@pytest.mark.asyncio
async def test_run_single_tellurium(fastapi_app: object) -> None:
    experiment = _fake_experiment(sim_id=20)
    with _patch_load_biomodel(), _patch_biomodels_run_compose(experiment):
        async with AsyncClient(transport=ASGITransport(app=fastapi_app), base_url="http://testserver") as client:  # type: ignore[arg-type]
            response = await client.post(
                "/compose/v1/biomodels/run",
                json={"model_ids": ["BIOMD001"], "simulators": ["tellurium"]},
            )
    assert response.status_code == 200
    assert response.json()["submitted"][0]["simulation_database_id"] == 20


@pytest.mark.asyncio
async def test_run_load_failure_is_reported_not_raised(fastapi_app: object) -> None:
    # A model that fails to load is collected in `failed` (200), not a 400 — the
    # unified endpoint cannot abort a batch on one bad model.
    with _patch_load_biomodel_error(RuntimeError("EBI down")):
        async with AsyncClient(transport=ASGITransport(app=fastapi_app), base_url="http://testserver") as client:  # type: ignore[arg-type]
            response = await client.post(
                "/compose/v1/biomodels/run",
                json={"model_ids": ["BIOMD001"]},
            )
    assert response.status_code == 200
    data = response.json()
    assert data["submitted"] == []
    assert data["failed"] == ["BIOMD001"]
    assert data["total_requested"] == 1


@pytest.mark.asyncio
async def test_run_batch_by_n_models(fastapi_app: object) -> None:
    experiment = _fake_experiment(sim_id=5)
    mock_ids = ["BIOMD001", "BIOMD002"]

    with _patch_load_biomodel(), _patch_biomodels_run_compose(experiment), _patch_biomodels_identifiers(mock_ids):
        async with AsyncClient(transport=ASGITransport(app=fastapi_app), base_url="http://testserver") as client:  # type: ignore[arg-type]
            response = await client.post(
                "/compose/v1/biomodels/run",
                json={"n_models": 2, "simulators": ["copasi"]},
            )
    assert response.status_code == 200
    data = response.json()
    assert data["total_requested"] == 2
    assert len(data["submitted"]) == 2
    assert data["failed"] == []


@pytest.mark.asyncio
async def test_run_cross_validation_multi_simulator(fastapi_app: object) -> None:
    # The former "audit": one model, several simulators wired into one document.
    experiment = _fake_experiment(sim_id=99)
    with _patch_load_biomodel(), _patch_biomodels_run_compose(experiment):
        async with AsyncClient(transport=ASGITransport(app=fastapi_app), base_url="http://testserver") as client:  # type: ignore[arg-type]
            response = await client.post(
                "/compose/v1/biomodels/run",
                json={"model_ids": ["BIOMD001"], "simulators": ["copasi", "tellurium"]},
            )
    assert response.status_code == 200
    data = response.json()
    assert data["total_requested"] == 1
    assert len(data["submitted"]) == 1
    assert data["submitted"][0]["simulation_database_id"] == 99


@pytest.mark.asyncio
async def test_run_regression_multi_model_multi_simulator(fastapi_app: object) -> None:
    # The former "regression": N models, several simulators each.
    experiment = _fake_experiment(sim_id=7)
    mock_ids = ["BIOMD001", "BIOMD002"]

    with _patch_load_biomodel(), _patch_biomodels_run_compose(experiment), _patch_biomodels_identifiers(mock_ids):
        async with AsyncClient(transport=ASGITransport(app=fastapi_app), base_url="http://testserver") as client:  # type: ignore[arg-type]
            response = await client.post(
                "/compose/v1/biomodels/run",
                json={"n_models": 2, "simulators": ["copasi", "tellurium"]},
            )
    assert response.status_code == 200
    data = response.json()
    assert data["total_requested"] == 2
    assert len(data["submitted"]) == 2
    assert data["failed"] == []
