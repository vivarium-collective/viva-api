from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import cast

import pytest
from httpx import ASGITransport, AsyncClient

from viva_api.api.main import app
from viva_api.dependencies import get_database_service, set_database_service
from viva_api.simulation.database_service import DatabaseService
from viva_api.simulation.models import Simulation, SimulationConfig, SimulatorVersion
from viva_api.simulation.observable_reader import ObservableInfo, StoreIndex

BASE = "/api/v1"

# v2ecoli repo => compute_backend_for_repo => RAY (observables are Ray-only).
_RAY_REPO = "https://github.com/vivarium-collective/v2ecoli"
_VECOLI_REPO = "https://github.com/CovertLab/vEcoli"


class _FakeDB:
    def __init__(self, sim: Simulation | None, repo_url: str = _RAY_REPO) -> None:
        self._sim = sim
        self._repo_url = repo_url

    async def get_simulation(self, simulation_id: int) -> Simulation | None:
        return self._sim

    async def get_simulator(self, simulator_id: int) -> SimulatorVersion:
        return SimulatorVersion(
            database_id=simulator_id, git_commit_hash="abc1234", git_repo_url=self._repo_url, git_branch="master"
        )


def _sim(experiment_id: str = "exp-abc") -> Simulation:
    # Construct a minimal valid Simulation with all required fields.
    return Simulation(
        database_id=49,
        simulator_id=1,
        parca_dataset_id=1,
        experiment_id=experiment_id,
        simulation_config_filename="api_simulation_default.json",
        config=SimulationConfig(experiment_id=experiment_id),
    )


@asynccontextmanager
async def _client() -> AsyncGenerator[AsyncClient]:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as c:
        yield c


@pytest.mark.asyncio
async def test_observables_index_ok(monkeypatch: pytest.MonkeyPatch) -> None:
    saved = get_database_service()
    set_database_service(cast(DatabaseService, _FakeDB(_sim())))
    monkeypatch.setattr(
        "viva_api.simulation.observable_reader.list_observables",
        lambda uri: StoreIndex(store="zarr", observables=[ObservableInfo("mass", ["time"], [3])]),
    )
    try:
        async with _client() as c:
            r = await c.get(f"{BASE}/simulations/49/observables/index")
        assert r.status_code == 200
        body = r.json()
        assert body["experiment_id"] == "exp-abc"
        assert body["store"] == "zarr"
        assert body["observables"][0]["name"] == "mass"
    finally:
        set_database_service(saved)


@pytest.mark.asyncio
async def test_observables_index_404_when_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    saved = get_database_service()
    set_database_service(cast(DatabaseService, _FakeDB(None)))
    try:
        async with _client() as c:
            r = await c.get(f"{BASE}/simulations/999/observables/index")
        assert r.status_code == 404
    finally:
        set_database_service(saved)


@pytest.mark.asyncio
async def test_observables_409_for_non_ray_run() -> None:
    """Observables are Ray-only; a vEcoli/Nextflow run fails loudly (409), not 404."""
    saved = get_database_service()
    set_database_service(cast(DatabaseService, _FakeDB(_sim(), repo_url=_VECOLI_REPO)))
    try:
        async with _client() as c:
            r = await c.get(f"{BASE}/simulations/49/observables/index")
        assert r.status_code == 409
        assert "v2ecoli (Ray)" in r.json()["detail"]
    finally:
        set_database_service(saved)


@pytest.mark.asyncio
async def test_observables_series_ok(monkeypatch: pytest.MonkeyPatch) -> None:
    saved = get_database_service()
    set_database_service(cast(DatabaseService, _FakeDB(_sim())))
    monkeypatch.setattr(
        "viva_api.simulation.observable_reader.read_observables",
        lambda uri, names, *, stride=1, max_points=None: ("zarr", [0.0, 1.0, 2.0], {"mass": [1.0, 2.0, 3.0]}),
    )
    try:
        async with _client() as c:
            r = await c.get(f"{BASE}/simulations/49/observables", params={"names": "mass"})
        assert r.status_code == 200
        body = r.json()
        assert body["store"] == "zarr"
        assert body["time"] == [0.0, 1.0, 2.0]
        assert body["series"]["mass"] == [1.0, 2.0, 3.0]
    finally:
        set_database_service(saved)


@pytest.mark.asyncio
async def test_observables_series_forwards_decimation(monkeypatch: pytest.MonkeyPatch) -> None:
    """`stride` / `max_points` query params are forwarded to the reader."""
    saved = get_database_service()
    set_database_service(cast(DatabaseService, _FakeDB(_sim())))
    captured: dict[str, object] = {}

    def _capture(uri: str, names: list[str], *, stride: int = 1, max_points: int | None = None) -> tuple:  # type: ignore[type-arg]
        captured["stride"] = stride
        captured["max_points"] = max_points
        return "zarr", [0.0, 2.0], {"mass": [1.0, 3.0]}

    monkeypatch.setattr("viva_api.simulation.observable_reader.read_observables", _capture)
    try:
        async with _client() as c:
            r = await c.get(
                f"{BASE}/simulations/49/observables", params={"names": "mass", "stride": 2, "max_points": 100}
            )
        assert r.status_code == 200
        assert captured == {"stride": 2, "max_points": 100}
    finally:
        set_database_service(saved)


@pytest.mark.asyncio
async def test_observables_series_bad_name_400(monkeypatch: pytest.MonkeyPatch) -> None:
    saved = get_database_service()
    set_database_service(cast(DatabaseService, _FakeDB(_sim())))

    def _raise(uri: str, names: list[str], *, stride: int = 1, max_points: int | None = None) -> None:
        raise KeyError("observables not in store: ['nope']")

    monkeypatch.setattr("viva_api.simulation.observable_reader.read_observables", _raise)
    try:
        async with _client() as c:
            r = await c.get(f"{BASE}/simulations/49/observables", params={"names": "nope"})
        assert r.status_code == 400
    finally:
        set_database_service(saved)


@pytest.mark.asyncio
async def test_observables_series_multidim_400(monkeypatch: pytest.MonkeyPatch) -> None:
    """A multi-dimensional observable (ValueError from reader) must return HTTP 400."""
    saved = get_database_service()
    set_database_service(cast(DatabaseService, _FakeDB(_sim())))

    def _raise(uri: str, names: list[str], *, stride: int = 1, max_points: int | None = None) -> None:
        raise ValueError(
            "observable 'bulk' is not a 1-D timeseries (shape (3, 5)); multi-dimensional observables are not supported"
        )

    monkeypatch.setattr("viva_api.simulation.observable_reader.read_observables", _raise)
    try:
        async with _client() as c:
            r = await c.get(f"{BASE}/simulations/49/observables", params={"names": "bulk"})
        assert r.status_code == 400
        assert "1-D timeseries" in r.json()["detail"]
    finally:
        set_database_service(saved)
