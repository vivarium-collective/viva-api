"""Tests for the generic compose /simulation/run endpoint."""

from __future__ import annotations

import types
from contextlib import ExitStack
from typing import Any
from unittest.mock import MagicMock

import pytest
from httpx import ASGITransport, AsyncClient

import viva_core.api.routers.compose as compose_router
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


@pytest.fixture
def compose_services(monkeypatch: pytest.MonkeyPatch) -> Any:
    """Install a container whose compose services the test controls (P3e: the router asks the
    container of the moment, not module globals). Returns a setter: ``set(sim=..., monitor=...)``."""
    from viva_core import container as container_mod
    from viva_core.container import ComposeServices, CoreContainer

    held: dict[str, Any] = {}

    def _set(**fields: Any) -> None:
        base = {"db": MagicMock(), "sim": MagicMock(name="default_service"), "monitor": MagicMock()}
        base.update(fields)
        held["container"] = CoreContainer(settings=MagicMock(), compose=ComposeServices(**base))

    _set()
    monkeypatch.setattr(container_mod, "_provider", lambda: held["container"])
    return _set


# --- item 98: compute_backend -- per-request selection among the registered
# ComposeSimulationService backends (_require_sim's own resolution logic) ---


def test_require_sim_with_no_backend_returns_the_default(compose_services: Any) -> None:
    """None (the vast majority of requests -- this field is optional) preserves
    today's exact behavior: the deployment's single default service, no registry
    lookup at all."""
    default = MagicMock(name="default_service")
    compose_services(sim=default)
    assert compose_router._require_sim(None) is default


def test_require_sim_resolves_the_explicitly_requested_registered_backend(
    compose_services: Any,
) -> None:
    default = MagicMock(name="default_service")
    ray_service = MagicMock(name="ray_service")
    compose_services(sim=default, monitor=types.SimpleNamespace(sim_registry={ComputeBackend.RAY: ray_service}))
    # Explicitly asking for RAY must return the RAY service, not the default --
    # even when RAY also happens to BE the default, this proves the registry
    # path was actually taken rather than the None-shortcut above.
    assert compose_router._require_sim(ComputeBackend.RAY) is ray_service
    assert compose_router._require_sim(ComputeBackend.RAY) is not default


def test_require_sim_fails_loud_when_requested_backend_is_not_registered(
    compose_services: Any,
) -> None:
    """Regression target, named directly in the source comment: a caller who
    explicitly asks for a backend must never silently get a different one back
    (viva-api#353's own "looked successful, ran the wrong thing" class of bug)."""
    from fastapi import HTTPException

    compose_services(monitor=types.SimpleNamespace(sim_registry={ComputeBackend.RAY: MagicMock()}))
    with pytest.raises(HTTPException) as exc_info:
        compose_router._require_sim(ComputeBackend.SLURM)
    assert exc_info.value.status_code == 400
    assert "slurm" in str(exc_info.value.detail)


def test_require_sim_fails_loud_with_no_monitor_at_all(compose_services: Any) -> None:
    """Same fail-loud contract when the monitor itself was never initialized --
    an empty registry, not a crash or a silent default substitution."""
    from fastapi import HTTPException

    compose_services(monitor=types.SimpleNamespace(sim_registry={}))
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


# --- T5a: GET /compose/v1/simulation/{id}/results is backend-aware ---
#
# Ray/Batch compose sims write straight to S3 (no zip ever exists); SLURM
# compose sims still SSH/SCP a zip off the HPC filesystem. The route must
# branch on the ComposeHpcRun.job_backend, the same way get_simulation_status
# already resolves it, and leave the SLURM branch's behavior unchanged.


@pytest.mark.asyncio
async def test_get_results_ray_backend_streams_s3_tar_gz(fastapi_app: object) -> None:
    """A compose sim whose HpcRun.job_backend == 'ray' must take the S3
    streaming branch (never SSH/SCP) and produce a valid .tar.gz built from
    every object under RayLayout.experiment_prefix(experiment_id) -- the sim
    output AND a chained analysis manifest live under that same prefix
    (compose-results-land-findings.md §4), so streaming "everything under the
    prefix" is required to capture both in one dispatch."""
    import io
    import tarfile
    from datetime import UTC, datetime
    from unittest.mock import AsyncMock, patch

    from viva_api.common.models import JobBackend
    from viva_api.common.storage import data_layout
    from viva_api.common.storage.file_service import FileService, ListingItem
    from viva_api.compose.models import ComposeHpcRun, ComposeJobType
    from viva_api.dependencies import get_file_service, set_file_service

    experiment_id = "ray-exp-t5a"
    prefix = data_layout.RayLayout.experiment_prefix(experiment_id)
    now = datetime.now(UTC)
    listing = [
        ListingItem(Key=f"{prefix}/v2ecoli_seed00.zarr/.zattrs", LastModified=now, ETag="a", Size=3),
        ListingItem(Key=f"{prefix}/summary.json", LastModified=now, ETag="b", Size=3),
        # the chained analysis job's manifest -- lives under the SAME prefix
        ListingItem(Key=f"{prefix}/analyses/ptools/_manifest.json", LastModified=now, ETag="c", Size=3),
    ]
    contents = {item.Key: f"{item.Key}-content".encode() for item in listing}

    class _FakeFileService(FileService):
        """Minimal in-memory FileService stub, mirroring the pattern in
        tests/common/handlers/test_simulations_handler.py's _FakeFileService."""

        async def download_file(self, s3_path: Any, file_path: Any = None) -> Any:  # pragma: no cover
            raise NotImplementedError

        async def upload_file(self, file_path: Any, s3_path: Any) -> Any:  # pragma: no cover
            raise NotImplementedError

        async def upload_bytes(self, file_contents: Any, s3_path: Any) -> Any:  # pragma: no cover
            raise NotImplementedError

        async def get_modified_date(self, s3_path: Any) -> Any:  # pragma: no cover
            raise NotImplementedError

        async def get_listing(self, s3_path: Any) -> list[ListingItem]:
            key_prefix = str(s3_path.s3_path)
            if not key_prefix.endswith("/"):
                key_prefix += "/"
            return [item for item in listing if item.Key.startswith(key_prefix)]

        async def get_file_contents(self, s3_path: Any) -> bytes | None:
            return contents.get(str(s3_path.s3_path))

        async def delete_file(self, s3_path: Any) -> None:  # pragma: no cover
            pass

        async def close(self) -> None:
            pass

    saved_file_service = get_file_service()
    set_file_service(_FakeFileService())
    # The router's file service is the container's (P3e); the test provides one with the same fake.
    from viva_core import container as container_mod
    from viva_core.container import ComposeServices, CoreContainer

    held = CoreContainer(
        settings=MagicMock(),
        compose=ComposeServices(db=MagicMock(), sim=MagicMock(), monitor=MagicMock(), files=_FakeFileService()),
    )
    saved_provider = container_mod._provider
    container_mod._provider = lambda: held

    fake_db = MagicMock()
    fake_hpc_run = ComposeHpcRun(
        database_id=1,
        slurmjobid=0,
        job_id_ext="batch-job-1",
        job_backend=JobBackend.RAY.value,
        correlation_id="corr-ray-1",
        job_type=ComposeJobType.SIMULATION,
        sim_id=99,
        simulator_id=1,
    )
    fake_db.get_hpc_db.return_value.get_hpcrun_by_ref = AsyncMock(return_value=fake_hpc_run)
    fake_db.get_simulator_db.return_value.get_simulations_experiment_id = AsyncMock(return_value=experiment_id)

    try:
        with patch("viva_api.api.routers.compose._require_db", return_value=fake_db):
            async with AsyncClient(transport=ASGITransport(app=fastapi_app), base_url="http://testserver") as client:  # type: ignore[arg-type]
                response = await client.get("/compose/v1/simulation/99/results")
    finally:
        set_file_service(saved_file_service)
        container_mod._provider = saved_provider

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/gzip"
    assert response.headers["content-disposition"] == f'attachment; filename="{experiment_id}.tar.gz"'

    with tarfile.open(fileobj=io.BytesIO(response.content), mode="r:gz") as tar:
        members = tar.getnames()

    assert any(m.endswith("_manifest.json") for m in members), members
    assert any(m.endswith("summary.json") for m in members), members
    assert any(m.endswith(".zattrs") for m in members), members


@pytest.mark.asyncio
async def test_get_results_slurm_backend_keeps_the_ssh_scp_zip_branch(
    fastapi_app: object, tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A job_backend='slurm' compose sim must keep taking the pre-existing
    SSH/SCP zip branch, unchanged by T5a -- proves the new Ray/Batch branch is
    additive, not a replacement."""
    from unittest.mock import AsyncMock, patch

    from viva_api.compose.models import ComposeHpcRun, ComposeJobType

    experiment_id = "slurm-exp-t5a"

    fake_db = MagicMock()
    fake_hpc_run = ComposeHpcRun(
        database_id=2,
        slurmjobid=123,
        job_backend="slurm",
        correlation_id="corr-slurm-1",
        job_type=ComposeJobType.SIMULATION,
        sim_id=42,
        simulator_id=1,
    )
    fake_db.get_hpc_db.return_value.get_hpcrun_by_ref = AsyncMock(return_value=fake_hpc_run)
    fake_db.get_simulator_db.return_value.get_simulations_experiment_id = AsyncMock(return_value=experiment_id)

    # The SLURM branch's local disk cache is hardcoded to /app/.results_cache
    # (a path that only exists inside the deployed container) -- redirect just
    # that one literal to a pytest tmp_path so this test can run locally
    # without touching that unrelated, pre-existing hardcoding.
    # The download is the SLURM backend's own (``results_archive``, P3d-4d-2): the route asks the
    # backend registered for SLURM and serves what it hands back; how it fetched is not the route's.
    zip_path = tmp_path / f"{experiment_id}_results.zip"
    zip_path.write_bytes(b"fake-zip-bytes")
    fake_slurm_backend = MagicMock()
    fake_slurm_backend.results_archive = AsyncMock(return_value=zip_path)

    with (
        patch("viva_api.api.routers.compose._require_db", return_value=fake_db),
        patch("viva_api.api.routers.compose._require_sim", return_value=fake_slurm_backend) as require_sim,
    ):
        async with AsyncClient(transport=ASGITransport(app=fastapi_app), base_url="http://testserver") as client:  # type: ignore[arg-type]
            response = await client.get("/compose/v1/simulation/42/results")

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/zip"
    assert response.content == b"fake-zip-bytes"
    require_sim.assert_called_once_with(ComputeBackend.SLURM)
    fake_slurm_backend.results_archive.assert_awaited_once_with(experiment_id)
