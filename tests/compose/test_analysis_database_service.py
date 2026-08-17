"""AnalysisORMExecutor: real-Postgres CRUD for the compose_analysis table (item 50 Gap 6).

Run against the testcontainer Postgres (create_all from the ORM) — the first real
DB-backed test suite for the compose subsystem's own database service (prior compose
tests either avoid a live DB entirely or only round-trip enum mappers).
"""

import uuid
from collections.abc import AsyncGenerator
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncEngine

from viva_api.compose.container_def import ContainerizationFileRepr
from viva_api.compose.database_service import ComposeDatabaseService
from viva_api.compose.models import (
    ComposeAnalysisStatus,
    ComposeSimulationRequest,
    SimulationFileType,
)
from viva_api.compose.tables_orm import create_compose_db


@pytest_asyncio.fixture(scope="function")
async def compose_db_service(async_postgres_engine: AsyncEngine) -> AsyncGenerator[ComposeDatabaseService]:
    from sqlalchemy.ext.asyncio import async_sessionmaker

    await create_compose_db(async_postgres_engine)
    session_maker = async_sessionmaker(async_postgres_engine, expire_on_commit=False)
    yield ComposeDatabaseService(session_maker)


@pytest_asyncio.fixture
async def compose_simulation_id(compose_db_service: ComposeDatabaseService) -> int:
    """A real compose_simulation row — compose_analysis.simulation_id is a real FK,
    so every analysis test needs a genuine parent row, not an arbitrary int. The
    singularity def is unique per call: insert_simulator uniqueness-constrains on
    its content hash, and the Postgres testcontainer is module-scoped (data persists
    across tests in this file), so an identical def would collide on the 2nd+ test."""
    simulator = await compose_db_service.get_simulator_db().insert_simulator(
        ContainerizationFileRepr(representation=f"Bootstrap: docker\nFrom: python:3.12\n# {uuid.uuid4()}")
    )
    sim_request = ComposeSimulationRequest(
        request_file_path=Path("/tmp/doc.pbg"),  # noqa: S108
        simulation_file_type=SimulationFileType.PBG,
        is_batch=False,
    )
    simulation = await compose_db_service.get_simulator_db().insert_simulation(
        sim_request=sim_request, experiment_id=f"exp-analysis-test-{uuid.uuid4()}", simulator_version=simulator
    )
    return simulation.database_id


@pytest.mark.asyncio
async def test_insert_and_get_analysis(compose_db_service: ComposeDatabaseService, compose_simulation_id: int) -> None:
    db = compose_db_service.get_analysis_db()
    inserted = await db.insert_analysis(
        name="analysis-exp-1",
        config={"n_seeds": 2, "n_generations": 2, "modules": "applicable"},
        simulation_id=compose_simulation_id,
        job_id_ext="batch-job-1",
        job_backend="ray",
        result_uri="s3://bucket/exp-1/analyses/analysis-exp-1",
    )
    assert inserted.database_id > 0
    assert inserted.status == ComposeAnalysisStatus.COMPUTING
    assert inserted.attempt == 1

    fetched = await db.get_analysis(inserted.database_id)
    assert fetched is not None
    assert fetched.name == "analysis-exp-1"
    assert fetched.simulation_id == compose_simulation_id
    assert fetched.job_id_ext == "batch-job-1"
    assert fetched.config["n_seeds"] == 2
    assert fetched.result_uri == "s3://bucket/exp-1/analyses/analysis-exp-1"


@pytest.mark.asyncio
async def test_get_missing_returns_none(compose_db_service: ComposeDatabaseService) -> None:
    assert await compose_db_service.get_analysis_db().get_analysis(999999) is None


@pytest.mark.asyncio
async def test_list_active_analyses_only_returns_computing_rows_with_a_job_id(
    compose_db_service: ComposeDatabaseService, compose_simulation_id: int
) -> None:
    db = compose_db_service.get_analysis_db()
    active = await db.insert_analysis(
        name="a-active", config={}, simulation_id=compose_simulation_id, job_id_ext="job-active", job_backend="ray"
    )
    no_job = await db.insert_analysis(
        name="a-no-job", config={}, simulation_id=compose_simulation_id, job_id_ext=None, job_backend="ray"
    )
    ready = await db.insert_analysis(
        name="a-ready", config={}, simulation_id=compose_simulation_id, job_id_ext="job-ready", job_backend="ray"
    )
    await db.update_analysis_status(ready.database_id, ComposeAnalysisStatus.READY)

    # The Postgres testcontainer is module-scoped (data persists across tests in this
    # file), so other tests' own COMPUTING rows may legitimately still be present —
    # assert membership/exclusion for THIS test's own rows, not exact list equality.
    rows_by_id = {a.database_id: a for a in await db.list_active_analyses()}
    assert active.database_id in rows_by_id
    assert rows_by_id[active.database_id].job_id_ext == "job-active"
    assert no_job.database_id not in rows_by_id  # no job_id_ext -> never "active"
    assert ready.database_id not in rows_by_id  # READY, not COMPUTING -> not active


@pytest.mark.asyncio
async def test_update_analysis_status_to_ready_with_result_uri(
    compose_db_service: ComposeDatabaseService, compose_simulation_id: int
) -> None:
    db = compose_db_service.get_analysis_db()
    inserted = await db.insert_analysis(
        name="a1", config={}, simulation_id=compose_simulation_id, job_id_ext="job-1", job_backend="ray"
    )
    await db.update_analysis_status(inserted.database_id, ComposeAnalysisStatus.READY, result_uri="s3://bucket/final")
    fetched = await db.get_analysis(inserted.database_id)
    assert fetched is not None
    assert fetched.status == ComposeAnalysisStatus.READY
    assert fetched.result_uri == "s3://bucket/final"


@pytest.mark.asyncio
async def test_update_analysis_status_to_failed_with_error_message(
    compose_db_service: ComposeDatabaseService, compose_simulation_id: int
) -> None:
    db = compose_db_service.get_analysis_db()
    inserted = await db.insert_analysis(
        name="a1", config={}, simulation_id=compose_simulation_id, job_id_ext="job-1", job_backend="ray"
    )
    await db.update_analysis_status(
        inserted.database_id, ComposeAnalysisStatus.FAILED, error_message="OOM: retries exhausted"
    )
    fetched = await db.get_analysis(inserted.database_id)
    assert fetched is not None
    assert fetched.status == ComposeAnalysisStatus.FAILED
    assert fetched.error_message == "OOM: retries exhausted"


@pytest.mark.asyncio
async def test_update_analysis_status_on_missing_row_raises(compose_db_service: ComposeDatabaseService) -> None:
    with pytest.raises(RuntimeError, match="ComposeAnalysis 999999 not found"):
        await compose_db_service.get_analysis_db().update_analysis_status(999999, ComposeAnalysisStatus.READY)


@pytest.mark.asyncio
async def test_update_analysis_job_id_bumps_attempt_and_swaps_the_physical_job(
    compose_db_service: ComposeDatabaseService, compose_simulation_id: int
) -> None:
    """Same logical row, new physical job id per retry attempt — mirrors the legacy
    update_analysis_job_id (PR #239)."""
    db = compose_db_service.get_analysis_db()
    inserted = await db.insert_analysis(
        name="retry-me",
        config={},
        simulation_id=compose_simulation_id,
        job_id_ext="job-attempt1",
        job_backend="ray",
    )
    await db.update_analysis_job_id(inserted.database_id, job_id_ext="job-attempt2", attempt=2)

    rows = await db.list_active_analyses()
    (refetched,) = [a for a in rows if a.database_id == inserted.database_id]
    assert refetched.job_id_ext == "job-attempt2"
    assert refetched.attempt == 2


@pytest.mark.asyncio
async def test_update_analysis_job_id_on_missing_row_raises(compose_db_service: ComposeDatabaseService) -> None:
    with pytest.raises(RuntimeError, match="ComposeAnalysis 999999 not found"):
        await compose_db_service.get_analysis_db().update_analysis_job_id(999999, job_id_ext="x", attempt=2)
