"""Standalone analysis runs as traced runs, scheduler side (data provenance slice 1).

A standalone analysis (``POST /simulations/{id}/analysis``) now has its own
``JobType.ANALYSIS`` HpcRun. These tests pin the three things the scheduler must do
with it:

* list it as an event-ingest candidate (a K8s Job, which the SIMULATION rule never
  admitted -- and still does not admit for simulations);
* hand the ingester the simulation the analysis read, found through the analysis
  record rather than through the run's own reference id;
* end the run when its analysis record resolves, so the ingest grace window closes.

Runs against the testcontainer Postgres; the S3 read and the status resolver are
patched. Nothing reaches a hosted service.
"""

import datetime
import uuid
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tests.simulation.test_event_ingest import _insert_run
from viva_api.analysis.models import AnalysisRun, ExperimentAnalysisDTO
from viva_api.common.models import JobId, JobStatus
from viva_api.simulation.database_service import DatabaseServiceSQL
from viva_api.simulation.event_ingest import IngestResult
from viva_api.simulation.job_scheduler import JobScheduler
from viva_api.simulation.models import HpcRun, JobType, Simulation
from viva_api.simulation.tables_orm import AnalysisStatusDB


async def _analysis_run(db: DatabaseServiceSQL, simulation: Simulation) -> tuple[int, int]:
    """An analysis record plus its ANALYSIS HpcRun, shaped like the standalone handler's."""
    record = await db.record_analysis(
        experiment_id=simulation.experiment_id,
        n_tp=None,
        status=AnalysisStatusDB.COMPUTING,
        config={"analysis_options": {"experiment_id": [simulation.experiment_id]}},
        name=f"analysis-{uuid.uuid4().hex[:8]}",
        simulation_id=simulation.database_id,
        backend="ray",
        result_uri=f"s3://bucket/vecoli-output/{simulation.experiment_id}/analyses/a",
    )
    run = await db.insert_hpcrun(
        job_id=JobId.k8s(f"ana-{record.database_id}"),
        job_type=JobType.ANALYSIS,
        ref_id=record.database_id,
        correlation_id=f"analysis-{record.database_id}",
    )
    return record.database_id, run.database_id


def _scheduler(db: DatabaseServiceSQL) -> JobScheduler:
    return JobScheduler(messaging_service=MagicMock(), database_service=db)


@pytest.mark.asyncio
async def test_an_active_analysis_run_is_an_ingest_candidate_but_a_k8s_simulation_is_not(
    database_service: DatabaseServiceSQL,
) -> None:
    simulation, simulation_run = await _insert_run(database_service, correlation_id=f"sim-{uuid.uuid4().hex[:8]}")
    _analysis_id, analysis_run_id = await _analysis_run(database_service, simulation)
    plain_k8s_simulation = await database_service.insert_hpcrun(
        job_id=JobId.k8s(f"k8s-{uuid.uuid4().hex[:8]}"),
        job_type=JobType.SIMULATION,
        ref_id=simulation.database_id,
        correlation_id=f"k8s-{uuid.uuid4().hex[:8]}",
    )

    since = datetime.datetime.now() - datetime.timedelta(minutes=15)
    ids = {run.database_id for run in await database_service.list_hpcruns_for_event_ingest(since)}

    assert analysis_run_id in ids
    assert simulation_run.database_id in ids  # unchanged: a Nextflow simulation still qualifies
    assert plain_k8s_simulation.database_id not in ids  # unchanged: the widening is ANALYSIS-only


@pytest.mark.asyncio
async def test_the_ingest_tick_hands_an_analysis_run_the_simulation_it_read(
    database_service: DatabaseServiceSQL,
) -> None:
    simulation, _ = await _insert_run(database_service, correlation_id=f"sim-{uuid.uuid4().hex[:8]}")
    _analysis_id, analysis_run_id = await _analysis_run(database_service, simulation)
    seen: dict[int, Simulation | None] = {}

    async def _fake_ingest(hpc_run: HpcRun, sim: Simulation | None, *_args: Any, **_kwargs: Any) -> IngestResult:
        seen[hpc_run.database_id] = sim
        return IngestResult(hpcrun_id=hpc_run.database_id)

    with (
        patch("viva_api.dependencies.get_file_service", return_value=MagicMock()),
        patch("viva_api.simulation.event_ingest.ingest_run_events", side_effect=_fake_ingest),
    ):
        await _scheduler(database_service).ingest_run_events()

    assert analysis_run_id in seen, "the analysis run was not offered to the ingester"
    handed = seen[analysis_run_id]
    assert handed is not None and handed.database_id == simulation.database_id


def _resolver(outcomes: dict[int, AnalysisRun]) -> AsyncMock:
    """A stand-in for handle_get_ray_analysis_status: RUNNING unless told otherwise."""

    async def _resolve(_db: Any, record: ExperimentAnalysisDTO) -> AnalysisRun:
        return outcomes.get(record.database_id, AnalysisRun(id=record.database_id, status=JobStatus.RUNNING))

    return AsyncMock(side_effect=_resolve)


@pytest.mark.asyncio
async def test_an_analysis_run_ends_when_its_analysis_resolves(database_service: DatabaseServiceSQL) -> None:
    simulation, _ = await _insert_run(database_service, correlation_id=f"sim-{uuid.uuid4().hex[:8]}")
    done_analysis, done_run = await _analysis_run(database_service, simulation)
    _busy_analysis, busy_run = await _analysis_run(database_service, simulation)

    outcomes = {done_analysis: AnalysisRun(id=done_analysis, status=JobStatus.COMPLETED)}
    with patch("viva_api.common.handlers.analyses.handle_get_ray_analysis_status", _resolver(outcomes)):
        await _scheduler(database_service).update_analysis_runs()

    finished = await database_service.get_hpcrun(done_run)
    assert finished is not None and finished.status == JobStatus.COMPLETED
    assert finished.end_time is not None

    still_running = await database_service.get_hpcrun(busy_run)
    assert still_running is not None and still_running.status == JobStatus.RUNNING
    assert still_running.end_time is None


@pytest.mark.asyncio
async def test_a_failed_analysis_carries_its_reason_onto_the_run(database_service: DatabaseServiceSQL) -> None:
    simulation, _ = await _insert_run(database_service, correlation_id=f"sim-{uuid.uuid4().hex[:8]}")
    failed_analysis, failed_run = await _analysis_run(database_service, simulation)

    outcomes = {failed_analysis: AnalysisRun(id=failed_analysis, status=JobStatus.FAILED, error_log="ImagePullBackOff")}
    with patch("viva_api.common.handlers.analyses.handle_get_ray_analysis_status", _resolver(outcomes)):
        await _scheduler(database_service).update_analysis_runs()

    run = await database_service.get_hpcrun(failed_run)
    assert run is not None and run.status == JobStatus.FAILED
    assert run.error_message == "ImagePullBackOff"
    # A run that has ended is no longer active, so the next tick leaves it alone.
    assert failed_run not in {r.database_id for r in await database_service.list_active_analysis_hpcruns()}
