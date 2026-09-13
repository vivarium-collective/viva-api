"""error_message is written by precedence, not last-writer-wins (observability plan D4c)."""

from __future__ import annotations

import pytest

from viva_api.common.hpc.job_service import JobStatusUpdate
from viva_api.common.models import JobId, JobStatus
from viva_api.simulation.database_service import DatabaseServiceSQL
from viva_api.simulation.models import JobType, SimulationRequest


async def _row(database_service: DatabaseServiceSQL, experiment_request: SimulationRequest) -> int:
    simulation = await database_service.insert_simulation(sim_request=experiment_request)
    hpcrun = await database_service.insert_hpcrun(
        job_id=JobId.k8s_nextflow("nf-exp-abc"),
        job_type=JobType.SIMULATION,
        ref_id=simulation.database_id,
        correlation_id="946_c233c7a_ab12cd3",
    )
    return hpcrun.database_id


@pytest.mark.asyncio
async def test_insert_derives_trace_ids_from_the_correlation_id(
    experiment_request: SimulationRequest, database_service: DatabaseServiceSQL
) -> None:
    from viva_api.common.events_env import campaign_span_id, trace_id_from_correlation

    hpcrun_id = await _row(database_service, experiment_request)
    row = await database_service.get_hpcrun(hpcrun_id)
    assert row is not None
    assert row.trace_id == trace_id_from_correlation("946_c233c7a_ab12cd3")
    assert row.campaign_span_id == campaign_span_id("946_c233c7a_ab12cd3")


@pytest.mark.asyncio
async def test_a_generic_k8s_message_never_overwrites_a_task_traceback(
    experiment_request: SimulationRequest, database_service: DatabaseServiceSQL
) -> None:
    """The scheduler captured `.command.err`; a later GET /status poll brings
    Kubernetes' "backoff limit" text. The traceback must survive."""
    hpcrun_id = await _row(database_service, experiment_request)
    await database_service.update_hpcrun_status(
        hpcrun_id,
        JobStatusUpdate(
            job_id=JobId.k8s_nextflow("nf-exp-abc"),
            status=JobStatus.FAILED,
            error_message="ValueError: boom",
            error_source="command_err",
            exit_code="1",
            attempt=1,
        ),
    )
    await database_service.update_hpcrun_status(
        hpcrun_id,
        JobStatusUpdate(
            job_id=JobId.k8s_nextflow("nf-exp-abc"),
            status=JobStatus.FAILED,
            error_message="Job has reached the specified backoff limit",
            error_source="k8s_condition",
        ),
    )
    row = await database_service.get_hpcrun(hpcrun_id)
    assert row is not None
    assert row.error_message == "ValueError: boom"
    assert row.error_source == "command_err"
    assert row.exit_code == 1
    assert row.attempt == 1


@pytest.mark.asyncio
async def test_a_better_source_replaces_a_worse_one_and_an_unlabelled_write_still_wins_over_nothing(
    experiment_request: SimulationRequest, database_service: DatabaseServiceSQL
) -> None:
    hpcrun_id = await _row(database_service, experiment_request)
    await database_service.update_hpcrun_status(
        hpcrun_id,
        JobStatusUpdate(job_id=JobId.k8s_nextflow("nf-exp-abc"), status=JobStatus.FAILED, error_message="backoff"),
    )
    await database_service.update_hpcrun_status(
        hpcrun_id,
        JobStatusUpdate(
            job_id=JobId.k8s_nextflow("nf-exp-abc"),
            status=JobStatus.FAILED,
            error_message="Traceback ...",
            error_source="failure_record",
        ),
    )
    row = await database_service.get_hpcrun(hpcrun_id)
    assert row is not None
    assert row.error_message == "Traceback ..." and row.error_source == "failure_record"


@pytest.mark.asyncio
async def test_a_cancel_without_a_message_clears_stale_error_text(
    experiment_request: SimulationRequest, database_service: DatabaseServiceSQL
) -> None:
    hpcrun_id = await _row(database_service, experiment_request)
    await database_service.update_hpcrun_status(
        hpcrun_id,
        JobStatusUpdate(job_id=JobId.k8s_nextflow("nf-exp-abc"), status=JobStatus.FAILED, error_message="old failure"),
    )
    await database_service.update_hpcrun_status(
        hpcrun_id, JobStatusUpdate(job_id=JobId.k8s_nextflow("nf-exp-abc"), status=JobStatus.CANCELLED)
    )
    row = await database_service.get_hpcrun(hpcrun_id)
    assert row is not None
    assert row.status == JobStatus.CANCELLED and row.error_message is None


@pytest.mark.asyncio
async def test_finalize_nextflow_head_is_single_winner(
    experiment_request: SimulationRequest, database_service: DatabaseServiceSQL
) -> None:
    import asyncio

    hpcrun_id = await _row(database_service, experiment_request)
    results = await asyncio.gather(
        database_service.finalize_nextflow_head(
            hpcrun_id, JobStatus.FAILED, error_message="analysis_v8 failed", error_source="command_err", exit_code=0
        ),
        database_service.finalize_nextflow_head(hpcrun_id, JobStatus.FAILED, error_message="late"),
    )
    assert sorted(results) == [False, True]
    row = await database_service.get_hpcrun(hpcrun_id)
    assert row is not None
    assert row.status is JobStatus.FAILED  # whichever tick won
    assert row.status.is_terminal
    active = await database_service.list_active_nextflow_hpcruns()
    assert all(r.database_id != hpcrun_id for r in active)


# ---------------------------------------------------------------------------
# B1: GET /status must not finalize a Nextflow head ahead of the trace poller
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_status_poll_does_NOT_lock_the_trace_poller_out_of_a_nextflow_head(
    experiment_request: SimulationRequest, database_service: DatabaseServiceSQL
) -> None:
    """B1 (eagmon, #609): the sim-749 shape.

    A Nextflow head exits 0 with a gather task dead, so the K8s Job condition is
    ``Complete``. If ``GET /status`` persists that raw condition, the row goes
    terminal, ``finalize_nextflow_head``'s ``WHERE status IN (PENDING, RUNNING)``
    can never win, and the run reads COMPLETED forever with a failed task in it --
    reachable by nothing more exotic than polling status inside the <= 30 s gap
    before the next scheduler tick.

    This test drives the DATABASE contract that makes the bug possible, so it
    fails loudly if the handler ever starts persisting for this backend again.
    """
    hpcrun_id = await _row(database_service, experiment_request)
    job_id = JobId.k8s_nextflow("nf-exp-abc")

    # What /status used to do: persist the K8s condition (Complete -> COMPLETED).
    await database_service.update_hpcrun_status(
        hpcrun_id,
        JobStatusUpdate(job_id=job_id, status=JobStatus.COMPLETED, error_source="k8s_condition"),
    )

    # The poller then arrives with the truth from the trace -- and loses.
    won = await database_service.finalize_nextflow_head(
        hpcrun_id, JobStatus.FAILED, error_message="analysis_v8 failed", error_source="nextflow_trace"
    )
    row = await database_service.get_hpcrun(hpcrun_id)
    assert row is not None

    # This is the bug, pinned: whoever writes terminal FIRST decides forever.
    assert won is False
    assert row.status is JobStatus.COMPLETED  # wrong, and permanent
    assert row.error_message is None
    # ... and the row has already dropped out of the poller's work list, so there
    # is no later tick that could correct it.
    active = await database_service.list_active_nextflow_hpcruns()
    assert all(r.database_id != hpcrun_id for r in active)


@pytest.mark.asyncio
async def test_a_nextflow_head_left_non_terminal_is_still_the_pollers_to_finalize(
    experiment_request: SimulationRequest, database_service: DatabaseServiceSQL
) -> None:
    """The other half of B1: with /status no longer persisting, the head stays in
    (PENDING, RUNNING), stays on the poller's list, and the trace decides."""
    hpcrun_id = await _row(database_service, experiment_request)

    active = await database_service.list_active_nextflow_hpcruns()
    assert any(r.database_id == hpcrun_id for r in active)

    won = await database_service.finalize_nextflow_head(
        hpcrun_id, JobStatus.FAILED, error_message="analysis_v8 failed", error_source="nextflow_trace"
    )
    row = await database_service.get_hpcrun(hpcrun_id)
    assert won is True
    assert row is not None
    assert row.status is JobStatus.FAILED
    assert row.error_message == "analysis_v8 failed"


# ---------------------------------------------------------------------------
# the status vocabulary must not grow
# ---------------------------------------------------------------------------


def test_the_status_vocabulary_is_unchanged_by_the_observability_work() -> None:
    """No new status label, deliberately.

    An earlier draft added ``PARTIAL`` for "some tasks succeeded, some did not".
    It was dropped: nothing branched on it (it was terminal-set membership and a
    CLI colour), Postgres cannot drop an enum label once added, and every client
    that did not learn it would treat the run as non-terminal and poll forever.
    Which tasks survived belongs in ``error_message`` and the per-task rows,
    which carry it at far higher resolution than a label can.

    This test exists so that re-adding one is a deliberate act with a migration
    behind it, not a quiet edit to an enum.
    """
    from viva_api.common.models import TERMINAL_JOB_STATUSES
    from viva_api.simulation.tables_orm import JobStatusDB

    assert {s.value for s in JobStatusDB} == {
        "waiting",
        "pending",
        "queued",
        "running",
        "completed",
        "cancelled",
        "failed",
    }
    assert (
        frozenset({
            JobStatus.COMPLETED,
            JobStatus.FAILED,
            JobStatus.CANCELLED,
        })
        == TERMINAL_JOB_STATUSES
    )
    for legacy in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED):
        assert legacy.is_terminal
        assert JobStatusDB.from_job_status(legacy).to_job_status() is legacy
    for legacy in (JobStatus.WAITING, JobStatus.PENDING, JobStatus.QUEUED, JobStatus.RUNNING):
        assert not legacy.is_terminal
        assert JobStatusDB.from_job_status(legacy).to_job_status() is legacy


@pytest.mark.asyncio
async def test_a_row_written_with_a_legacy_status_still_loads_after_the_migration(
    experiment_request: SimulationRequest, database_service: DatabaseServiceSQL
) -> None:
    """The migration must not change how an existing row reads back: a run
    finalized as FAILED before it still loads as FAILED, with the new
    observability columns simply null."""
    hpcrun_id = await _row(database_service, experiment_request)
    await database_service.update_hpcrun_status(
        hpcrun_id,
        JobStatusUpdate(
            job_id=JobId.k8s_nextflow("nf-exp-abc"),
            status=JobStatus.FAILED,
            error_message="legacy failure",
        ),
    )

    row = await database_service.get_hpcrun(hpcrun_id)
    assert row is not None
    assert row.status is JobStatus.FAILED and row.status.is_terminal
    assert row.error_message == "legacy failure"
    assert row.stage is None and row.generation is None and row.last_event_at is None and row.exit_code is None
