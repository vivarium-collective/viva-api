"""``/simulations/{id}/events``, ``/tasks`` and the richer ``/status`` (observability plan D4d).

Handler-level, with the database and the backend mocked the way the sibling
status tests do; the ingester itself is covered against Postgres in
``tests/simulation/test_event_ingest.py``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from viva_api.common.handlers import simulations as handlers
from viva_api.common.models import JobId, JobStatus
from viva_api.simulation import event_ingest
from viva_api.simulation.models import HpcRun, JobType, SimulationSpan
from viva_api.simulation.simulation_service_ray import BatchJobDetail, SimulationServiceRay

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "events"
TRACE = "4bf92f3577b34da6a3ce929d0e0e4736"


def _stream(name: str) -> tuple[list[Any], list[SimulationSpan]]:
    events, _ = event_ingest.parse_event_lines((FIXTURES / name).read_text())
    stored = [e for e in events if e.event not in event_ingest.UNSTORED_EVENTS]
    for i, e in enumerate(stored, start=1):
        e.cursor = i
    spans: dict[str, SimulationSpan] = {}
    event_ingest.apply_span_events(spans, events)
    return stored, list(spans.values())


def _row(**overrides: Any) -> HpcRun:
    base: dict[str, Any] = {
        "database_id": 11,
        "job_id": JobId.k8s_nextflow("nf-sim943"),
        "correlation_id": "943_abc_xyz",
        "job_type": JobType.SIMULATION,
        "ref_id": 943,
        "status": JobStatus.RUNNING,
        "trace_id": TRACE,
        "generation": 1,
        "last_event_at": "2026-09-10 06:48:02",
        "attempt": 1,
    }
    base.update(overrides)
    return HpcRun(**base)


def _db(row: HpcRun, events: list[Any] | None = None, spans: list[SimulationSpan] | None = None) -> MagicMock:
    db = MagicMock()
    db.get_simulation = AsyncMock(return_value=MagicMock(experiment_id="sim193-run3-pilot-combo21-5g-769-ad0c"))
    db.get_hpcrun_by_ref = AsyncMock(return_value=row)
    db.list_hpcrun_spans = AsyncMock(return_value=spans or [])

    async def _list(hpcrun_id: int, **filters: Any) -> list[Any]:
        out = events or []
        if filters.get("level"):
            out = [e for e in out if e.level == filters["level"]]
        if filters.get("event"):
            out = [e for e in out if e.event == filters["event"]]
        if filters.get("generation") is not None:
            out = [e for e in out if e.generation == filters["generation"]]
        if filters.get("after") is not None:
            out = [e for e in out if (e.cursor or 0) > filters["after"]]
        return out[: filters.get("limit", 1000)]

    db.list_hpcrun_events = AsyncMock(side_effect=_list)
    return db


@pytest.mark.asyncio
async def test_status_carries_stage_generation_and_open_spans_from_the_row_and_spans() -> None:
    _events, spans = _stream("lineage_running_gen1.jsonl")
    db = _db(_row(), spans=spans)
    service = MagicMock()
    service.get_job_status = AsyncMock(return_value=MagicMock(status=JobStatus.RUNNING, error_message=None))
    with patch.object(handlers, "get_simulation_service_for_job", return_value=service):
        run = await handlers.get_simulation_status(db_service=db, id=943)
    assert run.status == JobStatus.RUNNING
    assert run.open_spans == ["lineage[variant=0,lineage_seed=0]", "generation[generation=1]"]
    assert run.stage == "lineage[variant=0,lineage_seed=0] > generation[generation=1]"
    assert run.generation == 1 and run.last_event_at == "2026-09-10 06:48:02" and run.trace_id == TRACE


@pytest.mark.asyncio
async def test_status_asks_SQL_for_open_spans_only() -> None:
    """S2 (eagmon, #612): ``/status`` used to read EVERY span of the run just to
    derive ``stage``, which is a span per stage/generation/seed on a long campaign.

    A plain LIMIT would be the wrong bound here: the query is ordered by
    ``start_ts``, so truncating drops the NEWEST spans -- precisely the open ones
    ``stage`` is computed from. Filtering to ``end_ts IS NULL`` in SQL bounds the
    read by how many spans can be open AT ONCE instead, and cannot drop the rows
    the answer depends on.
    """
    _events, spans = _stream("lineage_running_gen1.jsonl")
    db = _db(_row(), spans=[s for s in spans if s.end_ts is None])
    service = MagicMock()
    service.get_job_status = AsyncMock(return_value=MagicMock(status=JobStatus.RUNNING, error_message=None))
    with patch.object(handlers, "get_simulation_service_for_job", return_value=service):
        run = await handlers.get_simulation_status(db_service=db, id=943)

    db.list_hpcrun_spans.assert_awaited_once()
    assert db.list_hpcrun_spans.await_args.kwargs.get("open_only") is True
    # and the answer is unchanged by the narrower read
    assert run.stage == "lineage[variant=0,lineage_seed=0] > generation[generation=1]"


@pytest.mark.asyncio
async def test_status_of_a_row_without_a_trace_answers_the_three_classic_fields_only() -> None:
    """A pre-migration row (or any run on a legacy image) answers exactly what
    it always did. Every field this plan adds is null, and no span lookup is
    even attempted -- the additive-only guarantee, field by field, so a later
    change cannot start returning a placeholder or raising on old rows."""
    db = _db(_row(trace_id=None, generation=None, last_event_at=None, attempt=None, status=JobStatus.COMPLETED))
    run = await handlers.get_simulation_status(db_service=db, id=943)

    # the three fields every existing client reads, unchanged
    assert (run.id, run.status, run.error_message) == (943, JobStatus.COMPLETED, None)
    # everything the observability plan added: absent, not empty-but-present
    for field in ("stage", "generation", "last_event_at", "attempt", "exit_code", "error_source", "trace_id"):
        assert getattr(run, field) is None, field
    assert run.open_spans is None
    db.list_hpcrun_spans.assert_not_awaited()


@pytest.mark.asyncio
async def test_the_span_tree_query_is_BOUNDED() -> None:
    """S2 (eagmon, #612): the flat event list is bounded (limit <= 1000 + cursor);
    the ?tree= span query must be too. A long multiseed x multigen run emits a span
    per stage/generation/seed, so an uncapped read is an unbounded result set on an
    ordinary API call."""
    from viva_api.common.handlers.simulations import _MAX_SPAN_ROWS, get_simulation_events

    db = _db(_row(), events=[], spans=[])
    await get_simulation_events(db_service=db, id=1, tree=True)

    db.list_hpcrun_spans.assert_awaited_once()
    assert db.list_hpcrun_spans.await_args.kwargs.get("limit") == _MAX_SPAN_ROWS
    assert _MAX_SPAN_ROWS > 0


@pytest.mark.asyncio
async def test_events_filters_page_and_tree() -> None:
    events, spans = _stream("lineage_failed_gen1.jsonl")
    db = _db(_row(status=JobStatus.FAILED), events=events, spans=spans)

    flat = await handlers.get_simulation_events(db_service=db, id=943)
    assert flat.trace_id == TRACE and flat.next is None
    assert [e.event for e in flat.events][:3] == ["span.start", "task.start", "span.start"]
    assert all(e.event != "tick" for e in flat.events)

    errors = await handlers.get_simulation_events(db_service=db, id=943, level="error")
    assert {e.event for e in errors.events} == {"process.exception", "lineage.failure", "span.end", "task.end"}

    gen1 = await handlers.get_simulation_events(db_service=db, id=943, generation=1, event="process.exception")
    assert len(gen1.events) == 1 and gen1.events[0].payload is not None
    assert gen1.events[0].payload["path"] == "agents/00/ecoli-polypeptide-elongation"
    assert gen1.events[0].variant == 0 and gen1.events[0].lineage_seed == 0

    page = await handlers.get_simulation_events(db_service=db, id=943, limit=5)
    assert len(page.events) == 5 and page.next == 5
    rest = await handlers.get_simulation_events(db_service=db, id=943, after=page.next)
    assert rest.events[0].cursor == 6

    tree = await handlers.get_simulation_events(db_service=db, id=943, tree=True)
    assert tree.tree is not None and len(tree.tree) == 1
    root = tree.tree[0]
    assert root.span.label == "lineage[variant=0,lineage_seed=0]" and root.span.status == "error"
    gen_labels = [c.span.label for c in root.children]
    assert gen_labels == ["generation[generation=0]", "generation[generation=1]"]
    gen1_node = root.children[1]
    assert {e.event for e in gen1_node.events} >= {"process.exception", "lineage.failure"}


@pytest.mark.asyncio
async def test_events_on_a_legacy_run_is_an_empty_page_not_a_404() -> None:
    """A run with no observability records -- no trace id, no rows -- is a
    legitimate, answerable question: the run exists and has no events yet.
    404 stays reserved for a simulation that does not exist, so a client can
    tell "nothing recorded" from "wrong id"."""
    db = _db(_row(trace_id=None, generation=None, last_event_at=None, attempt=None), events=[], spans=[])

    page = await handlers.get_simulation_events(db_service=db, id=943)
    assert page.id == 943 and page.events == [] and page.next is None and page.trace_id is None

    tree = await handlers.get_simulation_events(db_service=db, id=943, tree=True)
    assert tree.tree == [] and tree.events == []


@pytest.mark.asyncio
async def test_events_404_for_an_unknown_simulation() -> None:
    db = MagicMock()
    db.get_simulation = AsyncMock(return_value=None)
    with pytest.raises(ValueError, match="not found"):
        await handlers.get_simulation_events(db_service=db, id=1)


@pytest.mark.asyncio
async def test_tasks_for_a_nextflow_run_come_from_the_trace() -> None:
    from viva_api.dependencies import get_file_service, set_file_service

    trace = (
        "task_id\thash\tnative_id\tname\tstatus\texit\tsubmit\n"
        "1\taa/111111\taaa-111\tparca_v0\tCOMPLETED\t0\t-\n"
        "2\tbb/222222\tbbb-222\truns_v0:lineage_v0_s0\tFAILED\t1\t-\n"
        "3\tbb/333333\tbbb-333\truns_v0:lineage_v0_s0\tFAILED\t1\t-\n"
    )

    class _S3:
        async def get_file_contents(self, s3_path: Any) -> bytes | None:
            return trace.encode() if str(s3_path.s3_path).endswith("/trace.csv") else None

    db = _db(_row(status=JobStatus.FAILED))
    saved = get_file_service()
    set_file_service(_S3())  # type: ignore[arg-type]
    try:
        tasks = await handlers.get_simulation_tasks(db_service=db, id=943)
    finally:
        set_file_service(saved)
    assert [(t.name, t.status, t.job_id, t.attempt) for t in tasks] == [
        ("parca_v0", "COMPLETED", "aaa-111", 1),
        ("runs_v0:lineage_v0_s0", "FAILED", "bbb-222", 1),
        ("runs_v0:lineage_v0_s0", "FAILED", "bbb-333", 2),
    ]
    assert tasks[1].exit_code == 1 and tasks[1].task_hash == "bb/222222"


@pytest.mark.asyncio
async def test_tasks_for_a_chain_campaign_come_from_describe_jobs() -> None:
    row = _row(
        job_id=JobId.ray("parca-job"),
        chain_final_job_ids=["job-a", "job-b"],
        chain_current_job_ids=[None, None],
        chain_n_generations=5,
        status=JobStatus.FAILED,
    )
    db = _db(row)
    service = MagicMock(spec=SimulationServiceRay)
    service.get_batch_job_details.return_value = {
        "job-a": BatchJobDetail(
            job_id="job-a", job_name="chain-seed0-lineage-x", status=JobStatus.COMPLETED, exit_code=0
        ),
        "job-b": BatchJobDetail(
            job_id="job-b",
            job_name="chain-seed1-lineage-x",
            status=JobStatus.FAILED,
            status_reason="Essential container in task exited",
            exit_code=1,
            attempts=1,
        ),
    }
    with patch.object(handlers, "get_simulation_service_for_job", return_value=service):
        tasks = await handlers.get_simulation_tasks(db_service=db, id=943)
    assert [(t.name, t.status, t.exit_code) for t in tasks] == [
        ("chain-seed0-lineage-x", "completed", 0),
        ("chain-seed1-lineage-x", "failed", 1),
    ]
    assert tasks[1].status_reason == "Essential container in task exited"


@pytest.mark.asyncio
async def test_tasks_for_other_backends_is_empty_not_an_error() -> None:
    db = _db(_row(job_id=JobId.slurm(7)))
    assert await handlers.get_simulation_tasks(db_service=db, id=943) == []
