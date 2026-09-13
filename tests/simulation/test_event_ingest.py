"""The run-event ingester (observability plan D4b, ``viva_api.simulation.event_ingest``).

Pure parsing/folding first (no database), then the ingest pass against real
Postgres (testcontainers, the ``database_service`` fixture) with an in-memory
FileService double standing in for the tasks' ``events.jsonl`` objects.
"""

from __future__ import annotations

import datetime
import uuid
from datetime import UTC
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from viva_api.common.models import JobId, JobStatus
from viva_api.common.storage.file_service import FileService, ListingItem
from viva_api.simulation import event_ingest
from viva_api.simulation.database_service import DatabaseServiceSQL
from viva_api.simulation.models import (
    HpcRun,
    JobType,
    ParcaDatasetRequest,
    ParcaOptions,
    Simulation,
    SimulationConfig,
    SimulationRequest,
)

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "events"
TRACE = "4bf92f3577b34da6a3ce929d0e0e4736"


def _lines(name: str) -> str:
    return (FIXTURES / name).read_text()


# ---------------------------------------------------------------------------
# pure
# ---------------------------------------------------------------------------


def test_parse_promotes_identity_from_baggage_and_from_top_level() -> None:
    """The settled schema carries domain keys in an opaque ``baggage`` map with
    string values; an older draft stamped them at the top level. Both read the
    same way, ints are coerced, and viva-api presents them first-class."""
    events, bad = event_ingest.parse_event_lines(_lines("lineage_failed_gen1.jsonl"), expected_trace_id=TRACE)
    assert bad == 1  # the deliberately bad line
    gen_start = next(e for e in events if e.event == "lineage.generation.start")
    assert gen_start.generation == 0 and gen_start.variant == 0 and gen_start.lineage_seed == 0
    assert gen_start.baggage is not None and gen_start.baggage["sim_id"] == "946"

    # the engine's own stream (process-bigraph#209, growth-division example): generic baggage
    # keys ride through untouched and nothing is promoted, because the engine names no domain key
    engine_events, bad = event_ingest.parse_event_lines(_lines("engine_stream.jsonl"))
    assert bad == 0 and engine_events
    first = engine_events[0]
    assert first.component == "process_bigraph" and first.event == "span.start"
    assert first.baggage == {"experiment": "exp-1", "replicate": "3", "stage": "pilot"}
    assert first.generation is None and first.variant is None and first.lineage_seed is None
    assert {e.event for e in engine_events} >= {"run.start", "tick", "structure.changed", "run.end", "span.end"}


def test_parse_drops_other_traces_and_non_events() -> None:
    other = '{"event": "tick", "seq": 1, "trace_id": "ffff", "ts": "2026-09-10T00:00:00Z"}'
    assert event_ingest.parse_event_line(other, expected_trace_id=TRACE) is None
    assert event_ingest.parse_event_line('{"seq": 1}') is None
    assert event_ingest.parse_event_line('["not", "an", "object"]') is None
    assert event_ingest.parse_event_line("") is None
    legacy = event_ingest.parse_event_line(
        '{"event": "run_start", "seq": 3, "ts": "2026-09-10T00:00:00Z", "layer": "engine",'
        ' "generation": 2, "variant": "1"}'
    )
    assert legacy is not None and legacy.component == "engine" and legacy.generation == 2 and legacy.variant == 1
    ok = event_ingest.parse_event_line('{"event": "run.start", "seq": "7", "ts": "2026-09-10T00:00:00Z"}')
    assert ok is not None and ok.seq == 7 and ok.source == "unknown" and ok.component == "process_bigraph"


def test_spans_fold_into_a_tree_and_the_open_ones_are_the_stage() -> None:
    events, _ = event_ingest.parse_event_lines(_lines("lineage_running_gen1.jsonl"))
    spans: dict[str, Any] = {}
    changed = event_ingest.apply_span_events(spans, events)
    assert changed == {"1111aaaa2222bbbb", "3333cccc4444dddd", "5555eeee6666ffff", "7777aaaa8888bbbb"}
    # generation 0 and its run are closed with durations; generation 1 and the task are open
    assert spans["3333cccc4444dddd"].status == "ok" and spans["3333cccc4444dddd"].duration_s == 2520.1
    assert spans["5555eeee6666ffff"].end_ts is not None
    assert spans["7777aaaa8888bbbb"].end_ts is None and spans["1111aaaa2222bbbb"].end_ts is None
    assert event_ingest.open_span_labels(spans) == ["lineage[variant=0,lineage_seed=0]", "generation[generation=1]"]
    assert event_ingest.stage_from_spans(spans) == "lineage[variant=0,lineage_seed=0] > generation[generation=1]"

    tree = event_ingest.build_span_tree(list(spans.values()), events)
    assert len(tree) == 1 and tree[0].span.name == "lineage"
    gens = [child.span.label for child in tree[0].children]
    assert gens == ["generation[generation=0]", "generation[generation=1]"]
    gen0 = tree[0].children[0]
    assert {e.event for e in gen0.events} == {
        "lineage.generation.start",
        "lineage.chunk.flushed",
        "lineage.division",
        "lineage.generation.end",
    }
    assert [c.span.name for c in gen0.children] == ["run"]


def test_span_end_without_a_start_still_creates_the_span() -> None:
    only_end = (
        '{"event": "span.end", "seq": 5, "ts": "2026-09-10T06:47:01.100Z", "span_id": "abc", "parent_span_id": null,'
        ' "payload": {"name": "generation", "attrs": {"generation": 3}, "start_ts": "2026-09-10T06:05:01.000Z",'
        ' "end_ts": "2026-09-10T06:47:01.100Z", "duration_s": 2520.1, "status": "ok"}}'
    )
    events, _ = event_ingest.parse_event_lines(only_end)
    spans: dict[str, Any] = {}
    event_ingest.apply_span_events(spans, events)
    span = spans["abc"]
    assert span.start_ts == "2026-09-10T06:05:01.000Z" and span.end_ts == "2026-09-10T06:47:01.100Z"
    assert span.label == "generation[generation=3]" and span.status == "ok"


def test_fold_progress_takes_the_newest_generation_and_timestamp_including_heartbeats() -> None:
    events, _ = event_ingest.parse_event_lines(_lines("lineage_running_gen1.jsonl"))
    progress = event_ingest.fold_progress(events)
    assert progress.generation == 1
    assert progress.last_event_at == datetime.datetime(2026, 9, 10, 6, 48, 2)  # the gen-1 tick
    assert progress.events_seen == len(events)


def test_key_from_events_uri_refuses_a_foreign_bucket() -> None:
    assert event_ingest.key_from_events_uri("s3://work/nextflow/work/exp/events/", "work") == "nextflow/work/exp/events"
    assert event_ingest.key_from_events_uri("s3://other/x/events/", "work") is None
    assert event_ingest.key_from_events_uri("s3://any/x/events/", None) == "x/events"
    assert event_ingest.key_from_events_uri("plain/prefix/", "work") == "plain/prefix"


def _run(status: JobStatus, *, end_time: str | None = None, last_event_at: str | None = None) -> HpcRun:
    return HpcRun(
        database_id=1,
        job_id=JobId.k8s_nextflow("nf-x"),
        correlation_id="c",
        job_type=JobType.SIMULATION,
        ref_id=1,
        status=status,
        end_time=end_time,
        last_event_at=last_event_at,
        trace_id=TRACE,
    )


def test_is_ingest_candidate_rules() -> None:
    now = datetime.datetime(2026, 9, 10, 7, 0, 0)
    settings = MagicMock(events_ingest_idle_seconds=600, events_ingest_terminal_grace_seconds=900)
    assert event_ingest.is_ingest_candidate(_run(JobStatus.RUNNING), settings, now)
    assert event_ingest.is_ingest_candidate(_run(JobStatus.RUNNING, last_event_at="2026-09-10 06:55:00"), settings, now)
    assert not event_ingest.is_ingest_candidate(
        _run(JobStatus.RUNNING, last_event_at="2026-09-10 06:40:00"), settings, now
    )
    assert event_ingest.is_ingest_candidate(_run(JobStatus.COMPLETED, end_time="2026-09-10 06:50:00"), settings, now)
    assert not event_ingest.is_ingest_candidate(
        _run(JobStatus.COMPLETED, end_time="2026-09-10 06:30:00"), settings, now
    )
    assert not event_ingest.is_ingest_candidate(_run(JobStatus.FAILED), settings, now)  # terminal, no end_time


# ---------------------------------------------------------------------------
# the ingest pass, against Postgres
# ---------------------------------------------------------------------------


class _S3(FileService):
    """A FileService double: ``objects`` maps keys to bytes; listing is by prefix."""

    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = objects
        self.reads: list[str] = []
        self.listings: list[str] = []

    async def download_file(self, s3_path: Any, file_path: Any = None) -> Any:
        raise NotImplementedError

    async def upload_file(self, file_path: Any, s3_path: Any) -> Any:
        raise NotImplementedError

    async def upload_bytes(self, file_contents: bytes, s3_path: Any) -> Any:
        raise NotImplementedError

    async def get_modified_date(self, s3_path: Any) -> Any:
        raise NotImplementedError

    async def delete_file(self, s3_path: Any) -> None:
        raise NotImplementedError

    async def close(self) -> None:
        return None

    async def get_listing(self, s3_path: Any) -> list[ListingItem]:
        prefix = str(s3_path.s3_path).rstrip("/") + "/"
        self.listings.append(prefix)
        return [
            ListingItem(Key=k, LastModified=datetime.datetime.now(UTC), ETag="e", Size=len(v))
            for k, v in sorted(self.objects.items())
            if k.startswith(prefix)
        ]

    async def get_file_contents(self, s3_path: Any) -> bytes | None:
        key = str(s3_path.s3_path)
        self.reads.append(key)
        return self.objects.get(key)


def _settings(**overrides: Any) -> MagicMock:
    settings = MagicMock()
    settings.events_enabled = True
    settings.events_s3_prefix = ""
    settings.s3_work_bucket = "work"
    settings.s3_work_prefix = "nextflow/work"
    settings.storage_s3_bucket = "work"
    settings.events_ingest_max_objects_per_tick = 50
    settings.events_ingest_idle_seconds = 600
    settings.events_ingest_terminal_grace_seconds = 900
    for key, value in overrides.items():
        setattr(settings, key, value)
    return settings


async def _insert_run(database_service: DatabaseServiceSQL, *, correlation_id: str) -> tuple[Simulation, HpcRun]:
    simulator = await database_service.insert_simulator(
        git_commit_hash=str(uuid.uuid4()), git_repo_url="https://github.com/CovertLabEcoli/sms-ecoli", git_branch="main"
    )
    parca = await database_service.insert_parca_dataset(
        parca_dataset_request=ParcaDatasetRequest(simulator_version=simulator, parca_config=ParcaOptions())
    )
    experiment_id = f"test-events-{uuid.uuid4().hex[:8]}"
    simulation = await database_service.insert_simulation(
        sim_request=SimulationRequest(
            simulation_config_filename="config_filename",
            experiment_id=experiment_id,
            parca_dataset_id=parca.database_id,
            simulator_id=simulator.database_id,
            config=SimulationConfig(experiment_id=experiment_id),
        )
    )
    hpcrun = await database_service.insert_hpcrun(
        job_id=JobId.k8s_nextflow(f"nf-{experiment_id}"),
        job_type=JobType.SIMULATION,
        ref_id=simulation.database_id,
        correlation_id=correlation_id,
    )
    return simulation, hpcrun


def _retag(text: str, trace_id: str) -> str:
    return text.replace(TRACE, trace_id)


@pytest.mark.asyncio
async def test_ingest_stores_events_spans_and_progress_and_skips_unchanged_objects(
    database_service: DatabaseServiceSQL,
) -> None:
    simulation, hpcrun = await _insert_run(database_service, correlation_id=f"c-{uuid.uuid4().hex[:6]}")
    assert hpcrun.trace_id
    key = f"nextflow/work/{simulation.experiment_id}/events/{hpcrun.trace_id}/batch-job-946-seed0.jsonl"
    s3 = _S3({key: _retag(_lines("lineage_running_gen1.jsonl"), hpcrun.trace_id).encode()})
    settings = _settings()

    result = await event_ingest.ingest_run_events(hpcrun, simulation, s3, database_service, settings)
    assert result.skipped_reason is None
    assert result.objects_read == 1 and result.bad_lines == 1
    assert result.events_inserted == result.events_parsed - 3  # three ticks are never stored
    assert result.generation == 1
    assert result.stage == "lineage[variant=0,lineage_seed=0] > generation[generation=1]"

    stored = await database_service.list_hpcrun_events(hpcrun.database_id)
    assert {e.event for e in stored} >= {
        "lineage.generation.start",
        "lineage.division",
        "lineage.generation.end",
        "run.end",
    }
    assert all(e.event != "tick" for e in stored)
    gen_start = next(e for e in stored if e.event == "lineage.generation.start")
    assert gen_start.generation == 0 and gen_start.variant == 0 and gen_start.lineage_seed == 0
    assert gen_start.baggage is not None and gen_start.baggage["experiment_id"].startswith("sim193")
    assert gen_start.tags == {"backend": "nextflow"}  # the baggage does not leak into the opaque tags on read

    spans = await database_service.list_hpcrun_spans(hpcrun.database_id)
    by_name = {(s.name, (s.attrs or {}).get("generation")): s for s in spans}
    assert by_name[("generation", 0)].status == "ok" and by_name[("generation", 0)].duration_s == pytest.approx(2520.1)
    assert by_name[("generation", 1)].end_ts is None

    refetched = await database_service.get_hpcrun(hpcrun.database_id)
    assert refetched is not None
    assert refetched.stage == result.stage and refetched.generation == 1
    assert refetched.last_event_at is not None and "06:48:02" in refetched.last_event_at
    assert refetched.events_s3_prefix == f"s3://work/nextflow/work/{simulation.experiment_id}/events/"

    # second pass, nothing changed: the object is not even read
    again = await event_ingest.ingest_run_events(refetched, simulation, s3, database_service, settings)
    assert again.objects_read == 0 and s3.reads == [key]


@pytest.mark.asyncio
async def test_ingest_is_idempotent_across_rewrites_and_closes_spans_on_a_terminal_row(
    database_service: DatabaseServiceSQL,
) -> None:
    simulation, hpcrun = await _insert_run(database_service, correlation_id=f"c-{uuid.uuid4().hex[:6]}")
    assert hpcrun.trace_id
    key = f"nextflow/work/{simulation.experiment_id}/events/{hpcrun.trace_id}/batch-job-946-seed0.jsonl"
    running = _retag(_lines("lineage_running_gen1.jsonl"), hpcrun.trace_id)
    s3 = _S3({key: running.encode()})
    settings = _settings()
    first = await event_ingest.ingest_run_events(hpcrun, simulation, s3, database_service, settings)

    # the writer rewrites the whole object: the failed stream repeats every earlier line
    s3.objects[key] = _retag(_lines("lineage_failed_gen1.jsonl"), hpcrun.trace_id).encode()
    refetched = await database_service.get_hpcrun(hpcrun.database_id)
    assert refetched is not None
    second = await event_ingest.ingest_run_events(refetched, simulation, s3, database_service, settings)
    assert second.objects_read == 1
    stored = await database_service.list_hpcrun_events(hpcrun.database_id)
    seqs = sorted((e.source, e.seq) for e in stored)
    assert len(seqs) == len(set(seqs))  # no duplicate (source, seq) despite the repeated lines
    assert second.events_inserted == len(stored) - (first.events_inserted)
    assert {e.event for e in stored if e.level == "error"} >= {"process.exception", "lineage.failure", "task.end"}

    spans = {
        s.name + str((s.attrs or {}).get("generation", "")): s
        for s in await database_service.list_hpcrun_spans(hpcrun.database_id)
    }
    assert spans["generation1"].status == "error" and "NegativeCountsError" in (spans["generation1"].error or "")
    assert spans["lineage"].status == "error"
    assert second.stage is None  # every span closed by the stream itself

    # a shorter rewrite (object shrank) is re-read from the start, still without duplicates
    s3.objects[key] = running.encode()
    refetched = await database_service.get_hpcrun(hpcrun.database_id)
    assert refetched is not None
    third = await event_ingest.ingest_run_events(refetched, simulation, s3, database_service, settings)
    assert third.objects_read == 1 and third.events_inserted == 0

    # a run that went terminal with a span still open: the ingester closes it as unknown
    open_key = f"nextflow/work/{simulation.experiment_id}/events/{hpcrun.trace_id}/late-writer.jsonl"
    s3.objects[open_key] = (
        '{"event": "span.start", "seq": 1, "ts": "2026-09-10T06:50:00.000Z", "source": "late-writer",'
        ' "layer": "runner",'
        f' "trace_id": "{hpcrun.trace_id}", "span_id": "9999aaaa9999aaaa", "parent_span_id": null,'
        ' "payload": {"name": "analysis", "attrs": {}, "start_ts": "2026-09-10T06:50:00.000Z"}}\n'
    ).encode()
    assert await database_service.finalize_nextflow_head(hpcrun.database_id, JobStatus.FAILED, error_message="x")
    terminal = await database_service.get_hpcrun(hpcrun.database_id)
    assert terminal is not None and terminal.status == JobStatus.FAILED
    fourth = await event_ingest.ingest_run_events(terminal, simulation, s3, database_service, settings)
    assert fourth.stage is None
    analysis = next(s for s in await database_service.list_hpcrun_spans(hpcrun.database_id) if s.name == "analysis")
    assert analysis.status == "unknown" and analysis.end_ts is not None


@pytest.mark.asyncio
async def test_ingest_respects_the_per_tick_object_budget_and_the_bucket(
    database_service: DatabaseServiceSQL,
) -> None:
    simulation, hpcrun = await _insert_run(database_service, correlation_id=f"c-{uuid.uuid4().hex[:6]}")
    assert hpcrun.trace_id
    prefix = f"nextflow/work/{simulation.experiment_id}/events/{hpcrun.trace_id}"
    body = _retag(_lines("lineage_running_gen1.jsonl"), hpcrun.trace_id).encode()
    s3 = _S3({f"{prefix}/w{i}.jsonl": body for i in range(5)})
    result = await event_ingest.ingest_run_events(
        hpcrun,
        simulation,
        s3,
        database_service,
        _settings(events_ingest_max_objects_per_tick=2),
    )
    assert result.objects_listed == 5 and result.objects_read == 2

    # a prefix in another bucket is refused, not silently empty
    other = await event_ingest.ingest_run_events(
        hpcrun,
        simulation,
        s3,
        database_service,
        _settings(storage_s3_bucket="elsewhere"),
    )
    assert other.objects_read == 0 and "not in the file service's bucket" in (other.skipped_reason or "")

    # S3 events disabled: nothing to read, said plainly
    off = await event_ingest.ingest_run_events(
        hpcrun,
        simulation,
        s3,
        database_service,
        _settings(events_enabled=False),
    )
    assert off.skipped_reason is not None and "no events prefix" in off.skipped_reason


@pytest.mark.asyncio
async def test_dispatcher_events_get_their_own_sequence_and_list_candidates(
    database_service: DatabaseServiceSQL,
) -> None:
    from viva_api.simulation.models import SimulationEvent

    simulation, hpcrun = await _insert_run(database_service, correlation_id=f"c-{uuid.uuid4().hex[:6]}")
    assert hpcrun.trace_id
    assert await database_service.next_hpcrun_event_seq(hpcrun.trace_id, "api") == 1
    now = datetime.datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    inserted = await database_service.insert_hpcrun_events(
        hpcrun.database_id,
        hpcrun.trace_id,
        [
            SimulationEvent(
                seq=1,
                source="api",
                ts=now,
                component="viva_api.dispatch",
                event="dispatch.head.exit",
                payload={"exit_code": 1},
            ),
            SimulationEvent(
                seq=1,
                source="api",
                ts=now,
                component="viva_api.dispatch",
                event="dispatch.head.exit",
                payload={"dup": True},
            ),
        ],
    )
    assert inserted == 1
    assert await database_service.next_hpcrun_event_seq(hpcrun.trace_id, "api") == 2
    listed = await database_service.list_hpcrun_events(hpcrun.database_id, event="dispatch.head.exit")
    assert len(listed) == 1 and listed[0].cursor is not None and listed[0].component == "viva_api.dispatch"
    assert await database_service.list_hpcrun_events(hpcrun.database_id, after=listed[0].cursor) == []

    candidates = await database_service.list_hpcruns_for_event_ingest(
        datetime.datetime.now() - datetime.timedelta(minutes=15)
    )
    assert hpcrun.database_id in {r.database_id for r in candidates}


# ---------------------------------------------------------------------------
# legacy rows and legacy images: the absence of observability must be graceful
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_pre_migration_row_is_skipped_without_touching_storage_or_the_database() -> None:
    """Every ``hpcrun`` row that existed before the observability migration has
    ``trace_id IS NULL`` (the column is new and nullable). The ingester runs
    over those rows on every scheduler tick for as long as they stay active, so
    the skip has to be free: no listing, no object read, no write.
    """
    legacy = HpcRun(
        database_id=7,
        job_id=JobId.k8s_nextflow("nf-legacy"),
        correlation_id="749_abc_xyz",
        job_type=JobType.SIMULATION,
        ref_id=749,
        status=JobStatus.RUNNING,
        trace_id=None,
        events_s3_prefix=None,
    )
    s3 = _S3({})
    db = MagicMock()  # any await on a bare MagicMock would raise; none is expected

    result = await event_ingest.ingest_run_events(legacy, None, s3, db, _settings())

    assert result.skipped_reason == "no trace_id on the row"
    assert result.objects_listed == 0 and result.objects_read == 0 and result.events_inserted == 0
    assert s3.listings == [] and s3.reads == []
    assert db.mock_calls == []


@pytest.mark.asyncio
async def test_a_run_on_a_legacy_image_writes_no_events_and_keeps_its_progress(
    database_service: DatabaseServiceSQL,
) -> None:
    """A run dispatched on a simulator image that predates the emitters has a
    trace id (the API derives one) but never writes an events object. The pass
    says so and leaves the row exactly as it found it -- in particular it must
    not stamp an empty ``stage``/``last_event_at`` over what is already there.
    """
    simulation, hpcrun = await _insert_run(database_service, correlation_id=f"c-{uuid.uuid4().hex[:6]}")
    assert hpcrun.trace_id
    s3 = _S3({})  # the prefix exists in the layout, but the legacy image put nothing under it

    result = await event_ingest.ingest_run_events(hpcrun, simulation, s3, database_service, _settings())

    assert result.skipped_reason == "no events objects yet"
    assert result.objects_listed == 0 and result.events_inserted == 0
    assert s3.reads == []  # listed once, read nothing

    refetched = await database_service.get_hpcrun(hpcrun.database_id)
    assert refetched is not None
    assert refetched.stage is None and refetched.generation is None and refetched.last_event_at is None
    assert await database_service.get_hpcrun_events_cursor(hpcrun.database_id) in (None, {})
    assert await database_service.list_hpcrun_events(hpcrun.database_id) == []
    assert await database_service.list_hpcrun_spans(hpcrun.database_id) == []


@pytest.mark.asyncio
async def test_an_older_emitter_shape_still_parses_and_stores(
    database_service: DatabaseServiceSQL,
) -> None:
    """An image built before the schema settled writes ``layer`` instead of
    ``component`` and stamps the domain keys at the top level rather than in
    ``baggage``. Those objects must still land in the table -- an image and an
    API of different vintages is the normal state during a rollout.
    """
    simulation, hpcrun = await _insert_run(database_service, correlation_id=f"c-{uuid.uuid4().hex[:6]}")
    assert hpcrun.trace_id
    old_shape = "\n".join([
        (
            '{"v": 1, "seq": 1, "ts": "2026-09-10T06:00:00.000Z", "source": "batch-legacy",'
            f' "trace_id": "{hpcrun.trace_id}", "layer": "runner", "event": "lineage.generation.start",'
            ' "level": "info", "sim_id": "749", "experiment_id": "sim184-legacy", "generation": 0,'
            ' "variant": "1", "lineage_seed": "2", "payload": {"gen_seed": 11}}'
        ),
        (
            '{"v": 1, "seq": 2, "ts": "2026-09-10T06:30:00.000Z", "source": "batch-legacy",'
            f' "trace_id": "{hpcrun.trace_id}", "layer": "runner", "event": "lineage.generation.end",'
            ' "level": "info", "generation": 0, "payload": {"duration": 2528.0}}'
        ),
    ])
    key = f"nextflow/work/{simulation.experiment_id}/events/{hpcrun.trace_id}/batch-legacy.jsonl"
    s3 = _S3({key: old_shape.encode()})

    result = await event_ingest.ingest_run_events(hpcrun, simulation, s3, database_service, _settings())

    assert result.bad_lines == 0 and result.events_inserted == 2
    stored = await database_service.list_hpcrun_events(hpcrun.database_id)
    start = next(e for e in stored if e.event == "lineage.generation.start")
    assert start.component == "runner"  # the old `layer` value, carried through rather than rejected
    assert start.generation == 0 and start.variant == 1 and start.lineage_seed == 2
    refetched = await database_service.get_hpcrun(hpcrun.database_id)
    assert refetched is not None and refetched.generation == 0
