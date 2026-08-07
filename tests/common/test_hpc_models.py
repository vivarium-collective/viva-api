import json
from pathlib import Path

import pytest

from viva_api.common.hpc.models import (
    NextflowEvent,
    NextflowMetadataEvent,
    NextflowTraceEvent,
    NextflowTraceStatus,
    parse_nextflow_event,
)


@pytest.fixture
def nextflow_events_file() -> Path:
    """Path to the test NDJSON events file."""
    return Path(__file__).parent.parent / "fixtures" / "nextflow_data" / "nextflow_test_.events.ndjson"


def test_parse_nextflow_started_event(nextflow_events_file: Path) -> None:
    """Test parsing a Nextflow 'started' metadata event."""
    with open(nextflow_events_file) as f:
        first_line = f.readline()

    data = json.loads(first_line)
    event = parse_nextflow_event(data)

    assert isinstance(event, NextflowMetadataEvent)
    assert event.event == "started"
    assert event.run_id == "d3604325-4a08-49aa-b1c7-279fea132e45"
    assert event.run_name == "berserk_escher"
    assert event.utc_time == "2025-12-30T17:03:21Z"
    assert event.metadata.workflow.success is False  # Not yet completed
    assert event.metadata.workflow.exit_status is None
    assert event.metadata.workflow.user_name == "test_user"
    assert event.metadata.workflow.nextflow.version == "25.04.6"


def test_parse_nextflow_completed_event(nextflow_events_file: Path) -> None:
    """Test parsing a Nextflow 'completed' metadata event."""
    with open(nextflow_events_file) as f:
        lines = f.readlines()

    # Last non-empty line is the completed event
    last_line = [line for line in lines if line.strip()][-1]
    data = json.loads(last_line)
    event = parse_nextflow_event(data)

    assert isinstance(event, NextflowMetadataEvent)
    assert event.event == "completed"
    assert event.run_id == "d3604325-4a08-49aa-b1c7-279fea132e45"
    assert event.metadata.workflow.success is True
    assert event.metadata.workflow.exit_status == 0
    assert event.metadata.workflow.duration == 2266
    assert event.metadata.workflow.stats.succeeded_count == 2
    assert event.metadata.workflow.stats.failed_count == 0
    assert len(event.metadata.workflow.stats.processes) == 2


def test_parse_nextflow_process_submitted_event(nextflow_events_file: Path) -> None:
    """Test parsing a Nextflow 'process_submitted' trace event."""
    with open(nextflow_events_file) as f:
        lines = f.readlines()

    # Second line is process_submitted
    data = json.loads(lines[1])
    event = parse_nextflow_event(data)

    assert isinstance(event, NextflowTraceEvent)
    assert event.event == "process_submitted"
    assert event.trace.task_id == 1
    assert event.trace.status == NextflowTraceStatus.SUBMITTED
    assert event.trace.name == "sayHello"
    assert event.trace.process == "sayHello"
    assert event.trace.hash == "d6/66e3c1"
    assert event.trace.cpus == 1
    assert event.trace.native_id is None  # Not yet assigned


def test_parse_nextflow_process_started_event(nextflow_events_file: Path) -> None:
    """Test parsing a Nextflow 'process_started' trace event."""
    with open(nextflow_events_file) as f:
        lines = f.readlines()

    # Third line is process_started
    data = json.loads(lines[2])
    event = parse_nextflow_event(data)

    assert isinstance(event, NextflowTraceEvent)
    assert event.event == "process_started"
    assert event.trace.task_id == 1
    assert event.trace.status == NextflowTraceStatus.RUNNING
    assert event.trace.native_id == 299056  # Now has OS process ID


def test_parse_nextflow_process_completed_event(nextflow_events_file: Path) -> None:
    """Test parsing a Nextflow 'process_completed' trace event."""
    with open(nextflow_events_file) as f:
        lines = f.readlines()

    # Fifth line is first process_completed (sayHello)
    data = json.loads(lines[4])
    event = parse_nextflow_event(data)

    assert isinstance(event, NextflowTraceEvent)
    assert event.event == "process_completed"
    assert event.trace.task_id == 1
    assert event.trace.status == NextflowTraceStatus.COMPLETED
    assert event.trace.exit == 0
    assert event.trace.duration == 175
    assert event.trace.realtime == 53
    assert event.trace.percent_cpu == 48.4
    assert event.trace.cpu_model == "AMD EPYC 7662 64-Core Processor"
    assert event.trace.is_completed() is True


def test_parse_all_events_from_file(nextflow_events_file: Path) -> None:
    """Test parsing all events from the fixture file."""
    events: list[NextflowEvent] = []

    with open(nextflow_events_file) as f:
        for line in f:
            if line.strip():
                data = json.loads(line)
                event = parse_nextflow_event(data)
                events.append(event)

    assert len(events) == 8

    # First event should be started
    assert isinstance(events[0], NextflowMetadataEvent)
    assert events[0].event == "started"

    # Last event should be completed
    assert isinstance(events[-1], NextflowMetadataEvent)
    assert events[-1].event == "completed"

    # Middle events should be trace events
    trace_events = [e for e in events if isinstance(e, NextflowTraceEvent)]
    assert len(trace_events) == 6

    # Count by event type
    submitted = [e for e in trace_events if e.event == "process_submitted"]
    started = [e for e in trace_events if e.event == "process_started"]
    completed = [e for e in trace_events if e.event == "process_completed"]

    assert len(submitted) == 2
    assert len(started) == 2
    assert len(completed) == 2


def test_trace_is_completed() -> None:
    """Test the is_completed helper method on trace events."""
    with open(Path(__file__).parent.parent / "fixtures" / "nextflow_data" / "nextflow_test_.events.ndjson") as f:
        lines = f.readlines()

    # Submitted - not completed
    submitted = parse_nextflow_event(json.loads(lines[1]))
    assert isinstance(submitted, NextflowTraceEvent)
    assert submitted.trace.is_completed() is False

    # Running - not completed
    running = parse_nextflow_event(json.loads(lines[2]))
    assert isinstance(running, NextflowTraceEvent)
    assert running.trace.is_completed() is False

    # Completed - is completed
    completed = parse_nextflow_event(json.loads(lines[4]))
    assert isinstance(completed, NextflowTraceEvent)
    assert completed.trace.is_completed() is True


def test_parse_unknown_event_raises_error() -> None:
    """Test that parsing an unknown event type raises ValueError."""
    data = {"event": "unknown_event", "runId": "test", "runName": "test", "utcTime": "2025-01-01T00:00:00Z"}

    with pytest.raises(ValueError, match="Unknown Nextflow event type"):
        parse_nextflow_event(data)
