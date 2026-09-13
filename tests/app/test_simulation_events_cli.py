"""CLI tests for ``atlantis simulation events`` / ``tasks`` and the richer ``status`` panel.

They pin the WIRING and the rendering, not the values: a captured task stream
(``tests/fixtures/events``) is fed through the same parser/tree builder the API
uses, and the CLI must show the events, the span tree, and the progress fields
without the API or AWS.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from app.cli import cli as cli_app
from viva_api.common.models import JobStatus
from viva_api.simulation import event_ingest
from viva_api.simulation.models import SimulationEvents, SimulationRun, SimulationTask

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "events"
runner = CliRunner(env={"COLUMNS": "200"})  # Rich sizes tables to $COLUMNS; a 80-col fake tty hides the payload column


def _page(name: str, *, tree: bool) -> SimulationEvents:
    events, _ = event_ingest.parse_event_lines((FIXTURES / name).read_text())
    stored = [e for e in events if e.event not in event_ingest.UNSTORED_EVENTS]
    for i, e in enumerate(stored, start=1):
        e.cursor = i
    spans: dict[str, Any] = {}
    event_ingest.apply_span_events(spans, events)
    page = SimulationEvents(id=943, trace_id="4bf92f3577b34da6a3ce929d0e0e4736", events=stored, next=None)
    if tree:
        page.tree = event_ingest.build_span_tree(list(spans.values()), stored)
    return page


def _svc(name: str = "lineage_failed_gen1.jsonl") -> MagicMock:
    svc = MagicMock()
    svc.get_workflow_events.side_effect = lambda simulation_id, **kw: _page(name, tree=bool(kw.get("tree")))
    svc.get_workflow_status.return_value = SimulationRun(id=943, status=JobStatus.FAILED)
    svc.get_workflow_tasks.return_value = [
        SimulationTask(
            name="parca_v0", status="COMPLETED", job_id="aaa-111", task_hash="aa/111111", exit_code=0, attempt=1
        ),
        SimulationTask(
            name="runs_v0:lineage_v0_s0",
            status="FAILED",
            job_id="bbb-222",
            task_hash="bb/222222",
            exit_code=1,
            attempt=1,
        ),
    ]
    return svc


def test_events_table_lists_runner_and_engine_events_but_never_ticks() -> None:
    svc = _svc()
    with patch("app.cli.get_data_service", return_value=svc):
        result = runner.invoke(cli_app, ["simulation", "events", "943"])
    assert result.exit_code == 0, result.output
    out = result.output
    assert "lineage.generation.start" in out and "process.exception" in out and "lineage.failure" in out
    assert "NegativeCountsError" in out
    assert "tick" not in out.replace("stick", "")
    kwargs = svc.get_workflow_events.call_args.kwargs
    assert kwargs["simulation_id"] == 943 and kwargs["tree"] is False and kwargs["after"] is None


def test_events_filters_are_forwarded() -> None:
    svc = _svc()
    with patch("app.cli.get_data_service", return_value=svc):
        result = runner.invoke(
            cli_app,
            ["simulation", "events", "943", "--level", "error", "--generation", "1", "--event", "process.exception"],
        )
    assert result.exit_code == 0, result.output
    kwargs = svc.get_workflow_events.call_args.kwargs
    assert kwargs["level"] == "error" and kwargs["generation"] == 1 and kwargs["event"] == "process.exception"


def test_events_tree_renders_the_span_tree_with_the_failure() -> None:
    svc = _svc()
    with patch("app.cli.get_data_service", return_value=svc):
        result = runner.invoke(cli_app, ["simulation", "events", "943", "--tree"])
    assert result.exit_code == 0, result.output
    out = result.output
    assert "lineage[variant=0,lineage_seed=0]" in out
    assert "generation[generation=0]" in out and "generation[generation=1]" in out
    assert "error" in out and "NegativeCountsError" in out
    assert "lineage.division" in out  # a notable event listed under its span
    assert svc.get_workflow_events.call_args.kwargs["tree"] is True


def test_events_follow_stops_when_the_run_is_terminal() -> None:
    svc = _svc("lineage_running_gen1.jsonl")
    with patch("app.cli.get_data_service", return_value=svc), patch("time.sleep"):
        result = runner.invoke(cli_app, ["simulation", "events", "943", "--follow"])
    assert result.exit_code == 0, result.output
    assert "Run is failed; no more events." in result.output
    svc.get_workflow_status.assert_called_once()


def test_tasks_table() -> None:
    svc = _svc()
    with patch("app.cli.get_data_service", return_value=svc):
        result = runner.invoke(cli_app, ["simulation", "tasks", "943"])
    assert result.exit_code == 0, result.output
    assert "runs_v0:lineage_v0_s0" in result.output and "FAILED" in result.output and "bbb-222" in result.output


def test_status_panel_shows_the_progress_fields() -> None:
    from viva_api.common.handlers.simulations import _progress_lines

    run = SimulationRun(
        id=943,
        status=JobStatus.RUNNING,
        stage="lineage[variant=0,lineage_seed=0] > generation[generation=1]",
        generation=1,
        last_event_at="2026-09-10 06:48:02",
        attempt=1,
    )
    text = _progress_lines(run)
    # Rich markup: the square brackets of a span label are escaped so they render literally
    assert "stage: lineage\\[variant=0,lineage_seed=0] > generation\\[generation=1]" in text
    assert "generation: 1" in text and "last event: 2026-09-10 06:48:02" in text and "attempt: 1" in text
    assert _progress_lines(SimulationRun(id=1, status=JobStatus.RUNNING)) == ""


# ---------------------------------------------------------------------------
# legacy runs: no events recorded, and the CLI has to say so rather than crash
# ---------------------------------------------------------------------------


def _empty_svc() -> MagicMock:
    """A run with no observability records at all -- the shape every campaign
    dispatched before the emitters shipped will keep for the rest of its life."""
    svc = MagicMock()
    svc.get_workflow_events.side_effect = lambda simulation_id, **kw: SimulationEvents(
        id=749, trace_id=None, events=[], next=None, tree=[] if kw.get("tree") else None
    )
    svc.get_workflow_status.return_value = SimulationRun(id=749, status=JobStatus.COMPLETED)
    svc.get_workflow_tasks.return_value = []
    return svc


def test_events_on_a_legacy_run_says_so_and_exits_zero() -> None:
    svc = _empty_svc()
    with patch("app.cli.get_data_service", return_value=svc):
        result = runner.invoke(cli_app, ["simulation", "events", "749"])
    assert result.exit_code == 0, result.output
    assert "No events recorded yet" in result.output
    assert "Traceback" not in result.output


def test_events_tree_on_a_legacy_run_says_so_and_exits_zero() -> None:
    svc = _empty_svc()
    with patch("app.cli.get_data_service", return_value=svc):
        result = runner.invoke(cli_app, ["simulation", "events", "749", "--tree"])
    assert result.exit_code == 0, result.output
    assert "No spans recorded yet" in result.output
    assert "Traceback" not in result.output


def test_tasks_on_a_legacy_run_says_so_and_exits_zero() -> None:
    svc = _empty_svc()
    with patch("app.cli.get_data_service", return_value=svc):
        result = runner.invoke(cli_app, ["simulation", "tasks", "749"])
    assert result.exit_code == 0, result.output
    assert "No tasks reported for this run" in result.output
    assert "Traceback" not in result.output


def test_status_of_a_legacy_run_renders_the_classic_panels_only() -> None:
    """``atlantis simulation status`` on a run with no progress fields must
    look exactly as it did before this plan: the Workflow Log and Simulation
    Status panels, and no half-filled progress line offering ``stage: None``."""
    svc = _empty_svc()
    svc.get_workflow_log.return_value = "N E X T F L O W\nexecutor >  awsbatch (3)\nCompleted at: ...\n"
    with patch("app.app_data_service.E2EDataService", return_value=svc):
        result = runner.invoke(cli_app, ["simulation", "status", "749"])
    assert result.exit_code == 0, result.output
    out = result.output
    assert "Workflow Log" in out and "Simulation Status" in out and "COMPLETED" in out
    for absent in ("stage:", "generation:", "last event:", "attempt:", "exit code:"):
        assert absent not in out, absent
