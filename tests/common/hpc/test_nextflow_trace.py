"""trace.csv is the Nextflow head's real accounting (observability plan D4c).

Every trace here is a REAL artifact of a local `nextflow run` of the toy
workflow in tests/fixtures/nextflow/ (see its README for the exact commands),
not a hand-written row -- except the Spot-reclaimed shape, which a laptop cannot
produce.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from viva_api.common.handlers.simulations import _truncate_log
from viva_api.common.hpc import nextflow_trace as nt
from viva_api.common.models import JobStatus
from viva_api.common.storage.file_service import ListingItem

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "nextflow"


def _fixture(name: str) -> str:
    return (FIXTURES / name).read_text()


def test_parses_a_real_trace_file() -> None:
    rows = nt.parse_trace_csv(_fixture("trace.failed.csv"))
    assert [r.name for r in rows] == ["parca_v0", "lineage_v0_s0", "analysis_v0"]
    assert rows[2].status == "FAILED" and rows[2].exit == 1 and rows[2].failed
    assert rows[2].hash == "5c/fdb081" and rows[2].native_id and rows[2].task_id == "3"
    assert rows[0].succeeded and rows[0].exit == 0
    assert rows[0].raw["submit"].startswith("2026-09-10")
    assert nt.parse_trace_csv("") == []
    assert nt.parse_trace_csv(_fixture("trace.ok.csv").splitlines()[0]) == []  # header only


def test_all_completed_is_completed() -> None:
    summary = nt.summarize(nt.parse_trace_csv(_fixture("trace.ok.csv")))
    assert summary.all_succeeded and summary.total == 3 and summary.max_attempt == 1
    assert nt.classify_run(0, summary) is JobStatus.COMPLETED
    assert summary.describe() == "3 tasks, 3 completed"


def test_a_resumed_run_is_all_cached_and_counts_as_success() -> None:
    summary = nt.summarize(nt.parse_trace_csv(_fixture("trace.cached.csv")))
    assert summary.cached == 3 and summary.completed == 0
    assert nt.classify_run(0, summary) is JobStatus.COMPLETED


def test_a_failed_task_among_completed_ones_is_failed_and_names_the_task() -> None:
    """Sim 749's shape: the head exits non-zero under `errorStrategy finish` after
    one task failed while every other task completed.

    The status is FAILED either way -- the run did not deliver what was asked of
    it. What the trace buys is not a different *label* but a usable *message*:
    the failing task is named, so nobody has to open CloudWatch to find out
    which one, and the head's exit code is no longer the sole evidence.
    """
    summary = nt.summarize(nt.parse_trace_csv(_fixture("trace.failed.csv")))
    assert summary.completed == 2 and summary.failed == 1
    assert [r.name for r in nt.final_failed_rows(summary)] == ["analysis_v0"]
    assert nt.classify_run(1, summary) is JobStatus.FAILED
    assert nt.classify_run(0, summary) is JobStatus.FAILED
    headline = nt.failure_headline(1, summary, "head pod Error")
    assert "analysis_v0" in headline and "head exit 1" in headline and "head pod Error" in headline


def test_nothing_completed_is_failed_and_no_rows_is_failed() -> None:
    rows = [r for r in nt.parse_trace_csv(_fixture("trace.failed.csv")) if r.failed]
    assert nt.classify_run(1, nt.summarize(rows)) is JobStatus.FAILED
    assert nt.classify_run(0, nt.summarize([])) is JobStatus.FAILED


def test_all_succeeded_but_head_nonzero_is_failed() -> None:
    """The head's exit code is the tie-break only when the science ran: a
    stage-out failure after every task completed is still a failed run."""
    summary = nt.summarize(nt.parse_trace_csv(_fixture("trace.ok.csv")))
    assert nt.classify_run(1, summary) is JobStatus.FAILED


def test_a_retry_that_later_succeeded_is_not_a_run_failure() -> None:
    """`flaky_v0` exits 137 on attempt 1 under `errorStrategy retry` and completes
    on attempt 2: two rows, one FAILED, and a COMPLETED run."""
    summary = nt.summarize(nt.parse_trace_csv(_fixture("trace.retry.csv")))
    assert summary.total == 5 and summary.failed == 1 and summary.max_attempt == 2
    assert summary.failed_rows[0].name == "flaky_v0" and summary.failed_rows[0].exit == 137
    assert nt.final_failed_rows(summary) == []
    assert nt.classify_run(0, summary) is JobStatus.COMPLETED


def test_reclaimed_task_with_no_exit_code_is_failed_not_a_crash() -> None:
    """Hand-written: Batch retries a Spot reclaim internally, and the exhausted
    case reaches Nextflow with no exit code (`-`). A laptop cannot produce it."""
    header = _fixture("trace.ok.csv").splitlines()[0]
    ok_row = _fixture("trace.ok.csv").splitlines()[1]
    reclaimed = "9\tab/cdef01\t-\tlineage_v0_s1\tABORTED\t-\t2026-09-10 02:10:08.500\t-\t-\t-\t-\t-\t-\t-"
    summary = nt.summarize(nt.parse_trace_csv("\n".join([header, ok_row, reclaimed])))
    assert summary.failed_rows[0].exit is None
    assert nt.classify_run(0, summary) is JobStatus.FAILED


def _fake_file_service(keys: list[str], contents: dict[str, bytes]) -> MagicMock:
    fs = MagicMock()
    fs.get_listing = AsyncMock(
        return_value=[ListingItem(Key=k, LastModified=datetime.now(UTC), ETag="e", Size=1) for k in keys]
    )

    async def _get(s3_path: object) -> bytes | None:
        return contents.get(str(getattr(s3_path, "s3_path", s3_path)))

    fs.get_file_contents = AsyncMock(side_effect=_get)
    return fs


@pytest.mark.asyncio
async def test_fetch_command_err_resolves_the_short_hash_and_tails_the_real_log() -> None:
    """The trace's `hash` is `5c/fdb081`; the work dir the run actually wrote is
    `5c/fdb0811581f9f8de0dfbe254e5b6bd` (fixtures README) -- resolved by listing."""
    row = nt.parse_trace_csv(_fixture("trace.failed.csv"))[2]
    work = "s3://mybucket/nextflow/work/exp1/work"
    workdir = "nextflow/work/exp1/work/5c/fdb0811581f9f8de0dfbe254e5b6bd"
    fs = _fake_file_service(
        keys=[f"{workdir}/.command.sh", f"{workdir}/.command.err", "nextflow/work/exp1/work/5c/other000/.command.err"],
        contents={f"{workdir}/.command.err": (FIXTURES / "command.err.failed").read_bytes()},
    )
    text = await nt.fetch_command_err(fs, work, row)
    assert text is not None
    assert text.startswith("Nextflow task analysis_v0 (exit 1) failed; .command.err tail:")
    assert text.endswith("FileNotFoundError: [Errno 2] No such file or directory: 'analysis.config.json'")
    listed = fs.get_listing.await_args.args[0]
    assert str(getattr(listed, "s3_path", listed)) == str(Path("nextflow/work/exp1/work/5c"))


@pytest.mark.asyncio
async def test_fetch_command_err_tails_long_logs_and_is_none_when_the_work_dir_is_gone() -> None:
    row = nt.parse_trace_csv(_fixture("trace.failed.csv"))[2]
    workdir = "nextflow/work/exp1/work/5c/fdb0811581f9f8de0dfbe254e5b6bd"
    long_err = ("\n".join(f"line {i}" for i in range(50)) + "\nValueError: boom\n").encode()
    fs = _fake_file_service(keys=[f"{workdir}/.command.err"], contents={f"{workdir}/.command.err": long_err})
    text = await nt.fetch_command_err(fs, "s3://mybucket/nextflow/work/exp1/work", row, tail=10)
    assert text is not None and "earlier lines elided" in text and text.endswith("ValueError: boom")
    assert await nt.fetch_command_err(_fake_file_service([], {}), "s3://mybucket/nextflow/work/exp1/work", row) is None


def test_a_real_nextflow_log_truncates_to_header_plus_final_block() -> None:
    """The `/log?truncate=true` view of the failed run's real `.nextflow.log`:
    the head (Nextflow banner + config) and everything from the last `executor`
    line, which is where the failure summary lives."""
    log = _fixture("nextflow.failed.log")
    truncated = _truncate_log(log)
    assert "... truncated ..." in truncated
    assert truncated.startswith(log.splitlines(keepends=True)[0])
    assert "WorkflowStats[succeededCount=2; failedCount=1" in truncated
    assert truncated.rstrip().endswith("Execution complete -- Goodbye")
