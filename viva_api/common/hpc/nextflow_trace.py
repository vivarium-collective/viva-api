"""Read a Nextflow run's own accounting: ``trace.csv`` and a failed task's ``.command.err``.

A Nextflow head on this stack exits with ``errorStrategy finish`` semantics: a
task that fails after its retries stops the campaign from submitting more work,
but the head itself may still exit 0 with every OTHER task complete (sim 749:
100/100 generations published, the per-variant gather dead). The Kubernetes
Job condition sees only the pod's exit code, so the run's status was read from
the wrong source. ``trace.csv`` -- written by ``-with-trace`` for every task,
including retries and cached reuses -- is the head's real accounting, and it is
staged to the run's results prefix by ``_render_nf_command``. This module
parses it and turns it, together with the head's exit code, into a run status
and an ``error_message`` that names the task and shows its traceback
(observability plan D4c).

Everything here is pure or takes a ``FileService``; no AWS client is built.
"""

from __future__ import annotations

import csv
import io
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from viva_api.common.models import JobStatus

if TYPE_CHECKING:
    from viva_api.common.storage.file_service import FileService

logger = logging.getLogger(__name__)

#: Task statuses Nextflow writes to the trace file.
COMPLETED_STATUSES = frozenset({"COMPLETED", "CACHED"})
FAILED_STATUSES = frozenset({"FAILED", "ABORTED"})

#: How many lines of ``.command.err`` to keep for ``error_message``.
COMMAND_ERR_TAIL_LINES = 40


@dataclass(frozen=True)
class TraceRow:
    """One row of ``trace.csv``. A retried task appears once per attempt with the
    same ``name`` and a new ``task_id``/``hash``; a reused one shows ``CACHED``."""

    task_id: str
    hash: str
    native_id: str
    name: str
    status: str
    exit: int | None
    raw: dict[str, str] = field(default_factory=dict, compare=False)

    @property
    def process(self) -> str:
        """The process name without the ``(tag)`` suffix, e.g. ``analysis_v8``."""
        return self.name.split(" (", 1)[0].strip()

    @property
    def succeeded(self) -> bool:
        return self.status in COMPLETED_STATUSES

    @property
    def failed(self) -> bool:
        return self.status in FAILED_STATUSES


@dataclass(frozen=True)
class TraceSummary:
    rows: tuple[TraceRow, ...]
    completed: int
    cached: int
    failed: int
    other: int
    max_attempt: int
    failed_rows: tuple[TraceRow, ...]

    @property
    def total(self) -> int:
        return len(self.rows)

    @property
    def all_succeeded(self) -> bool:
        return self.total > 0 and self.failed == 0 and self.other == 0

    def describe(self) -> str:
        parts = [f"{self.total} tasks", f"{self.completed} completed"]
        if self.cached:
            parts.append(f"{self.cached} cached")
        if self.failed:
            parts.append(f"{self.failed} failed")
        if self.other:
            parts.append(f"{self.other} not terminal")
        return ", ".join(parts)


def _parse_exit(value: str | None) -> int | None:
    if value is None:
        return None
    value = value.strip()
    if not value or value == "-":
        return None
    try:
        return int(value)
    except ValueError:
        return None


def parse_trace_csv(text: str) -> list[TraceRow]:
    """Parse Nextflow's trace file (tab-separated, header row). Tolerates an
    empty file and unknown columns; a row missing ``name``/``status`` is skipped."""
    if not text or not text.strip():
        return []
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")
    rows: list[TraceRow] = []
    for record in reader:
        name = (record.get("name") or "").strip()
        status = (record.get("status") or "").strip().upper()
        if not name or not status:
            continue
        rows.append(
            TraceRow(
                task_id=(record.get("task_id") or "").strip(),
                hash=(record.get("hash") or "").strip(),
                native_id=(record.get("native_id") or "").strip(),
                name=name,
                status=status,
                exit=_parse_exit(record.get("exit")),
                raw={k: v for k, v in record.items() if k is not None and v is not None},
            )
        )
    return rows


def summarize(rows: list[TraceRow]) -> TraceSummary:
    """Counts plus the failed rows. ``max_attempt`` is the largest number of
    rows sharing one task ``name`` -- a retried task is listed once per attempt."""
    completed = sum(1 for r in rows if r.status == "COMPLETED")
    cached = sum(1 for r in rows if r.status == "CACHED")
    failed_rows = tuple(r for r in rows if r.failed)
    other = len(rows) - completed - cached - len(failed_rows)
    attempts: dict[str, int] = {}
    for row in rows:
        attempts[row.name] = attempts.get(row.name, 0) + 1
    return TraceSummary(
        rows=tuple(rows),
        completed=completed,
        cached=cached,
        failed=len(failed_rows),
        other=other,
        max_attempt=max(attempts.values(), default=0),
        failed_rows=failed_rows,
    )


def final_failed_rows(summary: TraceSummary) -> list[TraceRow]:
    """The failed rows whose task never later succeeded -- a retry that passed
    on attempt 2 leaves a FAILED row behind that is not a run failure."""
    succeeded_names = {r.name for r in summary.rows if r.succeeded}
    return [r for r in summary.failed_rows if r.name not in succeeded_names]


def classify_run(head_exit_code: int | None, summary: TraceSummary) -> JobStatus:
    """The run's status from its trace, with the head's exit code as the tie-break.

    The trace decides, not the head's exit code, because under ``errorStrategy
    finish`` the head exits non-zero after ANY task failure -- sim 749's head
    exited 1 with 100/100 generations published and one gather dead, and reading
    the exit code alone reported that as a total failure:

    * no trace rows at all -> FAILED (the head never reached a task: a render
      error, a missing cache, ...);
    * any task that never succeeded -> FAILED, however many others completed;
    * every task COMPLETED/CACHED -> COMPLETED, unless the head itself exited
      non-zero (a publish/stage-out failure after the science ran) -> FAILED.

    **There is deliberately no "partly succeeded" status.** A run that stopped
    with only some results stopped *because something failed*, so FAILED is the
    honest label; which tasks survived is a question for the per-task rows
    (``/simulations/{id}/tasks``) and ``error_message``, which carry it at far
    higher resolution than any single label could. What actually fixed 749's
    mislabelling was reading the trace here instead of trusting the head's exit
    code -- not a new name for one of the outcomes.
    """
    if summary.total == 0:
        return JobStatus.FAILED
    if final_failed_rows(summary) or summary.other:
        return JobStatus.FAILED
    if head_exit_code not in (None, 0):
        return JobStatus.FAILED
    return JobStatus.COMPLETED


def _key_from_uri(uri: str) -> str:
    return uri.removeprefix("s3://").split("/", 1)[1] if uri.startswith("s3://") else uri


async def find_task_workdir(file_service: FileService, work_dir_uri: str, task_hash: str) -> str | None:
    """Resolve a trace row's short ``hash`` (``1b/18aa5e``) to the task's work
    dir key under ``work_dir_uri`` (``s3://bucket/nextflow/work/<exp>/work``)."""
    from viva_api.common.storage.file_paths import S3FilePath

    if "/" not in task_hash:
        return None
    bucket_dir, rest = task_hash.split("/", 1)
    prefix = f"{_key_from_uri(work_dir_uri).rstrip('/')}/{bucket_dir}/"
    try:
        listing = await file_service.get_listing(S3FilePath(s3_path=Path(prefix)))
    except Exception:
        logger.debug("could not list %s for task hash %s", prefix, task_hash)
        return None
    for item in listing:
        key = str(item.Key)
        remainder = key[len(prefix) :] if key.startswith(prefix) else key.split(f"/{bucket_dir}/", 1)[-1]
        first = remainder.split("/", 1)[0]
        if first.startswith(rest):
            return f"{prefix}{first}"
    return None


def tail_lines(text: str, n: int = COMMAND_ERR_TAIL_LINES) -> str:
    lines = text.splitlines()
    if len(lines) <= n:
        return text.strip()
    return "\n".join([f"… ({len(lines) - n} earlier lines elided)", *lines[-n:]])


async def fetch_command_err(
    file_service: FileService, work_dir_uri: str, row: TraceRow, tail: int = COMMAND_ERR_TAIL_LINES
) -> str | None:
    """The tail of a failed task's ``.command.err`` (falling back to
    ``.command.log``), prefixed with the task name and exit code -- or ``None``
    when the work dir cannot be found or read."""
    from viva_api.common.storage.file_paths import S3FilePath

    workdir = await find_task_workdir(file_service, work_dir_uri, row.hash)
    if workdir is None:
        return None
    for filename in (".command.err", ".command.log"):
        try:
            content = await file_service.get_file_contents(S3FilePath(s3_path=Path(f"{workdir}/{filename}")))
        except Exception:
            content = None
        if content:
            text = content.decode("utf-8", errors="replace")
            if text.strip():
                exit_note = f" (exit {row.exit})" if row.exit is not None else ""
                return f"Nextflow task {row.name}{exit_note} failed; {filename} tail:\n{tail_lines(text, tail)}"
    return None


def failure_headline(head_exit_code: int | None, summary: TraceSummary, head_reason: str | None = None) -> str:
    """A one-line error message when no task log could be fetched."""
    failed = final_failed_rows(summary)
    names = ", ".join(sorted({r.name for r in failed})) or "(no failed task)"
    parts = [f"Nextflow run: {summary.describe()}; failed tasks: {names}"]
    if head_exit_code not in (None, 0):
        parts.append(f"head exit {head_exit_code}")
    if head_reason:
        parts.append(head_reason)
    return "; ".join(parts)
