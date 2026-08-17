"""OOM-retry-escalation decision logic for the analysis DAG node — backlog item 38
track B / item 50 Gap 6.

Extracted as a pure, backend/data-shape-agnostic function so it can be shared between
a poller resolving state from the legacy ``analysis`` table (``Simulation``/``Simulator``,
see viva-api PR #239, not yet landed) and one resolving from the compose subsystem's
``compose_analysis`` table (``ComposeSimulation``/``ComposeSimulatorVersion``,
``ComposeJobMonitor``) — the decision itself (terminal? OOM? escalate or give up?) does
not care which table the caller fetched its state from, only the data-fetch/resubmit
plumbing differs (item 50 backlog `50.md` Part 5, point 3).

Mirrors vEcoli-private's own Nextflow ``scaledMemory`` closure + ``maxRetries``
(``runscripts/nextflow/config.template``): each retry multiplies the baseline memory by
``attempt + 1``, capped at ``max_attempts`` attempts, then gives up (matching Nextflow's
own graceful ``ignore`` after exhausting retries).
"""

from dataclasses import dataclass
from enum import StrEnum

from viva_api.common.models import JobStatus

# Mirrors vEcoli-private's own Nextflow `maxRetries = 3` (runscripts/nextflow/config.template)
# exactly — backlog item 38 track B.
DEFAULT_MAX_ANALYSIS_ATTEMPTS = 3

# AWS Batch's container exitCode for an OOM-killed process (SIGKILL, 128+9).
OOM_EXIT_CODE = "137"

# A job in any of these states is still in flight (or transiently unreadable) —
# nothing to decide yet, poll again next interval.
_NON_TERMINAL = (JobStatus.WAITING, JobStatus.PENDING, JobStatus.QUEUED, JobStatus.RUNNING, JobStatus.UNKNOWN)


class AnalysisRetryAction(StrEnum):
    WAIT = "wait"  # still in flight (or status unreadable) — do nothing this poll
    SUCCEED = "succeed"  # job COMPLETED — mark the analysis READY
    ESCALATE = "escalate"  # OOM'd with retries remaining — resubmit at next_memory_mib
    FAIL = "fail"  # non-OOM failure, or OOM with retries exhausted — mark FAILED


@dataclass(frozen=True)
class AnalysisRetryDecision:
    action: AnalysisRetryAction
    next_attempt: int | None = None
    next_memory_mib: int | None = None
    error_message: str | None = None


def decide_analysis_retry(
    *,
    status: JobStatus | None,
    exit_code: str | None,
    error_message: str | None,
    current_attempt: int,
    base_memory_mib: int,
    max_attempts: int = DEFAULT_MAX_ANALYSIS_ATTEMPTS,
) -> AnalysisRetryDecision:
    """Decide what to do with one COMPUTING analysis row given its Batch job's real status.

    ``status``/``exit_code``/``error_message`` are exactly the fields
    ``SimulationServiceRay.get_job_status`` (a ``JobStatusInfo``) already returns for any
    MNP job — this function makes no AWS calls itself, it only decides given what the
    caller already fetched. ``base_memory_mib`` is the attempt-1 baseline (from
    ``analysis_memory_mib_for`` or a caller-supplied CDK default) — escalation always
    multiplies THIS baseline by the next attempt number, never compounds off the
    previous attempt's already-escalated value (matching PR #239's real, tested formula).
    """
    if status is None or status in _NON_TERMINAL:
        return AnalysisRetryDecision(action=AnalysisRetryAction.WAIT)

    if status == JobStatus.COMPLETED:
        return AnalysisRetryDecision(action=AnalysisRetryAction.SUCCEED)

    is_oom = exit_code == OOM_EXIT_CODE
    if is_oom and current_attempt < max_attempts:
        next_attempt = current_attempt + 1
        next_mib = base_memory_mib * next_attempt  # mirrors Nextflow's `1.GB * baseMem * task.attempt`
        return AnalysisRetryDecision(
            action=AnalysisRetryAction.ESCALATE,
            next_attempt=next_attempt,
            next_memory_mib=next_mib,
        )

    return AnalysisRetryDecision(
        action=AnalysisRetryAction.FAIL,
        error_message=error_message or ("OOM: retries exhausted" if is_oom else "analysis job failed"),
    )
