"""decide_analysis_retry: pure OOM-retry-escalation decision logic (item 50 Gap 6 / PR #239)."""

from viva_api.common.analysis_retry import (
    AnalysisRetryAction,
    decide_analysis_retry,
)
from viva_api.common.models import JobStatus


def test_none_status_waits() -> None:
    decision = decide_analysis_retry(
        status=None, exit_code=None, error_message=None, current_attempt=1, base_memory_mib=60000
    )
    assert decision.action == AnalysisRetryAction.WAIT


def test_non_terminal_statuses_wait() -> None:
    for status in (JobStatus.WAITING, JobStatus.PENDING, JobStatus.QUEUED, JobStatus.RUNNING, JobStatus.UNKNOWN):
        decision = decide_analysis_retry(
            status=status, exit_code=None, error_message=None, current_attempt=1, base_memory_mib=60000
        )
        assert decision.action == AnalysisRetryAction.WAIT, status


def test_completed_succeeds() -> None:
    decision = decide_analysis_retry(
        status=JobStatus.COMPLETED, exit_code=None, error_message=None, current_attempt=1, base_memory_mib=60000
    )
    assert decision.action == AnalysisRetryAction.SUCCEED


def test_oom_on_attempt_1_escalates_to_2x_baseline() -> None:
    decision = decide_analysis_retry(
        status=JobStatus.FAILED, exit_code="137", error_message=None, current_attempt=1, base_memory_mib=60000
    )
    assert decision.action == AnalysisRetryAction.ESCALATE
    assert decision.next_attempt == 2
    assert decision.next_memory_mib == 120000  # base x (attempt+1) = 60000 x 2


def test_oom_on_attempt_2_escalates_to_3x_baseline_not_compounded() -> None:
    """next_mib is base_memory_mib x next_attempt, never the PREVIOUS escalated value x
    next_attempt — a caller passing the same base_memory_mib each time must see 3x, not
    2x x 3 = 6x."""
    decision = decide_analysis_retry(
        status=JobStatus.FAILED, exit_code="137", error_message=None, current_attempt=2, base_memory_mib=60000
    )
    assert decision.action == AnalysisRetryAction.ESCALATE
    assert decision.next_attempt == 3
    assert decision.next_memory_mib == 180000  # base x (attempt+1) = 60000 x 3


def test_oom_at_max_attempts_fails_permanently() -> None:
    decision = decide_analysis_retry(
        status=JobStatus.FAILED,
        exit_code="137",
        error_message=None,
        current_attempt=3,
        base_memory_mib=60000,
        max_attempts=3,
    )
    assert decision.action == AnalysisRetryAction.FAIL
    assert decision.error_message == "OOM: retries exhausted"


def test_non_oom_failure_fails_permanently_even_on_attempt_1() -> None:
    decision = decide_analysis_retry(
        status=JobStatus.FAILED,
        exit_code="1",
        error_message="ModuleNotFoundError: no module named 'reports'",
        current_attempt=1,
        base_memory_mib=60000,
    )
    assert decision.action == AnalysisRetryAction.FAIL
    assert decision.error_message == "ModuleNotFoundError: no module named 'reports'"


def test_non_oom_failure_with_no_error_message_gets_a_default() -> None:
    decision = decide_analysis_retry(
        status=JobStatus.FAILED, exit_code="1", error_message=None, current_attempt=1, base_memory_mib=60000
    )
    assert decision.action == AnalysisRetryAction.FAIL
    assert decision.error_message == "analysis job failed"


def test_cancelled_status_fails_permanently() -> None:
    decision = decide_analysis_retry(
        status=JobStatus.CANCELLED, exit_code=None, error_message=None, current_attempt=1, base_memory_mib=60000
    )
    assert decision.action == AnalysisRetryAction.FAIL


def test_custom_max_attempts_respected() -> None:
    """A caller-supplied max_attempts of 1 means even a first-attempt OOM fails
    permanently — no escalation attempted at all."""
    decision = decide_analysis_retry(
        status=JobStatus.FAILED,
        exit_code="137",
        error_message=None,
        current_attempt=1,
        base_memory_mib=60000,
        max_attempts=1,
    )
    assert decision.action == AnalysisRetryAction.FAIL
