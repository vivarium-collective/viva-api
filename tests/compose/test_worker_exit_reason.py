"""Why a worker stopped, from the cluster rather than from the socket.

A worker that dies mid-call gives the caller exactly one fact — the socket
closed — and that fact is identical for an OOM kill, a segfault, a node
eviction and a deliberate `kubectl delete`. None of those call for the same
response from a user, so "worker closed the connection" on its own is a dead
end. Kubernetes records the cause on the pod; these tests pin that it is read
and carried into the task's error, and that failing to read it is never worse
than not trying.

Found the slow way: a manufactured partial-failure run on dev returned
`failed / "worker closed the connection"`, and only `kubectl get pod -o
jsonpath` revealed `OOMKilled (exit 137)` — a 2Gi limit, not a bug in the study.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from viva_api.common.hpc.k8s_job_service import K8sJobService
from viva_api.compose.env_worker_relay import TaskRunner, WorkerUnavailable

JOB = "env-worker-f78672f-partial1-f44cb5fd"


def _pod(*, reason: str | None, exit_code: int | None) -> SimpleNamespace:
    terminated = SimpleNamespace(reason=reason, exit_code=exit_code)
    return SimpleNamespace(
        status=SimpleNamespace(container_statuses=[SimpleNamespace(state=SimpleNamespace(terminated=terminated))])
    )


def _service(pods: list[object]) -> K8sJobService:
    svc = K8sJobService.__new__(K8sJobService)
    svc._namespace = "sms-api-stanford-test"
    api = MagicMock()
    api.list_namespaced_pod.return_value = SimpleNamespace(items=pods)
    svc._core_api = api
    return svc


# --- reading the pod --------------------------------------------------------


def test_an_oom_kill_is_named_along_with_its_exit_code() -> None:
    """The real case. 137 alone is a shibboleth; "OOMKilled" is the answer."""
    assert _service([_pod(reason="OOMKilled", exit_code=137)]).get_pod_termination(JOB) == "OOMKilled (exit 137)"


def test_an_ordinary_crash_is_reported_too() -> None:
    """Not only OOM — anything the pod terminated with is more than the socket knew."""
    assert _service([_pod(reason="Error", exit_code=1)]).get_pod_termination(JOB) == "Error (exit 1)"


def test_a_pod_still_running_yields_nothing_rather_than_a_guess() -> None:
    """A socket can die while the pod lives (a desync, a network drop). Saying
    nothing is correct; inventing a termination would be worse than silence."""
    running = SimpleNamespace(
        status=SimpleNamespace(container_statuses=[SimpleNamespace(state=SimpleNamespace(terminated=None))])
    )
    assert _service([running]).get_pod_termination(JOB) is None


def test_a_reaped_pod_yields_nothing() -> None:
    """Job TTL deletes pods. Asking after that is routine, not an error."""
    assert _service([]).get_pod_termination(JOB) is None


def test_an_unreachable_api_yields_nothing_rather_than_raising() -> None:
    """This is only ever called on a path where something already failed."""
    from kubernetes import client as k8s_client

    svc = _service([])
    svc._core_api.list_namespaced_pod.side_effect = k8s_client.rest.ApiException(status=403)
    assert svc.get_pod_termination(JOB) is None


# --- carrying it into the task record ---------------------------------------


@pytest.mark.asyncio
async def test_the_task_error_carries_the_pod_reason() -> None:
    """What the caller actually reads. Both halves must survive: the socket's
    account (what this process saw) and the cluster's (why it happened)."""
    runner = TaskRunner(MagicMock(), explain_exit=lambda job: "OOMKilled (exit 137)")
    msg = await runner._describe_unavailable(JOB, WorkerUnavailable("worker closed the connection"))
    assert msg == "worker closed the connection (worker pod: OOMKilled (exit 137))"


@pytest.mark.asyncio
async def test_without_a_reason_the_message_is_unchanged() -> None:
    """No reason available must not add an empty parenthetical that reads like
    a bug — the message degrades to exactly what it was before this existed."""
    runner = TaskRunner(MagicMock(), explain_exit=lambda job: None)
    msg = await runner._describe_unavailable(JOB, WorkerUnavailable("worker closed the connection"))
    assert msg == "worker closed the connection"


@pytest.mark.asyncio
async def test_a_deployment_without_the_hook_still_reports_the_failure() -> None:
    """explain_exit is optional; a SLURM/no-K8s deployment is not degraded."""
    runner = TaskRunner(MagicMock())
    msg = await runner._describe_unavailable(JOB, WorkerUnavailable("worker closed the connection"))
    assert msg == "worker closed the connection"


@pytest.mark.asyncio
async def test_a_raising_explainer_does_not_swallow_the_real_error() -> None:
    """The whole point is to say more about a failure. Saying less — or raising
    a second, unrelated error — would make the diagnosis worse than absent."""

    def boom(job: str) -> str | None:
        raise RuntimeError("API server unreachable")

    runner = TaskRunner(MagicMock(), explain_exit=boom)
    msg = await runner._describe_unavailable(JOB, WorkerUnavailable("worker closed the connection"))
    assert msg == "worker closed the connection"
