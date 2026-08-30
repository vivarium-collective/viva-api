"""The atlantis worker task tier (plan §E option (e), step 7).

Hermetic: httpx is mocked at the transport, so nothing here can reach a
deployment. That matters more than usual in this file -- `BaseUrl.LOCAL_8080`,
the default, IS the SSM tunnel to dev on a developer's laptop, and a test that
forgot to mock would submit real work to a real cluster.

What is pinned:

* the identity header is OPT-IN and is not authentication -- absent config means
  anonymous, which is the normal steady state and not an error;
* the batch listing carries `has_result`, never results, because a handful of
  run_study results is megabytes and a poll loop would refetch all of it;
* a FAILED task and a completed task whose result carries stage errors are
  different things, and the CLI has to show that difference.
"""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

from app.app_data_service import DEFAULT_IDENTITY_HEADER, BaseUrl, E2EDataService


def _service(handler: Any, **kwargs: Any) -> E2EDataService:
    """A service whose transport is a callable, so no socket is ever opened."""
    svc = E2EDataService(base_url=BaseUrl.LOCAL_8080, timeout=5, **kwargs)
    svc.client = httpx.Client(
        base_url=BaseUrl.LOCAL_8080,
        transport=httpx.MockTransport(handler),
        headers=svc.client.headers,
    )
    return svc


def _recorder(payload: Any, status: int = 200) -> tuple[Any, list[httpx.Request]]:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(status, json=payload)

    return handler, seen


# --- identity: opt-in, and not a login ---------------------------------------


def test_no_identity_means_no_header_at_all() -> None:
    """The normal case. Most deployments have nothing in front of them that sets
    one, and inventing a value would be worse than admitting there is none."""
    handler, seen = _recorder({"task_id": 1})
    _service(handler).worker_submit("job-1", "run_study")
    assert DEFAULT_IDENTITY_HEADER not in seen[0].headers


def test_an_identity_is_sent_under_the_default_header() -> None:
    handler, seen = _recorder({"task_id": 1})
    _service(handler, identity="a@b.example").worker_submit("job-1", "run_study")
    assert seen[0].headers[DEFAULT_IDENTITY_HEADER] == "a@b.example"


def test_the_header_name_is_configurable() -> None:
    """It is a property of whatever proxy sits in front -- oauth2-proxy sets
    X-Auth-Request-Email, an ALB OIDC action sets X-Amzn-Oidc-Identity."""
    handler, seen = _recorder({"task_id": 1})
    svc = _service(handler, identity="a@b.example", identity_header="X-Amzn-Oidc-Identity")
    svc.worker_submit("job-1", "run_study")
    assert seen[0].headers["X-Amzn-Oidc-Identity"] == "a@b.example"
    assert DEFAULT_IDENTITY_HEADER not in seen[0].headers


def test_identity_comes_from_the_environment_when_not_passed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ATLANTIS_IDENTITY", "env@b.example")
    handler, seen = _recorder({"task_id": 1})
    _service(handler).worker_submit("job-1", "run_study")
    assert seen[0].headers[DEFAULT_IDENTITY_HEADER] == "env@b.example"


def test_an_explicit_identity_beats_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ATLANTIS_IDENTITY", "env@b.example")
    handler, seen = _recorder({"task_id": 1})
    _service(handler, identity="flag@b.example").worker_submit("job-1", "run_study")
    assert seen[0].headers[DEFAULT_IDENTITY_HEADER] == "flag@b.example"


def test_whitespace_only_identity_is_not_an_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    """An unset shell variable expands to empty; sending `X-...: ` would make the
    caller look identified-as-nobody rather than anonymous."""
    monkeypatch.setenv("ATLANTIS_IDENTITY", "   ")
    handler, seen = _recorder({"task_id": 1})
    _service(handler).worker_submit("job-1", "run_study")
    assert DEFAULT_IDENTITY_HEADER not in seen[0].headers


# --- the calls themselves ----------------------------------------------------


def test_submit_posts_the_job_method_and_params() -> None:
    handler, seen = _recorder({"task_id": 7, "status": "queued"})
    out = _service(handler).worker_submit("job-1", "run_study", {"study_slug": "s1"})
    assert seen[0].method == "POST"
    assert seen[0].url.path == "/env-worker/v1/tasks"
    assert json.loads(seen[0].content) == {
        "job_name": "job-1",
        "method": "run_study",
        "params": {"study_slug": "s1"},
    }
    assert out["task_id"] == 7


def test_batch_status_asks_for_every_id_and_carries_no_results() -> None:
    """`ids` repeats rather than joining with commas, matching the endpoint. The
    response has `has_result`, not `result` -- refetching megabytes of run_study
    output on every poll tick is the thing this shape exists to avoid."""
    handler, seen = _recorder([{"task_id": 1, "has_result": True}, {"task_id": 2, "has_result": False}])
    rows = _service(handler).worker_tasks([1, 2])
    assert seen[0].url.params.get_list("ids") == ["1", "2"]
    assert all("result" not in row for row in rows)


def test_cancel_is_a_delete_on_the_task() -> None:
    handler, seen = _recorder({"task_id": 3, "status": "cancelled"})
    _service(handler).worker_cancel(3)
    assert seen[0].method == "DELETE"
    assert seen[0].url.path == "/env-worker/v1/tasks/3"


def test_a_named_read_goes_to_its_own_url_not_to_call() -> None:
    """The named endpoints turn the worker's in-band failures into statuses; the
    generic /call returns them as 200 with a sentinel body."""
    handler, seen = _recorder({"generators": []})
    _service(handler).worker_read("job-1", "generators")
    assert seen[0].method == "GET"
    assert seen[0].url.path == "/env-worker/v1/relay/workers/job-1/generators"


def test_a_failing_status_raises_rather_than_returning_a_body() -> None:
    """403 on cancel is the one authorization answer in the API. Returning its
    body as though it were a task would hide the refusal."""
    handler, _ = _recorder({"detail": "started by someone else"}, status=403)
    with pytest.raises(httpx.HTTPStatusError):
        _service(handler).worker_cancel(3)


# --- the CLI's own framing ---------------------------------------------------


def test_job_class_list_matches_the_workbench_and_excludes_run_process() -> None:
    """`run_process` reads like a job-class method and is not one: it builds one
    class and runs a single update(), which is the Composite Explorer's "try this
    process" button. That misreading has already been made once in this codebase.
    """
    from app.cli import JOB_CLASS_METHODS

    assert set(JOB_CLASS_METHODS) == {"run_study", "run_study_analyses", "run_investigation_analysis"}
    assert "run_process" not in JOB_CLASS_METHODS


def test_the_worker_commands_are_registered() -> None:
    from app.cli import worker_cli

    names = {c.name for c in worker_cli.registered_commands}
    assert {"start", "call", "stop", "submit", "task", "tasks", "cancel"} <= names


def test_submit_warns_when_the_server_ignored_the_identity() -> None:
    """Found on dev: `--as jim@…` printed "as jim@…" and the task came back
    `Owner: anonymous`, because VIVA_API_IDENTITY_HEADER is unset there.

    The seam behaving that way is correct -- absent configuration means anonymous.
    The CLI claiming otherwise is not: the user only discovers it at cancel time,
    when their own task refuses them. Say it at submit."""
    import inspect

    from app.cli import worker_submit

    source = inspect.getsource(worker_submit)
    assert 'if identity and not task.get("created_by")' in source
    assert "VIVA_API_IDENTITY_HEADER" in source
