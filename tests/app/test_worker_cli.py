"""CLI tests for `atlantis worker *` — the env-worker relay (plan §C).

The relay's only laptop client used to be hand-rolled `curl`. These pin the
CLI that replaced it, and especially the ERROR translation: each relay status
code means a specific, different thing, and 503 in particular is a *deployment*
answer ("the relay is not switched on here"), not a fault worth retrying.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx
from typer.testing import CliRunner

from app.cli import cli as cli_app

runner = CliRunner()


def _svc(**results: object) -> MagicMock:
    svc = MagicMock()
    for method, value in results.items():
        getattr(svc, method).return_value = value
    return svc


def _http_error(status: int, detail: str) -> httpx.HTTPStatusError:
    request = httpx.Request("POST", "http://x/env-worker/v1/relay/workers/j/call")
    response = httpx.Response(status, json={"detail": detail}, request=request)
    return httpx.HTTPStatusError(detail, request=request, response=response)


# --- start ------------------------------------------------------------------


def test_start_reports_connected_and_names_the_next_command() -> None:
    """A job name the user cannot copy is a dead end; the whole point of the
    handle is the call that follows it."""
    svc = _svc(worker_start={"job_name": "job-1", "image": "ecr/v2ecoli:abc", "namespace": "ns"})
    with patch("app.cli.get_data_service", return_value=svc):
        res = runner.invoke(cli_app, ["worker", "start", "abc1234"])
    assert res.exit_code == 0, res.output
    assert "CONNECTED" in res.output
    assert "job-1" in res.output
    assert "worker call job-1" in res.output.replace("\n", " ")


def test_start_forwards_the_optional_flags_only_when_given() -> None:
    """Empty strings are how Typer says 'not supplied'; forwarding them would
    override deployment defaults with blanks."""
    svc = _svc(worker_start={"job_name": "j", "image": "i", "namespace": "n"})
    with patch("app.cli.get_data_service", return_value=svc):
        runner.invoke(cli_app, ["worker", "start", "abc1234"])
    kwargs = svc.worker_start.call_args.kwargs
    assert kwargs["commit"] == "abc1234"
    assert kwargs["workspace"] is None
    assert kwargs["session_key"] is None


def test_start_passes_session_key_and_accept_timeout_through() -> None:
    svc = _svc(worker_start={"job_name": "j", "image": "i", "namespace": "n"})
    with patch("app.cli.get_data_service", return_value=svc):
        runner.invoke(
            cli_app,
            ["worker", "start", "abc", "--session-key", "s1", "--accept-timeout", "42"],
        )
    kwargs = svc.worker_start.call_args.kwargs
    assert kwargs["session_key"] == "s1"
    assert kwargs["accept_timeout"] == 42.0


# --- call -------------------------------------------------------------------


def test_call_prints_the_unwrapped_result() -> None:
    svc = _svc(worker_call={"result": {"generators": ["a", "b"]}})
    with patch("app.cli.get_data_service", return_value=svc):
        res = runner.invoke(cli_app, ["worker", "call", "job-1", "list_generators"])
    assert res.exit_code == 0, res.output
    assert "generators" in res.output
    assert "result" not in res.output.split("generators")[0]  # unwrapped, not the envelope


def test_call_with_a_null_result_says_so_rather_than_crashing() -> None:
    """A method that only acts legitimately returns null."""
    svc = _svc(worker_call={"result": None})
    with patch("app.cli.get_data_service", return_value=svc):
        res = runner.invoke(cli_app, ["worker", "call", "job-1", "shutdown"])
    assert res.exit_code == 0, res.output
    assert "no result" in res.output


def test_call_parses_params_json() -> None:
    svc = _svc(worker_call={"result": 1})
    with patch("app.cli.get_data_service", return_value=svc):
        runner.invoke(
            cli_app, ["worker", "call", "j", "build", "--params", '{"ref": "pkg.c"}']
        )
    assert svc.worker_call.call_args.kwargs["params"] == {"ref": "pkg.c"}


def test_bad_params_json_fails_before_any_request() -> None:
    """--params is the flag most likely typed by hand; the error must name what
    was wrong with THEIR json, and must not spend a round trip first."""
    svc = _svc(worker_call={"result": 1})
    with patch("app.cli.get_data_service", return_value=svc):
        res = runner.invoke(cli_app, ["worker", "call", "j", "m", "--params", "{not json"])
    assert res.exit_code == 1
    assert "not valid JSON" in res.output
    svc.worker_call.assert_not_called()


def test_params_must_be_an_object_not_a_bare_value() -> None:
    svc = _svc(worker_call={"result": 1})
    with patch("app.cli.get_data_service", return_value=svc):
        res = runner.invoke(cli_app, ["worker", "call", "j", "m", "--params", "[1,2]"])
    assert res.exit_code == 1
    assert "JSON object" in res.output
    svc.worker_call.assert_not_called()


# --- error translation ------------------------------------------------------


def test_503_is_reported_as_a_deployment_answer_not_a_retryable_fault() -> None:
    """The single most confusing outcome: everything is healthy, the feature is
    simply not switched on here."""
    svc = MagicMock()
    svc.worker_call.side_effect = _http_error(503, "relay is not enabled on this deployment")
    with patch("app.cli.get_data_service", return_value=svc):
        res = runner.invoke(cli_app, ["worker", "call", "j", "m"])
    assert res.exit_code == 1
    assert "503" in res.output
    assert "ENV_WORKER_RELAY_ADVERTISE_HOST" in res.output.replace("\n", "")


def test_404_explains_that_a_viva_api_restart_drops_sockets() -> None:
    svc = MagicMock()
    svc.worker_call.side_effect = _http_error(404, "no relayed worker registered as 'j'")
    with patch("app.cli.get_data_service", return_value=svc):
        res = runner.invoke(cli_app, ["worker", "call", "j", "m"])
    assert res.exit_code == 1
    assert "restarted" in res.output.replace("\n", " ")


def test_422_says_the_worker_ran_and_refused() -> None:
    svc = MagicMock()
    svc.worker_call.side_effect = _http_error(422, "unknown method: 'nope'")
    with patch("app.cli.get_data_service", return_value=svc):
        res = runner.invoke(cli_app, ["worker", "call", "j", "nope"])
    assert res.exit_code == 1
    assert "unknown method" in res.output
    assert "refused" in res.output.replace("\n", " ")


def test_no_response_at_all_points_at_the_tunnel() -> None:
    """On this path a connection failure is almost always the SSM tunnel, and
    saying so beats a bare stack trace."""
    svc = MagicMock()
    svc.worker_call.side_effect = httpx.ConnectError("connection refused")
    with patch("app.cli.get_data_service", return_value=svc):
        res = runner.invoke(cli_app, ["worker", "call", "j", "m"])
    assert res.exit_code == 1
    assert "tunnel" in res.output


# --- stop -------------------------------------------------------------------


def test_stop_distinguishes_a_held_connection_from_none() -> None:
    """Running stop twice is normal; the second must not look like a failure,
    but should not claim it closed something either."""
    svc = _svc(worker_stop={"job_name": "j", "status": "deleted", "was_connected": True})
    with patch("app.cli.get_data_service", return_value=svc):
        first = runner.invoke(cli_app, ["worker", "stop", "j"])
    assert first.exit_code == 0
    assert "no live connection" not in first.output

    svc = _svc(worker_stop={"job_name": "j", "status": "deleted", "was_connected": False})
    with patch("app.cli.get_data_service", return_value=svc):
        second = runner.invoke(cli_app, ["worker", "stop", "j"])
    assert second.exit_code == 0, second.output
    assert "no live connection" in second.output
