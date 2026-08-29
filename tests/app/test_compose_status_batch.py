"""`atlantis compose status` with several IDs — the one /compose endpoint no
client here exposed (`GET /compose/v1/simulations/status/batch`).

The single-ID path must stay byte-for-byte what it was: this is an addition, not
a replacement, and every existing invocation and script keeps its shape.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from app.cli import cli as cli_app

runner = CliRunner()


def _svc(**results: object) -> MagicMock:
    svc = MagicMock()
    for method, value in results.items():
        getattr(svc, method).return_value = value
    return svc


def test_one_id_uses_the_single_endpoint_and_the_old_panel() -> None:
    """Unchanged behaviour is the contract here, not an implementation detail."""
    svc = _svc(compose_get_simulation_status={"database_id": 2, "status": "completed"})
    with patch("app.cli.get_data_service", return_value=svc):
        res = runner.invoke(cli_app, ["compose", "status", "2"])
    assert res.exit_code == 0, res.output
    assert "COMPLETED" in res.output
    assert "Compose simulation 2" in res.output
    svc.compose_get_simulation_status.assert_called_once()
    svc.compose_get_simulations_status_batch.assert_not_called()


def test_several_ids_make_exactly_ONE_request() -> None:
    """The whole point: twenty runs must not be twenty round trips, which
    through the SSM tunnel is felt rather than theoretical."""
    svc = _svc(
        compose_get_simulations_status_batch=[
            {"database_id": 1, "status": "completed"},
            {"database_id": 2, "status": "failed", "error_message": "boom"},
        ]
    )
    with patch("app.cli.get_data_service", return_value=svc):
        res = runner.invoke(cli_app, ["compose", "status", "1", "2"])
    assert res.exit_code == 0, res.output
    svc.compose_get_simulations_status_batch.assert_called_once_with(simulation_ids=[1, 2])
    svc.compose_get_simulation_status.assert_not_called()
    assert "completed" in res.output
    assert "boom" in res.output


def test_an_id_the_server_omits_is_reported_as_not_found() -> None:
    """A missing row is an id that does not exist here — not 'unknown status'.
    A blank line would leave the user to investigate a non-question."""
    svc = _svc(compose_get_simulations_status_batch=[{"database_id": 1, "status": "completed"}])
    with patch("app.cli.get_data_service", return_value=svc):
        res = runner.invoke(cli_app, ["compose", "status", "1", "99"])
    assert res.exit_code == 0, res.output
    assert "not found" in res.output
    assert "99" in res.output


def test_rows_are_matched_by_any_of_the_id_fields_the_api_uses() -> None:
    """The batch payload carries database_id/sim_id (and ref_id on some shapes);
    matching on only one of them silently renders every row 'not found'."""
    svc = _svc(compose_get_simulations_status_batch=[{"sim_id": 7, "status": "running"}])
    with patch("app.cli.get_data_service", return_value=svc):
        res = runner.invoke(cli_app, ["compose", "status", "7"] + ["8"])
    assert "running" in res.output


def test_the_summary_counts_by_status() -> None:
    svc = _svc(
        compose_get_simulations_status_batch=[
            {"database_id": 1, "status": "completed"},
            {"database_id": 2, "status": "completed"},
            {"database_id": 3, "status": "failed"},
        ]
    )
    with patch("app.cli.get_data_service", return_value=svc):
        res = runner.invoke(cli_app, ["compose", "status", "1", "2", "3"])
    flat = " ".join(res.output.split())
    assert "2 completed" in flat
    assert "1 failed" in flat
