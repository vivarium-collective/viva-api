"""CLI tests for `atlantis composite run --simulation-config`.

The composite dispatch always fell back to the deployment default config
(``api_simulation_default.json``). That file does not exist in every simulator
repo: **sms-ecoli ships named configs only**, so every composite dispatch
against an sms-ecoli simulator 404'd —

    Config file 'api_simulation_default.json' not found in
    https://github.com/CovertLabEcoli/sms-ecoli at commit 538e8c6

`run_workflow` already accepted ``config_filename``; only the CLI never passed
it, which made the pbg-native path unreachable from `atlantis` for the one repo
CD2 actually dispatches. These pin the wiring in both directions, because the
bug was not a wrong value — it was a value that never left the CLI.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from app.cli import cli as cli_app

runner = CliRunner()


def _svc() -> MagicMock:
    svc = MagicMock()
    sim = MagicMock()
    sim.database_id = 326
    sim.model_dump.return_value = {"database_id": 326}
    svc.run_workflow.return_value = sim
    return svc


def _run(*extra: str) -> MagicMock:
    svc = _svc()
    with patch("app.cli.get_data_service", return_value=svc):
        result = runner.invoke(
            cli_app,
            ["composite", "run", "exp-1", "128", "--no-poll", *extra],
        )
    assert result.exit_code == 0, result.output
    return svc


def test_simulation_config_reaches_run_workflow() -> None:
    """The whole defect: the flag exists on the API and the service, and the
    dispatch is useless against sms-ecoli unless the CLI forwards it."""
    svc = _run("--simulation-config", "mecillinam_wellmixed.json")
    assert svc.run_workflow.call_args.kwargs["config_filename"] == "mecillinam_wellmixed.json"


def test_omitting_it_forwards_none_not_a_guessed_default() -> None:
    """Absent must stay absent. Inventing a default here would re-hide the 404
    behind a filename the CLI chose rather than the deployment."""
    svc = _run()
    assert svc.run_workflow.call_args.kwargs["config_filename"] is None
