"""All three clients expose the task tier, and say the same things about it.

CLAUDE.md's EUTE rule is that the CLI, TUI and marimo GUI are implementations of
ONE workflow in three media -- not three products that drifted. Parity is easy to
claim and easy to lose, and it is lost silently: a capability added to the CLI
and forgotten in the TUI looks like nothing at all.

Two things are pinned, and only two, because over-pinning would make every
cosmetic edit a test failure:

* each client can REACH each operation (they share E2EDataService, so this is
  about wiring, not about HTTP);
* each client draws THE distinction the tier exists for -- a task that FAILED
  (the job died) versus one that COMPLETED carrying stage errors (the science
  failed). Collapsing those into "error" is the specific regression that would
  make the whole arc pointless, and it is exactly the kind of thing a hurried
  edit does.
"""

from __future__ import annotations

import inspect
from pathlib import Path

import pytest

_APP = Path(__file__).resolve().parents[3] / "app"
_TUI = (_APP / "tui.py").read_text(encoding="utf-8")
_GUI = (_APP / "gui.py").read_text(encoding="utf-8")

#: The service methods that make up the tier. Named here rather than derived, so
#: adding one to E2EDataService without surfacing it anywhere fails loudly.
TASK_TIER = ("worker_start", "worker_submit", "worker_task", "worker_cancel", "worker_stop")


def test_the_service_actually_has_these_methods() -> None:
    """Guards the list above from rotting into a set of names nothing implements."""
    from app.app_data_service import E2EDataService

    missing = [m for m in (*TASK_TIER, "worker_tasks", "worker_read") if not hasattr(E2EDataService, m)]
    assert not missing, missing


@pytest.mark.parametrize("method", TASK_TIER)
def test_the_cli_reaches_every_task_tier_call(method: str) -> None:
    import app.cli as cli

    source = inspect.getsource(cli)
    assert f".{method}(" in source, f"the CLI never calls {method}"


@pytest.mark.parametrize("method", TASK_TIER)
def test_the_tui_reaches_every_task_tier_call(method: str) -> None:
    assert f".{method}(" in _TUI, f"the TUI never calls {method}"


@pytest.mark.parametrize("method", TASK_TIER)
def test_the_gui_reaches_every_task_tier_call(method: str) -> None:
    assert f".{method}(" in _GUI, f"the marimo GUI never calls {method}"


def test_the_tui_has_a_workers_domain_wired_to_its_buttons() -> None:
    """A button with no dispatch branch renders and does nothing, which is worse
    than an absent button because it looks implemented."""
    import re

    ids = set(re.findall(r'id="(wrk-[a-z]+)"', _TUI))
    assert ids, "no worker buttons in the TUI"
    unhandled = sorted(i for i in ids if f'bid == "{i}"' not in _TUI)
    assert not unhandled, f"TUI worker buttons with no dispatch branch: {unhandled}"


def test_the_gui_panel_is_reachable_from_a_rendered_cell() -> None:
    """marimo only runs a cell whose outputs something references. A panel built
    and never displayed is invisible, and nothing errors."""
    assert "wrk_panel" in _GUI
    assert _GUI.count("wrk_panel") >= 2, "wrk_panel is built but never rendered"


# --- the distinction, in all three ------------------------------------------


@pytest.mark.parametrize(
    ("client", "source"),
    [("tui", _TUI), ("gui", _GUI)],
)
def test_a_dead_job_and_failed_stages_read_differently(client: str, source: str) -> None:
    """The one substantive thing parity has to preserve.

    `task.error_message` means the JOB died -- the worker was OOM-killed, the
    socket dropped. `result["errors"]` means the job ran fine and the SCIENCE
    failed. Different causes, different fixes, and a client that prints one word
    for both hands the user back the ambiguity the task tier removed.
    """
    assert "error_message" in source, f"{client} never surfaces the job-failed reason"
    assert '"errors"' in source or "'errors'" in source, f"{client} never surfaces per-stage errors"
    assert "The job failed" in source, f"{client} does not distinguish a failed JOB"
    assert "stage(s) failed" in source, f"{client} does not distinguish failed STAGES"


def test_the_cli_draws_the_same_distinction() -> None:
    import app.cli as cli

    source = inspect.getsource(cli)
    assert "The job failed" in source
    assert "stage(s) failed" in source


# --- identity, described the same way everywhere -----------------------------


@pytest.mark.parametrize(("client", "source"), [("tui", _TUI), ("gui", _GUI)])
def test_identity_is_not_described_as_authentication(client: str, source: str) -> None:
    """A UI that labels this "login" or "sign in" would be lying: the header is
    as trustworthy as the proxy that sets it, and where nothing sets one anybody
    may claim anything. Each client has to say so where the user types it."""
    assert "not authentication" in source, f"{client} does not say what identity is NOT"
    for forbidden in ("Log in", "Sign in", "Password", "Authenticate"):
        assert forbidden not in source, f"{client} presents identity as {forbidden!r}"


@pytest.mark.parametrize(("client", "source"), [("tui", _TUI), ("gui", _GUI)])
def test_each_client_warns_when_the_server_ignored_the_identity(client: str, source: str) -> None:
    """Found on dev: the header is only read where a deployment names one, so
    `created_by` comes back NULL and the task is cancellable by anyone. Every
    client that offers an identity field owes the same warning at submit time,
    not at cancel time."""
    assert "IDENTITY_HEADER" in source, f"{client} never mentions the setting that would fix it"
    assert "anonymous" in source


@pytest.mark.parametrize(("client", "source"), [("tui", _TUI), ("gui", _GUI)])
def test_401_and_403_on_cancel_are_told_apart(client: str, source: str) -> None:
    """They mean different things and the fix differs: 401 is "say who you are",
    403 is "this is not yours". One shared error path would send the user to the
    wrong remedy."""
    assert "401" in source and "403" in source, f"{client} collapses the two cancel refusals"
