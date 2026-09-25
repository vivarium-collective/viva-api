"""All three clients expose the dataset registry, and draw the same producer line.

The same approach as ``test_client_parity.py`` (the task tier), for data provenance slice 1:
pin REACH -- each client calls each service method -- and the one distinction this surface
exists for: a file its run WROTE, versus a bundle the S3 walk only FOUND UNDER a simulation's
output that no analysis run claims. The words live in ``app.dataset_views``; a client that
re-derives them is exactly how the three would drift apart.
"""

from __future__ import annotations

import inspect
import re
from pathlib import Path

import pytest

_APP = Path(__file__).resolve().parents[3] / "app"
_TUI = (_APP / "tui.py").read_text(encoding="utf-8")
_GUI = (_APP / "ui" / "dashboard.py").read_text(encoding="utf-8")

#: Named here rather than derived, so a method added to E2EDataService and surfaced nowhere
#: fails loudly.
DATASET_TIER = (
    "list_datasets",
    "list_simulation_datasets",
    "list_analysis_datasets",
    "get_dataset",
    "get_dataset_provenance",
    "fetch_dataset",
    "list_dataset_tags",
    "list_dataset_attributes",
    "tag_dataset",
    "list_analyses",
)


def _source(client: str) -> str:
    if client == "tui":
        return _TUI
    if client == "gui":
        return _GUI
    import app.cli as cli

    return inspect.getsource(cli)


def test_the_service_actually_has_these_methods() -> None:
    from app.app_data_service import E2EDataService

    missing = [m for m in DATASET_TIER if not hasattr(E2EDataService, m)]
    assert not missing, missing


@pytest.mark.parametrize("client", ["cli", "tui", "gui"])
@pytest.mark.parametrize("method", DATASET_TIER)
def test_every_client_reaches_every_dataset_call(client: str, method: str) -> None:
    source = _source(client)
    assert f".{method}(" in source, f"the {client} never calls {method}"


def test_every_tui_dataset_and_analysis_button_has_a_dispatch_branch() -> None:
    """A button with no dispatch branch renders and does nothing, which looks implemented."""
    ids = set(re.findall(r'id="((?:ds|ana|sim)-[a-z-]+)"', _TUI))
    assert {"ds-list", "ds-provenance", "ds-fetch", "ds-tag", "ana-list", "ana-datasets", "sim-datasets"} <= ids
    unhandled = sorted(i for i in ids if f'bid == "{i}"' not in _TUI)
    assert not unhandled, f"TUI buttons with no dispatch branch: {unhandled}"


def test_the_tui_no_longer_claims_there_is_no_analysis_listing() -> None:
    assert "No listing endpoint available" not in _TUI


def test_the_gui_dataset_panel_is_reachable_from_a_rendered_cell() -> None:
    """marimo only shows a cell's last expression; a panel built and never displayed is invisible."""
    assert _GUI.count("ds_panel") >= 2, "ds_panel is built but never rendered"


@pytest.mark.parametrize("client", ["cli", "tui", "gui"])
def test_the_producer_relation_comes_from_the_shared_words(client: str) -> None:
    source = _source(client)
    assert "producer_relation(" in source, f"the {client} does not say written by / found under"
    assert "is_unclaimed(" in source and "UNCLAIMED_HINT" in source, f"the {client} does not explain found under"
    for literal in ('"found under"', '"written by"', "'found under'", "'written by'"):
        assert literal not in source, f"the {client} re-derives {literal} instead of using app.dataset_views"
