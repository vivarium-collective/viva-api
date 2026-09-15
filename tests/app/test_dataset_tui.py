"""The TUI's Datasets domain and analysis listing, driven headless with a mocked data service.

Parity with ``atlantis dataset`` / ``analysis list|datasets`` / ``simulation datasets``: these pin
that each button reaches its service call with the filters typed, that a selected row opens the
next hop, and that provenance says "found under" for a walk-found bundle no analysis run claims.
Nothing here touches HTTP: ``get_data_service`` is patched for the whole test.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from textual.widgets import DataTable, Input

from app.tui import AtlantisTUI, FileBrowserScreen
from viva_api.analysis.models import (
    AnalysisConfig,
    AnalysisConfigOptions,
    DatasetDTO,
    DatasetListDTO,
    DatasetProducerDTO,
    DatasetProvenanceDTO,
    ExperimentAnalysisDTO,
)
from viva_api.common.models import JobStatus

URI = "s3://bucket/vecoli-output/exp/analyses/analysis-ptools-multiseed/ptools/ptools_rna_multiseed__variant=0.tsv"


def _dataset(**overrides: Any) -> DatasetDTO:
    values: dict[str, Any] = {
        "database_id": 41,
        "kind": "ptools-analysis",
        "uri": URI,
        "simulation_id": 1002,
        "view": "ptools_rna",
        "size_bytes": 2048,
        "attributes": {"origin": "walk", "protocol": "multiseed", "variant": 0},
        "tags": ["cd2"],
    }
    values.update(overrides)
    return DatasetDTO(**values)


def _page(*datasets: DatasetDTO) -> DatasetListDTO:
    return DatasetListDTO(datasets=list(datasets), limit=100, offset=0)


def _analysis(**overrides: Any) -> ExperimentAnalysisDTO:
    values: dict[str, Any] = {
        "database_id": 7,
        "name": "analysis-ptools-multiseed",
        "config": AnalysisConfig(analysis_options=AnalysisConfigOptions(experiment_id=["exp"])),
        "last_updated": "2026-09-15 01:02:03.456",
        "status": JobStatus.COMPLETED,
        "backend": "ray",
        "simulation_id": 1002,
        "experiment_id": "exp",
    }
    values.update(overrides)
    return ExperimentAnalysisDTO(**values)


def _service() -> MagicMock:
    svc = MagicMock()
    svc.list_datasets.return_value = _page(_dataset(), _dataset(database_id=42, available=False))
    svc.list_analysis_datasets.return_value = _page(_dataset(simulation_id=None, analysis_id=7))
    svc.list_simulation_datasets.return_value = _page(_dataset())
    svc.list_analyses.return_value = [_analysis(), _analysis(database_id=8, status=JobStatus.RUNNING)]
    svc.get_dataset_provenance.return_value = DatasetProvenanceDTO(
        dataset=_dataset(),
        producer=DatasetProducerDTO(kind="simulation", id=1002, name="exp", status="completed"),
    )
    svc.tag_dataset.return_value = _dataset(tags=["cd2", "demo"])
    return svc


class _Harness:
    """An app whose log lines are captured as plain strings (RichLog renders lazily)."""

    def __init__(self, svc: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
        # Patched for the whole test, not only construction: the server Select fires on mount
        # and rebuilds the service, and a test must never reach a real server.
        monkeypatch.setattr("app.tui.get_data_service", lambda *args, **kwargs: svc)
        self.app = AtlantisTUI()
        self.logs: list[str] = []
        monkeypatch.setattr(self.app, "write_log", lambda msg: self.logs.append(str(msg)))

    def log_text(self) -> str:
        return "\n".join(self.logs)

    def table(self) -> DataTable[str]:
        table: DataTable[str] = self.app.query_one("#data-table", DataTable)
        return table


async def _settle(harness: _Harness, pilot: Any) -> None:
    """Let a pressed button's message start its worker, then wait the worker out."""
    await pilot.pause()
    await harness.app.workers.wait_for_complete()
    await pilot.pause()


async def _answer(harness: _Harness, pilot: Any, value: str, *, settle: bool = True) -> None:
    """Type into the modal prompt that is on top and submit it."""
    await pilot.pause()
    harness.app.screen.query_one(Input).value = value
    await pilot.press("enter")
    if settle:
        await _settle(harness, pilot)


async def _select_first_row(harness: _Harness, pilot: Any) -> None:
    table = harness.table()
    table.focus()
    table.move_cursor(row=0)
    await pilot.press("enter")
    await _settle(harness, pilot)


@pytest.mark.asyncio
async def test_datasets_nav_lists_everything_and_a_row_opens_found_under_provenance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    svc = _service()
    harness = _Harness(svc, monkeypatch)
    async with harness.app.run_test(size=(220, 60)) as pilot:
        await pilot.click("#nav-datasets")
        await _settle(harness, pilot)
        svc.list_datasets.assert_called_once_with()
        table = harness.table()
        assert table.row_count == 2
        assert [str(cell) for cell in table.get_row_at(1)][6] == "walk gone"
        assert "1 gone" in harness.log_text()

        await _select_first_row(harness, pilot)
        svc.get_dataset_provenance.assert_called_once_with(41)
    text = harness.log_text()
    assert "found under" in text and "written by" not in text
    assert "no analysis run claims this bundle" in text
    assert "no traced run row" in text


@pytest.mark.asyncio
async def test_the_dataset_filter_prompt_reaches_the_service_and_a_bad_one_does_not(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    svc = _service()
    harness = _Harness(svc, monkeypatch)
    async with harness.app.run_test(size=(220, 60)) as pilot:
        await pilot.click("#nav-datasets")
        await _settle(harness, pilot)

        await pilot.click("#ds-list")
        await _answer(harness, pilot, "kind=figure tag=cd2 simulation=1002 variant=0")
        assert svc.list_datasets.call_args.kwargs == {
            "kind": "figure",
            "tag": "cd2",
            "simulation_id": 1002,
            "attrs": ["variant=0"],
        }

        calls = svc.list_datasets.call_count
        await pilot.click("#ds-list")
        await _answer(harness, pilot, "kind=table")
        assert svc.list_datasets.call_count == calls
    assert "kind must be one of" in harness.log_text()


@pytest.mark.asyncio
async def test_analyses_nav_lists_analyses_and_a_row_lists_what_it_wrote(monkeypatch: pytest.MonkeyPatch) -> None:
    svc = _service()
    harness = _Harness(svc, monkeypatch)
    async with harness.app.run_test(size=(220, 60)) as pilot:
        await pilot.click("#nav-analyses")
        await _settle(harness, pilot)
        svc.list_analyses.assert_called_once_with(limit=50)
        assert harness.table().row_count == 2

        await _select_first_row(harness, pilot)
        svc.list_analysis_datasets.assert_called_once_with(7)
        assert [str(cell) for cell in harness.table().get_row_at(0)][4] == "analysis 7"

        await pilot.click("#ana-list")
        await _answer(harness, pilot, "experiment=exp status=completed")
        assert svc.list_analyses.call_args.kwargs == {"experiment_id": "exp", "status": "completed", "limit": 50}


@pytest.mark.asyncio
async def test_simulation_datasets_and_tagging(monkeypatch: pytest.MonkeyPatch) -> None:
    svc = _service()
    harness = _Harness(svc, monkeypatch)
    async with harness.app.run_test(size=(220, 60)) as pilot:
        await pilot.click("#nav-simulations")
        await _settle(harness, pilot)
        await pilot.click("#sim-datasets")
        await _answer(harness, pilot, "1002")
        svc.list_simulation_datasets.assert_called_once_with(1002)

        await pilot.click("#nav-datasets")
        await _settle(harness, pilot)
        await pilot.click("#ds-tag")
        await _answer(harness, pilot, "41")
        await _answer(harness, pilot, "cd2, demo")
        svc.tag_dataset.assert_called_once_with(41, ["cd2", "demo"])
    assert "Tagged dataset 41" in harness.log_text()


@pytest.mark.asyncio
async def test_fetch_streams_into_a_temp_dir_and_opens_the_file_browser(monkeypatch: pytest.MonkeyPatch) -> None:
    svc = _service()
    fetched: list[Path] = []

    def _fetch(dataset_id: int, dest: str) -> Path:
        path = Path(dest) / "ptools_rna_multiseed__variant=0.tsv"
        path.write_text("gene\tt1\n")
        fetched.append(path)
        return path

    svc.fetch_dataset.side_effect = _fetch
    harness = _Harness(svc, monkeypatch)
    async with harness.app.run_test(size=(220, 60)) as pilot:
        await pilot.click("#nav-datasets")
        await _settle(harness, pilot)
        await pilot.click("#ds-fetch")
        # Not _settle: the file browser's DirectoryTree keeps a loader worker alive, so
        # "every worker complete" never arrives once it is open. Wait for the screen instead.
        await _answer(harness, pilot, "41", settle=False)
        for _ in range(100):
            if isinstance(harness.app.screen, FileBrowserScreen):
                break
            await pilot.pause(0.05)
        assert isinstance(harness.app.screen, FileBrowserScreen)
        assert harness.app.screen.root_path == fetched[0].parent
        await pilot.press("escape")
    svc.fetch_dataset.assert_called_once()
    assert "Saved dataset 41" in harness.log_text()
    assert not fetched[0].exists(), "the temp copy is removed when the TUI exits"
