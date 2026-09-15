"""CLI tests for ``atlantis dataset``, ``atlantis analysis list|datasets`` and ``atlantis simulation datasets``.

The data service is a mock returning real DTOs, so these pin the wiring (which filters
reach the service) and the rendering, without the API.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from app.cli import cli as cli_app
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
from viva_api.simulation.models import SimulationSpan

runner = CliRunner(env={"COLUMNS": "220"})

URI = "s3://bucket/vecoli-output/exp/analyses/analysis-ptools-multiseed/ptools/ptools_rna_multiseed__variant=0.tsv"


def _dataset(**overrides: Any) -> DatasetDTO:
    values: dict[str, Any] = {
        "database_id": 41,
        "kind": "ptools-analysis",
        "uri": URI,
        "analysis_id": 7,
        "view": "ptools_rna",
        "size_bytes": 2048,
        "attributes": {"origin": "walk", "protocol": "multiseed", "variant": 0, "n_tp": 8},
        "tags": ["cd2"],
        "updated_at": "2026-09-15T01:02:03.456",
    }
    values.update(overrides)
    return DatasetDTO(**values)


def _page(*datasets: DatasetDTO, next_offset: int | None = None) -> DatasetListDTO:
    return DatasetListDTO(datasets=list(datasets), limit=100, offset=0, next_offset=next_offset)


def _analysis(**overrides: Any) -> ExperimentAnalysisDTO:
    values: dict[str, Any] = {
        "database_id": 7,
        "name": "analysis-ptools-multiseed",
        "config": AnalysisConfig(analysis_options=AnalysisConfigOptions(experiment_id=["exp"])),
        "last_updated": "2026-09-15 01:02:03.456",
        "status": JobStatus.COMPLETED,
        "backend": "walk",
        "simulation_id": 1002,
        "experiment_id": "exp",
        "tags": ["cd2"],
    }
    values.update(overrides)
    return ExperimentAnalysisDTO(**values)


def _invoke(svc: MagicMock, *args: str) -> str:
    """The command's output with Rich's styling removed (it highlights numbers mid-string)."""
    with patch("app.cli.get_data_service", return_value=svc):
        result = runner.invoke(cli_app, list(args))
    assert result.exit_code == 0, result.output
    return re.sub(r"\x1b\[[0-9;]*m", "", result.output)


def test_dataset_list_forwards_every_filter_and_renders_a_table() -> None:
    svc = MagicMock()
    svc.list_datasets.return_value = _page(
        _dataset(),
        _dataset(
            database_id=42,
            view="ptools_rxns",
            available=False,
            attributes={"origin": "event", "protocol": "single", "variant": 0, "seed": 3, "generation": 12},
        ),
        next_offset=100,
    )
    out = _invoke(
        svc,
        "dataset",
        "list",
        "--kind",
        "ptools-analysis",
        "--tag",
        "cd2",
        "--attr",
        "variant=0",
        "--attr",
        "protocol=multiseed",
        "--simulation",
        "1002",
        "--source",
        "sim:1002",
        "--available",
        "any",
        "--limit",
        "100",
    )
    kwargs = svc.list_datasets.call_args.kwargs
    assert kwargs["kind"] == "ptools-analysis" and kwargs["tag"] == "cd2"
    assert kwargs["attrs"] == ["variant=0", "protocol=multiseed"]
    assert kwargs["simulation_id"] == 1002 and kwargs["source"] == "sim:1002" and kwargs["available"] == "any"
    assert "ptools_rna" in out and "ptools_rxns" in out
    assert "multiseed v=0" in out and "single v=0 s=3 g=12" in out
    assert "analysis 7" in out and "2.0 KB" in out
    assert "gone" in out  # the unavailable row says so
    assert "--offset 100" in out


def test_dataset_list_json_is_the_page_and_an_empty_page_says_why() -> None:
    svc = MagicMock()
    svc.list_datasets.return_value = _page(_dataset())
    out = _invoke(svc, "dataset", "list", "--json")
    assert json.loads(out)["datasets"][0]["uri"] == URI

    svc.list_datasets.return_value = _page()
    assert "No datasets match" in _invoke(svc, "dataset", "list")


def test_simulation_and_analysis_datasets_reach_their_routes() -> None:
    svc = MagicMock()
    svc.list_simulation_datasets.return_value = _page(_dataset())
    svc.list_analysis_datasets.return_value = _page(_dataset())

    out = _invoke(svc, "simulation", "datasets", "1002", "--include-analyses", "--kind", "figure")
    assert svc.list_simulation_datasets.call_args.args == (1002,)
    kwargs = svc.list_simulation_datasets.call_args.kwargs
    assert kwargs["include_analyses"] is True and kwargs["kind"] == "figure" and kwargs["available"] == "true"
    assert "sim 1002" in out

    out = _invoke(svc, "analysis", "datasets", "7", "--available", "false")
    assert svc.list_analysis_datasets.call_args.args == (7,)
    assert svc.list_analysis_datasets.call_args.kwargs["available"] == "false"
    assert "analysis 7" in out


def test_analysis_list_forwards_filters_and_shows_status() -> None:
    svc = MagicMock()
    svc.list_analyses.return_value = [
        _analysis(),
        _analysis(database_id=8, status=JobStatus.RUNNING, backend="k8s", tags=[]),
    ]
    out = _invoke(
        svc, "analysis", "list", "--status", "completed", "--source", "sim:1002", "--tag", "cd2", "--limit", "2"
    )
    kwargs = svc.list_analyses.call_args.kwargs
    assert kwargs["status"] == "completed" and kwargs["source"] == "sim:1002" and kwargs["tag"] == "cd2"
    assert kwargs["limit"] == 2
    assert "analysis-ptools-multiseed" in out and "completed" in out and "running" in out
    assert "--offset 2" in out  # a full page offers the next one

    _invoke(svc, "analysis", "list", "--limit", "0")
    assert svc.list_analyses.call_args.kwargs["limit"] is None  # 0 means every match

    out = _invoke(svc, "analysis", "list", "--json")
    assert [row["database_id"] for row in json.loads(out)] == [7, 8]


def test_dataset_get_fetch_tag_tags_and_attributes() -> None:
    svc = MagicMock()
    svc.get_dataset.return_value = _dataset()
    assert json.loads(_invoke(svc, "dataset", "get", "41"))["uri"] == URI

    with runner.isolated_filesystem():
        saved = Path("ptools_rna_multiseed__variant=0.tsv")
        saved.write_bytes(b"x" * 10)
        svc.fetch_dataset.return_value = saved
        out = _invoke(svc, "dataset", "fetch", "41", "--dest", "downloads/")
        assert svc.fetch_dataset.call_args.args == (41, "downloads/")
        assert "Saved dataset 41" in out and "10 B" in out

    svc.tag_dataset.return_value = _dataset(tags=["cd2", "keep"])
    out = _invoke(svc, "dataset", "tag", "41", "keep")
    assert svc.tag_dataset.call_args.args == (41, ["keep"]) and "keep" in out

    svc.list_dataset_tags.return_value = {"cd2": 12}
    out = _invoke(svc, "dataset", "tags", "--kind", "ptools-analysis")
    assert svc.list_dataset_tags.call_args.kwargs == {"kind": "ptools-analysis"} and "cd2" in out and "12" in out

    svc.list_dataset_attributes.return_value = {"variant": list(range(25)), "protocol": ["multiseed", "single"]}
    out = _invoke(svc, "dataset", "attributes")
    assert "protocol" in out and "multiseed, single" in out and "+5 more" in out


def test_dataset_provenance_renders_the_run_trace_span_and_inputs() -> None:
    upstream = _dataset(database_id=40, kind="parquet", uri="s3://bucket/vecoli-output/exp/history/variant=0/")
    svc = MagicMock()
    svc.get_dataset_provenance.return_value = DatasetProvenanceDTO(
        dataset=_dataset(),
        producer=DatasetProducerDTO(
            kind="analysis",
            id=7,
            name="analysis-ptools-multiseed",
            status="completed",
            source={"kind": "simulation", "ref": "1002"},
            tags=["cd2"],
            hpcrun_id=99,
            trace_id="4bf92f3577b34da6a3ce929d0e0e4736",
        ),
        span=SimulationSpan(span_id="00f067aa0ba902b7", name="analysis", attrs={"variant": 0}, status="ok"),
        inputs=[upstream],
    )
    out = _invoke(svc, "dataset", "provenance", "41")
    assert "written by" in out and "analysis 7" in out
    assert "4bf92f3577b34da6a3ce929d0e0e4736" in out and "hpcrun 99" in out
    assert "analysis[variant=0]" in out and "00f067aa0ba902b7" in out
    assert "1 registered dataset" in out and "history/variant=0/" in out

    svc.get_dataset_provenance.return_value = DatasetProvenanceDTO(
        dataset=_dataset(available=False), producer=DatasetProducerDTO(kind="analysis", id=8)
    )
    out = _invoke(svc, "dataset", "provenance", "41")
    assert "no traced run row" in out and "object gone" in out

    out = _invoke(svc, "dataset", "provenance", "41", "--json")
    assert json.loads(out)["producer"]["id"] == 8
