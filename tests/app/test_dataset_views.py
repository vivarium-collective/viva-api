"""The words the CLI, TUI and GUI share for datasets (``app.dataset_views``).

The distinction pinned hardest is the producer relation: a bundle the S3 walk only found
under a simulation's output reads "found under", never "written by".
"""

from __future__ import annotations

from typing import Any

import pytest

from app.dataset_views import (
    FOUND_UNDER,
    WRITTEN_BY,
    analysis_row,
    coordinate_label,
    dataset_name,
    dataset_row,
    format_bytes,
    is_unclaimed,
    parse_analysis_filter,
    parse_dataset_filter,
    producer_label,
    producer_relation,
)
from viva_api.analysis.models import (
    AnalysisConfig,
    AnalysisConfigOptions,
    DatasetDTO,
    DatasetProducerDTO,
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
        "updated_at": "2026-09-15T01:02:03.456",
    }
    values.update(overrides)
    return DatasetDTO(**values)


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
        "tags": ["cd2", "demo"],
    }
    values.update(overrides)
    return ExperimentAnalysisDTO(**values)


# --- labels -------------------------------------------------------------------


def test_coordinate_label_orders_protocol_then_short_keys() -> None:
    attributes = {"agent": "000", "generation": 12, "seed": 3, "variant": 0, "protocol": "single"}
    assert coordinate_label(attributes) == "single v=0 s=3 g=12 a=000"
    assert coordinate_label({"variant": 0}) == "v=0"
    assert coordinate_label({}) == ""


@pytest.mark.parametrize(
    ("size", "label"), [(None, "—"), (512, "512 B"), (2048, "2.0 KB"), (5 * 1024**2, "5.0 MB"), (3 * 1024**3, "3.0 GB")]
)
def test_format_bytes(size: int | None, label: str) -> None:
    assert format_bytes(size) == label


def test_producer_label_names_the_one_producer() -> None:
    assert producer_label(_dataset()) == "sim 1002"
    assert producer_label(_dataset(simulation_id=None, analysis_id=7)) == "analysis 7"
    assert producer_label(_dataset(simulation_id=None, parca_dataset_id=3)) == "parca 3"
    assert producer_label(_dataset(simulation_id=None)) == "—"


def test_dataset_name_falls_back_from_view_to_name_to_uri() -> None:
    assert dataset_name(_dataset()) == "ptools_rna"
    assert dataset_name(_dataset(view=None, attributes={"name": "summary.json"})) == "summary.json"
    assert dataset_name(_dataset(view=None, attributes={})) == "ptools_rna_multiseed__variant=0.tsv"


def test_dataset_row_matches_the_columns_and_marks_a_gone_object() -> None:
    assert dataset_row(_dataset()) == [
        "41",
        "ptools-analysis",
        "ptools_rna",
        "multiseed v=0",
        "sim 1002",
        "2.0 KB",
        "walk",
        "2026-09-15T01:02:03",
    ]
    assert dataset_row(_dataset(available=False))[6] == "walk gone"
    assert dataset_row(_dataset(attributes={}))[6] == "—"


def test_analysis_row() -> None:
    assert analysis_row(_analysis()) == [
        "7",
        "analysis-ptools-multiseed",
        "completed",
        "ray",
        "1002",
        "exp",
        "cd2, demo",
        "2026-09-15 01:02:03",
    ]
    row = analysis_row(_analysis(backend=None, simulation_id=None, experiment_id=None, tags=[]))
    assert row[3:7] == ["—", "—", "—", "—"]


# --- the producer relation ----------------------------------------------------


def test_a_walk_found_bundle_under_a_simulation_is_found_under_not_written_by() -> None:
    simulation = DatasetProducerDTO(kind="simulation", id=1002)
    assert is_unclaimed(_dataset(), simulation)
    assert producer_relation(_dataset(), simulation) == FOUND_UNDER


@pytest.mark.parametrize(
    ("origin", "producer_kind"), [("event", "simulation"), ("walk", "analysis"), ("event", "analysis"), (None, "parca")]
)
def test_everything_else_was_written_by_its_producer(origin: str | None, producer_kind: str) -> None:
    attributes = {"origin": origin} if origin else {}
    dataset = _dataset(attributes=attributes)
    producer = DatasetProducerDTO(kind=producer_kind, id=7)
    assert not is_unclaimed(dataset, producer)
    assert producer_relation(dataset, producer) == WRITTEN_BY


def test_no_producer_is_not_unclaimed() -> None:
    assert not is_unclaimed(_dataset(), None)


# --- filter strings -----------------------------------------------------------


def test_dataset_filter_maps_named_filters_and_treats_the_rest_as_attributes() -> None:
    kwargs = parse_dataset_filter(
        "kind=ptools-analysis tag=cd2 view=ptools_rna simulation=1002 available=any "
        'limit=20 variant=0 protocol=multiseed "display=two words"'
    )
    assert kwargs == {
        "kind": "ptools-analysis",
        "tag": "cd2",
        "view": "ptools_rna",
        "simulation_id": 1002,
        "available": "any",
        "limit": 20,
        "attrs": ["variant=0", "protocol=multiseed", "display=two words"],
    }
    assert parse_dataset_filter("analysis=7 parca=3") == {"analysis_id": 7, "parca_dataset_id": 3}
    assert parse_dataset_filter("") == {}
    assert parse_dataset_filter("   ") == {}


@pytest.mark.parametrize(
    ("text", "message"),
    [
        ("kind=table", "kind must be one of"),
        ("available=maybe", "available must be true, false or any"),
        ("simulation=abc", "simulation must be a number"),
        ("cd2", "expected key=value"),
        ("=cd2", "expected key=value"),
    ],
)
def test_a_bad_dataset_filter_names_the_problem(text: str, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        parse_dataset_filter(text)


def test_analysis_filter_maps_names_and_refuses_unknown_keys() -> None:
    assert parse_analysis_filter("experiment=exp simulation=1002 status=completed tag=cd2 limit=5 offset=10") == {
        "experiment_id": "exp",
        "simulation_id": 1002,
        "status": "completed",
        "tag": "cd2",
        "limit": 5,
        "offset": 10,
    }
    with pytest.raises(ValueError, match="unknown analysis filter 'variant'"):
        parse_analysis_filter("variant=0")
