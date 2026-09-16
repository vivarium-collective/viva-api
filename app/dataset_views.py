"""How datasets, their producers and analyses read, in every client (data provenance slice 1).

The CLI, the TUI and the marimo GUI are one workflow in three media (CLAUDE.md, EUTE), so
they share these words rather than each re-deriving them. The one distinction all three
must keep is the producer relation: a file its run WROTE, versus a file the S3 walk only
FOUND UNDER a simulation's output that no analysis run claims (plan-data-provenance.md §5).

Plain strings only: each client adds its own styling and escaping.
"""

from __future__ import annotations

import shlex
from typing import Any

from viva_api.analysis.models import DATASET_KINDS, DatasetDTO, DatasetProducerDTO, ExperimentAnalysisDTO

DATASET_COLUMNS: tuple[str, ...] = ("ID", "Kind", "View", "Coordinate", "Producer", "Size", "Origin", "Updated")
ANALYSIS_COLUMNS: tuple[str, ...] = ("ID", "Name", "Status", "Backend", "Sim", "Experiment", "Tags", "Updated")

NO_DATASETS_HINT = "No datasets match. Rows appear as a run's trace is ingested or the S3 walk finds them."
WRITTEN_BY = "written by"
FOUND_UNDER = "found under"
UNCLAIMED_HINT = "found by the S3 walk; no analysis run claims this bundle"
NO_PRODUCER_HINT = "no producer run recorded"
NO_TRACE_HINT = "no traced run row"
AVAILABILITY_CHOICES: tuple[str, ...] = ("true", "false", "any")

#: What a filter string may name for ``E2EDataService.list_datasets``; any other ``key=value``
#: is an attribute filter (``variant=0``, ``protocol=multiseed``).
_DATASET_FILTER_INTS = {"simulation": "simulation_id", "analysis": "analysis_id", "parca": "parca_dataset_id"}
_DATASET_FILTER_STRS = ("kind", "view", "tag", "source", "available", "since", "uri_prefix")
_PAGING = ("limit", "offset")

#: What a filter string may name for ``E2EDataService.list_analyses``.
_ANALYSIS_FILTER_STRS = {"experiment": "experiment_id", "status": "status", "backend": "backend", "source": "source"}
_ANALYSIS_FILTER_MORE = ("tag", "since")


def coordinate_label(attributes: dict[str, Any]) -> str:
    """``multiseed v=0`` / ``single v=0 s=3 g=12 a=000`` from a dataset's attributes."""
    parts = [str(attributes["protocol"])] if attributes.get("protocol") else []
    for key, short in (("variant", "v"), ("seed", "s"), ("generation", "g"), ("agent", "a")):
        if attributes.get(key) is not None:
            parts.append(f"{short}={attributes[key]}")
    return " ".join(parts)


def format_bytes(size: int | None) -> str:
    if size is None:
        return "—"
    for unit, scale in (("GB", 1024**3), ("MB", 1024**2), ("KB", 1024)):
        if size >= scale:
            return f"{size / scale:.1f} {unit}"
    return f"{size} B"


def producer_label(dataset: DatasetDTO) -> str:
    """``analysis 7`` / ``sim 1002`` / ``parca 3``: the one producer id the registry set."""
    for label, value in (
        ("analysis", dataset.analysis_id),
        ("sim", dataset.simulation_id),
        ("parca", dataset.parca_dataset_id),
    ):
        if value is not None:
            return f"{label} {value}"
    return "—"


def dataset_name(dataset: DatasetDTO) -> str:
    """The view, else the recorded name, else the last segment of the uri."""
    return dataset.view or str(dataset.attributes.get("name") or dataset.uri.rstrip("/").rsplit("/", 1)[-1])


def origin_label(dataset: DatasetDTO) -> str:
    """``event`` / ``walk`` (how the row came to exist), ``—`` when unrecorded."""
    return dataset.origin or "—"


def dataset_row(dataset: DatasetDTO) -> list[str]:
    """One row under :data:`DATASET_COLUMNS`, unstyled; a gone object reads ``<origin> gone``."""
    origin = origin_label(dataset) + ("" if dataset.available else " gone")
    return [
        str(dataset.database_id),
        dataset.kind,
        dataset_name(dataset),
        coordinate_label(dataset.attributes),
        producer_label(dataset),
        format_bytes(dataset.size_bytes),
        origin,
        (dataset.updated_at or "")[:19],
    ]


def analysis_status_label(analysis: ExperimentAnalysisDTO) -> str:
    return analysis.status.value if analysis.status is not None else "unknown"


def analysis_row(analysis: ExperimentAnalysisDTO) -> list[str]:
    """One row under :data:`ANALYSIS_COLUMNS`, unstyled."""
    return [
        str(analysis.database_id),
        analysis.name,
        analysis_status_label(analysis),
        analysis.backend or "—",
        str(analysis.simulation_id) if analysis.simulation_id is not None else "—",
        analysis.experiment_id or "—",
        ", ".join(analysis.tags) or "—",
        analysis.last_updated[:19],
    ]


def is_unclaimed(dataset: DatasetDTO, producer: DatasetProducerDTO | None) -> bool:
    """The walk found this under a simulation's output and no analysis run claims it: the
    simulation's output holds it, but nothing says its run wrote it."""
    return producer is not None and dataset.origin == "walk" and producer.kind == "simulation"


def producer_relation(dataset: DatasetDTO, producer: DatasetProducerDTO) -> str:
    """:data:`FOUND_UNDER` for an unclaimed walk-found bundle, else :data:`WRITTEN_BY`."""
    return FOUND_UNDER if is_unclaimed(dataset, producer) else WRITTEN_BY


def _tokens(text: str) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    for token in shlex.split(text or ""):
        key, sep, value = token.partition("=")
        if not sep or not key:
            raise ValueError(f"expected key=value, got {token!r}")
        pairs.append((key.strip(), value))
    return pairs


def _int(key: str, value: str) -> int:
    try:
        return int(value)
    except ValueError:
        raise ValueError(f"{key} must be a number, got {value!r}") from None


def parse_dataset_filter(text: str) -> dict[str, Any]:
    """``kind=ptools-analysis tag=cd2 variant=0`` → keyword arguments for ``list_datasets``.

    ``simulation`` / ``analysis`` / ``parca`` name the producer; ``kind``, ``view``, ``tag``,
    ``source``, ``available`` (true|false|any), ``since``, ``limit`` and ``offset`` are the
    listing's own filters; every other ``key=value`` becomes an attribute filter, exactly
    like the CLI's repeatable ``--attr``. Raises ``ValueError`` naming the bad token."""
    kwargs: dict[str, Any] = {}
    attrs: list[str] = []
    for key, value in _tokens(text):
        if key in _DATASET_FILTER_INTS:
            kwargs[_DATASET_FILTER_INTS[key]] = _int(key, value)
        elif key in _PAGING:
            kwargs[key] = _int(key, value)
        elif key in _DATASET_FILTER_STRS:
            kwargs[key] = value
        else:
            attrs.append(f"{key}={value}")
    if "kind" in kwargs and kwargs["kind"] not in DATASET_KINDS:
        raise ValueError(f"kind must be one of {', '.join(DATASET_KINDS)}; got {kwargs['kind']!r}")
    if "available" in kwargs and kwargs["available"] not in AVAILABILITY_CHOICES:
        raise ValueError(f"available must be true, false or any; got {kwargs['available']!r}")
    if attrs:
        kwargs["attrs"] = attrs
    return kwargs


def parse_analysis_filter(text: str) -> dict[str, Any]:
    """``experiment=exp tag=cd2 status=completed`` → keyword arguments for ``list_analyses``.

    Names: ``experiment``, ``simulation``, ``status``, ``backend``, ``source``, ``tag``,
    ``since``, ``limit``, ``offset``. Anything else raises ``ValueError``: an analysis has
    no free-form attributes to filter on."""
    kwargs: dict[str, Any] = {}
    for key, value in _tokens(text):
        if key == "simulation":
            kwargs["simulation_id"] = _int(key, value)
        elif key in _PAGING:
            kwargs[key] = _int(key, value)
        elif key in _ANALYSIS_FILTER_STRS:
            kwargs[_ANALYSIS_FILTER_STRS[key]] = value
        elif key in _ANALYSIS_FILTER_MORE:
            kwargs[key] = value
        else:
            names = sorted([*_ANALYSIS_FILTER_STRS, *_ANALYSIS_FILTER_MORE, "simulation", *_PAGING])
            raise ValueError(f"unknown analysis filter {key!r}; use {', '.join(names)}")
    return kwargs
