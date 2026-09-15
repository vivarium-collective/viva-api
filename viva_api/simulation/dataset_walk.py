"""Register the files a run wrote by WALKING S3: the backfill and reconciliation feeder (plan §5).

The trace feeder (:mod:`viva_api.simulation.dataset_registry`) is primary, but events are
best-effort by design (opt-in sink, size-skip, never raised), no producer emits
``artifact.written`` yet, and every bundle written before this code existed has no event
at all. This walk is the safety net: it lists a simulation's ``<out_uri>/analyses/``
prefix, registers the consumable files it finds with ``attributes.origin = "walk"``, and
marks rows unavailable when their object is gone. It never overwrites an event-sourced
row (``DatabaseService.upsert_dataset``).

What a bundle looks like (verified on dev, 2026-09-15)::

    <out_uri>/analyses/<bundle>/analysis.json                              -> report (listed, never read; 240 MB seen)
    <out_uri>/analyses/<bundle>/ptools/ptools_rna_multiseed__variant=0.tsv      -> ptools-analysis
    <out_uri>/analyses/<bundle>/ptools/ptools_rna__variant=0_seed=3_gen=12_agent=000.tsv
    <out_uri>/analyses/<bundle>/viz/<same naming>.html                         -> figure
    <out_uri>/analyses/<bundle>/driver.log                                      -> not a dataset

File names follow v2ecoli's ``analysis_runner``: ``f"{name}__{group}"`` with the group
key's ``/`` turned into ``_``. The group key's shape gives the protocol (``variant``
only = multiseed; ``variant, seed`` = multigeneration; ``variant, seed, gen, agent`` =
single; ``..., parent`` = multidaughter; ``all``), and a ``_<scale>`` suffix on the name,
when present, wins.

``n_tp`` comes from the TSV header, read with a small range request and skipped when the
object's size has not changed since the last walk. Real ptools headers are
``$  0m  124m ... 870m  0m_sd  124m_sd ...``: the timepoints are the columns after the
first that do not end in ``_sd``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from viva_api.analysis.models import DATASET_LIST_MAX_LIMIT, DATASET_ORIGIN_WALK
from viva_api.common.storage import data_layout
from viva_api.common.storage.file_paths import S3FilePath
from viva_api.simulation.tables_orm import AnalysisStatusDB

if TYPE_CHECKING:
    from viva_api.analysis.models import DatasetDTO
    from viva_api.common.storage.file_service import FileService, ListingItem
    from viva_api.simulation.database_service import DatabaseService
    from viva_api.simulation.models import Simulation

logger = logging.getLogger(__name__)

PTOOLS_DIR = "ptools/"
VIZ_DIR = "viz/"
REPORT_NAME = "analysis.json"
FIGURE_SUFFIXES: tuple[str, ...] = (".html", ".svg", ".png")
#: Scale suffixes an analysis name can carry (``ptools_rna_multiseed``).
SCALES: tuple[str, ...] = ("multigeneration", "multidaughter", "multiseed", "multivariant", "multiexperiment", "single")
#: Enough bytes for any ptools header line.
HEADER_BYTES = 8192
_MAX_REASONS = 5

_INT_KEYS = {"variant": "variant", "seed": "seed", "gen": "generation"}
_SHAPES: dict[frozenset[str], str] = {
    frozenset({"variant", "seed", "generation", "agent"}): "single",
    frozenset({"variant", "seed", "generation", "parent"}): "multidaughter",
    frozenset({"variant", "seed"}): "multigeneration",
    frozenset({"variant"}): "multiseed",
}


# ---------------------------------------------------------------------------
# pure helpers
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ArtifactName:
    view: str
    protocol: str | None
    coordinate: dict[str, Any]


def _coordinate_from_group(group: str) -> dict[str, Any] | None:
    coordinate: dict[str, Any] = {}
    if group == "all":
        return coordinate
    for part in group.split("_"):
        key, eq, value = part.partition("=")
        if not eq or not key:
            return None
        if key in _INT_KEYS:
            if not value.lstrip("-").isdigit():
                return None
            coordinate[_INT_KEYS[key]] = int(value)
        else:
            coordinate[key] = value
    return coordinate


def parse_artifact_name(filename: str) -> ArtifactName | None:
    """``view``, ``protocol`` and coordinate from a v2ecoli analysis output name, or ``None``
    when the name does not follow the ``<name>__<group>`` convention."""
    stem = filename.rsplit("/", 1)[-1]
    if "." in stem:
        stem = stem.rsplit(".", 1)[0]
    name, sep, group = stem.partition("__")
    if not sep or not name or not group:
        return None
    coordinate = _coordinate_from_group(group)
    if coordinate is None:
        return None
    view, protocol = name, None
    for scale in SCALES:
        if name.endswith(f"_{scale}"):
            view, protocol = name[: -len(scale) - 1], scale
            break
    if protocol is None:
        protocol = "all" if group == "all" else _SHAPES.get(frozenset(coordinate))
    return ArtifactName(view=view, protocol=protocol, coordinate=coordinate)


def timepoint_columns(header: str) -> int:
    """Timepoint columns of a ptools TSV header: those after the first that are not ``*_sd``."""
    columns = header.split("\n", 1)[0].rstrip("\r").split("\t")[1:]
    return sum(1 for column in columns if column and not column.endswith("_sd"))


def split_s3_uri(uri: str) -> tuple[str, str]:
    bucket, _, key = uri.removeprefix("s3://").partition("/")
    return bucket, key.strip("/")


def analyses_root_uri(simulation: Simulation) -> str:
    """``<out_uri>/analyses`` from the simulation's emitter config, else the Ray layout."""
    emitter_arg = getattr(simulation.config, "emitter_arg", None)
    out_uri = emitter_arg.get("out_uri") if isinstance(emitter_arg, dict) else None
    if isinstance(out_uri, str) and out_uri.startswith("s3://"):
        return f"{out_uri.rstrip('/')}/analyses"
    return data_layout.s3_uri(f"{data_layout.RayLayout.experiment_prefix(simulation.experiment_id)}/analyses")


def classify(relative: str) -> str | None:
    """The dataset kind of a path relative to a bundle directory, or ``None``."""
    if relative == REPORT_NAME:
        return "report"
    for directory, kind, suffixes in ((PTOOLS_DIR, "ptools-analysis", (".tsv",)), (VIZ_DIR, "figure", FIGURE_SUFFIXES)):
        rest = relative[len(directory) :] if relative.startswith(directory) else None
        if rest and "/" not in rest and rest.lower().endswith(suffixes):
            return kind
    return None


def _display_name(experiment_id: str, label: str, parsed: ArtifactName | None) -> str:
    parts = [experiment_id, label]
    if parsed is not None and parsed.protocol:
        parts.append(parsed.protocol)
        if "seed" in parsed.coordinate and "generation" in parsed.coordinate:
            parts.append(f"s{parsed.coordinate['seed']} g{parsed.coordinate['generation']}")
    return " · ".join(parts)


# ---------------------------------------------------------------------------
# walking
# ---------------------------------------------------------------------------


@dataclass
class WalkResult:
    bundles: int = 0
    analyses_created: int = 0
    registered: int = 0  # dataset rows inserted or changed
    unchanged: int = 0  # already registered exactly like this, or owned by an event
    skipped: int = 0
    unavailable: int = 0  # rows marked unavailable because their object is gone
    reasons: list[str] = field(default_factory=list)

    def skip(self, reason: str) -> None:
        self.skipped += 1
        if len(self.reasons) < _MAX_REASONS:
            self.reasons.append(reason)

    def merge(self, other: WalkResult) -> None:
        self.bundles += other.bundles
        self.analyses_created += other.analyses_created
        self.registered += other.registered
        self.unchanged += other.unchanged
        self.unavailable += other.unavailable
        for reason in other.reasons:
            self.skip(reason)
        self.skipped += other.skipped - len(other.reasons)


async def _rows_by_uri(db: DatabaseService, analysis_id: int) -> dict[str, DatasetDTO]:
    rows: dict[str, DatasetDTO] = {}
    offset = 0
    while True:
        page = await db.list_datasets(
            analysis_id=analysis_id, available=None, limit=DATASET_LIST_MAX_LIMIT, offset=offset
        )
        rows.update({row.uri: row for row in page})
        if len(page) < DATASET_LIST_MAX_LIMIT:
            return rows
        offset += len(page)


async def _n_tp(item: ListingItem, existing: DatasetDTO | None, file_service: FileService) -> int | None:
    if existing is not None and existing.size_bytes == item.Size and isinstance(existing.attributes.get("n_tp"), int):
        return int(existing.attributes["n_tp"])
    head = await file_service.get_file_head(S3FilePath(s3_path=Path(item.Key)), HEADER_BYTES)
    if not head:
        return None
    return timepoint_columns(head.decode("utf-8", errors="replace")) or None


def _simulation_source(simulation: Simulation, parsed: ArtifactName | None) -> dict[str, Any]:
    source: dict[str, Any] = {
        "kind": "simulation",
        "ref": str(simulation.database_id),
        "resolved_id": simulation.database_id,
    }
    if parsed is not None and (parsed.coordinate or parsed.protocol):
        source["coordinate"] = {**parsed.coordinate, **({"protocol": parsed.protocol} if parsed.protocol else {})}
    return source


async def _file_fields(
    item: ListingItem,
    *,
    kind: str,
    filename: str,
    bundle_name: str,
    existing: DatasetDTO | None,
    simulation: Simulation,
    file_service: FileService,
    extra_tags: list[str],
) -> dict[str, Any]:
    """The ``upsert_dataset`` fields a walked file contributes (everything but uri, kind, producer)."""
    parsed = parse_artifact_name(filename) if kind != "report" else None
    attributes: dict[str, Any] = {"name": filename, "analysis_dir": bundle_name}
    if parsed is not None:
        attributes.update(parsed.coordinate)
        if parsed.protocol:
            attributes["protocol"] = parsed.protocol
    if kind == "ptools-analysis":
        n_tp = await _n_tp(item, existing, file_service)
        if n_tp:
            attributes["n_tp"] = n_tp
    view = parsed.view if parsed is not None else None
    return {
        "view": view,
        "display_name": _display_name(simulation.experiment_id, view or filename, parsed),
        "size_bytes": item.Size,
        "attributes": attributes,
        "tags": [*simulation.tags, *extra_tags],
        "source": _simulation_source(simulation, parsed),
    }


async def register_bundle(
    items: list[ListingItem],
    *,
    bucket: str,
    bundle_key: str,
    analysis_id: int,
    simulation: Simulation,
    db: DatabaseService,
    file_service: FileService,
    tags: list[str] | None = None,
) -> WalkResult:
    """Register every consumable file under one bundle directory for ``analysis_id``, and
    mark that analysis's rows under the bundle unavailable when their object is gone.
    ``tags`` are added to the simulation's on every row (tags only ever union-merge)."""
    result = WalkResult(bundles=1)
    prefix = bundle_key.rstrip("/") + "/"
    bundle_name = prefix.rstrip("/").rsplit("/", 1)[-1]
    existing = await _rows_by_uri(db, analysis_id)
    seen: set[str] = set()
    for item in items:
        if not item.Key.startswith(prefix):
            continue
        relative = item.Key[len(prefix) :]
        kind = classify(relative)
        if kind is None:
            continue
        uri = f"s3://{bucket}/{item.Key}"
        seen.add(uri)
        fields = await _file_fields(
            item,
            kind=kind,
            filename=relative.rsplit("/", 1)[-1],
            bundle_name=bundle_name,
            existing=existing.get(uri),
            simulation=simulation,
            file_service=file_service,
            extra_tags=list(tags or []),
        )
        try:
            _dto, action = await db.upsert_dataset(
                uri=uri,
                kind=kind,
                origin=DATASET_ORIGIN_WALK,
                analysis_id=analysis_id,
                available=True,
                **fields,
            )
        except ValueError as refused:
            result.skip(f"{uri}: {refused}")
            continue
        if action in ("inserted", "updated"):
            result.registered += 1
        else:
            result.unchanged += 1

    gone_prefix = f"s3://{bucket}/{prefix}"
    for uri, row in existing.items():
        if uri.startswith(gone_prefix) and uri not in seen and row.available:
            await db.set_dataset_available(row.database_id, False)
            result.unavailable += 1
    return result


async def _analysis_for_bundle(
    result_uri: str, name: str, *, simulation: Simulation, out_uri: str, db: DatabaseService, result: WalkResult
) -> int:
    """The analysis run a bundle belongs to; a bundle nobody registered (a hand-dispatched
    fill) gets a READY run row of its own, ``backend = "walk"``."""
    existing = await db.get_analysis_by_result_uri(result_uri)
    if existing is not None:
        return existing.database_id
    record = await db.record_analysis(
        experiment_id=simulation.experiment_id,
        n_tp=None,
        status=AnalysisStatusDB.READY,
        config={"analysis_options": {"experiment_id": [simulation.experiment_id]}, "discovered_by": "walk"},
        name=name,
        simulation_id=simulation.database_id,
        backend="walk",
        result_uri=result_uri,
        source={
            "kind": "simulation",
            "ref": str(simulation.database_id),
            "resolved_id": simulation.database_id,
            "uri": out_uri,
        },
        tags=list(simulation.tags),
    )
    result.analyses_created += 1
    return record.database_id


async def _mark_vanished_bundles(
    simulation: Simulation, *, root: str, present: set[str], db: DatabaseService, result: WalkResult
) -> None:
    """An analysis whose whole bundle directory is gone: every row under it is unavailable."""
    for analysis in await db.list_analyses(simulation_id=simulation.database_id):
        result_uri = (analysis.result_uri or "").rstrip("/")
        if not result_uri.startswith(f"{root}/") or result_uri.rsplit("/", 1)[-1] in present:
            continue
        for row in (await _rows_by_uri(db, analysis.database_id)).values():
            if row.available and row.uri.startswith(f"{result_uri}/"):
                await db.set_dataset_available(row.database_id, False)
                result.unavailable += 1


async def reconcile_simulation(
    simulation: Simulation,
    *,
    db: DatabaseService,
    file_service: FileService,
    storage_bucket: str | None,
) -> WalkResult:
    """Walk one simulation's ``analyses/`` prefix: register bundles, create missing run rows,
    and mark rows whose objects (or whole bundles) are gone. One listing per simulation."""
    result = WalkResult()
    root = analyses_root_uri(simulation)
    bucket, root_key = split_s3_uri(root)
    if storage_bucket and bucket != storage_bucket:
        result.skip(f"{root} is not in the file service's bucket {storage_bucket}")
        return result

    root_prefix = root_key.rstrip("/") + "/"
    groups: dict[str, list[ListingItem]] = {}
    for item in await file_service.get_listing(S3FilePath(s3_path=Path(root_key))):
        if not item.Key.startswith(root_prefix):
            continue
        name, sep, rest = item.Key[len(root_prefix) :].partition("/")
        if sep and rest:
            groups.setdefault(name, []).append(item)

    out_uri = root.rsplit("/analyses", 1)[0]
    for name, items in sorted(groups.items()):
        bundle_key = f"{root_prefix}{name}"
        if not any(classify(item.Key[len(bundle_key) + 1 :]) for item in items):
            continue  # nothing consumable (e.g. only a driver.log): not a dataset bundle
        analysis_id = await _analysis_for_bundle(
            f"s3://{bucket}/{bundle_key}", name, simulation=simulation, out_uri=out_uri, db=db, result=result
        )
        result.merge(
            await register_bundle(
                items,
                bucket=bucket,
                bundle_key=bundle_key,
                analysis_id=analysis_id,
                simulation=simulation,
                db=db,
                file_service=file_service,
            )
        )

    await _mark_vanished_bundles(simulation, root=root, present=set(groups), db=db, result=result)
    return result
