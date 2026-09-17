"""Register the files a run wrote by WALKING S3: the backfill and reconciliation feeder (plan §5).

The trace feeder (:mod:`viva_api.simulation.dataset_registry`) is primary, but events are
best-effort by design (opt-in sink, size-skip, never raised), no producer emits
``artifact.written`` yet, and every bundle written before this code existed has no event
at all. This walk is the safety net: it lists a simulation's ``<out_uri>/analyses/``
prefix, registers the consumable files it finds with ``attributes.origin = "walk"``, and
marks rows unavailable when their object is gone. It never overwrites an event-sourced
row (``DatabaseService.upsert_dataset``).

The walk never creates run rows. A bundle belongs to the analysis run whose ``result_uri`` is
the bundle directory; a bundle no run claims (a hand-dispatched fill, say) is attributed to
the simulation whose output it sits under, and ``origin = walk`` says nobody claimed it. When
a real producer is recorded later, the next walk moves the rows to it.

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
from typing import TYPE_CHECKING

from viva_api.analysis.models import (
    DATASET_LIST_MAX_LIMIT,
    DATASET_ORIGIN_WALK,
    DatasetFields,
    JsonDict,
    ProducerRef,
)
from viva_api.common.storage import data_layout
from viva_api.common.storage.file_paths import S3FilePath

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
    coordinate: JsonDict


def _coordinate_from_group(group: str) -> JsonDict | None:
    coordinate: JsonDict = {}
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
    unclaimed: int = 0  # bundles no analysis run claims, attributed to the simulation
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
        self.unclaimed += other.unclaimed
        self.registered += other.registered
        self.unchanged += other.unchanged
        self.unavailable += other.unavailable
        for reason in other.reasons:
            self.skip(reason)
        self.skipped += other.skipped - len(other.reasons)


async def _rows_under(db: DatabaseService, uri_prefix: str) -> dict[str, DatasetDTO]:
    """Every registered row whose uri starts with ``uri_prefix``, available or not, by uri."""
    rows: dict[str, DatasetDTO] = {}
    offset = 0
    while True:
        page = await db.list_datasets(
            uri_prefix=uri_prefix, available=None, limit=DATASET_LIST_MAX_LIMIT, offset=offset
        )
        rows.update({row.uri: row for row in page})
        if len(page) < DATASET_LIST_MAX_LIMIT:
            return rows
        offset += len(page)


async def _n_tp(item: ListingItem, existing: DatasetDTO | None, file_service: FileService) -> int | None:
    """Timepoint columns of a ptools TSV: the recorded count when the object has not changed, a
    ranged header read otherwise. ``None`` means the header could not be READ, nothing else.

    **Zero is an answer, not a failure.** A ``ptools_overview`` header is commentary with no
    timepoint row, so the count is legitimately 0 -- and it must be recorded, or the cache can
    never hold for those files and every walk re-reads them forever. That was measured on the
    2026-09-16 registry (viva-api#673): 9,629 rows, one ranged GET each, on every pass, which was
    ~99.96% of a re-walk's wall clock. Only an unreadable header stays uncached, so a transient
    read failure still retries.
    """
    cached = existing.attributes.get("n_tp") if existing is not None and existing.size_bytes == item.Size else None
    if isinstance(cached, int) and not isinstance(cached, bool):
        return cached
    head = await file_service.get_file_head(S3FilePath(s3_path=Path(item.Key)), HEADER_BYTES)
    if not head:
        return None
    return timepoint_columns(head.decode("utf-8", errors="replace"))


def _simulation_source(simulation: Simulation, parsed: ArtifactName | None) -> JsonDict:
    source: JsonDict = {
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
) -> DatasetFields:
    """The ``upsert_dataset`` fields a walked file contributes (everything but uri, kind, producer)."""
    parsed = parse_artifact_name(filename) if kind != "report" else None
    attributes: JsonDict = {"name": filename, "analysis_dir": bundle_name}
    if parsed is not None:
        attributes.update(parsed.coordinate)
        if parsed.protocol:
            attributes["protocol"] = parsed.protocol
    if kind == "ptools-analysis":
        n_tp = await _n_tp(item, existing, file_service)
        if n_tp is not None:  # 0 is a real count and must be stored, or it is re-read forever
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


def _present(uri: str, bucket: str, keys: set[str]) -> bool:
    """Whether a row's object is in a listing; a directory-like uri is present while anything is under it."""
    key = uri.removeprefix(f"s3://{bucket}/")
    return any(k.startswith(key) for k in keys) if key.endswith("/") else key in keys


async def _mark_vanished(
    rows: dict[str, DatasetDTO],
    *,
    bucket: str,
    keys: set[str],
    under: str,
    db: DatabaseService,
    result: WalkResult,
    skip: tuple[str, ...] = (),
) -> None:
    """Mark the rows under ``under`` whose object the listing no longer has unavailable, whoever
    registered them (availability is a fact about the object). ``skip``: prefixes already handled."""
    for uri, row in rows.items():
        if not row.available or not uri.startswith(under) or (skip and uri.startswith(skip)):
            continue
        if not _present(uri, bucket, keys):
            await db.set_dataset_available(row.database_id, False)
            result.unavailable += 1


async def register_bundle(
    items: list[ListingItem],
    *,
    bucket: str,
    bundle_key: str,
    producer: ProducerRef,
    simulation: Simulation,
    db: DatabaseService,
    file_service: FileService,
    tags: list[str] | None = None,
    existing: dict[str, DatasetDTO] | None = None,
) -> WalkResult:
    """Register every consumable file under one bundle directory for ``producer``
    (``{"analysis_id": id}`` or ``{"simulation_id": id}``), and mark rows under the bundle
    unavailable when their object is gone. ``tags`` are added to the simulation's on every row
    (tags only ever union-merge). ``existing``: the registered rows under the bundle by uri,
    read from the database when not given."""
    result = WalkResult(bundles=1)
    prefix = bundle_key.rstrip("/") + "/"
    bundle_name = prefix.rstrip("/").rsplit("/", 1)[-1]
    uri_prefix = f"s3://{bucket}/{prefix}"
    rows = existing if existing is not None else await _rows_under(db, uri_prefix)
    for item in items:
        relative = item.Key[len(prefix) :] if item.Key.startswith(prefix) else ""
        kind = classify(relative) if relative else None
        if kind is None:
            continue
        uri = f"s3://{bucket}/{item.Key}"
        fields = await _file_fields(
            item,
            kind=kind,
            filename=relative.rsplit("/", 1)[-1],
            bundle_name=bundle_name,
            existing=rows.get(uri),
            simulation=simulation,
            file_service=file_service,
            extra_tags=list(tags or []),
        )
        try:
            dto, action = await db.upsert_dataset(
                uri=uri, kind=kind, origin=DATASET_ORIGIN_WALK, available=True, **producer, **fields
            )
        except ValueError as refused:
            result.skip(f"{uri}: {refused}")
            continue
        if action == "skipped" and not dto.available:
            # An event-sourced row the walk may not rewrite, whose object is back.
            await db.set_dataset_available(dto.database_id, True)
            action = "updated"
        result.registered += action in ("inserted", "updated")
        result.unchanged += action not in ("inserted", "updated")

    await _mark_vanished(rows, bucket=bucket, keys={item.Key for item in items}, under=uri_prefix, db=db, result=result)
    return result


async def _producer_for_bundle(
    result_uri: str, simulation: Simulation, db: DatabaseService, result: WalkResult
) -> ProducerRef:
    """The analysis run that claims a bundle (its ``result_uri`` is the bundle directory), else the
    simulation whose output the bundle sits under. Never creates a run row."""
    claimed = await db.get_analysis_by_result_uri(result_uri)
    if claimed is not None:
        return {"analysis_id": claimed.database_id}
    result.unclaimed += 1
    return {"simulation_id": simulation.database_id}


async def reconcile_simulation(
    simulation: Simulation,
    *,
    db: DatabaseService,
    file_service: FileService,
    storage_bucket: str | None,
) -> WalkResult:
    """Walk one simulation's ``analyses/`` prefix: register each bundle's files for the run that
    claims the bundle (else the simulation), and mark rows whose objects, or whole bundles, are
    gone. One listing per simulation; never creates a run row."""
    result = WalkResult()
    root = analyses_root_uri(simulation)
    bucket, root_key = split_s3_uri(root)
    if storage_bucket and bucket != storage_bucket:
        result.skip(f"{root} is not in the file service's bucket {storage_bucket}")
        return result

    root_prefix = root_key.rstrip("/") + "/"
    listing = [
        item
        for item in await file_service.get_listing(S3FilePath(s3_path=Path(root_key)))
        if item.Key.startswith(root_prefix)
    ]
    groups: dict[str, list[ListingItem]] = {}
    for item in listing:
        name, sep, rest = item.Key[len(root_prefix) :].partition("/")
        if sep and rest:
            groups.setdefault(name, []).append(item)

    root_uri = f"s3://{bucket}/{root_prefix}"
    existing = await _rows_under(db, root_uri)
    handled: list[str] = []
    for name, items in sorted(groups.items()):
        bundle_key = f"{root_prefix}{name}"
        if not any(classify(item.Key[len(bundle_key) + 1 :]) for item in items):
            continue  # nothing consumable (e.g. only a driver.log): not a dataset bundle
        producer = await _producer_for_bundle(f"s3://{bucket}/{bundle_key}", simulation, db, result)
        result.merge(
            await register_bundle(
                items,
                bucket=bucket,
                bundle_key=bundle_key,
                producer=producer,
                simulation=simulation,
                db=db,
                file_service=file_service,
                existing=existing,
            )
        )
        handled.append(f"s3://{bucket}/{bundle_key}/")

    # Rows under a bundle directory that is gone altogether.
    keys = {item.Key for item in listing}
    await _mark_vanished(existing, bucket=bucket, keys=keys, under=root_uri, skip=tuple(handled), db=db, result=result)
    return result
