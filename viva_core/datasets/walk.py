"""Register the files a run wrote by WALKING its storage: the backfill and reconciliation feeder.

The trace feeder (:mod:`viva_core.datasets.registry`) is primary, but events are best-effort by
design (opt-in sink, size-skip, never raised), and every bundle written before an emitter existed
has no event at all. This walk is the safety net: it lists one source's root prefix, registers the
consumable files it finds with ``attributes.origin = "walk"``, and marks rows unavailable when their
object is gone. It never overwrites an event-sourced row (the store's ``upsert``).

What a root looks like: a directory of **bundles**, each a directory of files::

    <root>/<bundle>/...      -> the classifier says which files are datasets, and of what kind

The walk never creates run rows. A bundle belongs to whoever the source says claims it (an
application's analysis run whose recorded output is the bundle directory, say); a bundle nobody
claims is attributed to the source's own owner, and ``origin = walk`` says nobody claimed it. When
a real claimant is recorded later, the next walk moves the rows to it.

**The walk never opens an object.** Every field it records comes from the LISTING -- key, size,
and what the classifier reads off the file name. Content features are the business of whoever
knows the format: they arrive in the ``artifact.written`` payload from the producer that wrote
the file. (Reading each file's header once cost ~99.96% of a re-walk's wall clock: viva-api#673.)

What core does not know it asks through two Protocols: an :class:`ArtifactClassifier` (which
relative paths are datasets, their kind, and the view / protocol / coordinate their name
encodes -- the application's file-naming convention) and a :class:`WalkSource` (where one
source's bundles live, what its data is OF, and who claims a bundle).

This is the application's ``dataset_walk`` moved verbatim (P4a-2, slice 2), bar those two seams.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

from viva_core.datasets.models import (
    DATASET_ORIGIN_WALK,
    DatasetFields,
    DatasetRecord,
    DatasetWriter,
    JsonDict,
    OwnerRef,
)
from viva_core.storage.file_paths import S3FilePath
from viva_core.storage.file_service import FileService, ListingItem

logger = logging.getLogger(__name__)

_MAX_REASONS = 5


# ---------------------------------------------------------------------------
# the seams
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Artifact:
    """What a classifier reads off one file's path: its dataset ``kind``, and -- when the name
    encodes them -- the logical ``view``, the ``protocol`` and the coordinate."""

    kind: str
    view: str | None = None
    protocol: str | None = None
    coordinate: JsonDict = field(default_factory=dict)


class ArtifactClassifier(Protocol):
    """Which files under a bundle are datasets. ``relative`` is the path below the bundle
    directory; ``None`` means "not a dataset" (a driver log, a scratch file)."""

    def classify(self, relative: str) -> Artifact | None: ...


class WalkSource(Protocol):
    """One thing to walk. ``root_uri`` is the ``s3://bucket/prefix`` whose children are bundles;
    ``owner`` is who the bundles belong to when nobody claims them; ``label`` and ``tags`` go on
    every row; ``subject`` is what the data is OF (``None`` for none)."""

    @property
    def root_uri(self) -> str: ...

    @property
    def owner(self) -> OwnerRef: ...

    @property
    def label(self) -> str: ...

    @property
    def tags(self) -> Sequence[str]: ...

    @property
    def subject(self) -> JsonDict | None: ...

    async def claimant(self, bundle_uri: str) -> OwnerRef | None:
        """The record that claims a bundle directory, or ``None``. Never creates one."""
        ...


# ---------------------------------------------------------------------------
# pure helpers
# ---------------------------------------------------------------------------


def split_s3_uri(uri: str) -> tuple[str, str]:
    bucket, _, key = uri.removeprefix("s3://").partition("/")
    return bucket, key.strip("/")


def _display_name(label: str, part: str, artifact: Artifact | None) -> str:
    parts = [label, part]
    if artifact is not None and artifact.protocol:
        parts.append(artifact.protocol)
        if "seed" in artifact.coordinate and "generation" in artifact.coordinate:
            parts.append(f"s{artifact.coordinate['seed']} g{artifact.coordinate['generation']}")
    return " · ".join(parts)


def _source(subject: JsonDict | None, artifact: Artifact | None) -> JsonDict | None:
    if subject is None:
        return None
    source: JsonDict = dict(subject)
    if artifact is not None and (artifact.coordinate or artifact.protocol):
        source["coordinate"] = {
            **artifact.coordinate,
            **({"protocol": artifact.protocol} if artifact.protocol else {}),
        }
    return source


def _file_fields(
    item: ListingItem,
    *,
    artifact: Artifact,
    filename: str,
    bundle_name: str,
    source: WalkSource,
    extra_tags: list[str],
) -> DatasetFields:
    """The upsert fields a walked file contributes (everything but uri, kind, origin, owner).

    Derived from the LISTING and the classifier only -- name, size, and the coordinate the name
    encodes. The walk never opens an object (viva-api#675)."""
    attributes: JsonDict = {"name": filename, "analysis_dir": bundle_name}
    attributes.update(artifact.coordinate)
    if artifact.protocol:
        attributes["protocol"] = artifact.protocol
    return {
        "view": artifact.view,
        "display_name": _display_name(source.label, artifact.view or filename, artifact),
        "size_bytes": item.Size,
        "attributes": attributes,
        "tags": [*source.tags, *extra_tags],
        "source": _source(source.subject, artifact),
    }


def _present(uri: str, bucket: str, keys: set[str]) -> bool:
    """Whether a row's object is in a listing; a directory-like uri is present while anything is under it."""
    key = uri.removeprefix(f"s3://{bucket}/")
    return any(k.startswith(key) for k in keys) if key.endswith("/") else key in keys


# ---------------------------------------------------------------------------
# walking
# ---------------------------------------------------------------------------


@dataclass
class WalkResult:
    bundles: int = 0
    unclaimed: int = 0  # bundles nobody claims, attributed to the source's owner
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


async def _rows_under(store: DatasetWriter, uri_prefix: str) -> dict[str, DatasetRecord]:
    """Every registered row whose uri starts with ``uri_prefix``, available or not, by uri."""
    return {row.uri: row for row in await store.list_under(uri_prefix)}


async def _mark_vanished(
    rows: Mapping[str, DatasetRecord],
    *,
    bucket: str,
    keys: set[str],
    under: str,
    store: DatasetWriter,
    result: WalkResult,
    skip: tuple[str, ...] = (),
) -> None:
    """Mark the rows under ``under`` whose object the listing no longer has unavailable, whoever
    registered them (availability is a fact about the object). ``skip``: prefixes already handled."""
    for uri, row in rows.items():
        if not row.available or not uri.startswith(under) or (skip and uri.startswith(skip)):
            continue
        if not _present(uri, bucket, keys):
            await store.set_available(row.id, False)
            result.unavailable += 1


async def register_bundle(
    items: list[ListingItem],
    *,
    bucket: str,
    bundle_key: str,
    owner: OwnerRef,
    source: WalkSource,
    classifier: ArtifactClassifier,
    store: DatasetWriter,
    tags: list[str] | None = None,
    existing: Mapping[str, DatasetRecord] | None = None,
) -> WalkResult:
    """Register every consumable file under one bundle directory for ``owner``, and mark rows under
    the bundle unavailable when their object is gone. ``tags`` are added to the source's on every
    row (tags only ever union-merge). ``existing``: the registered rows under the bundle by uri, read
    from the store when not given."""
    result = WalkResult(bundles=1)
    prefix = bundle_key.rstrip("/") + "/"
    bundle_name = prefix.rstrip("/").rsplit("/", 1)[-1]
    uri_prefix = f"s3://{bucket}/{prefix}"
    rows = existing if existing is not None else await _rows_under(store, uri_prefix)
    for item in items:
        relative = item.Key[len(prefix) :] if item.Key.startswith(prefix) else ""
        artifact = classifier.classify(relative) if relative else None
        if artifact is None:
            continue
        uri = f"s3://{bucket}/{item.Key}"
        fields = _file_fields(
            item,
            artifact=artifact,
            filename=relative.rsplit("/", 1)[-1],
            bundle_name=bundle_name,
            source=source,
            extra_tags=list(tags or []),
        )
        try:
            record, action = await store.upsert(
                {"uri": uri, "kind": artifact.kind, "origin": DATASET_ORIGIN_WALK, "available": True, **fields},
                owner=owner,
            )
        except ValueError as refused:
            result.skip(f"{uri}: {refused}")
            continue
        if action == "skipped" and not record.available:
            # An event-sourced row the walk may not rewrite, whose object is back.
            await store.set_available(record.id, True)
            action = "updated"
        result.registered += action in ("inserted", "updated")
        result.unchanged += action not in ("inserted", "updated")

    await _mark_vanished(
        rows, bucket=bucket, keys={item.Key for item in items}, under=uri_prefix, store=store, result=result
    )
    return result


async def _owner_for_bundle(bundle_uri: str, source: WalkSource, result: WalkResult) -> OwnerRef:
    """The record that claims a bundle, else the source's own owner. Never creates a record."""
    claimed = await source.claimant(bundle_uri)
    if claimed is not None:
        return claimed
    result.unclaimed += 1
    return source.owner


async def reconcile(
    source: WalkSource,
    *,
    classifier: ArtifactClassifier,
    store: DatasetWriter,
    file_service: FileService,
    storage_bucket: str | None,
) -> WalkResult:
    """Walk one source's root: register each bundle's files for the record that claims the bundle
    (else the source's owner), and mark rows whose objects, or whole bundles, are gone. One listing
    per source; never creates a record."""
    result = WalkResult()
    root = source.root_uri
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
    existing = await _rows_under(store, root_uri)
    handled: list[str] = []
    for name, items in sorted(groups.items()):
        bundle_key = f"{root_prefix}{name}"
        if not any(classifier.classify(item.Key[len(bundle_key) + 1 :]) for item in items):
            continue  # nothing consumable (e.g. only a driver.log): not a dataset bundle
        owner = await _owner_for_bundle(f"s3://{bucket}/{bundle_key}", source, result)
        result.merge(
            await register_bundle(
                items,
                bucket=bucket,
                bundle_key=bundle_key,
                owner=owner,
                source=source,
                classifier=classifier,
                store=store,
                existing=existing,
            )
        )
        handled.append(f"s3://{bucket}/{bundle_key}/")

    # Rows under a bundle directory that is gone altogether.
    keys = {item.Key for item in listing}
    await _mark_vanished(
        existing, bucket=bucket, keys=keys, under=root_uri, skip=tuple(handled), store=store, result=result
    )
    return result
