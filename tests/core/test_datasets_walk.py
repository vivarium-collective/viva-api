"""The reconciliation walk in core: bundles under a source's root become dataset rows through the
``ArtifactClassifier``, ``WalkSource`` and ``DatasetWriter`` Protocols (plan P4a-2, slice 2).

Pure and fast: an in-memory store, a listing double for the file service, a one-rule classifier
and a scripted source. The application's own tests (``tests/simulation/test_dataset_walk.py``)
run the same walk against Postgres with its naming convention and its claimant lookup.
"""

from __future__ import annotations

import datetime
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from viva_core.datasets.models import DatasetRecord, DatasetWrite, DatasetWriter, JsonDict, OwnerRef, UpsertAction
from viva_core.datasets.walk import Artifact, ArtifactClassifier, WalkSource, reconcile, register_bundle, split_s3_uri
from viva_core.storage.file_paths import S3FilePath
from viva_core.storage.file_service import FileService, ListingItem

BUCKET = "b"
ROOT = f"s3://{BUCKET}/out/exp-1/bundles"
SOURCE_OWNER: OwnerRef = {"owner_kind": "run", "owner_id": "12"}
CLAIMANT: OwnerRef = {"owner_kind": "step", "owner_id": "7"}
SUBJECT: JsonDict = {"kind": "run", "ref": "12", "resolved_id": 12}


@dataclass
class _Row:
    id: int
    uri: str
    available: bool
    write: DatasetWrite
    owner: OwnerRef


@dataclass
class _Store:
    """A ``DatasetWriter`` keyed on uri. A ``walk`` write never rewrites an ``event`` row (``skipped``);
    an identical rewrite is ``unchanged``."""

    rows: dict[str, _Row] = field(default_factory=dict)
    availability_changes: list[tuple[int, bool]] = field(default_factory=list)

    def seed(self, uri: str, *, origin: str = "walk", available: bool = True, owner: OwnerRef = SOURCE_OWNER) -> _Row:
        write: DatasetWrite = {"uri": uri, "kind": "table", "origin": origin, "available": available, "attributes": {}}
        row = _Row(len(self.rows) + 1, uri, available, write, owner)
        self.rows[uri] = row
        return row

    async def upsert(self, write: DatasetWrite, *, owner: OwnerRef) -> tuple[DatasetRecord, UpsertAction]:
        uri = write["uri"]
        existing = self.rows.get(uri)
        if existing is None:
            row = _Row(len(self.rows) + 1, uri, write.get("available", True), write, owner)
            self.rows[uri] = row
            return row, "inserted"
        if write.get("origin") == "walk" and existing.write.get("origin") == "event":
            return existing, "skipped"
        if existing.write == write and existing.owner == owner:
            return existing, "unchanged"
        existing.write, existing.owner = write, owner
        return existing, "updated"

    async def list_under(self, uri_prefix: str) -> list[_Row]:
        return [row for uri, row in self.rows.items() if uri.startswith(uri_prefix)]

    async def set_available(self, dataset_id: int, available: bool) -> None:
        self.availability_changes.append((dataset_id, available))
        for row in self.rows.values():
            if row.id == dataset_id:
                row.available = available


class _Classifier:
    """``data/<name>__<k>=<v>.tsv`` is a table whose name encodes one coordinate axis; ``report.json``
    is a report; everything else is not a dataset."""

    def classify(self, relative: str) -> Artifact | None:
        if relative == "report.json":
            return Artifact(kind="report")
        if relative.startswith("data/") and relative.endswith(".tsv") and "/" not in relative[5:]:
            stem = relative[5:-4]
            view, _, group = stem.partition("__")
            key, _, value = group.partition("=")
            coordinate: JsonDict = {key: int(value)} if value.isdigit() else {}
            return Artifact(kind="table", view=view, protocol="single" if coordinate else None, coordinate=coordinate)
        return None


@dataclass
class _Source:
    root_uri: str = ROOT
    owner: OwnerRef = field(default_factory=lambda: SOURCE_OWNER.copy())
    label: str = "exp-1"
    tags: list[str] = field(default_factory=lambda: ["cd2"])
    subject: JsonDict | None = field(default_factory=lambda: SUBJECT.copy())
    claimed: dict[str, OwnerRef] = field(default_factory=dict)
    asked: list[str] = field(default_factory=list)

    async def claimant(self, bundle_uri: str) -> OwnerRef | None:
        self.asked.append(bundle_uri)
        return self.claimed.get(bundle_uri)


class _Listing(FileService):
    """A ``FileService`` double that only lists: ``keys`` maps key to size."""

    def __init__(self, keys: dict[str, int]) -> None:
        self.keys = keys
        self.listings: list[str] = []

    async def get_listing(self, s3_path: S3FilePath) -> list[ListingItem]:
        prefix = str(s3_path.s3_path)
        self.listings.append(prefix)
        when = datetime.datetime(2026, 9, 15, tzinfo=datetime.UTC)
        return [
            ListingItem(Key=key, LastModified=when, ETag="e", Size=size)
            for key, size in sorted(self.keys.items())
            if key.startswith(prefix)
        ]

    async def download_file(self, s3_path: S3FilePath, file_path: Path | None = None) -> tuple[S3FilePath, str]:
        raise NotImplementedError

    async def upload_file(self, file_path: Path, s3_path: S3FilePath) -> S3FilePath:
        raise NotImplementedError

    async def upload_bytes(self, file_contents: bytes, s3_path: S3FilePath) -> S3FilePath:
        raise NotImplementedError

    async def get_modified_date(self, s3_path: S3FilePath) -> datetime.datetime:
        raise NotImplementedError

    async def get_file_contents(self, s3_path: S3FilePath) -> bytes | None:
        raise NotImplementedError

    async def delete_file(self, s3_path: S3FilePath) -> None:
        raise NotImplementedError

    async def close(self) -> None:
        return None


def _item(key: str, size: int = 10) -> ListingItem:
    return ListingItem(Key=key, LastModified=datetime.datetime(2026, 9, 15, tzinfo=datetime.UTC), ETag="e", Size=size)


async def _walk(store: _Store, keys: dict[str, int], source: _Source | None = None) -> tuple[_Source, _Listing]:
    source = source or _Source()
    listing = _Listing(keys)
    result = await reconcile(source, classifier=_Classifier(), store=store, file_service=listing, storage_bucket=BUCKET)
    source.result = result  # type: ignore[attr-defined]
    return source, listing


def test_the_protocols_are_satisfied_structurally() -> None:
    store: DatasetWriter = _Store()
    classifier: ArtifactClassifier = _Classifier()
    source: WalkSource = _Source()
    assert store is not None and classifier is not None and source is not None


def test_split_s3_uri() -> None:
    assert split_s3_uri("s3://b/out/exp/bundles/") == ("b", "out/exp/bundles")
    assert split_s3_uri("s3://b") == ("b", "")


@pytest.mark.asyncio
async def test_a_walk_registers_each_bundles_datasets_from_the_listing_alone() -> None:
    store = _Store()
    keys = {
        "out/exp-1/bundles/b1/report.json": 240_000_000,
        "out/exp-1/bundles/b1/data/view__seed=3.tsv": 2048,
        "out/exp-1/bundles/b1/data/nested/too-deep.tsv": 1,
        "out/exp-1/bundles/b1/driver.log": 5,
        "out/exp-1/bundles/b2/data/other__seed=0.tsv": 99,
        "out/exp-1/bundles/scratch/driver.log": 5,  # nothing consumable: not a bundle
        "out/exp-1/bundles/loose-file.txt": 1,
        "out/exp-1/elsewhere/x.tsv": 1,  # outside the root
    }
    source, listing = await _walk(store, keys)
    result = source.result  # type: ignore[attr-defined]

    assert listing.listings == ["out/exp-1/bundles"]
    assert (result.bundles, result.unclaimed, result.registered, result.skipped, result.unavailable) == (2, 2, 3, 0, 0)
    assert sorted(store.rows) == [
        f"s3://{BUCKET}/out/exp-1/bundles/b1/data/view__seed=3.tsv",
        f"s3://{BUCKET}/out/exp-1/bundles/b1/report.json",
        f"s3://{BUCKET}/out/exp-1/bundles/b2/data/other__seed=0.tsv",
    ]
    table = store.rows[f"s3://{BUCKET}/out/exp-1/bundles/b1/data/view__seed=3.tsv"]
    assert table.owner == SOURCE_OWNER
    assert table.write["kind"] == "table" and table.write["origin"] == "walk" and table.write["available"] is True
    assert table.write["view"] == "view" and table.write["size_bytes"] == 2048
    assert table.write["attributes"] == {
        "name": "view__seed=3.tsv",
        "analysis_dir": "b1",
        "seed": 3,
        "protocol": "single",
    }
    assert table.write["tags"] == ["cd2"]
    assert table.write["source"] == {
        "kind": "run",
        "ref": "12",
        "resolved_id": 12,
        "coordinate": {"seed": 3, "protocol": "single"},
    }
    assert table.write["display_name"] == "exp-1 · view · single"
    report = store.rows[f"s3://{BUCKET}/out/exp-1/bundles/b1/report.json"]
    assert report.write["kind"] == "report" and report.write["view"] is None
    assert report.write["display_name"] == "exp-1 · report.json"
    assert report.write["source"] == {"kind": "run", "ref": "12", "resolved_id": 12}
    assert source.asked == [f"s3://{BUCKET}/out/exp-1/bundles/b1", f"s3://{BUCKET}/out/exp-1/bundles/b2"]


@pytest.mark.asyncio
async def test_a_claimed_bundle_belongs_to_its_claimant_and_moves_when_a_claim_appears() -> None:
    store = _Store()
    keys = {"out/exp-1/bundles/b1/data/view__seed=1.tsv": 1}
    uri = f"s3://{BUCKET}/out/exp-1/bundles/b1/data/view__seed=1.tsv"
    first, _ = await _walk(store, keys)
    assert first.result.unclaimed == 1 and store.rows[uri].owner == SOURCE_OWNER  # type: ignore[attr-defined]

    claimed = _Source(claimed={f"s3://{BUCKET}/out/exp-1/bundles/b1": CLAIMANT})
    second, _ = await _walk(store, keys, claimed)
    assert second.result.unclaimed == 0 and second.result.registered == 1  # type: ignore[attr-defined]
    assert store.rows[uri].owner == CLAIMANT


@pytest.mark.asyncio
async def test_walking_twice_changes_nothing_and_an_event_row_is_never_downgraded() -> None:
    store = _Store()
    event_uri = f"s3://{BUCKET}/out/exp-1/bundles/b1/data/view__seed=1.tsv"
    store.seed(event_uri, origin="event")
    keys = {"out/exp-1/bundles/b1/data/view__seed=1.tsv": 1, "out/exp-1/bundles/b1/data/new__seed=2.tsv": 1}
    first, _ = await _walk(store, keys)
    assert (first.result.registered, first.result.unchanged) == (1, 1)  # type: ignore[attr-defined]
    assert store.rows[event_uri].write["origin"] == "event"
    again, _ = await _walk(store, keys)
    assert (again.result.registered, again.result.unchanged) == (0, 2)  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_gone_objects_and_gone_bundles_are_marked_unavailable_and_a_returning_event_row_comes_back() -> None:
    store = _Store()
    gone_file = store.seed(f"s3://{BUCKET}/out/exp-1/bundles/b1/data/gone__seed=1.tsv")
    gone_bundle = store.seed(f"s3://{BUCKET}/out/exp-1/bundles/old/data/x__seed=1.tsv")
    already = store.seed(f"s3://{BUCKET}/out/exp-1/bundles/old/report.json", available=False)
    returned = store.seed(f"s3://{BUCKET}/out/exp-1/bundles/b1/data/back__seed=2.tsv", origin="event", available=False)
    keys = {"out/exp-1/bundles/b1/data/back__seed=2.tsv": 1, "out/exp-1/bundles/b1/data/kept__seed=3.tsv": 1}

    source, _ = await _walk(store, keys)
    result = source.result  # type: ignore[attr-defined]

    assert (result.registered, result.unchanged, result.unavailable) == (2, 0, 2)
    assert store.availability_changes == [
        (returned.id, True),
        (gone_file.id, False),
        (gone_bundle.id, False),
    ]
    assert already.available is False and returned.available is True


@pytest.mark.asyncio
async def test_a_root_in_another_bucket_is_skipped_without_a_listing() -> None:
    store = _Store()
    listing = _Listing({})
    result = await reconcile(
        _Source(root_uri="s3://other/out/exp-1/bundles"),
        classifier=_Classifier(),
        store=store,
        file_service=listing,
        storage_bucket=BUCKET,
    )
    assert result.skipped == 1 and "not in the file service's bucket" in result.reasons[0]
    assert listing.listings == [] and store.rows == {}


@pytest.mark.asyncio
async def test_register_bundle_adds_tags_and_counts_a_store_refusal_as_a_skip() -> None:
    class _Refusing(_Store):
        async def upsert(self, write: DatasetWrite, *, owner: OwnerRef) -> tuple[DatasetRecord, UpsertAction]:
            if write["uri"].endswith("bad__seed=9.tsv"):
                raise ValueError("refused by the store")
            return await super().upsert(write, owner=owner)

    store = _Refusing()
    items = [_item("out/exp-1/bundles/b1/data/good__seed=1.tsv"), _item("out/exp-1/bundles/b1/data/bad__seed=9.tsv")]
    result = await register_bundle(
        items,
        bucket=BUCKET,
        bundle_key="out/exp-1/bundles/b1",
        owner=CLAIMANT,
        source=_Source(),
        classifier=_Classifier(),
        store=store,
        tags=["imported"],
    )
    assert (result.bundles, result.registered, result.skipped) == (1, 1, 1)
    assert "refused by the store" in result.reasons[0]
    good = store.rows[f"s3://{BUCKET}/out/exp-1/bundles/b1/data/good__seed=1.tsv"]
    assert good.owner == CLAIMANT and good.write["tags"] == ["cd2", "imported"]
