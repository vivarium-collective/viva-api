"""``/viva/v1/datasets`` served by a standalone core from a ``DatasetStore`` handed in through the
container (plan P4a-2, slice 3), and answering 503 by name on a core that has none.

Pure and fast: an in-memory store and a byte-map file service. The application's own tests
(``tests/api/test_viva_datasets_routes.py``) serve the same routes from its table.
"""

from __future__ import annotations

import datetime
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from pathlib import Path

from fastapi.testclient import TestClient
from pydantic import JsonValue

from viva_core.api.app import CORE_PREFIX, create_core_app
from viva_core.container import CoreContainer, DatasetServices
from viva_core.datasets.models import (
    Dataset,
    DatasetQuery,
    DatasetRecord,
    DatasetStore,
    DatasetWrite,
    OwnerRef,
    UpsertAction,
)
from viva_core.settings import CoreSettings
from viva_core.storage.file_paths import S3FilePath
from viva_core.storage.file_service import FileService, ListingItem

BUCKET = "b"
KINDS = ("table", "figure", "store")


def _dataset(id: int, **overrides: object) -> Dataset:
    values: dict[str, object] = {
        "id": id,
        "uri": f"s3://{BUCKET}/out/exp-1/bundles/b1/data/view__seed={id}.tsv",
        "kind": "table",
        "owner_kind": "run",
        "owner_id": "12",
        "producer_job_id": 7,
        "trace_id": "trace-7",
        "view": "view",
        "display_name": f"exp-1 · view · s{id}",
        "attributes": {"origin": "walk", "seed": id, "protocol": "single"},
        "tags": ["cd2"],
        "source": {"kind": "run", "ref": "12"},
        "available": True,
    }
    values.update(overrides)
    return Dataset.model_validate(values)


def _matches(dataset: Dataset, query: DatasetQuery) -> bool:
    checks = [
        query.get("kind") is None or dataset.kind == query["kind"],
        query.get("view") is None or dataset.view == query["view"],
        query.get("owner_kind") is None or dataset.owner_kind == query["owner_kind"],
        query.get("owner_id") is None or dataset.owner_id == query["owner_id"],
        query.get("available") is None or dataset.available is query["available"],
        not query.get("tags") or all(tag in dataset.tags for tag in query["tags"] or []),
        not query.get("attributes")
        or all(dataset.attributes.get(k) == v for k, v in (query["attributes"] or {}).items()),
        not query.get("source") or all((dataset.source or {}).get(k) == v for k, v in (query["source"] or {}).items()),
        query.get("uri_prefix") is None or dataset.uri.startswith(query["uri_prefix"] or ""),
        query.get("q") is None or (query["q"] or "").lower() in (dataset.display_name or dataset.uri).lower(),
    ]
    return all(checks)


@dataclass
class _Store:
    """A full ``DatasetStore`` in memory."""

    rows: dict[int, Dataset] = field(default_factory=dict)

    async def upsert(self, write: DatasetWrite, *, owner: OwnerRef) -> tuple[DatasetRecord, UpsertAction]:
        raise NotImplementedError

    async def list_under(self, uri_prefix: str) -> list[Dataset]:
        raise NotImplementedError

    async def set_available(self, dataset_id: int, available: bool) -> None:
        raise NotImplementedError

    async def get(self, dataset_id: int) -> Dataset | None:
        return self.rows.get(dataset_id)

    async def page(self, query: DatasetQuery, *, limit: int, offset: int) -> list[Dataset]:
        found = [row for row in self.rows.values() if _matches(row, query)]
        return sorted(found, key=lambda r: -r.id)[offset : offset + limit]

    async def count(self, query: DatasetQuery) -> int:
        return sum(1 for row in self.rows.values() if _matches(row, query))

    async def add_tags(self, dataset_id: int, tags: list[str]) -> Dataset | None:
        row = self.rows.get(dataset_id)
        if row is None:
            return None
        row.tags = [*row.tags, *(t for t in tags if t not in row.tags)]
        return row

    async def attribute_values(self, kind: str | None) -> dict[str, list[JsonValue]]:
        values: dict[str, list[JsonValue]] = {}
        for row in self.rows.values():
            if kind is None or row.kind == kind:
                for key, value in row.attributes.items():
                    if value not in values.setdefault(key, []):
                        values[key].append(value)
        return values

    async def tag_counts(self, kind: str | None) -> dict[str, int]:
        counts: dict[str, int] = {}
        for row in self.rows.values():
            if kind is None or row.kind == kind:
                for tag in row.tags:
                    counts[tag] = counts.get(tag, 0) + 1
        return counts


class _Files(FileService):
    """A ``FileService`` double: ``objects`` maps keys to bytes; a missing key streams nothing."""

    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = objects

    async def open_file_stream(self, s3_path: S3FilePath, chunk_size: int = 65536) -> AsyncIterator[bytes] | None:
        body = self.objects.get(str(s3_path.s3_path))
        if body is None:
            return None

        async def _chunks() -> AsyncIterator[bytes]:
            yield body

        return _chunks()

    async def get_listing(self, s3_path: S3FilePath) -> list[ListingItem]:
        raise NotImplementedError

    async def get_file_contents(self, s3_path: S3FilePath) -> bytes | None:
        raise NotImplementedError

    async def download_file(self, s3_path: S3FilePath, file_path: Path | None = None) -> tuple[S3FilePath, str]:
        raise NotImplementedError

    async def upload_file(self, file_path: Path, s3_path: S3FilePath) -> S3FilePath:
        raise NotImplementedError

    async def upload_bytes(self, file_contents: bytes, s3_path: S3FilePath) -> S3FilePath:
        raise NotImplementedError

    async def get_modified_date(self, s3_path: S3FilePath) -> datetime.datetime:
        raise NotImplementedError

    async def delete_file(self, s3_path: S3FilePath) -> None:
        raise NotImplementedError

    async def close(self) -> None:
        return None


def _client(store: _Store | None, objects: dict[str, bytes] | None = None) -> TestClient:
    services = (
        None
        if store is None
        else DatasetServices(
            store=store, kinds=KINDS, files=_Files(objects or {}), storage_bucket=BUCKET, unserved_kinds=("store",)
        )
    )
    return TestClient(create_core_app(CoreContainer(settings=CoreSettings(), datasets=services)))


def _seeded() -> _Store:
    store = _Store()
    for row in (
        _dataset(1),
        _dataset(2, tags=["cd2", "run3"], attributes={"origin": "event", "seed": 2, "protocol": "single"}),
        _dataset(3, kind="figure", uri=f"s3://{BUCKET}/out/exp-1/bundles/b1/viz/fig.html", view=None),
        _dataset(4, owner_kind="step", owner_id="9", available=False),
        _dataset(5, kind="store", uri=f"s3://{BUCKET}/out/exp-1/history/variant=0/"),
        _dataset(6, uri="s3://elsewhere/out/x.tsv"),
    ):
        store.rows[row.id] = row
    return store


def test_the_store_double_is_a_dataset_store() -> None:
    store: DatasetStore = _Store()
    assert store is not None


def test_a_page_carries_its_total_and_the_filters_narrow_it() -> None:
    client = _client(_seeded())
    page = client.get(f"{CORE_PREFIX}/datasets").json()
    assert page["total"] == 5 and [d["id"] for d in page["datasets"]] == [6, 5, 3, 2, 1]  # 4 is unavailable
    assert page["datasets"][0]["owner_kind"] == "run" and page["datasets"][0]["producer_job_id"] == 7
    assert "simulation_id" not in page["datasets"][0]

    assert client.get(f"{CORE_PREFIX}/datasets", params={"available": "any"}).json()["total"] == 6
    assert client.get(f"{CORE_PREFIX}/datasets", params={"owner": "step:9", "available": "any"}).json()["total"] == 1
    assert client.get(f"{CORE_PREFIX}/datasets", params={"kind": "figure"}).json()["total"] == 1
    assert client.get(f"{CORE_PREFIX}/datasets", params={"tag": "cd2,run3"}).json()["total"] == 1
    assert client.get(f"{CORE_PREFIX}/datasets", params={"attr": "seed=2"}).json()["total"] == 1
    assert client.get(f"{CORE_PREFIX}/datasets", params={"attr.origin": "event"}).json()["total"] == 1
    assert client.get(f"{CORE_PREFIX}/datasets", params={"source": "run:12"}).json()["total"] == 5
    assert client.get(f"{CORE_PREFIX}/datasets", params={"q": "s2"}).json()["total"] == 1
    paged = client.get(f"{CORE_PREFIX}/datasets", params={"limit": 2}).json()
    assert [d["id"] for d in paged["datasets"]] == [6, 5] and paged["next_offset"] == 2 and paged["total"] == 5


def test_a_malformed_filter_is_the_callers_fault() -> None:
    client = _client(_seeded())
    assert client.get(f"{CORE_PREFIX}/datasets", params={"kind": "spreadsheet"}).status_code == 400
    assert client.get(f"{CORE_PREFIX}/datasets", params={"owner": "nine"}).status_code == 400
    assert client.get(f"{CORE_PREFIX}/datasets", params={"attr": "seed"}).status_code == 400
    assert client.get(f"{CORE_PREFIX}/datasets", params={"source": "{not json"}).status_code == 400
    assert client.get(f"{CORE_PREFIX}/datasets/attributes", params={"kind": "spreadsheet"}).status_code == 400


def test_one_dataset_its_pickers_and_its_tags() -> None:
    client = _client(_seeded())
    one = client.get(f"{CORE_PREFIX}/datasets/2").json()
    assert (one["id"], one["kind"], one["owner_kind"], one["owner_id"], one["trace_id"]) == (
        2,
        "table",
        "run",
        "12",
        "trace-7",
    )
    assert client.get(f"{CORE_PREFIX}/datasets/99").status_code == 404

    attributes = client.get(f"{CORE_PREFIX}/datasets/attributes", params={"kind": "table"}).json()
    assert attributes["protocol"] == ["single"] and sorted(attributes["origin"]) == ["event", "walk"]
    assert client.get(f"{CORE_PREFIX}/datasets/tags").json() == {"cd2": 6, "run3": 1}

    tagged = client.post(f"{CORE_PREFIX}/datasets/1/tags", json={"tags": ["cd2", "keep"]}).json()
    assert tagged["tags"] == ["cd2", "keep"]
    assert client.post(f"{CORE_PREFIX}/datasets/99/tags", json={"tags": ["x"]}).status_code == 404


def test_content_streams_an_object_and_refuses_what_it_cannot_serve() -> None:
    body = b"$\t0m\t124m\nEG10001\t0.25\t0.27\n"
    client = _client(_seeded(), {"out/exp-1/bundles/b1/data/view__seed=1.tsv": body})
    served = client.get(f"{CORE_PREFIX}/datasets/1/content")
    assert served.status_code == 200 and served.content == body
    assert served.headers["content-type"].startswith("text/plain")
    assert served.headers["content-disposition"] == 'inline; filename="view__seed=1.tsv"'
    assert client.get(f"{CORE_PREFIX}/datasets/2/content").status_code == 404  # the object is gone
    assert client.get(f"{CORE_PREFIX}/datasets/5/content").status_code == 409  # a store kind, and a prefix
    assert client.get(f"{CORE_PREFIX}/datasets/6/content").status_code == 409  # another bucket
    assert client.get(f"{CORE_PREFIX}/datasets/99/content").status_code == 404


def test_a_core_with_a_store_says_so_and_one_without_answers_by_name() -> None:
    with_store = _client(_seeded())
    assert with_store.get(f"{CORE_PREFIX}/health").json()["services"]["datasets"] is True
    assert "viva-v1-datasets" in with_store.get(f"{CORE_PREFIX}/capabilities").json()["capabilities"]

    without = _client(None)
    assert without.get(f"{CORE_PREFIX}/health").json()["services"]["datasets"] is False
    assert "viva-v1-datasets" not in without.get(f"{CORE_PREFIX}/capabilities").json()["capabilities"]
    for path in ("/datasets", "/datasets/tags", "/datasets/attributes", "/datasets/1", "/datasets/1/content"):
        answer = without.get(f"{CORE_PREFIX}{path}")
        assert answer.status_code == 503 and "no dataset store" in answer.json()["detail"], path
    assert without.post(f"{CORE_PREFIX}/datasets/1/tags", json={"tags": ["x"]}).status_code == 503

    document = with_store.get(f"{CORE_PREFIX}/openapi.json").json()
    assert (
        f"{CORE_PREFIX}/datasets" in document["paths"] and f"{CORE_PREFIX}/datasets/{{id}}/content" in document["paths"]
    )
