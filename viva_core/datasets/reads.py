"""The dataset reads behind ``/viva/v1/datasets``: a page with its total, one row, its bytes.

Everything here reads: rows are written by the trace feeder (:mod:`viva_core.datasets.registry`)
and the walk (:mod:`viva_core.datasets.walk`); the only write on the read surface is union-merging
tags, which is the store's. Moved verbatim from the application's handlers (P4a-2, slice 3), with
the store and the storage details handed in.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Container
from pathlib import Path

from viva_core.datasets.models import Dataset, DatasetPage, DatasetQuery, DatasetStore
from viva_core.datasets.walk import split_s3_uri
from viva_core.storage.file_paths import S3FilePath
from viva_core.storage.file_service import FileService

#: Media types by file suffix, for serving a dataset inline; anything else is a byte stream.
_MEDIA_TYPES: tuple[tuple[str, str], ...] = (
    (".html", "text/html; charset=utf-8"),
    (".svg", "image/svg+xml"),
    (".png", "image/png"),
    (".json", "application/json"),
    (".tsv", "text/plain; charset=utf-8"),
    (".csv", "text/plain; charset=utf-8"),
    (".txt", "text/plain; charset=utf-8"),
)


class DatasetNotFoundError(LookupError):
    """An unknown dataset, or one whose object is gone (404)."""


class DatasetNotServableError(RuntimeError):
    """A dataset whose bytes this API does not serve (409)."""


async def list_page(store: DatasetStore, query: DatasetQuery, *, limit: int, offset: int) -> DatasetPage:
    """One page of datasets, newest change first, and the total the same filters match.

    ``next_offset`` is ``None`` once a page comes back short. The count is a second query rather
    than a window function because the page and the count share one clause builder in the store,
    which is what keeps them honest."""
    datasets = await store.page(query, limit=limit, offset=offset)
    total = await store.count(query)
    next_offset = offset + limit if len(datasets) == limit else None
    return DatasetPage(datasets=datasets, limit=limit, offset=offset, next_offset=next_offset, total=total)


async def require_dataset(store: DatasetStore, dataset_id: int) -> Dataset:
    dataset = await store.get(dataset_id)
    if dataset is None:
        raise DatasetNotFoundError(f"Dataset {dataset_id} not found")
    return dataset


async def open_content(
    store: DatasetStore,
    files: FileService,
    dataset_id: int,
    *,
    storage_bucket: str | None,
    unserved_kinds: Container[str],
) -> tuple[Dataset, AsyncIterator[bytes]]:
    """A dataset's bytes, streamed from the API's storage bucket.

    ``DatasetNotServableError`` for a store kind (``unserved_kinds``: a partition prefix, a cache
    -- read those from ``uri`` with storage credentials), a non-object uri or another bucket;
    ``DatasetNotFoundError`` for an unknown id or a gone object. Whether the object exists is
    settled before the first byte, so the caller can still answer 404."""
    dataset = await require_dataset(store, dataset_id)
    if dataset.kind in unserved_kinds:
        raise DatasetNotServableError(
            f"Dataset {dataset_id} is a {dataset.kind} store and is not served through the API; read {dataset.uri}"
        )
    bucket, key = split_s3_uri(dataset.uri)
    if not dataset.uri.startswith("s3://") or not key or dataset.uri.endswith("/"):
        raise DatasetNotServableError(f"Dataset {dataset_id} does not name an S3 object: {dataset.uri}")
    if storage_bucket and bucket != storage_bucket:
        raise DatasetNotServableError(f"Dataset {dataset_id} is in bucket {bucket!r}, not this API's storage bucket")
    stream = await files.open_file_stream(S3FilePath(s3_path=Path(key)))
    if stream is None:
        raise DatasetNotFoundError(f"Dataset {dataset_id}'s object is gone: {dataset.uri}")
    return dataset, stream


def content_headers(dataset: Dataset) -> tuple[str, dict[str, str]]:
    """Media type and headers for serving a dataset inline."""
    name = dataset.uri.rstrip("/").rsplit("/", 1)[-1]
    lowered = name.lower()
    media_type = next((media for suffix, media in _MEDIA_TYPES if lowered.endswith(suffix)), "application/octet-stream")
    return media_type, {"Content-Disposition": f'inline; filename="{name}"', "X-Content-Type-Options": "nosniff"}
