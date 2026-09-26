"""``/viva/v1/datasets``: the files runs actually wrote, as core serves them (plan P4a-2, slice 3).

A dataset row is never pre-created: the trace feeder registers one from an ``artifact.written``
event, the walk registers what no event did and marks rows whose object is gone. Listings therefore
lag the runs that write them. Rows are read through the container's :class:`DatasetServices`; a
deployment with none answers 503 by name on every route here. Provenance (one hop back to the run)
is the application's until core has a job record (P4b).
"""

from __future__ import annotations

import datetime
import logging
from enum import Enum

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request
from fastapi import Path as FastAPIPath
from fastapi.responses import StreamingResponse
from pydantic import JsonValue

from viva_core.container import DatasetServices, current_container
from viva_core.datasets.models import DATASET_LIST_MAX_LIMIT, Dataset, DatasetPage, DatasetQuery
from viva_core.datasets.queries import (
    AVAILABILITY,
    Availability,
    DatasetListParams,
    DatasetQueryError,
    check_kind,
    naive_utc,
    parse_attribute_filters,
    parse_owner,
    parse_source_filter,
    parse_tags,
)
from viva_core.datasets.reads import (
    DatasetNotFoundError,
    DatasetNotServableError,
    content_headers,
    list_page,
    open_content,
    require_dataset,
)

logger = logging.getLogger(__name__)
router = APIRouter()

TAGS: list[str | Enum] = ["Viva Core", "Datasets"]


def _services() -> DatasetServices:
    services = current_container().datasets
    if services is None:
        raise HTTPException(status_code=503, detail="this deployment provides no dataset store")
    return services


def dataset_list_params(
    kind: str | None = Query(default=None, description="Dataset kind (the deployment's vocabulary)."),
    view: str | None = Query(default=None, description="Logical view the file is one instance of."),
    tag: str | None = Query(default=None, description="Comma-separated tags; a dataset must carry all of them."),
    available: Availability = Query(
        default="true",
        description="'true' (default): datasets whose object exists; 'false': those whose object is gone; 'any'.",
    ),
    since: datetime.datetime | None = Query(default=None, description="Only datasets changed at or after this time."),
    uri_prefix: str | None = Query(
        default=None, description="Datasets whose uri starts with this (a bundle directory). Wildcards are literal."
    ),
    q: str | None = Query(
        default=None, description="Free text: a case-insensitive substring of the display name or the uri."
    ),
    limit: int = Query(default=100, ge=1, le=DATASET_LIST_MAX_LIMIT, description="Page size."),
    offset: int = Query(default=0, ge=0, description="Rows to skip (pagination, with limit)."),
) -> DatasetListParams:
    """FastAPI dependency: the query parameters every dataset listing takes."""
    return DatasetListParams(
        kind=kind,
        view=view,
        tags=parse_tags(tag),
        available=AVAILABILITY[available],
        since=naive_utc(since),
        uri_prefix=uri_prefix,
        q=(q or "").strip() or None,
        limit=limit,
        offset=offset,
    )


@router.get(
    "",
    response_model=DatasetPage,
    operation_id="viva-list-datasets",
    tags=TAGS,
    summary="List registered datasets by kind, view, tags, attributes, owner or source",
)
async def list_datasets(
    request: Request,
    params: DatasetListParams = Depends(dataset_list_params),
    attr: list[str] = Query(
        default=[],
        description="Attribute filter <key>=<value>, repeatable (all must match). A JSON scalar keeps its "
        'type: variant=0 matches the integer, variant="0" the string; a non-JSON value such as '
        "agent=000 is a string. attr.<key>=<value> parameters mean the same.",
    ),
    owner: str | None = Query(
        default=None, description="Whose it is: '<owner_kind>:<owner_id>', the owning record's kind and id."
    ),
    source: str | None = Query(
        default=None, description="What the data is OF: '<kind>:<ref>', or a JSON fragment of the stored source."
    ),
) -> DatasetPage:
    """Newest change first; ``next_offset`` pages. Unavailable datasets (object gone) are
    excluded unless ``available=false`` or ``available=any``."""
    services = _services()
    try:
        check_kind(params.kind, services.kinds)
        query: DatasetQuery = {
            **params.query(),
            "attributes": parse_attribute_filters(attr, list(request.query_params.multi_items())),
            "source": parse_source_filter(source),
        }
        who = parse_owner(owner)
        if who is not None:
            query["owner_kind"], query["owner_id"] = who["owner_kind"], who["owner_id"]
    except DatasetQueryError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return await list_page(services.store, query, limit=params.limit, offset=params.offset)


@router.get(
    "/attributes",
    response_model=dict[str, list[JsonValue]],
    operation_id="viva-list-dataset-attributes",
    tags=TAGS,
    summary="Distinct values of every dataset attribute (for building pickers)",
)
async def list_dataset_attributes(
    kind: str | None = Query(default=None, description="Only datasets of this kind."),
) -> dict[str, list[JsonValue]]:
    services = _services()
    try:
        check_kind(kind, services.kinds)
    except DatasetQueryError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return await services.store.attribute_values(kind)


@router.get(
    "/tags",
    response_model=dict[str, int],
    operation_id="viva-list-dataset-tags",
    tags=TAGS,
    summary="Every tag present on datasets, with how many datasets carry it",
)
async def list_dataset_tags(
    kind: str | None = Query(default=None, description="Only datasets of this kind."),
) -> dict[str, int]:
    services = _services()
    try:
        check_kind(kind, services.kinds)
    except DatasetQueryError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return await services.store.tag_counts(kind)


@router.get(
    "/{id}",
    response_model=Dataset,
    operation_id="viva-get-dataset",
    tags=TAGS,
    summary="One registered dataset",
)
async def get_dataset(id: int = FastAPIPath(description="Id of the dataset")) -> Dataset:
    try:
        return await require_dataset(_services().store, id)
    except DatasetNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e


@router.get(
    "/{id}/content",
    operation_id="viva-get-dataset-content",
    tags=TAGS,
    response_model=None,
    responses={
        200: {
            "content": {"text/plain": {}, "text/html": {}, "application/json": {}, "application/octet-stream": {}},
            "description": "The dataset's bytes, streamed; the media type follows the file",
        },
        404: {"description": "Unknown dataset, or its object is gone"},
        409: {"description": "A store kind, a prefix, or an object outside the API's storage bucket"},
    },
    summary="Stream one dataset's bytes through the API",
)
async def get_dataset_content(id: int = FastAPIPath(description="Id of the dataset")) -> StreamingResponse:
    """Serves single objects without storage credentials. Store-like kinds are refused with 409:
    read those from the dataset's ``uri``."""
    services = _services()
    if services.files is None:
        raise HTTPException(status_code=503, detail="this deployment provides no file service")
    try:
        dataset, stream = await open_content(
            services.store,
            services.files,
            id,
            storage_bucket=services.storage_bucket,
            unserved_kinds=services.unserved_kinds,
        )
    except DatasetNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except DatasetNotServableError as e:
        raise HTTPException(status_code=409, detail=str(e)) from e
    media_type, headers = content_headers(dataset)
    return StreamingResponse(stream, media_type=media_type, headers=headers)


@router.post(
    "/{id}/tags",
    response_model=Dataset,
    operation_id="viva-add-dataset-tags",
    tags=TAGS,
    summary="Attach one or more free-form tags to a dataset",
)
async def add_dataset_tags(
    id: int = FastAPIPath(description="Id of the dataset"),
    tags: list[str] = Body(..., embed=True, description="Tags to add (union-merged with existing tags)."),
) -> Dataset:
    dataset = await _services().store.add_tags(id, tags)
    if dataset is None:
        raise HTTPException(status_code=404, detail=f"Dataset {id} not found")
    return dataset
