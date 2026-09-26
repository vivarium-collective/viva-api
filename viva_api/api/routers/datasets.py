"""
/datasets: the files runs actually wrote (docs/plan-data-provenance.md §2a, §7).

**A dated facade (plan-core D14, removed at M4).** Since P4a-2 the datasets family is core's,
served at ``/viva/v1/datasets`` from the same table with core's record model (``owner_kind`` /
``owner_id`` instead of the three producer ids) and advertised as ``viva-v1-datasets``. This
router keeps the SMS shapes and its producer-id filters unchanged for the callers that have not
switched; ``/{id}/provenance`` stays here until core has a job record (P4b).

A dataset row is never pre-created. The scheduler's event ingester registers one when it
scrapes an ``artifact.written`` event out of a run's trace, and the reconciliation walk
registers what no event did and marks rows whose object is gone. Listings therefore lag the
runs that write them; ``GET /analyses/{id}`` reports ``status`` and ``n_datasets`` apart.
"""

import logging
from typing import Any

from fastapi import Body, Depends, HTTPException, Query, Request
from fastapi import Path as FastAPIPath
from fastapi.responses import StreamingResponse

from viva_api.analysis.models import DatasetDTO, DatasetListDTO, DatasetProvenanceDTO
from viva_api.common.gateway.utils import get_router_config
from viva_api.common.handlers import datasets as dataset_handlers
from viva_api.config import get_settings
from viva_api.dependencies import get_database_service, get_file_service
from viva_api.simulation.database_service import DatabaseService

logger = logging.getLogger(__name__)
config = get_router_config(prefix="api", version_major=False)


def _require_database_service() -> DatabaseService:
    database_service = get_database_service()
    if database_service is None:
        raise HTTPException(status_code=500, detail="Database service is not initialized")
    return database_service


@config.router.get(
    path="/datasets",
    response_model=DatasetListDTO,
    operation_id="list-datasets",
    tags=["Datasets"],
    summary="List registered datasets by kind, view, tags, attributes, producer run or source",
)
async def list_datasets(
    request: Request,
    params: dataset_handlers.DatasetListParams = Depends(dataset_handlers.dataset_list_params),
    attr: list[str] = Query(
        default=[],
        description="Attribute filter <key>=<value>, repeatable (all must match). A JSON scalar keeps its "
        'type: variant=0 matches the integer, variant="0" the string; a non-JSON value such as '
        "agent=000 is a string. attr.<key>=<value> parameters mean the same.",
    ),
    simulation_id: int | None = Query(default=None, description="Written by this simulation run."),
    analysis_id: int | None = Query(default=None, description="Written by this analysis run."),
    parca_dataset_id: int | None = Query(default=None, description="Written by this ParCa run."),
    source: str | None = Query(
        default=None, description="What the data is OF: 'sim:1002', 'analysis:7', or a JSON ProvenanceRef fragment."
    ),
) -> DatasetListDTO:
    """Newest change first; ``next_offset`` pages. Unavailable datasets (object gone) are
    excluded unless ``available=false`` or ``available=any``."""
    database_service = _require_database_service()
    try:
        return await dataset_handlers.list_page(
            database_service,
            params,
            attributes=dataset_handlers.parse_attribute_filters(attr, list(request.query_params.multi_items())),
            simulation_id=simulation_id,
            analysis_id=analysis_id,
            parca_dataset_id=parca_dataset_id,
            source=dataset_handlers.parse_source_filter(source),
        )
    except dataset_handlers.DatasetQueryError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.exception("Error listing datasets")
        raise HTTPException(status_code=500, detail=str(e)) from e


@config.router.get(
    path="/datasets/attributes",
    response_model=dict[str, list[Any]],
    operation_id="list-dataset-attributes",
    tags=["Datasets"],
    summary="Distinct values of every dataset attribute (for building pickers)",
)
async def list_dataset_attributes(
    kind: str | None = Query(default=None, description="Only datasets of this kind."),
) -> dict[str, list[Any]]:
    database_service = _require_database_service()
    try:
        dataset_handlers.check_kind(kind)
    except dataset_handlers.DatasetQueryError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return await database_service.list_dataset_attribute_values(kind=kind)


@config.router.get(
    path="/datasets/tags",
    response_model=dict[str, int],
    operation_id="list-dataset-tags",
    tags=["Datasets"],
    summary="Every tag present on datasets, with how many datasets carry it",
)
async def list_dataset_tags(
    kind: str | None = Query(default=None, description="Only datasets of this kind."),
) -> dict[str, int]:
    database_service = _require_database_service()
    try:
        dataset_handlers.check_kind(kind)
    except dataset_handlers.DatasetQueryError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return await database_service.list_dataset_tags(kind=kind)


@config.router.get(
    path="/datasets/{id}",
    response_model=DatasetDTO,
    operation_id="get-dataset",
    tags=["Datasets"],
    summary="One registered dataset",
)
async def get_dataset(id: int = FastAPIPath(description="Database ID of the dataset")) -> DatasetDTO:
    database_service = _require_database_service()
    try:
        return await dataset_handlers.require_dataset(database_service, id)
    except dataset_handlers.DatasetNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e


@config.router.get(
    path="/datasets/{id}/content",
    operation_id="get-dataset-content",
    tags=["Datasets"],
    response_model=None,
    responses={
        200: {
            "content": {"text/plain": {}, "text/html": {}, "application/json": {}, "application/octet-stream": {}},
            "description": "The dataset's bytes, streamed; the media type follows the file (TSV as text/plain)",
        },
        404: {"description": "Unknown dataset, or its object is gone"},
        409: {"description": "A store kind (parquet, parca-cache) or an object outside the API's storage bucket"},
    },
    summary="Stream one dataset's bytes (a ptools TSV, a figure, a report) through the API",
)
async def get_dataset_content(id: int = FastAPIPath(description="Database ID of the dataset")) -> StreamingResponse:
    """Serves single objects without storage credentials. Store-like kinds (a parquet prefix, a
    ParCa cache) are refused with 409: read those from the dataset's ``uri``."""
    database_service = _require_database_service()
    file_service = get_file_service()
    if file_service is None:
        raise HTTPException(status_code=500, detail="File service is not initialized")
    try:
        dataset, stream = await dataset_handlers.open_content(
            database_service, file_service, id, storage_bucket=get_settings().storage_s3_bucket or None
        )
    except dataset_handlers.DatasetNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except dataset_handlers.DatasetNotServableError as e:
        raise HTTPException(status_code=409, detail=str(e)) from e
    media_type, headers = dataset_handlers.content_headers(dataset)
    return StreamingResponse(stream, media_type=media_type, headers=headers)


@config.router.post(
    path="/datasets/{id}/tags",
    response_model=DatasetDTO,
    operation_id="add-dataset-tags",
    tags=["Datasets"],
    summary="Attach one or more free-form tags to a dataset",
)
async def add_dataset_tags(
    id: int = FastAPIPath(description="Database ID of the dataset"),
    tags: list[str] = Body(..., embed=True, description="Tags to add (union-merged with existing tags)."),
) -> DatasetDTO:
    database_service = _require_database_service()
    try:
        return await database_service.add_dataset_tags(id, tags)
    except RuntimeError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e


@config.router.get(
    path="/datasets/{id}/provenance",
    response_model=DatasetProvenanceDTO,
    operation_id="get-dataset-provenance",
    tags=["Datasets"],
    summary="What wrote a dataset: its producer run, trace and span, and the datasets that run read",
)
async def get_dataset_provenance(
    id: int = FastAPIPath(description="Database ID of the dataset"),
) -> DatasetProvenanceDTO:
    """One hop. ``producer`` is the run row (simulation, analysis or ParCa) with its ``source``
    and, when the run was traced, its ``hpcrun_id``/``trace_id``; ``span`` is the span the
    ``artifact.written`` event came from (walk-found datasets have none); ``inputs`` are
    registered datasets the producer's declared source names by ``uri``."""
    database_service = _require_database_service()
    try:
        return await dataset_handlers.provenance(database_service, id)
    except dataset_handlers.DatasetNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except Exception as e:
        logger.exception("Error resolving dataset provenance")
        raise HTTPException(status_code=500, detail=str(e)) from e
