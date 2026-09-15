import datetime
from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.dataset_list_dto import DatasetListDTO
from ...models.http_validation_error import HTTPValidationError
from ...models.list_datasets_available import ListDatasetsAvailable
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    attr: Union[Unset, list[str]] = UNSET,
    simulation_id: Union[None, Unset, int] = UNSET,
    analysis_id: Union[None, Unset, int] = UNSET,
    parca_dataset_id: Union[None, Unset, int] = UNSET,
    source: Union[None, Unset, str] = UNSET,
    kind: Union[None, Unset, str] = UNSET,
    view: Union[None, Unset, str] = UNSET,
    tag: Union[None, Unset, str] = UNSET,
    available: Union[Unset, ListDatasetsAvailable] = ListDatasetsAvailable.TRUE,
    since: Union[None, Unset, datetime.datetime] = UNSET,
    limit: Union[Unset, int] = 100,
    offset: Union[Unset, int] = 0,
) -> dict[str, Any]:
    params: dict[str, Any] = {}

    json_attr: Union[Unset, list[str]] = UNSET
    if not isinstance(attr, Unset):
        json_attr = attr

    params["attr"] = json_attr

    json_simulation_id: Union[None, Unset, int]
    if isinstance(simulation_id, Unset):
        json_simulation_id = UNSET
    else:
        json_simulation_id = simulation_id
    params["simulation_id"] = json_simulation_id

    json_analysis_id: Union[None, Unset, int]
    if isinstance(analysis_id, Unset):
        json_analysis_id = UNSET
    else:
        json_analysis_id = analysis_id
    params["analysis_id"] = json_analysis_id

    json_parca_dataset_id: Union[None, Unset, int]
    if isinstance(parca_dataset_id, Unset):
        json_parca_dataset_id = UNSET
    else:
        json_parca_dataset_id = parca_dataset_id
    params["parca_dataset_id"] = json_parca_dataset_id

    json_source: Union[None, Unset, str]
    if isinstance(source, Unset):
        json_source = UNSET
    else:
        json_source = source
    params["source"] = json_source

    json_kind: Union[None, Unset, str]
    if isinstance(kind, Unset):
        json_kind = UNSET
    else:
        json_kind = kind
    params["kind"] = json_kind

    json_view: Union[None, Unset, str]
    if isinstance(view, Unset):
        json_view = UNSET
    else:
        json_view = view
    params["view"] = json_view

    json_tag: Union[None, Unset, str]
    if isinstance(tag, Unset):
        json_tag = UNSET
    else:
        json_tag = tag
    params["tag"] = json_tag

    json_available: Union[Unset, str] = UNSET
    if not isinstance(available, Unset):
        json_available = available.value

    params["available"] = json_available

    json_since: Union[None, Unset, str]
    if isinstance(since, Unset):
        json_since = UNSET
    elif isinstance(since, datetime.datetime):
        json_since = since.isoformat()
    else:
        json_since = since
    params["since"] = json_since

    params["limit"] = limit

    params["offset"] = offset

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/api/v1/datasets",
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[DatasetListDTO, HTTPValidationError]]:
    if response.status_code == 200:
        response_200 = DatasetListDTO.from_dict(response.json())

        return response_200
    if response.status_code == 422:
        response_422 = HTTPValidationError.from_dict(response.json())

        return response_422
    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Response[Union[DatasetListDTO, HTTPValidationError]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: Union[AuthenticatedClient, Client],
    attr: Union[Unset, list[str]] = UNSET,
    simulation_id: Union[None, Unset, int] = UNSET,
    analysis_id: Union[None, Unset, int] = UNSET,
    parca_dataset_id: Union[None, Unset, int] = UNSET,
    source: Union[None, Unset, str] = UNSET,
    kind: Union[None, Unset, str] = UNSET,
    view: Union[None, Unset, str] = UNSET,
    tag: Union[None, Unset, str] = UNSET,
    available: Union[Unset, ListDatasetsAvailable] = ListDatasetsAvailable.TRUE,
    since: Union[None, Unset, datetime.datetime] = UNSET,
    limit: Union[Unset, int] = 100,
    offset: Union[Unset, int] = 0,
) -> Response[Union[DatasetListDTO, HTTPValidationError]]:
    """List registered datasets by kind, view, tags, attributes, producer run or source

     Newest change first; ``next_offset`` pages. Unavailable datasets (object gone) are
    excluded unless ``available=false`` or ``available=any``.

    Args:
        attr (Union[Unset, list[str]]): Attribute filter <key>=<value>, repeatable (all must
            match). A JSON scalar keeps its type: variant=0 matches the integer, variant="0" the
            string; a non-JSON value such as agent=000 is a string. attr.<key>=<value> parameters mean
            the same.
        simulation_id (Union[None, Unset, int]): Written by this simulation run.
        analysis_id (Union[None, Unset, int]): Written by this analysis run.
        parca_dataset_id (Union[None, Unset, int]): Written by this ParCa run.
        source (Union[None, Unset, str]): What the data is OF: 'sim:1002', 'analysis:7', or a JSON
            ProvenanceRef fragment.
        kind (Union[None, Unset, str]): Dataset kind: parquet, parca-cache, ptools-analysis,
            analysis, figure, report, other.
        view (Union[None, Unset, str]): Logical view, e.g. 'ptools_rna'.
        tag (Union[None, Unset, str]): Comma-separated tags; a dataset must carry all of them.
        available (Union[Unset, ListDatasetsAvailable]): 'true' (default): datasets whose object
            exists; 'false': those whose object is gone; 'any'. Default: ListDatasetsAvailable.TRUE.
        since (Union[None, Unset, datetime.datetime]): Only datasets changed at or after this
            time.
        limit (Union[Unset, int]): Page size. Default: 100.
        offset (Union[Unset, int]): Rows to skip (pagination, with limit). Default: 0.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[DatasetListDTO, HTTPValidationError]]
    """

    kwargs = _get_kwargs(
        attr=attr,
        simulation_id=simulation_id,
        analysis_id=analysis_id,
        parca_dataset_id=parca_dataset_id,
        source=source,
        kind=kind,
        view=view,
        tag=tag,
        available=available,
        since=since,
        limit=limit,
        offset=offset,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: Union[AuthenticatedClient, Client],
    attr: Union[Unset, list[str]] = UNSET,
    simulation_id: Union[None, Unset, int] = UNSET,
    analysis_id: Union[None, Unset, int] = UNSET,
    parca_dataset_id: Union[None, Unset, int] = UNSET,
    source: Union[None, Unset, str] = UNSET,
    kind: Union[None, Unset, str] = UNSET,
    view: Union[None, Unset, str] = UNSET,
    tag: Union[None, Unset, str] = UNSET,
    available: Union[Unset, ListDatasetsAvailable] = ListDatasetsAvailable.TRUE,
    since: Union[None, Unset, datetime.datetime] = UNSET,
    limit: Union[Unset, int] = 100,
    offset: Union[Unset, int] = 0,
) -> Optional[Union[DatasetListDTO, HTTPValidationError]]:
    """List registered datasets by kind, view, tags, attributes, producer run or source

     Newest change first; ``next_offset`` pages. Unavailable datasets (object gone) are
    excluded unless ``available=false`` or ``available=any``.

    Args:
        attr (Union[Unset, list[str]]): Attribute filter <key>=<value>, repeatable (all must
            match). A JSON scalar keeps its type: variant=0 matches the integer, variant="0" the
            string; a non-JSON value such as agent=000 is a string. attr.<key>=<value> parameters mean
            the same.
        simulation_id (Union[None, Unset, int]): Written by this simulation run.
        analysis_id (Union[None, Unset, int]): Written by this analysis run.
        parca_dataset_id (Union[None, Unset, int]): Written by this ParCa run.
        source (Union[None, Unset, str]): What the data is OF: 'sim:1002', 'analysis:7', or a JSON
            ProvenanceRef fragment.
        kind (Union[None, Unset, str]): Dataset kind: parquet, parca-cache, ptools-analysis,
            analysis, figure, report, other.
        view (Union[None, Unset, str]): Logical view, e.g. 'ptools_rna'.
        tag (Union[None, Unset, str]): Comma-separated tags; a dataset must carry all of them.
        available (Union[Unset, ListDatasetsAvailable]): 'true' (default): datasets whose object
            exists; 'false': those whose object is gone; 'any'. Default: ListDatasetsAvailable.TRUE.
        since (Union[None, Unset, datetime.datetime]): Only datasets changed at or after this
            time.
        limit (Union[Unset, int]): Page size. Default: 100.
        offset (Union[Unset, int]): Rows to skip (pagination, with limit). Default: 0.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[DatasetListDTO, HTTPValidationError]
    """

    return sync_detailed(
        client=client,
        attr=attr,
        simulation_id=simulation_id,
        analysis_id=analysis_id,
        parca_dataset_id=parca_dataset_id,
        source=source,
        kind=kind,
        view=view,
        tag=tag,
        available=available,
        since=since,
        limit=limit,
        offset=offset,
    ).parsed


async def asyncio_detailed(
    *,
    client: Union[AuthenticatedClient, Client],
    attr: Union[Unset, list[str]] = UNSET,
    simulation_id: Union[None, Unset, int] = UNSET,
    analysis_id: Union[None, Unset, int] = UNSET,
    parca_dataset_id: Union[None, Unset, int] = UNSET,
    source: Union[None, Unset, str] = UNSET,
    kind: Union[None, Unset, str] = UNSET,
    view: Union[None, Unset, str] = UNSET,
    tag: Union[None, Unset, str] = UNSET,
    available: Union[Unset, ListDatasetsAvailable] = ListDatasetsAvailable.TRUE,
    since: Union[None, Unset, datetime.datetime] = UNSET,
    limit: Union[Unset, int] = 100,
    offset: Union[Unset, int] = 0,
) -> Response[Union[DatasetListDTO, HTTPValidationError]]:
    """List registered datasets by kind, view, tags, attributes, producer run or source

     Newest change first; ``next_offset`` pages. Unavailable datasets (object gone) are
    excluded unless ``available=false`` or ``available=any``.

    Args:
        attr (Union[Unset, list[str]]): Attribute filter <key>=<value>, repeatable (all must
            match). A JSON scalar keeps its type: variant=0 matches the integer, variant="0" the
            string; a non-JSON value such as agent=000 is a string. attr.<key>=<value> parameters mean
            the same.
        simulation_id (Union[None, Unset, int]): Written by this simulation run.
        analysis_id (Union[None, Unset, int]): Written by this analysis run.
        parca_dataset_id (Union[None, Unset, int]): Written by this ParCa run.
        source (Union[None, Unset, str]): What the data is OF: 'sim:1002', 'analysis:7', or a JSON
            ProvenanceRef fragment.
        kind (Union[None, Unset, str]): Dataset kind: parquet, parca-cache, ptools-analysis,
            analysis, figure, report, other.
        view (Union[None, Unset, str]): Logical view, e.g. 'ptools_rna'.
        tag (Union[None, Unset, str]): Comma-separated tags; a dataset must carry all of them.
        available (Union[Unset, ListDatasetsAvailable]): 'true' (default): datasets whose object
            exists; 'false': those whose object is gone; 'any'. Default: ListDatasetsAvailable.TRUE.
        since (Union[None, Unset, datetime.datetime]): Only datasets changed at or after this
            time.
        limit (Union[Unset, int]): Page size. Default: 100.
        offset (Union[Unset, int]): Rows to skip (pagination, with limit). Default: 0.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[DatasetListDTO, HTTPValidationError]]
    """

    kwargs = _get_kwargs(
        attr=attr,
        simulation_id=simulation_id,
        analysis_id=analysis_id,
        parca_dataset_id=parca_dataset_id,
        source=source,
        kind=kind,
        view=view,
        tag=tag,
        available=available,
        since=since,
        limit=limit,
        offset=offset,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: Union[AuthenticatedClient, Client],
    attr: Union[Unset, list[str]] = UNSET,
    simulation_id: Union[None, Unset, int] = UNSET,
    analysis_id: Union[None, Unset, int] = UNSET,
    parca_dataset_id: Union[None, Unset, int] = UNSET,
    source: Union[None, Unset, str] = UNSET,
    kind: Union[None, Unset, str] = UNSET,
    view: Union[None, Unset, str] = UNSET,
    tag: Union[None, Unset, str] = UNSET,
    available: Union[Unset, ListDatasetsAvailable] = ListDatasetsAvailable.TRUE,
    since: Union[None, Unset, datetime.datetime] = UNSET,
    limit: Union[Unset, int] = 100,
    offset: Union[Unset, int] = 0,
) -> Optional[Union[DatasetListDTO, HTTPValidationError]]:
    """List registered datasets by kind, view, tags, attributes, producer run or source

     Newest change first; ``next_offset`` pages. Unavailable datasets (object gone) are
    excluded unless ``available=false`` or ``available=any``.

    Args:
        attr (Union[Unset, list[str]]): Attribute filter <key>=<value>, repeatable (all must
            match). A JSON scalar keeps its type: variant=0 matches the integer, variant="0" the
            string; a non-JSON value such as agent=000 is a string. attr.<key>=<value> parameters mean
            the same.
        simulation_id (Union[None, Unset, int]): Written by this simulation run.
        analysis_id (Union[None, Unset, int]): Written by this analysis run.
        parca_dataset_id (Union[None, Unset, int]): Written by this ParCa run.
        source (Union[None, Unset, str]): What the data is OF: 'sim:1002', 'analysis:7', or a JSON
            ProvenanceRef fragment.
        kind (Union[None, Unset, str]): Dataset kind: parquet, parca-cache, ptools-analysis,
            analysis, figure, report, other.
        view (Union[None, Unset, str]): Logical view, e.g. 'ptools_rna'.
        tag (Union[None, Unset, str]): Comma-separated tags; a dataset must carry all of them.
        available (Union[Unset, ListDatasetsAvailable]): 'true' (default): datasets whose object
            exists; 'false': those whose object is gone; 'any'. Default: ListDatasetsAvailable.TRUE.
        since (Union[None, Unset, datetime.datetime]): Only datasets changed at or after this
            time.
        limit (Union[Unset, int]): Page size. Default: 100.
        offset (Union[Unset, int]): Rows to skip (pagination, with limit). Default: 0.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[DatasetListDTO, HTTPValidationError]
    """

    return (
        await asyncio_detailed(
            client=client,
            attr=attr,
            simulation_id=simulation_id,
            analysis_id=analysis_id,
            parca_dataset_id=parca_dataset_id,
            source=source,
            kind=kind,
            view=view,
            tag=tag,
            available=available,
            since=since,
            limit=limit,
            offset=offset,
        )
    ).parsed
