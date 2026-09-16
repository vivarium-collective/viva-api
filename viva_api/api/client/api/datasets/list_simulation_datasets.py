import datetime
from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.dataset_list_dto import DatasetListDTO
from ...models.http_validation_error import HTTPValidationError
from ...models.list_simulation_datasets_available import ListSimulationDatasetsAvailable
from ...types import UNSET, Response, Unset


def _get_kwargs(
    id: int,
    *,
    include_analyses: Union[Unset, bool] = False,
    kind: Union[None, Unset, str] = UNSET,
    view: Union[None, Unset, str] = UNSET,
    tag: Union[None, Unset, str] = UNSET,
    available: Union[Unset, ListSimulationDatasetsAvailable] = ListSimulationDatasetsAvailable.TRUE,
    since: Union[None, Unset, datetime.datetime] = UNSET,
    uri_prefix: Union[None, Unset, str] = UNSET,
    limit: Union[Unset, int] = 100,
    offset: Union[Unset, int] = 0,
) -> dict[str, Any]:
    params: dict[str, Any] = {}

    params["include_analyses"] = include_analyses

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

    json_uri_prefix: Union[None, Unset, str]
    if isinstance(uri_prefix, Unset):
        json_uri_prefix = UNSET
    else:
        json_uri_prefix = uri_prefix
    params["uri_prefix"] = json_uri_prefix

    params["limit"] = limit

    params["offset"] = offset

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": f"/api/v1/simulations/{id}/datasets",
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
    id: int,
    *,
    client: Union[AuthenticatedClient, Client],
    include_analyses: Union[Unset, bool] = False,
    kind: Union[None, Unset, str] = UNSET,
    view: Union[None, Unset, str] = UNSET,
    tag: Union[None, Unset, str] = UNSET,
    available: Union[Unset, ListSimulationDatasetsAvailable] = ListSimulationDatasetsAvailable.TRUE,
    since: Union[None, Unset, datetime.datetime] = UNSET,
    uri_prefix: Union[None, Unset, str] = UNSET,
    limit: Union[Unset, int] = 100,
    offset: Union[Unset, int] = 0,
) -> Response[Union[DatasetListDTO, HTTPValidationError]]:
    """Datasets attributed to a simulation (optionally with those of its analyses)

     docs/plan-data-provenance.md §7. The simulation is the producer of what its run wrote and of
    bundles the S3 walk found under its output that no analysis run claims (``origin = walk``).
    Rows lag ingestion. 404 for an unknown simulation.

    Args:
        id (int): Database ID of the simulation
        include_analyses (Union[Unset, bool]): Also datasets attributed to analyses OF this
            simulation (matched on the dataset's source). Default: False.
        kind (Union[None, Unset, str]): Dataset kind: parquet, parca-cache, ptools-analysis,
            analysis, figure, report, other.
        view (Union[None, Unset, str]): Logical view, e.g. 'ptools_rna'.
        tag (Union[None, Unset, str]): Comma-separated tags; a dataset must carry all of them.
        available (Union[Unset, ListSimulationDatasetsAvailable]): 'true' (default): datasets
            whose object exists; 'false': those whose object is gone; 'any'. Default:
            ListSimulationDatasetsAvailable.TRUE.
        since (Union[None, Unset, datetime.datetime]): Only datasets changed at or after this
            time.
        uri_prefix (Union[None, Unset, str]): Datasets whose uri starts with this, e.g. a bundle
            directory 's3://bucket/vecoli-output/<experiment>/analyses/<bundle>/'. Wildcards are
            literal.
        limit (Union[Unset, int]): Page size. Default: 100.
        offset (Union[Unset, int]): Rows to skip (pagination, with limit). Default: 0.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[DatasetListDTO, HTTPValidationError]]
    """

    kwargs = _get_kwargs(
        id=id,
        include_analyses=include_analyses,
        kind=kind,
        view=view,
        tag=tag,
        available=available,
        since=since,
        uri_prefix=uri_prefix,
        limit=limit,
        offset=offset,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    id: int,
    *,
    client: Union[AuthenticatedClient, Client],
    include_analyses: Union[Unset, bool] = False,
    kind: Union[None, Unset, str] = UNSET,
    view: Union[None, Unset, str] = UNSET,
    tag: Union[None, Unset, str] = UNSET,
    available: Union[Unset, ListSimulationDatasetsAvailable] = ListSimulationDatasetsAvailable.TRUE,
    since: Union[None, Unset, datetime.datetime] = UNSET,
    uri_prefix: Union[None, Unset, str] = UNSET,
    limit: Union[Unset, int] = 100,
    offset: Union[Unset, int] = 0,
) -> Optional[Union[DatasetListDTO, HTTPValidationError]]:
    """Datasets attributed to a simulation (optionally with those of its analyses)

     docs/plan-data-provenance.md §7. The simulation is the producer of what its run wrote and of
    bundles the S3 walk found under its output that no analysis run claims (``origin = walk``).
    Rows lag ingestion. 404 for an unknown simulation.

    Args:
        id (int): Database ID of the simulation
        include_analyses (Union[Unset, bool]): Also datasets attributed to analyses OF this
            simulation (matched on the dataset's source). Default: False.
        kind (Union[None, Unset, str]): Dataset kind: parquet, parca-cache, ptools-analysis,
            analysis, figure, report, other.
        view (Union[None, Unset, str]): Logical view, e.g. 'ptools_rna'.
        tag (Union[None, Unset, str]): Comma-separated tags; a dataset must carry all of them.
        available (Union[Unset, ListSimulationDatasetsAvailable]): 'true' (default): datasets
            whose object exists; 'false': those whose object is gone; 'any'. Default:
            ListSimulationDatasetsAvailable.TRUE.
        since (Union[None, Unset, datetime.datetime]): Only datasets changed at or after this
            time.
        uri_prefix (Union[None, Unset, str]): Datasets whose uri starts with this, e.g. a bundle
            directory 's3://bucket/vecoli-output/<experiment>/analyses/<bundle>/'. Wildcards are
            literal.
        limit (Union[Unset, int]): Page size. Default: 100.
        offset (Union[Unset, int]): Rows to skip (pagination, with limit). Default: 0.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[DatasetListDTO, HTTPValidationError]
    """

    return sync_detailed(
        id=id,
        client=client,
        include_analyses=include_analyses,
        kind=kind,
        view=view,
        tag=tag,
        available=available,
        since=since,
        uri_prefix=uri_prefix,
        limit=limit,
        offset=offset,
    ).parsed


async def asyncio_detailed(
    id: int,
    *,
    client: Union[AuthenticatedClient, Client],
    include_analyses: Union[Unset, bool] = False,
    kind: Union[None, Unset, str] = UNSET,
    view: Union[None, Unset, str] = UNSET,
    tag: Union[None, Unset, str] = UNSET,
    available: Union[Unset, ListSimulationDatasetsAvailable] = ListSimulationDatasetsAvailable.TRUE,
    since: Union[None, Unset, datetime.datetime] = UNSET,
    uri_prefix: Union[None, Unset, str] = UNSET,
    limit: Union[Unset, int] = 100,
    offset: Union[Unset, int] = 0,
) -> Response[Union[DatasetListDTO, HTTPValidationError]]:
    """Datasets attributed to a simulation (optionally with those of its analyses)

     docs/plan-data-provenance.md §7. The simulation is the producer of what its run wrote and of
    bundles the S3 walk found under its output that no analysis run claims (``origin = walk``).
    Rows lag ingestion. 404 for an unknown simulation.

    Args:
        id (int): Database ID of the simulation
        include_analyses (Union[Unset, bool]): Also datasets attributed to analyses OF this
            simulation (matched on the dataset's source). Default: False.
        kind (Union[None, Unset, str]): Dataset kind: parquet, parca-cache, ptools-analysis,
            analysis, figure, report, other.
        view (Union[None, Unset, str]): Logical view, e.g. 'ptools_rna'.
        tag (Union[None, Unset, str]): Comma-separated tags; a dataset must carry all of them.
        available (Union[Unset, ListSimulationDatasetsAvailable]): 'true' (default): datasets
            whose object exists; 'false': those whose object is gone; 'any'. Default:
            ListSimulationDatasetsAvailable.TRUE.
        since (Union[None, Unset, datetime.datetime]): Only datasets changed at or after this
            time.
        uri_prefix (Union[None, Unset, str]): Datasets whose uri starts with this, e.g. a bundle
            directory 's3://bucket/vecoli-output/<experiment>/analyses/<bundle>/'. Wildcards are
            literal.
        limit (Union[Unset, int]): Page size. Default: 100.
        offset (Union[Unset, int]): Rows to skip (pagination, with limit). Default: 0.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[DatasetListDTO, HTTPValidationError]]
    """

    kwargs = _get_kwargs(
        id=id,
        include_analyses=include_analyses,
        kind=kind,
        view=view,
        tag=tag,
        available=available,
        since=since,
        uri_prefix=uri_prefix,
        limit=limit,
        offset=offset,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    id: int,
    *,
    client: Union[AuthenticatedClient, Client],
    include_analyses: Union[Unset, bool] = False,
    kind: Union[None, Unset, str] = UNSET,
    view: Union[None, Unset, str] = UNSET,
    tag: Union[None, Unset, str] = UNSET,
    available: Union[Unset, ListSimulationDatasetsAvailable] = ListSimulationDatasetsAvailable.TRUE,
    since: Union[None, Unset, datetime.datetime] = UNSET,
    uri_prefix: Union[None, Unset, str] = UNSET,
    limit: Union[Unset, int] = 100,
    offset: Union[Unset, int] = 0,
) -> Optional[Union[DatasetListDTO, HTTPValidationError]]:
    """Datasets attributed to a simulation (optionally with those of its analyses)

     docs/plan-data-provenance.md §7. The simulation is the producer of what its run wrote and of
    bundles the S3 walk found under its output that no analysis run claims (``origin = walk``).
    Rows lag ingestion. 404 for an unknown simulation.

    Args:
        id (int): Database ID of the simulation
        include_analyses (Union[Unset, bool]): Also datasets attributed to analyses OF this
            simulation (matched on the dataset's source). Default: False.
        kind (Union[None, Unset, str]): Dataset kind: parquet, parca-cache, ptools-analysis,
            analysis, figure, report, other.
        view (Union[None, Unset, str]): Logical view, e.g. 'ptools_rna'.
        tag (Union[None, Unset, str]): Comma-separated tags; a dataset must carry all of them.
        available (Union[Unset, ListSimulationDatasetsAvailable]): 'true' (default): datasets
            whose object exists; 'false': those whose object is gone; 'any'. Default:
            ListSimulationDatasetsAvailable.TRUE.
        since (Union[None, Unset, datetime.datetime]): Only datasets changed at or after this
            time.
        uri_prefix (Union[None, Unset, str]): Datasets whose uri starts with this, e.g. a bundle
            directory 's3://bucket/vecoli-output/<experiment>/analyses/<bundle>/'. Wildcards are
            literal.
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
            id=id,
            client=client,
            include_analyses=include_analyses,
            kind=kind,
            view=view,
            tag=tag,
            available=available,
            since=since,
            uri_prefix=uri_prefix,
            limit=limit,
            offset=offset,
        )
    ).parsed
