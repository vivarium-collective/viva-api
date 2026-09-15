import datetime
from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.experiment_analysis_dto import ExperimentAnalysisDTO
from ...models.http_validation_error import HTTPValidationError
from ...models.job_status import JobStatus
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    experiment_id: Union[None, Unset, str] = UNSET,
    simulation_id: Union[None, Unset, int] = UNSET,
    status: Union[JobStatus, None, Unset] = UNSET,
    backend: Union[None, Unset, str] = UNSET,
    source: Union[None, Unset, str] = UNSET,
    tag: Union[None, Unset, str] = UNSET,
    since: Union[None, Unset, datetime.datetime] = UNSET,
    limit: Union[None, Unset, int] = UNSET,
    offset: Union[Unset, int] = 0,
) -> dict[str, Any]:
    params: dict[str, Any] = {}

    json_experiment_id: Union[None, Unset, str]
    if isinstance(experiment_id, Unset):
        json_experiment_id = UNSET
    else:
        json_experiment_id = experiment_id
    params["experiment_id"] = json_experiment_id

    json_simulation_id: Union[None, Unset, int]
    if isinstance(simulation_id, Unset):
        json_simulation_id = UNSET
    else:
        json_simulation_id = simulation_id
    params["simulation_id"] = json_simulation_id

    json_status: Union[None, Unset, str]
    if isinstance(status, Unset):
        json_status = UNSET
    elif isinstance(status, JobStatus):
        json_status = status.value
    else:
        json_status = status
    params["status"] = json_status

    json_backend: Union[None, Unset, str]
    if isinstance(backend, Unset):
        json_backend = UNSET
    else:
        json_backend = backend
    params["backend"] = json_backend

    json_source: Union[None, Unset, str]
    if isinstance(source, Unset):
        json_source = UNSET
    else:
        json_source = source
    params["source"] = json_source

    json_tag: Union[None, Unset, str]
    if isinstance(tag, Unset):
        json_tag = UNSET
    else:
        json_tag = tag
    params["tag"] = json_tag

    json_since: Union[None, Unset, str]
    if isinstance(since, Unset):
        json_since = UNSET
    elif isinstance(since, datetime.datetime):
        json_since = since.isoformat()
    else:
        json_since = since
    params["since"] = json_since

    json_limit: Union[None, Unset, int]
    if isinstance(limit, Unset):
        json_limit = UNSET
    else:
        json_limit = limit
    params["limit"] = json_limit

    params["offset"] = offset

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/api/v1/analyses",
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[HTTPValidationError, list["ExperimentAnalysisDTO"]]]:
    if response.status_code == 200:
        response_200 = []
        _response_200 = response.json()
        for response_200_item_data in _response_200:
            response_200_item = ExperimentAnalysisDTO.from_dict(response_200_item_data)

            response_200.append(response_200_item)

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
) -> Response[Union[HTTPValidationError, list["ExperimentAnalysisDTO"]]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: Union[AuthenticatedClient, Client],
    experiment_id: Union[None, Unset, str] = UNSET,
    simulation_id: Union[None, Unset, int] = UNSET,
    status: Union[JobStatus, None, Unset] = UNSET,
    backend: Union[None, Unset, str] = UNSET,
    source: Union[None, Unset, str] = UNSET,
    tag: Union[None, Unset, str] = UNSET,
    since: Union[None, Unset, datetime.datetime] = UNSET,
    limit: Union[None, Unset, int] = UNSET,
    offset: Union[Unset, int] = 0,
) -> Response[Union[HTTPValidationError, list["ExperimentAnalysisDTO"]]]:
    """List analyses across all simulations, newest change first, with filters and paging

    Args:
        experiment_id (Union[None, Unset, str]): Optional: filter by experiment_id.
        simulation_id (Union[None, Unset, int]): Optional: filter by simulation database id.
        status (Union[JobStatus, None, Unset]): Filter by reported status: 'completed' (ready),
            'failed' (or 'cancelled'); any other value means still computing.
        backend (Union[None, Unset, str]): Filter by backend, e.g. 'ray', 'k8s', 'batch'.
        source (Union[None, Unset, str]): What the analysis is OF: 'sim:1002', or a JSON
            ProvenanceRef fragment.
        tag (Union[None, Unset, str]): Comma-separated tags; an analysis must carry all of them.
        since (Union[None, Unset, datetime.datetime]): Only analyses changed at or after this
            time.
        limit (Union[None, Unset, int]): Page size; omit to return every match.
        offset (Union[Unset, int]): Rows to skip (pagination, with limit). Default: 0.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[HTTPValidationError, list['ExperimentAnalysisDTO']]]
    """

    kwargs = _get_kwargs(
        experiment_id=experiment_id,
        simulation_id=simulation_id,
        status=status,
        backend=backend,
        source=source,
        tag=tag,
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
    experiment_id: Union[None, Unset, str] = UNSET,
    simulation_id: Union[None, Unset, int] = UNSET,
    status: Union[JobStatus, None, Unset] = UNSET,
    backend: Union[None, Unset, str] = UNSET,
    source: Union[None, Unset, str] = UNSET,
    tag: Union[None, Unset, str] = UNSET,
    since: Union[None, Unset, datetime.datetime] = UNSET,
    limit: Union[None, Unset, int] = UNSET,
    offset: Union[Unset, int] = 0,
) -> Optional[Union[HTTPValidationError, list["ExperimentAnalysisDTO"]]]:
    """List analyses across all simulations, newest change first, with filters and paging

    Args:
        experiment_id (Union[None, Unset, str]): Optional: filter by experiment_id.
        simulation_id (Union[None, Unset, int]): Optional: filter by simulation database id.
        status (Union[JobStatus, None, Unset]): Filter by reported status: 'completed' (ready),
            'failed' (or 'cancelled'); any other value means still computing.
        backend (Union[None, Unset, str]): Filter by backend, e.g. 'ray', 'k8s', 'batch'.
        source (Union[None, Unset, str]): What the analysis is OF: 'sim:1002', or a JSON
            ProvenanceRef fragment.
        tag (Union[None, Unset, str]): Comma-separated tags; an analysis must carry all of them.
        since (Union[None, Unset, datetime.datetime]): Only analyses changed at or after this
            time.
        limit (Union[None, Unset, int]): Page size; omit to return every match.
        offset (Union[Unset, int]): Rows to skip (pagination, with limit). Default: 0.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[HTTPValidationError, list['ExperimentAnalysisDTO']]
    """

    return sync_detailed(
        client=client,
        experiment_id=experiment_id,
        simulation_id=simulation_id,
        status=status,
        backend=backend,
        source=source,
        tag=tag,
        since=since,
        limit=limit,
        offset=offset,
    ).parsed


async def asyncio_detailed(
    *,
    client: Union[AuthenticatedClient, Client],
    experiment_id: Union[None, Unset, str] = UNSET,
    simulation_id: Union[None, Unset, int] = UNSET,
    status: Union[JobStatus, None, Unset] = UNSET,
    backend: Union[None, Unset, str] = UNSET,
    source: Union[None, Unset, str] = UNSET,
    tag: Union[None, Unset, str] = UNSET,
    since: Union[None, Unset, datetime.datetime] = UNSET,
    limit: Union[None, Unset, int] = UNSET,
    offset: Union[Unset, int] = 0,
) -> Response[Union[HTTPValidationError, list["ExperimentAnalysisDTO"]]]:
    """List analyses across all simulations, newest change first, with filters and paging

    Args:
        experiment_id (Union[None, Unset, str]): Optional: filter by experiment_id.
        simulation_id (Union[None, Unset, int]): Optional: filter by simulation database id.
        status (Union[JobStatus, None, Unset]): Filter by reported status: 'completed' (ready),
            'failed' (or 'cancelled'); any other value means still computing.
        backend (Union[None, Unset, str]): Filter by backend, e.g. 'ray', 'k8s', 'batch'.
        source (Union[None, Unset, str]): What the analysis is OF: 'sim:1002', or a JSON
            ProvenanceRef fragment.
        tag (Union[None, Unset, str]): Comma-separated tags; an analysis must carry all of them.
        since (Union[None, Unset, datetime.datetime]): Only analyses changed at or after this
            time.
        limit (Union[None, Unset, int]): Page size; omit to return every match.
        offset (Union[Unset, int]): Rows to skip (pagination, with limit). Default: 0.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[HTTPValidationError, list['ExperimentAnalysisDTO']]]
    """

    kwargs = _get_kwargs(
        experiment_id=experiment_id,
        simulation_id=simulation_id,
        status=status,
        backend=backend,
        source=source,
        tag=tag,
        since=since,
        limit=limit,
        offset=offset,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: Union[AuthenticatedClient, Client],
    experiment_id: Union[None, Unset, str] = UNSET,
    simulation_id: Union[None, Unset, int] = UNSET,
    status: Union[JobStatus, None, Unset] = UNSET,
    backend: Union[None, Unset, str] = UNSET,
    source: Union[None, Unset, str] = UNSET,
    tag: Union[None, Unset, str] = UNSET,
    since: Union[None, Unset, datetime.datetime] = UNSET,
    limit: Union[None, Unset, int] = UNSET,
    offset: Union[Unset, int] = 0,
) -> Optional[Union[HTTPValidationError, list["ExperimentAnalysisDTO"]]]:
    """List analyses across all simulations, newest change first, with filters and paging

    Args:
        experiment_id (Union[None, Unset, str]): Optional: filter by experiment_id.
        simulation_id (Union[None, Unset, int]): Optional: filter by simulation database id.
        status (Union[JobStatus, None, Unset]): Filter by reported status: 'completed' (ready),
            'failed' (or 'cancelled'); any other value means still computing.
        backend (Union[None, Unset, str]): Filter by backend, e.g. 'ray', 'k8s', 'batch'.
        source (Union[None, Unset, str]): What the analysis is OF: 'sim:1002', or a JSON
            ProvenanceRef fragment.
        tag (Union[None, Unset, str]): Comma-separated tags; an analysis must carry all of them.
        since (Union[None, Unset, datetime.datetime]): Only analyses changed at or after this
            time.
        limit (Union[None, Unset, int]): Page size; omit to return every match.
        offset (Union[Unset, int]): Rows to skip (pagination, with limit). Default: 0.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[HTTPValidationError, list['ExperimentAnalysisDTO']]
    """

    return (
        await asyncio_detailed(
            client=client,
            experiment_id=experiment_id,
            simulation_id=simulation_id,
            status=status,
            backend=backend,
            source=source,
            tag=tag,
            since=since,
            limit=limit,
            offset=offset,
        )
    ).parsed
