from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.http_validation_error import HTTPValidationError
from ...models.simulation_task import SimulationTask
from ...types import Response


def _get_kwargs(
    id: int,
) -> dict[str, Any]:
    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": f"/api/v1/simulations/{id}/tasks",
    }

    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[HTTPValidationError, list["SimulationTask"]]]:
    if response.status_code == 200:
        response_200 = []
        _response_200 = response.json()
        for response_200_item_data in _response_200:
            response_200_item = SimulationTask.from_dict(response_200_item_data)

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
) -> Response[Union[HTTPValidationError, list["SimulationTask"]]]:
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
) -> Response[Union[HTTPValidationError, list["SimulationTask"]]]:
    """The run's units of work as the backend saw them (Nextflow trace rows / chain seed jobs)

     Observability plan D4d. Nextflow: one row per ``trace.csv`` task (its
    ``job_id`` is the Batch job id); chain dispatch: one row per seed job with
    Batch's status reason; other backends: an empty list. 404 when the
    simulation or its run row does not exist.

    Args:
        id (int):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[HTTPValidationError, list['SimulationTask']]]
    """

    kwargs = _get_kwargs(
        id=id,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    id: int,
    *,
    client: Union[AuthenticatedClient, Client],
) -> Optional[Union[HTTPValidationError, list["SimulationTask"]]]:
    """The run's units of work as the backend saw them (Nextflow trace rows / chain seed jobs)

     Observability plan D4d. Nextflow: one row per ``trace.csv`` task (its
    ``job_id`` is the Batch job id); chain dispatch: one row per seed job with
    Batch's status reason; other backends: an empty list. 404 when the
    simulation or its run row does not exist.

    Args:
        id (int):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[HTTPValidationError, list['SimulationTask']]
    """

    return sync_detailed(
        id=id,
        client=client,
    ).parsed


async def asyncio_detailed(
    id: int,
    *,
    client: Union[AuthenticatedClient, Client],
) -> Response[Union[HTTPValidationError, list["SimulationTask"]]]:
    """The run's units of work as the backend saw them (Nextflow trace rows / chain seed jobs)

     Observability plan D4d. Nextflow: one row per ``trace.csv`` task (its
    ``job_id`` is the Batch job id); chain dispatch: one row per seed job with
    Batch's status reason; other backends: an empty list. 404 when the
    simulation or its run row does not exist.

    Args:
        id (int):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[HTTPValidationError, list['SimulationTask']]]
    """

    kwargs = _get_kwargs(
        id=id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    id: int,
    *,
    client: Union[AuthenticatedClient, Client],
) -> Optional[Union[HTTPValidationError, list["SimulationTask"]]]:
    """The run's units of work as the backend saw them (Nextflow trace rows / chain seed jobs)

     Observability plan D4d. Nextflow: one row per ``trace.csv`` task (its
    ``job_id`` is the Batch job id); chain dispatch: one row per seed job with
    Batch's status reason; other backends: an empty list. 404 when the
    simulation or its run row does not exist.

    Args:
        id (int):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[HTTPValidationError, list['SimulationTask']]
    """

    return (
        await asyncio_detailed(
            id=id,
            client=client,
        )
    ).parsed
