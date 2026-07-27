from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.http_validation_error import HTTPValidationError
from ...types import UNSET, Response, Unset


def _get_kwargs(
    simulation_id: int,
    *,
    interval_seconds: Union[Unset, float] = 30.0,
) -> dict[str, Any]:
    params: dict[str, Any] = {}

    params["interval_seconds"] = interval_seconds

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": f"/compose/v1/simulation/{simulation_id}/progress/stream",
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[Any, HTTPValidationError]]:
    if response.status_code == 200:
        response_200 = response.json()
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
) -> Response[Union[Any, HTTPValidationError]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    simulation_id: int,
    *,
    client: Union[AuthenticatedClient, Client],
    interval_seconds: Union[Unset, float] = 30.0,
) -> Response[Union[Any, HTTPValidationError]]:
    """Server-sent stream of BatchProgress for a running compose batch

     Stream :class:`BatchProgress` as ``text/event-stream``, recomputing every
    ``interval_seconds`` (clamped to a ≥5 s floor) until the run reaches a terminal
    state. Thin wrapper over the same computation as the polling endpoint.

    Args:
        simulation_id (int):
        interval_seconds (Union[Unset, float]):  Default: 30.0.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[Any, HTTPValidationError]]
    """

    kwargs = _get_kwargs(
        simulation_id=simulation_id,
        interval_seconds=interval_seconds,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    simulation_id: int,
    *,
    client: Union[AuthenticatedClient, Client],
    interval_seconds: Union[Unset, float] = 30.0,
) -> Optional[Union[Any, HTTPValidationError]]:
    """Server-sent stream of BatchProgress for a running compose batch

     Stream :class:`BatchProgress` as ``text/event-stream``, recomputing every
    ``interval_seconds`` (clamped to a ≥5 s floor) until the run reaches a terminal
    state. Thin wrapper over the same computation as the polling endpoint.

    Args:
        simulation_id (int):
        interval_seconds (Union[Unset, float]):  Default: 30.0.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[Any, HTTPValidationError]
    """

    return sync_detailed(
        simulation_id=simulation_id,
        client=client,
        interval_seconds=interval_seconds,
    ).parsed


async def asyncio_detailed(
    simulation_id: int,
    *,
    client: Union[AuthenticatedClient, Client],
    interval_seconds: Union[Unset, float] = 30.0,
) -> Response[Union[Any, HTTPValidationError]]:
    """Server-sent stream of BatchProgress for a running compose batch

     Stream :class:`BatchProgress` as ``text/event-stream``, recomputing every
    ``interval_seconds`` (clamped to a ≥5 s floor) until the run reaches a terminal
    state. Thin wrapper over the same computation as the polling endpoint.

    Args:
        simulation_id (int):
        interval_seconds (Union[Unset, float]):  Default: 30.0.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[Any, HTTPValidationError]]
    """

    kwargs = _get_kwargs(
        simulation_id=simulation_id,
        interval_seconds=interval_seconds,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    simulation_id: int,
    *,
    client: Union[AuthenticatedClient, Client],
    interval_seconds: Union[Unset, float] = 30.0,
) -> Optional[Union[Any, HTTPValidationError]]:
    """Server-sent stream of BatchProgress for a running compose batch

     Stream :class:`BatchProgress` as ``text/event-stream``, recomputing every
    ``interval_seconds`` (clamped to a ≥5 s floor) until the run reaches a terminal
    state. Thin wrapper over the same computation as the polling endpoint.

    Args:
        simulation_id (int):
        interval_seconds (Union[Unset, float]):  Default: 30.0.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[Any, HTTPValidationError]
    """

    return (
        await asyncio_detailed(
            simulation_id=simulation_id,
            client=client,
            interval_seconds=interval_seconds,
        )
    ).parsed
