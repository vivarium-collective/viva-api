from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.http_validation_error import HTTPValidationError
from ...models.simulation_observable_index import SimulationObservableIndex
from ...types import UNSET, Response, Unset


def _get_kwargs(
    id: int,
    *,
    seed: Union[Unset, int] = 0,
) -> dict[str, Any]:
    params: dict[str, Any] = {}

    params["seed"] = seed

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": f"/api/v1/simulations/{id}/observables/index",
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[HTTPValidationError, SimulationObservableIndex]]:
    if response.status_code == 200:
        response_200 = SimulationObservableIndex.from_dict(response.json())

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
) -> Response[Union[HTTPValidationError, SimulationObservableIndex]]:
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
    seed: Union[Unset, int] = 0,
) -> Response[Union[HTTPValidationError, SimulationObservableIndex]]:
    """List observables available in a simulation's emitter store (S3)

    Args:
        id (int): Database ID of the simulation
        seed (Union[Unset, int]):  Default: 0.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[HTTPValidationError, SimulationObservableIndex]]
    """

    kwargs = _get_kwargs(
        id=id,
        seed=seed,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    id: int,
    *,
    client: Union[AuthenticatedClient, Client],
    seed: Union[Unset, int] = 0,
) -> Optional[Union[HTTPValidationError, SimulationObservableIndex]]:
    """List observables available in a simulation's emitter store (S3)

    Args:
        id (int): Database ID of the simulation
        seed (Union[Unset, int]):  Default: 0.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[HTTPValidationError, SimulationObservableIndex]
    """

    return sync_detailed(
        id=id,
        client=client,
        seed=seed,
    ).parsed


async def asyncio_detailed(
    id: int,
    *,
    client: Union[AuthenticatedClient, Client],
    seed: Union[Unset, int] = 0,
) -> Response[Union[HTTPValidationError, SimulationObservableIndex]]:
    """List observables available in a simulation's emitter store (S3)

    Args:
        id (int): Database ID of the simulation
        seed (Union[Unset, int]):  Default: 0.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[HTTPValidationError, SimulationObservableIndex]]
    """

    kwargs = _get_kwargs(
        id=id,
        seed=seed,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    id: int,
    *,
    client: Union[AuthenticatedClient, Client],
    seed: Union[Unset, int] = 0,
) -> Optional[Union[HTTPValidationError, SimulationObservableIndex]]:
    """List observables available in a simulation's emitter store (S3)

    Args:
        id (int): Database ID of the simulation
        seed (Union[Unset, int]):  Default: 0.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[HTTPValidationError, SimulationObservableIndex]
    """

    return (
        await asyncio_detailed(
            id=id,
            client=client,
            seed=seed,
        )
    ).parsed
