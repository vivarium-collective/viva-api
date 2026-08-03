from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.http_validation_error import HTTPValidationError
from ...models.simulation_observables import SimulationObservables
from ...types import UNSET, Response, Unset


def _get_kwargs(
    id: int,
    *,
    names: Union[Unset, str] = "",
    seed: Union[Unset, int] = 0,
    stride: Union[Unset, int] = 1,
    max_points: Union[None, Unset, int] = UNSET,
) -> dict[str, Any]:
    params: dict[str, Any] = {}

    params["names"] = names

    params["seed"] = seed

    params["stride"] = stride

    json_max_points: Union[None, Unset, int]
    if isinstance(max_points, Unset):
        json_max_points = UNSET
    else:
        json_max_points = max_points
    params["max_points"] = json_max_points

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": f"/api/v1/simulations/{id}/observables",
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[HTTPValidationError, SimulationObservables]]:
    if response.status_code == 200:
        response_200 = SimulationObservables.from_dict(response.json())

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
) -> Response[Union[HTTPValidationError, SimulationObservables]]:
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
    names: Union[Unset, str] = "",
    seed: Union[Unset, int] = 0,
    stride: Union[Unset, int] = 1,
    max_points: Union[None, Unset, int] = UNSET,
) -> Response[Union[HTTPValidationError, SimulationObservables]]:
    """Read observable timeseries from a simulation's emitter store (S3)

    Args:
        id (int): Database ID of the simulation
        names (Union[Unset, str]):  Default: ''.
        seed (Union[Unset, int]):  Default: 0.
        stride (Union[Unset, int]): Return every Nth point (decimation). 1 = full resolution.
            Default: 1.
        max_points (Union[None, Unset, int]): Cap the number of points returned; overrides
            `stride` if it implies a coarser step.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[HTTPValidationError, SimulationObservables]]
    """

    kwargs = _get_kwargs(
        id=id,
        names=names,
        seed=seed,
        stride=stride,
        max_points=max_points,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    id: int,
    *,
    client: Union[AuthenticatedClient, Client],
    names: Union[Unset, str] = "",
    seed: Union[Unset, int] = 0,
    stride: Union[Unset, int] = 1,
    max_points: Union[None, Unset, int] = UNSET,
) -> Optional[Union[HTTPValidationError, SimulationObservables]]:
    """Read observable timeseries from a simulation's emitter store (S3)

    Args:
        id (int): Database ID of the simulation
        names (Union[Unset, str]):  Default: ''.
        seed (Union[Unset, int]):  Default: 0.
        stride (Union[Unset, int]): Return every Nth point (decimation). 1 = full resolution.
            Default: 1.
        max_points (Union[None, Unset, int]): Cap the number of points returned; overrides
            `stride` if it implies a coarser step.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[HTTPValidationError, SimulationObservables]
    """

    return sync_detailed(
        id=id,
        client=client,
        names=names,
        seed=seed,
        stride=stride,
        max_points=max_points,
    ).parsed


async def asyncio_detailed(
    id: int,
    *,
    client: Union[AuthenticatedClient, Client],
    names: Union[Unset, str] = "",
    seed: Union[Unset, int] = 0,
    stride: Union[Unset, int] = 1,
    max_points: Union[None, Unset, int] = UNSET,
) -> Response[Union[HTTPValidationError, SimulationObservables]]:
    """Read observable timeseries from a simulation's emitter store (S3)

    Args:
        id (int): Database ID of the simulation
        names (Union[Unset, str]):  Default: ''.
        seed (Union[Unset, int]):  Default: 0.
        stride (Union[Unset, int]): Return every Nth point (decimation). 1 = full resolution.
            Default: 1.
        max_points (Union[None, Unset, int]): Cap the number of points returned; overrides
            `stride` if it implies a coarser step.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[HTTPValidationError, SimulationObservables]]
    """

    kwargs = _get_kwargs(
        id=id,
        names=names,
        seed=seed,
        stride=stride,
        max_points=max_points,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    id: int,
    *,
    client: Union[AuthenticatedClient, Client],
    names: Union[Unset, str] = "",
    seed: Union[Unset, int] = 0,
    stride: Union[Unset, int] = 1,
    max_points: Union[None, Unset, int] = UNSET,
) -> Optional[Union[HTTPValidationError, SimulationObservables]]:
    """Read observable timeseries from a simulation's emitter store (S3)

    Args:
        id (int): Database ID of the simulation
        names (Union[Unset, str]):  Default: ''.
        seed (Union[Unset, int]):  Default: 0.
        stride (Union[Unset, int]): Return every Nth point (decimation). 1 = full resolution.
            Default: 1.
        max_points (Union[None, Unset, int]): Cap the number of points returned; overrides
            `stride` if it implies a coarser step.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[HTTPValidationError, SimulationObservables]
    """

    return (
        await asyncio_detailed(
            id=id,
            client=client,
            names=names,
            seed=seed,
            stride=stride,
            max_points=max_points,
        )
    ).parsed
