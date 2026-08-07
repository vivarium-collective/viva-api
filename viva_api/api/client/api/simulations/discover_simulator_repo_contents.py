from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.http_validation_error import HTTPValidationError
from ...models.repo_discovery import RepoDiscovery
from ...types import UNSET, Response


def _get_kwargs(
    *,
    simulator_id: int,
) -> dict[str, Any]:
    params: dict[str, Any] = {}

    params["simulator_id"] = simulator_id

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/api/v1/simulations/discovery",
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[HTTPValidationError, RepoDiscovery]]:
    if response.status_code == 200:
        response_200 = RepoDiscovery.from_dict(response.json())

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
) -> Response[Union[HTTPValidationError, RepoDiscovery]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: Union[AuthenticatedClient, Client],
    simulator_id: int,
) -> Response[Union[HTTPValidationError, RepoDiscovery]]:
    """Discover available config files and analysis modules for a simulator

     Enumerate config filenames and analysis modules available in the simulator's repo.

    Args:
        simulator_id (int): database_id of the simulator to introspect

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[HTTPValidationError, RepoDiscovery]]
    """

    kwargs = _get_kwargs(
        simulator_id=simulator_id,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: Union[AuthenticatedClient, Client],
    simulator_id: int,
) -> Optional[Union[HTTPValidationError, RepoDiscovery]]:
    """Discover available config files and analysis modules for a simulator

     Enumerate config filenames and analysis modules available in the simulator's repo.

    Args:
        simulator_id (int): database_id of the simulator to introspect

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[HTTPValidationError, RepoDiscovery]
    """

    return sync_detailed(
        client=client,
        simulator_id=simulator_id,
    ).parsed


async def asyncio_detailed(
    *,
    client: Union[AuthenticatedClient, Client],
    simulator_id: int,
) -> Response[Union[HTTPValidationError, RepoDiscovery]]:
    """Discover available config files and analysis modules for a simulator

     Enumerate config filenames and analysis modules available in the simulator's repo.

    Args:
        simulator_id (int): database_id of the simulator to introspect

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[HTTPValidationError, RepoDiscovery]]
    """

    kwargs = _get_kwargs(
        simulator_id=simulator_id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: Union[AuthenticatedClient, Client],
    simulator_id: int,
) -> Optional[Union[HTTPValidationError, RepoDiscovery]]:
    """Discover available config files and analysis modules for a simulator

     Enumerate config filenames and analysis modules available in the simulator's repo.

    Args:
        simulator_id (int): database_id of the simulator to introspect

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[HTTPValidationError, RepoDiscovery]
    """

    return (
        await asyncio_detailed(
            client=client,
            simulator_id=simulator_id,
        )
    ).parsed
