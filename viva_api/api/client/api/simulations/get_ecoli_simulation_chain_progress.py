from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.chain_progress import ChainProgress
from ...models.http_validation_error import HTTPValidationError
from ...types import Response


def _get_kwargs(
    id: int,
) -> dict[str, Any]:
    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": f"/api/v1/simulations/{id}/chain-progress",
    }

    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[ChainProgress, HTTPValidationError]]:
    if response.status_code == 200:
        response_200 = ChainProgress.from_dict(response.json())

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
) -> Response[Union[ChainProgress, HTTPValidationError]]:
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
) -> Response[Union[ChainProgress, HTTPValidationError]]:
    """Get real per-seed aggregate progress for a chain-dispatch campaign

     Backlog item 6: real seed-level progress (succeeded/failed/in-progress
    counts) for a chain-dispatch campaign (backlog item 33) — the SAME data
    ``/simulations/{id}/status`` already computes internally and collapses to
    one coarse phase, exposed at its real granularity. 404 when the
    simulation/HpcRun doesn't exist; 409 when it exists but isn't a
    chain-dispatch campaign (a plain single-shot run has nothing to
    aggregate — callers should use ``/status`` for those instead).

    Args:
        id (int):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ChainProgress, HTTPValidationError]]
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
) -> Optional[Union[ChainProgress, HTTPValidationError]]:
    """Get real per-seed aggregate progress for a chain-dispatch campaign

     Backlog item 6: real seed-level progress (succeeded/failed/in-progress
    counts) for a chain-dispatch campaign (backlog item 33) — the SAME data
    ``/simulations/{id}/status`` already computes internally and collapses to
    one coarse phase, exposed at its real granularity. 404 when the
    simulation/HpcRun doesn't exist; 409 when it exists but isn't a
    chain-dispatch campaign (a plain single-shot run has nothing to
    aggregate — callers should use ``/status`` for those instead).

    Args:
        id (int):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ChainProgress, HTTPValidationError]
    """

    return sync_detailed(
        id=id,
        client=client,
    ).parsed


async def asyncio_detailed(
    id: int,
    *,
    client: Union[AuthenticatedClient, Client],
) -> Response[Union[ChainProgress, HTTPValidationError]]:
    """Get real per-seed aggregate progress for a chain-dispatch campaign

     Backlog item 6: real seed-level progress (succeeded/failed/in-progress
    counts) for a chain-dispatch campaign (backlog item 33) — the SAME data
    ``/simulations/{id}/status`` already computes internally and collapses to
    one coarse phase, exposed at its real granularity. 404 when the
    simulation/HpcRun doesn't exist; 409 when it exists but isn't a
    chain-dispatch campaign (a plain single-shot run has nothing to
    aggregate — callers should use ``/status`` for those instead).

    Args:
        id (int):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ChainProgress, HTTPValidationError]]
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
) -> Optional[Union[ChainProgress, HTTPValidationError]]:
    """Get real per-seed aggregate progress for a chain-dispatch campaign

     Backlog item 6: real seed-level progress (succeeded/failed/in-progress
    counts) for a chain-dispatch campaign (backlog item 33) — the SAME data
    ``/simulations/{id}/status`` already computes internally and collapses to
    one coarse phase, exposed at its real granularity. 404 when the
    simulation/HpcRun doesn't exist; 409 when it exists but isn't a
    chain-dispatch campaign (a plain single-shot run has nothing to
    aggregate — callers should use ``/status`` for those instead).

    Args:
        id (int):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ChainProgress, HTTPValidationError]
    """

    return (
        await asyncio_detailed(
            id=id,
            client=client,
        )
    ).parsed
