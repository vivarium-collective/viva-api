from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.http_validation_error import HTTPValidationError
from ...models.task_status_response import TaskStatusResponse
from ...types import UNSET, Response


def _get_kwargs(
    *,
    ids: list[int],
) -> dict[str, Any]:
    params: dict[str, Any] = {}

    json_ids = ids

    params["ids"] = json_ids

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/env-worker/v1/tasks/status/batch",
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[HTTPValidationError, list["TaskStatusResponse"]]]:
    if response.status_code == 200:
        response_200 = []
        _response_200 = response.json()
        for response_200_item_data in _response_200:
            response_200_item = TaskStatusResponse.from_dict(response_200_item_data)

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
) -> Response[Union[HTTPValidationError, list["TaskStatusResponse"]]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: Union[AuthenticatedClient, Client],
    ids: list[int],
) -> Response[Union[HTTPValidationError, list["TaskStatusResponse"]]]:
    """Status for many env-worker tasks in one call (no result payloads)

     Status only — results are deliberately omitted. See TaskStatusResponse.

    Mirrors compose's /simulations/status/batch, which returns rows carrying no
    large payload. A campaign is many tasks, and polling them one at a time is
    what this endpoint exists to avoid; shipping every result inline reintroduced
    the cost in a different dimension.

    Args:
        ids (list[int]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[HTTPValidationError, list['TaskStatusResponse']]]
    """

    kwargs = _get_kwargs(
        ids=ids,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: Union[AuthenticatedClient, Client],
    ids: list[int],
) -> Optional[Union[HTTPValidationError, list["TaskStatusResponse"]]]:
    """Status for many env-worker tasks in one call (no result payloads)

     Status only — results are deliberately omitted. See TaskStatusResponse.

    Mirrors compose's /simulations/status/batch, which returns rows carrying no
    large payload. A campaign is many tasks, and polling them one at a time is
    what this endpoint exists to avoid; shipping every result inline reintroduced
    the cost in a different dimension.

    Args:
        ids (list[int]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[HTTPValidationError, list['TaskStatusResponse']]
    """

    return sync_detailed(
        client=client,
        ids=ids,
    ).parsed


async def asyncio_detailed(
    *,
    client: Union[AuthenticatedClient, Client],
    ids: list[int],
) -> Response[Union[HTTPValidationError, list["TaskStatusResponse"]]]:
    """Status for many env-worker tasks in one call (no result payloads)

     Status only — results are deliberately omitted. See TaskStatusResponse.

    Mirrors compose's /simulations/status/batch, which returns rows carrying no
    large payload. A campaign is many tasks, and polling them one at a time is
    what this endpoint exists to avoid; shipping every result inline reintroduced
    the cost in a different dimension.

    Args:
        ids (list[int]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[HTTPValidationError, list['TaskStatusResponse']]]
    """

    kwargs = _get_kwargs(
        ids=ids,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: Union[AuthenticatedClient, Client],
    ids: list[int],
) -> Optional[Union[HTTPValidationError, list["TaskStatusResponse"]]]:
    """Status for many env-worker tasks in one call (no result payloads)

     Status only — results are deliberately omitted. See TaskStatusResponse.

    Mirrors compose's /simulations/status/batch, which returns rows carrying no
    large payload. A campaign is many tasks, and polling them one at a time is
    what this endpoint exists to avoid; shipping every result inline reintroduced
    the cost in a different dimension.

    Args:
        ids (list[int]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[HTTPValidationError, list['TaskStatusResponse']]
    """

    return (
        await asyncio_detailed(
            client=client,
            ids=ids,
        )
    ).parsed
