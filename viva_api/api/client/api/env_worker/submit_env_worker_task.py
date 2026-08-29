from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.http_validation_error import HTTPValidationError
from ...models.task_response import TaskResponse
from ...models.task_submit_request import TaskSubmitRequest
from ...types import Response


def _get_kwargs(
    *,
    body: TaskSubmitRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/env-worker/v1/tasks",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[HTTPValidationError, TaskResponse]]:
    if response.status_code == 202:
        response_202 = TaskResponse.from_dict(response.json())

        return response_202
    if response.status_code == 422:
        response_422 = HTTPValidationError.from_dict(response.json())

        return response_422
    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Response[Union[HTTPValidationError, TaskResponse]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: Union[AuthenticatedClient, Client],
    body: TaskSubmitRequest,
) -> Response[Union[HTTPValidationError, TaskResponse]]:
    """Submit a long-running env-worker call; poll for its result

     202 with a task id. The row is written BEFORE this returns.

    That ordering is compose's contract and it matters more here: the client is
    told to poll and holds nothing else, so a status read that 404s because the
    row had not been written yet would be indistinguishable from a lost task.

    Args:
        body (TaskSubmitRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[HTTPValidationError, TaskResponse]]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: Union[AuthenticatedClient, Client],
    body: TaskSubmitRequest,
) -> Optional[Union[HTTPValidationError, TaskResponse]]:
    """Submit a long-running env-worker call; poll for its result

     202 with a task id. The row is written BEFORE this returns.

    That ordering is compose's contract and it matters more here: the client is
    told to poll and holds nothing else, so a status read that 404s because the
    row had not been written yet would be indistinguishable from a lost task.

    Args:
        body (TaskSubmitRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[HTTPValidationError, TaskResponse]
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: Union[AuthenticatedClient, Client],
    body: TaskSubmitRequest,
) -> Response[Union[HTTPValidationError, TaskResponse]]:
    """Submit a long-running env-worker call; poll for its result

     202 with a task id. The row is written BEFORE this returns.

    That ordering is compose's contract and it matters more here: the client is
    told to poll and holds nothing else, so a status read that 404s because the
    row had not been written yet would be indistinguishable from a lost task.

    Args:
        body (TaskSubmitRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[HTTPValidationError, TaskResponse]]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: Union[AuthenticatedClient, Client],
    body: TaskSubmitRequest,
) -> Optional[Union[HTTPValidationError, TaskResponse]]:
    """Submit a long-running env-worker call; poll for its result

     202 with a task id. The row is written BEFORE this returns.

    That ordering is compose's contract and it matters more here: the client is
    told to poll and holds nothing else, so a status read that 404s because the
    row had not been written yet would be indistinguishable from a lost task.

    Args:
        body (TaskSubmitRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[HTTPValidationError, TaskResponse]
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
