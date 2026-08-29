from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.http_validation_error import HTTPValidationError
from ...models.task_response import TaskResponse
from ...types import Response


def _get_kwargs(
    task_id: int,
) -> dict[str, Any]:
    _kwargs: dict[str, Any] = {
        "method": "delete",
        "url": f"/env-worker/v1/tasks/{task_id}",
    }

    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[HTTPValidationError, TaskResponse]]:
    if response.status_code == 200:
        response_200 = TaskResponse.from_dict(response.json())

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
) -> Response[Union[HTTPValidationError, TaskResponse]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    task_id: int,
    *,
    client: Union[AuthenticatedClient, Client],
) -> Response[Union[HTTPValidationError, TaskResponse]]:
    """Cancel a task you started

     The one authorization rule in this API: you cannot cancel someone else's work.

    It exists to prevent an ACCIDENT — killing a colleague's six-hour study —
    not an adversary, who can set the identity header to anything. See
    viva_api/api/auth.py.

    A task nobody claimed (created_by NULL, which is every task on a deployment
    with no identity proxy) is cancellable by anyone: there is no owner to
    protect, and refusing would make the endpoint useless exactly where identity
    is unavailable.

    Anonymity is refused only HERE, and only because the alternative is a
    decorative rule: if an unidentified caller could cancel anything, omitting
    the header would bypass ownership entirely.

    Args:
        task_id (int):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[HTTPValidationError, TaskResponse]]
    """

    kwargs = _get_kwargs(
        task_id=task_id,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    task_id: int,
    *,
    client: Union[AuthenticatedClient, Client],
) -> Optional[Union[HTTPValidationError, TaskResponse]]:
    """Cancel a task you started

     The one authorization rule in this API: you cannot cancel someone else's work.

    It exists to prevent an ACCIDENT — killing a colleague's six-hour study —
    not an adversary, who can set the identity header to anything. See
    viva_api/api/auth.py.

    A task nobody claimed (created_by NULL, which is every task on a deployment
    with no identity proxy) is cancellable by anyone: there is no owner to
    protect, and refusing would make the endpoint useless exactly where identity
    is unavailable.

    Anonymity is refused only HERE, and only because the alternative is a
    decorative rule: if an unidentified caller could cancel anything, omitting
    the header would bypass ownership entirely.

    Args:
        task_id (int):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[HTTPValidationError, TaskResponse]
    """

    return sync_detailed(
        task_id=task_id,
        client=client,
    ).parsed


async def asyncio_detailed(
    task_id: int,
    *,
    client: Union[AuthenticatedClient, Client],
) -> Response[Union[HTTPValidationError, TaskResponse]]:
    """Cancel a task you started

     The one authorization rule in this API: you cannot cancel someone else's work.

    It exists to prevent an ACCIDENT — killing a colleague's six-hour study —
    not an adversary, who can set the identity header to anything. See
    viva_api/api/auth.py.

    A task nobody claimed (created_by NULL, which is every task on a deployment
    with no identity proxy) is cancellable by anyone: there is no owner to
    protect, and refusing would make the endpoint useless exactly where identity
    is unavailable.

    Anonymity is refused only HERE, and only because the alternative is a
    decorative rule: if an unidentified caller could cancel anything, omitting
    the header would bypass ownership entirely.

    Args:
        task_id (int):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[HTTPValidationError, TaskResponse]]
    """

    kwargs = _get_kwargs(
        task_id=task_id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    task_id: int,
    *,
    client: Union[AuthenticatedClient, Client],
) -> Optional[Union[HTTPValidationError, TaskResponse]]:
    """Cancel a task you started

     The one authorization rule in this API: you cannot cancel someone else's work.

    It exists to prevent an ACCIDENT — killing a colleague's six-hour study —
    not an adversary, who can set the identity header to anything. See
    viva_api/api/auth.py.

    A task nobody claimed (created_by NULL, which is every task on a deployment
    with no identity proxy) is cancellable by anyone: there is no owner to
    protect, and refusing would make the endpoint useless exactly where identity
    is unavailable.

    Anonymity is refused only HERE, and only because the alternative is a
    decorative rule: if an unidentified caller could cancel anything, omitting
    the header would bypass ownership entirely.

    Args:
        task_id (int):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[HTTPValidationError, TaskResponse]
    """

    return (
        await asyncio_detailed(
            task_id=task_id,
            client=client,
        )
    ).parsed
