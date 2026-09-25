from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.environment_model import EnvironmentModel
from ...models.http_validation_error import HTTPValidationError
from ...models.resolve_environment_request import ResolveEnvironmentRequest
from ...types import Response


def _get_kwargs(
    *,
    body: ResolveEnvironmentRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/viva/v1/environments/resolve",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[EnvironmentModel, HTTPValidationError]]:
    if response.status_code == 200:
        response_200 = EnvironmentModel.from_dict(response.json())

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
) -> Response[Union[EnvironmentModel, HTTPValidationError]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: Union[AuthenticatedClient, Client],
    body: ResolveEnvironmentRequest,
) -> Response[Union[EnvironmentModel, HTTPValidationError]]:
    """Which environment would a run with this need be given? (select; never builds)

    Args:
        body (ResolveEnvironmentRequest): What is needed. ``explicit``: the environment named
            ``key`` (a commit, or its own marked tag),
            optionally its ``variant`` image. ``derived``: whatever satisfies ``dependencies`` -- an
            EMPTY list
            is a real request, for a composite that needs only the built-ins.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[EnvironmentModel, HTTPValidationError]]
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
    body: ResolveEnvironmentRequest,
) -> Optional[Union[EnvironmentModel, HTTPValidationError]]:
    """Which environment would a run with this need be given? (select; never builds)

    Args:
        body (ResolveEnvironmentRequest): What is needed. ``explicit``: the environment named
            ``key`` (a commit, or its own marked tag),
            optionally its ``variant`` image. ``derived``: whatever satisfies ``dependencies`` -- an
            EMPTY list
            is a real request, for a composite that needs only the built-ins.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[EnvironmentModel, HTTPValidationError]
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: Union[AuthenticatedClient, Client],
    body: ResolveEnvironmentRequest,
) -> Response[Union[EnvironmentModel, HTTPValidationError]]:
    """Which environment would a run with this need be given? (select; never builds)

    Args:
        body (ResolveEnvironmentRequest): What is needed. ``explicit``: the environment named
            ``key`` (a commit, or its own marked tag),
            optionally its ``variant`` image. ``derived``: whatever satisfies ``dependencies`` -- an
            EMPTY list
            is a real request, for a composite that needs only the built-ins.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[EnvironmentModel, HTTPValidationError]]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: Union[AuthenticatedClient, Client],
    body: ResolveEnvironmentRequest,
) -> Optional[Union[EnvironmentModel, HTTPValidationError]]:
    """Which environment would a run with this need be given? (select; never builds)

    Args:
        body (ResolveEnvironmentRequest): What is needed. ``explicit``: the environment named
            ``key`` (a commit, or its own marked tag),
            optionally its ``variant`` image. ``derived``: whatever satisfies ``dependencies`` -- an
            EMPTY list
            is a real request, for a composite that needs only the built-ins.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[EnvironmentModel, HTTPValidationError]
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
