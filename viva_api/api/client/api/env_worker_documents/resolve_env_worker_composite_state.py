from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.composite_ref import CompositeRef
from ...models.http_validation_error import HTTPValidationError
from ...types import Response


def _get_kwargs(
    job_name: str,
    *,
    body: CompositeRef,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": f"/env-worker/v1/relay/workers/{job_name}/composite-state",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
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
    job_name: str,
    *,
    client: Union[AuthenticatedClient, Client],
    body: CompositeRef,
) -> Response[Union[Any, HTTPValidationError]]:
    """Build a registered generator's composite state

    Args:
        job_name (str):
        body (CompositeRef): A composite named by a registered generator, optionally with
            overrides.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[Any, HTTPValidationError]]
    """

    kwargs = _get_kwargs(
        job_name=job_name,
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    job_name: str,
    *,
    client: Union[AuthenticatedClient, Client],
    body: CompositeRef,
) -> Optional[Union[Any, HTTPValidationError]]:
    """Build a registered generator's composite state

    Args:
        job_name (str):
        body (CompositeRef): A composite named by a registered generator, optionally with
            overrides.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[Any, HTTPValidationError]
    """

    return sync_detailed(
        job_name=job_name,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    job_name: str,
    *,
    client: Union[AuthenticatedClient, Client],
    body: CompositeRef,
) -> Response[Union[Any, HTTPValidationError]]:
    """Build a registered generator's composite state

    Args:
        job_name (str):
        body (CompositeRef): A composite named by a registered generator, optionally with
            overrides.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[Any, HTTPValidationError]]
    """

    kwargs = _get_kwargs(
        job_name=job_name,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    job_name: str,
    *,
    client: Union[AuthenticatedClient, Client],
    body: CompositeRef,
) -> Optional[Union[Any, HTTPValidationError]]:
    """Build a registered generator's composite state

    Args:
        job_name (str):
        body (CompositeRef): A composite named by a registered generator, optionally with
            overrides.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[Any, HTTPValidationError]
    """

    return (
        await asyncio_detailed(
            job_name=job_name,
            client=client,
            body=body,
        )
    ).parsed
