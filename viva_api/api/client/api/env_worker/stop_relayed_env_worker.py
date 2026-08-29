from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.http_validation_error import HTTPValidationError
from ...models.stop_relayed_env_worker_response_stop_relayed_env_worker import (
    StopRelayedEnvWorkerResponseStopRelayedEnvWorker,
)
from ...types import Response


def _get_kwargs(
    job_name: str,
) -> dict[str, Any]:
    _kwargs: dict[str, Any] = {
        "method": "delete",
        "url": f"/env-worker/v1/relay/workers/{job_name}",
    }

    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[HTTPValidationError, StopRelayedEnvWorkerResponseStopRelayedEnvWorker]]:
    if response.status_code == 200:
        response_200 = StopRelayedEnvWorkerResponseStopRelayedEnvWorker.from_dict(response.json())

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
) -> Response[Union[HTTPValidationError, StopRelayedEnvWorkerResponseStopRelayedEnvWorker]]:
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
) -> Response[Union[HTTPValidationError, StopRelayedEnvWorkerResponseStopRelayedEnvWorker]]:
    """Close a relayed worker's connection and delete its Job (idempotent)

    Args:
        job_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[HTTPValidationError, StopRelayedEnvWorkerResponseStopRelayedEnvWorker]]
    """

    kwargs = _get_kwargs(
        job_name=job_name,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    job_name: str,
    *,
    client: Union[AuthenticatedClient, Client],
) -> Optional[Union[HTTPValidationError, StopRelayedEnvWorkerResponseStopRelayedEnvWorker]]:
    """Close a relayed worker's connection and delete its Job (idempotent)

    Args:
        job_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[HTTPValidationError, StopRelayedEnvWorkerResponseStopRelayedEnvWorker]
    """

    return sync_detailed(
        job_name=job_name,
        client=client,
    ).parsed


async def asyncio_detailed(
    job_name: str,
    *,
    client: Union[AuthenticatedClient, Client],
) -> Response[Union[HTTPValidationError, StopRelayedEnvWorkerResponseStopRelayedEnvWorker]]:
    """Close a relayed worker's connection and delete its Job (idempotent)

    Args:
        job_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[HTTPValidationError, StopRelayedEnvWorkerResponseStopRelayedEnvWorker]]
    """

    kwargs = _get_kwargs(
        job_name=job_name,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    job_name: str,
    *,
    client: Union[AuthenticatedClient, Client],
) -> Optional[Union[HTTPValidationError, StopRelayedEnvWorkerResponseStopRelayedEnvWorker]]:
    """Close a relayed worker's connection and delete its Job (idempotent)

    Args:
        job_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[HTTPValidationError, StopRelayedEnvWorkerResponseStopRelayedEnvWorker]
    """

    return (
        await asyncio_detailed(
            job_name=job_name,
            client=client,
        )
    ).parsed
