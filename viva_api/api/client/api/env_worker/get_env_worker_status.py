from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.env_worker_status_response import EnvWorkerStatusResponse
from ...models.http_validation_error import HTTPValidationError
from ...types import UNSET, Response, Unset


def _get_kwargs(
    job_name: str,
    *,
    include_logs: Union[Unset, bool] = False,
) -> dict[str, Any]:
    params: dict[str, Any] = {}

    params["include_logs"] = include_logs

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": f"/env-worker/v1/workers/{job_name}",
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[EnvWorkerStatusResponse, HTTPValidationError]]:
    if response.status_code == 200:
        response_200 = EnvWorkerStatusResponse.from_dict(response.json())

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
) -> Response[Union[EnvWorkerStatusResponse, HTTPValidationError]]:
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
    include_logs: Union[Unset, bool] = False,
) -> Response[Union[EnvWorkerStatusResponse, HTTPValidationError]]:
    """Status of an env worker, with logs when it has failed

    Args:
        job_name (str):
        include_logs (Union[Unset, bool]):  Default: False.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[EnvWorkerStatusResponse, HTTPValidationError]]
    """

    kwargs = _get_kwargs(
        job_name=job_name,
        include_logs=include_logs,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    job_name: str,
    *,
    client: Union[AuthenticatedClient, Client],
    include_logs: Union[Unset, bool] = False,
) -> Optional[Union[EnvWorkerStatusResponse, HTTPValidationError]]:
    """Status of an env worker, with logs when it has failed

    Args:
        job_name (str):
        include_logs (Union[Unset, bool]):  Default: False.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[EnvWorkerStatusResponse, HTTPValidationError]
    """

    return sync_detailed(
        job_name=job_name,
        client=client,
        include_logs=include_logs,
    ).parsed


async def asyncio_detailed(
    job_name: str,
    *,
    client: Union[AuthenticatedClient, Client],
    include_logs: Union[Unset, bool] = False,
) -> Response[Union[EnvWorkerStatusResponse, HTTPValidationError]]:
    """Status of an env worker, with logs when it has failed

    Args:
        job_name (str):
        include_logs (Union[Unset, bool]):  Default: False.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[EnvWorkerStatusResponse, HTTPValidationError]]
    """

    kwargs = _get_kwargs(
        job_name=job_name,
        include_logs=include_logs,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    job_name: str,
    *,
    client: Union[AuthenticatedClient, Client],
    include_logs: Union[Unset, bool] = False,
) -> Optional[Union[EnvWorkerStatusResponse, HTTPValidationError]]:
    """Status of an env worker, with logs when it has failed

    Args:
        job_name (str):
        include_logs (Union[Unset, bool]):  Default: False.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[EnvWorkerStatusResponse, HTTPValidationError]
    """

    return (
        await asyncio_detailed(
            job_name=job_name,
            client=client,
            include_logs=include_logs,
        )
    ).parsed
