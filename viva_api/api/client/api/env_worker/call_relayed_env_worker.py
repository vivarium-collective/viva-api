from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.call_relayed_env_worker_body_type_0 import CallRelayedEnvWorkerBodyType0
from ...models.http_validation_error import HTTPValidationError
from ...models.relay_call_response import RelayCallResponse
from ...types import UNSET, Response


def _get_kwargs(
    job_name: str,
    *,
    body: Union["CallRelayedEnvWorkerBodyType0", None],
    method: str,
    timeout: float,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    params: dict[str, Any] = {}

    params["method"] = method

    params["timeout"] = timeout

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": f"/env-worker/v1/relay/workers/{job_name}/call",
        "params": params,
    }

    _kwargs["json"]: Union[None, dict[str, Any]]
    if isinstance(body, CallRelayedEnvWorkerBodyType0):
        _kwargs["json"] = body.to_dict()
    else:
        _kwargs["json"] = body

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[HTTPValidationError, RelayCallResponse]]:
    if response.status_code == 200:
        response_200 = RelayCallResponse.from_dict(response.json())

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
) -> Response[Union[HTTPValidationError, RelayCallResponse]]:
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
    body: Union["CallRelayedEnvWorkerBodyType0", None],
    method: str,
    timeout: float,
) -> Response[Union[HTTPValidationError, RelayCallResponse]]:
    """Forward one JSON-RPC call to a relayed env worker

     Forward one call down a held worker socket, mapping its failures to HTTP.

    Shared by the generic ``/call`` below and by every named capability endpoint,
    so those cannot drift on what a lost socket or a refused call means.

    Runs on a worker thread: the call holds a per-worker mutex for its whole
    duration (the worker's FIFO contract), and blocking the event loop on that
    would stall every unrelated request in this process.

    Args:
        job_name (str):
        method (str):
        timeout (float):
        body (Union['CallRelayedEnvWorkerBodyType0', None]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[HTTPValidationError, RelayCallResponse]]
    """

    kwargs = _get_kwargs(
        job_name=job_name,
        body=body,
        method=method,
        timeout=timeout,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    job_name: str,
    *,
    client: Union[AuthenticatedClient, Client],
    body: Union["CallRelayedEnvWorkerBodyType0", None],
    method: str,
    timeout: float,
) -> Optional[Union[HTTPValidationError, RelayCallResponse]]:
    """Forward one JSON-RPC call to a relayed env worker

     Forward one call down a held worker socket, mapping its failures to HTTP.

    Shared by the generic ``/call`` below and by every named capability endpoint,
    so those cannot drift on what a lost socket or a refused call means.

    Runs on a worker thread: the call holds a per-worker mutex for its whole
    duration (the worker's FIFO contract), and blocking the event loop on that
    would stall every unrelated request in this process.

    Args:
        job_name (str):
        method (str):
        timeout (float):
        body (Union['CallRelayedEnvWorkerBodyType0', None]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[HTTPValidationError, RelayCallResponse]
    """

    return sync_detailed(
        job_name=job_name,
        client=client,
        body=body,
        method=method,
        timeout=timeout,
    ).parsed


async def asyncio_detailed(
    job_name: str,
    *,
    client: Union[AuthenticatedClient, Client],
    body: Union["CallRelayedEnvWorkerBodyType0", None],
    method: str,
    timeout: float,
) -> Response[Union[HTTPValidationError, RelayCallResponse]]:
    """Forward one JSON-RPC call to a relayed env worker

     Forward one call down a held worker socket, mapping its failures to HTTP.

    Shared by the generic ``/call`` below and by every named capability endpoint,
    so those cannot drift on what a lost socket or a refused call means.

    Runs on a worker thread: the call holds a per-worker mutex for its whole
    duration (the worker's FIFO contract), and blocking the event loop on that
    would stall every unrelated request in this process.

    Args:
        job_name (str):
        method (str):
        timeout (float):
        body (Union['CallRelayedEnvWorkerBodyType0', None]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[HTTPValidationError, RelayCallResponse]]
    """

    kwargs = _get_kwargs(
        job_name=job_name,
        body=body,
        method=method,
        timeout=timeout,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    job_name: str,
    *,
    client: Union[AuthenticatedClient, Client],
    body: Union["CallRelayedEnvWorkerBodyType0", None],
    method: str,
    timeout: float,
) -> Optional[Union[HTTPValidationError, RelayCallResponse]]:
    """Forward one JSON-RPC call to a relayed env worker

     Forward one call down a held worker socket, mapping its failures to HTTP.

    Shared by the generic ``/call`` below and by every named capability endpoint,
    so those cannot drift on what a lost socket or a refused call means.

    Runs on a worker thread: the call holds a per-worker mutex for its whole
    duration (the worker's FIFO contract), and blocking the event loop on that
    would stall every unrelated request in this process.

    Args:
        job_name (str):
        method (str):
        timeout (float):
        body (Union['CallRelayedEnvWorkerBodyType0', None]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[HTTPValidationError, RelayCallResponse]
    """

    return (
        await asyncio_detailed(
            job_name=job_name,
            client=client,
            body=body,
            method=method,
            timeout=timeout,
        )
    ).parsed
