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

     Stop a worker. NOT ownership-checked, deliberately — and its tasks are
    settled with attribution instead.

    Stopping a worker is overwhelmingly an AUTOMATIC operation: the workbench's
    pool calls it on LRU eviction, idle reap, dead-worker replacement and process
    exit. The pool has no identity to present, so an ownership check here would
    either break it or have to let unidentified callers through — which is the
    very bypass such a check would exist to close, and would leave the perverse
    rule that anonymous callers may stop workers while identified ones may not.

    So the worker is shared infrastructure with an automatic lifecycle; the task
    is the unit of work that has an owner. What is owed to someone whose task
    dies with a worker is not a veto but an EXPLANATION: their task moves to a
    terminal state naming what happened and, where identity is configured, who
    did it — rather than hanging in `running` until somebody wonders.

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

     Stop a worker. NOT ownership-checked, deliberately — and its tasks are
    settled with attribution instead.

    Stopping a worker is overwhelmingly an AUTOMATIC operation: the workbench's
    pool calls it on LRU eviction, idle reap, dead-worker replacement and process
    exit. The pool has no identity to present, so an ownership check here would
    either break it or have to let unidentified callers through — which is the
    very bypass such a check would exist to close, and would leave the perverse
    rule that anonymous callers may stop workers while identified ones may not.

    So the worker is shared infrastructure with an automatic lifecycle; the task
    is the unit of work that has an owner. What is owed to someone whose task
    dies with a worker is not a veto but an EXPLANATION: their task moves to a
    terminal state naming what happened and, where identity is configured, who
    did it — rather than hanging in `running` until somebody wonders.

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

     Stop a worker. NOT ownership-checked, deliberately — and its tasks are
    settled with attribution instead.

    Stopping a worker is overwhelmingly an AUTOMATIC operation: the workbench's
    pool calls it on LRU eviction, idle reap, dead-worker replacement and process
    exit. The pool has no identity to present, so an ownership check here would
    either break it or have to let unidentified callers through — which is the
    very bypass such a check would exist to close, and would leave the perverse
    rule that anonymous callers may stop workers while identified ones may not.

    So the worker is shared infrastructure with an automatic lifecycle; the task
    is the unit of work that has an owner. What is owed to someone whose task
    dies with a worker is not a veto but an EXPLANATION: their task moves to a
    terminal state naming what happened and, where identity is configured, who
    did it — rather than hanging in `running` until somebody wonders.

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

     Stop a worker. NOT ownership-checked, deliberately — and its tasks are
    settled with attribution instead.

    Stopping a worker is overwhelmingly an AUTOMATIC operation: the workbench's
    pool calls it on LRU eviction, idle reap, dead-worker replacement and process
    exit. The pool has no identity to present, so an ownership check here would
    either break it or have to let unidentified callers through — which is the
    very bypass such a check would exist to close, and would leave the perverse
    rule that anonymous callers may stop workers while identified ones may not.

    So the worker is shared infrastructure with an automatic lifecycle; the task
    is the unit of work that has an owner. What is owed to someone whose task
    dies with a worker is not a veto but an EXPLANATION: their task moves to a
    terminal state naming what happened and, where identity is configured, who
    did it — rather than hanging in `running` until somebody wonders.

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
