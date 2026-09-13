from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.http_validation_error import HTTPValidationError
from ...models.simulation_events import SimulationEvents
from ...types import UNSET, Response, Unset


def _get_kwargs(
    id: int,
    *,
    level: Union[None, Unset, str] = UNSET,
    event: Union[None, Unset, str] = UNSET,
    generation: Union[None, Unset, int] = UNSET,
    span_id: Union[None, Unset, str] = UNSET,
    after: Union[None, Unset, int] = UNSET,
    limit: Union[Unset, int] = 1000,
    tree: Union[Unset, bool] = False,
) -> dict[str, Any]:
    params: dict[str, Any] = {}

    json_level: Union[None, Unset, str]
    if isinstance(level, Unset):
        json_level = UNSET
    else:
        json_level = level
    params["level"] = json_level

    json_event: Union[None, Unset, str]
    if isinstance(event, Unset):
        json_event = UNSET
    else:
        json_event = event
    params["event"] = json_event

    json_generation: Union[None, Unset, int]
    if isinstance(generation, Unset):
        json_generation = UNSET
    else:
        json_generation = generation
    params["generation"] = json_generation

    json_span_id: Union[None, Unset, str]
    if isinstance(span_id, Unset):
        json_span_id = UNSET
    else:
        json_span_id = span_id
    params["span_id"] = json_span_id

    json_after: Union[None, Unset, int]
    if isinstance(after, Unset):
        json_after = UNSET
    else:
        json_after = after
    params["after"] = json_after

    params["limit"] = limit

    params["tree"] = tree

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": f"/api/v1/simulations/{id}/events",
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[HTTPValidationError, SimulationEvents]]:
    if response.status_code == 200:
        response_200 = SimulationEvents.from_dict(response.json())

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
) -> Response[Union[HTTPValidationError, SimulationEvents]]:
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
    level: Union[None, Unset, str] = UNSET,
    event: Union[None, Unset, str] = UNSET,
    generation: Union[None, Unset, int] = UNSET,
    span_id: Union[None, Unset, str] = UNSET,
    after: Union[None, Unset, int] = UNSET,
    limit: Union[Unset, int] = 1000,
    tree: Union[Unset, bool] = False,
) -> Response[Union[HTTPValidationError, SimulationEvents]]:
    """Structured events of a simulation run (engine, runner and dispatch components), flat or as the span
    tree

     Observability plan D4d: what the run's tasks reported, without AWS
    credentials. Events arrive through the scheduler's ingester from the tasks'
    ``events.jsonl`` objects plus the API's own dispatcher-layer events; ``tick``
    heartbeats are folded into ``/status`` (``last_event_at``) and never stored.
    404 when the simulation or its run row does not exist.

    Args:
        id (int):
        level (Union[None, Unset, str]): debug | info | warning | error
        event (Union[None, Unset, str]): exact event name, e.g. process.exception,
            lineage.generation.end
        generation (Union[None, Unset, int]):
        span_id (Union[None, Unset, str]):
        after (Union[None, Unset, int]): cursor of the last event seen (paging)
        limit (Union[Unset, int]):  Default: 1000.
        tree (Union[Unset, bool]): also return the span tree with events attached Default: False.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[HTTPValidationError, SimulationEvents]]
    """

    kwargs = _get_kwargs(
        id=id,
        level=level,
        event=event,
        generation=generation,
        span_id=span_id,
        after=after,
        limit=limit,
        tree=tree,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    id: int,
    *,
    client: Union[AuthenticatedClient, Client],
    level: Union[None, Unset, str] = UNSET,
    event: Union[None, Unset, str] = UNSET,
    generation: Union[None, Unset, int] = UNSET,
    span_id: Union[None, Unset, str] = UNSET,
    after: Union[None, Unset, int] = UNSET,
    limit: Union[Unset, int] = 1000,
    tree: Union[Unset, bool] = False,
) -> Optional[Union[HTTPValidationError, SimulationEvents]]:
    """Structured events of a simulation run (engine, runner and dispatch components), flat or as the span
    tree

     Observability plan D4d: what the run's tasks reported, without AWS
    credentials. Events arrive through the scheduler's ingester from the tasks'
    ``events.jsonl`` objects plus the API's own dispatcher-layer events; ``tick``
    heartbeats are folded into ``/status`` (``last_event_at``) and never stored.
    404 when the simulation or its run row does not exist.

    Args:
        id (int):
        level (Union[None, Unset, str]): debug | info | warning | error
        event (Union[None, Unset, str]): exact event name, e.g. process.exception,
            lineage.generation.end
        generation (Union[None, Unset, int]):
        span_id (Union[None, Unset, str]):
        after (Union[None, Unset, int]): cursor of the last event seen (paging)
        limit (Union[Unset, int]):  Default: 1000.
        tree (Union[Unset, bool]): also return the span tree with events attached Default: False.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[HTTPValidationError, SimulationEvents]
    """

    return sync_detailed(
        id=id,
        client=client,
        level=level,
        event=event,
        generation=generation,
        span_id=span_id,
        after=after,
        limit=limit,
        tree=tree,
    ).parsed


async def asyncio_detailed(
    id: int,
    *,
    client: Union[AuthenticatedClient, Client],
    level: Union[None, Unset, str] = UNSET,
    event: Union[None, Unset, str] = UNSET,
    generation: Union[None, Unset, int] = UNSET,
    span_id: Union[None, Unset, str] = UNSET,
    after: Union[None, Unset, int] = UNSET,
    limit: Union[Unset, int] = 1000,
    tree: Union[Unset, bool] = False,
) -> Response[Union[HTTPValidationError, SimulationEvents]]:
    """Structured events of a simulation run (engine, runner and dispatch components), flat or as the span
    tree

     Observability plan D4d: what the run's tasks reported, without AWS
    credentials. Events arrive through the scheduler's ingester from the tasks'
    ``events.jsonl`` objects plus the API's own dispatcher-layer events; ``tick``
    heartbeats are folded into ``/status`` (``last_event_at``) and never stored.
    404 when the simulation or its run row does not exist.

    Args:
        id (int):
        level (Union[None, Unset, str]): debug | info | warning | error
        event (Union[None, Unset, str]): exact event name, e.g. process.exception,
            lineage.generation.end
        generation (Union[None, Unset, int]):
        span_id (Union[None, Unset, str]):
        after (Union[None, Unset, int]): cursor of the last event seen (paging)
        limit (Union[Unset, int]):  Default: 1000.
        tree (Union[Unset, bool]): also return the span tree with events attached Default: False.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[HTTPValidationError, SimulationEvents]]
    """

    kwargs = _get_kwargs(
        id=id,
        level=level,
        event=event,
        generation=generation,
        span_id=span_id,
        after=after,
        limit=limit,
        tree=tree,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    id: int,
    *,
    client: Union[AuthenticatedClient, Client],
    level: Union[None, Unset, str] = UNSET,
    event: Union[None, Unset, str] = UNSET,
    generation: Union[None, Unset, int] = UNSET,
    span_id: Union[None, Unset, str] = UNSET,
    after: Union[None, Unset, int] = UNSET,
    limit: Union[Unset, int] = 1000,
    tree: Union[Unset, bool] = False,
) -> Optional[Union[HTTPValidationError, SimulationEvents]]:
    """Structured events of a simulation run (engine, runner and dispatch components), flat or as the span
    tree

     Observability plan D4d: what the run's tasks reported, without AWS
    credentials. Events arrive through the scheduler's ingester from the tasks'
    ``events.jsonl`` objects plus the API's own dispatcher-layer events; ``tick``
    heartbeats are folded into ``/status`` (``last_event_at``) and never stored.
    404 when the simulation or its run row does not exist.

    Args:
        id (int):
        level (Union[None, Unset, str]): debug | info | warning | error
        event (Union[None, Unset, str]): exact event name, e.g. process.exception,
            lineage.generation.end
        generation (Union[None, Unset, int]):
        span_id (Union[None, Unset, str]):
        after (Union[None, Unset, int]): cursor of the last event seen (paging)
        limit (Union[Unset, int]):  Default: 1000.
        tree (Union[Unset, bool]): also return the span tree with events attached Default: False.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[HTTPValidationError, SimulationEvents]
    """

    return (
        await asyncio_detailed(
            id=id,
            client=client,
            level=level,
            event=event,
            generation=generation,
            span_id=span_id,
            after=after,
            limit=limit,
            tree=tree,
        )
    ).parsed
