from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.http_validation_error import HTTPValidationError
from ...models.simulation import Simulation
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    experiment_id: Union[None, Unset, str] = UNSET,
    tag: Union[None, Unset, str] = UNSET,
) -> dict[str, Any]:
    params: dict[str, Any] = {}

    json_experiment_id: Union[None, Unset, str]
    if isinstance(experiment_id, Unset):
        json_experiment_id = UNSET
    else:
        json_experiment_id = experiment_id
    params["experiment_id"] = json_experiment_id

    json_tag: Union[None, Unset, str]
    if isinstance(tag, Unset):
        json_tag = UNSET
    else:
        json_tag = tag
    params["tag"] = json_tag

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/api/v1/simulations",
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[HTTPValidationError, list["Simulation"]]]:
    if response.status_code == 200:
        response_200 = []
        _response_200 = response.json()
        for response_200_item_data in _response_200:
            response_200_item = Simulation.from_dict(response_200_item_data)

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
) -> Response[Union[HTTPValidationError, list["Simulation"]]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: Union[AuthenticatedClient, Client],
    experiment_id: Union[None, Unset, str] = UNSET,
    tag: Union[None, Unset, str] = UNSET,
) -> Response[Union[HTTPValidationError, list["Simulation"]]]:
    """List all simulation specs uploaded to the database

    Args:
        experiment_id (Union[None, Unset, str]): Comma-separated list of experiment IDs to filter
            by. Example: 'sim31-baseline-60bb,sim33-violacien-seeds1000-generations10-9617'
        tag (Union[None, Unset, str]): Comma-separated list of tags to filter by (e.g. 'cd1').
            Tags are free-form data on each simulation; an unknown tag simply matches nothing. Use GET
            /api/v1/simulations/tags to list tags present in the database.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[HTTPValidationError, list['Simulation']]]
    """

    kwargs = _get_kwargs(
        experiment_id=experiment_id,
        tag=tag,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: Union[AuthenticatedClient, Client],
    experiment_id: Union[None, Unset, str] = UNSET,
    tag: Union[None, Unset, str] = UNSET,
) -> Optional[Union[HTTPValidationError, list["Simulation"]]]:
    """List all simulation specs uploaded to the database

    Args:
        experiment_id (Union[None, Unset, str]): Comma-separated list of experiment IDs to filter
            by. Example: 'sim31-baseline-60bb,sim33-violacien-seeds1000-generations10-9617'
        tag (Union[None, Unset, str]): Comma-separated list of tags to filter by (e.g. 'cd1').
            Tags are free-form data on each simulation; an unknown tag simply matches nothing. Use GET
            /api/v1/simulations/tags to list tags present in the database.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[HTTPValidationError, list['Simulation']]
    """

    return sync_detailed(
        client=client,
        experiment_id=experiment_id,
        tag=tag,
    ).parsed


async def asyncio_detailed(
    *,
    client: Union[AuthenticatedClient, Client],
    experiment_id: Union[None, Unset, str] = UNSET,
    tag: Union[None, Unset, str] = UNSET,
) -> Response[Union[HTTPValidationError, list["Simulation"]]]:
    """List all simulation specs uploaded to the database

    Args:
        experiment_id (Union[None, Unset, str]): Comma-separated list of experiment IDs to filter
            by. Example: 'sim31-baseline-60bb,sim33-violacien-seeds1000-generations10-9617'
        tag (Union[None, Unset, str]): Comma-separated list of tags to filter by (e.g. 'cd1').
            Tags are free-form data on each simulation; an unknown tag simply matches nothing. Use GET
            /api/v1/simulations/tags to list tags present in the database.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[HTTPValidationError, list['Simulation']]]
    """

    kwargs = _get_kwargs(
        experiment_id=experiment_id,
        tag=tag,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: Union[AuthenticatedClient, Client],
    experiment_id: Union[None, Unset, str] = UNSET,
    tag: Union[None, Unset, str] = UNSET,
) -> Optional[Union[HTTPValidationError, list["Simulation"]]]:
    """List all simulation specs uploaded to the database

    Args:
        experiment_id (Union[None, Unset, str]): Comma-separated list of experiment IDs to filter
            by. Example: 'sim31-baseline-60bb,sim33-violacien-seeds1000-generations10-9617'
        tag (Union[None, Unset, str]): Comma-separated list of tags to filter by (e.g. 'cd1').
            Tags are free-form data on each simulation; an unknown tag simply matches nothing. Use GET
            /api/v1/simulations/tags to list tags present in the database.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[HTTPValidationError, list['Simulation']]
    """

    return (
        await asyncio_detailed(
            client=client,
            experiment_id=experiment_id,
            tag=tag,
        )
    ).parsed
