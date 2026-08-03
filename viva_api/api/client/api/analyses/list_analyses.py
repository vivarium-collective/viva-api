from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.experiment_analysis_dto import ExperimentAnalysisDTO
from ...models.http_validation_error import HTTPValidationError
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    experiment_id: Union[None, Unset, str] = UNSET,
    simulation_id: Union[None, Unset, int] = UNSET,
) -> dict[str, Any]:
    params: dict[str, Any] = {}

    json_experiment_id: Union[None, Unset, str]
    if isinstance(experiment_id, Unset):
        json_experiment_id = UNSET
    else:
        json_experiment_id = experiment_id
    params["experiment_id"] = json_experiment_id

    json_simulation_id: Union[None, Unset, int]
    if isinstance(simulation_id, Unset):
        json_simulation_id = UNSET
    else:
        json_simulation_id = simulation_id
    params["simulation_id"] = json_simulation_id

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/api/v1/analyses",
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[HTTPValidationError, list["ExperimentAnalysisDTO"]]]:
    if response.status_code == 200:
        response_200 = []
        _response_200 = response.json()
        for response_200_item_data in _response_200:
            response_200_item = ExperimentAnalysisDTO.from_dict(response_200_item_data)

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
) -> Response[Union[HTTPValidationError, list["ExperimentAnalysisDTO"]]]:
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
    simulation_id: Union[None, Unset, int] = UNSET,
) -> Response[Union[HTTPValidationError, list["ExperimentAnalysisDTO"]]]:
    """List all analyses across all simulations (exhaustive; filtering/paging to come)

    Args:
        experiment_id (Union[None, Unset, str]): Optional: filter by experiment_id.
        simulation_id (Union[None, Unset, int]): Optional: filter by simulation database id.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[HTTPValidationError, list['ExperimentAnalysisDTO']]]
    """

    kwargs = _get_kwargs(
        experiment_id=experiment_id,
        simulation_id=simulation_id,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: Union[AuthenticatedClient, Client],
    experiment_id: Union[None, Unset, str] = UNSET,
    simulation_id: Union[None, Unset, int] = UNSET,
) -> Optional[Union[HTTPValidationError, list["ExperimentAnalysisDTO"]]]:
    """List all analyses across all simulations (exhaustive; filtering/paging to come)

    Args:
        experiment_id (Union[None, Unset, str]): Optional: filter by experiment_id.
        simulation_id (Union[None, Unset, int]): Optional: filter by simulation database id.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[HTTPValidationError, list['ExperimentAnalysisDTO']]
    """

    return sync_detailed(
        client=client,
        experiment_id=experiment_id,
        simulation_id=simulation_id,
    ).parsed


async def asyncio_detailed(
    *,
    client: Union[AuthenticatedClient, Client],
    experiment_id: Union[None, Unset, str] = UNSET,
    simulation_id: Union[None, Unset, int] = UNSET,
) -> Response[Union[HTTPValidationError, list["ExperimentAnalysisDTO"]]]:
    """List all analyses across all simulations (exhaustive; filtering/paging to come)

    Args:
        experiment_id (Union[None, Unset, str]): Optional: filter by experiment_id.
        simulation_id (Union[None, Unset, int]): Optional: filter by simulation database id.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[HTTPValidationError, list['ExperimentAnalysisDTO']]]
    """

    kwargs = _get_kwargs(
        experiment_id=experiment_id,
        simulation_id=simulation_id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: Union[AuthenticatedClient, Client],
    experiment_id: Union[None, Unset, str] = UNSET,
    simulation_id: Union[None, Unset, int] = UNSET,
) -> Optional[Union[HTTPValidationError, list["ExperimentAnalysisDTO"]]]:
    """List all analyses across all simulations (exhaustive; filtering/paging to come)

    Args:
        experiment_id (Union[None, Unset, str]): Optional: filter by experiment_id.
        simulation_id (Union[None, Unset, int]): Optional: filter by simulation database id.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[HTTPValidationError, list['ExperimentAnalysisDTO']]
    """

    return (
        await asyncio_detailed(
            client=client,
            experiment_id=experiment_id,
            simulation_id=simulation_id,
        )
    ).parsed
