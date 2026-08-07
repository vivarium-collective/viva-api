from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.biomodel_simulator import BiomodelSimulator
from ...models.compose_simulation_experiment import ComposeSimulationExperiment
from ...models.http_validation_error import HTTPValidationError
from ...types import UNSET, Response, Unset


def _get_kwargs(
    biomodel_id: str,
    *,
    simulator: Union[Unset, BiomodelSimulator] = UNSET,
) -> dict[str, Any]:
    params: dict[str, Any] = {}

    json_simulator: Union[Unset, str] = UNSET
    if not isinstance(simulator, Unset):
        json_simulator = simulator.value

    params["simulator"] = json_simulator

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": f"/compose/v1/biomodels/{biomodel_id}/run",
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[ComposeSimulationExperiment, HTTPValidationError]]:
    if response.status_code == 200:
        response_200 = ComposeSimulationExperiment.from_dict(response.json())

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
) -> Response[Union[ComposeSimulationExperiment, HTTPValidationError]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    biomodel_id: str,
    *,
    client: Union[AuthenticatedClient, Client],
    simulator: Union[Unset, BiomodelSimulator] = UNSET,
) -> Response[Union[ComposeSimulationExperiment, HTTPValidationError]]:
    """Run a BioModels database model through Copasi or Tellurium

    Args:
        biomodel_id (str):
        simulator (Union[Unset, BiomodelSimulator]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ComposeSimulationExperiment, HTTPValidationError]]
    """

    kwargs = _get_kwargs(
        biomodel_id=biomodel_id,
        simulator=simulator,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    biomodel_id: str,
    *,
    client: Union[AuthenticatedClient, Client],
    simulator: Union[Unset, BiomodelSimulator] = UNSET,
) -> Optional[Union[ComposeSimulationExperiment, HTTPValidationError]]:
    """Run a BioModels database model through Copasi or Tellurium

    Args:
        biomodel_id (str):
        simulator (Union[Unset, BiomodelSimulator]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ComposeSimulationExperiment, HTTPValidationError]
    """

    return sync_detailed(
        biomodel_id=biomodel_id,
        client=client,
        simulator=simulator,
    ).parsed


async def asyncio_detailed(
    biomodel_id: str,
    *,
    client: Union[AuthenticatedClient, Client],
    simulator: Union[Unset, BiomodelSimulator] = UNSET,
) -> Response[Union[ComposeSimulationExperiment, HTTPValidationError]]:
    """Run a BioModels database model through Copasi or Tellurium

    Args:
        biomodel_id (str):
        simulator (Union[Unset, BiomodelSimulator]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ComposeSimulationExperiment, HTTPValidationError]]
    """

    kwargs = _get_kwargs(
        biomodel_id=biomodel_id,
        simulator=simulator,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    biomodel_id: str,
    *,
    client: Union[AuthenticatedClient, Client],
    simulator: Union[Unset, BiomodelSimulator] = UNSET,
) -> Optional[Union[ComposeSimulationExperiment, HTTPValidationError]]:
    """Run a BioModels database model through Copasi or Tellurium

    Args:
        biomodel_id (str):
        simulator (Union[Unset, BiomodelSimulator]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ComposeSimulationExperiment, HTTPValidationError]
    """

    return (
        await asyncio_detailed(
            biomodel_id=biomodel_id,
            client=client,
            simulator=simulator,
        )
    ).parsed
