from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.http_validation_error import HTTPValidationError
from ...models.simulation_analysis_figures import SimulationAnalysisFigures
from ...types import Response


def _get_kwargs(
    id: int,
) -> dict[str, Any]:
    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": f"/api/v1/simulations/{id}/analysis-figures",
    }

    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[HTTPValidationError, SimulationAnalysisFigures]]:
    if response.status_code == 200:
        response_200 = SimulationAnalysisFigures.from_dict(response.json())

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
) -> Response[Union[HTTPValidationError, SimulationAnalysisFigures]]:
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
) -> Response[Union[HTTPValidationError, SimulationAnalysisFigures]]:
    r"""List every analysis's rendered figures + ptools for a simulation (metadata only).

     List (never inline) the rendered ``viz/`` figures and ``ptools/`` tables of
    every analysis on a simulation, unioning DB analysis records with a direct S3
    walk so hand-dispatched \"fill\" analyses that created no DB record still surface
    (viva-api#648). The API reads S3 with its own credentials, so a credential-less
    client can then fetch each artifact via ``/simulations/{id}/analysis-figure``.

    Args:
        id (int): Database ID of the simulation

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[HTTPValidationError, SimulationAnalysisFigures]]
    """

    kwargs = _get_kwargs(
        id=id,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    id: int,
    *,
    client: Union[AuthenticatedClient, Client],
) -> Optional[Union[HTTPValidationError, SimulationAnalysisFigures]]:
    r"""List every analysis's rendered figures + ptools for a simulation (metadata only).

     List (never inline) the rendered ``viz/`` figures and ``ptools/`` tables of
    every analysis on a simulation, unioning DB analysis records with a direct S3
    walk so hand-dispatched \"fill\" analyses that created no DB record still surface
    (viva-api#648). The API reads S3 with its own credentials, so a credential-less
    client can then fetch each artifact via ``/simulations/{id}/analysis-figure``.

    Args:
        id (int): Database ID of the simulation

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[HTTPValidationError, SimulationAnalysisFigures]
    """

    return sync_detailed(
        id=id,
        client=client,
    ).parsed


async def asyncio_detailed(
    id: int,
    *,
    client: Union[AuthenticatedClient, Client],
) -> Response[Union[HTTPValidationError, SimulationAnalysisFigures]]:
    r"""List every analysis's rendered figures + ptools for a simulation (metadata only).

     List (never inline) the rendered ``viz/`` figures and ``ptools/`` tables of
    every analysis on a simulation, unioning DB analysis records with a direct S3
    walk so hand-dispatched \"fill\" analyses that created no DB record still surface
    (viva-api#648). The API reads S3 with its own credentials, so a credential-less
    client can then fetch each artifact via ``/simulations/{id}/analysis-figure``.

    Args:
        id (int): Database ID of the simulation

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[HTTPValidationError, SimulationAnalysisFigures]]
    """

    kwargs = _get_kwargs(
        id=id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    id: int,
    *,
    client: Union[AuthenticatedClient, Client],
) -> Optional[Union[HTTPValidationError, SimulationAnalysisFigures]]:
    r"""List every analysis's rendered figures + ptools for a simulation (metadata only).

     List (never inline) the rendered ``viz/`` figures and ``ptools/`` tables of
    every analysis on a simulation, unioning DB analysis records with a direct S3
    walk so hand-dispatched \"fill\" analyses that created no DB record still surface
    (viva-api#648). The API reads S3 with its own credentials, so a credential-less
    client can then fetch each artifact via ``/simulations/{id}/analysis-figure``.

    Args:
        id (int): Database ID of the simulation

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[HTTPValidationError, SimulationAnalysisFigures]
    """

    return (
        await asyncio_detailed(
            id=id,
            client=client,
        )
    ).parsed
