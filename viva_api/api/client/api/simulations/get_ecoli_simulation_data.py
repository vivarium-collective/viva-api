from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.http_validation_error import HTTPValidationError
from ...models.simulation_analysis_data_response_type import SimulationAnalysisDataResponseType
from ...types import UNSET, Response, Unset


def _get_kwargs(
    id: int,
    *,
    response_type: Union[Unset, SimulationAnalysisDataResponseType] = UNSET,
) -> dict[str, Any]:
    params: dict[str, Any] = {}

    json_response_type: Union[Unset, str] = UNSET
    if not isinstance(response_type, Unset):
        json_response_type = response_type.value

    params["response_type"] = json_response_type

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": f"/api/v1/simulations/{id}/data",
        "params": params,
    }

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
    id: int,
    *,
    client: Union[AuthenticatedClient, Client],
    response_type: Union[Unset, SimulationAnalysisDataResponseType] = UNSET,
) -> Response[Union[Any, HTTPValidationError]]:
    r"""Get simulation omics data as a downloadable tar.gz archive

     Get simulation outputs as a tar.gz archive.

    Choose response_type based on your use case:
    - **file**: Creates the archive and returns it as a downloadable file.
      Best for browser downloads and Swagger UI - shows a \"Download\" button.
    - **streaming**: Streams the archive in chunks as it's created.
      Better for very large files or when you want to start processing before download completes.

    Args:
        id (int): Database ID of the simulation.
        response_type (Union[Unset, SimulationAnalysisDataResponseType]): Response type for
            simulation data endpoint.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[Any, HTTPValidationError]]
    """

    kwargs = _get_kwargs(
        id=id,
        response_type=response_type,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    id: int,
    *,
    client: Union[AuthenticatedClient, Client],
    response_type: Union[Unset, SimulationAnalysisDataResponseType] = UNSET,
) -> Optional[Union[Any, HTTPValidationError]]:
    r"""Get simulation omics data as a downloadable tar.gz archive

     Get simulation outputs as a tar.gz archive.

    Choose response_type based on your use case:
    - **file**: Creates the archive and returns it as a downloadable file.
      Best for browser downloads and Swagger UI - shows a \"Download\" button.
    - **streaming**: Streams the archive in chunks as it's created.
      Better for very large files or when you want to start processing before download completes.

    Args:
        id (int): Database ID of the simulation.
        response_type (Union[Unset, SimulationAnalysisDataResponseType]): Response type for
            simulation data endpoint.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[Any, HTTPValidationError]
    """

    return sync_detailed(
        id=id,
        client=client,
        response_type=response_type,
    ).parsed


async def asyncio_detailed(
    id: int,
    *,
    client: Union[AuthenticatedClient, Client],
    response_type: Union[Unset, SimulationAnalysisDataResponseType] = UNSET,
) -> Response[Union[Any, HTTPValidationError]]:
    r"""Get simulation omics data as a downloadable tar.gz archive

     Get simulation outputs as a tar.gz archive.

    Choose response_type based on your use case:
    - **file**: Creates the archive and returns it as a downloadable file.
      Best for browser downloads and Swagger UI - shows a \"Download\" button.
    - **streaming**: Streams the archive in chunks as it's created.
      Better for very large files or when you want to start processing before download completes.

    Args:
        id (int): Database ID of the simulation.
        response_type (Union[Unset, SimulationAnalysisDataResponseType]): Response type for
            simulation data endpoint.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[Any, HTTPValidationError]]
    """

    kwargs = _get_kwargs(
        id=id,
        response_type=response_type,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    id: int,
    *,
    client: Union[AuthenticatedClient, Client],
    response_type: Union[Unset, SimulationAnalysisDataResponseType] = UNSET,
) -> Optional[Union[Any, HTTPValidationError]]:
    r"""Get simulation omics data as a downloadable tar.gz archive

     Get simulation outputs as a tar.gz archive.

    Choose response_type based on your use case:
    - **file**: Creates the archive and returns it as a downloadable file.
      Best for browser downloads and Swagger UI - shows a \"Download\" button.
    - **streaming**: Streams the archive in chunks as it's created.
      Better for very large files or when you want to start processing before download completes.

    Args:
        id (int): Database ID of the simulation.
        response_type (Union[Unset, SimulationAnalysisDataResponseType]): Response type for
            simulation data endpoint.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[Any, HTTPValidationError]
    """

    return (
        await asyncio_detailed(
            id=id,
            client=client,
            response_type=response_type,
        )
    ).parsed
