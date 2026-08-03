from http import HTTPStatus
from typing import Any, Optional, Union, cast

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.compose_get_simulation_document_response_compose_get_simulation_document import (
    ComposeGetSimulationDocumentResponseComposeGetSimulationDocument,
)
from ...models.http_validation_error import HTTPValidationError
from ...types import Response


def _get_kwargs(
    simulation_id: int,
) -> dict[str, Any]:
    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": f"/compose/v1/simulation/{simulation_id}/document",
    }

    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[Any, ComposeGetSimulationDocumentResponseComposeGetSimulationDocument, HTTPValidationError]]:
    if response.status_code == 200:
        response_200 = ComposeGetSimulationDocumentResponseComposeGetSimulationDocument.from_dict(response.json())

        return response_200
    if response.status_code == 404:
        response_404 = cast(Any, None)
        return response_404
    if response.status_code == 422:
        response_422 = HTTPValidationError.from_dict(response.json())

        return response_422
    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Response[Union[Any, ComposeGetSimulationDocumentResponseComposeGetSimulationDocument, HTTPValidationError]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    simulation_id: int,
    *,
    client: Union[AuthenticatedClient, Client],
) -> Response[Union[Any, ComposeGetSimulationDocumentResponseComposeGetSimulationDocument, HTTPValidationError]]:
    """Retrieve the process-bigraph document used for a compose simulation

     Return the process-bigraph document (PBG JSON, SBML XML, or OMEX manifest)
    that was uploaded when this simulation was submitted.

    Args:
        simulation_id (int):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[Any, ComposeGetSimulationDocumentResponseComposeGetSimulationDocument, HTTPValidationError]]
    """

    kwargs = _get_kwargs(
        simulation_id=simulation_id,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    simulation_id: int,
    *,
    client: Union[AuthenticatedClient, Client],
) -> Optional[Union[Any, ComposeGetSimulationDocumentResponseComposeGetSimulationDocument, HTTPValidationError]]:
    """Retrieve the process-bigraph document used for a compose simulation

     Return the process-bigraph document (PBG JSON, SBML XML, or OMEX manifest)
    that was uploaded when this simulation was submitted.

    Args:
        simulation_id (int):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[Any, ComposeGetSimulationDocumentResponseComposeGetSimulationDocument, HTTPValidationError]
    """

    return sync_detailed(
        simulation_id=simulation_id,
        client=client,
    ).parsed


async def asyncio_detailed(
    simulation_id: int,
    *,
    client: Union[AuthenticatedClient, Client],
) -> Response[Union[Any, ComposeGetSimulationDocumentResponseComposeGetSimulationDocument, HTTPValidationError]]:
    """Retrieve the process-bigraph document used for a compose simulation

     Return the process-bigraph document (PBG JSON, SBML XML, or OMEX manifest)
    that was uploaded when this simulation was submitted.

    Args:
        simulation_id (int):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[Any, ComposeGetSimulationDocumentResponseComposeGetSimulationDocument, HTTPValidationError]]
    """

    kwargs = _get_kwargs(
        simulation_id=simulation_id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    simulation_id: int,
    *,
    client: Union[AuthenticatedClient, Client],
) -> Optional[Union[Any, ComposeGetSimulationDocumentResponseComposeGetSimulationDocument, HTTPValidationError]]:
    """Retrieve the process-bigraph document used for a compose simulation

     Return the process-bigraph document (PBG JSON, SBML XML, or OMEX manifest)
    that was uploaded when this simulation was submitted.

    Args:
        simulation_id (int):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[Any, ComposeGetSimulationDocumentResponseComposeGetSimulationDocument, HTTPValidationError]
    """

    return (
        await asyncio_detailed(
            simulation_id=simulation_id,
            client=client,
        )
    ).parsed
