from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.biomodel_info import BiomodelInfo
from ...models.http_validation_error import HTTPValidationError
from ...types import Response


def _get_kwargs(
    biomodel_id: str,
) -> dict[str, Any]:
    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": f"/compose/v1/biomodels/{biomodel_id}/metadata",
    }

    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[BiomodelInfo, HTTPValidationError]]:
    if response.status_code == 200:
        response_200 = BiomodelInfo.from_dict(response.json())

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
) -> Response[Union[BiomodelInfo, HTTPValidationError]]:
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
) -> Response[Union[BiomodelInfo, HTTPValidationError]]:
    """Get metadata for a BioModels database entry

    Args:
        biomodel_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[BiomodelInfo, HTTPValidationError]]
    """

    kwargs = _get_kwargs(
        biomodel_id=biomodel_id,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    biomodel_id: str,
    *,
    client: Union[AuthenticatedClient, Client],
) -> Optional[Union[BiomodelInfo, HTTPValidationError]]:
    """Get metadata for a BioModels database entry

    Args:
        biomodel_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[BiomodelInfo, HTTPValidationError]
    """

    return sync_detailed(
        biomodel_id=biomodel_id,
        client=client,
    ).parsed


async def asyncio_detailed(
    biomodel_id: str,
    *,
    client: Union[AuthenticatedClient, Client],
) -> Response[Union[BiomodelInfo, HTTPValidationError]]:
    """Get metadata for a BioModels database entry

    Args:
        biomodel_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[BiomodelInfo, HTTPValidationError]]
    """

    kwargs = _get_kwargs(
        biomodel_id=biomodel_id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    biomodel_id: str,
    *,
    client: Union[AuthenticatedClient, Client],
) -> Optional[Union[BiomodelInfo, HTTPValidationError]]:
    """Get metadata for a BioModels database entry

    Args:
        biomodel_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[BiomodelInfo, HTTPValidationError]
    """

    return (
        await asyncio_detailed(
            biomodel_id=biomodel_id,
            client=client,
        )
    ).parsed
