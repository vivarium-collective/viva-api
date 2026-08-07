from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.http_validation_error import HTTPValidationError
from ...models.tsv_output_file import TsvOutputFile
from ...types import Response


def _get_kwargs(
    id: int,
) -> dict[str, Any]:
    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": f"/api/v1/analyses/{id}/data",
    }

    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[HTTPValidationError, list["TsvOutputFile"]]]:
    if response.status_code == 200:
        response_200 = []
        _response_200 = response.json()
        for response_200_item_data in _response_200:
            response_200_item = TsvOutputFile.from_dict(response_200_item_data)

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
) -> Response[Union[HTTPValidationError, list["TsvOutputFile"]]]:
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
) -> Response[Union[HTTPValidationError, list["TsvOutputFile"]]]:
    """Retrieve the output files (TSV/CSV/TXT/HTML) of an existing analysis by id

     Pure retrieval of a pre-computed analysis's files by id (never computes).

    Returns the same ``list[TsvOutputFile]`` shape as the legacy ``POST /analyses``.
    409 if the analysis is not READY; 404 if the analysis id is unknown.

    Args:
        id (int): Database ID of the analysis

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[HTTPValidationError, list['TsvOutputFile']]]
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
) -> Optional[Union[HTTPValidationError, list["TsvOutputFile"]]]:
    """Retrieve the output files (TSV/CSV/TXT/HTML) of an existing analysis by id

     Pure retrieval of a pre-computed analysis's files by id (never computes).

    Returns the same ``list[TsvOutputFile]`` shape as the legacy ``POST /analyses``.
    409 if the analysis is not READY; 404 if the analysis id is unknown.

    Args:
        id (int): Database ID of the analysis

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[HTTPValidationError, list['TsvOutputFile']]
    """

    return sync_detailed(
        id=id,
        client=client,
    ).parsed


async def asyncio_detailed(
    id: int,
    *,
    client: Union[AuthenticatedClient, Client],
) -> Response[Union[HTTPValidationError, list["TsvOutputFile"]]]:
    """Retrieve the output files (TSV/CSV/TXT/HTML) of an existing analysis by id

     Pure retrieval of a pre-computed analysis's files by id (never computes).

    Returns the same ``list[TsvOutputFile]`` shape as the legacy ``POST /analyses``.
    409 if the analysis is not READY; 404 if the analysis id is unknown.

    Args:
        id (int): Database ID of the analysis

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[HTTPValidationError, list['TsvOutputFile']]]
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
) -> Optional[Union[HTTPValidationError, list["TsvOutputFile"]]]:
    """Retrieve the output files (TSV/CSV/TXT/HTML) of an existing analysis by id

     Pure retrieval of a pre-computed analysis's files by id (never computes).

    Returns the same ``list[TsvOutputFile]`` shape as the legacy ``POST /analyses``.
    409 if the analysis is not READY; 404 if the analysis id is unknown.

    Args:
        id (int): Database ID of the analysis

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[HTTPValidationError, list['TsvOutputFile']]
    """

    return (
        await asyncio_detailed(
            id=id,
            client=client,
        )
    ).parsed
