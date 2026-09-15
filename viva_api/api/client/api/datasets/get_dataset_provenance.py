from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.dataset_provenance_dto import DatasetProvenanceDTO
from ...models.http_validation_error import HTTPValidationError
from ...types import Response


def _get_kwargs(
    id: int,
) -> dict[str, Any]:
    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": f"/api/v1/datasets/{id}/provenance",
    }

    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[DatasetProvenanceDTO, HTTPValidationError]]:
    if response.status_code == 200:
        response_200 = DatasetProvenanceDTO.from_dict(response.json())

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
) -> Response[Union[DatasetProvenanceDTO, HTTPValidationError]]:
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
) -> Response[Union[DatasetProvenanceDTO, HTTPValidationError]]:
    """What wrote a dataset: its producer run, trace and span, and the datasets that run read

     One hop. ``producer`` is the run row (simulation, analysis or ParCa) with its ``source``
    and, when the run was traced, its ``hpcrun_id``/``trace_id``; ``span`` is the span the
    ``artifact.written`` event came from (walk-found datasets have none); ``inputs`` are
    registered datasets the producer's declared source names by ``uri``.

    Args:
        id (int): Database ID of the dataset

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[DatasetProvenanceDTO, HTTPValidationError]]
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
) -> Optional[Union[DatasetProvenanceDTO, HTTPValidationError]]:
    """What wrote a dataset: its producer run, trace and span, and the datasets that run read

     One hop. ``producer`` is the run row (simulation, analysis or ParCa) with its ``source``
    and, when the run was traced, its ``hpcrun_id``/``trace_id``; ``span`` is the span the
    ``artifact.written`` event came from (walk-found datasets have none); ``inputs`` are
    registered datasets the producer's declared source names by ``uri``.

    Args:
        id (int): Database ID of the dataset

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[DatasetProvenanceDTO, HTTPValidationError]
    """

    return sync_detailed(
        id=id,
        client=client,
    ).parsed


async def asyncio_detailed(
    id: int,
    *,
    client: Union[AuthenticatedClient, Client],
) -> Response[Union[DatasetProvenanceDTO, HTTPValidationError]]:
    """What wrote a dataset: its producer run, trace and span, and the datasets that run read

     One hop. ``producer`` is the run row (simulation, analysis or ParCa) with its ``source``
    and, when the run was traced, its ``hpcrun_id``/``trace_id``; ``span`` is the span the
    ``artifact.written`` event came from (walk-found datasets have none); ``inputs`` are
    registered datasets the producer's declared source names by ``uri``.

    Args:
        id (int): Database ID of the dataset

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[DatasetProvenanceDTO, HTTPValidationError]]
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
) -> Optional[Union[DatasetProvenanceDTO, HTTPValidationError]]:
    """What wrote a dataset: its producer run, trace and span, and the datasets that run read

     One hop. ``producer`` is the run row (simulation, analysis or ParCa) with its ``source``
    and, when the run was traced, its ``hpcrun_id``/``trace_id``; ``span`` is the span the
    ``artifact.written`` event came from (walk-found datasets have none); ``inputs`` are
    registered datasets the producer's declared source names by ``uri``.

    Args:
        id (int): Database ID of the dataset

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[DatasetProvenanceDTO, HTTPValidationError]
    """

    return (
        await asyncio_detailed(
            id=id,
            client=client,
        )
    ).parsed
