from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.http_validation_error import HTTPValidationError
from ...models.new_gene_cache_job import NewGeneCacheJob
from ...models.new_gene_cache_request import NewGeneCacheRequest
from ...types import Response


def _get_kwargs(
    *,
    body: NewGeneCacheRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/api/v1/parca/new-gene-cache",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[HTTPValidationError, NewGeneCacheJob]]:
    if response.status_code == 200:
        response_200 = NewGeneCacheJob.from_dict(response.json())

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
) -> Response[Union[HTTPValidationError, NewGeneCacheJob]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: Union[AuthenticatedClient, Client],
    body: NewGeneCacheRequest,
) -> Response[Union[HTTPValidationError, NewGeneCacheJob]]:
    """Stamp an induction level onto a completed ParCa dataset's cache (backlog item 105)

    Args:
        body (NewGeneCacheRequest): Backlog item 105: stamp an induction level onto a COMPLETED
            ParCa
            dataset's cache (``scripts/build_new_gene_cache.py``, the "other half" of
            ``new_genes`` presence/absence -- see ``SimulationServiceRay.
            submit_new_gene_cache_job``). Ray/Batch backend only; the source dataset's
            own request must have set ``parca_options.new_genes`` (an all-zero-
            expression source has nothing to induce -- not re-validated here, same
            pure-passthrough philosophy as ``injected_processes``/``variants``).

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[HTTPValidationError, NewGeneCacheJob]]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: Union[AuthenticatedClient, Client],
    body: NewGeneCacheRequest,
) -> Optional[Union[HTTPValidationError, NewGeneCacheJob]]:
    """Stamp an induction level onto a completed ParCa dataset's cache (backlog item 105)

    Args:
        body (NewGeneCacheRequest): Backlog item 105: stamp an induction level onto a COMPLETED
            ParCa
            dataset's cache (``scripts/build_new_gene_cache.py``, the "other half" of
            ``new_genes`` presence/absence -- see ``SimulationServiceRay.
            submit_new_gene_cache_job``). Ray/Batch backend only; the source dataset's
            own request must have set ``parca_options.new_genes`` (an all-zero-
            expression source has nothing to induce -- not re-validated here, same
            pure-passthrough philosophy as ``injected_processes``/``variants``).

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[HTTPValidationError, NewGeneCacheJob]
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: Union[AuthenticatedClient, Client],
    body: NewGeneCacheRequest,
) -> Response[Union[HTTPValidationError, NewGeneCacheJob]]:
    """Stamp an induction level onto a completed ParCa dataset's cache (backlog item 105)

    Args:
        body (NewGeneCacheRequest): Backlog item 105: stamp an induction level onto a COMPLETED
            ParCa
            dataset's cache (``scripts/build_new_gene_cache.py``, the "other half" of
            ``new_genes`` presence/absence -- see ``SimulationServiceRay.
            submit_new_gene_cache_job``). Ray/Batch backend only; the source dataset's
            own request must have set ``parca_options.new_genes`` (an all-zero-
            expression source has nothing to induce -- not re-validated here, same
            pure-passthrough philosophy as ``injected_processes``/``variants``).

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[HTTPValidationError, NewGeneCacheJob]]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: Union[AuthenticatedClient, Client],
    body: NewGeneCacheRequest,
) -> Optional[Union[HTTPValidationError, NewGeneCacheJob]]:
    """Stamp an induction level onto a completed ParCa dataset's cache (backlog item 105)

    Args:
        body (NewGeneCacheRequest): Backlog item 105: stamp an induction level onto a COMPLETED
            ParCa
            dataset's cache (``scripts/build_new_gene_cache.py``, the "other half" of
            ``new_genes`` presence/absence -- see ``SimulationServiceRay.
            submit_new_gene_cache_job``). Ray/Batch backend only; the source dataset's
            own request must have set ``parca_options.new_genes`` (an all-zero-
            expression source has nothing to induce -- not re-validated here, same
            pure-passthrough philosophy as ``injected_processes``/``variants``).

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[HTTPValidationError, NewGeneCacheJob]
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
