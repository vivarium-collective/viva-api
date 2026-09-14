from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.http_validation_error import HTTPValidationError
from ...types import UNSET, Response


def _get_kwargs(
    id: int,
    *,
    analysis: str,
    path: str,
) -> dict[str, Any]:
    params: dict[str, Any] = {}

    params["analysis"] = analysis

    params["path"] = path

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": f"/api/v1/simulations/{id}/analysis-figure",
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
    analysis: str,
    path: str,
) -> Response[Union[Any, HTTPValidationError]]:
    """Fetch one rendered analysis artifact (a viz/ figure or ptools/ table) by relative path.

     Return a single rendered artifact's bytes with a content-type by extension.

    ``path`` is confined to the analysis prefix (must start with ``viz/`` or
    ``ptools/``; no ``..``); its S3 location is resolved server-side, so the caller
    never handles a raw S3 uri. 400 on a bad path, 404 on an unknown
    simulation/analysis or missing object.

    Args:
        id (int): Database ID of the simulation
        analysis (str): Analysis directory name (e.g. analysis-ptools-multiseed)
        path (str): Artifact path relative to the analysis dir, e.g. viz/foo.html

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[Any, HTTPValidationError]]
    """

    kwargs = _get_kwargs(
        id=id,
        analysis=analysis,
        path=path,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    id: int,
    *,
    client: Union[AuthenticatedClient, Client],
    analysis: str,
    path: str,
) -> Optional[Union[Any, HTTPValidationError]]:
    """Fetch one rendered analysis artifact (a viz/ figure or ptools/ table) by relative path.

     Return a single rendered artifact's bytes with a content-type by extension.

    ``path`` is confined to the analysis prefix (must start with ``viz/`` or
    ``ptools/``; no ``..``); its S3 location is resolved server-side, so the caller
    never handles a raw S3 uri. 400 on a bad path, 404 on an unknown
    simulation/analysis or missing object.

    Args:
        id (int): Database ID of the simulation
        analysis (str): Analysis directory name (e.g. analysis-ptools-multiseed)
        path (str): Artifact path relative to the analysis dir, e.g. viz/foo.html

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[Any, HTTPValidationError]
    """

    return sync_detailed(
        id=id,
        client=client,
        analysis=analysis,
        path=path,
    ).parsed


async def asyncio_detailed(
    id: int,
    *,
    client: Union[AuthenticatedClient, Client],
    analysis: str,
    path: str,
) -> Response[Union[Any, HTTPValidationError]]:
    """Fetch one rendered analysis artifact (a viz/ figure or ptools/ table) by relative path.

     Return a single rendered artifact's bytes with a content-type by extension.

    ``path`` is confined to the analysis prefix (must start with ``viz/`` or
    ``ptools/``; no ``..``); its S3 location is resolved server-side, so the caller
    never handles a raw S3 uri. 400 on a bad path, 404 on an unknown
    simulation/analysis or missing object.

    Args:
        id (int): Database ID of the simulation
        analysis (str): Analysis directory name (e.g. analysis-ptools-multiseed)
        path (str): Artifact path relative to the analysis dir, e.g. viz/foo.html

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[Any, HTTPValidationError]]
    """

    kwargs = _get_kwargs(
        id=id,
        analysis=analysis,
        path=path,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    id: int,
    *,
    client: Union[AuthenticatedClient, Client],
    analysis: str,
    path: str,
) -> Optional[Union[Any, HTTPValidationError]]:
    """Fetch one rendered analysis artifact (a viz/ figure or ptools/ table) by relative path.

     Return a single rendered artifact's bytes with a content-type by extension.

    ``path`` is confined to the analysis prefix (must start with ``viz/`` or
    ``ptools/``; no ``..``); its S3 location is resolved server-side, so the caller
    never handles a raw S3 uri. 400 on a bad path, 404 on an unknown
    simulation/analysis or missing object.

    Args:
        id (int): Database ID of the simulation
        analysis (str): Analysis directory name (e.g. analysis-ptools-multiseed)
        path (str): Artifact path relative to the analysis dir, e.g. viz/foo.html

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
            analysis=analysis,
            path=path,
        )
    ).parsed
