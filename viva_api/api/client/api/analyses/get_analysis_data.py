from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.http_validation_error import HTTPValidationError
from ...models.tsv_output_file import TsvOutputFile
from ...types import UNSET, Response, Unset


def _get_kwargs(
    id: int,
    *,
    view: Union[None, Unset, str] = UNSET,
    protocol: Union[None, Unset, str] = UNSET,
    variant: Union[None, Unset, int] = UNSET,
    seed: Union[None, Unset, int] = UNSET,
    generation: Union[None, Unset, int] = UNSET,
) -> dict[str, Any]:
    params: dict[str, Any] = {}

    json_view: Union[None, Unset, str]
    if isinstance(view, Unset):
        json_view = UNSET
    else:
        json_view = view
    params["view"] = json_view

    json_protocol: Union[None, Unset, str]
    if isinstance(protocol, Unset):
        json_protocol = UNSET
    else:
        json_protocol = protocol
    params["protocol"] = json_protocol

    json_variant: Union[None, Unset, int]
    if isinstance(variant, Unset):
        json_variant = UNSET
    else:
        json_variant = variant
    params["variant"] = json_variant

    json_seed: Union[None, Unset, int]
    if isinstance(seed, Unset):
        json_seed = UNSET
    else:
        json_seed = seed
    params["seed"] = json_seed

    json_generation: Union[None, Unset, int]
    if isinstance(generation, Unset):
        json_generation = UNSET
    else:
        json_generation = generation
    params["generation"] = json_generation

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": f"/api/v1/analyses/{id}/data",
        "params": params,
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
    view: Union[None, Unset, str] = UNSET,
    protocol: Union[None, Unset, str] = UNSET,
    variant: Union[None, Unset, int] = UNSET,
    seed: Union[None, Unset, int] = UNSET,
    generation: Union[None, Unset, int] = UNSET,
) -> Response[Union[HTTPValidationError, list["TsvOutputFile"]]]:
    """Retrieve the output files (TSV/CSV/TXT/HTML) of an existing analysis by id

     Pure retrieval of a pre-computed analysis's files by id (never computes).

    Returns the same ``list[TsvOutputFile]`` shape as the legacy ``POST /analyses``. The
    coordinate filters select files before any is downloaded. When the selection holds one file
    per view, ``filename`` is aliased to ``<view>.tsv`` (what an unpatched ptools page expects)
    and ``path`` carries the real name. 409 if the analysis is not READY; 404 if the id is unknown.

    Args:
        id (int): Database ID of the analysis
        view (Union[None, Unset, str]): Only files of this view, e.g. 'ptools_rna'.
        protocol (Union[None, Unset, str]): Only files of this protocol: single, multigeneration,
            multiseed, multidaughter, all.
        variant (Union[None, Unset, int]): Only files of this variant.
        seed (Union[None, Unset, int]): Only files of this lineage seed.
        generation (Union[None, Unset, int]): Only files of this generation.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[HTTPValidationError, list['TsvOutputFile']]]
    """

    kwargs = _get_kwargs(
        id=id,
        view=view,
        protocol=protocol,
        variant=variant,
        seed=seed,
        generation=generation,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    id: int,
    *,
    client: Union[AuthenticatedClient, Client],
    view: Union[None, Unset, str] = UNSET,
    protocol: Union[None, Unset, str] = UNSET,
    variant: Union[None, Unset, int] = UNSET,
    seed: Union[None, Unset, int] = UNSET,
    generation: Union[None, Unset, int] = UNSET,
) -> Optional[Union[HTTPValidationError, list["TsvOutputFile"]]]:
    """Retrieve the output files (TSV/CSV/TXT/HTML) of an existing analysis by id

     Pure retrieval of a pre-computed analysis's files by id (never computes).

    Returns the same ``list[TsvOutputFile]`` shape as the legacy ``POST /analyses``. The
    coordinate filters select files before any is downloaded. When the selection holds one file
    per view, ``filename`` is aliased to ``<view>.tsv`` (what an unpatched ptools page expects)
    and ``path`` carries the real name. 409 if the analysis is not READY; 404 if the id is unknown.

    Args:
        id (int): Database ID of the analysis
        view (Union[None, Unset, str]): Only files of this view, e.g. 'ptools_rna'.
        protocol (Union[None, Unset, str]): Only files of this protocol: single, multigeneration,
            multiseed, multidaughter, all.
        variant (Union[None, Unset, int]): Only files of this variant.
        seed (Union[None, Unset, int]): Only files of this lineage seed.
        generation (Union[None, Unset, int]): Only files of this generation.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[HTTPValidationError, list['TsvOutputFile']]
    """

    return sync_detailed(
        id=id,
        client=client,
        view=view,
        protocol=protocol,
        variant=variant,
        seed=seed,
        generation=generation,
    ).parsed


async def asyncio_detailed(
    id: int,
    *,
    client: Union[AuthenticatedClient, Client],
    view: Union[None, Unset, str] = UNSET,
    protocol: Union[None, Unset, str] = UNSET,
    variant: Union[None, Unset, int] = UNSET,
    seed: Union[None, Unset, int] = UNSET,
    generation: Union[None, Unset, int] = UNSET,
) -> Response[Union[HTTPValidationError, list["TsvOutputFile"]]]:
    """Retrieve the output files (TSV/CSV/TXT/HTML) of an existing analysis by id

     Pure retrieval of a pre-computed analysis's files by id (never computes).

    Returns the same ``list[TsvOutputFile]`` shape as the legacy ``POST /analyses``. The
    coordinate filters select files before any is downloaded. When the selection holds one file
    per view, ``filename`` is aliased to ``<view>.tsv`` (what an unpatched ptools page expects)
    and ``path`` carries the real name. 409 if the analysis is not READY; 404 if the id is unknown.

    Args:
        id (int): Database ID of the analysis
        view (Union[None, Unset, str]): Only files of this view, e.g. 'ptools_rna'.
        protocol (Union[None, Unset, str]): Only files of this protocol: single, multigeneration,
            multiseed, multidaughter, all.
        variant (Union[None, Unset, int]): Only files of this variant.
        seed (Union[None, Unset, int]): Only files of this lineage seed.
        generation (Union[None, Unset, int]): Only files of this generation.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[HTTPValidationError, list['TsvOutputFile']]]
    """

    kwargs = _get_kwargs(
        id=id,
        view=view,
        protocol=protocol,
        variant=variant,
        seed=seed,
        generation=generation,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    id: int,
    *,
    client: Union[AuthenticatedClient, Client],
    view: Union[None, Unset, str] = UNSET,
    protocol: Union[None, Unset, str] = UNSET,
    variant: Union[None, Unset, int] = UNSET,
    seed: Union[None, Unset, int] = UNSET,
    generation: Union[None, Unset, int] = UNSET,
) -> Optional[Union[HTTPValidationError, list["TsvOutputFile"]]]:
    """Retrieve the output files (TSV/CSV/TXT/HTML) of an existing analysis by id

     Pure retrieval of a pre-computed analysis's files by id (never computes).

    Returns the same ``list[TsvOutputFile]`` shape as the legacy ``POST /analyses``. The
    coordinate filters select files before any is downloaded. When the selection holds one file
    per view, ``filename`` is aliased to ``<view>.tsv`` (what an unpatched ptools page expects)
    and ``path`` carries the real name. 409 if the analysis is not READY; 404 if the id is unknown.

    Args:
        id (int): Database ID of the analysis
        view (Union[None, Unset, str]): Only files of this view, e.g. 'ptools_rna'.
        protocol (Union[None, Unset, str]): Only files of this protocol: single, multigeneration,
            multiseed, multidaughter, all.
        variant (Union[None, Unset, int]): Only files of this variant.
        seed (Union[None, Unset, int]): Only files of this lineage seed.
        generation (Union[None, Unset, int]): Only files of this generation.

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
            view=view,
            protocol=protocol,
            variant=variant,
            seed=seed,
            generation=generation,
        )
    ).parsed
