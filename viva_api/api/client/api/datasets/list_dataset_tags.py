from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.http_validation_error import HTTPValidationError
from ...models.list_dataset_tags_response_list_dataset_tags import ListDatasetTagsResponseListDatasetTags
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    kind: Union[None, Unset, str] = UNSET,
) -> dict[str, Any]:
    params: dict[str, Any] = {}

    json_kind: Union[None, Unset, str]
    if isinstance(kind, Unset):
        json_kind = UNSET
    else:
        json_kind = kind
    params["kind"] = json_kind

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/api/v1/datasets/tags",
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[HTTPValidationError, ListDatasetTagsResponseListDatasetTags]]:
    if response.status_code == 200:
        response_200 = ListDatasetTagsResponseListDatasetTags.from_dict(response.json())

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
) -> Response[Union[HTTPValidationError, ListDatasetTagsResponseListDatasetTags]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: Union[AuthenticatedClient, Client],
    kind: Union[None, Unset, str] = UNSET,
) -> Response[Union[HTTPValidationError, ListDatasetTagsResponseListDatasetTags]]:
    """Every tag present on datasets, with how many datasets carry it

    Args:
        kind (Union[None, Unset, str]): Only datasets of this kind.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[HTTPValidationError, ListDatasetTagsResponseListDatasetTags]]
    """

    kwargs = _get_kwargs(
        kind=kind,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: Union[AuthenticatedClient, Client],
    kind: Union[None, Unset, str] = UNSET,
) -> Optional[Union[HTTPValidationError, ListDatasetTagsResponseListDatasetTags]]:
    """Every tag present on datasets, with how many datasets carry it

    Args:
        kind (Union[None, Unset, str]): Only datasets of this kind.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[HTTPValidationError, ListDatasetTagsResponseListDatasetTags]
    """

    return sync_detailed(
        client=client,
        kind=kind,
    ).parsed


async def asyncio_detailed(
    *,
    client: Union[AuthenticatedClient, Client],
    kind: Union[None, Unset, str] = UNSET,
) -> Response[Union[HTTPValidationError, ListDatasetTagsResponseListDatasetTags]]:
    """Every tag present on datasets, with how many datasets carry it

    Args:
        kind (Union[None, Unset, str]): Only datasets of this kind.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[HTTPValidationError, ListDatasetTagsResponseListDatasetTags]]
    """

    kwargs = _get_kwargs(
        kind=kind,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: Union[AuthenticatedClient, Client],
    kind: Union[None, Unset, str] = UNSET,
) -> Optional[Union[HTTPValidationError, ListDatasetTagsResponseListDatasetTags]]:
    """Every tag present on datasets, with how many datasets carry it

    Args:
        kind (Union[None, Unset, str]): Only datasets of this kind.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[HTTPValidationError, ListDatasetTagsResponseListDatasetTags]
    """

    return (
        await asyncio_detailed(
            client=client,
            kind=kind,
        )
    ).parsed
