from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.http_validation_error import HTTPValidationError
from ...types import UNSET, Response


def _get_kwargs(
    job_name: str,
    *,
    package_path: str,
) -> dict[str, Any]:
    params: dict[str, Any] = {}

    params["package_path"] = package_path

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": f"/env-worker/v1/relay/workers/{job_name}/core-snapshot",
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
    job_name: str,
    *,
    client: Union[AuthenticatedClient, Client],
    package_path: str,
) -> Response[Union[Any, HTTPValidationError]]:
    """Registry snapshot plus the workspace document, for a report render

     `package_path` is REQUIRED rather than defaulted. The worker imports
    `<package_path>.core` and `<package_path>.document`; a default here would
    guess at the caller's workspace and import whatever that guess named.

    Args:
        job_name (str):
        package_path (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[Any, HTTPValidationError]]
    """

    kwargs = _get_kwargs(
        job_name=job_name,
        package_path=package_path,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    job_name: str,
    *,
    client: Union[AuthenticatedClient, Client],
    package_path: str,
) -> Optional[Union[Any, HTTPValidationError]]:
    """Registry snapshot plus the workspace document, for a report render

     `package_path` is REQUIRED rather than defaulted. The worker imports
    `<package_path>.core` and `<package_path>.document`; a default here would
    guess at the caller's workspace and import whatever that guess named.

    Args:
        job_name (str):
        package_path (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[Any, HTTPValidationError]
    """

    return sync_detailed(
        job_name=job_name,
        client=client,
        package_path=package_path,
    ).parsed


async def asyncio_detailed(
    job_name: str,
    *,
    client: Union[AuthenticatedClient, Client],
    package_path: str,
) -> Response[Union[Any, HTTPValidationError]]:
    """Registry snapshot plus the workspace document, for a report render

     `package_path` is REQUIRED rather than defaulted. The worker imports
    `<package_path>.core` and `<package_path>.document`; a default here would
    guess at the caller's workspace and import whatever that guess named.

    Args:
        job_name (str):
        package_path (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[Any, HTTPValidationError]]
    """

    kwargs = _get_kwargs(
        job_name=job_name,
        package_path=package_path,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    job_name: str,
    *,
    client: Union[AuthenticatedClient, Client],
    package_path: str,
) -> Optional[Union[Any, HTTPValidationError]]:
    """Registry snapshot plus the workspace document, for a report render

     `package_path` is REQUIRED rather than defaulted. The worker imports
    `<package_path>.core` and `<package_path>.document`; a default here would
    guess at the caller's workspace and import whatever that guess named.

    Args:
        job_name (str):
        package_path (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[Any, HTTPValidationError]
    """

    return (
        await asyncio_detailed(
            job_name=job_name,
            client=client,
            package_path=package_path,
        )
    ).parsed
