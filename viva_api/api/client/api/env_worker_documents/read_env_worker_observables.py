from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.composite_selector import CompositeSelector
from ...models.http_validation_error import HTTPValidationError
from ...types import Response


def _get_kwargs(
    job_name: str,
    *,
    body: CompositeSelector,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": f"/env-worker/v1/relay/workers/{job_name}/observables",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
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
    body: CompositeSelector,
) -> Response[Union[Any, HTTPValidationError]]:
    """Observable leaves and catalogs of a composite

    Args:
        job_name (str):
        body (CompositeSelector): A composite given EITHER by `ref` OR inline as `{state,
            schema}`.

            Both forms are real and the worker accepts either; sending neither is the
            mistake worth catching here, because the worker answers it with
            `__not_registered__` -- which reads as "your ref is wrong" to a caller who
            sent no ref at all.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[Any, HTTPValidationError]]
    """

    kwargs = _get_kwargs(
        job_name=job_name,
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    job_name: str,
    *,
    client: Union[AuthenticatedClient, Client],
    body: CompositeSelector,
) -> Optional[Union[Any, HTTPValidationError]]:
    """Observable leaves and catalogs of a composite

    Args:
        job_name (str):
        body (CompositeSelector): A composite given EITHER by `ref` OR inline as `{state,
            schema}`.

            Both forms are real and the worker accepts either; sending neither is the
            mistake worth catching here, because the worker answers it with
            `__not_registered__` -- which reads as "your ref is wrong" to a caller who
            sent no ref at all.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[Any, HTTPValidationError]
    """

    return sync_detailed(
        job_name=job_name,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    job_name: str,
    *,
    client: Union[AuthenticatedClient, Client],
    body: CompositeSelector,
) -> Response[Union[Any, HTTPValidationError]]:
    """Observable leaves and catalogs of a composite

    Args:
        job_name (str):
        body (CompositeSelector): A composite given EITHER by `ref` OR inline as `{state,
            schema}`.

            Both forms are real and the worker accepts either; sending neither is the
            mistake worth catching here, because the worker answers it with
            `__not_registered__` -- which reads as "your ref is wrong" to a caller who
            sent no ref at all.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[Any, HTTPValidationError]]
    """

    kwargs = _get_kwargs(
        job_name=job_name,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    job_name: str,
    *,
    client: Union[AuthenticatedClient, Client],
    body: CompositeSelector,
) -> Optional[Union[Any, HTTPValidationError]]:
    """Observable leaves and catalogs of a composite

    Args:
        job_name (str):
        body (CompositeSelector): A composite given EITHER by `ref` OR inline as `{state,
            schema}`.

            Both forms are real and the worker accepts either; sending neither is the
            mistake worth catching here, because the worker answers it with
            `__not_registered__` -- which reads as "your ref is wrong" to a caller who
            sent no ref at all.

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
            body=body,
        )
    ).parsed
