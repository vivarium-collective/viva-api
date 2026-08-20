from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.server_capabilities import ServerCapabilities
from ...types import Response


def _get_kwargs() -> dict[str, Any]:
    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/core/v1/capabilities",
    }

    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[ServerCapabilities]:
    if response.status_code == 200:
        response_200 = ServerCapabilities.from_dict(response.json())

        return response_200
    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Response[ServerCapabilities]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: Union[AuthenticatedClient, Client],
) -> Response[ServerCapabilities]:
    r"""What this deployment can actually do (for client feature detection)

     Advertise this deployment's capabilities as data, for clients to feature-detect against.

    Clients branch on membership in ``capabilities`` -- never on ``version``.
    ``version`` is returned for humans, logs and bug reports only.

    Comparing versions is wrong here for two reasons this project has already
    hit: a deployment can run an image built from an unmerged branch that no
    version ordering describes (production, 2026-08-19), and having the code is
    not the same as being able to use it -- several capabilities are gated on
    deployment configuration as well. Each entry means \"this deployment, right
    now, can genuinely serve this\".

    Unrecognised names must be ignored; an absent name means \"not available
    here\", never \"unknown\". See ``viva_api.common.capabilities`` for the naming
    contract.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ServerCapabilities]
    """

    kwargs = _get_kwargs()

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: Union[AuthenticatedClient, Client],
) -> Optional[ServerCapabilities]:
    r"""What this deployment can actually do (for client feature detection)

     Advertise this deployment's capabilities as data, for clients to feature-detect against.

    Clients branch on membership in ``capabilities`` -- never on ``version``.
    ``version`` is returned for humans, logs and bug reports only.

    Comparing versions is wrong here for two reasons this project has already
    hit: a deployment can run an image built from an unmerged branch that no
    version ordering describes (production, 2026-08-19), and having the code is
    not the same as being able to use it -- several capabilities are gated on
    deployment configuration as well. Each entry means \"this deployment, right
    now, can genuinely serve this\".

    Unrecognised names must be ignored; an absent name means \"not available
    here\", never \"unknown\". See ``viva_api.common.capabilities`` for the naming
    contract.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ServerCapabilities
    """

    return sync_detailed(
        client=client,
    ).parsed


async def asyncio_detailed(
    *,
    client: Union[AuthenticatedClient, Client],
) -> Response[ServerCapabilities]:
    r"""What this deployment can actually do (for client feature detection)

     Advertise this deployment's capabilities as data, for clients to feature-detect against.

    Clients branch on membership in ``capabilities`` -- never on ``version``.
    ``version`` is returned for humans, logs and bug reports only.

    Comparing versions is wrong here for two reasons this project has already
    hit: a deployment can run an image built from an unmerged branch that no
    version ordering describes (production, 2026-08-19), and having the code is
    not the same as being able to use it -- several capabilities are gated on
    deployment configuration as well. Each entry means \"this deployment, right
    now, can genuinely serve this\".

    Unrecognised names must be ignored; an absent name means \"not available
    here\", never \"unknown\". See ``viva_api.common.capabilities`` for the naming
    contract.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ServerCapabilities]
    """

    kwargs = _get_kwargs()

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: Union[AuthenticatedClient, Client],
) -> Optional[ServerCapabilities]:
    r"""What this deployment can actually do (for client feature detection)

     Advertise this deployment's capabilities as data, for clients to feature-detect against.

    Clients branch on membership in ``capabilities`` -- never on ``version``.
    ``version`` is returned for humans, logs and bug reports only.

    Comparing versions is wrong here for two reasons this project has already
    hit: a deployment can run an image built from an unmerged branch that no
    version ordering describes (production, 2026-08-19), and having the code is
    not the same as being able to use it -- several capabilities are gated on
    deployment configuration as well. Each entry means \"this deployment, right
    now, can genuinely serve this\".

    Unrecognised names must be ignored; an absent name means \"not available
    here\", never \"unknown\". See ``viva_api.common.capabilities`` for the naming
    contract.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ServerCapabilities
    """

    return (
        await asyncio_detailed(
            client=client,
        )
    ).parsed
