from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.biomodel_simulator import BiomodelSimulator
from ...models.biomodels_audit_result import BiomodelsAuditResult
from ...models.http_validation_error import HTTPValidationError
from ...types import UNSET, Response, Unset


def _get_kwargs(
    biomodel_id: str,
    *,
    simulators: Union[Unset, list[BiomodelSimulator]] = UNSET,
) -> dict[str, Any]:
    params: dict[str, Any] = {}

    json_simulators: Union[Unset, list[str]] = UNSET
    if not isinstance(simulators, Unset):
        json_simulators = []
        for simulators_item_data in simulators:
            simulators_item = simulators_item_data.value
            json_simulators.append(simulators_item)

    params["simulators"] = json_simulators

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": f"/compose/v1/biomodels/{biomodel_id}/audit",
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[BiomodelsAuditResult, HTTPValidationError]]:
    if response.status_code == 200:
        response_200 = BiomodelsAuditResult.from_dict(response.json())

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
) -> Response[Union[BiomodelsAuditResult, HTTPValidationError]]:
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
    simulators: Union[Unset, list[BiomodelSimulator]] = UNSET,
) -> Response[Union[BiomodelsAuditResult, HTTPValidationError]]:
    """Run a BioModel on multiple simulators for cross-validation

    Args:
        biomodel_id (str):
        simulators (Union[Unset, list[BiomodelSimulator]]): Simulators to run. Both are wired into
            a single PB document.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[BiomodelsAuditResult, HTTPValidationError]]
    """

    kwargs = _get_kwargs(
        biomodel_id=biomodel_id,
        simulators=simulators,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    biomodel_id: str,
    *,
    client: Union[AuthenticatedClient, Client],
    simulators: Union[Unset, list[BiomodelSimulator]] = UNSET,
) -> Optional[Union[BiomodelsAuditResult, HTTPValidationError]]:
    """Run a BioModel on multiple simulators for cross-validation

    Args:
        biomodel_id (str):
        simulators (Union[Unset, list[BiomodelSimulator]]): Simulators to run. Both are wired into
            a single PB document.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[BiomodelsAuditResult, HTTPValidationError]
    """

    return sync_detailed(
        biomodel_id=biomodel_id,
        client=client,
        simulators=simulators,
    ).parsed


async def asyncio_detailed(
    biomodel_id: str,
    *,
    client: Union[AuthenticatedClient, Client],
    simulators: Union[Unset, list[BiomodelSimulator]] = UNSET,
) -> Response[Union[BiomodelsAuditResult, HTTPValidationError]]:
    """Run a BioModel on multiple simulators for cross-validation

    Args:
        biomodel_id (str):
        simulators (Union[Unset, list[BiomodelSimulator]]): Simulators to run. Both are wired into
            a single PB document.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[BiomodelsAuditResult, HTTPValidationError]]
    """

    kwargs = _get_kwargs(
        biomodel_id=biomodel_id,
        simulators=simulators,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    biomodel_id: str,
    *,
    client: Union[AuthenticatedClient, Client],
    simulators: Union[Unset, list[BiomodelSimulator]] = UNSET,
) -> Optional[Union[BiomodelsAuditResult, HTTPValidationError]]:
    """Run a BioModel on multiple simulators for cross-validation

    Args:
        biomodel_id (str):
        simulators (Union[Unset, list[BiomodelSimulator]]): Simulators to run. Both are wired into
            a single PB document.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[BiomodelsAuditResult, HTTPValidationError]
    """

    return (
        await asyncio_detailed(
            biomodel_id=biomodel_id,
            client=client,
            simulators=simulators,
        )
    ).parsed
