from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.biomodels_run_request import BiomodelsRunRequest
from ...models.biomodels_run_result import BiomodelsRunResult
from ...models.http_validation_error import HTTPValidationError
from ...types import Response


def _get_kwargs(
    *,
    body: BiomodelsRunRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/compose/v1/biomodels/run",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[BiomodelsRunResult, HTTPValidationError]]:
    if response.status_code == 200:
        response_200 = BiomodelsRunResult.from_dict(response.json())

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
) -> Response[Union[BiomodelsRunResult, HTTPValidationError]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: Union[AuthenticatedClient, Client],
    body: BiomodelsRunRequest,
) -> Response[Union[BiomodelsRunResult, HTTPValidationError]]:
    """Run one or more BioModels through one or more simulators

     Single entry point for BioModels runs — subsumes the former
    single/batch/audit/regression endpoints.

    - ``model_ids`` (or the first ``n_models``) selects which models to run.
    - ``simulators``: one runs each model on that simulator; several wire all of
      them into one PB document per model for cross-validation.

    Each model is submitted independently; one that fails to load or submit is
    collected in ``failed`` rather than aborting the run.

    Args:
        body (BiomodelsRunRequest): One request shape for every BioModels run — subsumes the
            former
            single/batch/audit/regression endpoints. Cardinality comes from
            ``model_ids``/``n_models``; per-model cross-validation comes from listing
            more than one simulator.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[BiomodelsRunResult, HTTPValidationError]]
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
    body: BiomodelsRunRequest,
) -> Optional[Union[BiomodelsRunResult, HTTPValidationError]]:
    """Run one or more BioModels through one or more simulators

     Single entry point for BioModels runs — subsumes the former
    single/batch/audit/regression endpoints.

    - ``model_ids`` (or the first ``n_models``) selects which models to run.
    - ``simulators``: one runs each model on that simulator; several wire all of
      them into one PB document per model for cross-validation.

    Each model is submitted independently; one that fails to load or submit is
    collected in ``failed`` rather than aborting the run.

    Args:
        body (BiomodelsRunRequest): One request shape for every BioModels run — subsumes the
            former
            single/batch/audit/regression endpoints. Cardinality comes from
            ``model_ids``/``n_models``; per-model cross-validation comes from listing
            more than one simulator.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[BiomodelsRunResult, HTTPValidationError]
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: Union[AuthenticatedClient, Client],
    body: BiomodelsRunRequest,
) -> Response[Union[BiomodelsRunResult, HTTPValidationError]]:
    """Run one or more BioModels through one or more simulators

     Single entry point for BioModels runs — subsumes the former
    single/batch/audit/regression endpoints.

    - ``model_ids`` (or the first ``n_models``) selects which models to run.
    - ``simulators``: one runs each model on that simulator; several wire all of
      them into one PB document per model for cross-validation.

    Each model is submitted independently; one that fails to load or submit is
    collected in ``failed`` rather than aborting the run.

    Args:
        body (BiomodelsRunRequest): One request shape for every BioModels run — subsumes the
            former
            single/batch/audit/regression endpoints. Cardinality comes from
            ``model_ids``/``n_models``; per-model cross-validation comes from listing
            more than one simulator.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[BiomodelsRunResult, HTTPValidationError]]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: Union[AuthenticatedClient, Client],
    body: BiomodelsRunRequest,
) -> Optional[Union[BiomodelsRunResult, HTTPValidationError]]:
    """Run one or more BioModels through one or more simulators

     Single entry point for BioModels runs — subsumes the former
    single/batch/audit/regression endpoints.

    - ``model_ids`` (or the first ``n_models``) selects which models to run.
    - ``simulators``: one runs each model on that simulator; several wire all of
      them into one PB document per model for cross-validation.

    Each model is submitted independently; one that fails to load or submit is
    collected in ``failed`` rather than aborting the run.

    Args:
        body (BiomodelsRunRequest): One request shape for every BioModels run — subsumes the
            former
            single/batch/audit/regression endpoints. Cardinality comes from
            ``model_ids``/``n_models``; per-model cross-validation comes from listing
            more than one simulator.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[BiomodelsRunResult, HTTPValidationError]
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
