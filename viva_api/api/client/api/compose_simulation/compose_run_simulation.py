from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.body_compose_run_simulation import BodyComposeRunSimulation
from ...models.compose_simulation_experiment import ComposeSimulationExperiment
from ...models.http_validation_error import HTTPValidationError
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    body: BodyComposeRunSimulation,
    interval_time: Union[Unset, float] = 1.0,
    batch_submission: Union[Unset, bool] = False,
    extra_pip_deps: Union[None, Unset, list[str]] = UNSET,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    params: dict[str, Any] = {}

    params["interval_time"] = interval_time

    params["batch_submission"] = batch_submission

    json_extra_pip_deps: Union[None, Unset, list[str]]
    if isinstance(extra_pip_deps, Unset):
        json_extra_pip_deps = UNSET
    elif isinstance(extra_pip_deps, list):
        json_extra_pip_deps = extra_pip_deps

    else:
        json_extra_pip_deps = extra_pip_deps
    params["extra_pip_deps"] = json_extra_pip_deps

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/compose/v1/simulation/run",
        "params": params,
    }

    _kwargs["files"] = body.to_multipart()

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[ComposeSimulationExperiment, HTTPValidationError]]:
    if response.status_code == 200:
        response_200 = ComposeSimulationExperiment.from_dict(response.json())

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
) -> Response[Union[ComposeSimulationExperiment, HTTPValidationError]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: Union[AuthenticatedClient, Client],
    body: BodyComposeRunSimulation,
    interval_time: Union[Unset, float] = 1.0,
    batch_submission: Union[Unset, bool] = False,
    extra_pip_deps: Union[None, Unset, list[str]] = UNSET,
) -> Response[Union[ComposeSimulationExperiment, HTTPValidationError]]:
    """Run a process-bigraph simulation (OMEX/PBG/SBML upload)

    Args:
        interval_time (Union[Unset, float]):  Default: 1.0.
        batch_submission (Union[Unset, bool]):  Default: False.
        extra_pip_deps (Union[None, Unset, list[str]]):
        body (BodyComposeRunSimulation):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ComposeSimulationExperiment, HTTPValidationError]]
    """

    kwargs = _get_kwargs(
        body=body,
        interval_time=interval_time,
        batch_submission=batch_submission,
        extra_pip_deps=extra_pip_deps,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: Union[AuthenticatedClient, Client],
    body: BodyComposeRunSimulation,
    interval_time: Union[Unset, float] = 1.0,
    batch_submission: Union[Unset, bool] = False,
    extra_pip_deps: Union[None, Unset, list[str]] = UNSET,
) -> Optional[Union[ComposeSimulationExperiment, HTTPValidationError]]:
    """Run a process-bigraph simulation (OMEX/PBG/SBML upload)

    Args:
        interval_time (Union[Unset, float]):  Default: 1.0.
        batch_submission (Union[Unset, bool]):  Default: False.
        extra_pip_deps (Union[None, Unset, list[str]]):
        body (BodyComposeRunSimulation):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ComposeSimulationExperiment, HTTPValidationError]
    """

    return sync_detailed(
        client=client,
        body=body,
        interval_time=interval_time,
        batch_submission=batch_submission,
        extra_pip_deps=extra_pip_deps,
    ).parsed


async def asyncio_detailed(
    *,
    client: Union[AuthenticatedClient, Client],
    body: BodyComposeRunSimulation,
    interval_time: Union[Unset, float] = 1.0,
    batch_submission: Union[Unset, bool] = False,
    extra_pip_deps: Union[None, Unset, list[str]] = UNSET,
) -> Response[Union[ComposeSimulationExperiment, HTTPValidationError]]:
    """Run a process-bigraph simulation (OMEX/PBG/SBML upload)

    Args:
        interval_time (Union[Unset, float]):  Default: 1.0.
        batch_submission (Union[Unset, bool]):  Default: False.
        extra_pip_deps (Union[None, Unset, list[str]]):
        body (BodyComposeRunSimulation):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ComposeSimulationExperiment, HTTPValidationError]]
    """

    kwargs = _get_kwargs(
        body=body,
        interval_time=interval_time,
        batch_submission=batch_submission,
        extra_pip_deps=extra_pip_deps,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: Union[AuthenticatedClient, Client],
    body: BodyComposeRunSimulation,
    interval_time: Union[Unset, float] = 1.0,
    batch_submission: Union[Unset, bool] = False,
    extra_pip_deps: Union[None, Unset, list[str]] = UNSET,
) -> Optional[Union[ComposeSimulationExperiment, HTTPValidationError]]:
    """Run a process-bigraph simulation (OMEX/PBG/SBML upload)

    Args:
        interval_time (Union[Unset, float]):  Default: 1.0.
        batch_submission (Union[Unset, bool]):  Default: False.
        extra_pip_deps (Union[None, Unset, list[str]]):
        body (BodyComposeRunSimulation):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ComposeSimulationExperiment, HTTPValidationError]
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
            interval_time=interval_time,
            batch_submission=batch_submission,
            extra_pip_deps=extra_pip_deps,
        )
    ).parsed
