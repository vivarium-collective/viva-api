from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.compose_simulation_experiment import ComposeSimulationExperiment
from ...models.http_validation_error import HTTPValidationError
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    duration: Union[Unset, float] = 60.0,
    seed: Union[Unset, int] = 0,
    interval: Union[Unset, float] = 1.0,
    features: Union[Unset, str] = "[]",
    cache_dir: Union[Unset, str] = "/out/cache",
) -> dict[str, Any]:
    params: dict[str, Any] = {}

    params["duration"] = duration

    params["seed"] = seed

    params["interval"] = interval

    params["features"] = features

    params["cache_dir"] = cache_dir

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/compose/v1/curated/ecoli",
        "params": params,
    }

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
    duration: Union[Unset, float] = 60.0,
    seed: Union[Unset, int] = 0,
    interval: Union[Unset, float] = 1.0,
    features: Union[Unset, str] = "[]",
    cache_dir: Union[Unset, str] = "/out/cache",
) -> Response[Union[ComposeSimulationExperiment, HTTPValidationError]]:
    """Run a v2ecoli whole-cell simulation via process-bigraph

     Run a v2ecoli whole-cell E. coli simulation.

    Unlike Copasi/Tellurium, v2ecoli does not require an SBML upload.
    The biological model is pre-computed in the ParCa cache and the
    55 biological processes are composed at runtime via process-bigraph.

    Args:
        duration (Union[Unset, float]): Simulation duration in seconds. Default: 60.0.
        seed (Union[Unset, int]): Random seed for stochastic processes. Default: 0.
        interval (Union[Unset, float]): Execution interval (timestep) in seconds. Default: 1.0.
        features (Union[Unset, str]): JSON list of feature modules, e.g. '["ppgpp_regulation"]'
            Default: '[]'.
        cache_dir (Union[Unset, str]): Absolute path to pre-computed ParCa cache inside container.
            Default: '/out/cache'.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ComposeSimulationExperiment, HTTPValidationError]]
    """

    kwargs = _get_kwargs(
        duration=duration,
        seed=seed,
        interval=interval,
        features=features,
        cache_dir=cache_dir,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: Union[AuthenticatedClient, Client],
    duration: Union[Unset, float] = 60.0,
    seed: Union[Unset, int] = 0,
    interval: Union[Unset, float] = 1.0,
    features: Union[Unset, str] = "[]",
    cache_dir: Union[Unset, str] = "/out/cache",
) -> Optional[Union[ComposeSimulationExperiment, HTTPValidationError]]:
    """Run a v2ecoli whole-cell simulation via process-bigraph

     Run a v2ecoli whole-cell E. coli simulation.

    Unlike Copasi/Tellurium, v2ecoli does not require an SBML upload.
    The biological model is pre-computed in the ParCa cache and the
    55 biological processes are composed at runtime via process-bigraph.

    Args:
        duration (Union[Unset, float]): Simulation duration in seconds. Default: 60.0.
        seed (Union[Unset, int]): Random seed for stochastic processes. Default: 0.
        interval (Union[Unset, float]): Execution interval (timestep) in seconds. Default: 1.0.
        features (Union[Unset, str]): JSON list of feature modules, e.g. '["ppgpp_regulation"]'
            Default: '[]'.
        cache_dir (Union[Unset, str]): Absolute path to pre-computed ParCa cache inside container.
            Default: '/out/cache'.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ComposeSimulationExperiment, HTTPValidationError]
    """

    return sync_detailed(
        client=client,
        duration=duration,
        seed=seed,
        interval=interval,
        features=features,
        cache_dir=cache_dir,
    ).parsed


async def asyncio_detailed(
    *,
    client: Union[AuthenticatedClient, Client],
    duration: Union[Unset, float] = 60.0,
    seed: Union[Unset, int] = 0,
    interval: Union[Unset, float] = 1.0,
    features: Union[Unset, str] = "[]",
    cache_dir: Union[Unset, str] = "/out/cache",
) -> Response[Union[ComposeSimulationExperiment, HTTPValidationError]]:
    """Run a v2ecoli whole-cell simulation via process-bigraph

     Run a v2ecoli whole-cell E. coli simulation.

    Unlike Copasi/Tellurium, v2ecoli does not require an SBML upload.
    The biological model is pre-computed in the ParCa cache and the
    55 biological processes are composed at runtime via process-bigraph.

    Args:
        duration (Union[Unset, float]): Simulation duration in seconds. Default: 60.0.
        seed (Union[Unset, int]): Random seed for stochastic processes. Default: 0.
        interval (Union[Unset, float]): Execution interval (timestep) in seconds. Default: 1.0.
        features (Union[Unset, str]): JSON list of feature modules, e.g. '["ppgpp_regulation"]'
            Default: '[]'.
        cache_dir (Union[Unset, str]): Absolute path to pre-computed ParCa cache inside container.
            Default: '/out/cache'.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[ComposeSimulationExperiment, HTTPValidationError]]
    """

    kwargs = _get_kwargs(
        duration=duration,
        seed=seed,
        interval=interval,
        features=features,
        cache_dir=cache_dir,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: Union[AuthenticatedClient, Client],
    duration: Union[Unset, float] = 60.0,
    seed: Union[Unset, int] = 0,
    interval: Union[Unset, float] = 1.0,
    features: Union[Unset, str] = "[]",
    cache_dir: Union[Unset, str] = "/out/cache",
) -> Optional[Union[ComposeSimulationExperiment, HTTPValidationError]]:
    """Run a v2ecoli whole-cell simulation via process-bigraph

     Run a v2ecoli whole-cell E. coli simulation.

    Unlike Copasi/Tellurium, v2ecoli does not require an SBML upload.
    The biological model is pre-computed in the ParCa cache and the
    55 biological processes are composed at runtime via process-bigraph.

    Args:
        duration (Union[Unset, float]): Simulation duration in seconds. Default: 60.0.
        seed (Union[Unset, int]): Random seed for stochastic processes. Default: 0.
        interval (Union[Unset, float]): Execution interval (timestep) in seconds. Default: 1.0.
        features (Union[Unset, str]): JSON list of feature modules, e.g. '["ppgpp_regulation"]'
            Default: '[]'.
        cache_dir (Union[Unset, str]): Absolute path to pre-computed ParCa cache inside container.
            Default: '/out/cache'.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[ComposeSimulationExperiment, HTTPValidationError]
    """

    return (
        await asyncio_detailed(
            client=client,
            duration=duration,
            seed=seed,
            interval=interval,
            features=features,
            cache_dir=cache_dir,
        )
    ).parsed
