from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.analysis_options import AnalysisOptions
from ...models.http_validation_error import HTTPValidationError
from ...models.run_ecoli_simulation_new_composite_type_0 import RunEcoliSimulationNewCompositeType0
from ...models.run_ecoli_simulation_new_vecoli_source_type_0 import RunEcoliSimulationNewVecoliSourceType0
from ...models.simulation import Simulation
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    body: Union["AnalysisOptions", None],
    simulator_id: int,
    experiment_id: Union[None, Unset, str] = UNSET,
    simulation_config_filename: Union[Unset, str] = "api_simulation_default.json",
    num_generations: Union[None, Unset, int] = UNSET,
    num_seeds: Union[None, Unset, int] = UNSET,
    composite: Union[None, RunEcoliSimulationNewCompositeType0, Unset] = UNSET,
    condition: Union[None, Unset, str] = UNSET,
    max_generations: Union[None, Unset, int] = UNSET,
    vecoli_source: Union[None, RunEcoliSimulationNewVecoliSourceType0, Unset] = UNSET,
    description: Union[None, Unset, str] = UNSET,
    run_parca: Union[None, Unset, bool] = UNSET,
    observables: Union[None, Unset, list[str]] = UNSET,
    ecoli_sources_uri: Union[None, Unset, str] = UNSET,
    ecoli_sources_overlays: Union[None, Unset, str] = UNSET,
    ecoli_sources_repo_url: Union[None, Unset, str] = UNSET,
    ecoli_sources_ref: Union[None, Unset, str] = UNSET,
    tags: Union[None, Unset, list[str]] = UNSET,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    params: dict[str, Any] = {}

    params["simulator_id"] = simulator_id

    json_experiment_id: Union[None, Unset, str]
    if isinstance(experiment_id, Unset):
        json_experiment_id = UNSET
    else:
        json_experiment_id = experiment_id
    params["experiment_id"] = json_experiment_id

    params["simulation_config_filename"] = simulation_config_filename

    json_num_generations: Union[None, Unset, int]
    if isinstance(num_generations, Unset):
        json_num_generations = UNSET
    else:
        json_num_generations = num_generations
    params["num_generations"] = json_num_generations

    json_num_seeds: Union[None, Unset, int]
    if isinstance(num_seeds, Unset):
        json_num_seeds = UNSET
    else:
        json_num_seeds = num_seeds
    params["num_seeds"] = json_num_seeds

    json_composite: Union[None, Unset, str]
    if isinstance(composite, Unset):
        json_composite = UNSET
    elif isinstance(composite, RunEcoliSimulationNewCompositeType0):
        json_composite = composite.value
    else:
        json_composite = composite
    params["composite"] = json_composite

    json_condition: Union[None, Unset, str]
    if isinstance(condition, Unset):
        json_condition = UNSET
    else:
        json_condition = condition
    params["condition"] = json_condition

    json_max_generations: Union[None, Unset, int]
    if isinstance(max_generations, Unset):
        json_max_generations = UNSET
    else:
        json_max_generations = max_generations
    params["max_generations"] = json_max_generations

    json_vecoli_source: Union[None, Unset, str]
    if isinstance(vecoli_source, Unset):
        json_vecoli_source = UNSET
    elif isinstance(vecoli_source, RunEcoliSimulationNewVecoliSourceType0):
        json_vecoli_source = vecoli_source.value
    else:
        json_vecoli_source = vecoli_source
    params["vecoli_source"] = json_vecoli_source

    json_description: Union[None, Unset, str]
    if isinstance(description, Unset):
        json_description = UNSET
    else:
        json_description = description
    params["description"] = json_description

    json_run_parca: Union[None, Unset, bool]
    if isinstance(run_parca, Unset):
        json_run_parca = UNSET
    else:
        json_run_parca = run_parca
    params["run_parca"] = json_run_parca

    json_observables: Union[None, Unset, list[str]]
    if isinstance(observables, Unset):
        json_observables = UNSET
    elif isinstance(observables, list):
        json_observables = observables

    else:
        json_observables = observables
    params["observables"] = json_observables

    json_ecoli_sources_uri: Union[None, Unset, str]
    if isinstance(ecoli_sources_uri, Unset):
        json_ecoli_sources_uri = UNSET
    else:
        json_ecoli_sources_uri = ecoli_sources_uri
    params["ecoli_sources_uri"] = json_ecoli_sources_uri

    json_ecoli_sources_overlays: Union[None, Unset, str]
    if isinstance(ecoli_sources_overlays, Unset):
        json_ecoli_sources_overlays = UNSET
    else:
        json_ecoli_sources_overlays = ecoli_sources_overlays
    params["ecoli_sources_overlays"] = json_ecoli_sources_overlays

    json_ecoli_sources_repo_url: Union[None, Unset, str]
    if isinstance(ecoli_sources_repo_url, Unset):
        json_ecoli_sources_repo_url = UNSET
    else:
        json_ecoli_sources_repo_url = ecoli_sources_repo_url
    params["ecoli_sources_repo_url"] = json_ecoli_sources_repo_url

    json_ecoli_sources_ref: Union[None, Unset, str]
    if isinstance(ecoli_sources_ref, Unset):
        json_ecoli_sources_ref = UNSET
    else:
        json_ecoli_sources_ref = ecoli_sources_ref
    params["ecoli_sources_ref"] = json_ecoli_sources_ref

    json_tags: Union[None, Unset, list[str]]
    if isinstance(tags, Unset):
        json_tags = UNSET
    elif isinstance(tags, list):
        json_tags = tags

    else:
        json_tags = tags
    params["tags"] = json_tags

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/api/v1/simulations",
        "params": params,
    }

    _kwargs["json"]: Union[None, dict[str, Any]]
    if isinstance(body, AnalysisOptions):
        _kwargs["json"] = body.to_dict()
    else:
        _kwargs["json"] = body

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[HTTPValidationError, Simulation]]:
    if response.status_code == 200:
        response_200 = Simulation.from_dict(response.json())

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
) -> Response[Union[HTTPValidationError, Simulation]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: Union[AuthenticatedClient, Client],
    body: Union["AnalysisOptions", None],
    simulator_id: int,
    experiment_id: Union[None, Unset, str] = UNSET,
    simulation_config_filename: Union[Unset, str] = "api_simulation_default.json",
    num_generations: Union[None, Unset, int] = UNSET,
    num_seeds: Union[None, Unset, int] = UNSET,
    composite: Union[None, RunEcoliSimulationNewCompositeType0, Unset] = UNSET,
    condition: Union[None, Unset, str] = UNSET,
    max_generations: Union[None, Unset, int] = UNSET,
    vecoli_source: Union[None, RunEcoliSimulationNewVecoliSourceType0, Unset] = UNSET,
    description: Union[None, Unset, str] = UNSET,
    run_parca: Union[None, Unset, bool] = UNSET,
    observables: Union[None, Unset, list[str]] = UNSET,
    ecoli_sources_uri: Union[None, Unset, str] = UNSET,
    ecoli_sources_overlays: Union[None, Unset, str] = UNSET,
    ecoli_sources_repo_url: Union[None, Unset, str] = UNSET,
    ecoli_sources_ref: Union[None, Unset, str] = UNSET,
    tags: Union[None, Unset, list[str]] = UNSET,
) -> Response[Union[HTTPValidationError, Simulation]]:
    """[New] Launch a vEcoli simulation workflow (engine/composite, generations, seeds, condition)

     Run a vEcoli simulation workflow with simplified parameters.

    This endpoint reads the workflow configuration from the vEcoli repo on the HPC
    system and allows overriding specific parameters via query params.

    Args:
        simulator_id (int): `database_id` of the simulator object returned by
            /core/v1/simulator/upload
        experiment_id (Union[None, Unset, str]): Unique experiment identifier
        simulation_config_filename (Union[Unset, str]): Config filename in vEcoli/configs/. Use
            GET /simulations/discovery to list available files. Default:
            'api_simulation_default.json'.
        num_generations (Union[None, Unset, int]): Number of generations to simulate
        num_seeds (Union[None, Unset, int]): Number of initial seeds (lineages)
        composite (Union[None, RunEcoliSimulationNewCompositeType0, Unset]): Ray two-engine
            comparison: 'v2ecoli' (ported) or 'vecoli' (imported via build_composite_native). When
            set, runs the comparison ensemble driver instead of the phase0 ensemble.
        condition (Union[None, Unset, str]): Growth condition/media for the comparison run (e.g.
            basal, acetate).
        max_generations (Union[None, Unset, int]): Generations per lineage for the comparison
            ensemble.
        vecoli_source (Union[None, RunEcoliSimulationNewVecoliSourceType0, Unset]): How the
            genuine vEcoli side runs (composite='vecoli' only): 'upstream' (default, ~50 pbg steps) or
            'vivarium-process' (vEcoli as one pbg node with vivarium-core's Engine inside).
        description (Union[None, Unset, str]): Description of the simulation
        run_parca (Union[None, Unset, bool]): If true, run the simulation parameter calculator
            prior to running simulation (re-parameterizes simulation workflow).
        observables (Union[None, Unset, list[str]]): Dot-separated vEcoli output paths to observe.
            E.g. ['bulk', 'listeners.mass.cell_mass']. Maps to engine_process_reports in the vEcoli
            config. If omitted, all outputs are emitted.
        ecoli_sources_uri (Union[None, Unset, str]): S3 URI for the ECOLI_SOURCES env var on the
            simulation container. Set automatically when ecoli_sources_repo_url is provided, or
            manually via the CLI's --sources flag.
        ecoli_sources_overlays (Union[None, Unset, str]): Semicolon-separated overlay manifest
            URIs for ECOLI_SOURCES_OVERLAYS.
        ecoli_sources_repo_url (Union[None, Unset, str]): GitHub repo URL for ecoli-sources data.
            The server downloads and syncs to S3 automatically, then injects ECOLI_SOURCES on the
            container. No AWS CLI needed on the client.
        ecoli_sources_ref (Union[None, Unset, str]): Git ref (branch/tag/commit) for
            ecoli_sources_repo_url. Defaults to 'main'.
        tags (Union[None, Unset, list[str]]): Free-form tags to attach to this simulation for
            later filtering (e.g. 'cd1'). Repeat the param for multiple tags. Tags can also be added
            later via POST /simulations/{id}/tags.
        body (Union['AnalysisOptions', None]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[HTTPValidationError, Simulation]]
    """

    kwargs = _get_kwargs(
        body=body,
        simulator_id=simulator_id,
        experiment_id=experiment_id,
        simulation_config_filename=simulation_config_filename,
        num_generations=num_generations,
        num_seeds=num_seeds,
        composite=composite,
        condition=condition,
        max_generations=max_generations,
        vecoli_source=vecoli_source,
        description=description,
        run_parca=run_parca,
        observables=observables,
        ecoli_sources_uri=ecoli_sources_uri,
        ecoli_sources_overlays=ecoli_sources_overlays,
        ecoli_sources_repo_url=ecoli_sources_repo_url,
        ecoli_sources_ref=ecoli_sources_ref,
        tags=tags,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: Union[AuthenticatedClient, Client],
    body: Union["AnalysisOptions", None],
    simulator_id: int,
    experiment_id: Union[None, Unset, str] = UNSET,
    simulation_config_filename: Union[Unset, str] = "api_simulation_default.json",
    num_generations: Union[None, Unset, int] = UNSET,
    num_seeds: Union[None, Unset, int] = UNSET,
    composite: Union[None, RunEcoliSimulationNewCompositeType0, Unset] = UNSET,
    condition: Union[None, Unset, str] = UNSET,
    max_generations: Union[None, Unset, int] = UNSET,
    vecoli_source: Union[None, RunEcoliSimulationNewVecoliSourceType0, Unset] = UNSET,
    description: Union[None, Unset, str] = UNSET,
    run_parca: Union[None, Unset, bool] = UNSET,
    observables: Union[None, Unset, list[str]] = UNSET,
    ecoli_sources_uri: Union[None, Unset, str] = UNSET,
    ecoli_sources_overlays: Union[None, Unset, str] = UNSET,
    ecoli_sources_repo_url: Union[None, Unset, str] = UNSET,
    ecoli_sources_ref: Union[None, Unset, str] = UNSET,
    tags: Union[None, Unset, list[str]] = UNSET,
) -> Optional[Union[HTTPValidationError, Simulation]]:
    """[New] Launch a vEcoli simulation workflow (engine/composite, generations, seeds, condition)

     Run a vEcoli simulation workflow with simplified parameters.

    This endpoint reads the workflow configuration from the vEcoli repo on the HPC
    system and allows overriding specific parameters via query params.

    Args:
        simulator_id (int): `database_id` of the simulator object returned by
            /core/v1/simulator/upload
        experiment_id (Union[None, Unset, str]): Unique experiment identifier
        simulation_config_filename (Union[Unset, str]): Config filename in vEcoli/configs/. Use
            GET /simulations/discovery to list available files. Default:
            'api_simulation_default.json'.
        num_generations (Union[None, Unset, int]): Number of generations to simulate
        num_seeds (Union[None, Unset, int]): Number of initial seeds (lineages)
        composite (Union[None, RunEcoliSimulationNewCompositeType0, Unset]): Ray two-engine
            comparison: 'v2ecoli' (ported) or 'vecoli' (imported via build_composite_native). When
            set, runs the comparison ensemble driver instead of the phase0 ensemble.
        condition (Union[None, Unset, str]): Growth condition/media for the comparison run (e.g.
            basal, acetate).
        max_generations (Union[None, Unset, int]): Generations per lineage for the comparison
            ensemble.
        vecoli_source (Union[None, RunEcoliSimulationNewVecoliSourceType0, Unset]): How the
            genuine vEcoli side runs (composite='vecoli' only): 'upstream' (default, ~50 pbg steps) or
            'vivarium-process' (vEcoli as one pbg node with vivarium-core's Engine inside).
        description (Union[None, Unset, str]): Description of the simulation
        run_parca (Union[None, Unset, bool]): If true, run the simulation parameter calculator
            prior to running simulation (re-parameterizes simulation workflow).
        observables (Union[None, Unset, list[str]]): Dot-separated vEcoli output paths to observe.
            E.g. ['bulk', 'listeners.mass.cell_mass']. Maps to engine_process_reports in the vEcoli
            config. If omitted, all outputs are emitted.
        ecoli_sources_uri (Union[None, Unset, str]): S3 URI for the ECOLI_SOURCES env var on the
            simulation container. Set automatically when ecoli_sources_repo_url is provided, or
            manually via the CLI's --sources flag.
        ecoli_sources_overlays (Union[None, Unset, str]): Semicolon-separated overlay manifest
            URIs for ECOLI_SOURCES_OVERLAYS.
        ecoli_sources_repo_url (Union[None, Unset, str]): GitHub repo URL for ecoli-sources data.
            The server downloads and syncs to S3 automatically, then injects ECOLI_SOURCES on the
            container. No AWS CLI needed on the client.
        ecoli_sources_ref (Union[None, Unset, str]): Git ref (branch/tag/commit) for
            ecoli_sources_repo_url. Defaults to 'main'.
        tags (Union[None, Unset, list[str]]): Free-form tags to attach to this simulation for
            later filtering (e.g. 'cd1'). Repeat the param for multiple tags. Tags can also be added
            later via POST /simulations/{id}/tags.
        body (Union['AnalysisOptions', None]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[HTTPValidationError, Simulation]
    """

    return sync_detailed(
        client=client,
        body=body,
        simulator_id=simulator_id,
        experiment_id=experiment_id,
        simulation_config_filename=simulation_config_filename,
        num_generations=num_generations,
        num_seeds=num_seeds,
        composite=composite,
        condition=condition,
        max_generations=max_generations,
        vecoli_source=vecoli_source,
        description=description,
        run_parca=run_parca,
        observables=observables,
        ecoli_sources_uri=ecoli_sources_uri,
        ecoli_sources_overlays=ecoli_sources_overlays,
        ecoli_sources_repo_url=ecoli_sources_repo_url,
        ecoli_sources_ref=ecoli_sources_ref,
        tags=tags,
    ).parsed


async def asyncio_detailed(
    *,
    client: Union[AuthenticatedClient, Client],
    body: Union["AnalysisOptions", None],
    simulator_id: int,
    experiment_id: Union[None, Unset, str] = UNSET,
    simulation_config_filename: Union[Unset, str] = "api_simulation_default.json",
    num_generations: Union[None, Unset, int] = UNSET,
    num_seeds: Union[None, Unset, int] = UNSET,
    composite: Union[None, RunEcoliSimulationNewCompositeType0, Unset] = UNSET,
    condition: Union[None, Unset, str] = UNSET,
    max_generations: Union[None, Unset, int] = UNSET,
    vecoli_source: Union[None, RunEcoliSimulationNewVecoliSourceType0, Unset] = UNSET,
    description: Union[None, Unset, str] = UNSET,
    run_parca: Union[None, Unset, bool] = UNSET,
    observables: Union[None, Unset, list[str]] = UNSET,
    ecoli_sources_uri: Union[None, Unset, str] = UNSET,
    ecoli_sources_overlays: Union[None, Unset, str] = UNSET,
    ecoli_sources_repo_url: Union[None, Unset, str] = UNSET,
    ecoli_sources_ref: Union[None, Unset, str] = UNSET,
    tags: Union[None, Unset, list[str]] = UNSET,
) -> Response[Union[HTTPValidationError, Simulation]]:
    """[New] Launch a vEcoli simulation workflow (engine/composite, generations, seeds, condition)

     Run a vEcoli simulation workflow with simplified parameters.

    This endpoint reads the workflow configuration from the vEcoli repo on the HPC
    system and allows overriding specific parameters via query params.

    Args:
        simulator_id (int): `database_id` of the simulator object returned by
            /core/v1/simulator/upload
        experiment_id (Union[None, Unset, str]): Unique experiment identifier
        simulation_config_filename (Union[Unset, str]): Config filename in vEcoli/configs/. Use
            GET /simulations/discovery to list available files. Default:
            'api_simulation_default.json'.
        num_generations (Union[None, Unset, int]): Number of generations to simulate
        num_seeds (Union[None, Unset, int]): Number of initial seeds (lineages)
        composite (Union[None, RunEcoliSimulationNewCompositeType0, Unset]): Ray two-engine
            comparison: 'v2ecoli' (ported) or 'vecoli' (imported via build_composite_native). When
            set, runs the comparison ensemble driver instead of the phase0 ensemble.
        condition (Union[None, Unset, str]): Growth condition/media for the comparison run (e.g.
            basal, acetate).
        max_generations (Union[None, Unset, int]): Generations per lineage for the comparison
            ensemble.
        vecoli_source (Union[None, RunEcoliSimulationNewVecoliSourceType0, Unset]): How the
            genuine vEcoli side runs (composite='vecoli' only): 'upstream' (default, ~50 pbg steps) or
            'vivarium-process' (vEcoli as one pbg node with vivarium-core's Engine inside).
        description (Union[None, Unset, str]): Description of the simulation
        run_parca (Union[None, Unset, bool]): If true, run the simulation parameter calculator
            prior to running simulation (re-parameterizes simulation workflow).
        observables (Union[None, Unset, list[str]]): Dot-separated vEcoli output paths to observe.
            E.g. ['bulk', 'listeners.mass.cell_mass']. Maps to engine_process_reports in the vEcoli
            config. If omitted, all outputs are emitted.
        ecoli_sources_uri (Union[None, Unset, str]): S3 URI for the ECOLI_SOURCES env var on the
            simulation container. Set automatically when ecoli_sources_repo_url is provided, or
            manually via the CLI's --sources flag.
        ecoli_sources_overlays (Union[None, Unset, str]): Semicolon-separated overlay manifest
            URIs for ECOLI_SOURCES_OVERLAYS.
        ecoli_sources_repo_url (Union[None, Unset, str]): GitHub repo URL for ecoli-sources data.
            The server downloads and syncs to S3 automatically, then injects ECOLI_SOURCES on the
            container. No AWS CLI needed on the client.
        ecoli_sources_ref (Union[None, Unset, str]): Git ref (branch/tag/commit) for
            ecoli_sources_repo_url. Defaults to 'main'.
        tags (Union[None, Unset, list[str]]): Free-form tags to attach to this simulation for
            later filtering (e.g. 'cd1'). Repeat the param for multiple tags. Tags can also be added
            later via POST /simulations/{id}/tags.
        body (Union['AnalysisOptions', None]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[HTTPValidationError, Simulation]]
    """

    kwargs = _get_kwargs(
        body=body,
        simulator_id=simulator_id,
        experiment_id=experiment_id,
        simulation_config_filename=simulation_config_filename,
        num_generations=num_generations,
        num_seeds=num_seeds,
        composite=composite,
        condition=condition,
        max_generations=max_generations,
        vecoli_source=vecoli_source,
        description=description,
        run_parca=run_parca,
        observables=observables,
        ecoli_sources_uri=ecoli_sources_uri,
        ecoli_sources_overlays=ecoli_sources_overlays,
        ecoli_sources_repo_url=ecoli_sources_repo_url,
        ecoli_sources_ref=ecoli_sources_ref,
        tags=tags,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: Union[AuthenticatedClient, Client],
    body: Union["AnalysisOptions", None],
    simulator_id: int,
    experiment_id: Union[None, Unset, str] = UNSET,
    simulation_config_filename: Union[Unset, str] = "api_simulation_default.json",
    num_generations: Union[None, Unset, int] = UNSET,
    num_seeds: Union[None, Unset, int] = UNSET,
    composite: Union[None, RunEcoliSimulationNewCompositeType0, Unset] = UNSET,
    condition: Union[None, Unset, str] = UNSET,
    max_generations: Union[None, Unset, int] = UNSET,
    vecoli_source: Union[None, RunEcoliSimulationNewVecoliSourceType0, Unset] = UNSET,
    description: Union[None, Unset, str] = UNSET,
    run_parca: Union[None, Unset, bool] = UNSET,
    observables: Union[None, Unset, list[str]] = UNSET,
    ecoli_sources_uri: Union[None, Unset, str] = UNSET,
    ecoli_sources_overlays: Union[None, Unset, str] = UNSET,
    ecoli_sources_repo_url: Union[None, Unset, str] = UNSET,
    ecoli_sources_ref: Union[None, Unset, str] = UNSET,
    tags: Union[None, Unset, list[str]] = UNSET,
) -> Optional[Union[HTTPValidationError, Simulation]]:
    """[New] Launch a vEcoli simulation workflow (engine/composite, generations, seeds, condition)

     Run a vEcoli simulation workflow with simplified parameters.

    This endpoint reads the workflow configuration from the vEcoli repo on the HPC
    system and allows overriding specific parameters via query params.

    Args:
        simulator_id (int): `database_id` of the simulator object returned by
            /core/v1/simulator/upload
        experiment_id (Union[None, Unset, str]): Unique experiment identifier
        simulation_config_filename (Union[Unset, str]): Config filename in vEcoli/configs/. Use
            GET /simulations/discovery to list available files. Default:
            'api_simulation_default.json'.
        num_generations (Union[None, Unset, int]): Number of generations to simulate
        num_seeds (Union[None, Unset, int]): Number of initial seeds (lineages)
        composite (Union[None, RunEcoliSimulationNewCompositeType0, Unset]): Ray two-engine
            comparison: 'v2ecoli' (ported) or 'vecoli' (imported via build_composite_native). When
            set, runs the comparison ensemble driver instead of the phase0 ensemble.
        condition (Union[None, Unset, str]): Growth condition/media for the comparison run (e.g.
            basal, acetate).
        max_generations (Union[None, Unset, int]): Generations per lineage for the comparison
            ensemble.
        vecoli_source (Union[None, RunEcoliSimulationNewVecoliSourceType0, Unset]): How the
            genuine vEcoli side runs (composite='vecoli' only): 'upstream' (default, ~50 pbg steps) or
            'vivarium-process' (vEcoli as one pbg node with vivarium-core's Engine inside).
        description (Union[None, Unset, str]): Description of the simulation
        run_parca (Union[None, Unset, bool]): If true, run the simulation parameter calculator
            prior to running simulation (re-parameterizes simulation workflow).
        observables (Union[None, Unset, list[str]]): Dot-separated vEcoli output paths to observe.
            E.g. ['bulk', 'listeners.mass.cell_mass']. Maps to engine_process_reports in the vEcoli
            config. If omitted, all outputs are emitted.
        ecoli_sources_uri (Union[None, Unset, str]): S3 URI for the ECOLI_SOURCES env var on the
            simulation container. Set automatically when ecoli_sources_repo_url is provided, or
            manually via the CLI's --sources flag.
        ecoli_sources_overlays (Union[None, Unset, str]): Semicolon-separated overlay manifest
            URIs for ECOLI_SOURCES_OVERLAYS.
        ecoli_sources_repo_url (Union[None, Unset, str]): GitHub repo URL for ecoli-sources data.
            The server downloads and syncs to S3 automatically, then injects ECOLI_SOURCES on the
            container. No AWS CLI needed on the client.
        ecoli_sources_ref (Union[None, Unset, str]): Git ref (branch/tag/commit) for
            ecoli_sources_repo_url. Defaults to 'main'.
        tags (Union[None, Unset, list[str]]): Free-form tags to attach to this simulation for
            later filtering (e.g. 'cd1'). Repeat the param for multiple tags. Tags can also be added
            later via POST /simulations/{id}/tags.
        body (Union['AnalysisOptions', None]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[HTTPValidationError, Simulation]
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
            simulator_id=simulator_id,
            experiment_id=experiment_id,
            simulation_config_filename=simulation_config_filename,
            num_generations=num_generations,
            num_seeds=num_seeds,
            composite=composite,
            condition=condition,
            max_generations=max_generations,
            vecoli_source=vecoli_source,
            description=description,
            run_parca=run_parca,
            observables=observables,
            ecoli_sources_uri=ecoli_sources_uri,
            ecoli_sources_overlays=ecoli_sources_overlays,
            ecoli_sources_repo_url=ecoli_sources_repo_url,
            ecoli_sources_ref=ecoli_sources_ref,
            tags=tags,
        )
    ).parsed
