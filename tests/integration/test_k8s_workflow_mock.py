"""K8s Batch Integration Tests (Mock Mode) — exercises handlers with mocked backends.

Tests the full workflow logic through the handler functions (not the HTTP layer)
with mocked K8s and S3 backends. Always runs — no AWS credentials or cluster
access required.

Run with: uv run pytest tests/integration/test_k8s_workflow_mock.py -v -s

Prerequisites:
- Docker running (for Postgres testcontainer)
"""

import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException
from sqlalchemy import select

import viva_api.dependencies as deps
from tests.fixtures.api_fixtures import SimulatorRepoInfo
from viva_api.common.handlers import simulations as sim_handlers
from viva_api.common.hpc.job_service import JobStatusInfo
from viva_api.common.models import JobBackend, JobId, JobStatus
from viva_api.common.simulator_defaults import RepoUrl
from viva_api.config import ComputeBackend, get_settings
from viva_api.simulation.database_service import DatabaseServiceSQL
from viva_api.simulation.models import (
    AnalysisOptions,
    JobType,
    ParcaDatasetRequest,
    ParcaOptions,
    RepoDiscovery,
    SimulatorVersion,
)
from viva_api.simulation.simulation_service_k8s import SimulationServiceK8s
from viva_api.simulation.simulation_service_ray import SimulationServiceRay
from viva_api.simulation.tables_orm import ORMHpcRun

CONFIG_TEMPLATE = json.dumps({
    "experiment_id": "EXPERIMENT_ID_PLACEHOLDER",
    "generations": 1,
    "n_init_sims": 1,
    "parca_options": {"cpus": 1},
    "analysis_options": {},
    "sim_data_path": "HPC_SIM_BASE_PATH_PLACEHOLDER/default/kb/simData.cPickle",
})


async def _get_or_create_simulator(
    database_service: DatabaseServiceSQL, repo_info: SimulatorRepoInfo
) -> SimulatorVersion:
    for _simulator in await database_service.list_simulators():
        if _simulator.git_commit_hash == repo_info.commit_hash:
            return _simulator
    return await database_service.insert_simulator(
        git_commit_hash=repo_info.commit_hash,
        git_repo_url=repo_info.url,
        git_branch=repo_info.branch,
    )


@pytest.mark.asyncio
async def test_k8s_submit_and_status(
    database_service: DatabaseServiceSQL,
    simulation_service_k8s_mock: SimulationServiceK8s,
    mock_k8s_job_service: MagicMock,
    simulator_repo_info: SimulatorRepoInfo,
) -> None:
    """Test submitting a simulation and checking status via K8s backend."""
    simulator = await _get_or_create_simulator(database_service, simulator_repo_info)
    await database_service.insert_parca_dataset(
        parca_dataset_request=ParcaDatasetRequest(simulator_version=simulator, parca_config=ParcaOptions())
    )

    # Mock the config template read
    simulation_service_k8s_mock.read_config_template = AsyncMock(return_value=CONFIG_TEMPLATE)  # type: ignore[method-assign]

    # Submit via handler
    simulation = await sim_handlers.run_simulation_workflow(
        database_service=database_service,
        simulation_service=simulation_service_k8s_mock,
        simulator_id=simulator.database_id,
        experiment_id="k8s-mock-test",
        simulation_config_filename="api_simulation_default.json",
    )

    assert simulation.database_id is not None
    assert simulation.job_id is not None

    # Verify K8s Job and ConfigMap were created
    mock_k8s_job_service.create_job.assert_called_once()
    mock_k8s_job_service.create_configmap.assert_called_once()

    job_spec = mock_k8s_job_service.create_job.call_args[0][0]
    pod_spec = job_spec.spec.template.spec
    assert len(pod_spec.containers) == 1
    assert pod_spec.containers[0].name == "workflow"

    # Check status
    status = await sim_handlers.get_simulation_status(db_service=database_service, id=simulation.database_id)
    assert status.status == JobStatus.COMPLETED


@pytest.mark.asyncio
async def test_k8s_cancel(
    database_service: DatabaseServiceSQL,
    simulation_service_k8s_mock: SimulationServiceK8s,
    mock_k8s_job_service: MagicMock,
    simulator_repo_info: SimulatorRepoInfo,
) -> None:
    """Test cancelling a running K8s simulation."""
    simulator = await _get_or_create_simulator(database_service, simulator_repo_info)
    await database_service.insert_parca_dataset(
        parca_dataset_request=ParcaDatasetRequest(simulator_version=simulator, parca_config=ParcaOptions())
    )

    simulation_service_k8s_mock.read_config_template = AsyncMock(return_value=CONFIG_TEMPLATE)  # type: ignore[method-assign]

    # Make status return RUNNING so cancel is meaningful
    mock_k8s_job_service.get_job_status.return_value = JobStatusInfo(job_id=JobId.k8s("test"), status=JobStatus.RUNNING)

    simulation = await sim_handlers.run_simulation_workflow(
        database_service=database_service,
        simulation_service=simulation_service_k8s_mock,
        simulator_id=simulator.database_id,
        experiment_id="k8s-cancel-test",
        simulation_config_filename="api_simulation_default.json",
    )

    # Cancel
    result = await sim_handlers.cancel_simulation(
        db_service=database_service,
        simulation_service=simulation_service_k8s_mock,
        simulation_id=simulation.database_id,
    )
    assert result.status == JobStatus.CANCELLED
    mock_k8s_job_service.delete_job.assert_called_once()


@pytest.mark.asyncio
async def test_k8s_log_retrieval(
    database_service: DatabaseServiceSQL,
    simulation_service_k8s_mock: SimulationServiceK8s,
    mock_k8s_job_service: MagicMock,
    simulator_repo_info: SimulatorRepoInfo,
) -> None:
    """Test retrieving K8s pod logs for a simulation."""
    simulator = await _get_or_create_simulator(database_service, simulator_repo_info)
    await database_service.insert_parca_dataset(
        parca_dataset_request=ParcaDatasetRequest(simulator_version=simulator, parca_config=ParcaOptions())
    )

    simulation_service_k8s_mock.read_config_template = AsyncMock(return_value=CONFIG_TEMPLATE)  # type: ignore[method-assign]
    mock_k8s_job_service.get_job_logs.return_value = "N E X T F L O W\nWorkflow completed OK"

    simulation = await sim_handlers.run_simulation_workflow(
        database_service=database_service,
        simulation_service=simulation_service_k8s_mock,
        simulator_id=simulator.database_id,
        experiment_id="k8s-log-test",
        simulation_config_filename="api_simulation_default.json",
    )

    # Get log
    response = await sim_handlers.get_simulation_log(db_service=database_service, simulation_id=simulation.database_id)
    body = response.body
    assert isinstance(body, bytes)
    assert "N E X T F L O W" in body.decode()


@pytest.mark.asyncio
async def test_k8s_log_fallback_to_s3(
    database_service: DatabaseServiceSQL,
    simulation_service_k8s_mock: SimulationServiceK8s,
    mock_k8s_job_service: MagicMock,
    mock_file_service: MagicMock,
    simulator_repo_info: SimulatorRepoInfo,
) -> None:
    """Test that log retrieval falls back to S3 when K8s pod logs are unavailable."""
    simulator = await _get_or_create_simulator(database_service, simulator_repo_info)
    await database_service.insert_parca_dataset(
        parca_dataset_request=ParcaDatasetRequest(simulator_version=simulator, parca_config=ParcaOptions())
    )

    simulation_service_k8s_mock.read_config_template = AsyncMock(return_value=CONFIG_TEMPLATE)  # type: ignore[method-assign]

    # K8s pod logs unavailable (pod cleaned up)
    mock_k8s_job_service.get_job_logs.return_value = None

    # S3 has the .nextflow.log
    mock_file_service.get_file_contents = AsyncMock(return_value=b"N E X T F L O W\nWorkflow completed from S3")

    simulation = await sim_handlers.run_simulation_workflow(
        database_service=database_service,
        simulation_service=simulation_service_k8s_mock,
        simulator_id=simulator.database_id,
        experiment_id="k8s-s3-log-test",
        simulation_config_filename="api_simulation_default.json",
    )

    response = await sim_handlers.get_simulation_log(db_service=database_service, simulation_id=simulation.database_id)
    body = response.body
    assert isinstance(body, bytes)
    assert "from S3" in body.decode()


@pytest.mark.asyncio
async def test_k8s_workflow_config_contents(
    database_service: DatabaseServiceSQL,
    simulation_service_k8s_mock: SimulationServiceK8s,
    mock_k8s_job_service: MagicMock,
    simulator_repo_info: SimulatorRepoInfo,
) -> None:
    """Verify the workflow config in ConfigMap has correct structure."""
    simulator = await _get_or_create_simulator(database_service, simulator_repo_info)
    await database_service.insert_parca_dataset(
        parca_dataset_request=ParcaDatasetRequest(simulator_version=simulator, parca_config=ParcaOptions())
    )

    simulation_service_k8s_mock.read_config_template = AsyncMock(return_value=CONFIG_TEMPLATE)  # type: ignore[method-assign]

    with patch("viva_api.common.handlers.simulations.get_job_backend", return_value=ComputeBackend.BATCH):
        await sim_handlers.run_simulation_workflow(
            database_service=database_service,
            simulation_service=simulation_service_k8s_mock,
            simulator_id=simulator.database_id,
            experiment_id="k8s-config-test",
            simulation_config_filename="api_simulation_default.json",
        )

    configmap = mock_k8s_job_service.create_configmap.call_args[0][0]
    config_data = json.loads(configmap.data["workflow.json"])

    # Verify AWS section
    assert config_data["aws"]["build_image"] is False
    assert "batch_queue" in config_data["aws"]
    assert "container_image" in config_data["aws"]
    assert config_data["progress_bar"] is False

    # Verify placeholders were replaced
    assert "EXPERIMENT_ID_PLACEHOLDER" not in config_data["experiment_id"]
    assert "k8s-config-test" in config_data["experiment_id"]


@pytest.mark.asyncio
async def test_extra_params_merge_into_config_without_overriding_named_params(
    database_service: DatabaseServiceSQL,
    simulation_service_k8s_mock: SimulationServiceK8s,
    mock_k8s_job_service: MagicMock,
    simulator_repo_info: SimulatorRepoInfo,
) -> None:
    """extra_params fills gaps the named parameters don't cover (e.g. a composite's own
    injected_processes/multi_node_dispatch knobs, per backlog items 86/88) but never
    overrides a key one of the named parameters — or the config template — already set.
    """
    simulator = await _get_or_create_simulator(database_service, simulator_repo_info)
    await database_service.insert_parca_dataset(
        parca_dataset_request=ParcaDatasetRequest(simulator_version=simulator, parca_config=ParcaOptions())
    )
    simulation_service_k8s_mock.read_config_template = AsyncMock(return_value=CONFIG_TEMPLATE)  # type: ignore[method-assign]

    with patch("viva_api.common.handlers.simulations.get_job_backend", return_value=ComputeBackend.BATCH):
        await sim_handlers.run_simulation_workflow(
            database_service=database_service,
            simulation_service=simulation_service_k8s_mock,
            simulator_id=simulator.database_id,
            experiment_id="extra-params-test",
            simulation_config_filename="api_simulation_default.json",
            num_generations=3,
            extra_params={
                # New key, no dedicated named parameter — should pass through untouched.
                "injected_processes": {"swap_processes": {"ecoli-metabolism": "ecoli-metabolism-redux"}},
                # Deliberately collides with num_generations's own explicit override
                # (config_data["generations"] = 3) — must NOT win.
                "generations": 999,
            },
        )

    configmap = mock_k8s_job_service.create_configmap.call_args[0][0]
    config_data = json.loads(configmap.data["workflow.json"])

    assert config_data["injected_processes"] == {"swap_processes": {"ecoli-metabolism": "ecoli-metabolism-redux"}}
    assert config_data["generations"] == 3  # num_generations wins, extra_params does not clobber it


@pytest.mark.asyncio
async def test_analysis_options_default_public_repo(
    database_service: DatabaseServiceSQL,
    simulation_service_k8s_mock: SimulationServiceK8s,
    mock_k8s_job_service: MagicMock,
) -> None:
    """Public-repo simulators should NOT get cd1_* analysis defaults."""
    simulator = await database_service.insert_simulator(
        git_commit_hash="abc1234",
        git_repo_url=RepoUrl.VECOLI_PUBLIC_REPO_URL,
        git_branch="master",
    )
    await database_service.insert_parca_dataset(
        parca_dataset_request=ParcaDatasetRequest(simulator_version=simulator, parca_config=ParcaOptions())
    )
    simulation_service_k8s_mock.read_config_template = AsyncMock(return_value=CONFIG_TEMPLATE)  # type: ignore[method-assign]

    with patch("viva_api.common.handlers.simulations.get_job_backend", return_value=ComputeBackend.BATCH):
        await sim_handlers.run_simulation_workflow(
            database_service=database_service,
            simulation_service=simulation_service_k8s_mock,
            simulator_id=simulator.database_id,
            experiment_id="public-repo-analysis",
            simulation_config_filename="api_simulation_default.json",
        )

    configmap = mock_k8s_job_service.create_configmap.call_args[0][0]
    config_data = json.loads(configmap.data["workflow.json"])
    analysis = config_data["analysis_options"]
    assert "multiseed" in analysis
    assert not any(k.startswith("cd1_") for k in analysis.get("multiseed", {}))


@pytest.mark.asyncio
async def test_analysis_options_default_private_repo(
    database_service: DatabaseServiceSQL,
    simulation_service_k8s_mock: SimulationServiceK8s,
    mock_k8s_job_service: MagicMock,
) -> None:
    """Private-repo simulators should get cd1_* analysis defaults."""
    simulator = await database_service.insert_simulator(
        git_commit_hash="def5678",
        git_repo_url=RepoUrl.VECOLI_PRIVATE_REPO_URL,
        git_branch="master",
    )
    await database_service.insert_parca_dataset(
        parca_dataset_request=ParcaDatasetRequest(simulator_version=simulator, parca_config=ParcaOptions())
    )
    simulation_service_k8s_mock.read_config_template = AsyncMock(return_value=CONFIG_TEMPLATE)  # type: ignore[method-assign]

    await sim_handlers.run_simulation_workflow(
        database_service=database_service,
        simulation_service=simulation_service_k8s_mock,
        simulator_id=simulator.database_id,
        experiment_id="private-repo-analysis",
        simulation_config_filename="api_simulation_default.json",
    )

    configmap = mock_k8s_job_service.create_configmap.call_args[0][0]
    config_data = json.loads(configmap.data["workflow.json"])
    analysis = config_data["analysis_options"]
    assert any(k.startswith("cd1_") for k in analysis.get("multiseed", {}))


@pytest.mark.asyncio
async def test_analysis_options_user_override(
    database_service: DatabaseServiceSQL,
    simulation_service_k8s_mock: SimulationServiceK8s,
    mock_k8s_job_service: MagicMock,
) -> None:
    """User-specified analysis_options should override defaults regardless of repo."""
    simulator = await database_service.insert_simulator(
        git_commit_hash="ghi9012",
        git_repo_url=RepoUrl.VECOLI_PRIVATE_REPO_URL,
        git_branch="master",
    )
    await database_service.insert_parca_dataset(
        parca_dataset_request=ParcaDatasetRequest(simulator_version=simulator, parca_config=ParcaOptions())
    )
    simulation_service_k8s_mock.read_config_template = AsyncMock(return_value=CONFIG_TEMPLATE)  # type: ignore[method-assign]

    user_analyses = AnalysisOptions.model_validate({"multiseed": {"ptools_rna": {"n_tp": 10}}})
    await sim_handlers.run_simulation_workflow(
        database_service=database_service,
        simulation_service=simulation_service_k8s_mock,
        simulator_id=simulator.database_id,
        experiment_id="user-override-analysis",
        simulation_config_filename="api_simulation_default.json",
        analysis_options=user_analyses,
    )

    configmap = mock_k8s_job_service.create_configmap.call_args[0][0]
    config_data = json.loads(configmap.data["workflow.json"])
    analysis = config_data["analysis_options"]
    assert "ptools_rna" in analysis.get("multiseed", {})
    assert not any(k.startswith("cd1_") for k in analysis.get("multiseed", {}))


@pytest.mark.asyncio
async def test_analysis_options_validation_rejects_invalid_module(
    database_service: DatabaseServiceSQL,
    simulation_service_k8s_mock: SimulationServiceK8s,
    mock_k8s_job_service: MagicMock,
) -> None:
    """User-specified analysis module that doesn't exist in the repo should be rejected."""
    simulator = await database_service.insert_simulator(
        git_commit_hash="val1234",
        git_repo_url=RepoUrl.VECOLI_PUBLIC_REPO_URL,
        git_branch="master",
    )
    await database_service.insert_parca_dataset(
        parca_dataset_request=ParcaDatasetRequest(simulator_version=simulator, parca_config=ParcaOptions())
    )
    simulation_service_k8s_mock.read_config_template = AsyncMock(return_value=CONFIG_TEMPLATE)  # type: ignore[method-assign]

    # Mock discovery to return known modules
    discovery = RepoDiscovery(
        simulator_id=simulator.database_id,
        git_repo_url=simulator.git_repo_url,
        git_commit_hash=simulator.git_commit_hash,
        config_filenames=["api_simulation_default.json"],
        analysis_modules={"multiseed": ["mass_fraction_summary", "ptools_rna"]},
    )
    simulation_service_k8s_mock.discover_repo_contents = AsyncMock(return_value=discovery)  # type: ignore[method-assign]

    bad_analyses = AnalysisOptions.model_validate({"multiseed": {"nonexistent_module": {}}})
    with pytest.raises(HTTPException) as exc_info:
        await sim_handlers.run_simulation_workflow(
            database_service=database_service,
            simulation_service=simulation_service_k8s_mock,
            simulator_id=simulator.database_id,
            experiment_id="bad-module-test",
            simulation_config_filename="api_simulation_default.json",
            analysis_options=bad_analyses,
        )
    assert exc_info.value.status_code == 400
    assert "nonexistent_module" in str(exc_info.value.detail)
    assert "mass_fraction_summary" in str(exc_info.value.detail)


@pytest.mark.asyncio
async def test_analysis_options_validation_accepts_valid_module(
    database_service: DatabaseServiceSQL,
    simulation_service_k8s_mock: SimulationServiceK8s,
    mock_k8s_job_service: MagicMock,
) -> None:
    """User-specified analysis module that exists in the repo should pass validation."""
    simulator = await database_service.insert_simulator(
        git_commit_hash="val5678",
        git_repo_url=RepoUrl.VECOLI_PUBLIC_REPO_URL,
        git_branch="master",
    )
    await database_service.insert_parca_dataset(
        parca_dataset_request=ParcaDatasetRequest(simulator_version=simulator, parca_config=ParcaOptions())
    )
    simulation_service_k8s_mock.read_config_template = AsyncMock(return_value=CONFIG_TEMPLATE)  # type: ignore[method-assign]

    discovery = RepoDiscovery(
        simulator_id=simulator.database_id,
        git_repo_url=simulator.git_repo_url,
        git_commit_hash=simulator.git_commit_hash,
        config_filenames=["api_simulation_default.json"],
        analysis_modules={"multiseed": ["mass_fraction_summary", "ptools_rna"]},
    )
    simulation_service_k8s_mock.discover_repo_contents = AsyncMock(return_value=discovery)  # type: ignore[method-assign]

    valid_analyses = AnalysisOptions.model_validate({"multiseed": {"ptools_rna": {"n_tp": 10}}})
    with patch("viva_api.common.handlers.simulations.get_job_backend", return_value=ComputeBackend.BATCH):
        simulation = await sim_handlers.run_simulation_workflow(
            database_service=database_service,
            simulation_service=simulation_service_k8s_mock,
            simulator_id=simulator.database_id,
            experiment_id="valid-module-test",
            simulation_config_filename="api_simulation_default.json",
            analysis_options=valid_analyses,
        )
    assert simulation.database_id is not None


@pytest.mark.asyncio
async def test_discovery_failure_does_not_block_workflow(
    database_service: DatabaseServiceSQL,
    simulation_service_k8s_mock: SimulationServiceK8s,
    mock_k8s_job_service: MagicMock,
) -> None:
    """If discovery fails (e.g. GitHub API down), the workflow should still proceed."""
    simulator = await database_service.insert_simulator(
        git_commit_hash="fail123",
        git_repo_url=RepoUrl.VECOLI_PUBLIC_REPO_URL,
        git_branch="master",
    )
    await database_service.insert_parca_dataset(
        parca_dataset_request=ParcaDatasetRequest(simulator_version=simulator, parca_config=ParcaOptions())
    )
    simulation_service_k8s_mock.read_config_template = AsyncMock(return_value=CONFIG_TEMPLATE)  # type: ignore[method-assign]
    simulation_service_k8s_mock.discover_repo_contents = AsyncMock(side_effect=RuntimeError("GitHub API down"))  # type: ignore[method-assign]

    user_analyses = AnalysisOptions.model_validate({"multiseed": {"anything": {}}})
    with patch("viva_api.common.handlers.simulations.get_job_backend", return_value=ComputeBackend.BATCH):
        simulation = await sim_handlers.run_simulation_workflow(
            database_service=database_service,
            simulation_service=simulation_service_k8s_mock,
            simulator_id=simulator.database_id,
            experiment_id="discovery-fail-test",
            simulation_config_filename="api_simulation_default.json",
            analysis_options=user_analyses,
        )
    # Should succeed despite discovery failure
    assert simulation.database_id is not None


@pytest.mark.asyncio
async def test_ray_canonical_batch_baseline_routes_through_real_entrypoint_to_chain_dispatch(
    database_service: DatabaseServiceSQL,
) -> None:
    """The REAL dispatch entrypoint (run_simulation_workflow ->
    SimulationServiceRay.submit_ecoli_simulation_job), not
    submit_chain_dispatch_job called directly, must reach chain-dispatch for a
    canonical batch_baseline-shaped (composite unset, multiseed x
    multigeneration) request against a real v2ecoli/sms-ecoli simulator.

    This is the test that would have caught the real wiring gap found in
    review: submit_chain_dispatch_job existed and was fully tested in
    isolation from the moment it was built (tests/simulation/test_ray_backend.py),
    but nothing on the real request path ever called it until backlog item
    33's routing fix landed -- a real dispatch would have silently kept
    exercising the old array-job path forever, since submit_ecoli_simulation_job
    itself was never touched by the original rework.

    The Ray backend is registered into the SAME per-repo service registry a
    real deployment uses (viva_api.dependencies.global_simulation_services),
    keyed by the real sms-ecoli repo URL -- exercising the actual repo-based
    backend resolution (compute_backend_for_repo / get_simulation_service_for_repo)
    a real request goes through, not just passing the service in as a raw
    parameter. AWS Batch itself is mocked; everything else (config assembly,
    backend routing, chain-dispatch submission, HpcRun bookkeeping) is real.
    """
    saved_registry = dict(deps.global_simulation_services)
    saved_default = deps.get_simulation_service()
    try:
        ray_service = SimulationServiceRay()
        ray_service.read_config_template = AsyncMock(return_value=CONFIG_TEMPLATE)  # type: ignore[method-assign]
        deps.set_simulation_service_registry({ComputeBackend.RAY: ray_service})

        simulator = await database_service.insert_simulator(
            git_commit_hash="e2e1234",
            git_repo_url=RepoUrl.SMS_ECOLI_REPO_URL,
            git_branch="main",
        )

        # Chain-dispatch now submits ONLY ParCa upfront (backlog item 71 Phase
        # 4) -- as a container-type job, which needs the real (cached)
        # Settings singleton's container fields set. Patched as attributes on
        # the REAL get_settings() instance (not a mock replacing get_settings
        # itself) so "config assembly... is real" (this test's own stated
        # intent) stays true for everything else.
        settings = get_settings()
        mnp_base_props = {
            "numNodes": 4,
            "mainNode": 0,
            "nodeRangeProperties": [{"targetNodes": "0:", "container": {"image": "x:tag", "vcpus": 16}}],
        }
        container_base_props = {"image": "x:tag", "vcpus": 16, "memory": 32000}
        mock_batch = MagicMock()

        def _describe(**kwargs: Any) -> dict[str, Any]:
            name = kwargs.get("jobDefinitionName")
            if name == settings.ray_mnp_job_definition:
                return {"jobDefinitions": [{"revision": 1, "nodeProperties": mnp_base_props}]}
            if name == settings.ray_container_job_definition:
                return {"jobDefinitions": [{"revision": 1, "containerProperties": container_base_props}]}
            return {"jobDefinitions": []}

        mock_batch.describe_job_definitions.side_effect = _describe
        mock_batch.register_job_definition.side_effect = lambda **kw: {
            "jobDefinitionName": kw["jobDefinitionName"],
            "revision": 1,
        }
        # ParCa is now the ONE upfront submission; the campaign's own
        # generation jobs are submitted incrementally by JobScheduler, not
        # exercised by this entrypoint-routing test (see
        # tests/simulation/test_scheduler.py::TestAdvanceChainCampaign for that).
        mock_batch.submit_job.side_effect = [{"jobId": "parca-1"}]

        fake_file_service = AsyncMock()
        fake_file_service.upload_file = AsyncMock()

        with (
            patch("viva_api.simulation.simulation_service_ray.boto3.client", return_value=mock_batch),
            patch("viva_api.dependencies.get_file_service", return_value=fake_file_service),
            patch("viva_api.simulation.simulation_service_ray.asyncio.sleep", new=AsyncMock()),
            patch.object(settings, "ray_container_queue", "smscdk-ray-standalone"),
            patch.object(settings, "ray_container_job_definition", "smscdk-ray-container"),
        ):
            simulation = await sim_handlers.run_simulation_workflow(
                database_service=database_service,
                simulation_service=ray_service,  # fallback only -- the registry above is what actually resolves
                simulator_id=simulator.database_id,
                experiment_id="e2e-canonical-batch-baseline",
                simulation_config_filename="api_simulation_default.json",
                num_generations=3,
                num_seeds=2,
            )

            # The real entrypoint now returns as soon as the campaign is
            # SCHEDULED, issuing the N*G submissions on a background task
            # (SimulationServiceRay._submit_chain_dispatch_background), so
            # simulation.job_id is the LOCAL task id rather than the ParCa Batch
            # job id. Await that task -- inside the patch context, since the task
            # is what actually calls boto3 -- before checking anything it is
            # responsible for. Awaiting the asyncio.Task directly blocks until it
            # is done and re-raises whatever it hit. Only the TIMING of the
            # checks below changes; every invariant is the one this test has
            # always asserted.
            assert simulation.job_id is not None
            assert simulation.job_id in ray_service._local._tasks
            campaign_job_id = await ray_service._local._tasks[simulation.job_id]

        # Reached chain-dispatch, not the array path: exactly ONE submission
        # (ParCa, container-type, backlog item 71 Phase 4) -- never a single
        # array-shaped submission, and no per-seed generation jobs upfront.
        assert mock_batch.submit_job.call_count == 1
        (parca_call,) = mock_batch.submit_job.call_args_list
        assert "arrayProperties" not in parca_call.kwargs
        assert "dependsOn" not in parca_call.kwargs
        assert "containerOverrides" in parca_call.kwargs

        # Chain-dispatch's own return convention (the ParCa job -- there is no
        # single "sim" job for a chain campaign) is now the BACKGROUND TASK's
        # result. The API response's own job_id is the LOCAL task id, which is
        # what GET /simulations/{id}/status resolves while submission is still in
        # flight.
        assert campaign_job_id == JobId.ray("parca-1")

        # TWO HpcRun rows exist for this simulation, and this test now pins what
        # each one is. Previously there was exactly one; the second is the
        # synchronous placeholder _submit_chain_dispatch_background commits
        # before returning, so a status poll arriving during the (minutes-long)
        # submission has a real row to read. The invariant this raw row count was
        # written to protect is unchanged and still checked: the handler in
        # viva_api.common.handlers.simulations must NOT add a generic row of its
        # own on top -- its idempotent-insert guard sees the placeholder's
        # correlation_id and skips. A THIRD row here would mean that guard broke.
        # Counting rows rather than trusting get_hpcrun_by_ref's
        # most-recent-wins lookup, so such a regression cannot hide behind "the
        # newest row happens to look right."
        async with database_service.async_sessionmaker() as session:
            result = await session.execute(
                select(ORMHpcRun).where(ORMHpcRun.jobref_simulation_id == simulation.database_id).order_by(ORMHpcRun.id)
            )
            rows = list(result.scalars().all())
        assert len(rows) == 2
        placeholder_row, campaign_row = rows
        # The placeholder: LOCAL backend, both chain fields unset (so the
        # scheduler's chain_n_generations IS NOT NULL poll set skips it).
        assert placeholder_row.job_backend == JobBackend.LOCAL
        assert placeholder_row.job_id_ext == simulation.job_id
        assert placeholder_row.chain_n_generations is None
        assert placeholder_row.chain_final_job_ids is None
        # The real campaign row, inserted last and therefore the one every later
        # status read resolves to. Same correlation_id as the placeholder, which
        # is exactly what makes the handler's guard fire.
        assert campaign_row.correlation_id == placeholder_row.correlation_id

        hpc_run = await database_service.get_hpcrun_by_ref(ref_id=simulation.database_id, job_type=JobType.SIMULATION)
        assert hpc_run is not None
        assert hpc_run.database_id == campaign_row.id
        assert hpc_run.job_id == JobId.ray("parca-1")
        assert hpc_run.chain_n_generations == 3
        assert hpc_run.chain_final_job_ids == []
        assert hpc_run.chain_current_job_ids == [None, None]
        assert hpc_run.chain_parca_done is False
    finally:
        deps.set_simulation_service_registry(saved_registry)
        deps.set_simulation_service(saved_default)
