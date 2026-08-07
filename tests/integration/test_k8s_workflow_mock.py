"""K8s Batch Integration Tests (Mock Mode) — exercises handlers with mocked backends.

Tests the full workflow logic through the handler functions (not the HTTP layer)
with mocked K8s and S3 backends. Always runs — no AWS credentials or cluster
access required.

Run with: uv run pytest tests/integration/test_k8s_workflow_mock.py -v -s

Prerequisites:
- Docker running (for Postgres testcontainer)
"""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from tests.fixtures.api_fixtures import SimulatorRepoInfo
from viva_api.common.handlers import simulations as sim_handlers
from viva_api.common.hpc.job_service import JobStatusInfo
from viva_api.common.models import JobId, JobStatus
from viva_api.common.simulator_defaults import RepoUrl
from viva_api.config import ComputeBackend
from viva_api.simulation.database_service import DatabaseServiceSQL
from viva_api.simulation.models import (
    AnalysisOptions,
    ParcaDatasetRequest,
    ParcaOptions,
    RepoDiscovery,
    SimulatorVersion,
)
from viva_api.simulation.simulation_service_k8s import SimulationServiceK8s

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
