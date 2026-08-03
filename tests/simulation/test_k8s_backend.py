"""Tests for K8s backend: K8sJobService, SimulationServiceK8s, LocalTaskService, config backend selection."""

import asyncio
import json
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sms_api.common.hpc.k8s_job_service import K8sJobService, _job_to_status
from sms_api.common.hpc.local_task_service import LocalTaskService
from sms_api.common.models import JobBackend, JobId, JobStatus
from sms_api.config import ComputeBackend, get_settings
from sms_api.simulation.simulation_service_k8s import SimulationServiceK8s

if TYPE_CHECKING:
    from sms_api.simulation.database_service import DatabaseServiceSQL
    from sms_api.simulation.models import SimulationRequest


class TestJobToStatus:
    """Test K8s Job condition → JobStatus mapping."""

    def test_completed_job(self) -> None:
        job = MagicMock()
        job.status.conditions = [MagicMock(type="Complete", status="True")]
        job.status.active = 0
        job.status.ready = 0
        assert _job_to_status(job) == JobStatus.COMPLETED

    def test_failed_job(self) -> None:
        job = MagicMock()
        job.status.conditions = [MagicMock(type="Failed", status="True")]
        job.status.active = 0
        assert _job_to_status(job) == JobStatus.FAILED

    def test_running_job(self) -> None:
        job = MagicMock()
        job.status.conditions = None
        job.status.active = 1
        job.status.ready = 0
        assert _job_to_status(job) == JobStatus.RUNNING

    def test_pending_job(self) -> None:
        job = MagicMock()
        job.status.conditions = None
        job.status.active = 0
        job.status.ready = 0
        assert _job_to_status(job) == JobStatus.PENDING

    def test_no_status(self) -> None:
        job = MagicMock()
        job.status = None
        assert _job_to_status(job) == JobStatus.UNKNOWN

    def test_condition_not_true(self) -> None:
        job = MagicMock()
        job.status.conditions = [MagicMock(type="Complete", status="False")]
        job.status.active = 1
        job.status.ready = 0
        assert _job_to_status(job) == JobStatus.RUNNING


class TestGetJobBackend:
    def test_slurm_backend(self) -> None:
        from sms_api.config import get_job_backend

        with patch("sms_api.config.get_settings") as mock_settings:
            mock_settings.return_value = MagicMock(compute_backend="slurm")
            assert get_job_backend() == ComputeBackend.SLURM

    def test_batch_backend(self) -> None:
        from sms_api.config import get_job_backend

        with patch("sms_api.config.get_settings") as mock_settings:
            mock_settings.return_value = MagicMock(compute_backend="batch")
            assert get_job_backend() == ComputeBackend.BATCH

    def test_default_is_slurm(self) -> None:
        from sms_api.config import get_job_backend

        with patch("sms_api.config.get_settings") as mock_settings:
            mock_settings.return_value = MagicMock(compute_backend="slurm")
            assert get_job_backend() == ComputeBackend.SLURM


class TestK8sJobService:
    def test_get_job_status_not_found(self) -> None:
        from kubernetes.client.rest import ApiException

        service = MagicMock(spec=K8sJobService)
        service._namespace = "test-ns"

        # Simulate the real behavior: 404 returns None
        mock_batch_api = MagicMock()
        mock_batch_api.read_namespaced_job_status.side_effect = ApiException(status=404)

        with patch.object(K8sJobService, "__init__", lambda self, namespace: None):
            svc = K8sJobService.__new__(K8sJobService)
            svc._namespace = "test-ns"
            svc._batch_api = mock_batch_api
            svc._core_api = MagicMock()

            result = svc.get_job_status("nonexistent-job")
            assert result is None

    def test_get_job_status_running(self) -> None:
        mock_job = MagicMock()
        mock_job.status.conditions = None
        mock_job.status.active = 1
        mock_job.status.ready = 0
        mock_job.status.start_time = None
        mock_job.status.completion_time = None

        mock_batch_api = MagicMock()
        mock_batch_api.read_namespaced_job_status.return_value = mock_job

        with patch.object(K8sJobService, "__init__", lambda self, namespace: None):
            svc = K8sJobService.__new__(K8sJobService)
            svc._namespace = "test-ns"
            svc._batch_api = mock_batch_api
            svc._core_api = MagicMock()

            result = svc.get_job_status("my-job")
            assert result is not None
            assert result.status == JobStatus.RUNNING
            assert result.job_id == JobId.k8s("my-job")


class TestJobId:
    def test_slurm_factory(self) -> None:
        job_id = JobId.slurm(12345)
        assert job_id.value == "12345"
        assert job_id.backend == JobBackend.SLURM
        assert job_id.as_slurm_int == 12345
        assert str(job_id) == "12345"

    def test_k8s_factory(self) -> None:
        job_id = JobId.k8s("nf-sim-abc")
        assert job_id.value == "nf-sim-abc"
        assert job_id.backend == JobBackend.K8S
        assert str(job_id) == "nf-sim-abc"

    def test_slurm_int_raises_for_k8s(self) -> None:
        job_id = JobId.k8s("nf-sim-abc")
        with pytest.raises(TypeError, match="Not a SLURM job ID"):
            _ = job_id.as_slurm_int

    def test_equality(self) -> None:
        assert JobId.slurm(123) == JobId.slurm(123)
        assert JobId.slurm(123) != JobId.slurm(456)
        assert JobId.slurm(123) != JobId.k8s("123")
        assert JobId.k8s("abc") == JobId.k8s("abc")

    def test_local_factory(self) -> None:
        job_id = JobId.local("abc123")
        assert job_id.value == "abc123"
        assert job_id.backend == JobBackend.LOCAL
        assert str(job_id) == "abc123"

    def test_frozen(self) -> None:
        job_id = JobId.slurm(123)
        with pytest.raises(AttributeError):
            job_id.value = "456"  # type: ignore[misc]


@pytest.mark.asyncio
class TestLocalTaskService:
    async def test_submit_and_complete(self) -> None:
        service = LocalTaskService()

        async def quick_task() -> None:
            await asyncio.sleep(0.01)

        job_id = service.submit(quick_task(), name="test-task")
        assert job_id.backend == JobBackend.LOCAL

        # Should be RUNNING immediately
        status = service.get_status(job_id.value)
        assert status is not None
        assert status.status in (JobStatus.RUNNING, JobStatus.COMPLETED)

        # Wait for completion
        await asyncio.sleep(0.05)
        status = service.get_status(job_id.value)
        assert status is not None
        assert status.status == JobStatus.COMPLETED

    async def test_submit_and_fail(self) -> None:
        service = LocalTaskService()

        async def failing_task() -> None:
            raise RuntimeError("build exploded")

        job_id = service.submit(failing_task(), name="fail-task")
        await asyncio.sleep(0.05)

        status = service.get_status(job_id.value)
        assert status is not None
        assert status.status == JobStatus.FAILED
        assert "build exploded" in (status.error_message or "")

    async def test_cancel(self) -> None:
        service = LocalTaskService()

        async def slow_task() -> None:
            await asyncio.sleep(10)

        job_id = service.submit(slow_task(), name="slow-task")
        assert service.cancel(job_id.value) is True

        await asyncio.sleep(0.05)
        status = service.get_status(job_id.value)
        assert status is not None
        assert status.status == JobStatus.CANCELLED

    async def test_status_unknown_task(self) -> None:
        service = LocalTaskService()
        assert service.get_status("nonexistent") is None

    async def test_cleanup(self) -> None:
        service = LocalTaskService()

        async def quick() -> None:
            pass

        service.submit(quick(), name="cleanup-test")
        await asyncio.sleep(0.05)

        removed = service.cleanup_completed()
        assert removed == 1
        assert service.get_status("nonexistent") is None


@pytest.mark.asyncio
class TestSimulationServiceK8s:
    """Tests for SimulationServiceK8s using injected mock fixtures."""

    async def test_submit_simulation_creates_single_container_job(
        self,
        simulation_service_k8s_mock: SimulationServiceK8s,
        mock_k8s_job_service: MagicMock,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """Verify that submit creates a K8s Job with a single workflow container."""

        simulation = await database_service.insert_simulation(sim_request=experiment_request)
        job_id = await simulation_service_k8s_mock.submit_ecoli_simulation_job(
            ecoli_simulation=simulation, database_service=database_service, correlation_id="test-corr"
        )

        assert job_id.backend == JobBackend.K8S
        # Verify K8s Job and ConfigMap were created
        mock_k8s_job_service.create_job.assert_called_once()
        mock_k8s_job_service.create_configmap.assert_called_once()

        # Inspect the Job spec
        job_arg = mock_k8s_job_service.create_job.call_args[0][0]
        pod_spec = job_arg.spec.template.spec

        # Should have no init containers
        assert pod_spec.init_containers is None

        # Should have a single workflow container
        assert len(pod_spec.containers) == 1
        assert pod_spec.containers[0].name == "workflow"

        # Should have config volume only (no emptyDir)
        volume_names = [v.name for v in pod_spec.volumes]
        assert "config" in volume_names
        assert len(pod_spec.volumes) == 1

    async def test_submit_simulation_config_has_aws_section(
        self,
        simulation_service_k8s_mock: SimulationServiceK8s,
        mock_k8s_job_service: MagicMock,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """Verify workflow config in ConfigMap preserves aws section from handler."""
        # Simulate what the handler does: inject aws block before DB insert
        experiment_request.config.aws = {  # type: ignore[attr-defined]
            "build_image": False,
            "container_image": "123456.dkr.ecr.us-gov-west-1.amazonaws.com/vecoli:abc1234",
            "region": "us-gov-west-1",
            "batch_queue": "test-queue",
        }
        experiment_request.config.progress_bar = False  # type: ignore[attr-defined]

        simulation = await database_service.insert_simulation(sim_request=experiment_request)
        await simulation_service_k8s_mock.submit_ecoli_simulation_job(
            ecoli_simulation=simulation, database_service=database_service, correlation_id="test-corr"
        )

        # Get the ConfigMap that was created
        configmap_arg = mock_k8s_job_service.create_configmap.call_args[0][0]
        config_json = json.loads(configmap_arg.data["workflow.json"])

        assert config_json["aws"]["build_image"] is False
        assert "batch_queue" in config_json["aws"]
        assert "container_image" in config_json["aws"]
        assert "region" in config_json["aws"]
        assert config_json["progress_bar"] is False

    async def test_cancel_k8s_job_deletes_job_and_configmap(
        self,
        simulation_service_k8s_mock: SimulationServiceK8s,
        mock_k8s_job_service: MagicMock,
    ) -> None:
        """Verify cancel deletes the K8s Job and its ConfigMap."""
        job_id = JobId.k8s("nf-test-experiment")
        await simulation_service_k8s_mock.cancel_job(job_id)

        mock_k8s_job_service.delete_job.assert_called_once_with("nf-test-experiment")
        mock_k8s_job_service.delete_configmap.assert_called_once_with("nf-test-experiment-config")

    async def test_cancel_local_task_does_not_call_k8s(
        self,
        simulation_service_k8s_mock: SimulationServiceK8s,
        mock_k8s_job_service: MagicMock,
    ) -> None:
        """Verify LOCAL backend dispatches to LocalTaskService, not K8s."""
        job_id = JobId.local("some-uuid")
        await simulation_service_k8s_mock.cancel_job(job_id)

        mock_k8s_job_service.delete_job.assert_not_called()

    async def test_get_job_status_k8s_dispatches_to_k8s_service(
        self,
        simulation_service_k8s_mock: SimulationServiceK8s,
        mock_k8s_job_service: MagicMock,
    ) -> None:
        """Verify K8S backend queries K8sJobService."""
        job_id = JobId.k8s("nf-test-job")
        result = await simulation_service_k8s_mock.get_job_status(job_id)

        mock_k8s_job_service.get_job_status.assert_called_once_with("nf-test-job")
        assert result is not None
        assert result.status == JobStatus.COMPLETED

    async def test_get_job_status_local_dispatches_to_local_service(
        self,
        simulation_service_k8s_mock: SimulationServiceK8s,
        mock_k8s_job_service: MagicMock,
    ) -> None:
        """Verify LOCAL backend queries LocalTaskService, not K8s."""
        job_id = JobId.local("nonexistent")
        result = await simulation_service_k8s_mock.get_job_status(job_id)

        mock_k8s_job_service.get_job_status.assert_not_called()
        assert result is None  # No such task in LocalTaskService

    async def test_read_config_template_uses_github_api(
        self,
        simulation_service_k8s_mock: SimulationServiceK8s,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Verify read_config_template calls GitHub Contents API."""
        from sms_api.simulation.models import SimulatorVersion

        simulator = SimulatorVersion(
            database_id=1,
            git_commit_hash="abc1234",
            git_repo_url="https://github.com/test/repo",
            git_branch="main",
        )

        mock_response = AsyncMock()
        mock_response.text = '{"experiment_id": "test"}'
        mock_response.raise_for_status = MagicMock()

        mock_client = AsyncMock()
        mock_client.get.return_value = mock_response
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        monkeypatch.setattr("sms_api.simulation.github_repo.httpx.AsyncClient", lambda: mock_client)

        result = await simulation_service_k8s_mock.read_config_template(simulator, "test.json")

        assert result == '{"experiment_id": "test"}'
        mock_client.get.assert_called_once()
        call_url = mock_client.get.call_args[0][0]
        assert "api.github.com/repos/test/repo" in call_url
        assert "test.json" in call_url

    async def test_read_config_template_raises_on_404(
        self,
        simulation_service_k8s_mock: SimulationServiceK8s,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Verify read_config_template raises HTTPException(404) when config not found."""
        from fastapi import HTTPException

        from sms_api.simulation.models import SimulatorVersion

        simulator = SimulatorVersion(
            database_id=1,
            git_commit_hash="6f7e5b9",
            git_repo_url="https://github.com/CovertLab/vEcoli",
            git_branch="master",
        )

        mock_response = AsyncMock()
        mock_response.status_code = 404
        mock_response.raise_for_status = MagicMock(side_effect=Exception("should not be called"))

        mock_client = AsyncMock()
        mock_client.get.return_value = mock_response
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        monkeypatch.setattr("sms_api.simulation.github_repo.httpx.AsyncClient", lambda: mock_client)

        with pytest.raises(HTTPException) as exc_info:
            await simulation_service_k8s_mock.read_config_template(simulator, "nonexistent.json")
        assert exc_info.value.status_code == 404

    async def test_read_config_template_fallback_on_404(
        self,
        simulation_service_k8s_mock: SimulationServiceK8s,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Verify read_config_template returns embedded default when allow_default_fallback=True."""
        from sms_api.simulation.github_repo import _DEFAULT_CONFIG_TEMPLATE
        from sms_api.simulation.models import SimulatorVersion

        simulator = SimulatorVersion(
            database_id=1,
            git_commit_hash="6f7e5b9",
            git_repo_url="https://github.com/CovertLab/vEcoli",
            git_branch="master",
        )

        mock_response = AsyncMock()
        mock_response.status_code = 404
        mock_response.raise_for_status = MagicMock(side_effect=Exception("should not be called"))

        mock_client = AsyncMock()
        mock_client.get.return_value = mock_response
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        monkeypatch.setattr("sms_api.simulation.github_repo.httpx.AsyncClient", lambda: mock_client)

        result = await simulation_service_k8s_mock.read_config_template(
            simulator, "api_simulation_default.json", allow_default_fallback=True
        )

        parsed = json.loads(result)
        assert parsed["experiment_id"] == "EXPERIMENT_ID_PLACEHOLDER"
        assert parsed["parca_options"]["cpus"] == 6
        assert "aws_cdk" in parsed
        assert parsed == _DEFAULT_CONFIG_TEMPLATE

    async def test_read_config_template_rejects_configs_prefix(
        self,
        simulation_service_k8s_mock: SimulationServiceK8s,
    ) -> None:
        """Verify configs/ prefix in filename is rejected with a 400."""
        from fastapi import HTTPException

        from sms_api.simulation.models import SimulatorVersion

        simulator = SimulatorVersion(
            database_id=1,
            git_commit_hash="6f7e5b9",
            git_repo_url="https://github.com/CovertLab/vEcoli",
            git_branch="master",
        )

        with pytest.raises(HTTPException) as exc_info:
            await simulation_service_k8s_mock.read_config_template(simulator, "configs/campaigns/pilot_mixed.json")
        assert exc_info.value.status_code == 400
        assert "configs/" in str(exc_info.value.detail)

    async def test_build_command_generates_valid_script(
        self,
        simulation_service_k8s_mock: SimulationServiceK8s,
    ) -> None:
        """Verify _build_command generates a valid DooD build script."""
        from sms_api.common.simulator_defaults import DEFAULT_SIMULATOR

        # Task image command (no submit)
        cmd = simulation_service_k8s_mock._build_command(DEFAULT_SIMULATOR, image_tag="test123")
        assert cmd[0] == "sh"
        assert cmd[1] == "-c"
        script = cmd[2]
        assert "docker info" in script
        assert "build-and-push-ecr.sh" in script
        assert "test123" in script
        assert "Dockerfile-submit" not in script

        # Submit image command
        cmd_submit = simulation_service_k8s_mock._build_command(
            DEFAULT_SIMULATOR, image_tag="test123", submit_image=True
        )
        script_submit = cmd_submit[2]
        assert "Dockerfile-submit" in script_submit
        assert "default-jre-headless" in script_submit
        assert "nextflow" in script_submit

    async def test_submit_ray_native_analysis_targets_v2ecoli_image(
        self,
        simulation_service_k8s_mock: SimulationServiceK8s,
        mock_k8s_job_service: MagicMock,
    ) -> None:
        """submit_standalone_analysis's legacy job template pulls vecoli:<commit>-amd64-submit
        (ECR repo "vecoli"), an image never produced for Ray-backend (v2ecoli/sms-ecoli)
        builds -- confirmed live via ImagePullBackOff on a real trigger. This must instead
        target the v2ecoli:<commit> image (ECR repo "v2ecoli", no arch/submit suffix) that
        Ray-backend simulation dispatch actually builds and pushes."""
        settings = get_settings()
        params = {
            "out_uri": "s3://bucket/vecoli-output/exp123",
            "n_seeds": 2,
            "modules": {"multiseed": {"doubling_time_distribution": {}}},
            "analysis_name": "analysis-exp123-ab12",
        }

        job_id = await simulation_service_k8s_mock.submit_ray_native_analysis(
            experiment_id="exp123",
            params=params,
            commit="deadbeef",
        )

        assert job_id.backend == JobBackend.K8S
        mock_k8s_job_service.create_job.assert_called_once()
        mock_k8s_job_service.create_configmap.assert_called_once()

        job_arg = mock_k8s_job_service.create_job.call_args[0][0]
        container = job_arg.spec.template.spec.containers[0]
        assert container.image == (
            f"{settings.ecr_account_id}.dkr.ecr.{settings.batch_region}.amazonaws.com"
            f"/{settings.ray_ecr_repository}:deadbeef"
        )
        assert "vecoli-amd64-submit" not in container.image
        assert "run_standalone_analysis.py" in container.command[2]
        assert "--config-file /config/params.json" in container.command[2]

        configmap_arg = mock_k8s_job_service.create_configmap.call_args[0][0]
        written_params = json.loads(configmap_arg.data["params.json"])
        assert written_params == params

    async def test_submit_ray_native_analysis_sets_sim_data_env_for_duckdb_analyses(
        self,
        simulation_service_k8s_mock: SimulationServiceK8s,
        mock_k8s_job_service: MagicMock,
    ) -> None:
        """An S3 sweep has no co-located sim_data pickle to glob (analysis_runner
        .resolve_sim_data only globs local paths), so the DuckDB-based Analysis
        family (cd1/ptools) can't resolve sim_data without this env var. The
        ParCa job and this analysis job derive the same commit-keyed cache URI
        independently -- no new hand-off plumbing needed."""
        from sms_api.common.storage import data_layout

        params = {
            "out_uri": "s3://bucket/vecoli-output/exp123",
            "n_seeds": 1,
            "modules": {"single": {"ptools_rna": {}}},
            "analysis_name": "analysis-exp123-cd1",
        }

        await simulation_service_k8s_mock.submit_ray_native_analysis(
            experiment_id="exp123",
            params=params,
            commit="deadbeef",
        )

        job_arg = mock_k8s_job_service.create_job.call_args[0][0]
        container = job_arg.spec.template.spec.containers[0]
        env_by_name = {e.name: e.value for e in container.env}
        assert env_by_name["V2ECOLI_SIM_DATA"] == (
            f"{data_layout.RayLayout.parca_cache_uri('deadbeef')}simData.cPickle"
        )

    async def test_submit_ray_native_analysis_job_name_unique_per_trigger(
        self,
        simulation_service_k8s_mock: SimulationServiceK8s,
        mock_k8s_job_service: MagicMock,
    ) -> None:
        """Two triggers for the SAME simulation must produce distinct K8s Job
        names -- job_name derived from bare experiment_id (the legacy
        submit_standalone_analysis's pattern) collides on repeat triggers,
        since K8s Job names must be unique. Deriving from the already-unique
        analysis_name (one per trigger, uuid-suffixed) avoids this."""
        base = {"out_uri": "s3://bucket/exp", "n_seeds": 1, "modules": {}}

        job_id_1 = await simulation_service_k8s_mock.submit_ray_native_analysis(
            experiment_id="exp123",
            params={**base, "analysis_name": "analysis-exp123-aaaa"},
            commit="deadbeef",
        )
        job_id_2 = await simulation_service_k8s_mock.submit_ray_native_analysis(
            experiment_id="exp123",
            params={**base, "analysis_name": "analysis-exp123-bbbb"},
            commit="deadbeef",
        )

        assert str(job_id_1) != str(job_id_2)
        assert mock_k8s_job_service.create_job.call_count == 2
        names = [c.args[0].metadata.name for c in mock_k8s_job_service.create_job.call_args_list]
        assert len(set(names)) == 2
