"""Per-simulator backend routing — one deployment serves vecoli (Batch) + v2ecoli (Ray)."""

from unittest.mock import MagicMock

import pytest

import viva_api.dependencies as deps
from viva_api.common.models import JobId
from viva_api.config import ComputeBackend, compute_backend_for_repo


class TestComputeBackendForRepo:
    @pytest.mark.parametrize(
        ("url", "expected"),
        [
            ("https://github.com/vivarium-collective/v2ecoli", ComputeBackend.RAY),
            ("https://github.com/vivarium-collective/v2Ecoli", ComputeBackend.RAY),  # case-insensitive
            # The production Ray repo — matches NEITHER substring; only the explicit
            # RepoUrl map routes it correctly (regression guard for the mis-route bug).
            ("https://github.com/CovertLabEcoli/sms-ecoli", ComputeBackend.RAY),
            ("https://github.com/CovertLab/vEcoli", ComputeBackend.BATCH),
            ("https://github.com/vivarium-collective/vEcoli", ComputeBackend.BATCH),  # the fork
            ("https://github.com/CovertLabEcoli/vEcoli-private", ComputeBackend.BATCH),
            # Fork/variant fallback (substring) at other URLs.
            ("https://github.com/someuser/sms-ecoli-experiment", ComputeBackend.RAY),
            ("https://github.com/someuser/my-vEcoli-fork", ComputeBackend.BATCH),
            ("https://github.com/someone/unrelated", None),
        ],
    )
    def test_mapping(self, url: str, expected: ComputeBackend | None) -> None:
        assert compute_backend_for_repo(url) == expected

    def test_sms_ecoli_is_not_matched_by_substring_alone(self) -> None:
        """Guard the exact bug: 'sms-ecoli' contains neither 'v2ecoli' nor 'vecoli',
        so without the explicit RepoUrl map it would fall through to None."""
        url = "https://github.com/CovertLabEcoli/sms-ecoli".lower()
        assert "v2ecoli" not in url and "vecoli" not in url
        assert compute_backend_for_repo(url) == ComputeBackend.RAY


class TestServiceRouting:
    """get_simulation_service_for_repo / _for_job route to the right registered service."""

    def teardown_method(self) -> None:
        deps.set_simulation_service_registry({})
        deps.set_simulation_service(None)

    def _both(self) -> tuple[MagicMock, MagicMock]:
        batch, ray = MagicMock(name="k8s"), MagicMock(name="ray")
        deps.set_simulation_service_registry({ComputeBackend.BATCH: batch, ComputeBackend.RAY: ray})
        deps.set_simulation_service(batch)  # default = batch
        return batch, ray

    def test_for_repo_routes_by_repo(self) -> None:
        batch, ray = self._both()
        assert deps.get_simulation_service_for_repo("https://github.com/vivarium-collective/v2ecoli") is ray
        assert deps.get_simulation_service_for_repo("https://github.com/CovertLab/vEcoli") is batch

    def test_for_repo_unknown_repo_uses_default(self) -> None:
        batch, _ = self._both()
        assert deps.get_simulation_service_for_repo("https://github.com/x/y") is batch  # default

    def test_for_repo_unconfigured_backend_uses_default(self) -> None:
        # Only Ray registered; a vEcoli repo (→BATCH) isn't in the registry → default (ray).
        ray = MagicMock(name="ray")
        deps.set_simulation_service_registry({ComputeBackend.RAY: ray})
        deps.set_simulation_service(ray)
        assert deps.get_simulation_service_for_repo("https://github.com/CovertLab/vEcoli") is ray

    def test_for_job_routes_by_backend(self) -> None:
        batch, ray = self._both()
        assert deps.get_simulation_service_for_job(JobId.k8s("j")) is batch
        assert deps.get_simulation_service_for_job(JobId.ray("j")) is ray
        # LOCAL (build) jobs go to the default — every service shares one LocalTaskService.
        assert deps.get_simulation_service_for_job(JobId.local("j")) is batch


class TestInitRegistry:
    """_init_simulation_service builds every configured backend, with the default selected."""

    def teardown_method(self) -> None:
        deps.set_simulation_service_registry({})
        deps.set_simulation_service(None)

    def test_builds_both_when_batch_and_ray_configured(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # K8sJobService loads kube config on construct — stub it.
        monkeypatch.setattr(
            "viva_api.common.hpc.k8s_job_service.K8sJobService", lambda namespace: MagicMock(name="k8s_job_svc")
        )
        settings = MagicMock(k8s_job_namespace="sms-api-stanford-test", ray_mnp_queue="smsvpctest-ray-mnp")

        deps._init_simulation_service("batch", settings)

        assert set(deps.global_simulation_services.keys()) == {ComputeBackend.BATCH, ComputeBackend.RAY}
        # default is the COMPUTE_BACKEND one (batch)
        assert deps.get_simulation_service() is deps.global_simulation_services[ComputeBackend.BATCH]
        # and a v2ecoli repo routes to Ray in this single deployment
        assert (
            deps.get_simulation_service_for_repo("https://github.com/vivarium-collective/v2ecoli")
            is deps.global_simulation_services[ComputeBackend.RAY]
        )

    def test_raises_when_default_backend_unconfigured(self) -> None:
        settings = MagicMock(k8s_job_namespace="", ray_mnp_queue="smsvpctest-ray-mnp")
        with pytest.raises(RuntimeError, match="COMPUTE_BACKEND=batch"):
            deps._init_simulation_service("batch", settings)
