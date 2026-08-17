"""ComposeSimulationServiceRay unit tests (no AWS): command shape + backend flags."""

import types
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from viva_api.common.hpc.job_service import JobStatusInfo
from viva_api.common.models import JobBackend, JobId, JobStatus
from viva_api.compose import simulation_service_ray as mod
from viva_api.compose.container_def import ContainerizationFileRepr
from viva_api.compose.models import (
    ComposeAnalysis,
    ComposeAnalysisStatus,
    ComposeSimulation,
    ComposeSimulationRequest,
    ComposeSimulatorVersion,
    SimulationFileType,
)
from viva_api.compose.simulation_service_ray import ComposeSimulationServiceRay


def _settings(**overrides: object) -> types.SimpleNamespace:
    base: dict[str, object] = {
        "compose_ray_image_tag": "abc123",
        "compose_parca_cache_dir": "",
        "compose_pbg_core_builder": "",
        "ecr_account_id": "111122223333",
        "batch_region": "us-gov-west-1",
        "ray_ecr_repository": "v2ecoli",
    }
    base.update(overrides)
    return types.SimpleNamespace(**base)


def test_backend_flags() -> None:
    svc = ComposeSimulationServiceRay()
    assert svc.backend == JobBackend.RAY
    assert svc.requires_container_build is False


def test_compose_command_stages_doc_and_runner_from_s3() -> None:
    svc = ComposeSimulationServiceRay()
    cmd = svc._compose_command("s3://bucket/exp/input.pbg", "s3://bucket/exp/run_pbg.py", steps=7)
    # downloads BOTH the doc and the runner from S3, then runs with -n steps
    assert "aws s3 cp s3://bucket/exp/input.pbg" in cmd
    assert "aws s3 cp s3://bucket/exp/run_pbg.py" in cmd
    assert "-n 7" in cmd
    assert mod.COMPOSE_OUT_DIR in cmd


def test_compose_command_stays_under_batch_8192_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    """AWS Batch rejects a container override command longer than 8192 bytes with
    'Container Overrides length must be at most 8192'. The runner is fetched from S3
    (not embedded) precisely so the command length is independent of run_pbg.py's
    size — assert it stays comfortably under, with a realistically-long core builder.
    """
    monkeypatch.setattr(
        mod, "get_settings", lambda: _settings(compose_pbg_core_builder="some.long.workspace.module:build_core")
    )
    cmd = ComposeSimulationServiceRay()._compose_command(
        "s3://bucket/very/long/experiment/prefix/input.pbg",
        "s3://bucket/very/long/experiment/prefix/run_pbg.py",
        steps=1000,
    )
    assert len(cmd) < 8192, f"compose command is {len(cmd)} bytes — over the Batch 8192 limit"
    # and it must NOT inline the runner source (the regression this guards)
    assert "def _redirect_emitters" not in cmd
    assert "PBG_RUNNER_EOF" not in cmd


# --- B1: an unset image tag must fail at SUBMIT, not as an opaque Batch pull error ---


def test_image_uri_raises_when_tag_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    """The ECR repo is populated per-commit and has no "latest", so an unset tag can
    only ever resolve to a nonexistent image. Fail here, naming the setting."""
    monkeypatch.setattr(mod, "get_settings", lambda: _settings(compose_ray_image_tag=""))
    with pytest.raises(RuntimeError, match="compose_ray_image_tag"):
        ComposeSimulationServiceRay()._image_uri()


def test_image_uri_builds_the_commit_pinned_uri(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mod, "get_settings", lambda: _settings(compose_ray_image_tag="a08e20bd"))
    uri = ComposeSimulationServiceRay()._image_uri()
    assert uri == "111122223333.dkr.ecr.us-gov-west-1.amazonaws.com/v2ecoli:a08e20bd"


# --- B2: the driver swap must not drop the ensemble path's ParCa cache staging ---


def test_parca_staging_disabled_when_no_cache_dir(monkeypatch: pytest.MonkeyPatch) -> None:
    """Generic default: a composite that needs no prebuilt cache stages nothing."""
    monkeypatch.setattr(mod, "get_settings", lambda: _settings(compose_parca_cache_dir=""))
    assert ComposeSimulationServiceRay()._parca_staging() == (None, None)


def test_parca_staging_is_keyed_by_the_image_tag_commit(monkeypatch: pytest.MonkeyPatch) -> None:
    """Regression: the compose path called _submit_mnp WITHOUT stage_s3/stage_dir, so
    a composite whose cache_dir expects a populated ParCa bundle (v2ecoli baseline)
    started against an empty directory. The cache is commit-addressed and the image
    tag IS the commit."""
    monkeypatch.setattr(
        mod,
        "get_settings",
        lambda: _settings(compose_ray_image_tag="a08e20bd", compose_parca_cache_dir="/app/v2ecoli/out/cache"),
    )
    stage_s3, stage_dir = ComposeSimulationServiceRay()._parca_staging()
    assert stage_dir == "/app/v2ecoli/out/cache"
    assert stage_s3 is not None
    assert stage_s3.endswith("ray-parca-cache/a08e20bd/")


# --- B3: name the workspace's own core builder so its registered TYPES resolve ---


def test_compose_command_passes_core_builder_when_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    # The runner is no longer inlined, so the whole command is the exec line — a plain
    # substring check is meaningful (it can't false-match on the runner's own source).
    monkeypatch.setattr(mod, "get_settings", lambda: _settings(compose_pbg_core_builder="v2ecoli.core:build_core"))
    cmd = ComposeSimulationServiceRay()._compose_command("s3://b/i.pbg", "s3://b/run_pbg.py", steps=3)
    assert "PBG_CORE_BUILDER=v2ecoli.core:build_core" in cmd


def test_compose_command_omits_core_builder_when_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mod, "get_settings", lambda: _settings(compose_pbg_core_builder=""))
    cmd = ComposeSimulationServiceRay()._compose_command("s3://b/i.pbg", "s3://b/run_pbg.py", steps=3)
    assert "PBG_CORE_BUILDER" not in cmd


@pytest.mark.asyncio
async def test_submit_simulation_job_uses_the_unified_ray_num_nodes_setting(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Regression: compose used to read its OWN compose_ray_num_nodes setting,
    independent of the ensemble sim path's ray_num_nodes -- both ultimately call the
    same shared SimulationServiceRay._submit_mnp(), so two settings could (and did)
    drift apart. The CDK-side 24-node capacity scale-up only ever updated
    compose_ray_num_nodes, silently leaving the actually-used ensemble sim path stuck
    at 4 (live-reproduced 2026-08-05: a real 1000x10 baseline job ran on 4 nodes
    instead of 24, ~14-15 min/gen instead of ~8, had to be killed). Now there is only
    ray_num_nodes; this proves the compose path actually reads it."""
    monkeypatch.setattr(mod, "get_settings", lambda: _settings(ray_num_nodes=24, compose_parca_cache_dir=""))

    doc_path = tmp_path / "input.pbg"
    doc_path.write_text("{}")
    simulation = ComposeSimulation(
        database_id=1,
        sim_request=ComposeSimulationRequest(
            request_file_path=doc_path, simulation_file_type=SimulationFileType.PBG, is_batch=False
        ),
        simulator_version=ComposeSimulatorVersion(
            database_id=1,
            singularity_def=ContainerizationFileRepr(representation="Bootstrap: docker\n"),
            singularity_def_hash="x",
            packages=None,
        ),
    )

    svc = ComposeSimulationServiceRay()
    monkeypatch.setattr(svc._ray, "_ensure_mnp_job_def", lambda image, commit: "smscdk-ray-mnp:1")

    captured: dict[str, object] = {}

    def _capture_submit_mnp(**kwargs: object) -> str:
        captured.update(kwargs)
        return "batch-job-id"

    monkeypatch.setattr(svc._ray, "_submit_mnp", _capture_submit_mnp)

    fake_file_service = AsyncMock()
    fake_file_service.upload_file = AsyncMock()

    with patch("viva_api.dependencies.get_file_service", return_value=fake_file_service):
        await svc.submit_simulation_job(simulation, experiment_id="exp-1")

    assert captured["num_nodes"] == 24


@pytest.mark.asyncio
class TestGetJobStatusInfo:
    """get_job_status_info surfaces the full JobStatusInfo (incl. exit_code/
    error_message) — item 50 Gap 6's OOM-retry-escalation poller needs this; the
    existing get_job_status only returns the coarse ComposeJobStatus enum."""

    async def test_delegates_to_the_shared_ray_service_and_returns_full_info(self) -> None:
        svc = ComposeSimulationServiceRay()
        expected = JobStatusInfo(
            job_id=JobId.ray("job-1"),
            status=JobStatus.FAILED,
            start_time=None,
            end_time=None,
            exit_code="137",
            error_message="OOM killed",
        )
        svc._ray.get_job_status = AsyncMock(return_value=expected)  # type: ignore[method-assign]

        info = await svc.get_job_status_info("job-1")

        assert info is expected
        svc._ray.get_job_status.assert_awaited_once_with(JobId.ray("job-1"))

    async def test_none_when_the_underlying_job_is_not_found(self) -> None:
        svc = ComposeSimulationServiceRay()
        svc._ray.get_job_status = AsyncMock(return_value=None)  # type: ignore[method-assign]
        assert await svc.get_job_status_info("nope") is None


@pytest.mark.asyncio
class TestSubmitAnalysis:
    """submit_analysis: the compose-native analysis-DAG-node submit, item 50 Gap 6."""

    async def test_submits_the_v2ecoli_analysis_command_and_records_a_compose_analysis_row(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        svc = ComposeSimulationServiceRay()
        monkeypatch.setattr(mod, "get_settings", lambda: _settings(compose_ray_image_tag="commit123"))

        job_def_calls: list[dict[str, object]] = []
        monkeypatch.setattr(
            svc._ray,
            "_ensure_mnp_job_def",
            lambda image, commit, memory_mib=None: (
                job_def_calls.append({"image": image, "commit": commit, "memory_mib": memory_mib}),
                "smscdk-ray-mnp-commit123:1",
            )[1],
        )

        captured_command: dict[str, object] = {}
        monkeypatch.setattr(
            svc._ray,
            "_analysis_command",
            lambda **kwargs: (captured_command.update(kwargs), "cd /app/v2ecoli && python scripts/run...")[1],
        )

        captured_submit: dict[str, object] = {}

        def _capture_submit_mnp(**kwargs: object) -> str:
            captured_submit.update(kwargs)
            return "analysis-batch-job-id"

        monkeypatch.setattr(svc._ray, "_submit_mnp", _capture_submit_mnp)

        # a plain MagicMock, not AsyncMock: get_analysis_db() is a SYNC accessor on
        # the real ComposeDatabaseService (returns the sub-service directly, not a
        # coroutine) — only insert_analysis itself is async.
        db_service = MagicMock()
        inserted = ComposeAnalysis(
            database_id=7,
            name="analysis-exp-1-abcdef",
            config={},
            simulation_id=42,
            job_id_ext="analysis-batch-job-id",
            status=ComposeAnalysisStatus.COMPUTING,
        )
        db_service.get_analysis_db.return_value.insert_analysis = AsyncMock(return_value=inserted)

        result = await svc.submit_analysis(
            experiment_id="exp-1",
            simulation_id=42,
            db_service=db_service,
            n_seeds=2,
            n_generations=2,
            modules="applicable",
        )

        assert result is inserted
        # the job def is derived from the compose image tag, no memory override on a
        # first-attempt submit
        assert job_def_calls[0]["commit"] == "commit123"
        assert job_def_calls[0]["memory_mib"] is None
        # the analysis command was built with the caller's real params
        assert captured_command["experiment_id"] == "exp-1"
        assert captured_command["n_seeds"] == 2
        assert captured_command["n_generations"] == 2
        assert captured_command["modules"] == "applicable"
        assert captured_command["commit"] == "commit123"
        # the Batch job was submitted with no dependency (analysis has nothing
        # upstream to natively dependsOn at submit time)
        assert captured_submit["depends_on"] is None
        assert captured_submit["depends_type"] is None
        assert captured_submit["out_dir"] == mod.ANALYSIS_OUT_DIR
        # recorded against the compose_analysis table with the real returned job id
        insert_kwargs = db_service.get_analysis_db.return_value.insert_analysis.call_args.kwargs
        assert insert_kwargs["job_id_ext"] == "analysis-batch-job-id"
        assert insert_kwargs["simulation_id"] == 42
        assert insert_kwargs["job_backend"] == JobBackend.RAY.value
        assert insert_kwargs["config"]["n_seeds"] == 2
        assert insert_kwargs["config"]["analysis_name"] == captured_command["analysis_name"]


@pytest.mark.asyncio
class TestResubmitAnalysis:
    """resubmit_analysis: the OOM-retry-escalation resubmit half, item 50 Gap 6."""

    async def test_resubmits_at_escalated_memory_reusing_the_original_analysis_name(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        svc = ComposeSimulationServiceRay()
        monkeypatch.setattr(mod, "get_settings", lambda: _settings(compose_ray_image_tag="commit123"))

        job_def_calls: list[dict[str, object]] = []
        monkeypatch.setattr(
            svc._ray,
            "_ensure_mnp_job_def",
            lambda image, commit, memory_mib=None: (
                job_def_calls.append({"memory_mib": memory_mib}),
                "smscdk-ray-mnp-commit123-mem120000:1",
            )[1],
        )

        captured_command: dict[str, object] = {}
        monkeypatch.setattr(
            svc._ray,
            "_analysis_command",
            lambda **kwargs: (captured_command.update(kwargs), "cd /app/v2ecoli && python scripts/run...")[1],
        )
        monkeypatch.setattr(svc._ray, "_submit_mnp", lambda **kwargs: "retry-batch-job-id")

        analysis = ComposeAnalysis(
            database_id=7,
            name="analysis-exp-1-abcdef",
            config={
                "experiment_id": "exp-1",
                "n_seeds": 2,
                "n_generations": 2,
                "modules": "applicable",
                "analysis_name": "analysis-exp-1-abcdef",
            },
            simulation_id=42,
            job_id_ext="analysis-batch-job-id",
            status=ComposeAnalysisStatus.COMPUTING,
            attempt=1,
        )

        new_job_id = await svc.resubmit_analysis(analysis, memory_mib=120000)

        assert new_job_id == "retry-batch-job-id"
        assert job_def_calls[0]["memory_mib"] == 120000
        # replays the SAME analysis_name -- retry output lands in the same S3 prefix
        assert captured_command["analysis_name"] == "analysis-exp-1-abcdef"
        assert captured_command["n_seeds"] == 2
        assert captured_command["n_generations"] == 2
