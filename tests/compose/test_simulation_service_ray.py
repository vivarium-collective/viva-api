"""ComposeSimulationServiceRay unit tests (no AWS): command shape + backend flags."""

import types
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from sms_api.common.models import JobBackend
from sms_api.compose import simulation_service_ray as mod
from sms_api.compose.container_def import ContainerizationFileRepr
from sms_api.compose.models import (
    ComposeSimulation,
    ComposeSimulationRequest,
    ComposeSimulatorVersion,
    SimulationFileType,
)
from sms_api.compose.simulation_service_ray import ComposeSimulationServiceRay


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

    with patch("sms_api.dependencies.get_file_service", return_value=fake_file_service):
        await svc.submit_simulation_job(simulation, experiment_id="exp-1")

    assert captured["num_nodes"] == 24
