"""ComposeSimulationServiceRay unit tests (no AWS): command shape + backend flags."""

import types
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from viva_api.common.models import JobBackend
from viva_api.compose import simulation_service_ray as mod
from viva_api.compose.container_def import ContainerizationFileRepr
from viva_api.compose.models import (
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


# --- item 98: resolved per-run simulator_id overrides the deploy-wide static image ---


def test_image_uri_with_commit_delegates_to_the_shared_ensemble_primitive(monkeypatch: pytest.MonkeyPatch) -> None:
    """A resolved commit (item 98) must take the TRUE-commit-image shape the vEcoli
    ensemble path already uses -- not re-derive it -- so the two paths can never drift.
    Asserted by direct equality against the real delegation target, not a hardcoded
    string, so this doesn't need to know or duplicate that primitive's own format."""
    monkeypatch.setattr(mod, "get_settings", lambda: _settings(compose_ray_image_tag="deploy-wide-tag"))
    svc = ComposeSimulationServiceRay()
    uri = svc._image_uri(commit="a-different-per-run-commit")
    assert uri == svc._ray._image_uri("a-different-per-run-commit")
    # NOT the static deploy-wide tag -- the resolved per-run commit took over.
    assert "deploy-wide-tag" not in uri
    assert "a-different-per-run-commit" in uri


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


def test_parca_staging_with_commit_keys_by_the_resolved_commit_not_the_deploy_tag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """item 98: a resolved per-run simulator_id implies a real, distinct commit --
    staging the deploy-wide tag's cache instead would silently serve the WRONG
    commit's ParCa data. Regression target: this must key off `commit`, not the tag."""
    monkeypatch.setattr(
        mod,
        "get_settings",
        lambda: _settings(compose_ray_image_tag="deploy-wide-tag", compose_parca_cache_dir="/app/v2ecoli/out/cache"),
    )
    stage_s3, stage_dir = ComposeSimulationServiceRay()._parca_staging(commit="resolved-per-run-commit")
    assert stage_dir == "/app/v2ecoli/out/cache"
    assert stage_s3 is not None
    assert stage_s3.endswith("ray-parca-cache/resolved-per-run-commit/")
    assert "deploy-wide-tag" not in stage_s3


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


# --- item 98: _resolve_commit against the LEGACY simulator registry ---


@pytest.mark.asyncio
async def test_resolve_commit_returns_none_when_simulator_id_unset() -> None:
    """None preserves today's exact behavior (the deploy-wide static image) and must
    not touch the database service at all."""
    with patch("viva_api.dependencies.get_database_service") as get_db:
        assert await ComposeSimulationServiceRay()._resolve_commit(None) is None
        get_db.assert_not_called()


@pytest.mark.asyncio
async def test_resolve_commit_resolves_the_git_commit_hash() -> None:
    fake_simulator = types.SimpleNamespace(git_commit_hash="9e2040093e", git_repo_url="x", git_branch="main")
    fake_db = AsyncMock()
    fake_db.get_simulator = AsyncMock(return_value=fake_simulator)
    with patch("viva_api.dependencies.get_database_service", return_value=fake_db):
        commit = await ComposeSimulationServiceRay()._resolve_commit(42)
    assert commit == "9e2040093e"
    fake_db.get_simulator.assert_awaited_once_with(simulator_id=42)


@pytest.mark.asyncio
async def test_resolve_commit_raises_when_simulator_not_found() -> None:
    """Fail loud, naming the id -- matching the same convention the ensemble path
    uses for an unresolvable simulator_id (simulation_service_ray.py)."""
    fake_db = AsyncMock()
    fake_db.get_simulator = AsyncMock(return_value=None)
    with (
        patch("viva_api.dependencies.get_database_service", return_value=fake_db),
        pytest.raises(ValueError, match="Simulator 42 not found"),
    ):
        await ComposeSimulationServiceRay()._resolve_commit(42)


@pytest.mark.asyncio
async def test_resolve_commit_raises_when_database_service_not_initialized() -> None:
    with (
        patch("viva_api.dependencies.get_database_service", return_value=None),
        pytest.raises(RuntimeError, match="Database service not initialized"),
    ):
        await ComposeSimulationServiceRay()._resolve_commit(42)


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
async def test_submit_simulation_job_with_simulator_id_uses_the_resolved_per_commit_image(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """item 98 end-to-end: a request carrying simulator_id must dispatch against that
    build's OWN commit -- image, job-def revision key, AND ParCa cache staging -- not
    the deploy-wide static tag, and without touching ComposeSimulatorVersion (a
    different, container-def-based identity tracked regardless of this field)."""
    monkeypatch.setattr(
        mod,
        "get_settings",
        lambda: _settings(
            compose_ray_image_tag="deploy-wide-tag",
            compose_parca_cache_dir="/app/v2ecoli/out/cache",
            ray_num_nodes=4,
        ),
    )

    doc_path = tmp_path / "input.pbg"
    doc_path.write_text("{}")
    simulation = ComposeSimulation(
        database_id=1,
        sim_request=ComposeSimulationRequest(
            request_file_path=doc_path,
            simulation_file_type=SimulationFileType.PBG,
            is_batch=False,
            simulator_id=42,
        ),
        simulator_version=ComposeSimulatorVersion(
            database_id=1,
            singularity_def=ContainerizationFileRepr(representation="Bootstrap: docker\n"),
            singularity_def_hash="x",
            packages=None,
        ),
    )

    svc = ComposeSimulationServiceRay()

    captured_job_def_args: dict[str, str] = {}

    def _capture_ensure_job_def(image: str, commit: str) -> str:
        captured_job_def_args["image"] = image
        captured_job_def_args["commit"] = commit
        return "smscdk-ray-mnp:1"

    monkeypatch.setattr(svc._ray, "_ensure_mnp_job_def", _capture_ensure_job_def)

    captured_submit: dict[str, object] = {}

    def _capture_submit_mnp(**kwargs: object) -> str:
        captured_submit.update(kwargs)
        return "batch-job-id"

    monkeypatch.setattr(svc._ray, "_submit_mnp", _capture_submit_mnp)

    fake_file_service = AsyncMock()
    fake_file_service.upload_file = AsyncMock()
    fake_simulator = types.SimpleNamespace(git_commit_hash="resolved-commit-42", git_repo_url="x", git_branch="main")
    fake_db = AsyncMock()
    fake_db.get_simulator = AsyncMock(return_value=fake_simulator)

    with (
        patch("viva_api.dependencies.get_file_service", return_value=fake_file_service),
        patch("viva_api.dependencies.get_database_service", return_value=fake_db),
    ):
        await svc.submit_simulation_job(simulation, experiment_id="exp-1")

    fake_db.get_simulator.assert_awaited_once_with(simulator_id=42)
    assert captured_job_def_args["commit"] == "resolved-commit-42"
    assert "resolved-commit-42" in captured_job_def_args["image"]
    assert "deploy-wide-tag" not in captured_job_def_args["image"]
    stage_s3 = captured_submit["stage_s3"]
    assert isinstance(stage_s3, str)
    assert stage_s3.endswith("ray-parca-cache/resolved-commit-42/")


# --- item 102: per-request num_nodes override on ComposeSimulationRequest ---


@pytest.mark.asyncio
async def test_submit_simulation_job_with_explicit_num_nodes_overrides_the_deploy_default(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A request carrying num_nodes must override the deploy-wide ray_num_nodes
    setting -- the whole point of item 102 (previously fixed, not per-request)."""
    monkeypatch.setattr(mod, "get_settings", lambda: _settings(ray_num_nodes=4, compose_parca_cache_dir=""))

    doc_path = tmp_path / "input.pbg"
    doc_path.write_text("{}")
    simulation = ComposeSimulation(
        database_id=1,
        sim_request=ComposeSimulationRequest(
            request_file_path=doc_path,
            simulation_file_type=SimulationFileType.PBG,
            is_batch=False,
            num_nodes=16,
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

    assert captured["num_nodes"] == 16


@pytest.mark.asyncio
async def test_submit_simulation_job_omits_num_nodes_by_default(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """No num_nodes on the request -> the deploy-wide default, byte-for-byte
    today's exact behavior for every caller that doesn't know this field exists
    yet (mirrors the compute_backend/simulator_id fields' own default contract)."""
    monkeypatch.setattr(mod, "get_settings", lambda: _settings(ray_num_nodes=4, compose_parca_cache_dir=""))

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

    assert captured["num_nodes"] == 4
