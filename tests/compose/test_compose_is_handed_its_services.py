"""Compose is HANDED the application's services; it looks none of them up (``docs/plan-core.md`` P3d-3).

Four lookups in ``viva_api.dependencies`` -- the file service, SMS's simulator registry, and the SLURM
SSH sessions twice -- were made at the moment of use from inside the compose package. That module is
the application's composition root: a package that imports it cannot move into core.
"""

import ast
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from viva_api.compose.job_monitor import ComposeJobMonitor
from viva_api.compose.simulation_service import ComposeSimulationServiceHpc
from viva_api.compose.simulation_service_ray import ComposeSimulationServiceRay, EnvironmentKeyOf
from viva_api.simulation.compose_simulators import simulator_environment_key

COMPOSE = Path("viva_api/compose")


def test_nothing_in_the_compose_package_imports_the_applications_composition_root() -> None:
    """Parsed, not grepped: a lazy import inside a function is still an import."""
    offenders: list[str] = []
    for source in sorted(COMPOSE.rglob("*.py")):
        for node in ast.walk(ast.parse(source.read_text(encoding="utf-8"))):
            if isinstance(node, ast.ImportFrom):
                names = [node.module or ""]
            elif isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            else:
                continue
            offenders += [f"{source}:{node.lineno}" for name in names if name.startswith("viva_api.dependencies")]
    assert not offenders, f"compose looks a service up in the application's module: {offenders}"


@pytest.mark.asyncio
async def test_the_simulator_registry_that_was_handed_in_is_the_one_asked() -> None:
    asked = AsyncMock(return_value="9e2040093e")
    svc = ComposeSimulationServiceRay(batch=MagicMock(), environment_key_of=asked)
    assert await svc._resolve_commit(42) == "9e2040093e"
    asked.assert_awaited_once_with(42)


@pytest.mark.asyncio
async def test_a_simulator_id_with_no_registry_is_refused_not_run_on_some_other_image() -> None:
    svc = ComposeSimulationServiceRay(batch=MagicMock())
    assert await svc._resolve_commit(None) is None  # no simulator named: nothing to ask
    with pytest.raises(RuntimeError, match="No simulator registry"):
        await svc._resolve_commit(42)


@pytest.mark.asyncio
async def test_with_no_file_service_a_run_is_refused_before_anything_is_submitted() -> None:
    batch = MagicMock()
    svc = ComposeSimulationServiceRay(batch=batch)
    with pytest.raises(RuntimeError, match="FileService not initialized"):
        await svc.submit_simulation_job(MagicMock(), experiment_id="exp-1")
    batch.submit_mnp.assert_not_called()


def test_the_sms_lookup_is_the_hook_compose_declares() -> None:
    hook: EnvironmentKeyOf = simulator_environment_key  # mypy checks the shape; this pins the name
    assert callable(hook)


@pytest.mark.asyncio
async def test_slurm_is_reached_through_the_provider_and_only_when_it_is_needed() -> None:
    """A site without SLURM builds both objects and never has SSH sessions: the provider is asked
    at the moment of use, and its absence is said by name."""
    provider = MagicMock(side_effect=RuntimeError("SSHSessionService 'slurm' not initialized"))
    monitor = ComposeJobMonitor(nats_client=None, database_service=MagicMock(), slurm_ssh=provider)
    ComposeSimulationServiceHpc(env=MagicMock(), slurm_ssh=provider)
    provider.assert_not_called()  # building them asked for nothing

    run = MagicMock(slurmjobid=7)
    with pytest.raises(RuntimeError, match="not initialized"):
        await monitor._update_slurm_jobs([run])
    provider.assert_called_once_with()

    unwired = ComposeJobMonitor(nats_client=None, database_service=MagicMock())
    with pytest.raises(RuntimeError, match="No SLURM SSH session provider"):
        await unwired._update_slurm_jobs([run])


def test_the_sms_hooks_reader_reads_the_hooks_and_core_reads_its_own_runner() -> None:
    """Core stages ITS runner; the application hands compose the hooks' text (P3d-4c-2)."""
    from viva_api.compose.handlers import hooks_source
    from viva_core.compose.runner_files import runner_source

    assert "def emitter_override(" in hooks_source() and "batch_baseline_composite_ids" in hooks_source()
    assert "def _load_hooks(" in runner_source()


def test_the_compose_command_copies_the_hooks_beside_the_runner_only_when_it_has_some() -> None:
    """Core's runner is generic; an application's hooks are copied beside it and named to it, and only
    on the application's own image (P3d-4c-2)."""
    from viva_core.compose.runner_files import HOOKS_ENV, hooks_s3_uri

    svc = ComposeSimulationServiceRay(batch=MagicMock(), runner_hooks=lambda: "hooks")
    runner = "s3://b/out/exp-1/run_pbg.py"
    with patch("viva_core.compose.simulation_service_ray.get_settings") as settings:
        settings.return_value.compose_pbg_core_builder = ""
        with_hooks = svc._compose_command("s3://b/out/exp-1/input.pbg", runner, 5, hooks=True)
        without = svc._compose_command("s3://b/out/exp-1/input.pbg", runner, 5, hooks=False)
    assert f"aws s3 cp {hooks_s3_uri(runner)} /tmp/runner_hooks.py" in with_hooks and HOOKS_ENV in with_hooks
    assert "runner_hooks" not in without


@pytest.mark.asyncio
async def test_compose_stages_cores_runner_and_the_applications_hooks_beside_it() -> None:
    uploaded: list[str] = []

    async def upload_file(path: object, s3_path: object) -> object:
        uploaded.append(str(getattr(s3_path, "s3_path", s3_path)))
        return s3_path

    files = MagicMock()
    files.upload_file = upload_file
    batch = MagicMock()
    batch.ensure_mnp_job_def.return_value = "mnp:1"
    batch.submit_mnp.return_value = "job-1"
    svc = ComposeSimulationServiceRay(batch=batch, files=files, runner_hooks=lambda: "hooks text")
    request = MagicMock()
    request.sim_request.request_file_path = __file__
    request.sim_request.environment = None
    request.sim_request.simulator_id = None
    request.sim_request.end_time_point = 3
    request.sim_request.num_nodes = None
    with patch("viva_core.compose.simulation_service_ray.get_settings") as settings:
        s = settings.return_value
        s.s3_work_bucket, s.s3_output_prefix = "b", "out"
        s.compose_ray_image_tag, s.compose_pbg_core_builder, s.ray_num_nodes = "tag", "", 1
        s.ecr_account_id, s.batch_region, s.ray_ecr_repository = "1", "r", "repo"
        await svc.submit_simulation_job(request, experiment_id="exp-1")
    assert [u.rsplit("/", 1)[1] for u in uploaded] == ["input.pbg", "run_pbg.py", "runner_hooks.py"]
    assert "PBG_RUNNER_HOOKS=runner_hooks" in batch.submit_mnp.call_args.kwargs["ray_job_cmd"]
