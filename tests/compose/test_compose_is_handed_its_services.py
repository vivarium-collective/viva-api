"""Compose is HANDED the application's services; it looks none of them up (``docs/plan-core.md`` P3d-3).

Four lookups in ``viva_api.dependencies`` -- the file service, SMS's simulator registry, and the SLURM
SSH sessions twice -- were made at the moment of use from inside the compose package. That module is
the application's composition root: a package that imports it cannot move into core.
"""

import ast
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

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
