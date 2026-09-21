"""A compose run may name a registered ENVIRONMENT instead of a simulator's image (``docs/plan-core.md`` P2.3d-3).

``environment="runtime"`` runs the composite as ONE container in the core runtime image: for a
composite that needs only what process-bigraph ships, no simulator is involved at all -- no commit,
no ParCa cache, no Ray cluster, no chained analysis. What a CLIENT sees does not change: the same
runner, the same command, the same results prefix.
"""

import types
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException

import viva_api.compose.simulation_service_ray as mod
from viva_api.api.routers import compose as compose_router
from viva_api.compose.container_def import ContainerizationFileRepr
from viva_api.compose.models import (
    ComposeSimulation,
    ComposeSimulationRequest,
    ComposeSimulatorVersion,
    SimulationFileType,
)
from viva_api.compose.simulation_service_ray import COMPOSE_OUT_DIR, ComposeSimulationServiceRay
from viva_api.simulation.dispatch.batch_layer import BatchLayer

RUNTIME = "ghcr.io/vivarium-collective/viva-core-runtime:0.1.0"


def _settings(**overrides: object) -> types.SimpleNamespace:
    return types.SimpleNamespace(**{
        "ecr_account_id": "476270107793",
        "batch_region": "us-gov-west-1",
        "ray_ecr_repository": "v2ecoli",
        "compose_ray_image_tag": "deploy-wide-tag",
        "compose_pbg_core_builder": "v2ecoli.core:build_core",
        "compose_parca_cache_dir": "/app/v2ecoli/out/cache",
        "ray_num_nodes": 4,
        "core_runtime_image": RUNTIME,
        **overrides,
    })


def _request(tmp_path: Path, **fields: Any) -> ComposeSimulationRequest:
    doc = tmp_path / "input.pbg"
    doc.write_text("{}")
    return ComposeSimulationRequest(
        request_file_path=doc, simulation_file_type=SimulationFileType.PBG, is_batch=False, **fields
    )


def _simulation(request: ComposeSimulationRequest) -> ComposeSimulation:
    version = ComposeSimulatorVersion(
        database_id=1,
        singularity_def=ContainerizationFileRepr(representation="Bootstrap: docker\n"),
        singularity_def_hash="x",
        packages=None,
    )
    return ComposeSimulation(database_id=1, sim_request=request, simulator_version=version)


# ------------------------------------------------------------------ the service


@pytest.mark.asyncio
async def test_a_composite_in_the_runtime_environment_runs_as_one_container_and_needs_no_simulator(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(mod, "get_settings", _settings)
    svc = ComposeSimulationServiceRay(batch=BatchLayer())
    job_defs: list[tuple[str, str]] = []
    submitted: dict[str, object] = {}

    def submit_container(**kwargs: object) -> str:
        submitted.update(kwargs)
        return "container-job-id"

    def ensure_container_job_def(image: str, key: str) -> str:
        job_defs.append((image, key))
        return "jd:1"

    monkeypatch.setattr(svc._batch, "ensure_container_job_def", ensure_container_job_def)
    monkeypatch.setattr(svc._batch, "submit_container", submit_container)
    # everything that belongs to a simulator's image must stay untouched
    for name in ("ensure_mnp_job_def", "submit_mnp", "image_uri"):
        monkeypatch.setattr(svc._batch, name, lambda *a, _n=name, **k: pytest.fail(f"{_n} was called"))
    monkeypatch.setattr(svc, "_resolve_commit", AsyncMock(side_effect=AssertionError("no simulator is resolved")))

    with patch("viva_api.dependencies.get_file_service", return_value=AsyncMock()):
        job_id = await svc.submit_simulation_job(
            _simulation(_request(tmp_path, environment="runtime")), experiment_id="exp-1"
        )

    assert job_id == "container-job-id"
    assert job_defs == [(RUNTIME, "viva-core-runtime-0-1-0")]
    assert submitted["job_definition"] == "jd:1" and submitted["out_dir"] == COMPOSE_OUT_DIR
    assert str(submitted["out_s3"]).rstrip("/").endswith("/exp-1")  # the SAME results prefix any compose run uses
    assert "stage_s3" not in submitted  # no ParCa cache: nothing here is a whole-cell model
    command = str(submitted["job_cmd"])
    assert "/input.pbg" in command and "/run_pbg.py" in command and "-n 1" in command
    assert "PBG_REQUIRE_OUTPUT=1" in command
    assert "PBG_CORE_BUILDER" not in command  # the workspace's core builder is not in the runtime image


@pytest.mark.asyncio
async def test_without_an_environment_a_compose_run_is_the_multi_node_job_it_always_was(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(mod, "get_settings", lambda: _settings(compose_parca_cache_dir=""))
    svc = ComposeSimulationServiceRay(batch=BatchLayer())
    monkeypatch.setattr(svc._batch, "ensure_mnp_job_def", lambda image, commit: "mnp:1")
    submitted: dict[str, object] = {}

    def submit_mnp(**kwargs: object) -> str:
        submitted.update(kwargs)
        return "mnp-job-id"

    monkeypatch.setattr(svc._batch, "submit_mnp", submit_mnp)
    monkeypatch.setattr(svc._batch, "submit_container", lambda **kw: pytest.fail("container path taken"))

    with patch("viva_api.dependencies.get_file_service", return_value=AsyncMock()):
        assert await svc.submit_simulation_job(_simulation(_request(tmp_path)), experiment_id="exp-1") == "mnp-job-id"
    assert submitted["num_nodes"] == 4
    assert "PBG_CORE_BUILDER=v2ecoli.core:build_core" in str(submitted["ray_job_cmd"])


# ------------------------------------------------------------------ the router refuses BEFORE dispatching


def test_a_named_environment_is_checked_before_anything_is_dispatched(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The submission runs in a background task, where a refusal would reach nobody."""
    monkeypatch.setattr(compose_router, "get_settings", _settings)
    compose_router._check_environment(_request(tmp_path))  # nothing named: nothing to check
    compose_router._check_environment(_request(tmp_path, environment="runtime"))

    with pytest.raises(HTTPException) as unknown:
        compose_router._check_environment(_request(tmp_path, environment="copasi"))
    assert unknown.value.status_code == 422 and "unknown environment" in str(unknown.value.detail)

    mixtures: list[dict[str, Any]] = [{"simulator_id": 7}, {"num_nodes": 2}, {"analysis_options": {"multiseed": {}}}]
    for theirs in mixtures:
        with pytest.raises(HTTPException) as mixed:
            compose_router._check_environment(_request(tmp_path, environment="runtime", **theirs))
        assert mixed.value.status_code == 422 and next(iter(theirs)) in str(mixed.value.detail)

    monkeypatch.setattr(compose_router, "get_settings", lambda: _settings(core_runtime_image=""))
    with pytest.raises(HTTPException) as unavailable:
        compose_router._check_environment(_request(tmp_path, environment="runtime"))
    assert unavailable.value.status_code == 501 and "not available here" in str(unavailable.value.detail)
