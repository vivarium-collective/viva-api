"""Task 3 of the compose-results chain: chain the shared analysis DAG node
(``viva_api.common.analysis_dag.submit_analysis_dag_node``) onto a compose
Batch/Ray sim job when the request carries ``analysis_options``.

Mirrors the study Batch path's own chaining contract
(``SimulationServiceRay._submit_analysis_job``) and this dir's existing
``ComposeSimulationServiceRay`` unit-test conventions (mocked ``_ray``
plumbing, no AWS/docker) -- see ``test_simulation_service_ray.py``.
"""

from __future__ import annotations

import types
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from viva_api.compose import simulation_service_ray as mod
from viva_api.compose.container_def import ContainerizationFileRepr
from viva_api.compose.models import (
    ComposeSimulation,
    ComposeSimulationRequest,
    ComposeSimulatorVersion,
    SimulationFileType,
)
from viva_api.compose.simulation_service_ray import ComposeSimulationServiceRay
from viva_api.simulation import compose_analysis as chainer_mod
from viva_api.simulation.compose_analysis import ComposeAnalysisChainer
from viva_api.simulation.compose_simulators import simulator_environment_key
from viva_api.simulation.dispatch.batch_layer import BatchLayer
from viva_api.simulation.tables_orm import AnalysisStatusDB


# The runner is handed in, as the composition root hands it (P3d-4c-1); the tests only stage it.
def _RUNNER() -> str:
    return "print('a runner')\n"


_ANALYSIS_OPTIONS = {"report_cards": ["mass_conservation"]}


def _settings(**overrides: object) -> types.SimpleNamespace:
    base: dict[str, object] = {
        "compose_ray_image_tag": "abc123",
        "compose_parca_cache_dir": "",
        "compose_pbg_core_builder": "",
        "ecr_account_id": "111122223333",
        "batch_region": "us-gov-west-1",
        "ray_ecr_repository": "v2ecoli",
        "s3_work_bucket": "test-bucket",
        "s3_output_prefix": "vecoli-output",
        "ray_num_nodes": 4,
        "ray_container_job_definition": "smscdk-ray-container",
    }
    base.update(overrides)
    return types.SimpleNamespace(**base)


def _simulation(tmp_path: Path, *, analysis_options: dict[str, Any] | None) -> ComposeSimulation:
    doc_path = tmp_path / "input.pbg"
    doc_path.write_text("{}")
    return ComposeSimulation(
        database_id=1,
        sim_request=ComposeSimulationRequest(
            request_file_path=doc_path,
            simulation_file_type=SimulationFileType.PBG,
            is_batch=False,
            analysis_options=analysis_options,
        ),
        simulator_version=ComposeSimulatorVersion(
            database_id=1,
            singularity_def=ContainerizationFileRepr(representation="Bootstrap: docker\n"),
            singularity_def_hash="x",
            packages=None,
        ),
    )


@pytest.mark.asyncio
async def test_submit_simulation_job_chains_analysis_when_analysis_options_present(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(mod, "get_settings", lambda: _settings())
    monkeypatch.setattr(chainer_mod, "get_settings", lambda: _settings())

    simulation = _simulation(tmp_path, analysis_options=_ANALYSIS_OPTIONS)
    layer = BatchLayer()
    svc = ComposeSimulationServiceRay(
        runner_source=_RUNNER,
        batch=layer,
        after_submit=ComposeAnalysisChainer(layer),
        environment_key_of=simulator_environment_key,
    )

    monkeypatch.setattr(svc._batch, "ensure_mnp_job_def", lambda image, commit: "smscdk-ray-mnp:1")
    monkeypatch.setattr(svc._batch, "submit_mnp", lambda **kwargs: "compose-sim-job-1")

    captured_job_def_args: dict[str, str] = {}

    def _capture_ensure_container_job_def(image: str, commit: str) -> str:
        captured_job_def_args["image"] = image
        captured_job_def_args["commit"] = commit
        return "smscdk-ray-container:1"

    monkeypatch.setattr(svc._batch, "ensure_container_job_def", _capture_ensure_container_job_def)

    captured_submit_container: dict[str, object] = {}

    def _capture_submit_container(**kwargs: object) -> str:
        captured_submit_container.update(kwargs)
        return "analysis-job-1"

    monkeypatch.setattr(svc._batch, "submit_container", _capture_submit_container)

    fake_file_service = AsyncMock()
    fake_file_service.upload_file = AsyncMock()

    fake_db = AsyncMock()
    fake_db.record_analysis = AsyncMock()

    with (
        patch.object(svc, "_files", fake_file_service),
        patch("viva_api.dependencies.get_database_service", return_value=fake_db),
    ):
        await svc.submit_simulation_job(simulation, experiment_id="exp-1")

    # (a) the analysis job dependsOn the compose sim job id.
    assert captured_submit_container["depends_on"] == ["compose-sim-job-1"]

    # (b) it is pointed at the compose run's OWN output-store prefix...
    out_s3 = captured_submit_container["out_s3"]
    assert isinstance(out_s3, str)
    assert "exp-1" in out_s3
    job_cmd = captured_submit_container["job_cmd"]
    assert isinstance(job_cmd, str)
    from viva_api.common.storage import data_layout

    sweep_dir = data_layout.RayLayout.results_uri("exp-1").rstrip("/")
    assert sweep_dir in job_cmd

    # ...and its OWN cache -- the SAME commit/tag-keyed ParCa cache URI the compose
    # sim job itself stages (never a stock/unrelated per-commit path).
    expected_cache_prefix = data_layout.RayLayout.parca_cache_uri("abc123")
    assert expected_cache_prefix in job_cmd
    assert f"{expected_cache_prefix}simData.cPickle" in job_cmd

    # job def is a container-type def keyed by the same cache key (no per-run
    # commit resolved here, so the deploy-wide image tag).
    assert captured_job_def_args["commit"] == "abc123"

    # (c) it is recorded via the SAME analyses table GET /analyses/{id}/status reads.
    fake_db.record_analysis.assert_awaited_once()
    record_kwargs = fake_db.record_analysis.await_args.kwargs
    assert record_kwargs["experiment_id"] == "exp-1"
    assert record_kwargs["backend"] == "ray"
    assert record_kwargs["status"] == AnalysisStatusDB.COMPUTING
    assert record_kwargs["job_id_ext"] == "analysis-job-1"
    assert record_kwargs["config"]["analysis_options"]["experiment_id"] == ["exp-1"]


@pytest.mark.asyncio
async def test_submit_simulation_job_submits_no_analysis_when_analysis_options_absent(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(mod, "get_settings", lambda: _settings())
    monkeypatch.setattr(chainer_mod, "get_settings", lambda: _settings())

    simulation = _simulation(tmp_path, analysis_options=None)
    layer = BatchLayer()
    svc = ComposeSimulationServiceRay(
        runner_source=_RUNNER,
        batch=layer,
        after_submit=ComposeAnalysisChainer(layer),
        environment_key_of=simulator_environment_key,
    )

    monkeypatch.setattr(svc._batch, "ensure_mnp_job_def", lambda image, commit: "smscdk-ray-mnp:1")
    monkeypatch.setattr(svc._batch, "submit_mnp", lambda **kwargs: "compose-sim-job-1")

    ensure_container_job_def = AsyncMock()
    submit_container = AsyncMock()
    monkeypatch.setattr(svc._batch, "ensure_container_job_def", ensure_container_job_def)
    monkeypatch.setattr(svc._batch, "submit_container", submit_container)

    fake_file_service = AsyncMock()
    fake_file_service.upload_file = AsyncMock()

    fake_db = AsyncMock()
    fake_db.record_analysis = AsyncMock()

    with (
        patch.object(svc, "_files", fake_file_service),
        patch("viva_api.dependencies.get_database_service", return_value=fake_db),
    ):
        await svc.submit_simulation_job(simulation, experiment_id="exp-2")

    ensure_container_job_def.assert_not_called()
    submit_container.assert_not_called()
    fake_db.record_analysis.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_compose_service_with_no_hook_submits_the_run_and_chains_nothing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The analysis is the APPLICATION's, handed in as a hook (``docs/plan-core.md`` P3c). A compose
    service nobody handed one to -- a core with no application -- runs the composite and stops there:
    no container job, no database, no crash, even when the request carries ``analysis_options``."""
    monkeypatch.setattr(mod, "get_settings", lambda: _settings())
    svc = ComposeSimulationServiceRay(runner_source=_RUNNER, batch=BatchLayer())
    monkeypatch.setattr(svc._batch, "ensure_mnp_job_def", lambda image, commit: "smscdk-ray-mnp:1")
    monkeypatch.setattr(svc._batch, "submit_mnp", lambda **kwargs: "compose-sim-job-1")
    for name in ("ensure_container_job_def", "submit_container"):
        monkeypatch.setattr(svc._batch, name, lambda *a, _n=name, **k: pytest.fail(f"{_n} was called"))

    with patch.object(svc, "_files", AsyncMock()):
        job_id = await svc.submit_simulation_job(
            _simulation(tmp_path, analysis_options=_ANALYSIS_OPTIONS), experiment_id="exp-1"
        )
    assert job_id == "compose-sim-job-1"


def test_the_sms_chainer_is_the_hook_compose_declares() -> None:
    from viva_api.compose.simulation_service_ray import AfterSubmit

    hook: AfterSubmit = ComposeAnalysisChainer(BatchLayer())  # the assignment is the assertion (mypy checks tests)
    assert callable(hook)
