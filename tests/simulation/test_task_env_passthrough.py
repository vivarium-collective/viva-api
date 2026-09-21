"""`task_env` reaches every simulation TASK on every Batch-launching path.

Offline: every submitter takes an explicit batch client (or is patched), the
settings are doubles, and nothing here opens a socket.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from tests.simulation.test_ray_backend import (
    _container_env_of,
    _container_settings,
    _env_of,
    _fake_container_batch,
    _ray_settings,
)
from viva_api.common.dispatch_validation import DispatchValidationError
from viva_api.simulation.simulation_service_ray import SimulationServiceRay

TASK_ENV = {"V2ECOLI_SKIP_CACHE_VERIFY": "1", "V2E_ANALYSIS_MAX_WORKERS": "1"}


def _mnp_submit(task_env: dict[str, str] | None) -> Any:
    """Submit one MNP job with an explicit batch double and return the call."""
    batch = MagicMock()
    batch.submit_job.return_value = {"jobId": "mnp-1"}
    service = SimulationServiceRay()
    with patch("viva_api.simulation.ray._seams.get_settings", _ray_settings):
        service.batch.submit_mnp(
            job_name="ray-sim-x",
            job_definition="smscdk-ray-mnp-abc1234:1",
            num_nodes=2,
            ray_job_cmd="python run.py",
            out_s3="s3://mybucket/vecoli-output/x",
            out_dir="/out",
            batch_client=batch,
            task_env=task_env,
        )
    (call,) = batch.submit_job.call_args_list
    return call


def test_mnp_env_is_byte_identical_without_task_env() -> None:
    env = _env_of(_mnp_submit(None))
    assert "V2ECOLI_SKIP_CACHE_VERIFY" not in env
    assert env["RAY_JOB_CMD"] == "python run.py"


def test_mnp_task_env_reaches_the_single_all_node_override() -> None:
    """One override on `0:` carries the env to EVERY node (head and workers),
    which is where a cache is fetched and verified."""
    call = _mnp_submit(TASK_ENV)
    env = _env_of(call)
    assert env["V2ECOLI_SKIP_CACHE_VERIFY"] == "1"
    assert env["V2E_ANALYSIS_MAX_WORKERS"] == "1"
    # the service's own contract is untouched
    assert env["RAY_JOB_CMD"] == "python run.py"
    overrides = call.kwargs["nodeOverrides"]["nodePropertyOverrides"]
    assert len(overrides) == 1 and overrides[0]["targetNodes"] == "0:"


def _container_submit(task_env: dict[str, str] | None) -> Any:
    batch = _fake_container_batch(["c-1"])
    service = SimulationServiceRay()
    with patch("viva_api.simulation.ray._seams.get_settings", _container_settings):
        service.batch.submit_container(
            job_name="chain-seed0-gen0-x",
            job_definition="smscdk-ray-container-abc1234:1",
            job_cmd="python run.py",
            out_s3="s3://mybucket/vecoli-output/x",
            out_dir="/out",
            batch_client=batch,
            task_env=task_env,
        )
    (call,) = batch.submit_job.call_args_list
    return call


def test_container_env_is_byte_identical_without_task_env() -> None:
    env = _container_env_of(_container_submit(None))
    assert "V2ECOLI_SKIP_CACHE_VERIFY" not in env
    assert env["CONTAINER_JOB_CMD"] == "python run.py"


def test_container_task_env_reaches_the_job() -> None:
    """The chain-dispatch generation jobs, the mbp jobs and the campaign
    analysis all submit through here."""
    env = _container_env_of(_container_submit(TASK_ENV))
    assert env["V2ECOLI_SKIP_CACHE_VERIFY"] == "1"
    assert env["CONTAINER_JOB_CMD"] == "python run.py"


def test_chain_generation_forwards_task_env() -> None:
    """The real caller is JobScheduler, re-deriving it from Simulation.config
    every tick like lineage_debug_division."""
    mock_batch = _fake_container_batch(["s0g0"])
    service = SimulationServiceRay()
    with (
        patch("viva_api.simulation.ray._seams.get_settings", _container_settings),
        patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
        patch("viva_api.simulation.ray._seams.boto3.client", return_value=mock_batch),
    ):
        service._chain().submit_chain_generation(
            seed=0,
            generation_index=0,
            experiment_id="exp-1",
            commit="abc1234",
            cache_s3="s3://mybucket/cache/abc1234",
            runner_s3_uri="s3://mybucket/runner/run_pbg.py",
            tags={"Project": "v2ecoli-comparison", "Phase": "sim"},
            task_env=TASK_ENV,
        )
    (call,) = mock_batch.submit_job.call_args_list
    assert _container_env_of(call)["V2ECOLI_SKIP_CACHE_VERIFY"] == "1"


def _nf_params(task_env: dict[str, str] | None) -> dict[str, Any]:
    service = SimulationServiceRay()
    with patch("viva_api.simulation.ray._seams.get_settings", _ray_settings):
        return service._nextflow()._awsbatch_nf_params("abc1234", "exp-nf", task_env=task_env)


def test_nextflow_container_env_carries_the_merged_keys() -> None:
    """process-bigraph's awsbatch profile emits every `container_env` entry as a
    `--env K=V` containerOption on the profile's process scope, so this is the
    one place the env has to land for parca, lineage AND analysis tasks."""
    assert _nf_params(TASK_ENV)["container_env"] == {
        "PYTHONPATH": "/app/v2ecoli",
        "V2E_ROOT": "/app/v2ecoli",
        "V2ECOLI_SKIP_CACHE_VERIFY": "1",
        "V2E_ANALYSIS_MAX_WORKERS": "1",
    }


def test_nextflow_container_env_is_byte_identical_without_task_env() -> None:
    assert _nf_params(None)["container_env"] == {"PYTHONPATH": "/app/v2ecoli", "V2E_ROOT": "/app/v2ecoli"}


def test_nextflow_container_env_cannot_shadow_the_service_keys() -> None:
    """Belt and braces: the validator refuses PYTHONPATH long before this, and
    the merge order keeps the service's value if anything ever slipped past."""
    config = SimpleNamespace(task_env={"PYTHONPATH": "/evil"})
    from viva_api.common.dispatch_validation import resolve_task_env

    with pytest.raises(DispatchValidationError, match="PYTHONPATH"):
        resolve_task_env(config, {"composite_id": "c"})
