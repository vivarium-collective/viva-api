"""The nextflow_dispatch axis: a third dispatch path chosen per request.

Deliberately NOT a new ComputeBackend. `compute_backend_for_repo` maps repo ->
backend, so a NEXTFLOW member would either reroute every v2ecoli request or be
dead configuration. This is the same axis `multi_node_dispatch` already uses to
pick pbg-native over chain-dispatch: same repo, same image, one config field.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from viva_api.simulation.simulation_service_ray import SimulationServiceRay
from tests.simulation.test_ray_backend import _ray_settings, _v2ecoli_simulator


def _sim(**extras: Any) -> MagicMock:
    sim = MagicMock()
    cfg = MagicMock()
    cfg.experiment_id = "exp-nf"
    cfg.generations = 1
    # getattr(config, name, None) must miss for anything not explicitly given
    for name in ("nextflow_dispatch", "multi_node_dispatch", "composite"):
        setattr(cfg, name, extras.get(name))
    sim.config = cfg
    sim.simulator_id = 133
    return sim


def _db() -> MagicMock:
    db = MagicMock()
    db.get_simulator = AsyncMock(return_value=_v2ecoli_simulator())
    return db


@pytest.mark.asyncio
async def test_nextflow_dispatch_is_chosen_before_multi_node() -> None:
    """Order is load-bearing. A Nextflow request carries a composite_id too, so a
    later check would claim it first and the run would execute on the WRONG
    mechanism while looking like it worked -- the same misrouting the MNP check
    was placed ahead of chain-dispatch to avoid."""
    service = SimulationServiceRay()
    sim = _sim(
        nextflow_dispatch={"composite_id": "v2ecoli.composites.workflow_nf"},
        multi_node_dispatch={"composite_id": "v2ecoli.composites.lineage_ray_batch"},
    )
    with (
        patch.object(service, "_submit_nextflow_dispatch", new=AsyncMock(return_value="nf")) as nf,
        patch.object(service, "_submit_multi_node_composite", new=AsyncMock(return_value="mnp")) as mnp,
    ):
        await service.submit_ecoli_simulation_job(sim, _db(), correlation_id="c")
    assert nf.await_count == 1
    assert mnp.await_count == 0


@pytest.mark.asyncio
async def test_absent_axis_leaves_every_other_route_untouched() -> None:
    """A request that does not ask for Nextflow must dispatch exactly as before."""
    service = SimulationServiceRay()
    sim = _sim(multi_node_dispatch={"composite_id": "x"})
    with (
        patch.object(service, "_submit_nextflow_dispatch", new=AsyncMock()) as nf,
        patch.object(service, "_submit_multi_node_composite", new=AsyncMock(return_value="mnp")) as mnp,
    ):
        await service.submit_ecoli_simulation_job(sim, _db(), correlation_id="c")
    assert nf.await_count == 0
    assert mnp.await_count == 1


@pytest.mark.asyncio
async def test_missing_composite_id_fails_rather_than_guessing() -> None:
    service = SimulationServiceRay()
    with patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings):
        with pytest.raises(ValueError, match="composite_id is required"):
            await service._submit_nextflow_dispatch(_sim(), _db(), {})


# --- the command that actually runs in the container ------------------------


def _command(**dispatch: Any) -> str:
    service = SimulationServiceRay()
    with patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings):
        return service._render_nf_command(
            runner_s3_uri="s3://b/exp/render_nf.py",
            composite_id="v2ecoli.composites.workflow_nf",
            params=dispatch.get("params"),
            executor=dispatch.get("executor", "local"),
            launch=dispatch.get("launch", False),
            outdir="/app/v2ecoli/nf-render",
            work_dir=dispatch.get("work_dir"),
            resume=dispatch.get("resume", False),
        )


def test_command_stages_the_compiler_rather_than_inlining_it() -> None:
    """Batch caps a container override command at 8192 bytes, which is why the
    script travels through S3 -- the same reason run_pbg is staged."""
    cmd = _command()
    assert "aws s3 cp s3://b/exp/render_nf.py /tmp/render_nf.py" in cmd
    assert "python /tmp/render_nf.py" in cmd


def test_command_defaults_to_render_only_on_the_local_executor() -> None:
    """Phase 3 verifies executor=local INSIDE the real image before Phase 4
    introduces awsbatch, so a failure has one candidate cause, not two."""
    cmd = _command()
    assert "--executor local" in cmd
    assert "--launch" not in cmd
    assert "--resume" not in cmd


def test_command_always_writes_a_trace() -> None:
    """A reused task reports CACHED in the trace CSV and nowhere else, so without
    it there is no way to tell a resumed run from a repeated one (go/no-go 3)."""
    assert "--trace /app/v2ecoli/nf-render/trace.csv" in _command()


def test_launch_resume_and_workdir_reach_the_command() -> None:
    cmd = _command(launch=True, resume=True, work_dir="s3://bucket/work", executor="awsbatch")
    assert "--launch" in cmd and "--resume" in cmd
    assert "--work-dir s3://bucket/work" in cmd
    assert "--executor awsbatch" in cmd


def test_params_are_shell_quoted_json() -> None:
    """Generator params carry arbitrary nested values; an unquoted blob would be
    split by the shell and silently truncate the campaign shape."""
    cmd = _command(params={"n_seeds": 4, "variants": [{"variant_name": "a b"}]})
    assert "--overrides" in cmd
    payload = cmd.split("--overrides ", 1)[1].split(" --")[0]
    assert json.loads(payload.strip("'")) == {"n_seeds": 4, "variants": [{"variant_name": "a b"}]}


def test_head_image_is_the_submit_tag_not_the_task_image() -> None:
    """Only the process running `nextflow run` needs a JVM. The task image has no
    Java, so dispatching Nextflow against it would fail inside the container
    rather than at submit time."""
    service = SimulationServiceRay()
    with patch("viva_api.simulation.simulation_service_ray.get_settings", _ray_settings):
        assert service._submit_image_uri("abc1234").endswith("/v2ecoli:abc1234-submit")
