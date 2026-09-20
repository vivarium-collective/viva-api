"""Analysis memory-class routing (viva-api#625 / v2ecoli#788).

A multiseed/multigeneration gather that peaks past the standard 60 GB box
(v2ecoli#786) is routed to the large-memory container queue at submission time,
by declaration, instead of OOM-then-hand-rerun. Two pieces are exercised here:
the sizing rule (``analysis_memory_class``) and ``_submit_container``'s queue
selection, with its fallback to the standard queue when no large queue is
provisioned.

Offline: an explicit batch double, settings doubles, no sockets.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pytest

from tests.simulation.test_ray_backend import (
    _container_settings,
    _fake_container_batch,
)
from viva_api.simulation.ray.analysis_spec import analysis_memory_class
from viva_api.simulation.simulation_service_ray import SimulationServiceRay

# ---------------------------------------------------------------------------
# analysis_memory_class — the sizing rule
# ---------------------------------------------------------------------------


def test_single_scale_is_standard_at_any_size() -> None:
    opts: dict[str, Any] = {"single": {"ptools_rxns": {}}}
    assert analysis_memory_class(opts, n_seeds=64, n_generations=64) == "standard"


@pytest.mark.parametrize("scale", ["multigeneration", "multiseed"])
def test_multicell_scale_routes_large_past_the_standard_box(scale: str) -> None:
    opts: dict[str, Any] = {scale: {"ptools_rxns_multiseed": {}}}
    assert analysis_memory_class(opts, n_generations=4) == "standard"
    assert analysis_memory_class(opts, n_generations=8) == "large"
    assert analysis_memory_class(opts, n_generations=10) == "large"


def test_max_over_scales_wins() -> None:
    opts: dict[str, Any] = {
        "single": {"ptools_rxns": {}},
        "multigeneration": {"ptools_rxns_multigeneration": {}},
    }
    assert analysis_memory_class(opts, n_generations=10) == "large"
    assert analysis_memory_class(opts, n_generations=3) == "standard"


def test_non_scale_map_and_missing_generations_are_standard() -> None:
    # The "applicable" keyword (a str, resolved inside the image) has nothing to
    # size on here; neither does a missing generation count.
    assert analysis_memory_class("applicable", n_generations=10) == "standard"
    assert analysis_memory_class({"multiseed": {"x": {}}}, n_generations=None) == "standard"
    assert analysis_memory_class(None, n_generations=10) == "standard"


# ---------------------------------------------------------------------------
# _submit_container — queue selection
# ---------------------------------------------------------------------------


def _submit(memory_class: str, *, large_queue: str) -> Any:
    settings = _container_settings(ray_container_large_queue=large_queue)
    batch = _fake_container_batch(["c-1"])
    service = SimulationServiceRay()
    with patch("viva_api.simulation.ray._seams.get_settings", lambda: settings):
        service._submit_container(
            job_name="analysis-x",
            job_definition="smscdk-ray-container-abc1234:1",
            job_cmd="python run.py",
            out_s3="s3://mybucket/vecoli-output/x",
            out_dir="/out",
            batch_client=batch,
            memory_class=memory_class,
        )
    (call,) = batch.submit_job.call_args_list
    return call


def test_large_routes_to_the_large_queue_when_provisioned() -> None:
    call = _submit("large", large_queue="smscdk-ray-standalone-large")
    assert call.kwargs["jobQueue"] == "smscdk-ray-standalone-large"


def test_large_falls_back_to_standard_queue_when_unset() -> None:
    # Unprovisioned large queue: unchanged behaviour, the standard queue.
    call = _submit("large", large_queue="")
    assert call.kwargs["jobQueue"] == "smscdk-ray-standalone"


def test_standard_uses_the_standard_queue_even_when_large_exists() -> None:
    call = _submit("standard", large_queue="smscdk-ray-standalone-large")
    assert call.kwargs["jobQueue"] == "smscdk-ray-standalone"


def test_default_memory_class_is_standard() -> None:
    # Callers that don't pass memory_class (parca, builds, chain generations) are
    # unaffected: the standard queue, even with a large queue provisioned.
    settings = _container_settings(ray_container_large_queue="smscdk-ray-standalone-large")
    batch = _fake_container_batch(["c-1"])
    service = SimulationServiceRay()
    with patch("viva_api.simulation.ray._seams.get_settings", lambda: settings):
        service._submit_container(
            job_name="parca-x",
            job_definition="smscdk-ray-container-abc1234:1",
            job_cmd="python run.py",
            out_s3="s3://mybucket/vecoli-output/x",
            out_dir="/out",
            batch_client=batch,
        )
    (call,) = batch.submit_job.call_args_list
    assert call.kwargs["jobQueue"] == "smscdk-ray-standalone"
