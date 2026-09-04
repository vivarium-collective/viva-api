"""E2EDataService.run_workflow / submit_run_workflow -- the real JSON-body shape
POST /api/v1/simulations actually needs (backlog item 101/109, `atlantis
simulation run-pbg-native`).

Hermetic: httpx is mocked at the transport (matching test_worker_task_cli.py's
own convention), so nothing here can reach a real deployment.

What is pinned: the route declares TWO separate Body(...) params
(analysis_options, extra_params) -- FastAPI's real behavior for multiple body
params is to nest each under its own key
(`{"analysis_options": {...}, "extra_params": {...}}`), not send either one
bare. A prior version of this client sent `analysis_options` unwrapped
instead -- a latent bug that never surfaced because no existing caller set
BOTH at once. These tests would have caught it.
"""

from __future__ import annotations

import json
from typing import Any

import httpx

from app.app_data_service import BaseUrl, E2EDataService


def _service(handler: Any) -> E2EDataService:
    """A service whose transport is a callable, so no socket is ever opened."""
    svc = E2EDataService(base_url=BaseUrl.LOCAL_8080, timeout=5)
    svc.client = httpx.Client(
        base_url=BaseUrl.LOCAL_8080,
        transport=httpx.MockTransport(handler),
        headers=svc.client.headers,
    )
    return svc


def _recorder() -> tuple[Any, list[httpx.Request]]:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            200,
            json={
                "database_id": 1,
                "simulator_id": 1,
                "parca_dataset_id": 1,
                "config": {"experiment_id": "e"},
                "simulation_config_filename": "x.json",
                "experiment_id": "e",
                "last_updated": "2026-01-01 00:00:00",
                "job_id": None,
                "num_seeds": 1,
                "tags": [],
            },
        )

    return handler, seen


def _body(seen: list[httpx.Request]) -> Any:
    raw = seen[0].content
    return json.loads(raw) if raw else None


def test_neither_analysis_options_nor_extra_params_sends_no_body() -> None:
    """Every existing caller's own prior behavior, byte-identical."""
    handler, seen = _recorder()
    _service(handler).run_workflow(simulator_id=1, experiment_id="e")
    assert _body(seen) is None


def test_extra_params_alone_is_nested_under_its_own_key() -> None:
    handler, seen = _recorder()
    _service(handler).run_workflow(
        simulator_id=1,
        experiment_id="e",
        extra_params={"multi_node_dispatch": {"composite_id": "v2ecoli.composites.lineage_ray_batch"}},
    )
    assert _body(seen) == {
        "extra_params": {"multi_node_dispatch": {"composite_id": "v2ecoli.composites.lineage_ray_batch"}}
    }


def test_analysis_options_alone_is_nested_under_its_own_key() -> None:
    handler, seen = _recorder()
    _service(handler).run_workflow(
        simulator_id=1,
        experiment_id="e",
        analysis_options={"multiseed": {}},
    )
    assert _body(seen) == {"analysis_options": {"multiseed": {}}}


def test_both_together_nest_under_their_own_separate_keys() -> None:
    """The exact shape that was never exercised before -- this is the one a bare
    `json=analysis_options` would have gotten wrong."""
    handler, seen = _recorder()
    _service(handler).run_workflow(
        simulator_id=1,
        experiment_id="e",
        analysis_options={"multiseed": {}},
        extra_params={"multi_node_dispatch": {"num_nodes": 2}},
    )
    assert _body(seen) == {
        "analysis_options": {"multiseed": {}},
        "extra_params": {"multi_node_dispatch": {"num_nodes": 2}},
    }
