"""The response-shape contract (``app/contract.py``) and the ``contract`` smoke check.

What matters: a facade may GROW and may not SHRINK. An added key passes and is reported; a removed
key, a retyped position or a changed status fails. ``null`` is compatible with anything, because a
nullable field's recorded value is an accident of the data on the day.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx
import pytest

from app import contract, smoke
from tests.app.test_smoke import FakeService, _run

# --------------------------------------------------------------------------- shapes


def test_shape_of_records_types_keys_and_merged_array_items() -> None:
    shape = contract.shape_of({"id": 1, "tags": ["a"], "rows": [{"x": 1}, {"x": 2, "y": None}], "n": 1.5, "ok": True})
    assert shape["t"] == ["object"]
    assert shape["keys"]["id"] == {"t": ["integer"]}
    assert shape["keys"]["n"] == {"t": ["number"]}
    assert shape["keys"]["ok"] == {"t": ["boolean"]}
    assert shape["keys"]["tags"] == {"t": ["array"], "items": {"t": ["string"]}}
    rows = shape["keys"]["rows"]["items"]
    assert rows["keys"]["x"] == {"t": ["integer"]}
    assert rows["keys"]["y"] == {"t": ["null"]}
    assert rows["optional"] == ["y"]  # present in one element, not the other


def test_an_empty_array_records_no_item_shape() -> None:
    assert contract.shape_of([]) == {"t": ["array"], "items": None}


def test_a_removed_key_or_a_retyped_position_is_a_contract_change() -> None:
    recorded = contract.shape_of({"database_id": 1, "status": "ok", "config": {"n_tp": 5}})
    removed = contract.compare_shapes(recorded, contract.shape_of({"database_id": 1, "config": {"n_tp": 5}}))
    assert removed.problems == ["$.status: removed"]
    retyped = contract.compare_shapes(
        recorded, contract.shape_of({"database_id": "1", "status": "ok", "config": {"n_tp": 5}})
    )
    assert retyped.problems == ["$.database_id: type ['integer'] became ['string']"]
    nested = contract.compare_shapes(recorded, contract.shape_of({"database_id": 1, "status": "ok", "config": {}}))
    assert nested.problems == ["$.config.n_tp: removed"]


def test_an_added_key_is_allowed_and_reported() -> None:
    recorded = contract.shape_of({"a": 1})
    diff = contract.compare_shapes(recorded, contract.shape_of({"a": 1, "b": "new"}))
    assert diff.ok and diff.additions == ["$.b"]


def test_null_is_compatible_both_ways_and_integer_may_widen_to_number() -> None:
    recorded = contract.shape_of({"n_tp": None, "size": 1})
    assert contract.compare_shapes(recorded, contract.shape_of({"n_tp": 5, "size": 1.5})).ok
    recorded_set = contract.shape_of({"n_tp": 5})
    assert contract.compare_shapes(recorded_set, contract.shape_of({"n_tp": None})).ok


def test_an_optional_key_may_be_absent_and_an_empty_live_array_is_not_a_change() -> None:
    recorded = contract.shape_of([{"x": 1}, {"x": 2, "y": 3}])  # y optional
    assert contract.compare_shapes(recorded, contract.shape_of([{"x": 1}])).ok
    assert contract.compare_shapes(recorded, contract.shape_of([])).ok
    assert contract.compare_shapes(recorded, contract.shape_of([{"y": 3}])).problems == ["$[].x: removed"]


def test_a_status_change_is_a_contract_change_only_when_the_operation_has_one_status() -> None:
    gone = contract.compare_observations("op", contract.Observation(200, None), contract.Observation(404, None))
    assert gone.problems == ["op: status 200 became 404"]
    # an operation the older server did not serve, now served: an addition, not a change
    arrived = contract.compare_observations("op", contract.Observation(404, None), contract.Observation(200, None))
    assert arrived.ok and arrived.additions == ["op: now served (404 became 200)"]
    # several legitimate statuses (chain-progress: 200 / 404 / 409 depending on the newest run): not compared
    loose = contract.compare_observations(
        "op", contract.Observation(200, None), contract.Observation(409, None), strict_status=False
    )
    assert loose.ok and not loose.additions


def test_a_map_keyed_body_is_compared_by_its_values_not_its_keys() -> None:
    recorded = contract.shape_of_map({"cd2": 3, "run1": 1})
    assert recorded == {"t": ["map"], "values": {"t": ["integer"]}}
    assert contract.compare_shapes(recorded, contract.shape_of_map({"other-tag": 7})).ok
    assert contract.compare_shapes(recorded, contract.shape_of_map({"cd2": "3"})).problems


def test_deep_objects_record_only_that_they_are_objects() -> None:
    deep = {"a": {"b": {"c": {"d": {"e": {"f": 1}}}}}}
    shape = contract.shape_of(deep)
    inner = shape["keys"]["a"]["keys"]["b"]["keys"]["c"]["keys"]["d"]
    assert inner == {"t": ["object"]}


# --------------------------------------------------------------------------- the operations


def test_every_operation_is_read_only_and_names_who_calls_it() -> None:
    names = [op.name for op in contract.CONTRACT_OPERATIONS]
    assert len(names) == len(set(names))
    for op in contract.CONTRACT_OPERATIONS:
        assert op.callers, op.name
        assert 200 in op.accept, op.name
        assert op.path.startswith("/")
    # the PTools page's three calls are in the contract, by name
    ptools = {op.path for op in contract.CONTRACT_OPERATIONS if "ptools" in op.callers}
    assert ptools == {"/api/v1/simulations", "/api/v1/analyses", "/api/v1/analyses/{analysis_id}/data"}


def test_context_comes_from_the_newest_simulation_and_the_newest_analysis_anywhere() -> None:
    """The newest simulation rarely has analyses yet; the PTools calls are observed on the newest
    analysis, whichever experiment it belongs to."""
    answers = {
        "/api/v1/simulations": [{"database_id": 9, "simulator_id": 3, "experiment_id": "exp-9"}],
        # asked for a COMPLETED one: a running analysis answers 409 on /data, which is data, not contract
        "/api/v1/analyses": [{"database_id": 4, "experiment_id": "exp-2", "status": "completed"}],
    }
    calls: list[tuple[str, dict[str, Any]]] = []

    def get_json(path: str, **params: Any) -> Any:
        calls.append((path, params))
        return answers[path]

    context = contract.resolve_context(get_json)
    assert context == {"simulation_id": "9", "simulator_id": "3", "analysis_id": "4", "experiment_id": "exp-2"}
    assert calls[1] == ("/api/v1/analyses", {"status": "completed", "limit": 1})
    assert contract.resolve_context(lambda path, **params: []) == {}


def test_an_operation_missing_its_context_is_skipped_not_guessed() -> None:
    op = contract.operations_named(["simulation-status"])[0]
    assert op.needs() == {"simulation_id"}
    assert contract.observe(op, {}, lambda path, params: (200, {})) is None
    seen = contract.observe(op, {"simulation_id": "9"}, lambda path, params: (200, {"status": "ok"}))
    assert seen is not None and seen.status == 200 and seen.shape == contract.shape_of({"status": "ok"})


def test_an_unaccepted_status_is_a_broken_deployment_not_a_drift() -> None:
    op = contract.operations_named(["simulations"])[0]
    with pytest.raises(contract.ContractError, match="500"):
        contract.observe(op, {}, lambda path, params: (500, {"detail": "boom"}))


# --------------------------------------------------------------------------- the smoke check


def _routes(**overrides: httpx.Response) -> dict[tuple[str, str], httpx.Response]:
    routes: dict[tuple[str, str], httpx.Response] = {
        ("GET", "/version"): httpx.Response(200, json="9.9.9"),
        ("GET", "/api/v1/simulations"): httpx.Response(
            200, json=[{"database_id": 9, "simulator_id": 3, "experiment_id": "exp-9", "config": {"n_init_sims": 1}}]
        ),
        ("GET", "/api/v1/analyses"): httpx.Response(
            200, json=[{"database_id": 4, "n_tp": 5, "experiment_id": "exp-9", "status": "completed"}]
        ),
        ("GET", "/api/v1/datasets/tags"): httpx.Response(200, json={"cd2": 3}),
        ("GET", "/api/v1/analyses/4/data"): httpx.Response(200, json=[{"filename": "ptools_rna.tsv", "content": "x"}]),
        ("GET", "/api/v1/simulations/9/status"): httpx.Response(
            200, json={"status": "completed", "error_message": None}
        ),
    }
    for op in contract.CONTRACT_OPERATIONS:
        key = ("GET", op.path.format(simulation_id="9", analysis_id="4"))
        routes.setdefault(key, httpx.Response(200, json={"ok": True}))
    routes.update({("GET", path): resp for path, resp in overrides.items()})
    return routes


def _record(tmp_path: Path, routes: dict[tuple[str, str], httpx.Response]) -> Path:
    target = tmp_path / "shapes.json"
    result = _run("contract", FakeService(routes), record_contract_to=target)
    assert result.outcome is smoke.Outcome.PASS, result.detail
    return target


def test_recording_writes_every_observed_operation_and_the_context(tmp_path: Path) -> None:
    target = _record(tmp_path, _routes())
    document = json.loads(target.read_text())
    assert document["server_version"] == "9.9.9"
    assert set(document["operations"]) == {op.name for op in contract.CONTRACT_OPERATIONS}
    assert document["operations"]["analyses-for-experiment"]["shape"]["items"]["keys"]["n_tp"] == {"t": ["integer"]}


def test_the_check_skips_without_a_recorded_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(contract, "load_shapes", lambda path=None: {})
    result = _run("contract", FakeService(_routes()))
    assert result.outcome is smoke.Outcome.SKIP and "--record-contract" in result.detail


def test_the_check_passes_on_the_recorded_shapes_and_reports_additions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = _record(tmp_path, _routes())
    monkeypatch.setattr(contract, "load_shapes", lambda path=None: json.loads(target.read_text()))
    assert _run("contract", FakeService(_routes())).outcome is smoke.Outcome.PASS
    grown = _routes(**{
        "/api/v1/simulations/9/status": httpx.Response(
            200, json={"status": "completed", "error_message": None, "phase": "done"}
        )
    })
    result = _run("contract", FakeService(grown))
    assert result.outcome is smoke.Outcome.PASS
    assert "1 added key" in result.detail and result.evidence["additions"] == ["simulation-status.phase"]


def test_the_check_fails_on_a_removed_key_a_retyped_field_or_a_changed_status(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = _record(tmp_path, _routes())
    monkeypatch.setattr(contract, "load_shapes", lambda path=None: json.loads(target.read_text()))
    shrunk = _routes(**{
        "/api/v1/analyses": httpx.Response(
            200, json=[{"database_id": 4, "experiment_id": "exp-9", "status": "completed"}]
        )
    })
    result = _run("contract", FakeService(shrunk))
    assert result.outcome is smoke.Outcome.FAIL and "analyses-for-experiment[].n_tp: removed" in result.detail
    retyped = _routes(**{"/api/v1/analyses/4/data": httpx.Response(200, json=[{"filename": 1, "content": "x"}])})
    assert "type" in _run("contract", FakeService(retyped)).detail
    # a single-status operation that stops answering 200 is refused at observation: the deployment is broken
    moved = _routes(**{"/core/v1/simulator/versions": httpx.Response(404, json={"detail": "gone"})})
    result = _run("contract", FakeService(moved))
    assert result.outcome is smoke.Outcome.FAIL and "simulator-versions" in result.detail and "404" in result.detail
    # a tag nobody uses any more is data, not contract
    retagged = _routes(**{"/api/v1/datasets/tags": httpx.Response(200, json={"new-tag": 1})})
    assert _run("contract", FakeService(retagged)).outcome is smoke.Outcome.PASS


def test_the_check_skips_the_operations_it_cannot_fill_and_says_so(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = _record(tmp_path, _routes())
    monkeypatch.setattr(contract, "load_shapes", lambda path=None: json.loads(target.read_text()))
    empty = _routes(**{"/api/v1/simulations": httpx.Response(200, json=[])})
    result = _run("contract", FakeService(empty))
    assert result.outcome is smoke.Outcome.PASS
    assert "simulation-status" in result.evidence["skipped"] and "version" in result.evidence["observed"]


def test_a_broken_operation_fails_the_check_rather_than_drifting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = _record(tmp_path, _routes())
    monkeypatch.setattr(contract, "load_shapes", lambda path=None: json.loads(target.read_text()))
    broken = _routes(**{"/core/v1/simulator/versions": httpx.Response(500, text="boom")})
    result = _run("contract", FakeService(broken))
    assert result.outcome is smoke.Outcome.FAIL and "simulator-versions" in result.detail and "500" in result.detail


def test_the_packaged_baseline_when_present_is_a_document_this_module_wrote() -> None:
    document = contract.load_shapes()
    if not document:
        pytest.skip("no packaged baseline yet (recorded from a deployment with --record-contract)")
    assert set(document) >= {"recorded_from", "server_version", "recorded_at", "operations"}
    names = set(document["operations"])
    assert names <= {op.name for op in contract.CONTRACT_OPERATIONS}, (
        "the baseline names an operation this build no longer defines"
    )
