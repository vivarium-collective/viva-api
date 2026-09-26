"""The response-shape contract of the surfaces external callers depend on (``docs/plan-core.md`` P3g).

Strategy B (D14) dates every SMS-shaped surface and re-implements the ones that stay as facades over
core. Until each is removed, the promise is: **its response SHAPE does not change** -- the keys and
their JSON types that a caller reads. The workbench (33 operations, ``sms_api_client.py``) and the
PTools page (three calls in ``sms.js``) are the callers; this module records what their read-only
operations answer on a known deployment and compares a later deployment against it. It is the
safety net the refactor lacked: ``routes`` checks that an operation is served, not what it says.

Shapes, not values. A shape is the JSON type at every position -- object keys, array items, scalar
types -- with ``null`` treated as compatible with anything (a nullable field). Comparison follows
the tolerant-reader rule the capability contract already states: an **added** key is reported and
allowed; a **removed** key, a **changed type** or a **changed status** is a contract change and
fails the check. That is deliberately asymmetric: a facade may grow, it may not shrink.

Only GET operations that need at most an existing id are here (Tier 0 is read-only). The
mutations are exercised by the Tier 1 and 2 checks, which assert effects; their request shapes
are pinned by the OpenAPI document and ``routes``.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from importlib import resources
from pathlib import Path
from typing import Any

SHAPES_FILENAME = "contract_shapes.json"

#: A shape: ``{"t": [type names]}`` plus ``keys`` / ``optional`` for objects, ``items`` for arrays
#: and ``values`` for maps (objects whose keys are data, not a vocabulary -- see ``shape_of_map``).
Shape = dict[str, Any]

#: Below this depth an object records only that it is one. A caller reads the top of a response;
#: what is nested five levels down is a passthrough blob (a simulation's whole vEcoli ``config``),
#: whose keys are the solver's business, not this contract's.
MAX_DEPTH = 4


def shape_of(value: Any, depth: int = 0) -> Shape:
    """The JSON shape of a value. Array items are MERGED: a key present in some elements and not
    others is optional; a position seen with several scalar types lists them all."""
    if isinstance(value, dict):
        if depth >= MAX_DEPTH:
            return {"t": ["object"]}
        return {"t": ["object"], "keys": {str(k): shape_of(v, depth + 1) for k, v in value.items()}, "optional": []}
    if isinstance(value, list):
        items: Shape | None = None
        for element in value:
            element_shape = shape_of(element, depth + 1)
            items = element_shape if items is None else merge_shapes(items, element_shape)
        return {"t": ["array"], "items": items}
    if value is None:
        return {"t": ["null"]}
    if isinstance(value, bool):
        return {"t": ["boolean"]}
    if isinstance(value, int):
        return {"t": ["integer"]}
    if isinstance(value, float):
        return {"t": ["number"]}
    return {"t": ["string"]}


def shape_of_map(value: Any) -> Shape:
    """A map -- an object whose keys are DATA (tag names, attribute values), so the contract is the
    shape of its values, not the set of keys: a tag that nobody uses any more is not a change."""
    if not isinstance(value, dict):
        return shape_of(value)
    values: Shape | None = None
    for element in value.values():
        element_shape = shape_of(element, 1)
        values = element_shape if values is None else merge_shapes(values, element_shape)
    return {"t": ["map"], "values": values}


def merge_shapes(a: Shape, b: Shape) -> Shape:
    """One shape that both conform to -- what an array of mixed elements has in common."""
    types = sorted(set(a["t"]) | set(b["t"]))
    merged: Shape = {"t": types}
    if "map" in types:
        values_a, values_b = a.get("values"), b.get("values")
        merged["values"] = (
            values_a if values_b is None else (values_b if values_a is None else merge_shapes(values_a, values_b))
        )
    if "object" in types:
        keys_a: dict[str, Shape] = a.get("keys", {})
        keys_b: dict[str, Shape] = b.get("keys", {})
        keys: dict[str, Shape] = {}
        optional = set(a.get("optional", [])) | set(b.get("optional", []))
        for name in sorted(set(keys_a) | set(keys_b)):
            if name in keys_a and name in keys_b:
                keys[name] = merge_shapes(keys_a[name], keys_b[name])
            else:
                keys[name] = keys_a.get(name) or keys_b[name]
                if "object" in a["t"] and "object" in b["t"]:
                    optional.add(name)  # present in one object, absent in the other
        merged["keys"] = keys
        merged["optional"] = sorted(optional)
    if "array" in types:
        items_a, items_b = a.get("items"), b.get("items")
        merged["items"] = (
            items_a if items_b is None else (items_b if items_a is None else merge_shapes(items_a, items_b))
        )
    return merged


@dataclass
class ShapeDiff:
    """What changed between a recorded shape and a live one. ``problems`` fail the contract;
    ``additions`` are allowed and reported."""

    problems: list[str] = field(default_factory=list)
    additions: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.problems

    def extend(self, other: ShapeDiff) -> None:
        self.problems.extend(other.problems)
        self.additions.extend(other.additions)


def compare_shapes(recorded: Shape, live: Shape, where: str = "$") -> ShapeDiff:
    """Tolerant-reader comparison: live may add; it may not remove or retype. ``null`` on either
    side is compatible with anything -- a nullable field that happened to be null when recorded
    (or now) is not a change. An empty live array says nothing about its items."""
    diff = ShapeDiff()
    recorded_types = set(recorded["t"]) - {"null"}
    live_types = set(live["t"]) - {"null"}
    if recorded_types and live_types and not live_types <= recorded_types:
        # `number` where `integer` was recorded is a widening a JSON reader survives; the reverse too.
        numeric = {"integer", "number"}
        if not (live_types <= numeric and recorded_types <= numeric):
            diff.problems.append(f"{where}: type {sorted(recorded_types)} became {sorted(live_types)}")
            return diff
    if "object" in recorded_types and "object" in live_types:
        _compare_keys(recorded, live, where, diff)
    for kind, slot, suffix in (("array", "items", "[]"), ("map", "values", "{}")):
        if kind in recorded_types and kind in live_types:
            recorded_inner, live_inner = recorded.get(slot), live.get(slot)
            if recorded_inner is not None and live_inner is not None:
                diff.extend(compare_shapes(recorded_inner, live_inner, f"{where}{suffix}"))
    return diff


def _compare_keys(recorded: Shape, live: Shape, where: str, diff: ShapeDiff) -> None:
    recorded_keys: dict[str, Shape] = recorded.get("keys", {})
    live_keys: dict[str, Shape] = live.get("keys", {})
    optional = set(recorded.get("optional", []))
    for name, sub in recorded_keys.items():
        if name not in live_keys:
            if name not in optional:
                diff.problems.append(f"{where}.{name}: removed")
            continue
        diff.extend(compare_shapes(sub, live_keys[name], f"{where}.{name}"))
    diff.additions.extend(f"{where}.{name}" for name in live_keys if name not in recorded_keys)


# --------------------------------------------------------------------------- the operations


@dataclass(frozen=True)
class Operation:
    """One read-only call a caller makes. ``path`` and ``params`` may name a context value in
    braces (``{simulation_id}``), filled from what the deployment has; an operation whose value
    is missing is skipped, never guessed. ``accept`` lists the statuses that are a legitimate
    answer; the status is part of the recorded contract."""

    name: str
    path: str
    params: Mapping[str, str] = field(default_factory=dict)
    accept: tuple[int, ...] = (200,)
    callers: tuple[str, ...] = ("workbench",)
    #: The body is an object keyed by data (tag names); its contract is the shape of the values.
    map_keyed: bool = False

    @property
    def strict_status(self) -> bool:
        """When only 200 is accepted, a status change is a contract change. When several are
        (``chain-progress`` answers 409 for a run that is not a campaign), which one the newest
        record gets is the data's choice, not the deployment's."""
        return self.accept == (200,)

    def needs(self) -> set[str]:
        import re

        text = self.path + " " + " ".join(self.params.values())
        return set(re.findall(r"\{(\w+)\}", text))


WORKBENCH = ("workbench",)
PTOOLS = ("ptools",)
BOTH = ("workbench", "ptools")

#: The read-only operations of the four SMS-shaped surfaces that the workbench and the PTools page
#: call, plus core's own two. ``simulator/latest`` and ``simulations/discovery`` are left out: they
#: read GitHub, and a contract check must not depend on it.
CONTRACT_OPERATIONS: tuple[Operation, ...] = (
    Operation("version", "/version", callers=WORKBENCH),
    Operation("health", "/health", callers=("atlantis",)),
    Operation("capabilities", "/core/v1/capabilities", callers=WORKBENCH),
    Operation("simulator-versions", "/core/v1/simulator/versions", callers=WORKBENCH),
    Operation("simulator-status", "/core/v1/simulator/status", {"simulator_id": "{simulator_id}"}, callers=WORKBENCH),
    Operation("simulations", "/api/v1/simulations", callers=BOTH),
    Operation("simulation", "/api/v1/simulations/{simulation_id}", callers=WORKBENCH),
    Operation("simulation-status", "/api/v1/simulations/{simulation_id}/status", callers=WORKBENCH),
    Operation(
        "simulation-chain-progress",
        "/api/v1/simulations/{simulation_id}/chain-progress",
        accept=(200, 404, 409),  # the workbench treats 404 and 409 as phases, not errors
        callers=WORKBENCH,
    ),
    Operation("simulation-analyses", "/api/v1/simulations/{simulation_id}/analyses", callers=WORKBENCH),
    Operation("simulation-datasets", "/api/v1/simulations/{simulation_id}/datasets", callers=("atlantis",)),
    Operation("analyses-for-experiment", "/api/v1/analyses", {"experiment_id": "{experiment_id}"}, callers=PTOOLS),
    Operation("analysis", "/api/v1/analyses/{analysis_id}", callers=("atlantis",)),
    Operation("analysis-status", "/api/v1/analyses/{analysis_id}/status", accept=(200, 501), callers=WORKBENCH),
    Operation("analysis-data", "/api/v1/analyses/{analysis_id}/data", callers=PTOOLS),
    Operation("datasets", "/api/v1/datasets", callers=("atlantis",)),
    Operation("dataset-tags", "/api/v1/datasets/tags", callers=("atlantis",), map_keyed=True),
    Operation("dataset-attributes", "/api/v1/datasets/attributes", callers=("atlantis",)),
    Operation("compose-simulators", "/compose/v1/simulators", callers=("atlantis",)),
    Operation("compose-processes", "/compose/v1/processes", callers=("atlantis",)),
    Operation("core-health", "/viva/v1/health", callers=("core",)),
    Operation("core-capabilities", "/viva/v1/capabilities", accept=(200, 404), callers=("core",)),
)


def _first(items: Any) -> dict[str, Any] | None:
    if isinstance(items, list) and items and isinstance(items[0], dict):
        return items[0]
    return None


def resolve_context(get_json: Callable[..., Any]) -> dict[str, str]:
    """The ids the operations need, from what the deployment has: the newest simulation (its id and
    simulator), and the newest ANALYSIS anywhere -- with its experiment, so the PTools calls are
    observed on data that has what they read. Missing pieces are left out; the operations that
    need them skip."""
    context: dict[str, str] = {}
    newest = _first(get_json("/api/v1/simulations", limit=1))
    if newest is not None:
        if newest.get("database_id") is not None:
            context["simulation_id"] = str(newest["database_id"])
        if newest.get("simulator_id") is not None:
            context["simulator_id"] = str(newest["simulator_id"])
    # A COMPLETED analysis: `/data` answers 409 while one is still running, and that is the data's
    # state, not the contract's. (On dev the newest two hundred are smoke leftovers, all "running".)
    analysis = _first(get_json("/api/v1/analyses", status="completed", limit=1))
    if analysis is not None:
        if analysis.get("database_id") is not None:
            context["analysis_id"] = str(analysis["database_id"])
        if analysis.get("experiment_id"):
            context["experiment_id"] = str(analysis["experiment_id"])
    return context


def fill(template: str, context: Mapping[str, str]) -> str:
    return template.format(**context)


@dataclass
class Observation:
    """What one operation answered: the status, and the shape when the body was JSON."""

    status: int
    shape: Shape | None = None

    def to_json(self) -> dict[str, Any]:
        return {"status": self.status, "shape": self.shape}

    @classmethod
    def from_json(cls, raw: Mapping[str, Any]) -> Observation:
        return cls(status=int(raw["status"]), shape=raw.get("shape"))


def observe(operation: Operation, context: Mapping[str, str], request: Callable[..., Any]) -> Observation | None:
    """Call one operation. ``request(path, params) -> (status, body | None)``. ``None`` when the
    operation needs a context value the deployment could not supply."""
    if not operation.needs() <= set(context):
        return None
    path = fill(operation.path, context)
    params = {k: fill(v, context) for k, v in operation.params.items()}
    status, body = request(path, params)
    if status not in operation.accept:
        raise ContractError(f"{operation.name}: GET {path} -> {status}, expected one of {list(operation.accept)}")
    if body is None:
        return Observation(status=status, shape=None)
    return Observation(status=status, shape=shape_of_map(body) if operation.map_keyed else shape_of(body))


class ContractError(Exception):
    """An operation answered outside its accepted statuses -- the deployment is broken, not drifted."""


def observe_all(
    operations: Iterable[Operation], context: Mapping[str, str], request: Callable[..., Any]
) -> tuple[dict[str, Observation], list[str]]:
    """Every operation that can be filled, observed; the names of those that could not."""
    observed: dict[str, Observation] = {}
    skipped: list[str] = []
    for operation in operations:
        seen = observe(operation, context, request)
        if seen is None:
            skipped.append(operation.name)
        else:
            observed[operation.name] = seen
    return observed, skipped


def compare_all(recorded: Mapping[str, Observation], live: Mapping[str, Observation]) -> tuple[int, ShapeDiff]:
    """Every live operation that has a recorded counterpart, compared; how many, and the sum."""
    strict = {op.name: op.strict_status for op in CONTRACT_OPERATIONS}
    total = ShapeDiff()
    compared = 0
    for name, seen in live.items():
        if name not in recorded:
            continue
        diff = compare_observations(name, recorded[name], seen, strict_status=strict.get(name, True))
        total.problems.extend(diff.problems)
        total.additions.extend(diff.additions)
        compared += 1
    return compared, total


def compare_observations(
    name: str, recorded: Observation, live: Observation, *, strict_status: bool = True
) -> ShapeDiff:
    """A status change is a problem when the operation has one legitimate status and it was the
    one recorded; an operation that was NOT served when recorded (a 404 on an older server) and is
    now is an addition. Shapes are compared only when both answers were the same status."""
    if recorded.status != live.status:
        if recorded.status == 200 and strict_status:
            return ShapeDiff(problems=[f"{name}: status {recorded.status} became {live.status}"])
        if live.status == 200:
            return ShapeDiff(additions=[f"{name}: now served ({recorded.status} became 200)"])
        return ShapeDiff()
    if recorded.shape is None or live.shape is None:
        return ShapeDiff()
    return compare_shapes(recorded.shape, live.shape, where=name)


# --------------------------------------------------------------------------- the recorded document


def packaged_shapes_path() -> Path:
    return Path(str(resources.files("app").joinpath(SHAPES_FILENAME)))


def load_shapes(path: Path | None = None) -> dict[str, Any]:
    target = path or packaged_shapes_path()
    if not target.exists():
        return {}
    loaded: dict[str, Any] = json.loads(target.read_text(encoding="utf-8"))
    return loaded


def recorded_observations(document: Mapping[str, Any]) -> dict[str, Observation]:
    operations = document.get("operations", {})
    return {name: Observation.from_json(raw) for name, raw in operations.items() if isinstance(raw, Mapping)}


def render_document(
    observations: Mapping[str, Observation], *, recorded_from: str, server_version: str, now: datetime | None = None
) -> dict[str, Any]:
    return {
        "what": "response shapes of the read-only operations external callers depend on (app/contract.py)",
        "recorded_from": recorded_from,
        "server_version": server_version,
        "recorded_at": (now or datetime.now(UTC)).isoformat(timespec="seconds"),
        "operations": {name: obs.to_json() for name, obs in sorted(observations.items())},
    }


def write_shapes(path: Path, document: Mapping[str, Any]) -> None:
    path.write_text(json.dumps(document, indent=2, sort_keys=False) + "\n", encoding="utf-8")


def operations_named(names: Iterable[str]) -> list[Operation]:
    wanted = set(names)
    return [op for op in CONTRACT_OPERATIONS if op.name in wanted]
