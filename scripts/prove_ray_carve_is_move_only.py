"""Prove that a P2.1 cut changed only what it says it changed: every method of the Ray
service and of its Batch layer, compared BY SOURCE with ``origin/main``.

    git fetch origin && uv run python scripts/prove_ray_carve_is_move_only.py

``docs/plan-core.md`` P2.1 takes one 5,019-line class apart, one concern per PR. The early cuts
were pure moves and this script compared them byte for byte. The later ones are rewirings --
a mixin becomes a composed object, a private method becomes that object's public one -- and
"byte-identical" is no longer the claim. The claim is now: **every difference is one of a
short list of NAMED changes, and nothing else differs.** This script is that claim, runnable:

* ``RESPELLED`` -- the only textual change allowed inside a method that stayed put: a call to
  a member that moved is now spelled through the object it moved to. Each spelling is undone
  before comparing, so anything else still shows. A longer spelling can make the formatter
  re-flow the statement around it, so a respelled method is compared as an AST plus its
  comment lines: layout aside, still everything.
* ``RENAMED`` -- methods of the Batch layer that lost their underscore when they stopped being
  inherited and became another object's interface.
* ``REWIRED`` / ``GONE`` / ``NEW`` -- what the composition costs, each with its reason. A method
  that differs, disappears or appears WITHOUT being named there fails the script.

It proves the cut that is on the branch; the tables are rewritten per cut (their history is in
git, and in the plan's decision log). Once a cut has merged, both sides have the new shape
and the script reports "nothing to undo". What it cannot see -- ``hasattr`` probes, mocks,
strings -- is the job of mypy, the tests and the differential run described in each PR.

Delete this script when P2.1 is finished; it has no use after the carve.
"""

import ast
import subprocess
import sys
import textwrap
from collections.abc import Callable
from pathlib import Path

ROOT = "viva_api/simulation/"
SERVICE = (ROOT + "simulation_service_ray.py", "SimulationServiceRay")
LAYER = (ROOT + "ray/batch_layer.py", "RayBatchLayer")

# ---- P2.1 PR 5: ``RayBatchLayer`` stops being the service's base class and becomes ``service.batch``.

#: old name (a private method the service inherited) -> new name (the composed layer's interface)
RENAMED = {
    "_batch": "client",
    "_batch_jobs": "engine",
    "_image_uri": "image_uri",
    "_submit_image_uri": "submit_image_uri",
    "_ensure_mnp_job_def": "ensure_mnp_job_def",
    "_submit_mnp": "submit_mnp",
    "_ensure_container_job_def": "ensure_container_job_def",
    "_submit_container": "submit_container",
    "_resolve_log_group": "resolve_log_group",
}
#: how a call to one of them is spelled now -> how it was spelled. Inside the layer ...
RESPELLED_IN_LAYER = {f"self.{new}": f"self.{old}" for old, new in RENAMED.items()}
#: ... and inside the service, which reaches the layer through ``self.batch``.
RESPELLED_IN_SERVICE = {
    **{f"self.batch.{new}": f"self.{old}" for old, new in RENAMED.items()},
    "data_layout.RayLayout.results_uri(": "self._results_s3_uri(",
}
#: differ by design
REWIRED = {
    "__init__": "moved from the layer to the service, and now also builds ``self.batch``",
    "tasks": "RayTaskService is handed the layer, a latest-commit callable and a results-URI callable",
    "parca": "RayParcaService is handed the layer instead of the service",
}
GONE = {"_results_s3_uri": "a one-line wrapper of data_layout.RayLayout.results_uri; callers call that"}
#: on the service only because the scheduler and a handler still ask the SERVICE (until P6)
NEW_DELEGATES = {"get_batch_job_statuses", "get_batch_job_details"}


def methods_of(source: str | None, class_name: str) -> dict[str, str]:
    """``name -> source`` (decorators included) for each method of ``class_name``."""
    if source is None:
        return {}
    classes = [n for n in ast.parse(source).body if isinstance(n, ast.ClassDef) and n.name == class_name]
    if not classes:
        return {}
    lines = source.split("\n")
    found: dict[str, str] = {}
    for node in classes[0].body:
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            first = min([node.lineno] + [d.lineno for d in node.decorator_list])
            found[node.name] = "\n".join(lines[first - 1 : node.end_lineno])
    return found


def comments_of(source: str) -> list[str]:
    return [line.strip() for line in source.split("\n") if line.strip().startswith("#")]


def same_code(one: str, other: str) -> bool:
    dumps = [ast.dump(ast.parse(textwrap.dedent(source))) for source in (one, other)]
    return dumps[0] == dumps[1] and comments_of(one) == comments_of(other)


def undo(source: str, spellings: dict[str, str]) -> str:
    # longest first, so ``self.batch.submit_image_uri`` is not half-undone as ``...image_uri``
    for new in sorted(spellings, key=len, reverse=True):
        source = source.replace(new, spellings[new])
    return source


def read_origin_main(path: str) -> str | None:
    shown = subprocess.run(["git", "show", f"origin/main:{path}"], capture_output=True, text=True, check=False)  # noqa: S603, S607
    return shown.stdout if shown.returncode == 0 else None


def read_working_tree(path: str) -> str | None:
    file = Path(path)
    return file.read_text(encoding="utf-8") if file.exists() else None


def collect(read: Callable[[str], str | None]) -> tuple[dict[str, str], dict[str, str]]:
    return methods_of(read(SERVICE[0]), SERVICE[1]), methods_of(read(LAYER[0]), LAYER[1])


def main() -> int:
    service_before, layer_before = collect(read_origin_main)
    service_after, layer_after = collect(read_working_tree)

    if not set(RENAMED) & set(layer_before):
        print("origin/main already has the composed layer: nothing to undo, comparing as is")
        before = {("service", n): s for n, s in service_before.items()} | {
            ("layer", n): s for n, s in layer_before.items()
        }
        after = {("service", n): s for n, s in service_after.items()} | {
            ("layer", n): s for n, s in layer_after.items()
        }
        differing = sorted(k for k in before if k in after and before[k] != after[k])
        lost, added = sorted(set(before) - set(after)), sorted(set(after) - set(before))
        print(f"differing: {differing}\nlost: {lost}\nadded: {added}")
        return 1 if differing or lost or added else 0

    # origin/main: the service INHERITED the layer, so its methods are the two pooled.
    before = {**layer_before, **service_before}

    back = {new: old for old, new in RENAMED.items()}
    after: dict[str, str] = {}
    respelled: list[str] = []
    for name, source in layer_after.items():
        old_name = back.get(name, name)
        undone = undo(source, RESPELLED_IN_LAYER).replace(f"def {name}(", f"def {old_name}(", 1)
        after[old_name] = undone
    for name, source in service_after.items():
        if name in NEW_DELEGATES and name in after:
            continue  # the layer's is the method; the service's is the named delegate
        undone = undo(source, RESPELLED_IN_SERVICE)
        if undone != source:
            respelled.append(name)
        after[name] = undone

    differing = sorted(
        name for name in before if name in after and name not in REWIRED and not same_code(before[name], after[name])
    )
    lost = sorted(set(before) - set(after) - set(GONE))
    added = sorted(set(after) - set(before) - set(REWIRED))
    delegates = sorted(n for n in NEW_DELEGATES if n in service_after and n in layer_after)

    print(f"methods: origin/main {len(before)} (service + inherited layer)")
    print(f"methods: working tree {len(service_after)} service, {len(layer_after)} layer")
    print(f"layer methods renamed: {sorted(n for n in layer_after if n in back)}")
    print(f"service methods whose only change is how they spell a Batch-layer call: {len(respelled)}")
    print(f"named: rewired {sorted(REWIRED)}, gone {sorted(GONE)}, new delegates on the service {delegates}")
    print(f"differing: {differing}")
    print(f"lost: {lost}")
    print(f"added: {added}")
    return 1 if differing or lost or added or delegates != sorted(NEW_DELEGATES) else 0


if __name__ == "__main__":
    sys.exit(main())
