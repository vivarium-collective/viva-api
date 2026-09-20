"""Prove that a P2.1 cut is a pure move: every method of the carved ``SimulationServiceRay``
hierarchy, compared BY SOURCE with ``origin/main``.

    git fetch origin && uv run python scripts/prove_ray_carve_is_move_only.py

``docs/plan-core.md`` P2.1 carves one 4,300-line class into mixins, one concern per PR, and
each PR claims "byte-identical". This is the claim, runnable. It collects the methods of
every class of the hierarchy from the working tree and from ``origin/main`` and reports any
method whose source differs, was lost, was added, or is defined in two classes. A cut that
only MOVES methods prints ``differing: []``, ``lost: []``, ``added: []`` and exits 0.

Tasks, image build and the ParCa cache jobs are NOT in the hierarchy: they are composed
services (``ray/tasks.py``, ``ray/build.py``, ``ray/parca.py``), proven by a differential run
instead, because turning a mixin into a service is a rewiring and cannot be byte-identical.

Two kinds of change are not "a pure move" and are therefore NAMED here rather than tolerated:
``BECAME_FUNCTIONS`` (methods that never used ``self`` and are now module functions -- checked
below: same AST once ``self`` is dropped, same comment lines) and ``REWIRED`` / ``TO_A_SERVICE``
/ ``NEW`` (what a composition costs the class: a property, a facade, a method that left). Any
method that differs, disappears or appears WITHOUT being named there fails the script.

It compares methods, not module-level code: a moved module function or constant is covered
by mypy (an importer that still points at the old home fails) and by the tests.

Delete this script when P2.1 is finished; it has no use after the carve.
"""

import ast
import subprocess
import sys
from collections import Counter
from collections.abc import Callable
from pathlib import Path

ROOT = "viva_api/simulation/"

#: file -> the class in it that belongs to the hierarchy. Files that do not exist yet (on
#: either side) are skipped, so the later cuts' entries can sit here ahead of time.
HIERARCHY = {
    ROOT + "simulation_service_ray.py": "SimulationServiceRay",
    ROOT + "ray/batch_layer.py": "RayBatchLayer",
    ROOT + "ray/parca.py": "RayParcaMixin",  # origin/main only: PR 4 dissolved it
    ROOT + "ray/nextflow.py": "RayNextflowMixin",
    ROOT + "ray/mbp_tracked.py": "RayMbpTrackedMixin",
    ROOT + "ray/mnp.py": "RayMnpMixin",
    ROOT + "ray/chain.py": "RayChainMixin",
}


#: PR 4. Methods of ``RayParcaMixin`` that never touched ``self`` -> functions of ``ray/parca_spec.py``.
SPEC = ROOT + "ray/parca_spec.py"
BECAME_FUNCTIONS = {
    "_upstream_cache_s3_uri": "upstream_cache_s3_uri",
    "_parca_command": "parca_command",
    "_upstream_parca_command": "upstream_parca_command",
    "_build_new_gene_cache_command": "new_gene_cache_command",
    "_build_variant_cache_command": "variant_cache_command",
}
#: The ONLY textual change allowed inside a method that stayed: a call to one of those methods
#: is now a call to the function. Undone before comparing, so anything else still shows.
CALL_SITES = {f"parca_spec.{new}(": f"self.{old}(" for old, new in BECAME_FUNCTIONS.items()}

#: ``cache_s3_uri`` became a function too, and ALSO stays on the class as a one-line delegate
#: (the scheduler and the handlers ask the service for it); ``submit_parca_job`` stays as the
#: facade ``SimulationService`` requires. Both bodies differ by design.
REWIRED = {"cache_s3_uri": "cache_s3_uri", "submit_parca_job": None}
#: Left the class for ``RayParcaService``; proven by the differential run (PR 4's description).
TO_A_SERVICE = {"submit_new_gene_cache_job", "submit_variant_cache_job"}
#: What composing the service added to the class.
NEW = {"parca"}


def as_function(method_source: str, old: str, new: str) -> str:
    """A method that never used ``self``, as the module function it became: dedented,
    renamed, the ``self`` parameter dropped. Compared as an AST, so a signature the formatter
    re-flowed once ``self`` was gone is not a difference; comments are compared separately."""
    import textwrap

    tree = ast.parse(textwrap.dedent(method_source))
    function = tree.body[0]
    if not (isinstance(function, ast.FunctionDef) and function.name == old):
        raise SystemExit(f"{old}: not a plain method")
    if not (function.args.args and function.args.args[0].arg == "self"):
        raise SystemExit(f"{old}: its first parameter is not self")
    function.name, function.args.args = new, function.args.args[1:]
    if [n for n in ast.walk(function) if isinstance(n, ast.Name) and n.id == "self"]:
        raise SystemExit(f"{old} uses self: it cannot have become a function unchanged")
    return ast.dump(function)


def same_code(one: str, other: str) -> bool:
    import textwrap

    dumps = [ast.dump(ast.parse(textwrap.dedent(source))) for source in (one, other)]
    return dumps[0] == dumps[1] and comments_of(one) == comments_of(other)


def comments_of(source: str) -> list[str]:
    return [line.strip() for line in source.split("\n") if line.strip().startswith("#")]


def check_functions(before: dict[str, str], read: Callable[[str], str | None]) -> list[str]:
    spec = read(SPEC)
    if spec is None:
        return [f"{SPEC} does not exist"]
    lines, problems = spec.split("\n"), []
    functions = {n.name: n for n in ast.parse(spec).body if isinstance(n, ast.FunctionDef)}
    for old, new in {**BECAME_FUNCTIONS, "cache_s3_uri": "cache_s3_uri"}.items():
        if old not in before:
            continue  # already gone from origin/main: this PR has merged, nothing left to compare
        if new not in functions:
            problems.append(f"{old} -> {new}: not in {SPEC}")
            continue
        node = functions[new]
        if ast.dump(node) != as_function(before[old], old, new):
            problems.append(f"{old} -> {new}: the function is not the method with self dropped")
        if comments_of(before[old]) != comments_of("\n".join(lines[node.lineno - 1 : node.end_lineno])):
            problems.append(f"{old} -> {new}: comment lines differ")
    return problems


def methods_of(source: str, class_name: str) -> dict[str, str]:
    """``name -> source`` (decorators included) for each method of ``class_name``."""
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


def collect(read: Callable[[str], str | None]) -> tuple[dict[str, str], dict[str, str]]:
    sources: dict[str, str] = {}
    owners: dict[str, str] = {}
    for path, class_name in HIERARCHY.items():
        text = read(path)
        if text is None:
            continue
        for name, source in methods_of(text, class_name).items():
            if name in sources:
                raise SystemExit(f"{name} is defined in both {owners[name]} and {class_name}")
            sources[name], owners[name] = source, class_name
    return sources, owners


def read_origin_main(path: str) -> str | None:
    shown = subprocess.run(["git", "show", f"origin/main:{path}"], capture_output=True, text=True, check=False)  # noqa: S603, S607
    return shown.stdout if shown.returncode == 0 else None


def read_working_tree(path: str) -> str | None:
    file = Path(path)
    return file.read_text(encoding="utf-8") if file.exists() else None


def main() -> int:
    before, _ = collect(read_origin_main)
    after, owners = collect(read_working_tree)
    respelled = []
    for name, source in after.items():
        undone = source
        for new_call, old_call in CALL_SITES.items():
            undone = undone.replace(new_call, old_call)
        if undone != source:
            respelled.append(name)
            # The longer spelling can make the formatter re-flow the statement around it, so a
            # respelled method is compared as an AST plus its comment lines -- layout aside,
            # still everything -- and then counts as identical.
            same = name in before and same_code(before[name], undone)
            after[name] = before[name] if same else undone
    print(f"methods whose only change may be a respelled ParCa call: {sorted(respelled)}")
    differing = sorted(name for name in before if name in after and before[name] != after[name])
    lost, added = sorted(set(before) - set(after)), sorted(set(after) - set(before))
    named = (
        f"named: {len(BECAME_FUNCTIONS) + 1} became functions, {len(REWIRED)} rewired, "
        f"{len(TO_A_SERVICE)} to a service, {len(NEW)} new"
    )
    print(named)
    not_as_named = check_functions(before, read_working_tree)
    print(f"functions that are not their method: {not_as_named}")
    differing = [n for n in differing if n not in REWIRED]
    lost = [n for n in lost if n not in BECAME_FUNCTIONS and n not in TO_A_SERVICE]
    added = [n for n in added if n not in NEW]
    print(f"methods: origin/main {len(before)}, working tree {len(after)}")
    print(f"differing: {differing}")
    print(f"lost: {lost}")
    print(f"added: {added}")
    print(f"where they live: {dict(Counter(owners.values()))}")
    return 1 if differing or lost or added or not_as_named else 0


if __name__ == "__main__":
    sys.exit(main())
