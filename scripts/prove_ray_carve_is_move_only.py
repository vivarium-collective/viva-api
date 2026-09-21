"""Prove that a P2.1 cut changed only what it says it changed: every method of the Ray
service, compared BY SOURCE with ``origin/main``.

    git fetch origin && uv run python scripts/prove_ray_carve_is_move_only.py

``docs/plan-core.md`` P2.1 takes one 5,019-line class apart, one concern per PR. The claim each
cut makes is: **every difference from ``origin/main`` is one of a short list of NAMED changes,
and nothing else differs.** This script is that claim, runnable. The tables below describe the
cut that is on the branch and are rewritten per cut (their history is in git and in the plan's
decision log); the machinery under them does not change:

* ``BECAME_FUNCTIONS`` -- methods that never touched ``self`` and are now module functions. Checked:
  same AST once ``self`` is dropped and the name changed, same comment lines.
* ``BECAME_STRATEGY_METHODS`` -- a mechanism's methods, now methods of a strategy object. Checked:
  same AST and comment lines once the listed respellings are undone. A respelling is the ONLY
  textual change allowed: how the method reaches something it used to find on ``self``.
* ``RESPELLED_IN_SERVICE`` -- the same, for methods that stayed and call something that moved.
* ``REWIRED`` / ``REWIRED_BODIES`` -- a method that stays on the class as a delegate (something outside
  still asks the service): the delegate differs by design, and the body it left behind must be found,
  unchanged, where the table says.
* ``EXTRACTED_TAILS`` -- a mechanism that was never a method but the END of one (the router's fall-through
  path): the tail must be the new method's body, the new method's preamble must be statements of the
  old head, and what stays must be the old head plus one hand-over ``return``.
* ``NEW`` -- what composing the strategy added to the class, with the reason.

A method that differs, disappears or appears WITHOUT being named fails the script. Comparison is
by AST plus comment lines, because a longer spelling can make the formatter re-flow a statement:
layout aside, that is still everything. What this cannot see -- ``hasattr`` probes, mocks,
strings, which OBJECT a strategy was handed -- is the job of mypy, the tests and the differential
run described in each PR.

Once a cut has merged both sides have the new shape, and the script says so and compares as is.
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

# ---- P2.1 PR 10: the router's inline ensemble path becomes ``EnsembleStrategy``.

ENSEMBLE = ROOT + "ray/ensemble.py"

#: old method -> (file, new function name). A ``@staticmethod`` had no ``self`` to drop.
BECAME_FUNCTIONS: dict[str, tuple[str, str]] = {
    "_sim_command": (ENSEMBLE, "sim_command"),
}
#: how the strategy spells what the old code found on ``self`` -> how it spelled it
_IN_STRATEGY = {
    "self._batch.": "self.batch.",
    "self._stage_runner(": "self.stage_runner(",
    "parca_spec.cache_s3_uri(": "self.cache_s3_uri(",
    "sim_command(": "self._sim_command(",
}
#: old method -> (file, class, new method name, {new spelling: old spelling})
BECAME_STRATEGY_METHODS: dict[str, tuple[str, str, str, dict[str, str]]] = {}
RESPELLED_IN_SERVICE: dict[str, str] = {}
REWIRED: dict[str, str] = {}
REWIRED_BODIES: dict[str, tuple[str, str, str, dict[str, str]]] = {}
#: This mechanism was not a method. It was the TAIL of the router: every statement from the one that
#: assigns ``first_assigned`` to the end. Checked three ways -- the tail is the new method's body after
#: its docstring and a preamble; every preamble statement is a statement of the router's head,
#: verbatim (the locals the tail read from there); and the router is its old head plus ONE return.
EXTRACTED_TAILS: dict[str, dict[str, object]] = {
    "submit_ecoli_simulation_job": {
        "first_assigned": "parca_dataset",
        "to": (ENSEMBLE, "EnsembleStrategy", "submit"),
        "preamble": 3,
        "respellings": _IN_STRATEGY,
        "hand_over": (
            "return await self._ensemble().submit(ecoli_simulation, database_service, correlation_id=correlation_id)"
        ),
    },
}
NEW = {"_ensemble": "builds EnsembleStrategy(self.batch, stage_runner=...)"}


def functions_of(source: str | None, class_name: str | None) -> dict[str, str]:
    """``name -> source`` (decorators included) of a class's methods, or of a module's functions."""
    if source is None:
        return {}
    tree = ast.parse(source)
    if class_name is None:
        body: list[ast.stmt] = tree.body
    else:
        classes = [n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == class_name]
        if not classes:
            return {}
        body = classes[0].body
    lines = source.split("\n")
    found: dict[str, str] = {}
    for node in body:
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
    for new in sorted(spellings, key=len, reverse=True):  # longest first: no half-undone prefixes
        source = source.replace(new, spellings[new])
    return source


def as_method(function_source: str, new: str, old: str, *, static: bool) -> str:
    """A module function, as the method it was: the old name, and ``self`` put back -- unless it
    was a ``@staticmethod``, which had none (its decorator is dropped from the other side)."""
    tree = ast.parse(textwrap.dedent(function_source))
    function = tree.body[0]
    if not isinstance(function, ast.FunctionDef | ast.AsyncFunctionDef) or function.name != new:
        raise SystemExit(f"{new}: not a plain function")
    function.name = old
    if not static:
        function.args.args.insert(0, ast.arg(arg="self"))
    return ast.dump(function)


def without_staticmethod(method_source: str) -> tuple[ast.stmt, bool]:
    node = ast.parse(textwrap.dedent(method_source)).body[0]
    if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
        raise SystemExit("not a method")
    static = any(isinstance(d, ast.Name) and d.id == "staticmethod" for d in node.decorator_list)
    node.decorator_list = [d for d in node.decorator_list if not (isinstance(d, ast.Name) and d.id == "staticmethod")]
    return node, static


def read_origin_main(path: str) -> str | None:
    shown = subprocess.run(["git", "show", f"origin/main:{path}"], capture_output=True, text=True, check=False)  # noqa: S603, S607
    return shown.stdout if shown.returncode == 0 else None


def read_working_tree(path: str) -> str | None:
    file = Path(path)
    return file.read_text(encoding="utf-8") if file.exists() else None


def compare_as_is(before: dict[str, str], after: dict[str, str]) -> int:
    differing = sorted(n for n in before if n in after and before[n] != after[n])
    lost, added = sorted(set(before) - set(after)), sorted(set(after) - set(before))
    print(f"differing: {differing}\nlost: {lost}\nadded: {added}")
    return 1 if differing or lost or added else 0


def check_moved(before: dict[str, str], read: Callable[[str], str | None]) -> list[str]:
    problems: list[str] = []
    for old, (path, new) in BECAME_FUNCTIONS.items():
        functions = functions_of(read(path), None)
        if new not in functions:
            problems.append(f"{old} -> {path}::{new}: not there")
            continue
        was, static = without_staticmethod(before[old])
        now = as_method(functions[new], new, old, static=static)
        if now != ast.dump(was) or comments_of(before[old]) != comments_of(functions[new]):
            problems.append(f"{old} -> {new}: the function is not the method with self dropped")
    for old, (path, class_name, new, spellings) in {**BECAME_STRATEGY_METHODS, **REWIRED_BODIES}.items():
        methods = functions_of(read(path), class_name)
        if new not in methods:
            problems.append(f"{old} -> {path}::{class_name}.{new}: not there")
            continue
        undone = undo(methods[new], spellings).replace(f"def {new}(", f"def {old}(", 1)
        if not same_code(before[old], undone):
            problems.append(f"{old} -> {class_name}.{new}: differs by more than the named respellings")
    return problems


def _function_node(source: str) -> ast.FunctionDef | ast.AsyncFunctionDef:
    node = ast.parse(textwrap.dedent(source)).body[0]
    if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
        raise SystemExit("not a function")
    return node


def _statements_source(source: str, statements: list[ast.stmt]) -> str:
    """The source lines spanned by ``statements`` -- with the comment lines directly above the first."""
    lines = textwrap.dedent(source).split("\n")
    first = statements[0].lineno - 1
    while first > 0 and lines[first - 1].strip().startswith("#"):
        first -= 1
    return "\n".join(lines[first : statements[-1].end_lineno])


def check_extracted_tails(
    before: dict[str, str], after: dict[str, str], read: Callable[[str], str | None]
) -> list[str]:
    problems: list[str] = []
    for old, spec in EXTRACTED_TAILS.items():
        path, class_name, new = spec["to"]  # type: ignore[misc]
        methods = functions_of(read(str(path)), str(class_name))
        if str(new) not in methods or old not in before or old not in after:
            problems.append(f"{old}: tail -> {class_name}.{new}: not there")
            continue
        was = _function_node(before[old])
        k = next(
            i
            for i, s in enumerate(was.body)
            if isinstance(s, ast.Assign) and getattr(s.targets[0], "id", "") == spec["first_assigned"]
        )
        head, tail = was.body[:k], was.body[k:]
        undone = undo(methods[str(new)], spec["respellings"])  # type: ignore[arg-type]
        now = _function_node(undone)
        skip = 1 + int(spec["preamble"])  # type: ignore[call-overload]  # docstring, then the preamble
        preamble, body = now.body[1:skip], now.body[skip:]
        if [ast.dump(s) for s in tail] != [ast.dump(s) for s in body]:
            problems.append(f"{old}: the tail is not {class_name}.{new}'s body (beyond the named respellings)")
        if comments_of(_statements_source(before[old], tail)) != comments_of(_statements_source(undone, body)):
            problems.append(f"{old}: the tail's comment lines differ")
        head_dumps = {ast.dump(s) for s in head}
        strangers = [ast.unparse(s) for s in preamble if ast.dump(s) not in head_dumps]
        if strangers:
            problems.append(f"{class_name}.{new}: preamble statements that are not the router's own: {strangers}")
        kept = _function_node(after[old])
        expected = [ast.dump(s) for s in head]
        got = [ast.dump(s) for s in kept.body]
        if got[:-1] != expected or ast.unparse(kept.body[-1]) != ast.unparse(
            _function_node("async def f():\n    " + str(spec["hand_over"])).body[0]
        ):
            problems.append(f"{old}: what stayed is not its old head plus the one hand-over return")
        if comments_of(_statements_source(before[old], head)) != comments_of(
            _statements_source(after[old], kept.body[:-1])
        ):
            problems.append(f"{old}: the head's comment lines differ")
    return problems


def main() -> int:
    before = functions_of(read_origin_main(SERVICE[0]), SERVICE[1])
    after = functions_of(read_working_tree(SERVICE[0]), SERVICE[1])
    moved = set(BECAME_FUNCTIONS) | set(BECAME_STRATEGY_METHODS)
    split = set(EXTRACTED_TAILS)
    if not moved & set(before):
        print("origin/main already has this cut: nothing to undo, comparing as is")
        return compare_as_is(before, after)

    not_as_named = check_moved(before, read_working_tree) + check_extracted_tails(before, after, read_working_tree)
    respelled = []
    for name, source in after.items():
        undone = undo(source, RESPELLED_IN_SERVICE)
        if undone != source:
            respelled.append(name)
            after[name] = undone
    differing = sorted(
        n for n in before if n in after and n not in REWIRED and n not in split and not same_code(before[n], after[n])
    )
    lost = sorted(set(before) - set(after) - moved)
    added = sorted(set(after) - set(before) - set(NEW))

    print(f"methods: origin/main {len(before)}, working tree {len(after)}")
    print(f"became functions: {sorted(BECAME_FUNCTIONS)}")
    print(f"became strategy methods: {sorted(BECAME_STRATEGY_METHODS)}; tails extracted from: {sorted(split)}")
    print(f"moved, but not as named: {not_as_named}")
    print(f"stayed, respelled: {sorted(respelled)}; rewired: {sorted(REWIRED)}; new: {sorted(NEW)}")
    print(f"differing: {differing}")
    print(f"lost: {lost}")
    print(f"added: {added}")
    return 1 if differing or lost or added or not_as_named else 0


if __name__ == "__main__":
    sys.exit(main())
