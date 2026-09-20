"""Prove that a P2.1 cut is a pure move: every method of the carved ``SimulationServiceRay``
hierarchy, compared BY SOURCE with ``origin/main``.

    git fetch origin && uv run python scripts/prove_ray_carve_is_move_only.py

``docs/plan-core.md`` P2.1 carves one 4,300-line class into mixins, one concern per PR, and
each PR claims "byte-identical". This is the claim, runnable. It collects the methods of
every class of the hierarchy from the working tree and from ``origin/main`` and reports any
method whose source differs, was lost, was added, or is defined in two classes. A cut that
only MOVES methods prints ``differing: []``, ``lost: []``, ``added: []`` and exits 0.

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
    ROOT + "ray/tasks.py": "RayTasksMixin",
    ROOT + "ray/build.py": "RayBuildMixin",
    ROOT + "ray/parca.py": "RayParcaMixin",
    ROOT + "ray/analysis.py": "RayAnalysisMixin",
    ROOT + "ray/nextflow.py": "RayNextflowMixin",
    ROOT + "ray/mbp_tracked.py": "RayMbpTrackedMixin",
    ROOT + "ray/mnp.py": "RayMnpMixin",
    ROOT + "ray/chain.py": "RayChainMixin",
}


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
    differing = sorted(name for name in before if name in after and before[name] != after[name])
    lost, added = sorted(set(before) - set(after)), sorted(set(after) - set(before))
    print(f"methods: origin/main {len(before)}, working tree {len(after)}")
    print(f"differing: {differing}")
    print(f"lost: {lost}")
    print(f"added: {added}")
    print(f"where they live: {dict(Counter(owners.values()))}")
    return 1 if differing or lost or added else 0


if __name__ == "__main__":
    sys.exit(main())
