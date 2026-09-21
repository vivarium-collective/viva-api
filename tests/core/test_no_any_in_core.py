"""D12 -- ``viva_core`` carries no ``Any`` (``docs/plan-core.md``).

``strict = true`` does not forbid ``Any``. Two mypy flags do, and they are on for ``viva_core.*``
(a glob: a module that moves into core later comes under the ban the day it moves) and for the
modules of ``viva_api.simulation.ray`` by name. mypy enforces the ban itself; this file guards the
three ways the ban could quietly stop meaning anything:

* the override disappears from ``pyproject.toml``, or stops covering a package;
* the one escape hatch -- ``# type: ignore[explicit-any]`` -- is used for something other than the
  one thing it exists for;
* a new module appears in the ray package and is simply never listed.

The escape hatch, and why it exists: under ``disallow_any_explicit`` mypy reports EVERY subclass of
pydantic's ``BaseModel`` / ``BaseSettings`` on its ``class`` line, because pydantic's own
``__init__(self, **data: Any)`` is part of the class. Three lines reproduce it; neither the
``pydantic.mypy`` plugin nor its ``init_typed`` option removes it. So those ``class`` lines, and
only those, carry the ignore. ``warn_unused_ignores`` (part of ``strict``) retires each one by
itself the day pydantic or mypy fixes it.
"""

import ast
import re
import tomllib
from pathlib import Path

IGNORE = re.compile(r"#\s*type:\s*ignore\[[^\]]*explicit-any[^\]]*\]")
PYDANTIC_BASES = {"BaseModel", "BaseSettings"}
RAY_PACKAGE = Path("viva_api/simulation/ray")

#: Modules of the ray package that are deliberately NOT under the ban yet. The dispatch strategies
#: (plan sequence PRs 7-11) arrive with the ``Any`` they have today, because those PRs are proven
#: by AST identity and an annotation change is an AST change. PR 12 empties this set and turns the
#: by-name list into ``viva_api.simulation.ray.*``. Adding a name here is a decision; say why.
NOT_YET_BANNED: set[str] = {
    # PR 7: moved with the `dict[str, Any]` dispatch block and command parameters it had in the service.
    "viva_api.simulation.ray.mbp_tracked",
    # PR 8: moved with its `dict[str, Any]` dispatch block, resources map and rendered params.
    "viva_api.simulation.ray.nextflow",
    # PR 9: moved with its `dict[str, Any]` dispatch block, params and analysis config.
    "viva_api.simulation.ray.multi_node",
}


def _banned_modules() -> list[str]:
    config = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))
    banned = [
        override
        for override in config["tool"]["mypy"]["overrides"]
        if override.get("disallow_any_explicit") is True and override.get("disallow_any_unimported") is True
    ]
    assert len(banned) == 1, "expected exactly one mypy override carrying the D12 ban"
    modules = banned[0]["module"]
    return [modules] if isinstance(modules, str) else list(modules)


def test_the_ban_covers_core_as_a_glob() -> None:
    modules = _banned_modules()
    assert "viva_core.*" in modules and "viva_core" in modules, modules


def test_every_module_of_the_ray_package_is_banned_or_named_as_not_yet() -> None:
    modules = set(_banned_modules())
    on_disk = {f"viva_api.simulation.ray.{path.stem}" for path in RAY_PACKAGE.glob("*.py") if path.stem != "__init__"}
    assert on_disk, "found no modules: is this running from the repository root?"
    if "viva_api.simulation.ray.*" in modules:
        assert not NOT_YET_BANNED, "the package is banned as a glob: NOT_YET_BANNED must be empty"
        return
    undecided = sorted(on_disk - modules - NOT_YET_BANNED)
    assert not undecided, (
        f"new in viva_api/simulation/ray and neither under the D12 ban (pyproject.toml) nor named in "
        f"NOT_YET_BANNED here: {undecided}"
    )
    stale = sorted((modules | NOT_YET_BANNED) - on_disk - {"viva_core", "viva_core.*", "viva_api.simulation.ray"})
    assert not stale, f"listed but not on disk: {stale}"
    assert not (modules & NOT_YET_BANNED), "a module is both banned and named as not yet banned"


def _explicit_any_ignores(path: Path) -> list[tuple[int, str]]:
    lines = path.read_text(encoding="utf-8").split("\n")
    return [(number, line) for number, line in enumerate(lines, start=1) if IGNORE.search(line)]


def _pydantic_class_lines(path: Path) -> set[int]:
    """Line numbers of ``class X(... BaseModel | BaseSettings ...)``, plus classes whose base is
    such a class defined in the same file (a subclass of a model is a model)."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    classes = [node for node in ast.walk(tree) if isinstance(node, ast.ClassDef)]
    models: set[str] = set()
    lines: set[int] = set()
    changed = True
    while changed:
        changed = False
        for node in classes:
            bases = {base.id for base in node.bases if isinstance(base, ast.Name)} | {
                base.attr for base in node.bases if isinstance(base, ast.Attribute)
            }
            if node.name not in models and bases & (PYDANTIC_BASES | models):
                models.add(node.name)
                lines.add(node.lineno)
                changed = True
    return lines


def test_the_escape_hatch_is_used_only_on_pydantic_class_lines() -> None:
    misused = []
    count = 0
    for root in (Path("viva_core"), RAY_PACKAGE):
        for path in sorted(root.rglob("*.py")):
            allowed = _pydantic_class_lines(path)
            for number, line in _explicit_any_ignores(path):
                count += 1
                if number not in allowed:
                    misused.append(f"{path}:{number}: {line.strip()[:100]}")
    assert not misused, (
        "`# type: ignore[explicit-any]` hides a real Any (it is for pydantic class lines only):\n  "
        + "\n  ".join(misused)
    )
    assert count > 0, "no ignores found at all: has the pattern drifted from what the code writes?"


def test_the_guard_tells_a_model_class_from_anything_else(tmp_path: Path) -> None:
    source = tmp_path / "m.py"
    source.write_text(
        "from pydantic import BaseModel\n"
        "import pydantic_settings\n"
        "class A(BaseModel):  # type: ignore[explicit-any]\n    x: int\n"
        "class B(A):  # type: ignore[explicit-any]\n    y: int\n"
        "class S(pydantic_settings.BaseSettings):  # type: ignore[explicit-any]\n    z: int\n"
        "class Plain:  # type: ignore[explicit-any]\n    pass\n"
        "def f(v):  # type: ignore[explicit-any]\n    return v\n",
        encoding="utf-8",
    )
    allowed = _pydantic_class_lines(source)
    flagged = [number for number, _ in _explicit_any_ignores(source) if number not in allowed]
    assert allowed == {3, 5, 7}
    assert flagged == [9, 11]
