"""D12 -- ``viva_core`` carries no ``Any`` (``docs/plan-core.md``).

``strict = true`` does not forbid ``Any``. Two mypy flags do, and they are on for ``viva_core.*``
and for ``viva_api.simulation.dispatch.*`` -- both GLOBS: a module that moves into core, or a new module
of the dispatch package, comes under the ban the day it arrives. (The dispatch package was listed by name
while the dispatch strategies moved; plan sequence PR 12 made it a glob.) mypy enforces the ban
itself; this file guards the two ways the ban could quietly stop meaning anything:

* the override disappears from ``pyproject.toml``, or stops covering a package as a glob;
* the one escape hatch -- ``# type: ignore[explicit-any]`` -- is used for something other than the
  one thing it exists for.

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
DISPATCH_PACKAGE = Path("viva_api/simulation/dispatch")


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


def test_the_ban_covers_the_dispatch_package_as_a_glob() -> None:
    """A glob, so nothing has to remember to list a new module. A module named on its own beside
    the glob would be harmless to mypy and misleading to a reader, so there are none."""
    modules = _banned_modules()
    assert "viva_api.simulation.dispatch.*" in modules and "viva_api.simulation.dispatch" in modules, modules
    by_name = sorted(m for m in modules if m.startswith("viva_api.simulation.dispatch.") and not m.endswith(".*"))
    assert not by_name, f"listed by name beside the glob that already covers them: {by_name}"
    assert any(DISPATCH_PACKAGE.glob("*.py")), "found no modules: is this running from the repository root?"


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
    for root in (Path("viva_core"), DISPATCH_PACKAGE):
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
