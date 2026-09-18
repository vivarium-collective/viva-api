"""The standalone gate for ``viva_core`` (``docs/plan-core.md``, principle D7).

``viva_core`` must work with ``viva_api`` and ``app`` absent. ``make check`` enforces that
statically (the ``core-is-standalone`` import-linter contract); these tests enforce it at
runtime, and enforce the second rule the linter cannot see: no domain terms in core's
constructs. The suite grows with every phase -- by P5 it boots the core app with no hooks
registered and exercises each service end to end.
"""

import ast
import pkgutil
import subprocess
import sys
from pathlib import Path

import pytest

import viva_core

CORE_ROOT = Path(viva_core.__file__).resolve().parent

#: Organism, model and pipeline names belong to applications, never to core's constructs.
DOMAIN_TERMS = ("ecoli", "parca", "vecoli", "biocyc", "ptools")


def _core_modules() -> list[str]:
    modules = [info.name for info in pkgutil.walk_packages([str(CORE_ROOT)], prefix="viva_core.") if not info.ispkg]
    return ["viva_core", *sorted(modules)]


def test_every_core_module_imports_with_viva_api_and_app_blocked() -> None:
    """A fresh interpreter in which importing ``viva_api`` or ``app`` raises."""
    program = f"""
import importlib, sys

class _Blocked:
    def find_spec(self, name, path=None, target=None):
        if name.split(".")[0] in ("viva_api", "app", "sms_api"):
            raise ImportError(f"viva_core reached for {{name!r}} -- core must be standalone")
        return None

sys.meta_path.insert(0, _Blocked())
for module in {_core_modules()!r}:
    importlib.import_module(module)
leaked = sorted(m for m in sys.modules if m.split(".")[0] in ("viva_api", "app", "sms_api"))
assert not leaked, leaked
print("ok")
"""
    result = subprocess.run([sys.executable, "-c", program], capture_output=True, text=True, check=False)  # noqa: S603
    assert result.returncode == 0, result.stderr[-2000:]
    assert result.stdout.strip() == "ok"


def _docstring_nodes(tree: ast.Module) -> set[int]:
    found: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Module | ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef):
            body = node.body
            if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
                found.add(id(body[0].value))
    return found


def _construct_text(node: ast.AST, docstrings: set[int]) -> str | None:
    """The text a node contributes as a construct, or None. One node kind per line."""
    if isinstance(node, ast.Constant):
        return node.value if isinstance(node.value, str) and id(node) not in docstrings else None
    for kind, attribute in _NAMED_NODES:
        if isinstance(node, kind):
            value = getattr(node, attribute)
            return value if isinstance(value, str) else None
    return None


_NAMED_NODES: tuple[tuple[type[ast.AST], str], ...] = (
    (ast.Name, "id"),
    (ast.Attribute, "attr"),
    (ast.ClassDef, "name"),
    (ast.FunctionDef, "name"),
    (ast.AsyncFunctionDef, "name"),
    (ast.arg, "arg"),
    (ast.keyword, "arg"),
    (ast.alias, "name"),
)


def _constructs(tree: ast.Module) -> list[tuple[int, str]]:
    """Identifiers and non-docstring string constants -- NOT comments, NOT docstrings.

    An AST walk, never a line scan: a comment or docstring may name the application that
    motivated a design; a name, attribute, argument, keyword or string literal may not.
    """
    docstrings = _docstring_nodes(tree)
    found: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        text = _construct_text(node, docstrings)
        if text is not None:
            found.append((getattr(node, "lineno", 0), text))
    return found


@pytest.mark.parametrize("source", sorted(CORE_ROOT.rglob("*.py")), ids=lambda p: str(p.relative_to(CORE_ROOT)))
def test_no_domain_terms_in_core_constructs(source: Path) -> None:
    tree = ast.parse(source.read_text(encoding="utf-8"))
    offenders = [
        f"{source.relative_to(CORE_ROOT)}:{line}: {text[:80]!r}"
        for line, text in _constructs(tree)
        for term in DOMAIN_TERMS
        if term in text.lower()
    ]
    assert not offenders, "domain term(s) in a viva_core construct:\n  " + "\n  ".join(sorted(set(offenders)))
