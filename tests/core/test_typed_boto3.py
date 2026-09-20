"""``types-boto3`` types the AWS clients for mypy and must never be needed to RUN anything.

It is a dev dependency, and the API image is built with ``uv sync --no-default-groups``
(``Dockerfile-api``): the package that makes ``from types_boto3_batch import BatchClient`` work
on a laptop and in CI does not exist in production. An import of it outside ``if
TYPE_CHECKING:`` would pass every test here and fail at import time in the pod -- of the Batch
engine, the module everything submits through. So it is checked two ways: by reading the
source, and by importing the typed modules with the stubs made unimportable.
"""

import ast
import subprocess
import sys
from pathlib import Path

STUB_PACKAGES = ("types_boto3", "mypy_boto3", "types_aiobotocore", "types_aioboto3", "botocore_stubs")
RUNTIME_PACKAGES = ("viva_core", "viva_api", "app")

#: the modules annotated against the stubs (docs/plan-core.md P2.1, PR 6a)
TYPED_MODULES = (
    "viva_core.backends.batch",
    "viva_api.simulation.ray.batch_layer",
    "viva_api.simulation.batch_build",
    "viva_api.simulation.simulation_service_ray",
    "viva_api.simulation.simulation_service_k8s",
    # typed in PR 6b (D12)
    "viva_api.simulation.ray.tasks",
    "viva_core.storage.file_service_qumulo_s3",
    "viva_core.backends.k8s_job_service",
)


def _is_type_checking(test: ast.expr) -> bool:
    return (isinstance(test, ast.Name) and test.id == "TYPE_CHECKING") or (
        isinstance(test, ast.Attribute) and test.attr == "TYPE_CHECKING"
    )


def _runtime_stub_imports(tree: ast.AST) -> list[tuple[int, str]]:
    """Imports of a stub package that are NOT under ``if TYPE_CHECKING:``."""
    found: list[tuple[int, str]] = []

    def visit(node: ast.AST, guarded: bool) -> None:
        if isinstance(node, ast.If) and _is_type_checking(node.test):
            for statement in node.body:
                visit(statement, True)
            for statement in node.orelse:
                visit(statement, guarded)
            return
        names: list[str] = []
        if isinstance(node, ast.Import):
            names = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom) and node.module:
            names = [node.module]
        for name in names:
            if name.split(".")[0].startswith(STUB_PACKAGES) and not guarded:
                found.append((getattr(node, "lineno", 0), name))
        for child in ast.iter_child_nodes(node):
            visit(child, guarded)

    visit(tree, False)
    return found


def test_the_stubs_are_imported_only_under_type_checking() -> None:
    offenders = []
    scanned = 0
    for package in RUNTIME_PACKAGES:
        for path in sorted(Path(package).rglob("*.py")):
            scanned += 1
            for line, name in _runtime_stub_imports(ast.parse(path.read_text(encoding="utf-8"))):
                offenders.append(f"{path}:{line} imports {name} at runtime")
    assert scanned > 100, f"scanned only {scanned} files: is this running from the repository root?"
    assert not offenders, "a dev-only stub package is imported outside `if TYPE_CHECKING:`:\n  " + "\n  ".join(
        offenders
    )


def test_the_scan_sees_an_unguarded_import_and_ignores_a_guarded_one() -> None:
    unguarded = ast.parse("from types_boto3_batch import BatchClient\n")
    guarded = ast.parse(
        "from typing import TYPE_CHECKING\nif TYPE_CHECKING:\n    from types_boto3_batch import BatchClient\n"
    )
    in_else = ast.parse("import typing\nif typing.TYPE_CHECKING:\n    pass\nelse:\n    import mypy_boto3_s3\n")
    assert [name for _, name in _runtime_stub_imports(unguarded)] == ["types_boto3_batch"]
    assert _runtime_stub_imports(guarded) == []
    assert [name for _, name in _runtime_stub_imports(in_else)] == ["mypy_boto3_s3"]


def test_the_typed_modules_import_with_the_stubs_unavailable() -> None:
    """What production looks like: every stub package raises ``ImportError``."""
    program = f"""
import importlib, importlib.abc, sys

class NoStubs(importlib.abc.MetaPathFinder):
    def find_spec(self, name, path=None, target=None):
        if name.split(".")[0].startswith({STUB_PACKAGES!r}):
            raise ImportError(f"{{name}} is a dev dependency and is not installed here")
        return None

sys.meta_path.insert(0, NoStubs())
try:
    import types_boto3_batch
except ImportError:
    pass
else:
    raise SystemExit("the blocker does not block: this test would prove nothing")
for module in {TYPED_MODULES!r}:
    importlib.import_module(module)
print("imported", len({TYPED_MODULES!r}))
"""
    done = subprocess.run([sys.executable, "-c", program], capture_output=True, text=True, check=False, timeout=180)  # noqa: S603
    assert done.returncode == 0, done.stderr[-2000:]
    assert done.stdout.strip().endswith(f"imported {len(TYPED_MODULES)}")
