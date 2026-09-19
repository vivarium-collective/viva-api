"""The Ray service reaches settings and AWS through ONE seam, and only through it.

``viva_api/simulation/ray/_seams.py`` explains why: the tests patch ``get_settings`` by
NAME, and a name patch is positional -- it reaches only the module it names. When the
4,305-line ``SimulationServiceRay`` is carved into several modules (``docs/plan-core.md``
P2.1), a module that imported ``get_settings`` directly would silently stop being patched
and run with the developer's real settings. These tests are the ratchet that keeps the carve
honest: they fail the moment a Ray-service module binds either name itself.
"""

import ast
from pathlib import Path
from unittest.mock import patch

import pytest

import viva_api.simulation.ray as ray_package
from viva_api.simulation import simulation_service_ray

SEAM_NAMES = {"boto3", "get_settings"}
RAY_PACKAGE_DIR = Path(ray_package.__file__).resolve().parent
TESTS_DIR = Path(__file__).resolve().parents[1]


def _ray_service_modules() -> list[Path]:
    """``simulation_service_ray.py`` and everything in ``simulation/ray/`` except the seam."""
    carved = [p for p in sorted(RAY_PACKAGE_DIR.rglob("*.py")) if p.name != "_seams.py"]
    return [Path(simulation_service_ray.__file__).resolve(), *carved]


def _names_bound_by_imports(tree: ast.Module) -> set[str]:
    bound: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            bound |= {(alias.asname or alias.name).split(".")[0] for alias in node.names}
        elif isinstance(node, ast.ImportFrom):
            bound |= {alias.asname or alias.name for alias in node.names}
    return bound


@pytest.mark.parametrize("module_path", _ray_service_modules(), ids=lambda p: p.name)
def test_no_ray_service_module_binds_a_seam_name_itself(module_path: Path) -> None:
    """Not ``import boto3``, not ``from viva_api.config import get_settings``, and not
    ``from ..._seams import get_settings`` either -- that last one LOOKS like using the seam
    and defeats it, because it copies the name into this module at import time."""
    bound = _names_bound_by_imports(ast.parse(module_path.read_text(encoding="utf-8")))
    offenders = sorted(bound & SEAM_NAMES)
    assert not offenders, (
        f"{module_path.name} imports {offenders} directly. Read them through the seam at call time "
        "(`_seams.get_settings()`, `_seams.boto3.client(...)`) so the tests' patches keep reaching this code."
    )


def test_the_service_module_no_longer_carries_the_names() -> None:
    for name in SEAM_NAMES:
        assert not hasattr(simulation_service_ray, name), f"simulation_service_ray.{name} is bound again"


def test_no_test_patches_the_old_location() -> None:
    """A patch aimed at ``simulation_service_ray.get_settings`` now either errors or --
    worse, if someone re-adds the import to make it pass -- patches a name nothing reads."""
    old = "viva_api.simulation.simulation_service_ray."
    this_file = Path(__file__).resolve()
    offenders = [
        f"{path.relative_to(TESTS_DIR)}:{number}"
        for path in sorted(TESTS_DIR.rglob("*.py"))
        if path.resolve() != this_file
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1)
        if any(f"{old}{name}" in line for name in SEAM_NAMES) and ("patch" in line or "setattr" in line)
    ]
    assert not offenders, "patch the seam (viva_api.simulation.ray._seams.<name>) instead:\n  " + "\n  ".join(offenders)


def test_a_patch_on_the_seam_reaches_the_service() -> None:
    """The point of the whole arrangement, exercised end to end on a cheap method."""
    from types import SimpleNamespace

    fake = SimpleNamespace(ecr_account_id="123456789012", batch_region="xx-test-1", ray_ecr_repository="some-repo")
    with patch("viva_api.simulation.ray._seams.get_settings", lambda: fake):
        uri = simulation_service_ray.SimulationServiceRay()._image_uri("abc1234")
    assert "123456789012" in uri and "xx-test-1" in uri and uri.endswith("some-repo:abc1234")


def test_no_method_is_defined_twice_across_the_carved_classes() -> None:
    """The carve moves methods out of ``SimulationServiceRay`` into mixins it inherits. A
    method left behind in the service -- or copied into two mixins -- would silently shadow
    the other by MRO, and the shadowed copy would rot unnoticed. One definition each."""
    carved = [
        c
        for c in simulation_service_ray.SimulationServiceRay.__mro__
        if c.__module__.startswith("viva_api.simulation.ray.")
    ]
    assert carved, "SimulationServiceRay inherits nothing from viva_api.simulation.ray -- has the carve been undone?"
    owners: dict[str, list[str]] = {}
    for cls in [simulation_service_ray.SimulationServiceRay, *carved]:
        for name, value in vars(cls).items():
            if callable(value) or isinstance(value, staticmethod | classmethod):
                owners.setdefault(name, []).append(cls.__name__)
    twice = {name: classes for name, classes in owners.items() if len(classes) > 1 and not name.startswith("__")}
    assert not twice, f"defined in more than one class of the hierarchy: {twice}"
