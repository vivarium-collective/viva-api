"""Every module that moved into ``viva_core`` is still importable under its old name --
and the old name is the SAME module object, not a copy.

That identity is the point. A re-exporting stub (``from new import *``) would give two
modules with two sets of attributes: ``mock.patch("old.path.X")`` would patch a name
nothing reads, and module-level state would fork. The stubs therefore replace themselves
in ``sys.modules`` with the real module, and keep a ``TYPE_CHECKING`` star-import so mypy
still resolves the old path. This test finds the stubs by that contract and checks each.
"""

import importlib
import re
from pathlib import Path
from unittest import mock

import pytest

import viva_api

API_ROOT = Path(viva_api.__file__).resolve().parent
_TARGET = re.compile(r"^from (viva_core[\w.]*) import (\w+) as _moved$", re.MULTILINE)


def _stubs() -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    for source in sorted(API_ROOT.rglob("*.py")):
        text = source.read_text(encoding="utf-8")
        if "sys.modules[__name__] = _moved" not in text:
            continue
        match = _TARGET.search(text)
        assert match, f"{source}: aliasing stub without a recognisable target import"
        old = "viva_api." + ".".join(source.relative_to(API_ROOT).with_suffix("").parts)
        pairs.append((old, f"{match.group(1)}.{match.group(2)}"))
    return pairs


def test_there_are_stubs_to_check() -> None:
    assert _stubs(), "no aliasing stubs found -- the discovery contract changed"


@pytest.mark.parametrize(("old_name", "new_name"), _stubs())
def test_old_name_is_the_new_module(old_name: str, new_name: str) -> None:
    old = importlib.import_module(old_name)
    new = importlib.import_module(new_name)
    assert old is new

    parent_name, _, child = old_name.rpartition(".")
    assert getattr(importlib.import_module(parent_name), child) is new


@pytest.mark.parametrize(("old_name", "new_name"), _stubs())
def test_patching_through_the_old_name_patches_the_real_module(old_name: str, new_name: str) -> None:
    new = importlib.import_module(new_name)
    public = [name for name in vars(new) if not name.startswith("_")]
    assert public, f"{new_name} exports nothing public"
    target = public[0]
    sentinel = object()
    with mock.patch(f"{old_name}.{target}", sentinel):
        assert getattr(new, target) is sentinel
