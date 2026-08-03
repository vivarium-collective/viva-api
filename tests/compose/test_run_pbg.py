import json
import sys
import types
from pathlib import Path
from typing import Any

import pytest

from sms_api.compose import run_pbg


class FakeCore:
    def __init__(self) -> None:
        self.links: dict[str, Any] = {}

    def register_link(self, key: str, link: Any) -> None:
        self.links[key] = link


def test_run_writes_final_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeComposite:
        def __init__(self, doc: Any, core: Any = None) -> None:
            self.doc = doc
            self.core = core
            self.n = 0

        def run(self, n: int) -> None:
            self.n = n

        def serialize_state(self) -> dict[str, int]:
            return {"ran": self.n}

    # run() does `from process_bigraph import Composite, register_types` and
    # `from bigraph_schema import allocate_core`. Inject fake modules so the
    # runner is testable without the (container-only) process-bigraph/pbg-emitters
    # install.
    fake_pbg_mod = types.ModuleType("process_bigraph")
    fake_pbg_mod.Composite = FakeComposite  # type: ignore[attr-defined]
    fake_pbg_mod.register_types = lambda core: core  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "process_bigraph", fake_pbg_mod)

    fake_schema_mod = types.ModuleType("bigraph_schema")
    fake_schema_mod.allocate_core = FakeCore  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "bigraph_schema", fake_schema_mod)

    pbg = tmp_path / "m.pbg"
    pbg.write_text(json.dumps({"state": {}, "composition": {}}))
    out = run_pbg.run(str(pbg), steps=5, results_dir=tmp_path / "output")

    assert out.name == "final_state.json"
    assert json.loads(out.read_text())["ran"] == 5


# --- _redirect_emitters: emitter output must land in the S3-synced results dir ---


def test_redirect_emitters_injects_out_dir_when_absent(tmp_path: Path) -> None:
    """v2ecoli's baseline OMITS out_dir on purpose so the emitter resolves it to
    <workspace>/.pbg/parquet-runs. In the container that dir is never synced to S3,
    so the run would succeed and produce nothing readable."""
    doc: dict[str, Any] = {"composition": {"emitter": {"address": "local:ParquetEmitter", "config": {}}}}
    n = run_pbg._redirect_emitters(doc, tmp_path / "out")
    assert n == 1
    assert doc["composition"]["emitter"]["config"]["out_dir"] == str(tmp_path / "out")


def test_redirect_emitters_overrides_an_authored_out_dir(tmp_path: Path) -> None:
    """An authored path came from the authoring environment and is meaningless here."""
    doc: dict[str, Any] = {"e": {"address": "local:ParquetEmitter", "config": {"out_dir": "/authored/elsewhere"}}}
    run_pbg._redirect_emitters(doc, tmp_path)
    assert doc["e"]["config"]["out_dir"] == str(tmp_path)


def test_redirect_emitters_uses_out_uri_when_that_is_the_emitters_key(tmp_path: Path) -> None:
    """XArrayEmitter speaks out_uri, not out_dir — don't add a key it ignores."""
    doc: dict[str, Any] = {"e": {"address": "local:XArrayEmitter", "config": {"out_uri": "s3://old/place"}}}
    run_pbg._redirect_emitters(doc, tmp_path)
    assert doc["e"]["config"]["out_uri"] == str(tmp_path)
    assert "out_dir" not in doc["e"]["config"]


def test_redirect_emitters_creates_a_missing_config_block(tmp_path: Path) -> None:
    doc: dict[str, Any] = {"e": {"address": "local:SQLiteEmitter"}}
    run_pbg._redirect_emitters(doc, tmp_path)
    assert doc["e"]["config"]["out_dir"] == str(tmp_path)


def test_redirect_emitters_finds_emitters_nested_in_lists(tmp_path: Path) -> None:
    doc: dict[str, Any] = {"emitters": [{"address": "local:ParquetEmitter", "config": {}}, {"address": "local:noop"}]}
    assert run_pbg._redirect_emitters(doc, tmp_path) == 1


def test_redirect_emitters_is_a_noop_without_emitters(tmp_path: Path) -> None:
    """A document need not declare one — 0 is a legitimate answer, not an error."""
    doc: dict[str, Any] = {"composition": {"proc": {"address": "local:SomeProcess", "config": {"out_dir": "keep"}}}}
    assert run_pbg._redirect_emitters(doc, tmp_path) == 0
    assert doc["composition"]["proc"]["config"]["out_dir"] == "keep"


# --- _workspace_core: the workspace registers types the generic core can't know ---


def test_workspace_core_returns_none_when_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PBG_CORE_BUILDER", raising=False)
    assert run_pbg._workspace_core() is None


@pytest.mark.parametrize("spec", ["no_colon_here", "v2ecoli.core:missing_fn", "no.such.module:f"])
def test_workspace_core_degrades_to_none_instead_of_raising(spec: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """A bad builder must not abort the run — the generic core is a valid fallback."""
    monkeypatch.setenv("PBG_CORE_BUILDER", spec)
    assert run_pbg._workspace_core() is None


def test_workspace_core_uses_the_named_builder(monkeypatch: pytest.MonkeyPatch) -> None:
    sentinel = FakeCore()
    mod = types.ModuleType("fake_ws")
    mod.build_core = lambda: sentinel  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "fake_ws", mod)
    monkeypatch.setenv("PBG_CORE_BUILDER", "fake_ws:build_core")
    assert run_pbg._workspace_core() is sentinel


def test_a_falsy_workspace_core_is_still_used(monkeypatch: pytest.MonkeyPatch) -> None:
    """Regression: selecting the core with `or` would silently discard a valid core
    that defines __bool__/__len__ (a registry with nothing registered yet), falling
    back to the generic one with no signal — the document would then fail to resolve
    on Batch for no visible reason."""

    class FalsyCore(FakeCore):
        def __len__(self) -> int:
            return 0

    sentinel = FalsyCore()
    assert not sentinel  # precondition: this core is falsy
    mod = types.ModuleType("fake_falsy_ws")
    mod.build_core = lambda: sentinel  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "fake_falsy_ws", mod)
    monkeypatch.setenv("PBG_CORE_BUILDER", "fake_falsy_ws:build_core")
    assert run_pbg._workspace_core() is sentinel
