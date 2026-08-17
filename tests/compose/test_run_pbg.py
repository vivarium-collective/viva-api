import json
import sys
import types
from pathlib import Path
from typing import Any

import pytest

from viva_api.compose import run_pbg


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


# --- _flush_emitters: ParquetEmitter's trailing batch must land on disk before exit ---


def test_run_flushes_parquet_emitters_after_composite_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Regression test for item 61: a ParquetEmitter is built deep inside a
    composite's step factory, so run() never sees the instance and can't call
    close() on it directly. Without an explicit flush_all_in_composite() call,
    the trailing partial batch stays in memory and is silently lost — the run
    "succeeds" and produces zero readable output. Fails pre-fix (flush_calls
    stays empty); passes post-fix."""

    class FakeComposite:
        def __init__(self, doc: Any, core: Any = None) -> None:
            self.doc = doc

        def run(self, n: int) -> None:
            pass

        def serialize_state(self) -> dict[str, int]:
            return {"ran": 1}

    fake_pbg_mod = types.ModuleType("process_bigraph")
    fake_pbg_mod.Composite = FakeComposite  # type: ignore[attr-defined]
    fake_pbg_mod.register_types = lambda core: core  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "process_bigraph", fake_pbg_mod)
    fake_schema_mod = types.ModuleType("bigraph_schema")
    fake_schema_mod.allocate_core = FakeCore  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "bigraph_schema", fake_schema_mod)

    flush_calls: list[tuple[Any, bool]] = []

    class FakeParquetEmitter:
        @staticmethod
        def flush_all_in_composite(composite: Any, success: bool = True) -> int:
            flush_calls.append((composite, success))
            return 1

    fake_emitters_mod = types.ModuleType("viva_emitters")
    fake_emitters_mod.ParquetEmitter = FakeParquetEmitter  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "viva_emitters", fake_emitters_mod)

    pbg = tmp_path / "m.pbg"
    pbg.write_text(json.dumps({"state": {}, "composition": {}}))
    run_pbg.run(str(pbg), steps=5, results_dir=tmp_path / "output")

    assert len(flush_calls) == 1
    composite_seen, success = flush_calls[0]
    assert isinstance(composite_seen, FakeComposite)
    assert success is True


def test_flush_emitters_is_a_noop_without_parquet_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    """[parquet] need not be installed in every compose image — degrade silently,
    matching _build_core()'s own per-emitter ImportError guard."""
    monkeypatch.setitem(sys.modules, "viva_emitters", None)  # forces `import viva_emitters` to raise ImportError
    run_pbg._flush_emitters(composite=object())  # must not raise


# --- _v2ecoli_parquet_emitter_override: item 61 follow-up ---
#
# The flush fix above (test_run_flushes_parquet_emitters_after_composite_run)
# mocks viva_emitters.ParquetEmitter.flush_all_in_composite entirely, so it
# proved run()'s driver *calls* the flush at the right point — it could not
# and did not prove the flush finds real, correctly-located data to flush.
# A real AWS re-dispatch against that merged fix (PR #251) showed zero
# parquet output, unchanged from before the fix. Root cause (full evidence
# chain in vivarium-workbench/.todo/backlog/61.md): v2ecoli composites built
# via @composite_generator(emitters=[...]) (ecoli_baseline/batch_baseline)
# eagerly construct their default ParquetEmitter *inside* to_document() —
# v2ecoli.composites._helpers._build_declared_emitter resolves out_dir from
# a workspace-relative default (_find_workspace_root()) at that point,
# before this runner's own _redirect_emitters() document-mutation ever runs
# — so mutating the document afterward has nothing left to act on.
#
# These tests verify the real, load-bearing CONTRACT instead: the exact
# keyword arguments this runner passes to v2ecoli's own parquet_vecoli()
# preset builder, and that set_parquet_emitter_override() receives that
# preset's real return value verbatim (not a hand-built dict — confirmed by
# reading _helpers.py directly: the override is spread straight into
# ParquetEmitter's config, `cfg = {'emit': ..., **parquet_override}`, never
# run through parquet_vecoli() itself unless the CALLER does that). v2ecoli
# is not a viva-api dependency, so parquet_vecoli/set_parquet_emitter_override
# are still real fakes here, not the genuine installed functions — full,
# non-mocked confirmation was done separately: a real local composite run
# (real process_bigraph, real bigraph_schema, real v2ecoli, real ParCa
# cache, real viva_emitters.ParquetEmitter) produced zero output without
# this fix and real hive-partitioned history/*.pq (with the real
# experiment_id/lineage_seed/generation partition columns intact) with it —
# see 61.md's evidence chain. Treat that local run, and the real AWS
# verification dispatch this fix still needs, as the actual proof; these
# unit tests only guard the argument-passing contract from silently drifting.


def test_v2ecoli_emitter_override_calls_parquet_vecoli_with_real_run_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[dict[str, Any]] = []

    def fake_parquet_vecoli(**kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        return {"the": "real preset", **kwargs}

    set_calls: list[Any] = []
    fake_presets_mod = types.ModuleType("v2ecoli.library.emitter_presets")
    fake_presets_mod.parquet_vecoli = fake_parquet_vecoli  # type: ignore[attr-defined]
    fake_helpers_mod = types.ModuleType("v2ecoli.composites._helpers")
    fake_helpers_mod.set_parquet_emitter_override = lambda cfg: set_calls.append(cfg)  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "v2ecoli.library.emitter_presets", fake_presets_mod)
    monkeypatch.setitem(sys.modules, "v2ecoli.composites._helpers", fake_helpers_mod)

    with run_pbg._v2ecoli_parquet_emitter_override(
        tmp_path / "out",
        {"experiment_id": "sim69-real-9c6d", "seed": 3, "initial_generation_index": 1},
    ):
        pass

    assert len(calls) == 1
    assert calls[0] == {
        "out_dir": str(tmp_path / "out"),
        "experiment_id": "sim69-real-9c6d",
        "lineage_seed": 3,
        "generation": 1,
    }
    # set_parquet_emitter_override must receive parquet_vecoli's own return
    # value verbatim (not a dict reconstructed by this runner) on entry, and
    # be cleared to None on exit.
    assert set_calls == [{"the": "real preset", **calls[0]}, None]


def test_v2ecoli_emitter_override_defaults_when_overrides_missing_seed_or_generation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A static-document run (no composite-id, no per-job overrides) must not
    crash resolving seed/generation — 0/None are v2ecoli's own defaults."""
    calls: list[dict[str, Any]] = []
    fake_presets_mod = types.ModuleType("v2ecoli.library.emitter_presets")
    fake_presets_mod.parquet_vecoli = lambda **kw: calls.append(kw) or kw  # type: ignore[attr-defined,func-returns-value]
    fake_helpers_mod = types.ModuleType("v2ecoli.composites._helpers")
    fake_helpers_mod.set_parquet_emitter_override = lambda cfg: None  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "v2ecoli.library.emitter_presets", fake_presets_mod)
    monkeypatch.setitem(sys.modules, "v2ecoli.composites._helpers", fake_helpers_mod)

    with run_pbg._v2ecoli_parquet_emitter_override(tmp_path / "out", None):
        pass

    assert calls == [
        {
            "out_dir": str(tmp_path / "out"),
            "experiment_id": "default",
            "lineage_seed": 0,
            "generation": None,
        }
    ]


def test_v2ecoli_emitter_override_clears_even_when_the_body_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    set_calls: list[Any] = []
    fake_presets_mod = types.ModuleType("v2ecoli.library.emitter_presets")
    fake_presets_mod.parquet_vecoli = lambda **kw: kw  # type: ignore[attr-defined]
    fake_helpers_mod = types.ModuleType("v2ecoli.composites._helpers")
    fake_helpers_mod.set_parquet_emitter_override = lambda cfg: set_calls.append(cfg)  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "v2ecoli.library.emitter_presets", fake_presets_mod)
    monkeypatch.setitem(sys.modules, "v2ecoli.composites._helpers", fake_helpers_mod)

    with pytest.raises(ValueError, match="boom"), run_pbg._v2ecoli_parquet_emitter_override(tmp_path / "out", {}):
        raise ValueError("boom")

    assert set_calls[-1] is None  # cleared in the finally, not left dangling


def test_v2ecoli_emitter_override_is_a_noop_without_v2ecoli_installed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Most compose images have no v2ecoli at all (it's one workspace among
    many this generic runner serves) — degrade silently, matching every
    other optional-dependency guard in this module."""
    monkeypatch.setitem(sys.modules, "v2ecoli.library.emitter_presets", None)
    with run_pbg._v2ecoli_parquet_emitter_override(tmp_path / "out", {}):
        pass  # must not raise


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


# --- _resolve_document / composite-id mode (backlog items 26/27) -----------------
# A model-specific dispatcher (e.g. viva-api's vEcoli ensemble endpoint) can submit
# a config-driven run through this SAME generic runner instead of a bespoke CLI
# script, by naming a registered composite id + overrides. Verified against a fake
# process_bigraph.composite_spec module so this stays testable without the real
# (container-only) process-bigraph install, matching test_run_writes_final_state's
# module-injection style above.


class FakeSpec:
    def __init__(self, doc: dict[str, Any]) -> None:
        self._doc = doc
        self.overrides_received: dict[str, Any] | None = None
        self.core_received: Any = None

    def to_document(self, overrides: dict[str, Any] | None = None, core: Any = None) -> dict[str, Any]:
        self.overrides_received = overrides
        self.core_received = core
        return self._doc


def _install_fake_composite_spec(monkeypatch: pytest.MonkeyPatch, registry: dict[str, Any]) -> list[str]:
    """Inject a fake process_bigraph.composite_spec module; returns discover_specs() call count."""
    discover_calls: list[str] = []
    fake_mod = types.ModuleType("process_bigraph.composite_spec")
    fake_mod.get = lambda spec_id: registry.get(spec_id)  # type: ignore[attr-defined]
    fake_mod.discover_specs = lambda: discover_calls.append("called")  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "process_bigraph.composite_spec", fake_mod)
    return discover_calls


def test_resolve_document_static_file_mode_unchanged(tmp_path: Path) -> None:
    """No composite_id: behaves exactly as before — reads the JSON file verbatim."""
    pbg = tmp_path / "m.pbg"
    pbg.write_text(json.dumps({"state": {"x": 1}}))
    doc = run_pbg._resolve_document(str(pbg), None, None, core=FakeCore())
    assert doc == {"state": {"x": 1}}


def test_resolve_document_composite_id_mode_builds_via_registered_spec(monkeypatch: pytest.MonkeyPatch) -> None:
    spec = FakeSpec({"state": {"batch_runner": {}}})
    _install_fake_composite_spec(monkeypatch, {"v2ecoli.composites.ecoli_baseline": spec})
    core = FakeCore()
    overrides = {"n_seeds": 1000, "n_generations": 10}

    doc = run_pbg._resolve_document(None, "v2ecoli.composites.ecoli_baseline", overrides, core=core)

    assert doc == {"state": {"batch_runner": {}}}
    assert spec.overrides_received == overrides
    assert spec.core_received is core


def test_resolve_document_composite_id_retries_after_discover_specs(monkeypatch: pytest.MonkeyPatch) -> None:
    """The composite's defining module may not be imported yet — its decorator only
    fires on import. discover_specs() forces that; a second lookup then succeeds."""
    spec = FakeSpec({"state": {}})
    registry: dict[str, Any] = {}
    discover_calls = _install_fake_composite_spec(monkeypatch, registry)

    def _discover_and_populate() -> None:
        discover_calls.append("called")
        registry["late.module.ecoli_baseline"] = spec

    sys.modules["process_bigraph.composite_spec"].discover_specs = _discover_and_populate  # type: ignore[attr-defined]

    doc = run_pbg._resolve_document(None, "late.module.ecoli_baseline", {}, core=FakeCore())
    assert doc == {"state": {}}
    assert discover_calls == ["called"]


def test_resolve_document_composite_id_unresolvable_raises_clearly(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_composite_spec(monkeypatch, {})
    with pytest.raises(SystemExit, match="no composite registered as 'missing.id'"):
        run_pbg._resolve_document(None, "missing.id", None, core=FakeCore())


def test_main_requires_exactly_one_of_input_file_or_composite_id(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    captured: dict[str, Any] = {}
    monkeypatch.setattr(run_pbg, "run", lambda *a, **kw: captured.update(kw) or Path("x"))

    with pytest.raises(SystemExit):
        run_pbg.main([])  # neither given

    pbg = tmp_path / "m.pbg"
    pbg.write_text("{}")
    with pytest.raises(SystemExit):
        run_pbg.main([str(pbg), "--composite-id", "x.y"])  # both given


def test_main_composite_id_mode_parses_overrides_json_and_calls_run(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    monkeypatch.setattr(run_pbg, "run", lambda *a, **kw: captured.update(args=a, kwargs=kw) or Path("out"))

    run_pbg.main(["--composite-id", "v2ecoli.composites.ecoli_baseline", "--overrides", '{"n_seeds": 2}', "-n", "1"])

    assert captured["kwargs"]["composite_id"] == "v2ecoli.composites.ecoli_baseline"
    assert captured["kwargs"]["overrides"] == {"n_seeds": 2}


def test_run_composite_id_mode_end_to_end(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Full run() path: composite-id resolves to a document, Composite runs it, and
    final_state.json still lands — the composite-id branch is a drop-in alternative
    to the file branch, not a separate code path downstream of document acquisition."""

    class FakeComposite:
        def __init__(self, doc: Any, core: Any = None) -> None:
            self.doc = doc

        def run(self, n: int) -> None:
            pass

        def serialize_state(self) -> dict[str, Any]:
            return {"doc_seen": self.doc}

    fake_pbg_mod = types.ModuleType("process_bigraph")
    fake_pbg_mod.Composite = FakeComposite  # type: ignore[attr-defined]
    fake_pbg_mod.register_types = lambda core: core  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "process_bigraph", fake_pbg_mod)
    fake_schema_mod = types.ModuleType("bigraph_schema")
    fake_schema_mod.allocate_core = FakeCore  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "bigraph_schema", fake_schema_mod)

    spec = FakeSpec({"state": {"batch_runner": {}}})
    _install_fake_composite_spec(monkeypatch, {"v2ecoli.composites.ecoli_baseline": spec})

    out = run_pbg.run(
        None,
        steps=1,
        results_dir=tmp_path / "output",
        composite_id="v2ecoli.composites.ecoli_baseline",
        overrides={"n_seeds": 2, "n_generations": 3},
    )

    assert json.loads(out.read_text())["doc_seen"] == {"state": {"batch_runner": {}}}
    assert spec.overrides_received == {"n_seeds": 2, "n_generations": 3}
