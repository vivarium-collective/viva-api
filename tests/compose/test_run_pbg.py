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


def _install_fake_protocol_registration(monkeypatch: pytest.MonkeyPatch) -> None:
    """run() does `from process_bigraph.protocols import register_types as
    register_protocol_types` unconditionally (backlog item 88 -- registers
    'ray'/'rest'/'parallel'/'git' as resolvable protocol types; without it
    any document referencing e.g. a ray: address fails at Composite-build
    time with "value is not a protocol: ray", confirmed live). Every test
    that exercises the real run() needs this fake too, alongside the
    existing process_bigraph/bigraph_schema fakes."""
    fake_protocols_mod = types.ModuleType("process_bigraph.protocols")
    fake_protocols_mod.register_types = lambda core: core  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "process_bigraph.protocols", fake_protocols_mod)


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
    _install_fake_protocol_registration(monkeypatch)

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
    _install_fake_protocol_registration(monkeypatch)

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


def test_redirect_emitters_does_not_count_or_touch_a_ram_or_console_emitter(tmp_path: Path) -> None:
    """Backlog item 88 regression: RAMEmitter/ConsoleEmitter (process_bigraph
    built-ins) have NO out_dir/out_uri capability at all -- Emitter's base
    config_schema is just {'emit': 'schema'}, and neither subclass adds a
    location key (RAMEmitter keeps everything in self.history/self.table;
    ConsoleEmitter just print()s). Unlike ParquetEmitter/XArrayEmitter/
    SQLiteEmitter, stuffing out_dir into their config is a silent no-op, not a
    real redirect -- before this fix, the broad `"emitter" in address.lower()`
    match counted them anyway (confirmed: "emitter" in "local:ramemitter" is
    True), which silently defeated run()'s own
    `if n_redirected == 0: _persist_emitter_history(...)` fallback for exactly
    the plain-in-memory-emitter case that fallback exists to catch. A colony
    composite's own `emitter_from_wires({...})` resolves to
    address='local:RAMEmitter' by default (process_bigraph.emitter's own
    default), so this is not a hypothetical case."""
    doc: dict[str, Any] = {
        "ram": {"address": "local:RAMEmitter", "config": {}},
        "console": {"address": "local:ConsoleEmitter"},
    }
    n = run_pbg._redirect_emitters(doc, tmp_path / "out")
    assert n == 0
    assert doc["ram"]["config"] == {}  # no meaningless out_dir key added
    assert "config" not in doc["console"]  # no config block fabricated for it either


def test_redirect_emitters_still_counts_a_real_file_backed_emitter_alongside_a_ram_emitter(
    tmp_path: Path,
) -> None:
    """The exclusion is precise, not a blanket "skip anything with emitter in the
    name": a document mixing a RAMEmitter (e.g. a debug/console sink) with a real
    ParquetEmitter must still redirect the Parquet one and count exactly 1."""
    doc: dict[str, Any] = {
        "ram": {"address": "local:RAMEmitter", "config": {}},
        "parquet": {"address": "local:ParquetEmitter", "config": {}},
    }
    n = run_pbg._redirect_emitters(doc, tmp_path / "out")
    assert n == 1
    assert doc["ram"]["config"] == {}
    assert doc["parquet"]["config"]["out_dir"] == str(tmp_path / "out")


# --- _persist_emitter_history: the in-memory-emitter fallback (backlog item 88) ---


def test_persist_emitter_history_writes_json_from_gathered_results(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake_pbg_mod = types.ModuleType("process_bigraph")
    fake_pbg_mod.gather_emitter_results = lambda composite: {  # type: ignore[attr-defined]
        ("emitter",): [(0.0, {"a": 1}), (1.0, {"a": 2})],
        ("nested", "path"): [(0.0, {"b": 1})],
    }
    monkeypatch.setitem(sys.modules, "process_bigraph", fake_pbg_mod)

    out = run_pbg._persist_emitter_history(composite=object(), results_dir=tmp_path)

    assert out == tmp_path / "emitter_history.json"
    assert json.loads(out.read_text()) == {
        "emitter": [[0.0, {"a": 1}], [1.0, {"a": 2}]],
        "nested.path": [[0.0, {"b": 1}]],
    }


def test_persist_emitter_history_returns_none_and_writes_nothing_when_no_results(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An empty gather result (e.g. a document with no emitter at all) is a
    legitimate, honest absence -- not an error."""
    fake_pbg_mod = types.ModuleType("process_bigraph")
    fake_pbg_mod.gather_emitter_results = lambda composite: {}  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "process_bigraph", fake_pbg_mod)

    out = run_pbg._persist_emitter_history(composite=object(), results_dir=tmp_path)

    assert out is None
    assert not (tmp_path / "emitter_history.json").exists()


def test_persist_emitter_history_degrades_when_gather_emitter_results_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Best-effort, matching the function's own documented contract: a composite
    this generic runner knows nothing about may not support
    gather_emitter_results the way v2ecoli's colony composite does -- must never
    raise and abort an otherwise-successful dispatch."""

    def _raise(composite: Any) -> Any:
        raise RuntimeError("boom")

    fake_pbg_mod = types.ModuleType("process_bigraph")
    fake_pbg_mod.gather_emitter_results = _raise  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "process_bigraph", fake_pbg_mod)

    out = run_pbg._persist_emitter_history(composite=object(), results_dir=tmp_path)

    assert out is None
    assert not (tmp_path / "emitter_history.json").exists()


def test_persist_emitter_history_degrades_without_process_bigraph_gather_helper(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An older process_bigraph install without gather_emitter_results at all
    must degrade the same way as any other unsupported-composite case."""
    fake_pbg_mod = types.ModuleType("process_bigraph")  # no gather_emitter_results attribute
    monkeypatch.setitem(sys.modules, "process_bigraph", fake_pbg_mod)

    out = run_pbg._persist_emitter_history(composite=object(), results_dir=tmp_path)

    assert out is None


def _install_fake_pbg_for_run(monkeypatch: pytest.MonkeyPatch, gather_emitter_results: Any) -> list[Any]:
    """Install the full fake process_bigraph/bigraph_schema stack test_run_* needs
    to exercise the real run() function end-to-end, plus a spy on
    gather_emitter_results calls. Returns the list gather calls are recorded into."""

    class FakeComposite:
        def __init__(self, doc: Any, core: Any = None) -> None:
            self.doc = doc

        def run(self, n: int) -> None:
            pass

        def serialize_state(self) -> dict[str, bool]:
            return {"ran": True}

    calls: list[Any] = []

    def _spy_gather(composite: Any) -> Any:
        calls.append(composite)
        return gather_emitter_results(composite)

    fake_pbg_mod = types.ModuleType("process_bigraph")
    fake_pbg_mod.Composite = FakeComposite  # type: ignore[attr-defined]
    fake_pbg_mod.register_types = lambda core: core  # type: ignore[attr-defined]
    fake_pbg_mod.gather_emitter_results = _spy_gather  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "process_bigraph", fake_pbg_mod)
    fake_schema_mod = types.ModuleType("bigraph_schema")
    fake_schema_mod.allocate_core = FakeCore  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "bigraph_schema", fake_schema_mod)
    _install_fake_protocol_registration(monkeypatch)
    return calls


def test_run_persists_emitter_history_end_to_end_for_a_plain_ram_emitter_document(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The real regression this fix closes, exercised through the actual run()
    wiring (not just the two halves in isolation): a document shaped exactly
    like a colony composite's own (emitter_from_wires({...}) -> RAMEmitter, no
    out_dir/out_uri anywhere) must come out of a real run() call with
    emitter_history.json written under results_dir."""
    calls = _install_fake_pbg_for_run(
        monkeypatch, gather_emitter_results=lambda composite: {("emitter",): [(0.0, {"x": 1})]}
    )

    pbg = tmp_path / "m.pbg"
    pbg.write_text(json.dumps({"composition": {"emitter": {"address": "local:RAMEmitter", "config": {}}}}))
    run_pbg.run(str(pbg), steps=1, results_dir=tmp_path / "output")

    assert len(calls) == 1
    history_path = tmp_path / "output" / "emitter_history.json"
    assert history_path.exists()
    assert json.loads(history_path.read_text()) == {"emitter": [[0.0, {"x": 1}]]}


def test_run_skips_persisting_emitter_history_when_a_file_backed_emitter_already_shipped_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A document with a real file-backed emitter (ParquetEmitter) already ships
    its own output via _redirect_emitters -- _persist_emitter_history must not
    even be attempted for it (would be redundant, and gather_emitter_results is
    not guaranteed safe/meaningful once a file-backed emitter owns the data)."""
    calls = _install_fake_pbg_for_run(
        monkeypatch, gather_emitter_results=lambda composite: {("emitter",): [(0.0, {"x": 1})]}
    )

    pbg = tmp_path / "m.pbg"
    pbg.write_text(json.dumps({"composition": {"emitter": {"address": "local:ParquetEmitter", "config": {}}}}))
    run_pbg.run(str(pbg), steps=1, results_dir=tmp_path / "output")

    assert calls == []
    assert not (tmp_path / "output" / "emitter_history.json").exists()


# --- _assert_emitted_output: P0-3, a zero-output run must not report success ---


def test_has_emitted_output_true_for_nonempty_parquet(tmp_path: Path) -> None:
    (tmp_path / "history").mkdir()
    (tmp_path / "history" / "part.pq").write_bytes(b"PAR1-not-really-but-nonempty")
    assert run_pbg._has_emitted_output(tmp_path) is True


def test_has_emitted_output_true_for_nonempty_emitter_history(tmp_path: Path) -> None:
    (tmp_path / "emitter_history.json").write_text(json.dumps({"emitter": [[0.0, {"x": 1}]]}))
    assert run_pbg._has_emitted_output(tmp_path) is True


def test_has_emitted_output_false_for_empty_dir_or_only_final_state(tmp_path: Path) -> None:
    assert run_pbg._has_emitted_output(tmp_path) is False
    (tmp_path / "final_state.json").write_text("{}")  # the always-present fallback does NOT count
    (tmp_path / "emitter_history.json").write_text("{}")  # empty history does NOT count
    (tmp_path / "empty.pq").write_bytes(b"")  # zero-byte parquet does NOT count
    assert run_pbg._has_emitted_output(tmp_path) is False


def test_run_raises_when_require_output_set_and_nothing_emitted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The core P0-3 guard: a run that produced only final_state.json must exit
    non-zero under PBG_REQUIRE_OUTPUT instead of reporting success."""
    monkeypatch.setenv("PBG_REQUIRE_OUTPUT", "1")
    _install_fake_pbg_for_run(monkeypatch, gather_emitter_results=lambda composite: {})

    pbg = tmp_path / "m.pbg"
    pbg.write_text(json.dumps({"state": {}, "composition": {}}))
    with pytest.raises(SystemExit):
        run_pbg.run(str(pbg), steps=1, results_dir=tmp_path / "output")


def test_run_succeeds_when_require_output_set_and_history_written(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A real in-memory-emitter run (history persisted) satisfies the guard."""
    monkeypatch.setenv("PBG_REQUIRE_OUTPUT", "1")
    _install_fake_pbg_for_run(monkeypatch, gather_emitter_results=lambda composite: {("emitter",): [(0.0, {"x": 1})]})

    pbg = tmp_path / "m.pbg"
    pbg.write_text(json.dumps({"composition": {"emitter": {"address": "local:RAMEmitter", "config": {}}}}))
    out = run_pbg.run(str(pbg), steps=1, results_dir=tmp_path / "output")
    assert out.name == "final_state.json"
    assert (tmp_path / "output" / "emitter_history.json").exists()


def test_run_does_not_guard_output_when_require_output_unset(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Default (unset): the generic runner is unchanged — a bare document whose
    only artifact is final_state.json still succeeds."""
    monkeypatch.delenv("PBG_REQUIRE_OUTPUT", raising=False)
    _install_fake_pbg_for_run(monkeypatch, gather_emitter_results=lambda composite: {})

    pbg = tmp_path / "m.pbg"
    pbg.write_text(json.dumps({"state": {}, "composition": {}}))
    out = run_pbg.run(str(pbg), steps=1, results_dir=tmp_path / "output")  # must not raise
    assert out.name == "final_state.json"


# --- _assert_run_advanced: P0-3 effect check, a one-tick collapse must not report success ---


def _write_final_state(results_dir: Path, state: Any) -> None:
    results_dir.mkdir(parents=True, exist_ok=True)
    (results_dir / "final_state.json").write_text(json.dumps(state))


def test_final_global_time_reads_top_level(tmp_path: Path) -> None:
    _write_final_state(tmp_path, {"global_time": 2528.0, "ran": True})
    assert run_pbg._final_global_time(tmp_path) == 2528.0
    _write_final_state(tmp_path, {"global_time": 5})  # int is fine
    assert run_pbg._final_global_time(tmp_path) == 5.0


def test_final_global_time_none_when_absent_or_not_a_number(tmp_path: Path) -> None:
    assert run_pbg._final_global_time(tmp_path) is None  # no file
    _write_final_state(tmp_path, {"ran": True})  # no key
    assert run_pbg._final_global_time(tmp_path) is None
    _write_final_state(tmp_path, {"global_time": "1.0"})  # string, not a number
    assert run_pbg._final_global_time(tmp_path) is None
    _write_final_state(tmp_path, {"global_time": True})  # bool must not read as 1.0
    assert run_pbg._final_global_time(tmp_path) is None


def test_assert_run_advanced_noop_when_min_unset(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Default (unset): no check even on a one-tick final state."""
    monkeypatch.delenv("PBG_MIN_GLOBAL_TIME", raising=False)
    _write_final_state(tmp_path, {"global_time": 1.0})
    run_pbg._assert_run_advanced(tmp_path)  # must not raise


def test_assert_run_advanced_raises_on_one_tick_collapse(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The effect check: a non-empty store from a one-tick run (global_time ~= 1.0)
    must fail when a real generation was expected (#210 §3d / #375 §3e)."""
    monkeypatch.setenv("PBG_MIN_GLOBAL_TIME", "100")
    _write_final_state(tmp_path, {"global_time": 1.0})
    with pytest.raises(SystemExit):
        run_pbg._assert_run_advanced(tmp_path)


def test_assert_run_advanced_passes_a_real_generation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PBG_MIN_GLOBAL_TIME", "100")
    _write_final_state(tmp_path, {"global_time": 2528.0})
    run_pbg._assert_run_advanced(tmp_path)  # must not raise


def test_assert_run_advanced_raises_when_global_time_unverifiable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Caller demanded the check but the run left no readable global_time -> refuse
    to report success rather than pass blindly."""
    monkeypatch.setenv("PBG_MIN_GLOBAL_TIME", "100")
    _write_final_state(tmp_path, {"ran": True})  # no global_time
    with pytest.raises(SystemExit):
        run_pbg._assert_run_advanced(tmp_path)


def test_assert_run_advanced_raises_on_non_numeric_min(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PBG_MIN_GLOBAL_TIME", "not-a-number")
    _write_final_state(tmp_path, {"global_time": 2528.0})
    with pytest.raises(SystemExit):
        run_pbg._assert_run_advanced(tmp_path)


def test_run_raises_when_min_global_time_set_but_run_did_not_advance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """End-to-end: run() calls the effect check. The fake composite serializes no
    global_time, so with PBG_MIN_GLOBAL_TIME set the run must exit non-zero even
    though emitted history satisfies PBG_REQUIRE_OUTPUT (presence != effect)."""
    monkeypatch.setenv("PBG_REQUIRE_OUTPUT", "1")
    monkeypatch.setenv("PBG_MIN_GLOBAL_TIME", "100")
    _install_fake_pbg_for_run(monkeypatch, gather_emitter_results=lambda composite: {("emitter",): [(0.0, {"x": 1})]})

    pbg = tmp_path / "m.pbg"
    pbg.write_text(json.dumps({"composition": {"emitter": {"address": "local:RAMEmitter", "config": {}}}}))
    with pytest.raises(SystemExit):
        run_pbg.run(str(pbg), steps=1, results_dir=tmp_path / "output")


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
    def __init__(self, doc: dict[str, Any], core_extensions: list[Any] | None = None) -> None:
        self._doc = doc
        self.overrides_received: dict[str, Any] | None = None
        self.core_received: Any = None
        # Mirrors the real CompositeSpec's own field (process_bigraph/composite_spec.py,
        # default_factory=list) -- to_document() itself never applies these (only the
        # higher-level to_composite() does); _resolve_document applies them itself
        # (backlog item 88), same one line to_composite() uses.
        self.core_extensions = core_extensions or []

    def to_document(self, overrides: dict[str, Any] | None = None, core: Any = None) -> dict[str, Any]:
        self.overrides_received = overrides
        self.core_received = core
        return self._doc


def _apply_core_extensions(entry: Any, core: Any) -> Any:
    """Real process_bigraph.composite_generator.apply_core_extensions's own
    logic, reimplemented here only because process_bigraph isn't actually
    installed in this repo's venv (container-only) -- same semantics: an
    extension may mutate `core` in place OR return a new one to use instead."""
    for ext in entry.core_extensions or []:
        result = ext(core)
        if result is not None:
            core = result
    return core


def _install_fake_composite_spec(monkeypatch: pytest.MonkeyPatch, registry: dict[str, Any]) -> list[str]:
    """Inject fake process_bigraph.composite_spec + composite_generator modules
    (both required by _resolve_document's composite-id branch); returns
    discover_specs() call count."""
    discover_calls: list[str] = []
    fake_spec_mod = types.ModuleType("process_bigraph.composite_spec")
    fake_spec_mod.get = lambda spec_id: registry.get(spec_id)  # type: ignore[attr-defined]
    fake_spec_mod.discover_specs = lambda: discover_calls.append("called")  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "process_bigraph.composite_spec", fake_spec_mod)
    fake_generator_mod = types.ModuleType("process_bigraph.composite_generator")
    fake_generator_mod.apply_core_extensions = _apply_core_extensions  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "process_bigraph.composite_generator", fake_generator_mod)
    return discover_calls


def test_resolve_document_static_file_mode_unchanged(tmp_path: Path) -> None:
    """No composite_id: behaves exactly as before — reads the JSON file verbatim."""
    pbg = tmp_path / "m.pbg"
    pbg.write_text(json.dumps({"state": {"x": 1}}))
    original_core = FakeCore()
    doc, core = run_pbg._resolve_document(str(pbg), None, None, core=original_core)
    assert doc == {"state": {"x": 1}}
    assert core is original_core


def test_resolve_document_composite_id_mode_builds_via_registered_spec(monkeypatch: pytest.MonkeyPatch) -> None:
    spec = FakeSpec({"state": {"batch_runner": {}}})
    _install_fake_composite_spec(monkeypatch, {"v2ecoli.composites.ecoli_baseline": spec})
    original_core = FakeCore()
    overrides = {"n_seeds": 1000, "n_generations": 10}

    doc, core = run_pbg._resolve_document(None, "v2ecoli.composites.ecoli_baseline", overrides, core=original_core)

    assert doc == {"state": {"batch_runner": {}}}
    assert spec.overrides_received == overrides
    assert spec.core_received is original_core
    # No core_extensions declared -> the SAME core object passes through unchanged.
    assert core is original_core


def test_resolve_document_applies_the_spec_own_core_extensions_mutating_in_place(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """backlog item 88: a composite that declares core_extensions (e.g.
    ecoli_colony's pymunk_agent type + EcoliWCM/ColonyGrowthGif link
    registration via _register_colony_core) must have them applied before
    to_document() builds against `core` -- CompositeSpec.to_document() itself
    never does this (only to_composite() does), so _resolve_document must,
    via the same real apply_core_extensions() helper to_composite() uses.
    This covers the "mutates core in place, returns None" convention."""
    applied: list[Any] = []

    def _register_custom_types(core: Any) -> None:
        applied.append(core)
        core.register_link("SomeCustomLink", object())

    original_core = FakeCore()
    spec = FakeSpec({"state": {}}, core_extensions=[_register_custom_types])
    _install_fake_composite_spec(monkeypatch, {"some.workspace.multi_node_composite": spec})

    _doc, core = run_pbg._resolve_document(None, "some.workspace.multi_node_composite", {}, core=original_core)

    assert applied == [original_core]
    assert "SomeCustomLink" in original_core.links
    assert core is original_core


def test_resolve_document_applies_the_spec_own_core_extensions_returning_a_new_core(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The exact real bug this fix closes: an extension that does NOT mutate
    its input but returns a brand-new core instead (ecoli_colony's real
    _register_colony_core does exactly this -- confirmed live against a real
    2-node AWS Batch dispatch, which failed with "unable to parse type
    map[pymunk_agent]" when a first-attempt fix ran the extension but
    discarded its return value). _resolve_document must return the NEW core,
    not the original one, so the caller builds Composite() against the one
    that actually has the registration."""
    new_core = FakeCore()
    new_core.register_link("SomeCustomLink", object())

    def _returns_a_new_core(core: Any) -> Any:
        return new_core

    original_core = FakeCore()
    spec = FakeSpec({"state": {}}, core_extensions=[_returns_a_new_core])
    _install_fake_composite_spec(monkeypatch, {"some.workspace.multi_node_composite": spec})

    _doc, core = run_pbg._resolve_document(None, "some.workspace.multi_node_composite", {}, core=original_core)

    assert core is new_core
    assert core is not original_core
    assert "SomeCustomLink" not in original_core.links


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

    doc, _core = run_pbg._resolve_document(None, "late.module.ecoli_baseline", {}, core=FakeCore())
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
    _install_fake_protocol_registration(monkeypatch)

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


def test_run_registers_protocols_on_the_core_that_survives_core_extensions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The exact real bug this fix closes (backlog item 88, found on the
    FOURTH live pilot attempt): protocol registration ('ray' -> RayProtocol
    etc.) must be applied to whichever core _resolve_document actually
    returns, not the one run() started with. A composite whose
    core_extensions REPLACES the core (ecoli_colony's real
    _register_colony_core does this) would otherwise silently register
    protocols onto a core that gets thrown away one line later -- a first
    attempt did exactly this (registered protocols before calling
    _resolve_document) and failed identically on real AWS with "value is not
    a protocol: ray", even though the core_extensions fix itself was already
    correct."""

    class FakeComposite:
        def __init__(self, doc: Any, core: Any = None) -> None:
            self.doc = doc
            self.core_seen = core

        def run(self, n: int) -> None:
            pass

        def serialize_state(self) -> dict[str, Any]:
            return {"protocols_registered_on_final_core": getattr(self.core_seen, "protocols_registered", False)}

    fake_pbg_mod = types.ModuleType("process_bigraph")
    fake_pbg_mod.Composite = FakeComposite  # type: ignore[attr-defined]
    fake_pbg_mod.register_types = lambda core: core  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "process_bigraph", fake_pbg_mod)
    fake_schema_mod = types.ModuleType("bigraph_schema")
    fake_schema_mod.allocate_core = FakeCore  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "bigraph_schema", fake_schema_mod)

    def _mark_registered(core: Any) -> Any:
        core.protocols_registered = True
        return core

    fake_protocols_mod = types.ModuleType("process_bigraph.protocols")
    fake_protocols_mod.register_types = _mark_registered  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "process_bigraph.protocols", fake_protocols_mod)

    new_core = FakeCore()

    def _replaces_the_core(core: Any) -> Any:
        return new_core

    spec = FakeSpec({"state": {}}, core_extensions=[_replaces_the_core])
    _install_fake_composite_spec(monkeypatch, {"some.workspace.multi_node_composite": spec})

    out = run_pbg.run(
        None,
        steps=1,
        results_dir=tmp_path / "output",
        composite_id="some.workspace.multi_node_composite",
        overrides={},
    )

    assert json.loads(out.read_text())["protocols_registered_on_final_core"] is True
    assert getattr(new_core, "protocols_registered", False) is True
