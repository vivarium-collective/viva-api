import json
import shutil
import subprocess
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
    n, s3_locations = run_pbg._redirect_emitters(doc, tmp_path / "out")
    assert n == 1
    assert s3_locations == []  # nothing pre-existing to redirect FROM
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
    assert run_pbg._redirect_emitters(doc, tmp_path)[0] == 1


def test_redirect_emitters_is_a_noop_without_emitters(tmp_path: Path) -> None:
    """A document need not declare one — 0 is a legitimate answer, not an error."""
    doc: dict[str, Any] = {"composition": {"proc": {"address": "local:SomeProcess", "config": {"out_dir": "keep"}}}}
    assert run_pbg._redirect_emitters(doc, tmp_path) == (0, [])
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
    n, _ = run_pbg._redirect_emitters(doc, tmp_path / "out")
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
    n, _ = run_pbg._redirect_emitters(doc, tmp_path / "out")
    assert n == 1
    assert doc["ram"]["config"] == {}
    assert doc["parquet"]["config"]["out_dir"] == str(tmp_path / "out")


def test_redirect_emitters_captures_a_preexisting_s3_out_dir_for_crosscheck(tmp_path: Path) -> None:
    """The second return value exists precisely so _assert_emitted_output can find
    real output an emitter wrote to its ORIGINAL destination instead of honoring
    the local redirect (the real Dispatch 727:Run 3 seed0 shape, 2026-09-09)."""
    doc: dict[str, Any] = {"e": {"address": "local:ParquetEmitter", "config": {"out_dir": "s3://bucket/real/place"}}}
    n, s3_locations = run_pbg._redirect_emitters(doc, tmp_path)
    assert n == 1
    assert s3_locations == ["s3://bucket/real/place"]


def test_redirect_emitters_does_not_capture_a_non_s3_preexisting_location(tmp_path: Path) -> None:
    """A local/relative authored path is meaningless as a cross-check candidate --
    only a real s3:// URI is worth polling."""
    doc: dict[str, Any] = {"e": {"address": "local:ParquetEmitter", "config": {"out_dir": "/authored/elsewhere"}}}
    n, s3_locations = run_pbg._redirect_emitters(doc, tmp_path)
    assert n == 1
    assert s3_locations == []


# --- _redirect_emitters: xarray goes straight to S3 (CD2 Dispatch 665/666 fix) ---


def test_redirect_emitters_routes_xarray_straight_to_s3_when_ray_out_s3_is_set(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """zarr's own cross-generation continuity check (viva_emitters.xarray_emitter.
    zarr_writer._check_group) needs the PREVIOUS generation's group to still exist
    in the SAME store. The local-then-periodic-sync path every other file-backed
    emitter uses is best-effort and this process's own filesystem is not
    authoritative on a multi-node run (viva-api#419) -- if the actor owning a
    seed's lineage gets restarted on a different node between generations, the
    local redirect silently loses the previous generation's zarr group and
    _check_group fails loudly (real incident: CD2 Dispatch 665/666). Routing
    straight to RAY_OUT_S3 (the same shared, node-independent prefix every node's
    periodic sync already targets) fixes this at the source."""
    monkeypatch.setenv("RAY_OUT_S3", "s3://smsvpctest-shared/vecoli-output/exp-123/")
    doc: dict[str, Any] = {"e": {"address": "local:XArrayEmitter", "config": {"out_uri": "s3://old/place"}}}
    n, s3_locations = run_pbg._redirect_emitters(doc, tmp_path / "out")
    assert n == 1
    assert s3_locations == ["s3://old/place"]
    assert doc["e"]["config"]["out_uri"] == "s3://smsvpctest-shared/vecoli-output/exp-123/"


def test_redirect_emitters_leaves_parquet_on_the_local_results_dir_even_when_ray_out_s3_is_set(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The S3-direct routing above is xarray-specific (out_uri), not a blanket
    "prefer RAY_OUT_S3 whenever it's set": ParquetEmitter's own independent chunk
    files are safe under the existing local-then-best-effort-sync path, and
    changing that unnecessarily would be a real, untested behavior change to
    every other MNP composite dispatch."""
    monkeypatch.setenv("RAY_OUT_S3", "s3://smsvpctest-shared/vecoli-output/exp-123/")
    doc: dict[str, Any] = {"e": {"address": "local:ParquetEmitter", "config": {}}}
    run_pbg._redirect_emitters(doc, tmp_path / "out")
    assert doc["e"]["config"]["out_dir"] == str(tmp_path / "out")


def test_redirect_emitters_falls_back_to_local_for_xarray_when_ray_out_s3_unset(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A non-Batch/local dev context has no RAY_OUT_S3 -- xarray must keep working
    exactly as before (the local redirect), not raise or silently drop the key."""
    monkeypatch.delenv("RAY_OUT_S3", raising=False)
    doc: dict[str, Any] = {"e": {"address": "local:XArrayEmitter", "config": {"out_uri": "s3://old/place"}}}
    run_pbg._redirect_emitters(doc, tmp_path)
    assert doc["e"]["config"]["out_uri"] == str(tmp_path)


def test_redirect_emitters_checks_the_actual_class_not_just_the_out_uri_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """cd2-worker review of #520: `key` picks out_dir first if a config ever
    carried BOTH out_dir and out_uri, which would silently mask a real xarray
    emitter behind the old local redirect. No real emitter_arg does that today,
    but a hypothetical non-XArrayEmitter class that happens to speak out_uri
    (with no out_dir key at all) must ALSO not take the S3-direct branch --
    only the real, registered XArrayEmitter class does."""
    monkeypatch.setenv("RAY_OUT_S3", "s3://smsvpctest-shared/vecoli-output/exp-123/")
    doc: dict[str, Any] = {"e": {"address": "local:SomeOtherEmitter", "config": {"out_uri": "s3://old/place"}}}
    run_pbg._redirect_emitters(doc, tmp_path / "out")
    assert doc["e"]["config"]["out_uri"] == str(tmp_path / "out")


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


def _write_parquet(path: Path, columns: dict[str, list[float]]) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table(columns), str(path))


def test_has_emitted_output_false_for_global_time_only_parquet(tmp_path: Path) -> None:
    """A parquet with ONLY global_time (the always-emitted port) is an empty emit
    — undeclared emit_paths — and must NOT count as real output, even though it is
    a non-empty file. This is the CD2 Run 2 / mecillinam failure: a ~500-byte,
    1-column, global_time-only history that slipped through the old size>0 gate."""
    _write_parquet(tmp_path / "history" / "1.pq", {"global_time": [0.0, 1.0]})
    assert run_pbg._has_emitted_output(tmp_path) is False


def test_has_emitted_output_true_for_real_data_parquet(tmp_path: Path) -> None:
    _write_parquet(
        tmp_path / "history" / "1.pq",
        {"global_time": [0.0, 1.0], "listeners__mass__dry_mass": [300.0, 320.0]},
    )
    assert run_pbg._has_emitted_output(tmp_path) is True


def test_has_emitted_output_false_for_zarr_markers_without_chunk(tmp_path: Path) -> None:
    """A zarr store whose root metadata was written at construction but which never
    received a data chunk (empty view / one-tick collapse) is an empty emit. The
    old gate returned True on the marker's mere existence — the CD2 Run 4 failure."""
    store = tmp_path / "v2ecoli_seed0.zarr"
    store.mkdir()
    (store / "zarr.json").write_text("{}")
    (store / ".zattrs").write_text("{}")
    assert run_pbg._has_emitted_output(tmp_path) is False


def test_has_emitted_output_true_for_zarr_with_chunk(tmp_path: Path) -> None:
    store = tmp_path / "v2ecoli_seed0.zarr"
    (store / "c" / "0").mkdir(parents=True)
    (store / "zarr.json").write_text("{}")
    (store / "c" / "0" / "0").write_bytes(b"\x00\x01\x02\x03")  # a real data chunk
    assert run_pbg._has_emitted_output(tmp_path) is True


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


# --- viva-api#419: the driver's own filesystem is not authoritative on a multi-node run ---


def _fake_aws(monkeypatch: pytest.MonkeyPatch, listing: str, *, rc: int = 0, missing: bool = False) -> list[list[str]]:
    """Stub the `aws s3 ls` probe. Returns the calls made, so a test can assert it was
    NOT called on the paths where the local check already answered."""
    calls: list[list[str]] = []
    monkeypatch.setattr(shutil, "which", lambda _: None if missing else "/usr/local/bin/aws")

    def fake_run(cmd: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(cmd)
        return subprocess.CompletedProcess(cmd, rc, stdout=listing, stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)
    return calls


_S3_WITH_OUTPUT = (
    "2026-09-04 21:30:58        408 vecoli-output/x/success/generation=1/agent_id=00/s.pq\n"
    "2026-09-04 21:22:11   54677027 vecoli-output/x/history/generation=0/agent_id=0/400.pq\n"
)
_S3_EMPTY_ONLY = "2026-09-04 21:30:58       2147 vecoli-output/x/final_state.json\n"


def test_shared_prefix_rescues_a_run_whose_actors_ran_on_a_PEER_node(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The #419 regression, stated as the failure it actually was: two successful
    lineage runs were failed here because Ray placed every actor on another node, so
    the driver's local dir was empty while ~700MB of parquet sat in S3."""
    monkeypatch.setenv("PBG_REQUIRE_OUTPUT", "1")
    monkeypatch.setenv("RAY_OUT_S3", "s3://bucket/vecoli-output/x/")
    calls = _fake_aws(monkeypatch, _S3_WITH_OUTPUT)
    (tmp_path / "final_state.json").write_text("{}")  # only the fallback, locally

    run_pbg._assert_emitted_output(tmp_path)  # must NOT raise
    assert calls and calls[0][1:4] == ["s3", "ls", "--recursive"]


def test_local_output_short_circuits_without_consulting_s3(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """When this node did host an actor the local answer is sufficient — no probe,
    no 120s subprocess on the happy path."""
    monkeypatch.setenv("PBG_REQUIRE_OUTPUT", "1")
    monkeypatch.setenv("RAY_OUT_S3", "s3://bucket/vecoli-output/x/")
    calls = _fake_aws(monkeypatch, _S3_WITH_OUTPUT)
    (tmp_path / "part.pq").write_bytes(b"nonempty")

    run_pbg._assert_emitted_output(tmp_path)
    assert calls == []


def test_still_fails_when_neither_local_nor_shared_has_output(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The guard must keep doing its job: a genuinely empty run still fails, and the
    message names BOTH places looked at."""
    monkeypatch.setenv("PBG_REQUIRE_OUTPUT", "1")
    monkeypatch.setenv("RAY_OUT_S3", "s3://bucket/vecoli-output/x/")
    monkeypatch.setattr(run_pbg, "_SHARED_OUTPUT_WAIT_SECONDS", 0)
    _fake_aws(monkeypatch, _S3_EMPTY_ONLY)

    with pytest.raises(SystemExit) as exc:
        run_pbg._assert_emitted_output(tmp_path)
    assert "s3://bucket/vecoli-output/x/" in str(exc.value)


def test_unreachable_shared_prefix_says_so_rather_than_claiming_no_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """'Could not look' and 'nothing there' are different answers. Failing is still the
    conservative choice, but the message must not assert an absence it never verified."""
    monkeypatch.setenv("PBG_REQUIRE_OUTPUT", "1")
    monkeypatch.setenv("RAY_OUT_S3", "s3://bucket/vecoli-output/x/")
    monkeypatch.setattr(run_pbg, "_SHARED_OUTPUT_WAIT_SECONDS", 0)
    _fake_aws(monkeypatch, "", missing=True)  # no aws binary in the image

    with pytest.raises(SystemExit) as exc:
        run_pbg._assert_emitted_output(tmp_path)
    assert "could not be listed" in str(exc.value)


def test_without_ray_out_s3_behaviour_is_unchanged(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Single-node and non-Batch callers keep the old, purely local semantics."""
    monkeypatch.setenv("PBG_REQUIRE_OUTPUT", "1")
    monkeypatch.delenv("RAY_OUT_S3", raising=False)
    calls = _fake_aws(monkeypatch, _S3_WITH_OUTPUT)

    with pytest.raises(SystemExit) as exc:
        run_pbg._assert_emitted_output(tmp_path)
    assert calls == []
    assert "no shared prefix" in str(exc.value)


# --- redirected_from_s3: LineageProcess/chain-dispatch's own emitter can bypass the
# local redirect entirely and write straight to its pre-redirect S3 destination, with
# no RAY_OUT_S3 available at all to cross-check (real incident: Dispatch 727:Run 3
# seed0, 2026-09-09 -- a genuinely successful ~360MB run reported a hard failure) ---


def test_redirected_from_s3_rescues_a_chain_dispatch_run_with_no_ray_out_s3(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The exact real shape: RAY_OUT_S3 is unset (chain-dispatch never sets it --
    it's an MNP-only signal), but the document had a file-backed emitter whose
    pre-redirect out_dir/out_uri really was where the run's own LineageProcess
    wrote its real output. That location must be checked before failing."""
    monkeypatch.setenv("PBG_REQUIRE_OUTPUT", "1")
    monkeypatch.delenv("RAY_OUT_S3", raising=False)
    calls = _fake_aws(monkeypatch, _S3_WITH_OUTPUT)
    (tmp_path / "final_state.json").write_text("{}")  # only the fallback, locally

    run_pbg._assert_emitted_output(tmp_path, ["s3://bucket/vecoli-output/x/seed_00"])  # must NOT raise
    assert calls and calls[0][1:4] == ["s3", "ls", "--recursive"]


def test_redirected_from_s3_checked_even_when_ray_out_s3_is_also_set_but_empty(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """RAY_OUT_S3 and a redirected emitter's own original location are independent
    candidates, not mutually exclusive: a run must succeed if EITHER has real
    output, even if RAY_OUT_S3 itself is empty."""
    monkeypatch.setenv("PBG_REQUIRE_OUTPUT", "1")
    monkeypatch.setenv("RAY_OUT_S3", "s3://bucket/vecoli-output/empty-prefix/")
    (tmp_path / "final_state.json").write_text("{}")

    calls: list[list[str]] = []
    monkeypatch.setattr(shutil, "which", lambda _: "/usr/local/bin/aws")

    def fake_run(cmd: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(cmd)
        listing = _S3_WITH_OUTPUT if cmd[-1] == "s3://bucket/vecoli-output/x/seed_00" else _S3_EMPTY_ONLY
        return subprocess.CompletedProcess(cmd, 0, stdout=listing, stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)

    run_pbg._assert_emitted_output(tmp_path, ["s3://bucket/vecoli-output/x/seed_00"])  # must NOT raise
    assert any(c[-1] == "s3://bucket/vecoli-output/empty-prefix/" for c in calls)
    assert any(c[-1] == "s3://bucket/vecoli-output/x/seed_00" for c in calls)


def test_still_fails_when_redirected_from_s3_is_also_empty(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The new candidates don't weaken the guard: still raises, and the message
    now names every location actually checked."""
    monkeypatch.setenv("PBG_REQUIRE_OUTPUT", "1")
    monkeypatch.delenv("RAY_OUT_S3", raising=False)
    monkeypatch.setattr(run_pbg, "_SHARED_OUTPUT_WAIT_SECONDS", 0)
    _fake_aws(monkeypatch, _S3_EMPTY_ONLY)

    with pytest.raises(SystemExit) as exc:
        run_pbg._assert_emitted_output(tmp_path, ["s3://bucket/vecoli-output/x/seed_00"])
    assert "s3://bucket/vecoli-output/x/seed_00" in str(exc.value)


def test_redirected_from_s3_defaults_to_none_and_behaves_like_before(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every pre-existing caller passes only `results_dir` -- the new parameter
    must be fully optional and change nothing when omitted."""
    monkeypatch.setenv("PBG_REQUIRE_OUTPUT", "1")
    monkeypatch.delenv("RAY_OUT_S3", raising=False)
    calls = _fake_aws(monkeypatch, _S3_WITH_OUTPUT)

    with pytest.raises(SystemExit) as exc:
        run_pbg._assert_emitted_output(tmp_path)
    assert calls == []
    assert "no shared prefix" in str(exc.value)


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


# --- _lineage_generation_duration_total: LineageProcess's real per-generation clock,
# --- decoupled from the outer composite's own global_time (sms-ecoli#210, dispatch 297) ---


def test_lineage_generation_duration_total_sums_nested_summaries(tmp_path: Path) -> None:
    _write_final_state(
        tmp_path,
        {
            "global_time": 1.0,
            "seed_0000": {
                "summary": {
                    "generations": [
                        {"generation": 0, "duration": 2527.0, "divided": True},
                    ]
                }
            },
            "seed_0001": {"summary": {"generations": [{"generation": 0, "duration": 1800.0, "divided": True}]}},
        },
    )
    assert run_pbg._lineage_generation_duration_total(tmp_path) == 4327.0


def test_lineage_generation_duration_total_none_when_absent(tmp_path: Path) -> None:
    assert run_pbg._lineage_generation_duration_total(tmp_path) is None  # no file
    _write_final_state(tmp_path, {"global_time": 2528.0})  # no summary anywhere
    assert run_pbg._lineage_generation_duration_total(tmp_path) is None
    _write_final_state(tmp_path, {"seed_0000": {"summary": {"generations": []}}})  # empty list
    assert run_pbg._lineage_generation_duration_total(tmp_path) is None
    _write_final_state(tmp_path, {"seed_0000": {"summary": {"generations": [{"divided": True}]}}})  # no duration
    assert run_pbg._lineage_generation_duration_total(tmp_path) is None


def test_assert_run_advanced_passes_a_real_lineage_generation_even_though_global_time_reads_one(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reproduces dispatch 297 exactly (sms-ecoli#210): a chain-dispatch/pbg-native
    generation genuinely divided at t=2527s, but the outer composite's own
    global_time only reflects the single external run(interval) tick (~1.0) —
    the effect check must not treat this as a one-tick collapse."""
    monkeypatch.setenv("PBG_MIN_GLOBAL_TIME", "100")
    _write_final_state(
        tmp_path,
        {
            "global_time": 1.0,
            "seed_0000": {"summary": {"generations": [{"generation": 0, "duration": 2527.0, "divided": True}]}},
        },
    )
    run_pbg._assert_run_advanced(tmp_path)  # must not raise


def test_assert_run_advanced_still_raises_on_a_real_lineage_one_tick_collapse(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A lineage summary that itself reports a short duration is still a real
    collapse — the lineage-aware signal doesn't blanket-exempt this composite
    family from the check, it just reads the correct clock."""
    monkeypatch.setenv("PBG_MIN_GLOBAL_TIME", "100")
    _write_final_state(
        tmp_path,
        {"global_time": 1.0, "seed_0000": {"summary": {"generations": [{"duration": 1.0, "divided": False}]}}},
    )
    with pytest.raises(SystemExit):
        run_pbg._assert_run_advanced(tmp_path)


def test_lineage_generation_duration_total_reads_batch_baseline_runner_wall_s(tmp_path: Path) -> None:
    """The second, independently-real shape (2026-09-09, Dispatch 736:Run 3):
    BatchBaselineRunner-driven chain-dispatch (mecillinam_wellmixed.json and every
    other config using local:v2ecoli.steps.batch_baseline_runner.BatchBaselineRunner)
    reports real elapsed time as batch.wall_s, not summary.generations[].duration."""
    _write_final_state(
        tmp_path,
        {
            "global_time": 1.0,
            "batch": {"completed": True, "n_seeds": 1, "n_generations": 1, "wall_s": 2528.0},
        },
    )
    assert run_pbg._lineage_generation_duration_total(tmp_path) == 2528.0


def test_lineage_generation_duration_total_sums_both_shapes_together(tmp_path: Path) -> None:
    """The two shapes are independent signals of the same underlying fact (real
    elapsed time) and are summed together like every other duration source,
    not treated as mutually exclusive alternatives."""
    _write_final_state(
        tmp_path,
        {
            "global_time": 1.0,
            "batch": {"wall_s": 2528.0},
            "seed_0000": {"summary": {"generations": [{"duration": 1800.0, "divided": True}]}},
        },
    )
    assert run_pbg._lineage_generation_duration_total(tmp_path) == 4328.0


def test_lineage_generation_duration_total_ignores_a_non_numeric_wall_s(tmp_path: Path) -> None:
    _write_final_state(tmp_path, {"global_time": 1.0, "batch": {"wall_s": "not-a-number"}})
    assert run_pbg._lineage_generation_duration_total(tmp_path) is None
    _write_final_state(tmp_path, {"global_time": 1.0, "batch": {"wall_s": True}})  # bool is an int subclass
    assert run_pbg._lineage_generation_duration_total(tmp_path) is None


def test_assert_run_advanced_passes_a_real_batch_baseline_runner_generation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reproduces Dispatch 736:Run 3 seed0 exactly: a real division at t=2528s
    under BatchBaselineRunner's own batch.wall_s shape, global_time misleadingly
    reads 1.0 -- the effect check must not treat this as a one-tick collapse."""
    monkeypatch.setenv("PBG_MIN_GLOBAL_TIME", "100")
    _write_final_state(tmp_path, {"global_time": 1.0, "batch": {"wall_s": 2528.0}})
    run_pbg._assert_run_advanced(tmp_path)  # must not raise


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


# --- schema-aware guards (sms-ecoli#166: MNP run identity + required run interval) ---


class _Spec:
    def __init__(self, parameters: dict[str, Any]) -> None:
        self.parameters = parameters


_LINEAGE = _Spec({
    "n_generations": {"type": "integer", "default": 1},
    "max_duration_per_gen": {"type": "float", "default": 3600.0},
    "experiment_id": {"type": "string", "default": "lineage_ray_batch"},
    "seed": {"type": "integer", "default": 0},
})
_COLONY = _Spec({"n_cells": {"type": "integer", "default": 4}})


def test_run_identity_is_injected_only_when_the_composite_declares_it() -> None:
    from viva_api.compose.run_pbg import _apply_declared_run_identity

    assert _apply_declared_run_identity(_LINEAGE, {"seed": 3}, "sim172-run2-ab12") == {
        "seed": 3,
        "experiment_id": "sim172-run2-ab12",
    }
    # a composite that never declares it must NOT receive it (to_document would raise)
    assert _apply_declared_run_identity(_COLONY, {"n_cells": 6}, "sim172-run2-ab12") == {"n_cells": 6}
    # an explicit override always wins; None/"" identity is a no-op
    assert _apply_declared_run_identity(_LINEAGE, {"experiment_id": "mine"}, "other")["experiment_id"] == "mine"
    assert _apply_declared_run_identity(_LINEAGE, {}, None) == {}


def test_lineage_composite_refuses_to_under_run_even_with_all_defaults() -> None:
    """The hole the API-side clamp cannot see: n_generations AND steps both omitted."""
    from viva_api.compose.run_pbg import _check_required_run_interval

    with pytest.raises(SystemExit, match="refusing to under-run.*-n 1 < required 3600"):
        _check_required_run_interval(_LINEAGE, {}, 1)
    _check_required_run_interval(_LINEAGE, {}, 3600)  # exactly the contract: fine


def test_required_run_interval_uses_the_overrides_when_given() -> None:
    from viva_api.compose.run_pbg import _check_required_run_interval

    with pytest.raises(SystemExit, match="required 14400"):
        _check_required_run_interval(_LINEAGE, {"n_generations": 4}, 3600)
    with pytest.raises(SystemExit, match="required 2400"):
        _check_required_run_interval(_LINEAGE, {"n_generations": 2, "max_duration_per_gen": 1200.0}, 2399)
    _check_required_run_interval(_LINEAGE, {"n_generations": 2, "max_duration_per_gen": 1200.0}, 2400)


_BASELINE = _Spec({
    "n_seeds": {"type": "integer", "default": 1},
    "n_generations": {"type": "integer", "default": 1},
    "max_duration_per_gen": {"type": "float", "default": 3600.0},
    "seed": {"type": "integer", "default": 0},
})
_BASELINE_ID = "v2ecoli.composites.ecoli_baseline.ecoli_baseline"


def test_batch_shape_of_ecoli_baseline_is_exempt_from_the_under_run_check() -> None:
    """viva-api#578's whole-lineage chain job (sms-ecoli#166, 2026-09-10):
    ecoli_baseline with n_generations>1 builds BatchBaselineRunner, a Step whose
    one update runs the whole lineage, so `-n 1` is the complete run. On 0.9.135
    this guard refused every such job."""
    from viva_api.compose.run_pbg import _check_required_run_interval

    _check_required_run_interval(_BASELINE, {"n_generations": 20, "n_seeds": 1}, 1, composite_id=_BASELINE_ID)
    _check_required_run_interval(_BASELINE, {"n_generations": 1, "n_seeds": 4}, 1, composite_id=_BASELINE_ID)
    # The single-cell shape (one seed, one generation) is NOT the batch route:
    # a 1 s run there is Dispatch 438's failure shape and must still be refused.
    with pytest.raises(SystemExit, match="refusing to under-run"):
        _check_required_run_interval(_BASELINE, {"n_generations": 1, "n_seeds": 1}, 1, composite_id=_BASELINE_ID)
    # Another composite declaring n_generations keeps the contract (MNP's lineage_ray_batch).
    with pytest.raises(SystemExit, match="required 72000"):
        _check_required_run_interval(
            _LINEAGE, {"n_generations": 20}, 1, composite_id="v2ecoli.composites.lineage_ray_batch"
        )


def test_chain_whole_lineage_command_passes_the_under_run_guard() -> None:
    """The exact overrides `_seed_lineage_command` emits (n_generations=N, -n 1,
    no stop_at_division) must clear the guard the container runs them through."""
    import json
    import shlex

    from viva_api.compose.run_pbg import _check_required_run_interval
    from viva_api.simulation.simulation_service_ray import (
        V2ECOLI_BATCH_BASELINE_COMPOSITE_ID,
        SimulationServiceRay,
    )

    cmd = SimulationServiceRay._seed_lineage_command(
        SimulationServiceRay.__new__(SimulationServiceRay),
        seed=3,
        n_generations=20,
        experiment_id="sim189-run3-x",
        runner_s3_uri="s3://b/run_pbg.py",
    )
    tokens = shlex.split(cmd.split("python /tmp/run_pbg.py", 1)[1])
    overrides = json.loads(tokens[tokens.index("--overrides") + 1])
    steps = int(tokens[tokens.index("-n") + 1])
    composite_id = tokens[tokens.index("--composite-id") + 1]
    assert steps == 1 and composite_id == V2ECOLI_BATCH_BASELINE_COMPOSITE_ID
    assert overrides["n_generations"] == 20 and "stop_at_division" not in overrides
    _check_required_run_interval(_BASELINE, overrides, steps, composite_id=composite_id)


def test_stop_at_division_is_exempt_from_the_under_run_check() -> None:
    """CD2 Run 3 (chain-dispatch, sms-ecoli#166): each per-generation job
    hardcodes -n 1 deliberately (_seed_generation_command) -- stop_at_division
    makes LineageProcess advance to a real division internally, so steps is a
    trigger, not a simulated-time budget. Must not reopen Dispatch 438 (no
    stop_at_division at all, the real under-run this check exists for)."""
    from viva_api.compose.run_pbg import _check_required_run_interval

    _check_required_run_interval(_LINEAGE, {"stop_at_division": True}, 1)  # exempt: fine
    _check_required_run_interval(
        _LINEAGE, {"n_generations": 10, "stop_at_division": True}, 1
    )  # exempt regardless of n_generations
    with pytest.raises(SystemExit, match="refusing to under-run.*-n 1 < required 3600"):
        _check_required_run_interval(_LINEAGE, {"stop_at_division": False}, 1)  # explicit False still checked
    with pytest.raises(SystemExit, match="refusing to under-run.*-n 1 < required 3600"):
        _check_required_run_interval(_LINEAGE, {}, 1)  # Dispatch 438's own shape: still refused


def test_non_lineage_composites_are_never_checked() -> None:
    from viva_api.compose.run_pbg import _check_required_run_interval

    _check_required_run_interval(_COLONY, {"n_cells": 6}, 1)  # a colony's 1 step is its own business


def test_main_threads_experiment_id_through_to_run(monkeypatch: pytest.MonkeyPatch) -> None:
    from viva_api.compose import run_pbg

    seen: dict[str, Any] = {}

    def fake_run(
        input_file: str | None,
        steps: int,
        composite_id: str | None = None,
        overrides: dict[str, Any] | None = None,
        experiment_id: str | None = None,
        **kw: Any,
    ) -> Path:
        seen.update(steps=steps, composite_id=composite_id, overrides=overrides, experiment_id=experiment_id)
        return Path("/dev/null")

    monkeypatch.setattr(run_pbg, "run", fake_run)
    run_pbg.main(["--composite-id", "x.y", "--overrides", '{"seed": 1}', "-n", "7200", "--experiment-id", "sim1-r2-ab"])
    assert seen == {"steps": 7200, "composite_id": "x.y", "overrides": {"seed": 1}, "experiment_id": "sim1-r2-ab"}


# --- observability bootstrap: run_pbg must emit on the chain/MNP paths too (plan PR-D) ---
#
# Both paths run this script directly (never through process-bigraph's run_step),
# so with the engine's silent library default they emitted nothing (sim 957: 0
# events on the chain path vs 96 on the Nextflow path).


class _FakeEmitter:
    def __init__(self, sinks: list[Any] | None = None) -> None:
        self._sinks: list[Any] = list(sinks or [])
        self.events: list[dict[str, Any]] = []
        self.spans: list[dict[str, Any]] = []
        self.flushes = 0

    @property
    def enabled(self) -> bool:
        return bool(self._sinks)

    def start_span(self, span_name: str, **attrs: Any) -> Any:
        record: dict[str, Any] = {"name": span_name, "attrs": attrs, "status": None, "error": None}
        self.spans.append(record)

        class _Span:
            def end(_self, status: str = "ok", error: str | None = None) -> None:
                record["status"] = status
                record["error"] = error

        return _Span()

    def event(self, event_name: str, level: str = "info", component: str = "process_bigraph", **payload: Any) -> None:
        self.events.append({"event": event_name, "level": level, "component": component, **payload})

    def flush(self) -> None:
        self.flushes += 1


class _FakeFileSink:
    def __init__(self, path: str) -> None:
        self.path = path


def _install_fake_events(monkeypatch: pytest.MonkeyPatch, emitter: _FakeEmitter) -> types.ModuleType:
    """A `process_bigraph.events` that hands out *emitter*; `configure` records
    its call and enables the emitter (stdout sink) the way the real one does."""
    fake_pbg_mod = types.ModuleType("process_bigraph")
    fake_events = types.ModuleType("process_bigraph.events")
    fake_events.configure_calls = []  # type: ignore[attr-defined]

    def configure(spec: str | None = None, *, default: str = "none", **_: Any) -> _FakeEmitter:
        fake_events.configure_calls.append({"spec": spec, "default": default})
        emitter._sinks.append(f"<{default} sink>")
        return emitter

    fake_events.get_emitter = lambda: emitter  # type: ignore[attr-defined]
    fake_events.configure = configure  # type: ignore[attr-defined]
    fake_events.FileSink = _FakeFileSink  # type: ignore[attr-defined]
    fake_pbg_mod.events = fake_events  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "process_bigraph", fake_pbg_mod)
    monkeypatch.setitem(sys.modules, "process_bigraph.events", fake_events)
    return fake_events


def test_configure_events_is_a_noop_without_the_events_module(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    fake_pbg_mod = types.ModuleType("process_bigraph")  # an older engine: no `events`
    monkeypatch.setitem(sys.modules, "process_bigraph", fake_pbg_mod)
    monkeypatch.setitem(
        sys.modules, "process_bigraph.events", None
    )  # `from process_bigraph import events` -> ImportError
    assert run_pbg._configure_events(tmp_path) is None
    with run_pbg._task_span(None, "x") as span:
        assert span is None
    assert not (tmp_path / run_pbg.EVENTS_FILENAME).exists()


def test_configure_events_defaults_to_stdout_plus_a_file_sink(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    emitter = _FakeEmitter()
    fake_events = _install_fake_events(monkeypatch, emitter)
    out_dir = tmp_path / "output"

    got = run_pbg._configure_events(out_dir)

    assert got is emitter
    assert fake_events.configure_calls == [{"spec": None, "default": "stdout"}]
    assert out_dir.is_dir()
    file_sinks = [s for s in emitter._sinks if isinstance(s, _FakeFileSink)]
    assert [s.path for s in file_sinks] == [str(out_dir / "events.jsonl")]
    # Idempotent: a second call keeps the enabled emitter and does not add a second file sink.
    assert run_pbg._configure_events(out_dir) is emitter
    assert len(fake_events.configure_calls) == 1
    assert len([s for s in emitter._sinks if isinstance(s, _FakeFileSink)]) == 1


def test_configure_events_keeps_an_emitter_the_entrypoint_already_enabled(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    emitter = _FakeEmitter(sinks=["<entrypoint stdout sink>"])
    fake_events = _install_fake_events(monkeypatch, emitter)
    assert run_pbg._configure_events(tmp_path) is emitter
    assert fake_events.configure_calls == []
    assert emitter._sinks[0] == "<entrypoint stdout sink>"
    assert [s.path for s in emitter._sinks if isinstance(s, _FakeFileSink)] == [str(tmp_path / "events.jsonl")]


def test_configure_events_never_raises(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    fake_pbg_mod = types.ModuleType("process_bigraph")
    fake_events = types.ModuleType("process_bigraph.events")

    def boom() -> None:
        raise RuntimeError("sink resolution exploded")

    fake_events.get_emitter = boom  # type: ignore[attr-defined]
    fake_pbg_mod.events = fake_events  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "process_bigraph", fake_pbg_mod)
    monkeypatch.setitem(sys.modules, "process_bigraph.events", fake_events)
    assert run_pbg._configure_events(tmp_path) is None


def test_task_span_reports_ok_and_error(monkeypatch: pytest.MonkeyPatch) -> None:
    emitter = _FakeEmitter(sinks=["stdout"])
    with run_pbg._task_span(emitter, "ecoli_lineage", composite_id="ecoli_lineage", steps=3):
        pass
    assert emitter.spans == [
        {
            "name": "task",
            "attrs": {"name": "ecoli_lineage", "composite_id": "ecoli_lineage", "steps": 3},
            "status": "ok",
            "error": None,
        }
    ]
    assert [(e["event"], e["component"], e["level"]) for e in emitter.events] == [
        ("task.start", run_pbg.EVENTS_COMPONENT, "info"),
        ("task.end", run_pbg.EVENTS_COMPONENT, "info"),
    ]
    assert emitter.events[-1]["status"] == "ok"
    assert emitter.flushes == 1

    emitter = _FakeEmitter(sinks=["stdout"])
    with pytest.raises(ValueError, match="no output"), run_pbg._task_span(emitter, "ecoli_lineage"):
        raise ValueError("no output")
    assert emitter.spans[0]["status"] == "error"
    assert emitter.spans[0]["error"] == "ValueError: no output"
    assert emitter.events[-1] == {
        "event": "task.end",
        "level": "error",
        "component": run_pbg.EVENTS_COMPONENT,
        "status": "error",
        "name": "ecoli_lineage",
        "error": "ValueError: no output",
    }
    assert emitter.flushes == 1


def test_run_opens_a_task_span_named_from_the_composite_id(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """End to end through run(): the emitter is configured, the span is named
    from the composite id, and the run's own output is unchanged."""
    emitter = _FakeEmitter()
    fake_events = _install_fake_events(monkeypatch, emitter)

    class FakeComposite:
        def __init__(self, doc: Any, core: Any = None) -> None:
            self.n = 0

        def run(self, n: int) -> None:
            self.n = n

        def serialize_state(self) -> dict[str, int]:
            return {"ran": self.n}

    fake_pbg_mod = sys.modules["process_bigraph"]
    fake_pbg_mod.Composite = FakeComposite  # type: ignore[attr-defined]
    fake_pbg_mod.register_types = lambda core: core  # type: ignore[attr-defined]
    fake_schema_mod = types.ModuleType("bigraph_schema")
    fake_schema_mod.allocate_core = FakeCore  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "bigraph_schema", fake_schema_mod)
    _install_fake_protocol_registration(monkeypatch)

    pbg = tmp_path / "toy_model.pbg"
    pbg.write_text(json.dumps({"state": {}, "composition": {}}))
    out = run_pbg.run(str(pbg), steps=2, results_dir=tmp_path / "output")

    assert json.loads(out.read_text())["ran"] == 2
    assert fake_events.configure_calls == [{"spec": None, "default": "stdout"}]
    assert emitter.spans[0]["name"] == "task"
    assert emitter.spans[0]["attrs"]["name"] == "toy_model"  # input-file stem when no --composite-id
    assert emitter.spans[0]["attrs"]["steps"] == 2
    assert emitter.spans[0]["status"] == "ok"
    assert [s.path for s in emitter._sinks if isinstance(s, _FakeFileSink)] == [
        str(tmp_path / "output" / "events.jsonl")
    ]
