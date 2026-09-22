"""render_nf: compile a registered composite into a Nextflow workflow.

The guard is the point. A document whose nodes are all unrenderable produces a
main.nf that Nextflow accepts and that does nothing — a green run with no work in
it, which is the exact failure family this pipeline keeps paying for.
"""

from __future__ import annotations

import json
import shutil
import sys
import types
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from viva_api.compose import render_nf

# process_bigraph is NOT a viva-api dependency: it lives in the simulator image
# this runner is staged into, which is why run_pbg imports it lazily inside
# functions. So these tests must supply it rather than patch attributes on it --
# `patch("process_bigraph.nextflow_deploy.deploy")` needs the module to import,
# and in a clean environment it does not exist. Same fake-module approach
# test_run_pbg.py already uses for the same reason.

_MAIN_NF = """\
process parca_v0 {
    output:
    path "cache"
    script:
    "v2ecoli-parca"
}
workflow  {
    ch_cache_v0 = parca_v0()
}
"""


def _fake_deploy(outdir: Path, text: str = _MAIN_NF, configs: tuple[str, ...] = ("parca_v0.config.json",)) -> Any:
    """Stand in for process_bigraph's deploy(): writes what it would have written."""

    def deploy(composite: Any, **kwargs: Any) -> dict[str, Any]:
        deploy.kwargs = kwargs  # type: ignore[attr-defined]
        Path(kwargs["outdir"]).mkdir(parents=True, exist_ok=True)
        (Path(kwargs["outdir"]) / "main.nf").write_text(text)
        for name in configs:
            (Path(kwargs["outdir"]) / name).write_text("{}")
        return {"returncode": 0 if kwargs.get("launch") else None}

    deploy.kwargs = {}  # type: ignore[attr-defined]
    return deploy


def _install_fake_pbg(monkeypatch: pytest.MonkeyPatch, deploy: Any) -> None:
    """Provide the slice of process_bigraph render_nf actually touches."""
    pbg = types.ModuleType("process_bigraph")
    pbg.Composite = lambda document, core=None: object()  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "process_bigraph", pbg)

    deploy_mod = types.ModuleType("process_bigraph.nextflow_deploy")
    deploy_mod.deploy = deploy  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "process_bigraph.nextflow_deploy", deploy_mod)


def _render(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, deploy: Any, **kwargs: Any) -> dict[str, Any]:
    _install_fake_pbg(monkeypatch, deploy)
    with (
        patch("viva_core.compose.run_pbg._build_core", return_value=object()),
        patch("viva_core.compose.run_pbg._resolve_document", return_value=({"state": {}}, object())),
    ):
        return render_nf.render("v2ecoli.composites.workflow_nf", tmp_path, **kwargs)


def test_renders_without_launching_by_default(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Rendering is cheap and needs no nextflow binary; launching needs the head
    image. Default to the half that can always run."""
    deploy = _fake_deploy(tmp_path)
    summary = _render(monkeypatch, tmp_path, deploy)
    assert deploy.kwargs["launch"] is False
    assert summary["process_blocks"] == 1
    assert summary["staged_configs"] == ["parca_v0.config.json"]
    assert summary["returncode"] is None


def test_summary_is_written_beside_the_workflow(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _render(monkeypatch, tmp_path, _fake_deploy(tmp_path))
    written = json.loads((tmp_path / "render_summary.json").read_text())
    assert written["composite_id"] == "v2ecoli.composites.workflow_nf"
    assert written["executor"] == "local"


def test_a_workflow_with_no_process_block_FAILS(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """The guard: this file is valid Nextflow and does nothing. Without the check
    it renders, launches, exits 0, and reports success over an empty campaign."""
    deploy = _fake_deploy(tmp_path, text="workflow  {\n}\n", configs=())
    with pytest.raises(SystemExit, match="no `process` block"):
        _render(monkeypatch, tmp_path, deploy)


def test_a_missing_main_nf_FAILS(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    def deploy(composite: Any, **kwargs: Any) -> dict[str, Any]:
        return {"returncode": None}

    with pytest.raises(SystemExit, match="no main.nf"):
        _render(monkeypatch, tmp_path, deploy)


def test_launch_flags_reach_deploy(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """-resume is go/no-go 3; the trace CSV is how a resumed run is told apart
    from a repeated one, since a reused task reports CACHED only there."""
    deploy = _fake_deploy(tmp_path)
    _render(
        monkeypatch,
        tmp_path,
        deploy,
        launch=True,
        resume=True,
        trace=str(tmp_path / "t.csv"),
        report=str(tmp_path / "r.html"),
        weblog_url="http://receiver/events",
        work_dir="s3://bucket/work",
    )
    assert deploy.kwargs["launch"] is True
    assert deploy.kwargs["resume"] is True
    assert deploy.kwargs["trace"] == str(tmp_path / "t.csv")
    assert deploy.kwargs["weblog_url"] == "http://receiver/events"
    assert deploy.kwargs["work_dir"] == "s3://bucket/work"


def test_executor_is_forwarded(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Phase 3 verifies with executor='local' INSIDE the real image before Phase 4
    introduces awsbatch, so that 'does it render and launch here' is answered
    separately from 'does the awsbatch executor work'."""
    deploy = _fake_deploy(tmp_path)
    _render(monkeypatch, tmp_path, deploy, executor="awsbatch")
    assert deploy.kwargs["executor"] == "awsbatch"


# --- render_nf must survive where it actually RUNS -------------------------


def _is_ours(name: str) -> bool:
    """The two packages the simulator image does NOT have."""
    return name.split(".")[0] in ("viva_api", "viva_core")


def test_render_nf_resolves_run_pbg_without_viva_api(tmp_path: Path) -> None:
    """render_nf is staged into the SIMULATOR image, which has no `viva_api`.

    It reuses run_pbg's resolver, and originally did so via
    `from viva_core.compose.run_pbg import ...` (now `viva_core.compose.run_pbg`;
    the image has neither package). That import works in the api pod
    and fails in the only place the script actually runs -- after a successful
    5.8 GB pull and a clean container start, which is the most expensive
    possible moment to find out.

    This reproduces the staged layout exactly: both scripts as bare top-level
    modules in one directory, with `viva_api` genuinely unimportable.
    """
    import importlib.util
    import shutil
    import sys

    import viva_api.compose.render_nf as rn
    import viva_api.compose.run_pbg as rp

    staged = tmp_path / "staged"
    staged.mkdir()
    shutil.copy(rn.__file__, staged / "render_nf.py")
    shutil.copy(rp.__file__, staged / "run_pbg.py")

    class _BlockVivaApi:
        def find_spec(self, name: str, path: object = None, target: object = None) -> None:
            if _is_ours(name):
                raise ModuleNotFoundError(f"No module named {name!r}")
            return None

    saved_modules = {k: v for k, v in sys.modules.items() if _is_ours(k)}
    for k in saved_modules:
        del sys.modules[k]
    sys.meta_path.insert(0, _BlockVivaApi())
    sys.path.insert(0, str(staged))
    try:
        spec = importlib.util.spec_from_file_location("render_nf_staged", staged / "render_nf.py")
        assert spec is not None and spec.loader is not None
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)  # module scope must not need viva_api
        build_core, resolve_document, workspace_core = mod._load_run_pbg()
        assert callable(build_core) and callable(resolve_document) and callable(workspace_core)
    finally:
        sys.meta_path.pop(0)
        sys.path.remove(str(staged))
        sys.modules.update(saved_modules)


def test_render_nf_says_what_is_missing_when_run_pbg_was_not_staged(tmp_path: Path) -> None:
    """Staging only render_nf is a dispatcher bug; the error should name it."""
    import importlib.util
    import shutil
    import sys

    import viva_api.compose.render_nf as rn

    staged = tmp_path / "lonely"
    staged.mkdir()
    shutil.copy(rn.__file__, staged / "render_nf.py")  # run_pbg deliberately absent

    class _BlockAll:
        def find_spec(self, name: str, path: object = None, target: object = None) -> None:
            if _is_ours(name):
                raise ModuleNotFoundError(f"No module named {name!r}")
            return None

    # `run_pbg` must go too: a previous test imports it as a TOP-LEVEL module,
    # and a cached entry would satisfy the fallback and mask the failure.
    saved = {k: v for k, v in sys.modules.items() if _is_ours(k) or k == "run_pbg"}
    for k in saved:
        del sys.modules[k]
    sys.meta_path.insert(0, _BlockAll())
    try:
        spec = importlib.util.spec_from_file_location("render_nf_lonely", staged / "render_nf.py")
        assert spec is not None and spec.loader is not None
        assert spec is not None and spec.loader is not None
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        import pytest as _pytest

        with _pytest.raises(SystemExit, match="stage_render_nf"):
            mod._load_run_pbg()
    finally:
        sys.meta_path.pop(0)
        sys.modules.update(saved)


def test_render_prefers_the_workspace_core_over_the_generic_one(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """render_nf must make run_pbg's OWN core choice, not half of it.

    It called `_build_core()` directly, skipping `_workspace_core()` and so
    ignoring PBG_CORE_BUILDER entirely. Everything resolved except documents
    using workspace-registered TYPES -- addresses resolve dynamically, types do
    not -- so it failed only on nested Composites, i.e. only on the sub-workflow
    emission this whole path exists for.

    Uses the fake process_bigraph, because it is NOT a viva-api dependency: an
    earlier version of this test let `render` die at `from process_bigraph
    import Composite` and passed anyway, since a bare `pytest.raises(Exception)`
    cannot tell that apart from the thing it meant to assert.
    """
    calls: list[str] = []
    workspace_core = object()

    _install_fake_pbg(monkeypatch, lambda *a, **k: {"returncode": 0})
    # Must be VALID Nextflow: _assert_rendered now compiles it (see _assert_compiles).
    (tmp_path / "main.nf").write_text('process x {\n    script:\n    """\n    echo hi\n    """\n}\nworkflow { x() }\n')

    def _fake_workspace() -> object:
        calls.append("workspace")
        return workspace_core

    def _fake_generic() -> object:
        calls.append("generic")
        return object()

    seen: dict[str, object] = {}

    def _fake_resolve(_in: object, _cid: str, _ov: dict[str, Any], core: object) -> tuple[dict[str, Any], object]:
        seen["core"] = core
        return ({"state": {}}, core)

    monkeypatch.setattr(
        "viva_api.compose.render_nf._load_run_pbg",
        lambda: (_fake_generic, _fake_resolve, _fake_workspace),
    )
    from viva_api.compose.render_nf import render

    render("some.composite", tmp_path, executor="local")

    assert calls == ["workspace"], "the generic core must not be built when a workspace one exists"
    assert seen["core"] is workspace_core, "the workspace core must be the one handed to _resolve_document"


def test_render_falls_back_to_the_generic_core_when_no_builder_is_named(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """PBG_CORE_BUILDER unset is normal for a plain compose document."""
    calls: list[str] = []
    generic = object()

    _install_fake_pbg(monkeypatch, lambda *a, **k: {"returncode": 0})
    # Must be VALID Nextflow: _assert_rendered now compiles it (see _assert_compiles).
    (tmp_path / "main.nf").write_text('process x {\n    script:\n    """\n    echo hi\n    """\n}\nworkflow { x() }\n')

    def _fake_generic() -> object:
        calls.append("generic")
        return generic

    def _fake_workspace() -> object | None:
        calls.append("workspace")
        return None  # no PBG_CORE_BUILDER named

    def _fake_resolve(_i: object, _c: str, _o: dict[str, Any], core: object) -> tuple[dict[str, Any], object]:
        return ({"state": {}}, core)

    monkeypatch.setattr(
        "viva_api.compose.render_nf._load_run_pbg",
        lambda: (_fake_generic, _fake_resolve, _fake_workspace),
    )
    from viva_api.compose.render_nf import render

    render("some.composite", tmp_path, executor="local")
    assert calls == ["workspace", "generic"]


@pytest.mark.skipif(shutil.which("nextflow") is None, reason="nextflow binary not on PATH")
def test_a_workflow_that_does_not_compile_is_a_failed_render(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """The guard that would have caught process-bigraph#205.

    A render once produced the right sub-workflow structure, the right staged
    configs and exit 0 while emitting a `script:` block containing Groovy
    SOURCE. `main.nf` contained "process ", so every presence check passed;
    `nextflow run` then failed to compile the whole file. This reproduces that
    shape exactly -- an unquoted script body.
    """
    _install_fake_pbg(monkeypatch, lambda *a, **k: {"returncode": 0})
    (tmp_path / "main.nf").write_text(
        "process parca_v0 {\n    script:\nv2ecoli-parca --mode fast\n}\nworkflow { parca_v0() }\n"
    )
    monkeypatch.setattr(
        "viva_api.compose.render_nf._load_run_pbg",
        lambda: (
            lambda: object(),
            lambda _i, _c, _o, core: ({"state": {}}, core),
            lambda: object(),
        ),
    )
    from viva_api.compose.render_nf import render

    with pytest.raises(SystemExit, match="does not compile"):
        render("some.composite", tmp_path, executor="local")


@pytest.mark.skipif(shutil.which("nextflow") is None, reason="nextflow binary not on PATH")
def test_the_compile_probe_leaves_no_session_behind(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """The probe must not write `.nextflow/` into the render directory.

    Nextflow appends every run to `.nextflow/history` in its LAUNCH dir, and a
    bare `-resume` resumes the LAST entry. When this probe ran in `outdir` it
    appended itself after the real campaign, so `-resume` adopted the PROBE's
    session id -- which seeds every task hash -- and a completed 262 MB ParCa
    was re-run. This is the regression test for that.
    """
    _install_fake_pbg(monkeypatch, lambda *a, **k: {"returncode": 0})
    (tmp_path / "main.nf").write_text('process x {\n    script:\n    """\n    echo hi\n    """\n}\nworkflow { x() }\n')
    (tmp_path / "nextflow.config").write_text("profiles { local { process { executor='local' } } }\n")
    monkeypatch.setattr(
        "viva_api.compose.render_nf._load_run_pbg",
        lambda: (
            lambda: object(),
            lambda _i, _c, _o, core: ({"state": {}}, core),
            lambda: object(),
        ),
    )
    from viva_api.compose.render_nf import render

    render("some.composite", tmp_path, executor="local")
    assert not (tmp_path / ".nextflow").exists(), (
        "the probe polluted the render dir's session; a later -resume will adopt ITS session id"
    )
    assert not (tmp_path / ".nextflow.log").exists()
