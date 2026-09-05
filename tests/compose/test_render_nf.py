"""render_nf: compile a registered composite into a Nextflow workflow.

The guard is the point. A document whose nodes are all unrenderable produces a
main.nf that Nextflow accepts and that does nothing — a green run with no work in
it, which is the exact failure family this pipeline keeps paying for.
"""

from __future__ import annotations

import json
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
        patch("viva_api.compose.run_pbg._build_core", return_value=object()),
        patch("viva_api.compose.run_pbg._resolve_document", return_value=({"state": {}}, object())),
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
