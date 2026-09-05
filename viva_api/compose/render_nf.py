"""Render a registered composite into a Nextflow workflow, and optionally launch it.

Sibling of ``run_pbg.py``, staged into the simulator image the same way and run
there. Where ``run_pbg`` *executes* a composite in-process, this one compiles it
into ``main.nf`` + ``nextflow.config`` + one staged config per task node, and hands
execution to Nextflow.

**It deliberately reuses ``run_pbg._resolve_document``** rather than re-deriving
composite resolution. That function already carries hard-won behaviour: it walks
``composite_spec`` (retrying after ``discover_specs()``, because a generator's
decorator only fires on import), and it applies ``core_extensions`` returning the
NEW core — an extension may build a fresh core rather than mutate the one it was
given, and using the original silently builds against an unextended one.

Why launching is opt-in and defaults to render-only:

* Rendering is cheap, deterministic, and diffable. It answers "does this document
  compile to a sane workflow" without provisioning anything.
* Launching needs a ``nextflow`` binary, which exists only in the ``-submit`` head
  image (the plain task image has no JVM).

``--executor local`` inside a Batch container job is the intended first check:
it separates *does render+launch work in our real image* from *does the awsbatch
executor work*, which is the whole reason Phase 3 precedes Phase 4 in
``docs/plan-nextflow-dispatch.md``.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any

# Mirrors run_pbg's RESULTS_DIR convention: an env-var override, defaulting under
# the system temp dir rather than a hardcoded /tmp path.
DEFAULT_OUTDIR = Path(os.environ.get("NF_RENDER_DIR") or Path(tempfile.gettempdir()) / "nf-render")


def _env_truthy(value: str | None) -> bool:
    if not value:
        return False
    return value.strip().lower() not in ("0", "false", "no", "off", "")


def _assert_rendered(outdir: Path) -> None:
    """A render that produced no workflow is a failure, whatever the exit code.

    Same reasoning as ``run_pbg``'s PBG_REQUIRE_OUTPUT guard: the expensive
    mistake in this pipeline has consistently been reporting success over an
    artifact nobody checked. An empty or process-less ``main.nf`` means the
    document had nothing renderable, which is never what a caller wanted.
    """
    main_nf = outdir / "main.nf"
    if not main_nf.is_file():
        raise SystemExit(f"render_nf: no main.nf was written under {outdir}")
    text = main_nf.read_text()
    if "process " not in text:
        raise SystemExit(
            f"render_nf: {main_nf} contains no `process` block — the document rendered to an "
            f"empty workflow. A composite whose nodes are all unrenderable produces a file that "
            f"Nextflow accepts and that does nothing."
        )


def render(
    composite_id: str,
    outdir: Path,
    *,
    overrides: dict[str, Any] | None = None,
    executor: str = "local",
    launch: bool = False,
    resume: bool = False,
    work_dir: str | None = None,
    report: str | None = None,
    trace: str | None = None,
    weblog_url: str | None = None,
) -> dict[str, Any]:
    """Build the document, render it, and (optionally) run ``nextflow``."""
    from process_bigraph import Composite
    from process_bigraph.nextflow_deploy import deploy

    from viva_api.compose.run_pbg import _build_core, _resolve_document

    core = _build_core()
    document, core = _resolve_document(None, composite_id, overrides or {}, core)
    composite = Composite(document, core=core)

    outdir.mkdir(parents=True, exist_ok=True)
    result = deploy(
        composite,
        outdir=str(outdir),
        executor=executor,
        launch=launch,
        work_dir=work_dir,
        resume=resume,
        report=report,
        trace=trace,
        weblog_url=weblog_url,
    )
    _assert_rendered(outdir)

    main_nf = (outdir / "main.nf").read_text()
    summary = {
        "composite_id": composite_id,
        "outdir": str(outdir),
        "executor": executor,
        "launched": launch,
        "process_blocks": main_nf.count("process "),
        "subworkflows": main_nf.count("workflow ") - 1,  # minus the entry workflow
        "staged_configs": sorted(p.name for p in outdir.glob("*.config.json")),
        "returncode": result.get("returncode"),
    }
    (outdir / "render_summary.json").write_text(json.dumps(summary, indent=2))
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--composite-id", required=True, help="Registered composite to render.")
    parser.add_argument("--outdir", default=str(DEFAULT_OUTDIR))
    parser.add_argument("--overrides", default=None, help="JSON object of generator parameter overrides.")
    parser.add_argument("--executor", default="local", help="Nextflow profile: local, slurm, awsbatch, google-batch.")
    parser.add_argument("--launch", action="store_true", help="Actually run `nextflow run` (needs the binary).")
    parser.add_argument("--resume", action="store_true", help="Pass -resume; reuses cached successful tasks.")
    parser.add_argument("--work-dir", default=None)
    parser.add_argument("--report", default=None)
    parser.add_argument("--trace", default=None, help="Write a trace CSV; a reused task shows CACHED there.")
    parser.add_argument("--weblog-url", default=None)
    args = parser.parse_args(argv)

    overrides = json.loads(args.overrides) if args.overrides else None
    summary = render(
        args.composite_id,
        Path(args.outdir),
        overrides=overrides,
        executor=args.executor,
        launch=args.launch or _env_truthy(os.environ.get("NF_LAUNCH")),
        resume=args.resume,
        work_dir=args.work_dir,
        report=args.report,
        trace=args.trace,
        weblog_url=args.weblog_url,
    )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
