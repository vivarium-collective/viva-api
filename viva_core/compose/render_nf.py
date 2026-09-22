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
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from viva_core.compose.run_pbg import CompositeSpec, Core  # noqa: F401 -- only the aliases below

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
    _assert_compiles(outdir)


def _assert_compiles(outdir: Path) -> None:
    """Ask Nextflow whether the file it will be handed actually parses.

    The presence check above is not enough, and that is not hypothetical: a
    render once produced the right sub-workflow structure, the right staged
    configs and exit 0, while emitting a `script:` block containing Groovy
    SOURCE rather than a string. `main.nf` contained "process ", so every guard
    passed; `nextflow run` then failed to compile the whole file. Only running
    the parser catches that class, and the head image already has the binary.

    Uses ``-preview``, which compiles the script and builds the DAG while
    executing NO processes -- verified: it returns 1 with "Script compilation
    error" on a bad file, and 0 with zero ``executor >`` lines on a good one.
    (``nextflow config`` would not do: it parses nextflow.config only and never
    looks at main.nf.)

    Always ``-profile local``, whatever the render targets. Every profile is
    emitted, so it always resolves, and it removes any possibility of a
    validation step touching AWS.

    What it does NOT catch: a script that compiles but whose Groovy
    interpolation fails when the task materialises -- ``${VAR:-default}`` in a
    ``script:`` block, say. Those surface only on execution. This closes the
    compile-time class, which is the one that reached production.

    Skipped when `nextflow` is absent -- the plain task image has no JVM, and
    render-only callers are legitimate.
    """
    import shutil
    import subprocess
    import tempfile

    nextflow = shutil.which("nextflow")
    if nextflow is None:
        return
    # Run from a THROWAWAY cwd, never from `outdir`.
    #
    # Nextflow writes `.nextflow/history` into its LAUNCH directory, and a bare
    # `-resume` resumes the LAST entry there. Launching this probe in `outdir`
    # appended the probe's own run -- with its own session id -- after the real
    # campaign's. Nextflow seeds every task hash with `session.uniqueId`, so the
    # next `-resume` adopted the PROBE's session, every hash changed, and a
    # completed 262 MB ParCa was re-run. Measured: the saved history's last row
    # was `nextflow run main.nf -profile local -preview`.
    #
    # `projectDir` follows the SCRIPT's location, not the cwd, so an absolute
    # main.nf keeps `file("${projectDir}/…config.json")` resolving while the
    # session artefacts land somewhere we throw away.
    with tempfile.TemporaryDirectory(prefix="nf-preview-") as probe_cwd:
        probe = subprocess.run(  # noqa: S603  (fixed argv, resolved binary)
            [nextflow, "run", str((outdir / "main.nf").resolve()), "-profile", "local", "-preview"],
            cwd=probe_cwd,
            capture_output=True,
            text=True,
            # Nextflow's banner is UTF-8; under a C/POSIX locale -- which a
            # container very often has -- `text=True` decodes as ascii and raises
            # UnicodeDecodeError, turning a passing render into a crash.
            encoding="utf-8",
            errors="replace",
        )
    if probe.returncode != 0:
        raise SystemExit(f"render_nf: the rendered workflow does not compile.\n{(probe.stdout + probe.stderr)[-2000:]}")


ResolveDocument = Callable[
    [str | None, str | None, "Mapping[str, object] | None", "Core | None"], "tuple[dict[str, object], Core | None]"
]


def _load_run_pbg() -> tuple[Callable[[], Core], ResolveDocument, Callable[[], Core | None]]:
    """Import ``run_pbg``'s resolver, in-process OR as a staged sibling script.

    This module runs in two places, and only one of them has ``viva_api``:

    * in the api pod, as ``viva_api.compose.render_nf`` -- the package import works;
    * **staged into the SIMULATOR image** as a bare ``/tmp/render_nf.py``, where
      ``viva_api`` is not installed and never will be.

    ``run_pbg`` is deliberately stdlib-only at module scope for exactly this
    reason, and is staged beside this file. So fall back to importing it as a
    top-level module from this file's own directory.

    The package import is tried FIRST so that in-process callers get the same
    module object the rest of the app uses, rather than a second copy under a
    different name.
    """
    try:
        from viva_core.compose.run_pbg import _build_core, _resolve_document, _workspace_core
    except ModuleNotFoundError:
        here = str(Path(__file__).resolve().parent)
        if here not in sys.path:
            sys.path.insert(0, here)
        try:
            # Resolvable only in the STAGED layout, where run_pbg.py sits beside
            # this file; mypy cannot see a module that exists only at runtime.
            from run_pbg import (  # type: ignore[no-redef,import-not-found]
                _build_core,
                _resolve_document,
                _workspace_core,
            )
        except ModuleNotFoundError as exc:  # pragma: no cover - staging bug
            raise SystemExit(
                f"render_nf: could not import run_pbg. It is neither installed as "
                f"viva_core.compose.run_pbg nor staged beside {__file__}. The dispatcher "
                f"must stage BOTH scripts; see stage_render_nf."
            ) from exc
    return _build_core, _resolve_document, _workspace_core


def render(
    composite_id: str,
    outdir: Path,
    *,
    overrides: Mapping[str, object] | None = None,
    executor: str = "local",
    nf_params: Mapping[str, object] | None = None,
    resources: Mapping[str, Mapping[str, object]] | None = None,
    launch: bool = False,
    resume: bool = False,
    work_dir: str | None = None,
    report: str | None = None,
    trace: str | None = None,
    weblog_url: str | None = None,
    nextflow_args: list[str] | None = None,
) -> dict[str, object]:
    """Build the document, render it, and (optionally) run ``nextflow``.

    ``nf_params`` are Nextflow *config* params (queue, container image, region,
    work dir), not to be confused with ``overrides``, which are the composite
    generator's parameters. The ``awsbatch`` profile is built entirely out of
    them; process-bigraph raises if the ones it cannot invent are missing,
    rather than rendering ``queue = null`` into a config Nextflow accepts.
    """
    from process_bigraph import Composite
    from process_bigraph.nextflow_deploy import deploy

    _build_core, _resolve_document, _workspace_core = _load_run_pbg()

    # EXACTLY run_pbg's own selection (run_pbg.py:688). The generic core registers
    # only process-bigraph's base types plus the emitter links; a workspace's own
    # builder registers much more -- v2ecoli's registers ECOLI_TYPES and several
    # process/step links. Addresses resolve dynamically, but registered TYPES do
    # not, so building against the generic core fails on a document that uses
    # them: `no link found at address: {'protocol': 'local', 'data': 'composite'}`,
    # which is a nested Composite -- i.e. every sub-workflow this path exists to
    # emit.
    #
    # Tested against None explicitly rather than `or`: a Core is a registry-ish
    # object that may define __bool__/__len__, and `or` would silently discard a
    # valid-but-empty one. Same reasoning as run_pbg's comment there.
    core = _workspace_core()
    if core is None:
        core = _build_core()
    document, core = _resolve_document(None, composite_id, overrides or {}, core)
    composite = Composite(document, core=core)

    outdir.mkdir(parents=True, exist_ok=True)
    result = deploy(
        composite,
        outdir=str(outdir),
        executor=executor,
        params=nf_params,
        resources=resources,
        launch=launch,
        work_dir=work_dir,
        resume=resume,
        report=report,
        trace=trace,
        weblog_url=weblog_url,
        nextflow_args=nextflow_args,
    )
    _assert_rendered(outdir)

    main_nf = (outdir / "main.nf").read_text()
    summary: dict[str, object] = {
        "composite_id": composite_id,
        "outdir": str(outdir),
        "executor": executor,
        "launched": launch,
        "process_blocks": main_nf.count("process "),
        "subworkflows": main_nf.count("workflow ") - 1,  # minus the entry workflow
        "staged_configs": sorted(p.name for p in outdir.glob("*.config.json")),
        "nf_params": sorted(nf_params or {}),
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
    parser.add_argument(
        "--nf-params",
        default=None,
        help="JSON object of Nextflow config params (queue, container_image, aws_region, ...). "
        "Distinct from --overrides, which parameterizes the composite generator.",
    )
    parser.add_argument("--resources", default=None, help="JSON object of per-label {cpus, memory, time}.")
    parser.add_argument("--launch", action="store_true", help="Actually run `nextflow run` (needs the binary).")
    parser.add_argument("--resume", action="store_true", help="Pass -resume; reuses cached successful tasks.")
    parser.add_argument("--work-dir", default=None)
    parser.add_argument("--report", default=None)
    parser.add_argument("--trace", default=None, help="Write a trace CSV; a reused task shows CACHED there.")
    parser.add_argument("--weblog-url", default=None)
    parser.add_argument(
        "--nextflow-args",
        default=None,
        help="JSON array appended to the `nextflow run` command verbatim, e.g. "
        "'[\"-dump-hashes\"]'. A LIST, never a string: a string would have to be "
        "shell-split, and quoting is exactly where that goes wrong silently.",
    )
    args = parser.parse_args(argv)

    overrides = json.loads(args.overrides) if args.overrides else None
    summary = render(
        args.composite_id,
        Path(args.outdir),
        overrides=overrides,
        executor=args.executor,
        nf_params=json.loads(args.nf_params) if args.nf_params else None,
        resources=json.loads(args.resources) if args.resources else None,
        launch=args.launch or _env_truthy(os.environ.get("NF_LAUNCH")),
        resume=args.resume,
        work_dir=args.work_dir,
        report=args.report,
        trace=args.trace,
        weblog_url=args.weblog_url,
        nextflow_args=json.loads(args.nextflow_args) if args.nextflow_args else None,
    )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
