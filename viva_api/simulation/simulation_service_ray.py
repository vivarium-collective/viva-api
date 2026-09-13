"""AWS Batch multi-node-parallel (MNP) Ray implementation of SimulationService.

The v2ecoli whole-cell sim runs distributed on a *transient* Ray cluster: one
Batch MNP job gang-schedules N nodes, the Ray-on-Batch entrypoint
(``ray-batch-entrypoint.sh``, bundled in the v2ecoli image) forms the Ray
cluster, stages a ParCa cache from S3, exports ``RAY_ADDRESS``, and runs
``RAY_JOB_CMD`` (the v2ecoli ensemble) on the head — no Nextflow. This service
submits those MNP jobs.

Data flow (no shared filesystem):
  - ParCa runs first as its own 1-node MNP job; its cache is captured to a
    deterministic S3 URI (``RAY_OUT_S3``).
  - The simulation MNP job ``dependsOn`` the ParCa job (Batch gates it until
    ParCa SUCCEEDED), stages that cache (``RAY_STAGE_S3``), runs the ensemble,
    and captures the zarr/summary outputs to S3 (``RAY_OUT_S3``).
  - For the multi-generation batch_baseline sweep, an ANALYSIS job ``dependsOn``
    the simulation job and runs the ported cd1_*/ptools_* analyses over the
    landed S3 sweep. The whole pipeline is therefore one Batch dependency DAG
    (parca -> sim -> analysis); nothing external has to notice a completion and
    react to it. See ``_analysis_command`` for why this is a third DAG node and
    not the composite's own inline flush.

The image is the **workload-owned**, self-contained ``v2ecoli:<sha>`` (bundles
the AWS CLI + the Ray entrypoint), built by ``submit_build_image_job`` via a DooD
Batch job — symmetric with how ``SimulationServiceK8s`` builds ``vecoli:{commit}``.
Each run uses its simulator's TRUE commit image: since Batch MNP can't override the
image per submission, we derive a per-commit MNP job-def revision from the sms-cdk
base (cloning its node properties, swapping the image to ``v2ecoli:<commit>``).
"""

import asyncio
import copy
import functools
import importlib.resources as _res
import json
import logging
import math
import random
import re
import shlex
import string
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, override

import boto3
from botocore.config import Config
from pydantic import BaseModel

from viva_api.common import analysis_dag
from viva_api.common.dispatch_validation import resolve_task_env, task_env_as_batch_list, validate_nextflow_dispatch
from viva_api.common.events_env import with_events_env
from viva_api.common.hpc.job_service import JobStatusInfo
from viva_api.common.hpc.k8s_job_service import K8sJobService
from viva_api.common.hpc.local_task_service import LocalTaskService
from viva_api.common.models import JobBackend, JobId, JobStatus
from viva_api.common.simulator_defaults import DEFAULT_BRANCH, DEFAULT_REPO, RepoUrl
from viva_api.common.storage import data_layout
from viva_api.common.storage.file_paths import S3FilePath
from viva_api.config import get_settings
from viva_api.simulation import batch_build
from viva_api.simulation.database_service import DatabaseService
from viva_api.simulation.github_repo import (
    fetch_config_template,
    fetch_latest_commit_hash,
    fetch_repo_discovery,
)
from viva_api.simulation.models import (
    CompositeEngine,
    HpcRun,
    JobType,
    ParcaDataset,
    RepoDiscovery,
    Simulation,
    SimulatorVersion,
    TaskDTO,
    TaskLogsDTO,
    TaskRunRequest,
    VecoliSource,
)
from viva_api.simulation.simulation_service import SimulationService
from viva_api.simulation.tables_orm import AnalysisStatusDB, TaskStatusDB

logger = logging.getLogger(__name__)

# The generic runner every Ray-Batch job (ensemble or compose) executes. Read once as a
# resource (same source viva_api.compose.simulation_service_ray stages for compose jobs) so
# the multi-generation batch path below dispatches through the identical mechanism instead
# of a v2ecoli-specific CLI script — see backlog items 26/27.
_RUNNER_SRC = (_res.files("viva_api.compose") / "run_pbg.py").read_text()
# The Nextflow compiler, staged the same way and for the same reason (Batch caps a
# container override command at 8192 bytes).
_RENDER_NF_SRC = (_res.files("viva_api.compose") / "render_nf.py").read_text()

# Registered composite id (process_bigraph.composite_spec) for the multi-generation
# batch orchestrator, and the workspace core-builder that resolves its registered
# types (e.g. "inplace_dict"). Both are inherent facts about what THIS endpoint
# dispatches — this file already hardcodes v2ecoli-specific paths (V2ECOLI_DIR,
# PARCA_CACHE_DIR below); what item 27 removes is the bespoke EXECUTION MECHANISM (a
# CLI script), not this identity.
#
# The id is `f"{fn.__module__}.{name}"` (process_bigraph.composite_spec's own
# registration scheme, mirrored by pbg_superpowers.composite_generator). Two real
# pilot dispatches (2026-08-06) failed chasing wrong values for this constant before
# it was verified directly against the DEPLOYED sms-ecoli image (commit e38f742,
# `git show`/`git grep` against that exact commit — never the local v2ecoli
# checkout, a separate, structurally-diverged repo that is NOT a mirror of what's
# actually in this simulator image). At that commit the real module was
# v2ecoli/composites/batch_baseline.py (decorated function `batch_baseline`,
# name="batch_baseline") — this constant was correctly set to
# "v2ecoli.composites.batch_baseline.batch_baseline" and worked through build 62
# (commit 8d50ff0, item 1's real 1000x10 campaign).
#
# UPDATED 2026-08-16 (backlog item 55): sms-ecoli PR #56 (the sync that also
# carried item 52's wall-time fix) finally synced a v2ecoli upstream refactor that
# had been sitting unsynced since 2026-07-25 (v2ecoli #373, "Unify composites into
# baseline: knockouts + media + batch (n_seeds)") — it deleted
# composites/batch_baseline.py and folded its batch/lineage behavior into
# composites/ecoli_baseline.py's `baseline()` function (n_seeds/n_generations > 1
# switches it into what used to be the standalone batch_baseline composite).
# v2ecoli/composites/__init__.py deliberately registers NO legacy-id alias for the
# old name ("a stale `baseline` id resolving silently would only hide a missed
# reference") — so the old id now fails LOUDLY (confirmed via a real dispatch,
# sim 152, 2026-08-16: "no composite registered as
# 'v2ecoli.composites.batch_baseline.batch_baseline'"), exactly as its authors
# intended, rather than silently drifting. Re-verified the SAME way the 2026-08-06
# incident above did — `git show`/`git grep` directly against the real deployed
# commit (sms-ecoli c44b69a, build 63), never the separately-diverged local v2ecoli
# checkout. `baseline()`'s real signature (checked directly) is a strict superset
# of the old `batch_baseline` params EXCEPT one rename: `base_seed` -> `seed`.
V2ECOLI_BATCH_BASELINE_COMPOSITE_ID = "v2ecoli.composites.ecoli_baseline.ecoli_baseline"
V2ECOLI_CORE_BUILDER = "v2ecoli.core:build_core"

# Absolute paths inside the v2ecoli Ray image (WORKDIR=/app/v2ecoli). The
# entrypoint runs RAY_JOB_CMD on the head; v2ecoli reads the cache from
# CACHE_DIR and writes the ensemble outputs under OUT_DIR.
V2ECOLI_DIR = "/app/v2ecoli"
PARCA_CACHE_DIR = f"{V2ECOLI_DIR}/out/cache"
PARCA_SIMDATA_DIR = f"{V2ECOLI_DIR}/out/sim_data"
# Backlog item 105: scripts/build_new_gene_cache.py's own output dir, mirroring
# its DEFAULT_CACHE_DIR ("out/cache-new-genes") -- see submit_new_gene_cache_job.
NEW_GENE_INDUCED_CACHE_DIR = f"{V2ECOLI_DIR}/out/cache-new-genes"
# Backlog item 451: scripts/build_variant_cache.py's own output dir, mirroring
# its DEFAULT_CACHE_DIR ("out/cache-variant") -- see submit_variant_cache_job.
VARIANT_CACHE_DIR = f"{V2ECOLI_DIR}/out/cache-variant"
SIM_OUT_DIR = f"{V2ECOLI_DIR}/.pbg/runs/phase0-xarray"
# ecoli_baseline.baseline()'s injection branch (taken whenever injected_processes
# is passed) does `from scripts._compare.inject import (...)` -- a bare absolute
# import that only resolves when V2ECOLI_DIR (which DOES contain scripts/, copied
# in by sms-ecoli's Dockerfile `COPY . .`) is on sys.path. Every run_pbg.py
# invocation below runs it via an absolute /tmp path, which makes CPython put
# /tmp on sys.path[0] instead of the cwd -- the `cd {V2ECOLI_DIR}` alone doesn't
# fix this; PYTHONPATH does. Found live 2026-09-01 (backlog item 93): a real
# chain-dispatch run with a non-empty injected_processes failed
# ModuleNotFoundError('scripts') despite the cd already being correct.
# PBG_REQUIRE_OUTPUT=1: on the CD2 Ray/baseline dispatch a run that produced no
# emitted store is always a failure, so run_pbg.py must exit non-zero instead of
# reporting success on the final_state.json fallback alone (audit §2.4 / P0-3).
# PBG_MIN_GLOBAL_TIME: the EFFECT half of the output guard (viva-api #395 / #375 §3e).
# PBG_REQUIRE_OUTPUT proves an emitted store EXISTS; this proves the generation
# actually RAN. A swap campaign that collapses to one tick (#375 §3d/§3e; the
# item-103 test measured "global_time never exceeded 1.0 across 10 chained
# generations") still writes a non-empty 1.pq and passes the presence check — and
# that is exactly the mode #387 can expose (a nested swap that used to be dropped
# now reaches the run). A real whole-cell generation advances hundreds-to-thousands
# of seconds of global_time, so a floor well above one tick (1.0) and far below one
# generation catches the collapse with no false-fail. Conservative and tunable; only
# set on this CD2 baseline/lineage path, NOT the generic compose path (which can run
# legitimately short composites).
PBG_MIN_GLOBAL_TIME = 10.0
PBG_RUNNER_ENV = (
    f"PBG_RESULTS_DIR={SIM_OUT_DIR} PBG_CORE_BUILDER={V2ECOLI_CORE_BUILDER}"
    f" PYTHONPATH={V2ECOLI_DIR} PBG_REQUIRE_OUTPUT=1 PBG_MIN_GLOBAL_TIME={PBG_MIN_GLOBAL_TIME}"
)
# The analysis DAG node writes its outputs straight to S3 (see _analysis_command),
# so this local dir normally never exists and the entrypoint's RAY_OUT_DIR sync is a
# documented no-op ("no <dir>; nothing to upload"). It is still declared so anything
# the analysis does drop locally lands under the run's own S3 prefix.
ANALYSIS_OUT_DIR = f"{V2ECOLI_DIR}/.pbg/runs/analysis"
# In-region task compute (viva-api#631 slice 1): an arbitrary repo-path script
# run through the SAME standalone container path as ParCa/the analysis DAG
# node. Mirrors ANALYSIS_OUT_DIR's own rationale — a script that writes only to
# S3 leaves this empty (a documented sync no-op); declared so anything a
# script drops locally still lands under the run's own S3 prefix.
TASK_OUT_DIR = f"{V2ECOLI_DIR}/.pbg/runs/task"
# Where the container entrypoint syncs an uploaded task script (viva-api#631
# slice 2): submit_uploaded_task stages the script to an S3 prefix and passes it
# as CONTAINER_STAGE_S3, which batch-container-entrypoint.sh `aws s3 sync`s into
# CONTAINER_STAGE_DIR before running the command -- so the job_cmd runs
# `python <TASK_STAGE_DIR>/<script>`.
TASK_STAGE_DIR = f"{V2ECOLI_DIR}/.pbg/task_script"


def _safe_task_name(raw: str) -> str:
    """Reduce a task name to the Batch jobName charset ([A-Za-z0-9_-], <=128) so
    it can't fail submit_job. A user-provided name already passed
    TaskRunRequest's validator; this also sanitizes the auto-derived script stem
    (which can carry dots and other characters)."""
    cleaned = re.sub(r"[^A-Za-z0-9_-]", "-", raw)[:128].strip("-")
    return cleaned or "task"


# Where the head writes the entrypoint's metrics report (uploaded as report.json).
REPORT_PATH = "/tmp/report.json"  # noqa: S108

# The analysis scales a v2ecoli ``analysis_options`` map can carry. Everything else
# in that (extra="allow") model — ``cpus``, ``memory_gb``, vEcoli-Nextflow-only keys —
# is not a scale and must not be forwarded as one.
ANALYSIS_SCALES = ("single", "multidaughter", "multigeneration", "multiseed", "multivariant")
# The composite's own "every analysis this batch's shape has the cells for" keyword
# (v2ecoli.steps.batch_baseline_runner.build_analysis_options). Used when the caller
# named no modules: sms-api runs outside the model image and has no ANALYSIS_REGISTRY
# to enumerate, so it asks the image to resolve the set with its own resolver rather
# than carrying a second, drift-prone copy of the list.
APPLICABLE_ANALYSES = "applicable"

# --- Analysis memory-class routing (viva-api#625 / v2ecoli#788) --------------
#
# An analysis container runs on one of two Batch instance classes: "standard"
# (the 60 GB box the analysis job def defaults to, which OOMed on CD2 --
# v2ecoli#786) and "large" (a 200 GB r7i queue). The class is picked HERE, at
# submission time, because the queue is chosen before the container runs -- so
# sms-api cannot ask the model image, where v2ecoli's own analysis_memory_class
# lives. This is a deliberate small local copy of that sizing (sms-api does not
# import v2ecoli at runtime; the analysis SCALES it reads are already carried in
# analysis_options). The constants below MUST stay in step with
# v2ecoli.workflow.analysis_runner: a per-lineage multiseed/multigeneration group
# peaks ~8 GB/generation (~78 GB over 10 generations, #786), past the 60 GB
# standard box; single/multidaughter read one cell and stay standard.
#
# DRIFT TRAP: v2ecoli's copy also honors an analysis class that DECLARES
# `memory_class = "large"` (analysis_runner._declared_memory_class); this copy
# does NOT -- it derives purely from scale x generations, because sms-api has no
# ANALYSIS_REGISTRY to read a class attribute from. So declaring memory_class on
# an analysis currently has NO effect on which queue the API picks. Latent today
# (nothing declares it). If a module ever needs to force "large" regardless of
# scale, thread the declared class through analysis_options / task_env so this
# function can see it; until then a declaration would route large in v2ecoli's
# in-image reasoning but STANDARD here -- do not let that gap go silent.
_ANALYSIS_STANDARD_INSTANCE_GB = 60
_ANALYSIS_GB_PER_GENERATION = 8.0
_ANALYSIS_MULTI_CELL_SCALES = frozenset({"multigeneration", "multiseed"})


def analysis_memory_class(
    analysis_options: dict[str, Any] | str | None,
    *,
    n_seeds: int | None = None,
    n_generations: int | None = None,
) -> str:
    """The Batch instance memory class an analysis submission needs: ``"standard"``
    or ``"large"``.

    Derived from the scales named in ``analysis_options`` (the ``{scale: {name:
    params}}`` shape, e.g. from ``analysis_modules_for``) and the sweep's
    ``n_generations``. Generations drive the per-lineage peak; ``n_seeds`` is
    accepted for interface parity with v2ecoli's function but the chunked readers
    make it a non-factor. Any multiseed/multigeneration analysis over enough
    generations routes the whole job to the large instance; everything else stays
    standard. Mirrors ``v2ecoli.workflow.analysis_runner.analysis_memory_class``
    -- see the note above on why sms-api keeps a local copy."""
    if not n_generations or not isinstance(analysis_options, dict):
        # No generation count, or the "applicable" keyword / any non-scale-map
        # (the image resolves the set itself) -- nothing to size on here.
        return "standard"
    over_standard = _ANALYSIS_GB_PER_GENERATION * int(n_generations) > _ANALYSIS_STANDARD_INSTANCE_GB
    for scale, entries in analysis_options.items():
        if scale in _ANALYSIS_MULTI_CELL_SCALES and isinstance(entries, dict) and entries and over_standard:
            return "large"
    return "standard"


# ── Chain-dispatch campaign submission (backlog item 33) ────────────────────
#
# AWS Batch's SubmitJob is capped at 50 TPS per account, fixed -- not
# adjustable via a quota increase (AWS Batch service quotas, verified this
# session). A canonical 1000-seed x 10-generation campaign submits N*G=10,000
# individual per-seed-per-generation jobs upfront (see
# ``submit_chain_dispatch_job``), so that loop must stay safely under the cap.
_SUBMIT_JOB_SAFE_RATE = 40.0  # jobs/sec; headroom below the 50 TPS account cap
#                               for other concurrent Batch traffic in the same
#                               account (ParCa/analysis jobs, other campaigns).
_SUBMIT_JOB_MAX_ATTEMPTS = 5  # botocore "standard" retry attempts per submit_job
#                               call, for whatever transient/throttling errors
#                               proactive pacing alone doesn't fully prevent.
# (Per-generation-job retry no longer needs a manual override here as of item 71
# Phase 4: chain-dispatch generations now submit as container-type jobs, whose
# job definition already bakes in retryStrategy.attempts=2 -- see sms-cdk's
# RayContainerJobDef -- unlike the MNP job definition this superseded, which
# declared none of its own.)
# AWS Batch DescribeJobs accepts at most 100 job ids per call (verified against
# the real API model this session) -- the analysis-fan-in poller must chunk a
# campaign's up-to-1000 tracked job ids into batches this size.
_DESCRIBE_JOBS_MAX_BATCH = 100


def _rand_suffix() -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=6))


class _SubmitJobPacer:
    """Proactive client-side pacer for AWS Batch SubmitJob.

    Caps outbound ``submit_job`` calls to ``max_per_second``, computed from
    REAL elapsed wall-clock time since the previous call (not a fixed
    per-call sleep, which either over-throttles once call latency is added on
    top, or under-throttles if the guessed interval is even slightly off).
    Deliberately proactive rather than reactive: botocore's own "adaptive"
    retry mode only starts throttling client-side AFTER it has already
    observed a real throttling response, so pacing every call up front is
    what keeps a fresh several-thousand-call burst from front-loading
    avoidable 429s in the first place. This pacer and the "standard" retry
    mode configured on the submitting client (see
    ``submit_chain_dispatch_job``) are complementary, not redundant: this
    caps the steady-state rate; retry-on-throttle is the backstop for
    whatever pacing alone doesn't prevent (concurrent campaigns, other Batch
    traffic in the same account -- the 50 TPS cap is account-wide, not
    per-campaign).
    """

    def __init__(self, max_per_second: float = _SUBMIT_JOB_SAFE_RATE) -> None:
        self._min_interval = 1.0 / max_per_second
        self._last_call_at: float | None = None

    async def wait(self) -> None:
        now = time.monotonic()
        if self._last_call_at is not None:
            deficit = self._min_interval - (now - self._last_call_at)
            if deficit > 0:
                await asyncio.sleep(deficit)
                now = time.monotonic()
        self._last_call_at = now


def analysis_modules_for(config: Any) -> dict[str, dict[str, Any]] | str:
    """The analyses the analysis DAG node should run for this simulation.

    Reads the simulation's OWN ``config.analysis_options`` — the field the run
    endpoint already populates from the caller's ``--analysis-options`` (and that
    the workbench already fills from a study's ``spec.analyses``), and which this
    backend previously ignored entirely, so a remote dispatch's configured
    analyses never ran.

    Only real scale keys are forwarded, and only non-empty ones: the endpoint's
    own fallback default is ``{"multiseed": {}}`` — "no modules named", not "run
    nothing" — which resolves to the ``applicable`` keyword like any other
    unset case.
    """
    options: Any = getattr(config, "analysis_options", None)
    raw: dict[str, Any] = options.model_dump() if isinstance(options, BaseModel) else dict(options or {})
    modules = {
        scale: dict(entries)
        for scale, entries in raw.items()
        if scale in ANALYSIS_SCALES and isinstance(entries, dict) and entries
    }
    return modules or APPLICABLE_ANALYSES


def _is_upstream_vecoli(composite: CompositeEngine | None) -> bool:
    """The pristine upstream-vEcoli engine (``--composite vecoli``).

    The single source of truth for the routing question "does this run need the
    separate upstream ParCa cache + ``--vecoli-source`` flag?" — used both to
    select the ParCa cache/command in ``submit_ecoli_simulation_job`` and to gate
    the ``--vecoli-source`` arg in ``_sim_command``.
    """
    return composite == "vecoli"


def strain_from_config(config: Any) -> tuple[str | None, str | None]:
    """Return ``(new_genes, bundle_overrides)`` — the strain a run requested — from
    a config's ``parca_options``, or ``(None, None)`` for a wild-type/unset build.

    Same read the ParCa command uses (``getattr(config.parca_options, ...)``);
    threaded to the entrypoint as ``*_EXPECT_NEW_GENES`` / ``*_EXPECT_BUNDLE_OVERRIDES``
    so a wrong-strain staged cache is rejected (sms-ecoli#210 / #215). ``off``/empty
    is wild-type -> ``None`` (matches build_cache.py's own normalization).
    """
    parca_options = getattr(config, "parca_options", None)

    def _norm(value: Any) -> str | None:
        # bundle_overrides can now be a list (multiple stacked --bundle-overrides
        # flags, see ParcaOptions.bundle_overrides) -- joined with "," for this
        # single EXPECT_* env-var string. No consumer reads *_EXPECT_BUNDLE_
        # OVERRIDES yet (confirmed empirically: no match for that name anywhere
        # in v2ecoli), so this is a viva-api-only convention pending a real
        # verifier, not a wire format anything downstream already expects.
        if isinstance(value, list):
            s: str | None = ",".join(str(v).strip() for v in value if str(v).strip()) or None
        else:
            s = value.strip() if isinstance(value, str) else None
        return None if not s or s == "off" else s

    return (
        _norm(getattr(parca_options, "new_genes", None)),
        _norm(getattr(parca_options, "bundle_overrides", None)),
    )


def injected_processes_from_config(config: Any) -> dict[str, Any] | None:
    """Build ``ecoli_baseline.baseline()``'s own ``injected_processes`` kwarg
    (backlog item 93) from a legacy config's ``swap_processes``/
    ``add_processes``/``exclude_processes`` -- real ``ExperimentRequest``
    fields (``viva_api/simulation/models.py``) that ride through
    ``SimulationConfig`` as extras (``extra="allow"``), so ``getattr`` is the
    correct read whether or not the field was ever declared on the model.

    Returns ``None`` when none of the three are set, so a config with no
    injection intent produces the exact ``overrides`` dict this dispatch path
    already built before this existed -- the byte-for-byte-unchanged
    regression property backlog items 86/88's own ``extra_params`` passthrough
    established for this same class of fix.

    ``fork_repo`` is always ``""``: every caller of this helper dispatches
    through ``ecoli_baseline``, the NATIVE (fork-free) composite --
    ``assert_injection_sourcing`` (v2ecoli's ``composites/ecoli_baseline.py``)
    rejects a non-empty ``fork_repo`` for a native composite outright, and
    ``scripts._compare.inject.resolve_injections`` indexes
    ``injected_processes["fork_repo"]`` directly (not ``.get``), so the key
    must be present even though it is always empty on this path.
    """

    # Two accepted shapes, resolved PER FIELD, not as a whole-block either/or:
    #   (1) FLAT: swap_processes / add_processes / exclude_processes as top-level
    #       config extras (the legacy shape this helper was written for).
    #   (2) NESTED: a whole ``injected_processes`` block passed through as an extra
    #       -- e.g. ``extra_params={"injected_processes": {"swap_processes": ...}}``,
    #       exactly the shape ``run_comparison_ensemble.py --from-vecoli-config``
    #       emits. This is what viva-api#385 hit: the nested block reached the
    #       resolved config, but this helper only read the flat fields, found none,
    #       returned None, and the swap was silently dropped at every downstream hop
    #       (chain-dispatch ran wild-type, reporting success).
    # A nested submit and the config's own flat fields are NOT mutually-exclusive
    # alternatives (viva-api#401): choosing the whole shape once meant a nested
    # submit setting only ``swap_processes`` silently dropped a config's own flat
    # ``add_processes``/``exclude_processes``, even though nothing about the
    # caller's request implied dropping them -- observed live (sim 296): a
    # mecillinam config's own 4 ``add_processes`` vanished under a nested swap
    # that never mentioned them. Each of the three fields is now resolved
    # independently -- the nested value wins when set, else fall back to flat --
    # so a caller sending only ``swap_processes`` keeps the config's own
    # ``add_processes``/``exclude_processes`` rather than losing them.
    def _read(src: Any, key: str, default: Any) -> Any:
        if isinstance(src, dict):
            return src.get(key) or default
        return getattr(src, key, None) or default

    nested = getattr(config, "injected_processes", None)
    nested_has_intent = nested is not None and (
        _read(nested, "swap_processes", None)
        or _read(nested, "add_processes", None)
        or _read(nested, "exclude_processes", None)
    )

    flat_swap = _read(config, "swap_processes", {})
    flat_add = _read(config, "add_processes", [])
    flat_exclude = _read(config, "exclude_processes", [])
    nested_swap = _read(nested, "swap_processes", None)
    nested_add = _read(nested, "add_processes", None)
    nested_exclude = _read(nested, "exclude_processes", None)

    # A real conflict (both sides set the SAME field) is a silent override,
    # not just a merge -- worth a log line so it is at least discoverable,
    # per cplong90's own framing on #401 ("an override worth logging, not one
    # to make silently"), matching this codebase's own repeated fix pattern
    # for silent-success-masking-a-real-difference (items 93/103/111/114).
    for name, nested_val, flat_val in (
        ("swap_processes", nested_swap, flat_swap),
        ("add_processes", nested_add, flat_add),
        ("exclude_processes", nested_exclude, flat_exclude),
    ):
        if nested_val and flat_val:
            logger.warning(
                "injected_processes_from_config: nested %s overrides the config's own flat %s (nested=%r, flat=%r)",
                name,
                name,
                nested_val,
                flat_val,
            )

    swap_processes = nested_swap or flat_swap
    add_processes = nested_add or flat_add
    exclude_processes = nested_exclude or flat_exclude
    if not (swap_processes or add_processes or exclude_processes):
        return None

    # Carry the caller-supplied nested block through rather than rebuilding a
    # fresh dict from just the four keys below (viva-api#392): a nested submit
    # can carry additional real intent -- e.g. `cache_dir`, which a fork-free
    # swap's own resolve_injections() spec-building needs to load the target
    # process's config from the ParCa bundle. Reconstructing only
    # {swap_processes, add_processes, exclude_processes, fork_repo} silently
    # dropped it, so the swapped-in process mounted with an empty config
    # instead of failing loud or running correctly. The four keys are
    # normalized defaults layered ON TOP of the carried-through block, not a
    # replacement for it, so this stays byte-identical for the flat/legacy
    # shape (nothing to carry through there) and for any nested submit that
    # never set the four keys to begin with.
    result: dict[str, Any] = dict(nested) if isinstance(nested, dict) and nested_has_intent else {}
    result.update({
        "swap_processes": swap_processes,
        "add_processes": add_processes,
        "exclude_processes": exclude_processes,
        "fork_repo": "",
    })
    return result


def _thread_injected_processes_into_params(params: dict[str, Any], config: Any) -> None:
    """Thread the config's own injection intent into a multi-node composite's params.

    sms-ecoli#166 (2026-09-09): a config's top-level ``swap_processes`` /
    ``add_processes`` / ``exclude_processes`` (or a nested ``injected_processes``
    block) never reached the multi-node path -- only chain dispatch and the
    single-shot ``_sim_command`` called ``injected_processes_from_config`` -- so
    every ``lineage_ray_batch`` CD2 Run 4 dispatch (both arms) silently ran CLASSIC
    ``ecoli-metabolism`` although its stored config declared the redux swap (787's
    GLP_UNBND traceback is in ``v2ecoli/processes/metabolism.py``; 744's history
    carries the classic-only FBA listeners and none of redux's). The composite
    already accepts ``injected_processes`` as a param (v2ecoli#663).

    Explicit ``params["injected_processes"]`` wins (the same "explicit params win"
    rule every other key follows); a config with no injection intent leaves
    ``params`` byte-for-byte unchanged (the helper returns None). Mutates in place.
    """
    if "injected_processes" in params:
        return
    injected = injected_processes_from_config(config)
    if injected:
        params["injected_processes"] = injected


def _batch_domain_overrides(
    *,
    injected_processes: dict[str, Any] | None = None,
    variants: dict[str, Any] | None = None,
    config_overrides: dict[str, Any] | None = None,
    features: list[Any] | None = None,
    exchange_fluxes: dict[str, Any] | None = None,
    exchange_flux_basis: str | None = None,
) -> dict[str, Any]:
    """The submitted config's DOMAIN fields as ``ecoli_baseline.baseline()``'s own
    batch-mode ``--overrides`` keys (the CD2 native seam).

    v2ecoli #640 threaded ``injected_processes``/``features``/``exchange_fluxes``/
    ``exchange_flux_basis`` through ``_build_batch_document``; ``config_overrides``
    and ``variants`` already existed. Each key is emitted ONLY when non-empty, so a
    config with no injection/variant intent yields ``{}`` and the caller's overrides
    dict is byte-for-byte what this path built before threading was added -- the
    same regression contract as ``injected_processes_from_config`` and the 0.9.79
    chain-dispatch passthrough. ``exchange_flux_basis`` rides only alongside a flux
    map (the composite defaults it to "").
    """
    out: dict[str, Any] = {}
    if injected_processes:
        out["injected_processes"] = injected_processes
    if variants:
        out["variants"] = variants
    if config_overrides:
        out["config_overrides"] = config_overrides
    if features:
        out["features"] = features
    if exchange_fluxes:
        out["exchange_fluxes"] = exchange_fluxes
        if exchange_flux_basis:
            out["exchange_flux_basis"] = exchange_flux_basis
    return out


@dataclass
class ChainCampaignPollResult:
    """A chain-dispatch campaign's analysis-fan-in poll outcome — backlog item 33.

    ``terminal`` means every one of the campaign's tracked final-generation job
    ids (``HpcRun.chain_final_job_ids`` — one per seed, each seed's own LAST
    successfully-submitted generation job) has reached a Batch-terminal state
    (SUCCEEDED or FAILED); a campaign with any tracked job still
    SUBMITTED/PENDING/RUNNABLE/STARTING/RUNNING is NOT terminal and must be
    polled again next interval. A job id that hasn't shown up in ``describe_jobs``
    yet (e.g. brief eventual-consistency lag right after submission) is treated
    as not-yet-terminal, not an error — the caller just polls again.

    Unlike the per-generation-array design this superseded, there is no local
    array position to remap: each tracked id is already a real, independent AWS
    Batch job id (one seed's final chain link), so ``succeeded_job_ids`` /
    ``failed_job_ids`` are real job ids directly, usable as-is.
    """

    terminal: bool
    succeeded_job_ids: list[str] = field(default_factory=list)
    failed_job_ids: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class BatchJobDetail:
    """What ``describe_jobs`` says about one job, for reporting a failure by
    name and reason rather than by id (``get_batch_job_details``)."""

    job_id: str
    job_name: str
    status: JobStatus
    status_reason: str | None = None
    exit_code: int | None = None
    attempts: int = 0

    def describe(self) -> str:
        parts = [self.job_name or self.job_id, self.status.value]
        if self.exit_code is not None:
            parts.append(f"exit {self.exit_code}")
        if self.status_reason:
            parts.append(self.status_reason)
        return ": ".join(parts[:1]) + " (" + ", ".join(parts[1:]) + ")"


def _batch_exit_code(job: dict[str, Any]) -> str | None:
    """The container exit code from an AWS Batch ``describe_jobs`` job object,
    as a string (JobStatusInfo.exit_code is ``str | None``), or None when Batch
    has not reported one yet.

    Batch surfaces it at ``job["container"]["exitCode"]`` for a single-container
    job and at ``job["nodeProperties"]...["container"]["exitCode"]`` for a
    multi-node (MNP) job's main node; the top-level ``container`` key carries the
    main container for both shapes in ``describe_jobs`` output, so read it there.
    Previously hardcoded to None, discarding the real exit status (CD2 audit
    §2.4 / P1-13).
    """
    exit_code = (job.get("container") or {}).get("exitCode")
    return str(exit_code) if exit_code is not None else None


# Per-label resources for the Nextflow dispatch. NOT optional in practice: a
# dispatch that supplies none emits no `withLabel` block, so nf-amazon's
# auto-registered job definition takes ITS defaults (~1 GB, no timeout) and
# ParCa is killed with exit 137 before it does anything. Measured on simulation
# 355 -- three times, because `maxRetries` retried an OOM at the same size.
#
# `memory` is a Groovy CLOSURE (process-bigraph#205) that RAISES memory on 137
# specifically. Scaling on every failure would multiply memory for faults that
# have nothing to do with it; scaling on nothing makes the retries pointless.
# vEcoli's `scaledMemory` makes the same distinction.
#
# Sizes are anchored on what the chain/Ray paths actually run with -- their base
# job definitions request 16 vCPU / 60000 MB -- rather than invented. A caller
# may override any label via `nextflow_dispatch.resources`.
def _scaled_memory(base_gb: int) -> str:
    """Groovy: `base` normally, `base * attempt` after an OOM."""
    return f"{{ task.exitStatus == 137 ? {base_gb}.GB * task.attempt : {base_gb}.GB }}"


# Seconds the Nextflow head gets to shut down before SIGKILL. It terminates its
# own Batch tasks in that window -- the only party that knows exactly which
# tasks it submitted -- so this is the CORRECT path; the reap in cancel_job is
# the guarantee, not the mechanism.
NF_HEAD_TERMINATION_GRACE_SECONDS = 120

DEFAULT_NF_RESOURCES: dict[str, dict[str, Any]] = {
    # ParCa is the memory-hungry one and the reason this default exists.
    "parca": {"cpus": 8, "memory": _scaled_memory(32), "time": "4 h"},
    # A lineage is the LONG one -- hours of simulated generations -- so `time`
    # matters here more than anywhere: it is the only bound on a runaway task
    # (plan-nextflow-dispatch §11.1), and Spot reclaim already retries 10x.
    "lineage": {"cpus": 4, "memory": _scaled_memory(16), "time": "12 h"},
    # The gather loads EVERY sweep's history into one DuckDB, so its memory
    # grows with N x M while a lineage's does not. Measured on simulation 574
    # (the first gather to complete, a 3x2): 16 GB was OOM-killed at 1 min
    # (exit 137) and the x-attempt retry at 32 GB finished in 2 min. A base that
    # only works through the retry is not a base -- and Run 4 is 336 lineages.
    "analysis": {"cpus": 4, "memory": _scaled_memory(32), "time": "2 h"},
}


def _merge_nf_resources(
    overrides: dict[str, dict[str, Any]] | None,
) -> dict[str, dict[str, Any]]:
    """DEFAULT_NF_RESOURCES with per-label overrides merged in.

    Merged rather than replaced, and merged per KEY within a label: overriding
    `lineage.time` must not drop `lineage.memory` and take the run back to
    nf-amazon's ~1 GB, which is the failure this whole default exists to stop.
    """
    merged = {label: dict(res) for label, res in DEFAULT_NF_RESOURCES.items()}
    for label, res in (overrides or {}).items():
        merged.setdefault(label, {}).update(res)
    return merged


def _command_belongs_to_campaign(command: str, campaign_stem: str) -> bool:
    """Is this Batch task in a work dir this run OWNS OUTRIGHT?

    EXACT match after sanitising, and that strictness is the point. The work-dir
    segment is the CAMPAIGN key; `campaign_stem` comes from the head Job name,
    which is the RUN. They are equal only when the run created the campaign --
    i.e. it was not a `resume_from`.

    A resumed run writes into the campaign it joined, whose tasks may belong to a
    DIFFERENT, still-running head. Reaping by campaign there would terminate
    another live run's work. So a resumed run reaps nothing and falls back to the
    grace period, which is the real fix anyway (viva-api#472): leaking a task is
    recoverable, destroying someone else's campaign is not.

    Sanitised both sides because `_nf_head_job_name` lowercases and replaces
    non-alphanumerics -- comparing raw finds nothing and the reap silently does
    nothing. A head name truncated at 63 chars also fails to match, and again
    skips rather than guesses.
    """
    if not campaign_stem:
        return False
    for segment in re.findall(r"/nextflow/work/([^/\s]+)", command):
        if re.sub(r"[^a-z0-9-]+", "-", segment.lower()).strip("-") == campaign_stem:
            return True
    return False


class SimulationServiceRay(SimulationService):
    """Ray-on-Batch (MNP) implementation of SimulationService."""

    def __init__(
        self,
        local_task_service: LocalTaskService | None = None,
        k8s_job_service: "K8sJobService | None" = None,
    ) -> None:
        self._local = local_task_service or LocalTaskService()
        # Only the Nextflow dispatch uses this: its HEAD runs as a K8s Job so it
        # inherits the `batch-submit` ServiceAccount's IRSA identity. Every other
        # path here submits to Batch directly and needs no cluster access.
        self._k8s = k8s_job_service

    def _batch(self) -> Any:
        return boto3.client("batch", region_name=get_settings().batch_region)

    def cache_s3_uri(self, commit: str, *, variant: str | None = None) -> str:
        """Deterministic S3 URI for a commit's v2ecoli ParCa cache.

        Both the ParCa job (writes here) and the simulation job (stages from
        here) derive the same URI, so the cache hand-off needs no runtime wiring.

        ``variant`` (backlog item 105): None for every existing caller --
        unchanged commit-only key. See ``RayLayout.parca_cache_uri``'s own
        docstring for why a derived cache (e.g. ``submit_new_gene_cache_job``'s
        induced-expression build) MUST pass a real label here rather than ever
        writing to the shared bare-commit path.
        """
        return data_layout.RayLayout.parca_cache_uri(commit, variant=variant)

    def _upstream_cache_s3_uri(self, commit: str, *, variant: str | None = None) -> str:
        """S3 URI for the PRISTINE upstream-vEcoli ParCa cache (``--composite vecoli``).

        Kept SEPARATE from ``cache_s3_uri`` (the v2ecoli cache): the external
        upstream wrapper needs an UPSTREAM-MASTER-built ``simData.cPickle``, not
        the v2ecoli one (whose TCS ``modified_molecules`` skew makes upstream's
        two-component-system ODE go negative). Keyed by the same image commit so
        both engines' parca→sim hand-offs derive their URI with no runtime wiring.

        ``variant`` (item 87): None for every existing caller -- unchanged
        commit-only key. See ``RayLayout.parca_cache_uri``'s own docstring for why
        a config-driven build (e.g. a custom strain's ``new_genes``) MUST pass a
        real label here rather than ever writing to the shared bare-commit path.
        """
        return data_layout.RayLayout.parca_cache_uri(commit, upstream=True, variant=variant)

    def _results_s3_uri(self, experiment_id: str) -> str:
        return data_layout.RayLayout.results_uri(experiment_id)

    def _image_uri(self, commit: str) -> str:
        """The TRUE commit image for a run: <account>.dkr.ecr.<region>/v2ecoli:<commit>."""
        settings = get_settings()
        registry = f"{settings.ecr_account_id}.dkr.ecr.{settings.batch_region}.amazonaws.com"
        return f"{registry}/{settings.ray_ecr_repository}:{commit}"

    def _ensure_mnp_job_def(self, image: str, commit: str) -> str:
        """Return an MNP job definition (name:revision) whose image is the commit's image.

        Batch MNP can't override the image per-submission, so — symmetric with how K8s
        sets the image per-Job — we derive a per-commit job-def revision: describe the
        CDK base job def (``ray_mnp_job_definition``: roles, resources, shm, log config,
        node count), swap ONLY every node range's container image to ``image``, and
        register it as ``<base>-<commit>``. An existing active revision already pointing
        at this image is reused, so resubmits don't churn revisions.
        """
        settings = get_settings()
        batch = self._batch()
        name = f"{settings.ray_mnp_job_definition}-{commit}"

        # Reuse an existing active revision that already targets this exact image.
        existing = batch.describe_job_definitions(jobDefinitionName=name, status="ACTIVE")
        for jd in existing.get("jobDefinitions", []):
            images = {
                nr.get("container", {}).get("image")
                for nr in jd.get("nodeProperties", {}).get("nodeRangeProperties", [])
            }
            if images == {image}:
                return f"{name}:{jd['revision']}"

        # Otherwise clone the base job def's node properties and swap the image.
        base = batch.describe_job_definitions(jobDefinitionName=settings.ray_mnp_job_definition, status="ACTIVE")
        base_defs = base.get("jobDefinitions", [])
        if not base_defs:
            raise RuntimeError(f"Base Ray MNP job definition {settings.ray_mnp_job_definition!r} not found")
        node_properties = copy.deepcopy(max(base_defs, key=lambda d: d["revision"])["nodeProperties"])
        for nr in node_properties.get("nodeRangeProperties", []):
            nr.setdefault("container", {})["image"] = image

        response = batch.register_job_definition(
            jobDefinitionName=name,
            type="multinode",
            nodeProperties=node_properties,
        )
        logger.info("Registered Ray MNP job def %s:%s for image %s", name, response["revision"], image)
        return f"{name}:{response['revision']}"

    def _submit_mnp(
        self,
        *,
        job_name: str,
        job_definition: str,
        num_nodes: int,
        ray_job_cmd: str,
        out_s3: str,
        out_dir: str,
        stage_s3: str | None = None,
        stage_dir: str | None = None,
        depends_on: list[str] | None = None,
        depends_type: str | None = "SEQUENTIAL",
        tags: dict[str, str] | None = None,
        retry_strategy: dict[str, Any] | None = None,
        batch_client: Any = None,
        expect_new_genes: str | None = None,
        expect_bundle_overrides: str | list[str] | None = None,
        require_clean_chain: bool = False,
        lineage_debug_division: bool = False,
        task_env: dict[str, str] | None = None,
    ) -> str:
        """Submit a Ray MNP job via boto3, mirroring sms-cdk scripts/ray_batch_submit.sh.

        Env targeting matters: the entrypoint runs ``stage_inputs`` and the periodic
        output sync on EVERY node, so the staging/output/log knobs must reach all
        nodes — the workers need the ParCa cache to run seeds and must ship their own
        zarr to S3. Only ``RAY_JOB_CMD`` (the driver) and ``RAY_REPORT_PATH`` are
        head-only. So the shared env goes on node 0 (``0:0``) and, when there are
        workers, also on the worker range (``1:``). Returns the AWS Batch job id.

        ``depends_type`` selects the ``dependsOn`` shape. The default keeps the
        long-standing ParCa→sim edge byte-identical (``{"jobId": …, "type":
        "SEQUENTIAL"}``, live-verified). Pass ``None`` for a plain ``{"jobId": …}``
        wait — required when the DEPENDENCY is an Array job, whose parent id AWS
        Batch will not accept under a SEQUENTIAL type (real API rejection, hit live
        2026-08-06; see ``_submit_array``).

        ``retry_strategy``, passed through verbatim as ``SubmitJob.retryStrategy``,
        overrides whatever the job definition itself declares (per the real AWS
        Batch API — confirmed this session) — used by the per-seed chain-dispatch
        path (backlog item 33) to restore per-job retry on the MNP job definition,
        which (unlike the Array job definition) declares none of its own; omitted
        (``None``) everywhere else, unchanged from existing behavior.

        ``batch_client``, when given, is used INSTEAD of ``self._batch()`` for this
        one call — lets a caller submitting many jobs in a tight loop (chain
        dispatch) supply its own retry-configured client without changing what
        every other existing call site in this class gets from the shared
        ``self._batch()`` factory.
        """
        settings = get_settings()
        # Per-node knobs every node acts on (stage cache in, sync results out, ship logs).
        shared_env = self._stage_out_env(
            prefix="RAY",
            out_dir=out_dir,
            out_s3=out_s3,
            stage_s3=stage_s3,
            stage_dir=stage_dir,
            log_s3_prefix=settings.ray_log_s3_prefix,
            expect_new_genes=expect_new_genes,
            expect_bundle_overrides=expect_bundle_overrides,
            require_clean_chain=require_clean_chain,
            lineage_debug_division=lineage_debug_division,
        )
        # Ray's own documented safety net (not a bespoke workaround): by default Ray
        # refuses to start its plasma object store when the container's /dev/shm is
        # smaller than the size it wants to request, which is a real, observed
        # failure mode on this fleet -- a single-node lineage_ray_batch diagnostic
        # (item 105/109, database_id=344, 2026-09-05) died in raylet bootstrap,
        # before any application code ran, requesting ~10.2GB against ~9.66GB
        # available. This flag makes Ray fall back to a disk-backed object store
        # instead of erroring -- zero behavioral change on every node where shm is
        # already sufficient (every other MNP dispatch to date), a graceful
        # (slower, not silent) degradation instead of a hard crash on the ones
        # that aren't. Every node runs its own raylet, so this belongs in
        # shared_env, not head-only.
        shared_env.append({"name": "RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE", "value": "1"})
        # task_env (sms-ecoli#166): the request's own env, validated at the boundary
        # (dispatch_validation.validate_task_env), reaching EVERY node -- e.g.
        # V2ECOLI_SKIP_CACHE_VERIFY=1 after a cache-re-keying v2ecoli commit.
        if task_env:
            shared_env.extend(task_env_as_batch_list(task_env))
            logger.info("MNP job %s: task_env passthrough %s", job_name, dict(task_env))

        # The head additionally runs the workload (RAY_JOB_CMD) and writes the report.
        # Workers receive these too but never act on them — the entrypoint branches on
        # AWS_BATCH_JOB_NODE_INDEX and only the head executes RAY_JOB_CMD/writes the report.
        head_env: list[dict[str, str]] = [
            {"name": "RAY_JOB_CMD", "value": ray_job_cmd},
            {"name": "RAY_REPORT_PATH", "value": REPORT_PATH},
            *shared_env,
        ]

        # The CDK base job definition declares a SINGLE node range ("0:") — the entrypoint
        # self-branches head vs. worker — so the submit-time override must target that same
        # range. (Splitting into "0:0"/"1:" makes Batch reject: "NodeOverride targets should
        # match job definition".) One override on "0:" with the full env reaches every node;
        # the per-node staging/output knobs in shared_env are what workers need.
        node_property_overrides: list[dict[str, Any]] = [
            {"targetNodes": "0:", "containerOverrides": {"environment": head_env}},
        ]

        node_overrides: dict[str, Any] = {
            "numNodes": num_nodes,
            "nodePropertyOverrides": node_property_overrides,
        }
        # Backlog item 65: a standalone (numNodes=1) submission has no inter-node
        # traffic to protect, so it gains nothing from ray_mnp_queue's cluster-
        # placement-group compute environment and pays its full concurrency cost
        # for nothing -- route it to the dedicated no-placement-group queue
        # instead, when one is configured. Automatic and transparent to every
        # caller (chain-dispatch, ParCa, compose): both already pass their real
        # num_nodes here, no call-site changes needed. Falls back to
        # ray_mnp_queue unchanged for a genuine multi-node request (num_nodes >
        # 1, e.g. colony sims) or when ray_mnp_standalone_queue isn't set yet.
        job_queue = (
            settings.ray_mnp_standalone_queue
            if num_nodes == 1 and settings.ray_mnp_standalone_queue
            else settings.ray_mnp_queue
        )
        kwargs: dict[str, Any] = {
            "jobName": job_name,
            "jobQueue": job_queue,
            "jobDefinition": job_definition,
            "nodeOverrides": node_overrides,
        }
        if depends_on:
            kwargs["dependsOn"] = [
                ({"jobId": jid, "type": depends_type} if depends_type else {"jobId": jid}) for jid in depends_on
            ]
        if tags:
            # Cost-allocation tags: propagate to the underlying ECS tasks so the
            # payer account's Cost Explorer can attribute compute per run/engine.
            kwargs["tags"] = tags
            kwargs["propagateTags"] = True
        if retry_strategy:
            kwargs["retryStrategy"] = retry_strategy

        batch = batch_client if batch_client is not None else self._batch()
        response = batch.submit_job(**kwargs)
        batch_job_id = str(response["jobId"])
        logger.info(
            "Submitted Ray MNP job %s (id=%s, nodes=%d) to %s",
            job_name,
            batch_job_id,
            num_nodes,
            job_queue,
        )
        return batch_job_id

    def _stage_out_env(
        self,
        *,
        prefix: str,
        out_dir: str,
        out_s3: str,
        stage_s3: str | None = None,
        stage_dir: str | None = None,
        log_s3_prefix: str | None = None,
        expect_new_genes: str | None = None,
        expect_bundle_overrides: str | list[str] | None = None,
        require_clean_chain: bool = False,
        lineage_debug_division: bool = False,
    ) -> list[dict[str, str]]:
        """Shared stage/output/log env-var construction for both the MNP (``RAY_*``)
        and container (``CONTAINER_*``) submission paths (backlog item 71) -- same
        conditional logic (only emit STAGE_*/LOG_S3_PREFIX when configured), a
        different env-var prefix per job shape, since each entrypoint script only
        reads its own prefix -- the values can't literally share one env list.

        ``expect_new_genes``/``expect_bundle_overrides`` (sms-ecoli#210 / #215): the
        STRAIN this run requested. The entrypoint's ``stage_inputs`` already runs
        ``verify_cache_version`` (schema + source-hash) on the staged cache; these
        let it ALSO reject a WRONG-STRAIN cache (P1-6). Emitted as
        ``{prefix}_EXPECT_NEW_GENES`` / ``{prefix}_EXPECT_BUNDLE_OVERRIDES`` only for
        a real strain -- ``off``/empty is wild-type and emits nothing, so a
        wild-type run is byte-identical to before and the entrypoint check stays
        inert until a real strain is requested.

        ``expect_bundle_overrides`` accepts a list (backlog items 93/104/106,
        ``ParcaOptions.bundle_overrides`` accepts a list as of #486, for a strain
        recipe that stacks multiple ``--bundle-overrides`` files) -- joined with
        ``","`` for the single env var, same normalization ``strain_from_config``'s
        own ``_norm`` helper already applies for the job-scheduler verification
        path. Real, confirmed gap this closes: a caller reaching this helper
        DIRECTLY with a list (e.g. via ``getattr(config.parca_options,
        "bundle_overrides", None)``, not through ``strain_from_config``) crashed
        with ``AttributeError: 'list' object has no attribute 'strip'`` -- caught
        live firing a real K4/J3 chassis rebuild whose recipe genuinely needs two
        stacked override files.

        ``require_clean_chain`` (item 106/#166 chassis-provenance thread, v2ecoli#735):
        emitted verbatim as ``V2E_REQUIRE_CLEAN_CHAIN`` -- UNPREFIXED, unlike every
        other var this helper emits -- because it is read directly by v2ecoli's own
        ``os.environ.get("V2E_REQUIRE_CLEAN_CHAIN")`` (``save_sim_input``/
        ``save_cache``/``verify_cache_version``), not by the ``RAY_*``/``CONTAINER_*``
        entrypoint scripts this helper otherwise targets. Default ``False`` emits
        nothing -- byte-identical to before this param existed -- because most
        existing callers (``new_gene_cache``, ``variant_cache``,
        ``build_condition_cache``, ``run_comparison_ensemble``) don't pass
        ``sources=`` yet (that wiring is v2ecoli's own PR 3); setting this
        unconditionally would hard-fail every one of them the moment v2ecoli#735
        lands, including Run 4's own already-built new-gene caches.

        ``lineage_debug_division`` (item 106/#210, v2ecoli#733): emitted verbatim as
        ``LINEAGE_DEBUG_DIVISION`` -- UNPREFIXED, same reasoning as
        ``require_clean_chain`` above -- v2ecoli's own
        ``LineageProcess._run_until_division`` reads it directly via
        ``os.environ.get``. Opt-in diagnostic only; default ``False`` emits nothing.
        """
        env: list[dict[str, str]] = [
            {"name": f"{prefix}_OUT_DIR", "value": out_dir},
            {"name": f"{prefix}_OUT_S3", "value": out_s3},
        ]
        if stage_s3 and stage_dir:
            env.append({"name": f"{prefix}_STAGE_S3", "value": stage_s3})
            env.append({"name": f"{prefix}_STAGE_DIR", "value": stage_dir})
        if log_s3_prefix:
            env.append({"name": f"{prefix}_LOG_S3_PREFIX", "value": log_s3_prefix})
        # off/empty is wild-type -> no expectation to assert (matches the parca-side
        # normalization in build_cache.py and _parca_command's own flag guard).
        ng = (expect_new_genes or "").strip()
        if ng and ng != "off":
            env.append({"name": f"{prefix}_EXPECT_NEW_GENES", "value": ng})
        bo_raw = (
            ",".join(expect_bundle_overrides) if isinstance(expect_bundle_overrides, list) else expect_bundle_overrides
        )
        bo = (bo_raw or "").strip()
        if bo and bo != "off":
            env.append({"name": f"{prefix}_EXPECT_BUNDLE_OVERRIDES", "value": bo})
        if require_clean_chain:
            env.append({"name": "V2E_REQUIRE_CLEAN_CHAIN", "value": "1"})
        if lineage_debug_division:
            env.append({"name": "LINEAGE_DEBUG_DIVISION", "value": "1"})
        return env

    def _ensure_container_job_def(self, image: str, commit: str) -> str:
        """Return a container job definition (name:revision) whose image is the commit's image.

        Mirrors ``_ensure_mnp_job_def`` exactly, for the plain (non-MNP, non-array)
        standalone container job shape (backlog item 71 -- ParCa, the analysis DAG
        node, and eventually chain-dispatch's per-seed-per-generation jobs, none of
        which have any real inter-node traffic to protect). Plain container jobs
        can't override the image at submission time either -- same limitation as
        MNP -- so a per-commit job-def revision is derived the same way: describe
        the CDK base container job def (``ray_container_job_definition``: roles,
        resources, retry strategy, log config -- provisioned by sms-cdk's
        RayContainerJobDef), swap ONLY its image, and register it as
        ``<base>-<commit>``. An existing active revision already pointing at this
        image is reused, so resubmits don't churn revisions.
        """
        settings = get_settings()
        if not settings.ray_container_job_definition:
            # Matches this file's own compose_ray_image_tag precedent: fail loud with
            # the setting name rather than submit a doomed job with a blank job-def.
            raise RuntimeError("ray_container_job_definition is not set; cannot submit a container-type Batch job.")
        batch = self._batch()
        name = f"{settings.ray_container_job_definition}-{commit}"

        # Reuse an existing active revision that already targets this exact image.
        existing = batch.describe_job_definitions(jobDefinitionName=name, status="ACTIVE")
        for jd in existing.get("jobDefinitions", []):
            if jd.get("containerProperties", {}).get("image") == image:
                return f"{name}:{jd['revision']}"

        # Otherwise clone the base job def's container properties and swap the image.
        base = batch.describe_job_definitions(jobDefinitionName=settings.ray_container_job_definition, status="ACTIVE")
        base_defs = base.get("jobDefinitions", [])
        if not base_defs:
            raise RuntimeError(f"Base container job definition {settings.ray_container_job_definition!r} not found")
        container_properties = copy.deepcopy(max(base_defs, key=lambda d: d["revision"])["containerProperties"])
        container_properties["image"] = image

        response = batch.register_job_definition(
            jobDefinitionName=name,
            type="container",
            containerProperties=container_properties,
        )
        logger.info("Registered container job def %s:%s for image %s", name, response["revision"], image)
        return f"{name}:{response['revision']}"

    def _submit_container(
        self,
        *,
        job_name: str,
        job_definition: str,
        job_cmd: str,
        out_s3: str,
        out_dir: str,
        stage_s3: str | None = None,
        stage_dir: str | None = None,
        depends_on: list[str] | None = None,
        depends_type: str | None = "SEQUENTIAL",
        tags: dict[str, str] | None = None,
        retry_strategy: dict[str, Any] | None = None,
        batch_client: Any = None,
        expect_new_genes: str | None = None,
        expect_bundle_overrides: str | list[str] | None = None,
        require_clean_chain: bool = False,
        lineage_debug_division: bool = False,
        task_env: dict[str, str] | None = None,
        memory_class: str = "standard",
    ) -> str:
        """Submit a plain, standalone AWS Batch container-type job (backlog item 71).

        Sibling of ``_submit_mnp`` for the non-MNP, non-array job shape -- currently
        ParCa (``submit_parca_job``) and the analysis DAG node
        (``_submit_analysis_job``), both already ``num_nodes=1`` MNP jobs with no
        real inter-node traffic; chain-dispatch's per-seed-per-generation jobs
        migrate here too in a later phase. One task, one container: no node
        overrides, no head/worker split -- every env var goes in a single
        ``containerOverrides.environment`` list, matching
        ``docker/batch-container-entrypoint.sh``'s ``CONTAINER_*`` contract exactly
        (sms-ecoli). Returns the AWS Batch job id.

        Do NOT modify ``_submit_mnp`` -- this is a parallel path, not a
        replacement; genuinely multi-node Ray paths keep submitting through
        ``_submit_mnp`` unchanged.
        """
        settings = get_settings()
        if not settings.ray_container_queue:
            raise RuntimeError("ray_container_queue is not set; cannot submit a container-type Batch job.")

        # Memory-class routing (viva-api#625): a "large" job goes to the
        # large-memory (200 GB r7i) queue when one is provisioned; otherwise it
        # falls back to the standard queue (same convention as
        # ray_mnp_standalone_queue), so behaviour is unchanged until sms-cdk sets
        # ray_container_large_queue. Require a real non-empty string so a settings
        # double's auto-attribute can't accidentally route.
        job_queue = settings.ray_container_queue
        large_queue = getattr(settings, "ray_container_large_queue", "")
        if memory_class == "large" and isinstance(large_queue, str) and large_queue.strip():
            job_queue = large_queue
            logger.info("Container job %s: memory_class=large -> large-memory queue %s", job_name, job_queue)
        elif memory_class == "large":
            logger.info(
                "Container job %s: memory_class=large but no ray_container_large_queue set; using standard queue %s",
                job_name,
                job_queue,
            )

        env: list[dict[str, str]] = [
            {"name": "CONTAINER_JOB_CMD", "value": job_cmd},
            {"name": "CONTAINER_REPORT_PATH", "value": REPORT_PATH},
            *self._stage_out_env(
                prefix="CONTAINER",
                out_dir=out_dir,
                out_s3=out_s3,
                stage_s3=stage_s3,
                stage_dir=stage_dir,
                log_s3_prefix=settings.ray_log_s3_prefix,
                expect_new_genes=expect_new_genes,
                expect_bundle_overrides=expect_bundle_overrides,
                require_clean_chain=require_clean_chain,
                lineage_debug_division=lineage_debug_division,
            ),
        ]
        # task_env (sms-ecoli#166): see _submit_mnp -- same passthrough, one container.
        if task_env:
            env.extend(task_env_as_batch_list(task_env))
            logger.info("Container job %s: task_env passthrough %s", job_name, dict(task_env))

        kwargs: dict[str, Any] = {
            "jobName": job_name,
            "jobQueue": job_queue,
            "jobDefinition": job_definition,
            "containerOverrides": {"environment": env},
        }
        if depends_on:
            kwargs["dependsOn"] = [
                ({"jobId": jid, "type": depends_type} if depends_type else {"jobId": jid}) for jid in depends_on
            ]
        if tags:
            kwargs["tags"] = tags
            kwargs["propagateTags"] = True
        if retry_strategy:
            kwargs["retryStrategy"] = retry_strategy

        batch = batch_client if batch_client is not None else self._batch()
        response = batch.submit_job(**kwargs)
        batch_job_id = str(response["jobId"])
        logger.info("Submitted container job %s (id=%s) to %s", job_name, batch_job_id, settings.ray_container_queue)
        return batch_job_id

    def _parca_command(
        self,
        *,
        new_genes: str | None = None,
        bundle_overrides: str | list[str] | None = None,
        rnaseq_source: str | None = None,
        bundle_manifest_path: str | None = None,
        build_combined_bundle_manifest: bool = False,
        include_violacein_bundle: bool = False,
        deterministic_hash_seed: bool = False,
    ) -> str:
        """Run ParCa, then hydrate the sim-input bundle into PARCA_CACHE_DIR (out/cache).

        v2ecoli's sim loads ``out/cache/{initial_state.json, sim_data_cache.dill, ...}`` via
        ``build_composite(cache_dir=out/cache)``. ``v2ecoli-parca`` only emits the raw
        ``parca_state.pkl`` (+ a Km cache), so ``scripts/build_cache.py`` must hydrate that
        into the bundle. build_cache.py/load_parca_state read a GZIPPED fixture, so gzip the
        parca output first (the round-trip bridges v2ecoli's .pkl→.pkl.gz mismatch). Only
        PARCA_CACHE_DIR is synced to S3 (RAY_OUT_DIR), and that is exactly what the sim stages.

        ``new_genes`` (backlog item 93): a legacy config's own ``parca_options.new_genes``
        (e.g. a custom strain's new-gene insertion subdir) -- generic passthrough to
        ``v2ecoli-parca``'s own ``--new-genes SUBDIR`` flag (default ``"off"``, so omitting
        or passing ``"off"`` builds byte-for-byte the same command as before this param
        existed). No caller-side change required for any dispatch that doesn't set it.

        ``bundle_overrides`` (backlog item 104, extended item 106): a legacy
        config's own ``parca_options.bundle_overrides`` -- generic passthrough to
        ``v2ecoli-parca``'s own ``--bundle-overrides PATH`` flag (``cli/parca.py``,
        ``action="append"``). Same "missed in the new_genes pass" class of gap
        this mirrors exactly (sms-ecoli#184 / viva-api#365): the value survived on
        the stored request but was never read here, so ParCa silently built from
        defaults and any keys the overrides supply were absent. Accepts a single
        string (one flag, byte-for-byte the original behavior) OR a list of
        strings (one ``--bundle-overrides PATH`` per entry, IN ORDER) -- real,
        confirmed need: sms-ecoli's own declared recipe for the CD2 J3/K4
        candidate chassis (``cd2-pnnl-01-bundle-scenarios/sims/run_scenarios.sh``,
        scenario ``rung5_lam075``) stacks TWO overrides in one command
        (``--bundle-overrides .../vio-gfp/overrides.tsv --bundle-overrides
        .../rung5-lambda-075/overrides.tsv``); a single-string field could only
        ever carry one of the two layers. Default ``None`` builds byte-for-byte
        the same command as before this param existed.

        ``rnaseq_source`` (backlog item 106/#166 chassis-provenance thread): a legacy
        config's own ``parca_options.rnaseq_source`` -- generic passthrough to
        ``v2ecoli-parca``'s own ``--rnaseq-source {reference,experimental}`` flag
        (default ``"reference"``). Real, confirmed gap this closes: at least one
        real ``bundle_overrides`` manifest (sms-ecoli's
        ``cd2-pnnl-01-bundle-scenarios/bundles/rung5-lambda-075/overrides.tsv``)
        is a no-op without it -- its own header: "READ BY NOTHING without that
        flag... the scenario silently becomes its own control". Same silent-
        wrong-build failure mode as new_genes/bundle_overrides being dropped.
        Default ``None`` builds byte-for-byte the same command as before this
        param existed.

        ``bundle_manifest_path`` (item 451/#166, Run 4 founder-chassis rebuild):
        generic passthrough to ``v2ecoli-parca``'s own ``--bundle-manifest-path
        PATH`` flag -- the BASE reference-bundle manifest, distinct from
        ``bundle_overrides`` above (which layers on top of whatever base is in
        effect; this flag replaces the base itself). Real, confirmed need:
        sms-ecoli's own declared recipe for Run 4's violacein founder chassis
        (``scripts/build_run4_founder_caches.py``'s own module docstring) is
        ``--bundle-manifest-path out/combined_violacein.tsv --new-genes
        violacein_MG1655_M5`` -- unreachable from a remote dispatch without this
        flag. Default ``None`` builds byte-for-byte the same command as before
        this param existed.

        ``build_combined_bundle_manifest``/``include_violacein_bundle`` (item
        451/#166): when the first is set, prepends a real invocation of
        sms-ecoli's own ``scripts/build_combined_bundle_manifest.py``
        (``--include-violacein`` when the second is also set) ahead of the
        ``v2ecoli-parca`` call, so the combined manifest -- a deliberately
        machine-local, gitignored build artifact per that script's own notes,
        never committed -- gets generated fresh inside THIS remote container
        before ParCa reads it, then points ``--bundle-manifest-path`` at the
        generator's own known default output
        (``out/combined_bundle_manifest.tsv``). Mutually exclusive with passing
        ``bundle_manifest_path`` directly -- set one or the other, not both.
        Default ``False`` is a pure no-op.

        ``deterministic_hash_seed`` (item 451/#166): when set, prepends
        ``PYTHONHASHSEED=0`` to the ``v2ecoli-parca`` invocation. Real, confirmed
        need: Run 4's own founder-chassis recipe explicitly requires it (same
        module docstring as above) -- Python's hash randomization can otherwise
        perturb dict/set iteration order inside ParCa's own reconstruction code,
        which the recipe treats as a determinism requirement for a chassis meant
        to be deterministically re-derived. Opt-in rather than unconditional:
        changing hash-seed behavior for every existing ParCa dispatch is a
        bigger, untested behavioral change than this fix's own scope calls for.
        Default ``False`` is a pure no-op.

        Also copies the chassis-provenance sidecar (``parca_state.provenance.json``,
        item 106/#166, v2ecoli#735) alongside the raw state, non-fatally -- a
        pre-#735 v2ecoli image never writes this file, so the ``|| true`` keeps
        every dispatch working unchanged until that lands; once it does, the
        sidecar rides the same S3 sync ``parca_state.pkl.gz`` already does,
        without this command needing to change again.

        Also copies the gzipped RAW fitted state (``parca_state.pkl.gz``) into
        ``PARCA_CACHE_DIR`` itself (backlog item 105), so it rides along in the
        existing ``out_dir``/``out_s3`` sync instead of being discarded with the
        rest of ``PARCA_SIMDATA_DIR`` when the job's container exits.
        ``build_new_gene_cache.py`` needs exactly this raw file, not the
        hydrated ``out/cache`` bundle ``build_cache.py`` produces from it one
        line below -- see ``submit_new_gene_cache_job``. Unconditional: the file
        is small next to the rest of the cache, and every existing consumer of
        this cache dir already tolerates unknown files (nothing here globs or
        rejects extras).

        ``new_genes``/``bundle_overrides`` do NOT ride onto the ``build_cache.py``
        step below (confirmed 2026-09-04 against a real crash: its current CLI has
        neither flag, ``unrecognized arguments``). Not a gap -- its own bundle-write
        (``save_sim_input``) already produces a complete, correct ``cache_version.json``
        straight from ``sim_data``, which is already strain-specific because
        ``v2ecoli-parca`` received both flags one command earlier in this same chain.
        Restamping here would be redundant even where it was once supported.
        """
        if bundle_manifest_path and build_combined_bundle_manifest:
            raise ValueError(
                "bundle_manifest_path and build_combined_bundle_manifest are mutually exclusive -- "
                "set one or the other, not both."
            )
        settings = get_settings()
        new_genes_flag = f" --new-genes {shlex.quote(new_genes)}" if new_genes and new_genes != "off" else ""
        bundle_overrides_list = (
            [bundle_overrides] if isinstance(bundle_overrides, str) else list(bundle_overrides or [])
        )
        bundle_overrides_flag = "".join(f" --bundle-overrides {shlex.quote(path)}" for path in bundle_overrides_list)
        rnaseq_source_flag = f" --rnaseq-source {shlex.quote(rnaseq_source)}" if rnaseq_source else ""
        combined_manifest_prefix = ""
        effective_bundle_manifest_path = bundle_manifest_path
        if build_combined_bundle_manifest:
            violacein_flag = " --include-violacein" if include_violacein_bundle else ""
            combined_manifest_prefix = f"python scripts/build_combined_bundle_manifest.py{violacein_flag} && "
            effective_bundle_manifest_path = "out/combined_bundle_manifest.tsv"
        bundle_manifest_path_flag = (
            f" --bundle-manifest-path {shlex.quote(effective_bundle_manifest_path)}"
            if effective_bundle_manifest_path
            else ""
        )
        hash_seed_prefix = "PYTHONHASHSEED=0 " if deterministic_hash_seed else ""
        command = (
            f"cd {V2ECOLI_DIR}"
            f" && {combined_manifest_prefix}{hash_seed_prefix}v2ecoli-parca --mode {settings.ray_parca_mode}"
            f" --cpus {settings.ray_parca_cpus}"
            f" -o {PARCA_SIMDATA_DIR} --cache-dir {PARCA_CACHE_DIR}"
            f"{new_genes_flag}{bundle_overrides_flag}{rnaseq_source_flag}{bundle_manifest_path_flag}"
            f" && gzip -f -k {PARCA_SIMDATA_DIR}/parca_state.pkl"
            f" && python scripts/build_cache.py"
            f" --fixture {PARCA_SIMDATA_DIR}/parca_state.pkl.gz"
            f" --cache {PARCA_CACHE_DIR}"
            f" && cp {PARCA_SIMDATA_DIR}/parca_state.pkl.gz {PARCA_CACHE_DIR}/parca_state.pkl.gz"
            f" && (cp {PARCA_SIMDATA_DIR}/parca_state.provenance.json"
            f" {PARCA_CACHE_DIR}/parca_state.provenance.json 2>/dev/null || true)"
        )
        # A config that requests a real strain (new_genes != "off") MUST produce a
        # command that carries the flag — otherwise ParCa silently builds wild-type
        # and the run "succeeds" with the wrong genotype (CD2 audit §2.1 / P0-2).
        if new_genes and new_genes != "off":
            assert new_genes_flag and new_genes_flag in command, (  # noqa: S101  internal invariant, not input validation
                f"new_genes={new_genes!r} requested but the ParCa command does not carry --new-genes: {command!r}"
            )
        return command

    def _build_new_gene_cache_command(
        self,
        *,
        expression: float,
        translation_efficiency: float,
        rel_exp_adj: str | None = None,
        rel_trl_eff_adj: str | None = None,
        seed: int = 0,
        media_condition: str | None = None,
        fixed_media: str | None = None,
    ) -> str:
        """Run ``scripts/build_new_gene_cache.py`` against an ALREADY-STAGED
        commit cache (backlog item 105 -- the ``build_new_gene_cache.py``
        remote-reachability gap Chris/cplong90 flagged: "we have no idea
        whether step two is reachable remotely at all").

        ParCa inserts a new gene SILENT (``new_genes`` is presence/absence
        only, item 93). This is the OTHER half: it hydrates the raw
        ``parca_state.pkl.gz`` a prior ``_parca_command`` run left in
        ``PARCA_CACHE_DIR`` (staged into this job via ``stage_s3``/
        ``stage_dir`` -- see ``submit_new_gene_cache_job``) and writes a NEW,
        derived cache bundle to ``NEW_GENE_INDUCED_CACHE_DIR`` with the given
        gene(s) actually expressed at a caller-chosen level. The strain
        identity itself lives in THIS cache, not in any per-dispatch
        injection (Chris's own framing, sms-ecoli#166) -- ``expression``/
        ``translation_efficiency`` are the two required knobs the script
        itself requires; the rest are its own optional per-gene/media knobs,
        passed straight through.

        Caller (``submit_new_gene_cache_job``) is responsible for staging the
        SOURCE commit's cache (which must itself have been built with
        ``new_genes`` set -- an all-zero-expression source has nothing to
        induce) and for writing the output to a ``variant``-labeled S3 key,
        never the bare commit-only path a plain baseline stage would read.
        """
        rel_exp_flag = f" --rel-exp-adj {shlex.quote(rel_exp_adj)}" if rel_exp_adj else ""
        rel_trl_flag = f" --rel-trl-eff-adj {shlex.quote(rel_trl_eff_adj)}" if rel_trl_eff_adj else ""
        media_flag = f" --media-condition {shlex.quote(media_condition)}" if media_condition else ""
        fixed_media_flag = f" --fixed-media {shlex.quote(fixed_media)}" if fixed_media else ""
        return (
            f"cd {V2ECOLI_DIR}"
            f" && python scripts/build_new_gene_cache.py"
            f" --state {PARCA_CACHE_DIR}/parca_state.pkl.gz"
            f" --cache {NEW_GENE_INDUCED_CACHE_DIR}"
            f" --expression {expression} --translation-efficiency {translation_efficiency}"
            f" --seed {seed}"
            f"{rel_exp_flag}{rel_trl_flag}{media_flag}{fixed_media_flag}"
        )

    def _build_variant_cache_command(
        self,
        *,
        perturbations: dict[str, float],
        seed: int = 0,
        fixed_media: str | None = None,
    ) -> str:
        """Run ``scripts/build_variant_cache.py`` against an ALREADY-STAGED commit
        cache (backlog item 451 -- the other half of a design screen from
        ``_build_new_gene_cache_command`` above: NATIVE-gene translation-efficiency
        perturbations, not a new-gene induction level).

        Sibling of ``_build_new_gene_cache_command`` in every structural respect:
        hydrates the raw ``parca_state.pkl.gz`` a prior ``_parca_command`` run left
        in ``PARCA_CACHE_DIR`` (staged into this job via ``stage_s3``/``stage_dir``
        -- see ``submit_variant_cache_job``) and writes a NEW, derived cache bundle
        to ``VARIANT_CACHE_DIR`` with the given native genes perturbed via
        ``v2ecoli.perturbations.build_variant_cache`` (see that script's own
        docstring for why this cache route, not a config-level override, is
        required for a multi-generation study). ``perturbations`` is the one
        required knob the script itself requires (a gene-id -> multiplier JSON
        object); ``seed``/``fixed_media`` are its own optional knobs.

        Caller (``submit_variant_cache_job``) is responsible for staging the
        SOURCE commit's cache and for writing the output to a ``variant``-labeled
        S3 key, never the bare commit-only path a plain baseline stage would read
        -- identical contract to ``submit_new_gene_cache_job``.
        """
        fixed_media_flag = f" --fixed-media {shlex.quote(fixed_media)}" if fixed_media else ""
        return (
            f"cd {V2ECOLI_DIR}"
            f" && python scripts/build_variant_cache.py"
            f" --state {PARCA_CACHE_DIR}/parca_state.pkl.gz"
            f" --cache {VARIANT_CACHE_DIR}"
            f" --perturbations {shlex.quote(json.dumps(perturbations))}"
            f" --seed {seed}"
            f"{fixed_media_flag}"
        )

    def _upstream_parca_command(self, *, config_path: str | None = None) -> str:
        """Build a PRISTINE upstream-vEcoli ParCa simData for the ``--composite vecoli`` wrapper.

        Runs once on the 1-node parca job from the image's bundled upstream
        checkout (``$V2E_VECOLI_DIR=/app/vEcoli``), dropping a flat
        ``simData.cPickle`` into ``PARCA_CACHE_DIR``. The entrypoint then syncs
        that dir to ``_upstream_cache_s3_uri``; the N-node sim stages the SAME
        cache to every node (Ray workers must read identical sim_data — a
        per-node refit would diverge since ParCa is not bit-reproducible).

        ``--cpus 1`` is REQUIRED, not a perf knob: upstream's fit_sim_data_1 only
        spawns a worker Pool when cpus>1, and those workers re-import
        wholecell.utils.polymerize from the source-only /app/vEcoli checkout —
        which has no compiled Cython (.so) and hard-raises "Failed to import
        Cython module", looping forever. The serial path (cpus==1) runs entirely
        in the main process, where the wrapper's import shim has pinned the
        INSTALLED compiled wholecell into sys.modules, so the import resolves.

        ``config_path`` (item 87): None for every existing caller -- identical
        command to before this param existed (``build_upstream_parca.py``'s own
        ``--config`` defaults to the pristine baseline build). When set (an
        in-image path to a config declaring ``parca_options.new_genes``, e.g. a
        custom strain), threads it through so the built cache is config-driven.
        The CALLER is responsible for pairing this with a matching ``variant``
        label on ``_upstream_cache_s3_uri`` -- this method has no way to enforce
        that pairing itself.
        """
        config_flag = f" --config {config_path}" if config_path else ""
        return (
            f"cd {V2ECOLI_DIR} && python scripts/build_upstream_parca.py"
            f" --outdir {V2ECOLI_DIR}/out/upstream --cpus 1"
            f" --copy-to {PARCA_CACHE_DIR}{config_flag}"
        )

    async def stage_render_nf(self, experiment_id: str) -> str:
        """Upload the Nextflow compiler beside the run_pbg runner; return its URI.

        Same staging idiom and the same reason as ``stage_runner``: Batch caps a
        container override command at 8192 bytes, so the script travels through S3
        rather than the command line. Deterministic from ``experiment_id``.
        """
        from viva_api.dependencies import get_file_service

        file_service = get_file_service()
        if file_service is None:
            raise RuntimeError("FileService not initialized; cannot stage render_nf.py to S3.")
        exp_prefix = data_layout.RayLayout.experiment_prefix(experiment_id)
        runner_key = f"{exp_prefix}/render_nf.py"
        with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as tmp:
            tmp.write(_RENDER_NF_SRC)
            runner_local = tmp.name
        try:
            await file_service.upload_file(Path(runner_local), S3FilePath(s3_path=Path(runner_key)))
        finally:
            Path(runner_local).unlink(missing_ok=True)
        return data_layout.s3_uri(runner_key)

    def _submit_image_uri(self, commit: str) -> str:
        """The Nextflow HEAD image for a commit: ``<repo>:<commit>-submit``.

        Only the process running ``nextflow run`` needs a JVM; Batch TASKS run the
        plain science image. Built on request by ``include_submit_image``
        (viva-api#423/#426) -- a dispatch asking for Nextflow against a commit whose
        head image was never built fails at the Batch pull, which is why the
        submitter names the tag explicitly rather than reusing ``_image_uri``.
        """
        settings = get_settings()
        registry = f"{settings.ecr_account_id}.dkr.ecr.{settings.batch_region}.amazonaws.com"
        return f"{registry}/{settings.ray_ecr_repository}:{commit}-submit"

    def _awsbatch_nf_params(
        self, commit: str, experiment_id: str, task_env: dict[str, str] | None = None
    ) -> dict[str, Any]:
        """The `awsbatch` profile's inputs, derived from settings -- never from the request.

        These name the deployment's queue, registry and work bucket, so they are
        server-side facts rather than something a caller supplies. Missing ones raise
        here, at dispatch, instead of surfacing as an AWS error minutes later inside a
        Batch container.

        ``container_image`` is the PLAIN science image, not the ``-submit`` head: only
        the process running ``nextflow run`` needs a JVM, and the task container needs
        only the AWS CLI to stage the S3 work dir (which v2ecoli's Dockerfile installs).

        ``container_env`` carries ``PYTHONPATH``, which is not optional: Nextflow moves
        the task's cwd off ``/app/v2ecoli``, and v2ecoli bare-imports ``scripts.*``
        throughout. viva-api#359 fixed this for the chain/Ray paths by way of
        ``PBG_RUNNER_ENV``, which a Nextflow-emitted process block has no idea exists.
        It rides in ``container_env`` rather than a dedicated profile directive because
        it is a fact about THIS image, not about AWS Batch -- process-bigraph#204 keeps
        the profile free of any one consumer's layout.
        """
        settings = get_settings()
        missing = [
            name
            for name, value in (
                ("batch_amd64_queue", settings.batch_amd64_queue),
                ("s3_work_bucket", settings.s3_work_bucket),
                ("ecr_account_id", settings.ecr_account_id),
            )
            if not value
        ]
        if missing:
            raise ValueError(
                f"nextflow_dispatch executor='awsbatch' needs settings {', '.join(missing)}; "
                f"without them the profile renders with nulls and fails at submission."
            )
        return {
            "container_image": self._image_uri(commit),
            "queue": settings.batch_amd64_queue,
            "aws_region": settings.batch_region,
            # GovCloud's S3 endpoint. Emitting it natively is what retires the `sed`
            # that injects the same line into vEcoli's config.template
            # (simulation_service_k8s.py).
            "s3_endpoint": f"https://s3.{settings.batch_region}.amazonaws.com",
            # task_env (sms-ecoli#166) rides in the same directive: the renderer emits
            # every entry as a `--env K=V` containerOption on the awsbatch profile's
            # process scope, so it reaches EVERY Batch task (parca, lineage,
            # analysis) -- not the K8s head, which needs none of it. The service's
            # own two keys are listed first so a request cannot shadow them (the
            # validator refuses those names anyway).
            "container_env": {"PYTHONPATH": V2ECOLI_DIR, "V2E_ROOT": V2ECOLI_DIR, **(task_env or {})},
            "work_dir": f"s3://{settings.s3_work_bucket}/{settings.s3_work_prefix}/{experiment_id}/work",
        }

    def _nf_session_s3_uri(self, experiment_id: str) -> str:
        """Where this campaign's Nextflow session cache lives between dispatches.

        Beside the work dir and keyed the same way, because the two are only
        useful together: `-resume` matches a task by its hash in the SESSION and
        then reuses the outputs in the WORK DIR. Either one alone resumes
        nothing.
        """
        settings = get_settings()
        return f"s3://{settings.s3_work_bucket}/{settings.s3_work_prefix}/{experiment_id}/session"

    @staticmethod
    def _nf_generator_params(params: dict[str, Any] | None, run_id: str) -> dict[str, Any]:
        """The composite generator's parameters, with `experiment_id` defaulted to the run.

        `workflow_nf` defaults its own `experiment_id` to the literal string
        "workflow_nf", and that value is not cosmetic: it becomes the
        `experiment_id=` HIVE PARTITION the emitters write. Left unset, every
        campaign's parquet claims the same experiment_id -- observed on sim 392 as
        `sweep/workflow_nf/history/experiment_id=workflow_nf/...`.

        That is the same collision Chris and Alex spent a day chasing on the Ray
        path (sms-ecoli#235, viva-api#450), reappearing one layer down, inside the
        artifact rather than in its S3 prefix.

        A caller may still set it explicitly; this only supplies the default.
        """
        merged = dict(params or {})
        merged.setdefault("experiment_id", run_id)
        return merged

    def _render_nf_command(
        self,
        *,
        runner_s3_uri: str,
        composite_id: str,
        params: dict[str, Any] | None,
        executor: str,
        launch: bool,
        outdir: str,
        pbg_runner_s3_uri: str,
        nf_params: dict[str, Any] | None = None,
        resources: dict[str, dict[str, Any]] | None = None,
        work_dir: str | None = None,
        resume: bool = False,
        stage_out_s3: str | None = None,
        session_s3: str | None = None,
        nextflow_args: list[str] | None = None,
    ) -> str:
        """The container command: fetch the compiler, render, optionally launch.

        ``--executor local`` is the intended FIRST check (Phase 3 of
        docs/plan-nextflow-dispatch.md): it answers "does render+launch work in our
        real image" separately from "does the awsbatch executor work", so a failure
        has one candidate cause rather than two.
        """
        overrides_flag = ""
        if params:
            overrides_flag = f" --overrides {shlex.quote(json.dumps(params))}"
        # ``params`` parameterizes the COMPOSITE generator; ``nf_params`` configures
        # NEXTFLOW itself (queue, image, region, work dir). Two different things that
        # both got called "params" upstream, so they are kept separate on the wire.
        nf_params_flag = ""
        if nf_params:
            nf_params_flag = f" --nf-params {shlex.quote(json.dumps(nf_params))}"
        # Verbatim passthrough to `nextflow run` -- `-dump-hashes` above all, which
        # prints each component of a task hash and is the only way to see WHY a
        # `-resume` did not match. A list, never a string, for the same reason
        # deploy() insists on one.
        nextflow_args_flag = ""
        if nextflow_args:
            nextflow_args_flag = f" --nextflow-args {shlex.quote(json.dumps(list(nextflow_args)))}"
        resources_flag = ""
        if resources:
            resources_flag = f" --resources {shlex.quote(json.dumps(resources))}"
        launch_flag = " --launch" if launch else ""
        resume_flag = " --resume" if resume else ""
        work_dir_flag = f" --work-dir {shlex.quote(work_dir)}" if work_dir else ""
        # On the Batch container path an entrypoint synced out_dir -> out_s3. A
        # K8s pod has no such entrypoint, so the command stages its own results
        # -- and MUST preserve the exit code, or a failed render would be
        # reported as a success by the trailing copy. `|| true` on the copy
        # keeps a staging hiccup from masking a run that actually worked.
        # `-resume` needs a durable SESSION, not just a durable work dir. Nextflow
        # keeps `.nextflow/history` and its cache DB in the LAUNCH directory --
        # here an ephemeral pod -- so a second dispatch starts with no record of
        # the first and reports, verbatim:
        #
        #   WARN: It appears you have never run this project before
        #         -- Option `-resume` is ignored
        #
        # and re-runs a ParCa whose output is sitting complete in the work dir.
        # Measured on simulation 359; plan-nextflow-dispatch risk 2 called it.
        # So the session is restored before the run and saved after, keyed by the
        # same experiment the work dir is.
        session_restore = ""
        session_save = ""
        if session_s3:
            session_restore = (
                f" && (aws s3 cp --recursive {shlex.quote(session_s3)} {shlex.quote(outdir)}/.nextflow"
                f" --only-show-errors 2>/dev/null || true)"
            )
            # Saved unconditionally -- a FAILED run's session is exactly the one a
            # `-resume` needs, so guarding this on success would defeat the point.
            session_save = (
                f" ; aws s3 cp --recursive {shlex.quote(outdir)}/.nextflow {shlex.quote(session_s3)}"
                f" --only-show-errors || true"
            )

        stage_out = ""
        if stage_out_s3:
            stage_out = (
                f" ; NF_EXIT=$?"
                f"{session_save}"
                f" ; aws s3 cp --recursive {shlex.quote(outdir)} {shlex.quote(stage_out_s3)}"
                f" --only-show-errors || true"
                f" ; exit $NF_EXIT"
            )
        # A trace is how a resumed run is told apart from a repeated one: a reused
        # task reports CACHED there and nowhere else.
        return (
            f"cd {V2ECOLI_DIR}"
            f"{session_restore}"
            f" && aws s3 cp {shlex.quote(runner_s3_uri)} /tmp/render_nf.py"
            # render_nf reuses run_pbg's resolver, and the simulator image has no
            # `viva_api` -- so the sibling it imports has to be staged too, into
            # the SAME directory. Staging only render_nf fails at import, after a
            # successful pull and a clean start.
            f" && aws s3 cp {shlex.quote(pbg_runner_s3_uri)} /tmp/run_pbg.py"
            f" && python /tmp/render_nf.py"
            f" --composite-id {shlex.quote(composite_id)}"
            f" --outdir {shlex.quote(outdir)}"
            f" --executor {shlex.quote(executor)}"
            f" --trace {shlex.quote(outdir)}/trace.csv"
            f"{overrides_flag}{nf_params_flag}{resources_flag}{launch_flag}{resume_flag}{work_dir_flag}"
            f"{nextflow_args_flag}"
            f"{stage_out}"
        )

    async def _submit_nextflow_dispatch(
        self,
        ecoli_simulation: Simulation,
        database_service: DatabaseService,
        nf_dispatch: dict[str, Any],
        *,
        correlation_id: str | None = None,
    ) -> JobId:
        """Compile a registered composite to a Nextflow workflow and run it.

        The third dispatch path (docs/plan-nextflow-dispatch.md). The HEAD runs
        as a **K8s Job**, not as a Batch container job, and that is a permission
        fact rather than a preference:

        * A Batch-hosted head runs under the job definition's ``jobRoleArn``,
          which on this stack is ``smsvpctest-ray-mnp-job`` -- S3 on the shared
          bucket and **no ``batch:*`` whatsoever**. It could pull its image,
          parse its config and start, then fail at the first task submission.
        * None of the four roles viva-api may ``iam:PassRole`` fixes that: two
          carry no ``batch:*``, and the two that do (the IRSA submit role, the
          compute role) trust the EKS OIDC provider and ``ec2.amazonaws.com``
          respectively, so neither can be an ECS/Batch task role at all.
        * A K8s Job with ``serviceAccountName: batch-submit`` inherits the IRSA
          identity that **is** allowed to submit -- the same one vEcoli's
          Nextflow head has always used (``simulation_service_k8s.py``).

        It also fits the workload: the head submits and waits, so it wants
        500m/1Gi for hours, not a 16-vCPU Batch instance held idle.
        """
        simulator = await database_service.get_simulator(simulator_id=ecoli_simulation.simulator_id)
        if simulator is None:
            raise ValueError(f"Simulator {ecoli_simulation.simulator_id} not found")

        # Re-checked here, not only at the API boundary: this method is also
        # reachable directly, and it is the last point before a Job is created.
        validate_nextflow_dispatch(nf_dispatch)
        composite_id = nf_dispatch.get("composite_id")
        # The request's env, plus the run's PBG_* identity UNDER it (observability
        # plan D4a): every Batch task of this campaign gets the same trace id.
        task_env = with_events_env(
            resolve_task_env(ecoli_simulation.config, nf_dispatch),
            correlation_id=correlation_id,
            experiment_id=str(ecoli_simulation.experiment_id),
            sim_id=ecoli_simulation.database_id,
            backend="nextflow",
            settings=get_settings(),
        )

        commit = simulator.git_commit_hash
        # The RUN's own id, read from the simulation record rather than from the
        # config. Since #450 force-assigns the config's `experiment_id` these agree,
        # but they agree by way of a coupling nothing here would notice breaking --
        # and this path keys a *cache* on it, so a silent re-collision would resume
        # one campaign's tasks into another's. The record is the authority (viva-api#439).
        run_id = str(ecoli_simulation.experiment_id)
        outdir = f"{V2ECOLI_DIR}/nf-render"

        # Which campaign's work dir and session cache this dispatch joins.
        #
        # Every dispatch gets its own by default: two campaigns sharing one prefix
        # share the cache that decides what gets recomputed, and because Nextflow
        # task hashes are content-derived, a `-resume` in one could legitimately
        # match and reuse a task from the other -- silently, reported as `Cached`.
        #
        # Which makes `-resume` explicit rather than implicit. Before #450 a config's
        # baked `experiment_id` collided every dispatch onto one prefix, so a resume
        # found the previous run's session by accident. Now it would find nothing,
        # and Nextflow does not treat that as an error -- it warns "Option `-resume`
        # is ignored" and silently re-runs the entire campaign at full cost. So a
        # resume must NAME the run it continues.
        resume = bool(nf_dispatch.get("resume", False))
        resume_from = nf_dispatch.get("resume_from")
        campaign_key = str(resume_from) if resume_from else run_id

        if self._k8s is None:
            raise RuntimeError(
                "nextflow_dispatch runs its head as a K8s Job (it needs the batch-submit "
                "ServiceAccount to submit Batch tasks), but k8s_job_namespace is not configured"
            )
        runner_s3_uri = await self.stage_render_nf(run_id)
        pbg_runner_s3_uri = await self.stage_runner(run_id)
        executor = str(nf_dispatch.get("executor", "local"))
        nf_params: dict[str, Any] | None = None
        work_dir = nf_dispatch.get("work_dir")
        if executor == "awsbatch":
            nf_params = self._awsbatch_nf_params(commit, campaign_key, task_env=task_env)
            if task_env:
                logger.info("Nextflow dispatch %s: task_env passthrough %s", run_id, task_env)
            # Retry counts are the caller's to tune; the deployment's identity is not.
            for key in ("max_spot_attempts", "max_transfer_attempts", "max_retries"):
                if nf_dispatch.get(key) is not None:
                    nf_params[key] = nf_dispatch[key]
            # `-work-dir` on the command line wins over the profile's `workDir`; both
            # are set so a config lifted out of the render dir and run by hand behaves
            # the same as the dispatch did.
            work_dir = work_dir or nf_params["work_dir"]
            # Where `publishDir` copies task outputs. Without it a campaign that
            # exits 0 leaves its science in the work dir under a content hash --
            # measured at 633 MB across 43 objects, against 78 KB of render
            # artifacts in the results prefix (viva-api#439's verification).
            # The RUN's prefix, not the campaign's: a resumed run reuses another
            # run's cached TASKS, but its results are its own.
            nf_params["publish_dir"] = self._results_s3_uri(run_id).rstrip("/")
        command = self._render_nf_command(
            runner_s3_uri=runner_s3_uri,
            pbg_runner_s3_uri=pbg_runner_s3_uri,
            composite_id=str(composite_id),
            params=self._nf_generator_params(nf_dispatch.get("params"), run_id),
            executor=executor,
            launch=bool(nf_dispatch.get("launch", False)),
            outdir=outdir,
            nf_params=nf_params,
            # Defaults MERGED per label, not replaced wholesale: a caller who
            # overrides `lineage` must not silently lose parca's memory scaling.
            resources=_merge_nf_resources(nf_dispatch.get("resources")),
            work_dir=work_dir,
            resume=resume,
            stage_out_s3=self._results_s3_uri(run_id),
            session_s3=self._nf_session_s3_uri(campaign_key),
            nextflow_args=nf_dispatch.get("nextflow_args"),
        )
        # The Job names the RUN, never the campaign: each dispatch is its own pod,
        # and a name colliding with a live Job fails the create outright.
        job_name = self._nf_head_job_name(run_id)
        self._k8s.create_job(self._nf_head_job(job_name, run_id, commit, command))
        logger.info(
            "Created Nextflow head Job %s for run %s (campaign %s)",
            job_name,
            run_id,
            campaign_key,
        )
        # NOT JobId.ray: the value is a Job name, and NOT JobId.k8s either --
        # that tag also selects vEcoli's output layout on the download path.
        return JobId.k8s_nextflow(job_name)

    @staticmethod
    def _nf_head_job_name(experiment_id: str) -> str:
        """A DNS-1123 label: lowercase alphanumerics and '-', at most 63 chars.

        K8s rejects the underscores and uppercase that experiment ids carry, and
        it rejects them at create time -- so the sanitising happens here rather
        than surfacing as an ApiException on a dispatch that otherwise worked.
        """
        safe = re.sub(r"[^a-z0-9-]+", "-", experiment_id.lower()).strip("-")
        return f"nf-{safe}-{_rand_suffix()}"[:63].rstrip("-")

    def _nf_head_job(self, job_name: str, experiment_id: str, commit: str, command: str) -> Any:
        """The head Job, modelled on vEcoli's (``simulation_service_k8s.py``).

        ``backoff_limit=0`` deliberately: a half-finished Nextflow run is not
        safely restartable from scratch, and re-running the head would resubmit
        every task. Recovery is ``-resume`` on a NEW dispatch, which reuses the
        cached successful tasks -- that is the whole point of the session cache.
        """
        from kubernetes import client as k8s_client

        settings = get_settings()
        return k8s_client.V1Job(
            metadata=k8s_client.V1ObjectMeta(
                name=job_name,
                labels={
                    "app": "sms-api",
                    "job-type": "nextflow-head",
                    "experiment-id": re.sub(r"[^a-z0-9.-]+", "-", experiment_id.lower())[:63],
                },
            ),
            spec=k8s_client.V1JobSpec(
                backoff_limit=0,
                ttl_seconds_after_finished=86400,  # 24h, for log access after it ends
                template=k8s_client.V1PodTemplateSpec(
                    spec=k8s_client.V1PodSpec(
                        # The whole reason the head is here and not on Batch.
                        service_account_name="batch-submit",
                        restart_policy="Never",
                        # Nextflow's shutdown hook calls Batch TerminateJob once per
                        # in-flight task on SIGTERM. The default 30 s is not enough for
                        # a wide campaign, and the pod is SIGKILLed mid-way: measured on
                        # simulation 441, where 8 of 10 lineage tasks survived the
                        # cancel and ran for a further ~100 minutes, filling host disk
                        # until they broke the NEXT campaign (viva-api#472).
                        termination_grace_period_seconds=NF_HEAD_TERMINATION_GRACE_SECONDS,
                        containers=[
                            k8s_client.V1Container(
                                name="nextflow-head",
                                image=self._submit_image_uri(commit),
                                command=["/bin/bash", "-c", command],
                                env=[
                                    k8s_client.V1EnvVar(name="AWS_DEFAULT_REGION", value=settings.batch_region),
                                    k8s_client.V1EnvVar(name="AWS_REGION", value=settings.batch_region),
                                    k8s_client.V1EnvVar(name="AWS_STS_REGIONAL_ENDPOINTS", value="regional"),
                                    k8s_client.V1EnvVar(name="NXF_ANSI_LOG", value="false"),
                                    # The head RESOLVES the composite, so it needs the
                                    # workspace's own core builder and import root -- the
                                    # generic core registers only process-bigraph's base
                                    # types, and a nested Composite then fails to realize
                                    # (`no link found at address: local:composite`).
                                    # These live in PBG_RUNNER_ENV for the chain/Ray
                                    # paths (#359), which a K8s Job never sees; §Phase 0
                                    # of the plan predicted exactly this for PYTHONPATH.
                                    k8s_client.V1EnvVar(name="PBG_CORE_BUILDER", value=V2ECOLI_CORE_BUILDER),
                                    k8s_client.V1EnvVar(name="PYTHONPATH", value=V2ECOLI_DIR),
                                    # The checkout root, for artefacts that ship in the
                                    # repo rather than the wheel (v2ecoli resolves
                                    # scripts/build_cache.py against it at RENDER time).
                                    k8s_client.V1EnvVar(name="V2E_ROOT", value=V2ECOLI_DIR),
                                ],
                                resources=k8s_client.V1ResourceRequirements(
                                    requests={"cpu": "500m", "memory": "1Gi"},
                                    limits={"cpu": "1", "memory": "2Gi"},
                                ),
                            ),
                        ],
                    ),
                ),
            ),
        )

    def _mbp_tracked_command(
        self,
        *,
        variant: str,
        max_generations: int | None,
        duration_sec: int | None,
        chunk: int | None,
        emitter: str,
        cache_dir: str,
        single_daughters: bool,
        carbon_exhaustion_arrest: bool,
        seed: int | None = None,
        cells_per_agent: float | None = None,
        initial_glucose_mM: float | None = None,
        initial_ammonium_mM: float | None = None,
        injected_processes: str | None = None,
        reactor_config: str | None = None,
        aeration_schedule: str | None = None,
        aeration_trigger: str | None = None,
    ) -> str:
        """The container command for a ``run_mbp_tracked.py`` dispatch (backlog item
        105/106's own Run 1 sibling gap: the coupled composite's real missing-output
        fix -- Alex's Option 1 decision, 2026-09-06).

        ``run_mbp_tracked.py`` (v2ecoli#695) already lives in the image
        (``scripts/run_mbp_tracked.py``) -- no runner staging needed here, unlike
        ``run_pbg.py``/``render_nf.py``. ``V2E_STUDIES_ROOT`` is set to a path under
        ``SIM_OUT_DIR``, the ONE directory the entrypoint actually syncs to S3 --
        without this, a run's parquet lands under the image's own
        ``REPO_ROOT/studies`` and is silently discarded on container exit (confirmed
        root cause: Dispatch 322 ran ``reactor_bird_coupled`` cleanly for ~3h with
        zero retrievable output for exactly this reason).

        A prior fix attempt (declaring an emitter directly on the composite
        generator) is structurally impossible for this composite --
        ``_merge_emit_paths`` roots each declared path at its first segment as a
        top-level store, and 3 of the 6 real paths (``listeners/mass/*``,
        ``boundary/external/*``) live under ``agents/0/``, not top-level, so they
        silently drop no matter what is declared (v2ecoli#700, closed/superseded).
        ``run_mbp_tracked.py``'s own runtime emitter is the one mechanism confirmed
        (locally, by Eran) to carry all 6 real paths and survive division.

        The last 7 params (added 2026-09-06, real Run 1 dispatch per Chris's own
        exact spec on sms-ecoli#210): Dispatch 370's own request only needed
        ``variant``/``max_generations`` -- the real coupled experiment additionally
        needs a per-lineage ``--seed`` (which hive-partitions the parquet output;
        a mismatched/repeated seed across dispatches silently MERGES rather than
        erroring, real data loss), 3 environment overrides, and 3 file-path
        arguments the runner enforces as absolute. ``injected_processes``/
        ``reactor_config``/``aeration_schedule`` are given as paths RELATIVE to
        ``V2ECOLI_DIR`` (matching how every other file reference in this dispatch
        family is expressed) and resolved to absolute here, once, rather than
        pushing that concern onto every caller.

        ``aeration_trigger`` (added 2026-09-10, sms-ecoli#334's real recalibration):
        the local ``run_mbp_tracked.py --aeration-schedule`` flag requires an
        explicit ``--aeration-trigger`` (``biomass``|``time``) alongside it --
        without it, ``load_aeration_schedule()`` exits nonzero rather than
        guessing. This dispatch path passed ``aeration_schedule`` through since
        2026-09-06 with no way to also pass its required companion flag, a real
        gap only surfaced once a second aeration schedule (kLa 350) needed firing
        remotely -- every prior remote coupled dispatch used the one schedule
        this gap never affected. Only emitted when ``aeration_schedule`` is also
        set, matching the local script's own coupling between the two.
        """
        max_gens_flag = f" --max-generations {int(max_generations)}" if max_generations is not None else ""
        duration_flag = f" --duration-sec {int(duration_sec)}" if duration_sec is not None else ""
        chunk_flag = f" --chunk {int(chunk)}" if chunk is not None else ""
        daughters_flag = "" if single_daughters else " --no-single-daughters"
        arrest_flag = " --carbon-exhaustion-arrest" if carbon_exhaustion_arrest else ""
        seed_flag = f" --seed {int(seed)}" if seed is not None else ""
        cells_per_agent_flag = f" --cells-per-agent {cells_per_agent}" if cells_per_agent is not None else ""
        glucose_flag = f" --initial-glucose-mM {initial_glucose_mM}" if initial_glucose_mM is not None else ""
        ammonium_flag = f" --initial-ammonium-mM {initial_ammonium_mM}" if initial_ammonium_mM is not None else ""
        injected_processes_flag = (
            f" --injected-processes {shlex.quote(f'{V2ECOLI_DIR}/{injected_processes}')}" if injected_processes else ""
        )
        reactor_config_flag = (
            f" --reactor-config {shlex.quote(f'{V2ECOLI_DIR}/{reactor_config}')}" if reactor_config else ""
        )
        aeration_schedule_flag = (
            f" --aeration-schedule {shlex.quote(f'{V2ECOLI_DIR}/{aeration_schedule}')}" if aeration_schedule else ""
        )
        aeration_trigger_flag = (
            f" --aeration-trigger {shlex.quote(aeration_trigger)}" if aeration_schedule and aeration_trigger else ""
        )
        studies_root = f"{SIM_OUT_DIR}/studies"
        return (
            f"cd {V2ECOLI_DIR}"
            f" && V2E_STUDIES_ROOT={shlex.quote(studies_root)} python scripts/run_mbp_tracked.py"
            f" --variant {shlex.quote(variant)}"
            f" --emitter {shlex.quote(emitter)}"
            f" --cache-dir {shlex.quote(cache_dir)}"
            f"{max_gens_flag}{duration_flag}{chunk_flag}{daughters_flag}{arrest_flag}"
            f"{seed_flag}{cells_per_agent_flag}{glucose_flag}{ammonium_flag}"
            f"{injected_processes_flag}{reactor_config_flag}{aeration_schedule_flag}{aeration_trigger_flag}"
        )

    async def _submit_mbp_tracked_dispatch(
        self,
        ecoli_simulation: Simulation,
        database_service: DatabaseService,
        mbp_dispatch: dict[str, Any],
        *,
        correlation_id: str | None = None,
    ) -> JobId:
        """Dispatch a ``run_mbp_tracked.py`` variant remotely (Run 1's real
        missing-output fix, Alex's Option 1 decision, 2026-09-06 -- see
        ``_mbp_tracked_command``'s own docstring for why this mechanism and not a
        declared-emitter composite edit).

        Single-container job (matching ``reactor_bird_coupled``'s own confirmed
        non-``ray:``-distributed, single-process nature) -- mirrors
        ``_submit_nextflow_dispatch``'s exact shape, minus runner staging (the
        script already lives in the image).

        ``cache_variant`` mirrors the item105/106 guard (viva-api#437): a variant
        cache is meant to already exist -- checked via S3 existence before
        submitting anything, exactly like the multi-node-composite path, so this
        new dispatch path does not reproduce the exact class of bug #437 fixed
        there (the standing parity-check discipline, applied at build time rather
        than found later). Omitted (every existing caller) preserves the plain
        per-commit ParCa cache, byte-for-byte unaffected.
        """
        simulator = await database_service.get_simulator(simulator_id=ecoli_simulation.simulator_id)
        if simulator is None:
            raise ValueError(f"Simulator {ecoli_simulation.simulator_id} not found")

        variant = mbp_dispatch.get("variant")
        if not variant:
            raise ValueError("mbp_dispatch.variant is required")

        settings = get_settings()
        commit = simulator.git_commit_hash
        experiment_id = str(ecoli_simulation.config.experiment_id)
        cache_variant = mbp_dispatch.get("cache_variant") or None
        task_env = with_events_env(
            resolve_task_env(ecoli_simulation.config, mbp_dispatch),
            correlation_id=correlation_id,
            experiment_id=experiment_id,
            sim_id=ecoli_simulation.database_id,
            backend="mbp",
            settings=settings,
        )
        cache_s3 = self.cache_s3_uri(commit, variant=cache_variant)

        job_def = self._ensure_container_job_def(self._image_uri(commit), commit)

        base_tags = {
            "Project": "v2ecoli-mbp-tracked",
            "ExperimentId": experiment_id[:255],
            "Variant": str(variant)[:255],
            "Commit": str(commit)[:12],
            # Makes the resolved cache choice visible on the job itself (sms-
            # ecoli#210, cplong90) -- "stock" or the requested variant name,
            # instead of requiring a manual decode of the staged S3 path.
            "CacheVariant": str(cache_variant or "stock")[:255],
            "Team": getattr(settings, "cost_team_tag", None) or "covertlab",
        }

        parca_job_id: str | None
        if cache_variant:
            from viva_api.dependencies import get_file_service

            file_service = get_file_service()
            if file_service is None:
                raise RuntimeError("FileService not initialized; cannot verify cache_variant staging.")
            existing = await file_service.get_listing(S3FilePath(s3_path=Path(data_layout.key_from_uri(cache_s3))))
            if not existing:
                raise ValueError(
                    f"cache_variant={cache_variant!r} has no staged ParCa cache at commit "
                    f"{commit!r} ({cache_s3}). This dispatch path never builds a variant "
                    f"cache itself -- build it first via POST /parca/new-gene-cache or an "
                    f"external bridge sync, then retry."
                )
            parca_job_id = None
        else:
            parca_job_id = self._submit_container(
                job_name=f"mbp-parca-{commit}-{_rand_suffix()}",
                job_definition=job_def,
                job_cmd=self._parca_command(),
                out_s3=cache_s3,
                out_dir=PARCA_CACHE_DIR,
                tags={**base_tags, "Phase": "parca"},
                task_env=task_env,
            )

        command = self._mbp_tracked_command(
            variant=str(variant),
            max_generations=mbp_dispatch.get("max_generations"),
            duration_sec=mbp_dispatch.get("duration_sec"),
            chunk=mbp_dispatch.get("chunk"),
            emitter=str(mbp_dispatch.get("emitter") or "parquet"),
            cache_dir=PARCA_CACHE_DIR,
            single_daughters=bool(mbp_dispatch.get("single_daughters", True)),
            carbon_exhaustion_arrest=bool(mbp_dispatch.get("carbon_exhaustion_arrest", False)),
            seed=mbp_dispatch.get("seed"),
            cells_per_agent=mbp_dispatch.get("cells_per_agent"),
            initial_glucose_mM=mbp_dispatch.get("initial_glucose_mM"),
            initial_ammonium_mM=mbp_dispatch.get("initial_ammonium_mM"),
            injected_processes=mbp_dispatch.get("injected_processes"),
            reactor_config=mbp_dispatch.get("reactor_config"),
            aeration_schedule=mbp_dispatch.get("aeration_schedule"),
            aeration_trigger=mbp_dispatch.get("aeration_trigger"),
        )
        job_id = self._submit_container(
            job_name=f"mbp-tracked-{experiment_id}-{_rand_suffix()}"[:128],
            job_definition=job_def,
            job_cmd=command,
            out_s3=self._results_s3_uri(experiment_id),
            out_dir=SIM_OUT_DIR,
            stage_s3=cache_s3,
            stage_dir=PARCA_CACHE_DIR,
            depends_on=[parca_job_id] if parca_job_id else None,
            tags={**base_tags, "Phase": "mbp_tracked"},
            task_env=task_env,
        )
        return JobId.ray(job_id)

    async def stage_runner(self, experiment_id: str) -> str:
        """Upload the generic run_pbg.py runner to S3 for this experiment; return its URI.

        Mirrors ``viva_api.compose.simulation_service_ray.ComposeSimulationServiceRay``'s
        own runner staging exactly (same source, same per-experiment S3 layout) -- the
        multi-generation batch path below downloads and runs it the identical way a
        compose job does. Staged (not embedded via heredoc) because AWS Batch caps a
        container override command at 8192 bytes.

        The returned URI is fully DETERMINISTIC from ``experiment_id`` alone (the S3
        key has no random component) — safe, cheap, and idempotent to call again
        rather than cache: ``JobScheduler``'s per-tick chain-dispatch advance
        (backlog item 71 Phase 4) calls this fresh whenever it's about to submit a
        generation, rather than persisting the URI anywhere.
        """
        from viva_api.dependencies import get_file_service

        file_service = get_file_service()
        if file_service is None:
            raise RuntimeError("FileService not initialized; cannot stage run_pbg.py to S3.")
        exp_prefix = data_layout.RayLayout.experiment_prefix(experiment_id)
        runner_key = f"{exp_prefix}/run_pbg.py"
        with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as tmp:
            tmp.write(_RUNNER_SRC)
            runner_local = tmp.name
        try:
            await file_service.upload_file(Path(runner_local), S3FilePath(s3_path=Path(runner_key)))
        finally:
            Path(runner_local).unlink(missing_ok=True)
        return data_layout.s3_uri(runner_key)

    def _sim_command(
        self,
        n_seeds: int,
        n_steps: int,
        chunk: int,
        *,
        composite: CompositeEngine | None = None,
        condition: str | None = None,
        max_generations: int | None = None,
        vecoli_source: VecoliSource | None = None,
        n_generations: int = 1,
        experiment_id: str | None = None,
        runner_s3_uri: str | None = None,
        injected_processes: dict[str, Any] | None = None,
        variants: dict[str, Any] | None = None,
        config_overrides: dict[str, Any] | None = None,
        features: list[Any] | None = None,
        exchange_fluxes: dict[str, Any] | None = None,
        exchange_flux_basis: str | None = None,
    ) -> str:
        # When ``composite`` is set, run the two-engine comparison driver — both
        # engines (v2ecoli port + vEcoli imported via build_composite_native)
        # as bigraph composites on Ray, emitting only the compact XArray view →
        # zarr/S3. Otherwise: single-generation phase0 by default, or the real
        # multi-generation LineageProcess/batch_baseline_runner pipeline when
        # the caller actually requested more than one generation.
        #
        # Engine selection is decoupled from generation count: ``max_generations``
        # defaults to 1 (the phase0 single-gen baseline), so picking an engine
        # never implies a multi-generation comparison-ensemble run by itself —
        # callers opt into more generations explicitly.
        if composite:
            # vecoli_source selects HOW the genuine vEcoli side runs (only meaningful
            # for --composite vecoli): "upstream" (default, ~50 pbg steps) or
            # "vivarium-process" (vEcoli as ONE pbg node with vivarium-core's Engine
            # inside — faithful by construction). Both stage the SAME upstream ParCa
            # cache (is_upstream routing is unchanged), so only the driver flag differs.
            src = f" --vecoli-source {vecoli_source}" if (_is_upstream_vecoli(composite) and vecoli_source) else ""
            return (
                f"cd {V2ECOLI_DIR} && python scripts/run_comparison_ensemble.py"
                f" --composite {composite} --condition {condition or 'basal'}"
                f" --n-seeds {n_seeds} --max-generations {int(max_generations or 1)}"
                f" --chunk {chunk} --out-root {SIM_OUT_DIR} --mode ray{src}"
            )
        if int(n_generations) > 1:
            # Real multi-generation lineage (cell division across generations) --
            # scripts/run_phase0_xarray_ensemble.py below silently ignores this
            # entirely and only ever runs one generation per seed.
            #
            # Dispatched as a registered process-bigraph composite (sms-ecoli's
            # v2ecoli/composites/ecoli_baseline.py's `baseline()` — n_seeds/
            # n_generations > 1 switches it into the batch/lineage shape that used
            # to be the standalone batch_baseline composite before it was folded in
            # here, backlog item 55) run through the SAME generic run_pbg.py every
            # compose-on-Batch job already uses — not a v2ecoli-specific CLI script.
            # See backlog items 26/27: this is the one execution mechanism both the
            # ensemble endpoint and the generic compose endpoint dispatch through;
            # only the composite id + overrides differ per caller.
            if not experiment_id:
                raise RuntimeError("experiment_id is required for multi-generation batch dispatch")
            if not runner_s3_uri:
                raise RuntimeError(
                    "runner_s3_uri is required for multi-generation batch dispatch "
                    "(the generic run_pbg.py runner must be staged to S3 first)"
                )
            overrides: dict[str, Any] = {
                "n_seeds": int(n_seeds),
                "n_generations": int(n_generations),
                "cache_dir": PARCA_CACHE_DIR,
                "out_dir": SIM_OUT_DIR,
                "experiment_id": experiment_id,
                # The analysis DAG node (see _analysis_command) runs the same
                # analysis_runner.run_analyses() over the LANDED S3 sweep once this
                # job has succeeded -- skip the composite's own inline flush here so
                # the analyses run exactly once, over the whole sweep.
                "analyses": "none",
                "parallel": "ray",
            }
            # Thread the submitted config's DOMAIN fields into the batch composite's
            # own overrides so the native ``ecoli_baseline.baseline()`` batch run
            # actually carries the metabolism-redux/violacein swap (the CD2 native
            # seam). Without these keys the composite runs a plain basal baseline
            # even though the config requested a swap -- the composite never sees it.
            #
            # These are exactly ``ecoli_baseline.baseline()``'s own batch-mode kwargs
            # (v2ecoli #640 threaded injected_processes/features/exchange_fluxes/
            # exchange_flux_basis through ``_build_batch_document``; config_overrides
            # and variants already existed). Each is added ONLY when non-empty, so a
            # config with no injection/variant intent produces the exact overrides
            # dict this path built before -- the byte-for-byte-unchanged regression
            # property (mirrors ``injected_processes_from_config``'s own contract and
            # the 0.9.79 chain-dispatch passthrough).
            #
            # ``injected_processes`` is the mapped shape ``baseline()`` expects
            # ({swap_processes, add_processes, exclude_processes, fork_repo}), built
            # by ``injected_processes_from_config`` from the legacy config's
            # ``swap_processes`` -- the SAME mapping ``_seed_generation_command`` and
            # the chain-dispatch/JobScheduler path already use.
            overrides.update(
                _batch_domain_overrides(
                    injected_processes=injected_processes,
                    variants=variants,
                    config_overrides=config_overrides,
                    features=features,
                    exchange_fluxes=exchange_fluxes,
                    exchange_flux_basis=exchange_flux_basis,
                )
            )
            env = PBG_RUNNER_ENV
            return (
                f"cd {V2ECOLI_DIR}"
                f" && aws s3 cp {runner_s3_uri} /tmp/run_pbg.py"
                f" && {env} python /tmp/run_pbg.py"
                f" --composite-id {V2ECOLI_BATCH_BASELINE_COMPOSITE_ID}"
                f" --overrides {shlex.quote(json.dumps(overrides))} -n 1"
            )
        return (
            f"cd {V2ECOLI_DIR} && python scripts/run_phase0_xarray_ensemble.py"
            f" --n-seeds {n_seeds} --n-steps {n_steps} --chunk {chunk} --parallel ray"
        )

    def _seed_generation_command(
        self,
        *,
        seed: int,
        generation_index: int,
        experiment_id: str,
        runner_s3_uri: str,
        injected_processes: dict[str, Any] | None = None,
        variants: dict[str, Any] | None = None,
        composite_id: str | None = None,
        exchange_fluxes: dict[str, Any] | None = None,
        exchange_flux_basis: str | None = None,
    ) -> str:
        """Build ONE seed's ONE generation's command (backlog item 33 rework —
        per-seed independent job chains, mirroring vEcoli-private's own
        Nextflow task granularity, ``runscripts/nextflow/sim.nf``, where task
        retry at generation granularity IS checkpoint/resume).

        SIMPLER than the per-generation-array design this superseded (formerly
        ``_wave_sim_command``): this job is submitted as its own standalone
        Batch job (see ``submit_chain_dispatch_job``), not one array child
        sharing a command across many indices, so BOTH ``seed`` and
        ``generation_index`` are already known Python-side at SUBMISSION time
        — no ``AWS_BATCH_JOB_ARRAY_INDEX``, no lookup table, no container-start
        shell/python3 merge step at all. The full ``--overrides`` payload is
        computed once, here, and embedded as a single static JSON blob.

        ``initial_carry_state_path``/``daughter_state_out_path`` are exactly
        ``RayLayout.daughter_state_uri``'s own deterministic per-seed,
        per-generation S3 path — generation 0 has no prior generation, so
        ``initial_carry_state_path`` is "" (``LineageProcess`` defaults it to
        "", matching a fresh cell — the validated, non-error case). Every
        generation writes its own daughter state out, including the final
        one — a harmless no-op read by nobody if the chain ends there, cheaper
        than a special case to skip it.

        ``injected_processes``/``variants`` (backlog item 93): generic
        passthrough of ``ecoli_baseline.baseline()``'s own same-named kwargs —
        built by ``injected_processes_from_config``/read off the legacy config
        by this method's callers, never by this method itself. Both default to
        ``None`` and are omitted from ``overrides`` entirely when absent, so
        any caller not passing them builds the exact same command as before
        these params existed.

        ``stop_at_division: True`` (backlog item 103) is unconditional, always
        set: with ``n_seeds=1, n_generations=1`` (this method's own fixed
        values) and no ``stop_at_division``, ``ecoli_baseline.baseline()``'s own
        dispatch gate (``n_seeds>1 or n_generations>1 or stop_at_division``)
        evaluates False, routing every generation through the PLAIN single-cell
        build the composite's own docs call "NO division-stop" -- each job ran
        for exactly 1 simulated second (``-n 1`` below) regardless of whether
        the cell divided, and ``initial_carry_state_path``/
        ``daughter_state_out_path`` (this method's own checkpoint/resume
        mechanism, set above) were silently never consumed, since they only
        apply inside the gated branch. ``stop_at_division=True`` routes through
        the SAME batch/lineage path via ``LineageProcess`` (``generations=1``
        stops the lineage after the first real division, Option A / issue #495)
        -- that path's own checkpoint/resume handling is real and unconditional
        on the daughter-state write (``lineage.py``'s own comment: "a
        one-wave-per-invocation caller (generations=1) always takes the
        'complete' branch below, but still needs THIS generation's daughter
        written out"), so no other change here is needed for the hand-off to
        start working correctly. Confirmed empirically: campaign 171's own real
        production output showed generation 0/5/9 of the same lineage as
        MD5-identical files and ``global_time: 1.0`` after 10 chained
        "generations" -- this fixes that.

        CROSS-REPO CONTRACT: overrides threading these 3 keys through to
        ``v2ecoli/composites/ecoli_baseline.py``'s ``baseline()`` signature (the
        composite this command dispatches through by default, via
        ``V2ECOLI_BATCH_BASELINE_COMPOSITE_ID`` — formerly a dedicated
        ``composites/batch_baseline.py``, folded into ``ecoli_baseline.py`` by
        v2ecoli #373 and finally synced into sms-ecoli by PR #56, backlog item 55)
        is v2ecoli's own responsibility; nothing about that contract is affected by
        this per-seed rework, only WHICH viva-api command builder emits the same
        keys.

        ``composite_id`` (backlog item 105): caller-selectable override for
        ``--composite-id``, defaulting to ``V2ECOLI_BATCH_BASELINE_COMPOSITE_ID``
        when omitted — every existing caller keeps building the exact same
        command as before this param existed. Exists because chain-dispatch
        was previously hardcoded to ``ecoli_baseline`` only; some real
        composites (e.g. ``reactor_bird_coupled``) that ALSO support the
        ``n_seeds``/``n_generations``/``injected_processes``/``variants``
        overrides shape above (v2ecoli #648) were unreachable through this
        entrypoint purely because nothing could ever select them. Not
        validated here — same philosophy as ``injected_processes``/
        ``variants`` above: pure passthrough, v2ecoli's own
        ``composite_spec`` resolution (inside ``run_pbg.py``) is what fails
        loudly on a bad id, not this layer.

        ``out_dir`` is ``RayLayout.seed_results_uri`` (an ``s3://`` URI), not a
        local path: every generation's job for this seed shares it, so the
        composite's own parquet sweep / zarr store / summary.json (sms-ecoli's
        ``v2ecoli/cache.py``, ``workflow/lineage.py``, ``workflow/run.py`` —
        all made S3-URI-aware alongside this) accumulate under one seed-scoped
        S3 prefix instead of colliding on the flat, ensemble-wide
        ``experiment_prefix`` every seed's every generation would otherwise
        share (the real bug item 35's pilot found — every job's ``summary.json``
        clobbering the last one's). ``daughter_state_out_path``/
        ``initial_carry_state_path`` already lived under a per-seed prefix
        (``daughter_state_uri``), unaffected by this.

        ``exchange_fluxes``/``exchange_flux_basis`` (backlog item 105, the K4
        cell-only ensemble): the SAME two params ``_submit_multi_node_composite``
        already threads for pbg-native (item106) -- chain-dispatch never had
        them. Without ``exchange_fluxes`` the composite's own ExchangeFluxListener
        never mounts, so no ``listeners__exchange_flux__*`` columns are written
        AT ALL, with no refusal -- a real, silent measurement gap this project's
        own sms-ecoli collaborators (cplong90) documented precisely:
        "an absent product column has the same artifact signature as a run that
        never worked, and costs real time to tell apart." Pure passthrough,
        both default ``None`` and are omitted from ``overrides`` entirely when
        absent -- every existing caller builds the exact same command as before
        these params existed.
        """
        daughter_state_out_path = data_layout.RayLayout.daughter_state_uri(experiment_id, seed, generation_index)
        initial_carry_state_path = (
            data_layout.RayLayout.daughter_state_uri(experiment_id, seed, generation_index - 1)
            if generation_index > 0
            else ""
        )
        seed_out_dir = data_layout.RayLayout.seed_results_uri(experiment_id, seed).rstrip("/")
        overrides = {
            "n_seeds": 1,
            "n_generations": 1,
            # Backlog item 103: unconditional, always on -- see this method's
            # own docstring. Without it every generation silently ran a plain,
            # non-division-gated 1-simulated-second build regardless of
            # generation_index, and the checkpoint/resume fields below were
            # never actually consumed.
            "stop_at_division": True,
            "cache_dir": PARCA_CACHE_DIR,
            "out_dir": seed_out_dir,
            "experiment_id": experiment_id,
            "analyses": "none",
            "parallel": "",
            # ecoli_baseline.baseline()'s own param is `seed`, not `base_seed` --
            # the latter was correct for the old, now-deleted batch_baseline
            # composite (backlog item 55) but is an unexpected-kwarg TypeError here.
            "seed": int(seed),
            "initial_generation_index": int(generation_index),
            "initial_carry_state_path": initial_carry_state_path,
            "daughter_state_out_path": daughter_state_out_path,
        }
        if injected_processes:
            overrides["injected_processes"] = injected_processes
        if variants:
            overrides["variants"] = variants
        if exchange_fluxes:
            overrides["exchange_fluxes"] = exchange_fluxes
            if exchange_flux_basis:
                overrides["exchange_flux_basis"] = exchange_flux_basis
        env = PBG_RUNNER_ENV
        return (
            f"cd {V2ECOLI_DIR}"
            f" && aws s3 cp {runner_s3_uri} /tmp/run_pbg.py"
            f" && {env} python /tmp/run_pbg.py"
            f" --composite-id {composite_id or V2ECOLI_BATCH_BASELINE_COMPOSITE_ID}"
            f" --overrides {shlex.quote(json.dumps(overrides))} -n 1"
        )

    def _seed_lineage_command(
        self,
        *,
        seed: int,
        n_generations: int,
        experiment_id: str,
        runner_s3_uri: str,
        injected_processes: dict[str, Any] | None = None,
        variants: dict[str, Any] | None = None,
        composite_id: str | None = None,
        exchange_fluxes: dict[str, Any] | None = None,
        exchange_flux_basis: str | None = None,
    ) -> str:
        """Build ONE seed's WHOLE-LINEAGE command: all ``n_generations`` in a
        single ``LineageProcess`` (one Batch job per seed), not one job per
        generation.

        This is the Run-3 fix. ``_seed_generation_command`` runs one generation
        per job (``n_generations=1``, ``stop_at_division=True``) with an S3
        daughter-state checkpoint handed between jobs — so every generation job
        is a *fresh* ``LineageProcess`` whose ``lineage_time_offset`` restarts at
        0.0, and a ``field_timeline`` dose scheduled at a cumulative-lineage time
        (e.g. ``DOSE_ONSET_TIME_S=10000``) is compared against a per-generation
        clock that only reaches ~3000 s — so it NEVER fires (silent no-dose
        control; see docs/design-chain-one-lineageprocess.md).

        Running the whole lineage in ONE ``LineageProcess`` (exactly as the
        Nextflow path already does, and as the ``n_generations>1`` batch shape in
        ``_sim_command`` does) makes ``lineage_time_offset`` accumulate across
        generations, so the dose fires at its intended cumulative time. Division
        is in-process, so NO ``initial_generation_index`` /
        ``initial_carry_state_path`` / ``daughter_state_out_path`` and NO
        ``stop_at_division`` — ``baseline()``'s own ``n_seeds>1 or
        n_generations>1`` gate routes ``n_generations>1`` through the
        batch/lineage shape. ``seed``/``injected_processes``/``variants``/
        ``exchange_fluxes`` are threaded identically to
        ``_seed_generation_command`` (same per-seed S3 out layout).
        """
        seed_out_dir = data_layout.RayLayout.seed_results_uri(experiment_id, seed).rstrip("/")
        overrides: dict[str, Any] = {
            "n_seeds": 1,
            "n_generations": int(n_generations),
            "cache_dir": PARCA_CACHE_DIR,
            "out_dir": seed_out_dir,
            "experiment_id": experiment_id,
            # The campaign's analysis DAG node runs analyses once over the landed
            # sweep -- skip the composite's own inline flush (matches the batch
            # and per-generation paths).
            "analyses": "none",
            "parallel": "",
            # ecoli_baseline.baseline()'s own per-seed param (see
            # _seed_generation_command): run exactly this seed's lineage.
            "seed": int(seed),
        }
        if injected_processes:
            overrides["injected_processes"] = injected_processes
        if variants:
            overrides["variants"] = variants
        if exchange_fluxes:
            overrides["exchange_fluxes"] = exchange_fluxes
            if exchange_flux_basis:
                overrides["exchange_flux_basis"] = exchange_flux_basis
        env = PBG_RUNNER_ENV
        return (
            f"cd {V2ECOLI_DIR}"
            f" && aws s3 cp {runner_s3_uri} /tmp/run_pbg.py"
            f" && {env} python /tmp/run_pbg.py"
            f" --composite-id {composite_id or V2ECOLI_BATCH_BASELINE_COMPOSITE_ID}"
            f" --overrides {shlex.quote(json.dumps(overrides))} -n 1"
        )

    def _analysis_command(
        self,
        *,
        experiment_id: str,
        modules: dict[str, dict[str, Any]] | str,
        analysis_name: str,
        commit: str,
        cache_variant: str | None = None,
    ) -> str:
        """Build the analysis DAG node's command: the ported analyses over the S3 sweep.

        WHY A THIRD DAG NODE, not the composite's own inline flush. The composite
        (``v2ecoli.composites.ecoli_baseline``, formerly the standalone
        ``batch_baseline`` — backlog item 55) does ship a post-simulation flush that
        runs exactly these analyses, and the sim overrides deliberately disable it
        (``"analyses": "none"``). That is not a workaround for a broken flush — it is
        forced by the sweep's SHAPE on this backend:

          * The canonical dispatch is an AWS Batch ARRAY job: N independent children,
            one seed each, no shared filesystem. Each child's composite run sees 1/N
            of the sweep, so an inline flush there would run the cross-seed scales
            (multiseed/multivariant) against a single seed — N times over, racing on
            the same output prefix. The sweep only becomes whole once every child's
            output has landed in S3.
          * The whole-sweep analysis is therefore a GATHER node, and the DAG edge that
            expresses "after every child succeeded" is the same Batch ``dependsOn``
            the ParCa→sim edge already uses. No poller, no webhook, no external
            watcher: completion is an edge in the pipeline graph.

        The node itself reuses the model image's own ``v2ecoli-analyze`` console
        script (``v2ecoli.workflow.analysis_runner:main``) — the SAME function the
        composite's inline flush calls (``run_analyses``), reading the hive-parquet
        in place through DuckDB/httpfs. No new analysis logic. See
        ``viva_api.common.analysis_dag`` for the real CLI surface this conforms to
        (positional ``sweep_dir`` + ``--config`` — nothing else) and why the old
        ``--out-uri``/``--n-seeds``/``--n-generations``/``--modules``/
        ``--analysis-name`` flags folded into the ``--config`` JSON instead.

        ``sim_data`` points at the commit's ParCa cache in S3 (both via
        ``V2ECOLI_SIM_DATA`` and ``config["sim_data_path"]``) because an S3 sweep
        has no co-located pickle to glob (``analysis_runner.resolve_sim_data`` only
        globs local paths) — identical to how ``SimulationServiceK8s.
        submit_ray_native_analysis`` provisions the same script. Both this job and
        the ParCa job derive that URI from the commit independently, so it needs no
        hand-off plumbing.

        ``cache_variant`` (viva-api#448): the dispatch's own variant, if any — an
        analysis without it silently reads the plain per-commit (stock) simData
        even when the real simulation ran against a real strain's own cache,
        grading a candidate strain against stock data with no error. None (every
        caller before #448) is unaffected, matching the dispatch-side guard's own
        `cache_variant=None` default.
        """
        out_uri = self._results_s3_uri(experiment_id).rstrip("/")
        sim_data_uri = f"{data_layout.RayLayout.parca_cache_uri(commit, variant=cache_variant)}simData.cPickle"
        result_out_dir = f"{out_uri}/analyses/{analysis_name}"
        config = analysis_dag.build_analysis_config(
            analysis_options=modules,
            out_dir=result_out_dir,
            sim_data_path=sim_data_uri,
        )
        return analysis_dag.analysis_dag_command(
            v2ecoli_dir=V2ECOLI_DIR,
            sweep_dir=out_uri,
            sim_data_uri=sim_data_uri,
            config=config,
        )

    async def _submit_analysis_job(
        self,
        *,
        simulation: Simulation,
        database_service: DatabaseService,
        job_definition: str,
        commit: str,
        sim_job_id: str | None,
        n_seeds: int,
        n_generations: int,
        depends_type: str | None,
        tags: dict[str, str],
        cache_variant: str | None = None,
    ) -> str | None:
        """Submit the analysis DAG node and record it, returning its Batch job id.

        The analysis is tracked in the SAME ``analyses`` table (and therefore the same
        ``GET /analyses/{id}/status`` S3-manifest probe) the on-demand
        ``POST /simulations/{id}/analysis`` trigger already writes to — an
        auto-triggered analysis must be exactly as discoverable as a hand-triggered
        one, not an invisible side effect.

        Thin wrapper around ``viva_api.common.analysis_dag.submit_analysis_dag_node``
        (backlog: made reusable so a compose backend can share the exact same node
        instead of re-deriving its own argv) — this method owns only what's specific
        to the study Batch path: resolving the sim-data/results URIs from ``commit``/
        ``experiment_id``, and the ``analyses`` table's ``config`` record shape.

        ``cache_variant`` (viva-api#448): forwarded to the sim_data URI so a
        strain-specific dispatch's analysis reads its own cache, not the plain
        per-commit stock one. None (every caller before #448) is unaffected.

        ``sim_job_id`` is the single Batch job this analysis should natively
        ``dependsOn`` (item 24's original single-DAG-edge shape, still used by the
        single-shot dispatch paths). Pass ``None`` for the chain-dispatch campaign
        path (backlog item 33 rework), where by construction everything this
        analysis depends on has ALREADY finished by the time it's submitted — the
        analysis-fan-in poller's own "all tracked jobs terminal" check (see
        ``JobScheduler.update_chain_campaigns``) provides the "wait for all"
        semantics a native ``dependsOn`` can't express at 1000-seed scale (Batch
        caps a job at 20 dependencies), so no ``dependsOn`` is needed at all here.
        """
        experiment_id = simulation.config.experiment_id
        analysis_name = f"analysis-{experiment_id[:20]}-{_rand_suffix()}"
        out_uri = self._results_s3_uri(experiment_id).rstrip("/")
        result_uri = f"{out_uri}/analyses/{analysis_name}"
        modules = analysis_modules_for(simulation.config)
        sim_data_uri = f"{data_layout.RayLayout.parca_cache_uri(commit, variant=cache_variant)}simData.cPickle"
        params: dict[str, Any] = {
            "out_uri": out_uri,
            "n_seeds": int(n_seeds),
            "n_generations": int(n_generations),
            "modules": modules,
            "analysis_name": analysis_name,
            "trigger": "dispatch-dag",
            # ORMAnalysis.to_dto() unconditionally reads config["analysis_options"]
            # (AnalysisConfigOptions requires experiment_id) -- mirror the shape the
            # existing producers write so to_dto() doesn't KeyError.
            "analysis_options": {
                "experiment_id": [experiment_id],
                **(modules if isinstance(modules, dict) else {}),
            },
        }
        # Backlog item 71: the analysis DAG node has no real inter-node traffic
        # either (was a 1-node MNP job) -- moves to the plain container-type
        # path. `job_definition` must now be a container job def (see
        # submit_campaign_analysis's _ensure_container_job_def call).
        # Route a heavy multiseed/multigeneration gather to the large-memory
        # queue by declaration instead of OOM-then-hand-rerun (viva-api#625).
        memory_class = analysis_memory_class(modules, n_seeds=n_seeds, n_generations=n_generations)
        return await analysis_dag.submit_analysis_dag_node(
            sweep_dir=out_uri,
            analysis_options=modules,
            sim_data_uri=sim_data_uri,
            result_out_dir=result_uri,
            v2ecoli_dir=V2ECOLI_DIR,
            # task_env (sms-ecoli#166): the campaign's own env reaches its gather too.
            submit_container=functools.partial(
                self._submit_container,
                task_env=resolve_task_env(simulation.config),
                memory_class=memory_class,
            ),
            job_definition=job_definition,
            job_name=f"ray-analysis-{experiment_id}-{_rand_suffix()}"[:128],
            out_s3=self._results_s3_uri(experiment_id),
            container_out_dir=ANALYSIS_OUT_DIR,
            depends_on_job_id=sim_job_id,
            depends_type=depends_type,
            tags=tags,
            database_service=database_service,
            experiment_id=experiment_id,
            analysis_name=analysis_name,
            simulation_id=simulation.database_id,
            backend="ray",
            db_config=params,
            result_uri=result_uri,
        )

    @override
    async def get_latest_commit_hash(
        self,
        git_repo_url: str = DEFAULT_REPO,
        git_branch: str = DEFAULT_BRANCH,
    ) -> str:
        return await fetch_latest_commit_hash(git_repo_url, git_branch, get_settings().github_token)

    @override
    async def submit_build_image_job(
        self,
        simulator_version: SimulatorVersion,
        *,
        include_new_gene_data: bool = False,
        include_submit_image: bool = False,
        stage_private_fork: bool = False,
        vecoli_private_commit: str | None = None,
    ) -> JobId:
        """Build the self-contained v2ecoli Ray image via a DooD Batch job.

        Symmetric with SimulationServiceK8s.submit_build_image_job: a LOCAL task submits a
        DooD Batch build job that clones the workload repo at the commit and runs its own
        build-and-push recipe (v2ecoli/docker/build-and-push-ecr.sh) → v2ecoli:<commit>
        (plus the :latest deploy tag the Ray-MNP job def references). Returns immediately
        with a LOCAL JobId; _run_build polls the Batch job to completion.

        ``include_new_gene_data`` (item 87): False for every existing caller -- identical
        build to before this param existed. See ``_build_command``'s own docstring.

        ``stage_private_fork``/``vecoli_private_commit``: False for every existing caller --
        identical build to before these params existed. See ``_build_command``'s own
        docstring.
        """
        commit = simulator_version.git_commit_hash
        return self._local.submit(
            self._run_build(
                simulator_version,
                include_new_gene_data=include_new_gene_data,
                include_submit_image=include_submit_image,
                stage_private_fork=stage_private_fork,
                vecoli_private_commit=vecoli_private_commit,
            ),
            name=f"ray-build-{commit}",
        )

    def _build_command(
        self,
        simulator_version: SimulatorVersion,
        *,
        include_new_gene_data: bool = False,
        include_submit_image: bool = False,
        stage_private_fork: bool = False,
        vecoli_private_commit: str | None = None,
    ) -> list[str]:
        """DooD build command: clone v2ecoli@commit, run its build-and-push recipe.

        Mirrors SimulationServiceK8s._build_command (apk deps, PAT clone, in-repo recipe),
        but the workload repo is v2ecoli and the recipe is the v2ecoli image's own
        docker/build-and-push-ecr.sh → v2ecoli:<sha> (+ :latest).

        ``include_new_gene_data`` (item 87): False for every existing caller -- identical
        command to before this param existed (the outer clone's PAT is unset immediately,
        as before; ``-g`` is never passed). When True, the SAME PAT this method already
        fetches to clone the workload repo (both under the CovertLabEcoli org) stays
        exported for the build-and-push recipe's own ``-g`` flag, which threads it through
        as a Docker BuildKit secret (never a plain env/build-arg baked into a layer) so the
        image can stage private new-gene data for a ``--composite vecoli`` ParCa build that
        declares one. No new credential -- reuses this same Secrets Manager entry.

        ``stage_private_fork``/``vecoli_private_commit``: stage vEcoli-private -- not the
        Dockerfile's public default (``CovertLab/vEcoli@master``) -- as the image's own
        wrapped ``/app/vEcoli`` fork, via the SAME ``-s <spec>`` mechanism
        docker/build-and-push-ecr.sh already supports for any fork comparison. Every
        existing build reaches this recipe with no ``-s`` at all, so it silently falls
        through to that public default regardless of the simulator's own pinned commit --
        a raw ``!ParameterSerializer[...]`` tag whose value only exists in the private
        fork's own param_store (e.g. antibiotic-transport parameters) can never resolve on
        any remote dispatch today. The spec is generated INLINE in this same script (a
        heredoc, not a checked-in file) so there is nothing to go stale. Reuses the SAME
        PAT already fetched for the outer clone (vEcoli-private is private, same org) via
        the recipe's own ``--secret id=github_pat`` path -- no new credential.
        ``vecoli_private_commit`` is REQUIRED when ``stage_private_fork`` is True:
        deliberately no "latest" auto-resolution, so which commit gets staged is always an
        explicit, visible choice made by the caller, never a silent moving target.
        """
        if stage_private_fork and not vecoli_private_commit:
            raise ValueError("vecoli_private_commit is required when stage_private_fork is True")
        settings = get_settings()
        commit = simulator_version.git_commit_hash
        branch = simulator_version.git_branch
        repo_url = simulator_version.git_repo_url
        build_flags = " -g" if include_new_gene_data else ""
        if stage_private_fork:
            build_flags += " -s /tmp/vecoli-private-fork.yaml"
        keep_pat = include_new_gene_data or stage_private_fork
        unset_pat = "" if keep_pat else "unset GH_PAT\n"
        private_fork_spec_block = ""
        if stage_private_fork:
            private_fork_spec_block = f"""\
cat > /tmp/vecoli-private-fork.yaml <<'SPEC'
comparison:
  reference:
    repo: {RepoUrl.VECOLI_PRIVATE_REPO_URL}
    commit: {vecoli_private_commit}
    extra_deps:
      - jax
SPEC
"""
        script = f"""\
set -ex
export USER=${{USER:-sms-api}}
apk add --no-cache aws-cli git bash

# Docker daemon runs on the host (DooD) — verify the mounted socket.
docker info >/dev/null 2>&1 || {{ echo "ERROR: Docker socket not available"; exit 1; }}

# GitHub PAT (Secrets Manager) for the clone; x-access-token is GitHub's HTTPS convention.
# Disable xtrace around the secret so the PAT (and the clone URL embedding it) never lands
# in the build logs (CloudWatch). Re-enable tracing once the clone is done.
set +x
export GH_PAT=$(aws secretsmanager get-secret-value \
    --secret-id {settings.build_git_secret_arn} --query SecretString --output text)
CLONE_URL=$(echo "{repo_url}" | sed "s|https://github.com/|https://x-access-token:${{GH_PAT}}@github.com/|")
export GIT_TERMINAL_PROMPT=0
git clone --branch {branch} --single-branch "$CLONE_URL" /build/v2ecoli
unset CLONE_URL
{unset_pat}set -x
cd /build/v2ecoli
git checkout {commit}

{private_fork_spec_block}# The v2ecoli image is self-contained (bundles the AWS CLI + Ray entrypoint); its own
# recipe builds + pushes v2ecoli:<sha> and the :latest deploy tag the MNP job def uses.
bash docker/build-and-push-ecr.sh -i {commit} -r {settings.ray_ecr_repository} -R {settings.batch_region}{build_flags}
"""
        if include_submit_image:
            # The Nextflow HEAD image. Deliberately a thin derived layer, not a change to the
            # task image: on vEcoli's proven awsbatch profile the Batch tasks run
            # ``container = params.container_image`` -- the PLAIN science image, with no JVM
            # and no nextflow binary anywhere. Only the process that runs ``nextflow run``
            # needs Java. v2ecoli's own image already installs AWS CLI v2 (Dockerfile:149-156),
            # which is the one thing Nextflow *does* require inside a task container to stage
            # the S3 work dir, so nothing about the task side has to change.
            #
            # Mirrors SimulationServiceK8s._build_command(submit_image=True) rather than
            # inventing a second recipe; NEXTFLOW_VERSION is pinned to the same 25.10.2 that
            # image uses, so one Nextflow version spans the deployment. (Phase 0 of
            # docs/plan-nextflow-dispatch.md measured 25.04.3; both map `time` to Batch
            # attemptDurationSeconds -- see its §11.1 -- and the skew is resolved here in
            # favour of what already ships.)
            script += f"""
BASE_URI=$ECR_REGISTRY/{settings.ray_ecr_repository}:{commit}

cat > /tmp/Dockerfile-submit <<'DOCKERFILE'
ARG BASE_IMAGE
FROM ${{BASE_IMAGE}}
USER root
RUN apt-get update && apt-get install -y --no-install-recommends default-jre-headless \\
    && apt-get clean && rm -rf /var/lib/apt/lists/*
ARG NEXTFLOW_VERSION=25.10.2
RUN curl -fsSL "https://github.com/nextflow-io/nextflow/releases/download/v${{NEXTFLOW_VERSION}}/nextflow" \\
    -o /usr/local/bin/nextflow && chmod +x /usr/local/bin/nextflow
WORKDIR /app/v2ecoli
DOCKERFILE

docker build -t "$ECR_REGISTRY/{settings.ray_ecr_repository}:{commit}-submit" \
    --build-arg BASE_IMAGE="$BASE_URI" \
    -f /tmp/Dockerfile-submit /tmp
docker push "$ECR_REGISTRY/{settings.ray_ecr_repository}:{commit}-submit"
echo "Submit image pushed: $ECR_REGISTRY/{settings.ray_ecr_repository}:{commit}-submit"
"""
        return ["sh", "-c", script]

    async def _run_build(
        self,
        simulator_version: SimulatorVersion,
        *,
        include_new_gene_data: bool = False,
        include_submit_image: bool = False,
        stage_private_fork: bool = False,
        vecoli_private_commit: str | None = None,
    ) -> None:
        """Submit the DooD v2ecoli image build to Batch (amd64 queue) and poll it."""
        settings = get_settings()
        commit = simulator_version.git_commit_hash
        job_id = await batch_build.submit_batch_build(
            job_name=batch_build.ray_build_job_name(commit),
            queue=settings.build_amd64_queue,
            command=self._build_command(
                simulator_version,
                include_new_gene_data=include_new_gene_data,
                include_submit_image=include_submit_image,
                stage_private_fork=stage_private_fork,
                vecoli_private_commit=vecoli_private_commit,
            ),
        )
        # viva-api#414: persist the Batch handle on this task's HpcRun row so
        # the build's outcome is recoverable from any process, not only the
        # one holding this asyncio.Task (which dies with the pod).
        await self._local.record_external_job_ids([job_id])
        await batch_build.poll_batch_jobs([job_id])
        logger.info("v2ecoli Ray image build complete: %s:%s", settings.ray_ecr_repository, commit)

    @override
    async def submit_parca_job(self, parca_dataset: ParcaDataset) -> JobId:
        """Submit ParCa as a standalone container job (backlog item 71), capturing
        the cache to S3. Was a 1-node Ray MNP job; ParCa has no real inter-node
        traffic, so it moves to the plain container-type path -- see
        ``_submit_container``."""
        simulator_version = parca_dataset.parca_dataset_request.simulator_version
        commit = simulator_version.git_commit_hash
        job_def = self._ensure_container_job_def(self._image_uri(commit), commit)
        job_id = self._submit_container(
            job_name=f"ray-parca-{commit}-{_rand_suffix()}",
            job_definition=job_def,
            job_cmd=self._parca_command(),
            out_s3=self.cache_s3_uri(commit),
            out_dir=PARCA_CACHE_DIR,
        )
        return JobId.ray(job_id)

    async def submit_new_gene_cache_job(
        self,
        *,
        commit: str,
        variant: str,
        expression: float,
        translation_efficiency: float,
        rel_exp_adj: str | None = None,
        rel_trl_eff_adj: str | None = None,
        seed: int = 0,
        media_condition: str | None = None,
        fixed_media: str | None = None,
        source_variant: str | None = None,
    ) -> JobId:
        """Submit ``build_new_gene_cache.py`` as a standalone container job
        (backlog item 105), stamping a new-gene INDUCTION LEVEL onto a
        commit's already-built ParCa cache and capturing the result to a
        ``variant``-labeled S3 key. Sibling of ``submit_parca_job`` -- same
        1-node container shape, same job-def, same image; the only structural
        difference is this job STAGES IN a prior cache (``stage_s3``/
        ``stage_dir``) before running, since it composes on top of ParCa's
        output rather than producing it from scratch.

        ``variant`` is REQUIRED (not optional, unlike ``_upstream_parca_command``'s
        own ``config_path``): every caller of this method is, by construction,
        building a derived cache, so there is no "default" call that should
        ever land on the bare commit-only key -- see ``RayLayout.parca_cache_uri``'s
        own docstring for why writing there would silently corrupt every other
        concurrent dispatch on this commit.

        The source commit's cache MUST already have been built with
        ``new_genes`` set (``_parca_command``'s own param, item 93) -- an
        all-zero-expression source has nothing for this job to induce; that
        precondition is the CALLER's responsibility (e.g. a completed
        ``ParcaDataset`` whose own request set ``parca_options.new_genes``),
        not re-validated here, matching this class's existing pure-passthrough
        philosophy for ``injected_processes``/``variants``/``composite_id``.
        """
        job_def = self._ensure_container_job_def(self._image_uri(commit), commit)
        job_id = self._submit_container(
            job_name=f"new-gene-cache-{commit}-{_rand_suffix()}",
            job_definition=job_def,
            job_cmd=self._build_new_gene_cache_command(
                expression=expression,
                translation_efficiency=translation_efficiency,
                rel_exp_adj=rel_exp_adj,
                rel_trl_eff_adj=rel_trl_eff_adj,
                seed=seed,
                media_condition=media_condition,
                fixed_media=fixed_media,
            ),
            # ``source_variant`` (sms-ecoli#166, 2026-09-09): stage the chassis from
            # a variant slot instead of the shared bare commit slot, which any
            # chain dispatch's ``run_parca`` on this commit rewrites (last writer
            # wins). None keeps the bare-slot source byte-for-byte.
            stage_s3=self.cache_s3_uri(commit, variant=source_variant),
            stage_dir=PARCA_CACHE_DIR,
            out_s3=self.cache_s3_uri(commit, variant=variant),
            out_dir=NEW_GENE_INDUCED_CACHE_DIR,
        )
        return JobId.ray(job_id)

    async def submit_variant_cache_job(
        self,
        *,
        commit: str,
        variant: str,
        perturbations: dict[str, float],
        seed: int = 0,
        fixed_media: str | None = None,
    ) -> JobId:
        """Submit ``build_variant_cache.py`` as a standalone container job
        (backlog item 451), stamping NATIVE-gene translation-efficiency
        perturbations onto a commit's already-built ParCa cache and capturing
        the result to a ``variant``-labeled S3 key. Sibling of
        ``submit_new_gene_cache_job`` -- identical 1-node container shape,
        same job-def, same image, same stage-in-then-run structure; the only
        difference is which script runs and what it perturbs (native genes
        here, a new gene's own induction level there).

        ``variant`` is REQUIRED for the same reason as ``submit_new_gene_cache_job``'s
        own docstring: every caller is, by construction, building a derived
        cache, so there is no default call that should land on the bare
        commit-only key.

        The source commit's cache MUST already exist (any ``_parca_command``
        run for this commit) -- that precondition is the CALLER's
        responsibility, not re-validated here, matching this class's existing
        pure-passthrough philosophy.
        """
        job_def = self._ensure_container_job_def(self._image_uri(commit), commit)
        job_id = self._submit_container(
            job_name=f"variant-cache-{commit}-{_rand_suffix()}",
            job_definition=job_def,
            job_cmd=self._build_variant_cache_command(
                perturbations=perturbations,
                seed=seed,
                fixed_media=fixed_media,
            ),
            stage_s3=self.cache_s3_uri(commit),
            stage_dir=PARCA_CACHE_DIR,
            out_s3=self.cache_s3_uri(commit, variant=variant),
            out_dir=VARIANT_CACHE_DIR,
        )
        return JobId.ray(job_id)

    async def submit_task(self, request: TaskRunRequest, database_service: DatabaseService) -> TaskDTO:
        """Submit a self-contained repo-path script as a standalone AWS Batch
        container job (viva-api#631 slice 1).

        Same one-node container shape ``submit_parca_job``/the analysis DAG
        node already use (``_ensure_container_job_def`` + ``_submit_container``)
        — this method's only real job is building the ``python <script>
        <args...>`` command line and recording the result on the ``task``
        table (mirroring how ``_submit_analysis_job`` records to ``analysis``)
        so ``GET /tasks/{id}/status`` has a row to poll.

        ``request.commit`` pins the image commit the script runs in; ``None``
        resolves to the default repo/branch's latest commit, the same
        fallback every other Ray dispatch path here already uses.

        ``request.sim_data_refs``, when present, rides in as a single JSON env
        var (``TASK_SIM_DATA_REFS``) rather than individual vars — the shape
        is caller-defined (script-specific reference names -> URIs), so there
        is no fixed set of env-var names to emit.
        """
        commit = request.commit or await self.get_latest_commit_hash()
        task_name = _safe_task_name(request.name or Path(request.script).stem)
        job_cmd = self._task_job_cmd(request.script, request.args)
        return await self._dispatch_task(
            task_name=task_name,
            script_label=request.script,
            job_cmd=job_cmd,
            request=request,
            commit=commit,
            database_service=database_service,
        )

    async def submit_uploaded_task(
        self,
        request: TaskRunRequest,
        *,
        script_bytes: bytes,
        filename: str,
        database_service: DatabaseService,
    ) -> TaskDTO:
        """Submit an UPLOADED script (viva-api#631 slice 2).

        The script bytes are staged to an S3 prefix; the container entrypoint's
        existing input-staging (``CONTAINER_STAGE_S3`` -> ``CONTAINER_STAGE_DIR``,
        an ``aws s3 sync``) pulls it into the container before the command runs,
        so the job_cmd is ``python <TASK_STAGE_DIR>/<script>``. No entrypoint
        change is needed -- this reuses the same stage-in path ParCa's cache
        staging already uses.
        """
        commit = request.commit or await self.get_latest_commit_hash()
        safe_name = Path(filename).name  # never trust an uploaded path
        if not safe_name:
            raise ValueError("uploaded task script has no filename")
        task_name = _safe_task_name(request.name or Path(safe_name).stem)
        stage_s3 = self._results_s3_uri(f"tasks/scripts/{task_name}-{_rand_suffix()}").rstrip("/")
        self._upload_task_script(stage_s3, safe_name, script_bytes)
        job_cmd = self._task_job_cmd(f"{TASK_STAGE_DIR}/{safe_name}", request.args)
        return await self._dispatch_task(
            task_name=task_name,
            script_label=f"{stage_s3}/{safe_name}",
            job_cmd=job_cmd,
            request=request,
            commit=commit,
            database_service=database_service,
            stage_s3=stage_s3,
            stage_dir=TASK_STAGE_DIR,
        )

    @staticmethod
    def _task_job_cmd(script: str, args: list[str]) -> str:
        cmd = "python " + shlex.quote(script)
        if args:
            cmd += " " + " ".join(shlex.quote(a) for a in args)
        return cmd

    def _upload_task_script(self, stage_s3_prefix: str, filename: str, script_bytes: bytes) -> None:
        """Put the uploaded script under ``stage_s3_prefix`` so the container's
        input staging syncs it in. Uses the instance/task S3 credentials, same as
        every other S3 write on this service."""
        from urllib.parse import urlparse

        parsed = urlparse(stage_s3_prefix)
        bucket = parsed.netloc
        key = f"{parsed.path.strip('/')}/{filename}"
        boto3.client("s3", region_name=get_settings().storage_s3_region).put_object(
            Bucket=bucket, Key=key, Body=script_bytes
        )

    async def _dispatch_task(
        self,
        *,
        task_name: str,
        script_label: str,
        job_cmd: str,
        request: TaskRunRequest,
        commit: str,
        database_service: DatabaseService,
        stage_s3: str | None = None,
        stage_dir: str | None = None,
    ) -> TaskDTO:
        """Shared submit path for repo-path and uploaded tasks: one-node container
        job (``_ensure_container_job_def`` + ``_submit_container``) recorded on the
        ``task`` table so ``GET /tasks/{id}/status`` has a row to poll."""
        job_def = self._ensure_container_job_def(self._image_uri(commit), commit)
        out_uri = self._results_s3_uri(f"tasks/{task_name}-{_rand_suffix()}").rstrip("/")
        task_env: dict[str, str] | None = None
        if request.sim_data_refs:
            task_env = {"TASK_SIM_DATA_REFS": json.dumps(request.sim_data_refs)}
        batch_job_id = self._submit_container(
            job_name=f"task-{task_name}-{_rand_suffix()}"[:128],
            job_definition=job_def,
            job_cmd=job_cmd,
            out_s3=out_uri,
            out_dir=TASK_OUT_DIR,
            stage_s3=stage_s3,
            stage_dir=stage_dir,
            task_env=task_env,
            memory_class=request.memory_class,
        )
        return await database_service.record_task(
            name=task_name,
            script=script_label,
            args=list(request.args),
            sim_data_refs=request.sim_data_refs,
            memory_class=request.memory_class,
            status=TaskStatusDB.COMPUTING,
            job_id_ext=str(batch_job_id),
            out_uri=out_uri,
        )

    async def get_task_status(self, task_id: int, database_service: DatabaseService) -> TaskDTO:
        """Poll a task's tracked Batch job (if any) and persist its mapped status.

        A task with no ``job_id_ext`` yet (shouldn't happen post-``submit_task``,
        but mirrors the defensive style elsewhere in this class) is returned
        as-is rather than raising -- there is nothing to poll.
        """
        task = await database_service.get_task(task_id)
        if task.job_id_ext is None:
            return task
        statuses = self.get_batch_job_statuses([task.job_id_ext])
        batch_status = statuses.get(task.job_id_ext)
        if batch_status is None:
            return task
        return await database_service.update_task_status(task_id, TaskStatusDB.from_job_status(batch_status))

    def _resolve_log_group(self, job_definition: str | None) -> str | None:
        """The CloudWatch log group a container job writes to: the configured
        ``ray_batch_log_group`` if set, else the awslogs-group from the job
        definition's logConfiguration. None when neither is available."""
        configured = get_settings().ray_batch_log_group
        if configured:
            return configured
        if not job_definition:
            return None
        try:
            defs = self._batch().describe_job_definitions(jobDefinitions=[job_definition]).get("jobDefinitions", [])
            options = defs[0].get("containerProperties", {}).get("logConfiguration", {}).get("options", {})
            group = options.get("awslogs-group")
            return group if isinstance(group, str) else None
        except Exception:
            logger.warning("could not resolve log group from job definition %s", job_definition, exc_info=True)
            return None

    async def get_task_logs(self, task_id: int, database_service: DatabaseService, *, limit: int = 1000) -> TaskLogsDTO:
        """The CloudWatch logs for a task's Batch job (viva-api#631 slice 3).

        Resolves the job's log stream (``describe_jobs`` -> container.logStreamName)
        and log group, then reads up to ``limit`` recent events. Returns an empty
        ``lines`` (not an error) when the container hasn't started yet (no stream)
        or no group can be resolved, so a caller can poll until logs appear."""
        task = await database_service.get_task(task_id)
        result = TaskLogsDTO(task_id=task_id, job_id_ext=task.job_id_ext, status=task.status)
        log_prefix = get_settings().ray_log_s3_prefix
        if log_prefix and task.job_id_ext:
            result.report_uri = f"{log_prefix.rstrip('/')}/{task.job_id_ext}/report.json"
        if not task.job_id_ext:
            return result
        jobs = self._batch().describe_jobs(jobs=[task.job_id_ext]).get("jobs", [])
        if not jobs:
            return result
        job = jobs[0]
        stream = job.get("container", {}).get("logStreamName")
        if not stream:
            return result  # container not started yet
        group = self._resolve_log_group(job.get("jobDefinition"))
        if not group:
            return result
        result.log_stream = stream
        try:
            logs_client = boto3.client("logs", region_name=get_settings().storage_s3_region)
            events = logs_client.get_log_events(
                logGroupName=group, logStreamName=stream, startFromHead=True, limit=limit
            ).get("events", [])
            result.lines = [str(e.get("message", "")) for e in events]
        except Exception:
            logger.warning("could not read CloudWatch logs for task %s (stream %s)", task_id, stream, exc_info=True)
        return result

    @override
    async def submit_ecoli_simulation_job(
        self, ecoli_simulation: Simulation, database_service: DatabaseService, correlation_id: str
    ) -> JobId:
        """Submit ParCa (1 node) + the simulation ensemble (N nodes), gated by a Batch dependency.

        The tracked job id is the *simulation* job. Batch will not start it until
        the ParCa job SUCCEEDED, so the cache is in S3 before the sim stages it.

        ROUTING (backlog item 33 rework): the canonical batch_baseline sweep
        (``composite`` is None, more than one generation) is delegated ENTIRELY
        to ``submit_chain_dispatch_job`` — individual per-seed AWS Batch job
        chains, a true v2 analogy of vEcoli-private's own fully-asynchronous
        per-seed Nextflow execution (Alex's explicit decision). This check runs
        BEFORE any of the MNP/single-shot setup below, so a canonical request
        never touches that setup, and never needs Array-job machinery at all —
        ``_submit_array``/``_array_sim_command``/``_ensure_array_job_def`` were
        REMOVED as part of this rework once this routing landed made them dead
        code (their only caller was the array branch this replaced; confirmed
        via a fresh repo-wide grep before deleting them, not assumed).

        That delegation runs in the BACKGROUND (via
        ``_submit_chain_dispatch_background``), so this method returns in
        seconds no matter how large the campaign is, and the returned id is a
        ``JobId.local(...)`` rather than the ParCa job's ``JobId.ray(...)``.
        Submitting a campaign's N*G jobs takes minutes of real wall time
        (~15 for the canonical 1000x10 shape) and used to happen inline, inside
        the ``POST /api/v1/simulations`` request — see that method's docstring
        for the real production failure that caused. Progress stays trackable
        through the unchanged ``GET /api/v1/simulations/{id}/status``.
        ``submit_chain_dispatch_job`` itself is untouched and still runs
        synchronously to completion for its direct callers.

        Every OTHER shape still reaches the MNP path below exactly as before:
        the composite-driven two-engine comparison ensemble (genuinely fans out
        via Ray actors, at ANY generation count — chain-dispatch is v2ecoli-only
        and does not apply), and the single-generation phase0 ensemble.
        """
        if database_service is None:
            raise RuntimeError("DatabaseService is not available. Cannot submit Ray simulation job.")

        config = ecoli_simulation.config

        # Backlog item 88: a generic multi-node process-bigraph composite dispatch
        # (e.g. a colony composite distributed across N Ray-cluster nodes) arrives
        # as an extra key (SimulationConfig's extra="allow") rather than a declared
        # field -- checked FIRST, before the chain-dispatch routing below, since a
        # multi-node request may otherwise also satisfy that check's own
        # composite-is-None/generations>1 condition and would silently misroute.
        # A Nextflow dispatch is checked BEFORE multi_node_dispatch for the same
        # reason that one is checked before chain-dispatch: it carries a
        # composite_id too, so a later check would silently claim it first and the
        # request would run on the wrong mechanism while looking like it worked.
        #
        # Deliberately a per-request extra rather than a new ComputeBackend:
        # compute_backend_for_repo maps repo -> backend, so a NEXTFLOW member would
        # either reroute every v2ecoli request or be dead configuration. This is the
        # same axis multi_node_dispatch already uses to pick pbg-native over
        # chain-dispatch for the same repo, same image, one config field apart.
        nf_dispatch = getattr(config, "nextflow_dispatch", None)
        if nf_dispatch is not None:
            return await self._submit_nextflow_dispatch(
                ecoli_simulation, database_service, nf_dispatch, correlation_id=correlation_id
            )

        # Run 1's real missing-output fix (Alex's Option 1 decision, 2026-09-06):
        # a run_mbp_tracked.py variant dispatch, e.g. reactor_bird_coupled. Checked
        # here for the same reason nextflow_dispatch/multi_node_dispatch are --
        # it carries no composite_id/generations shape that would otherwise
        # satisfy a different branch's own routing condition, but checking early
        # keeps every extra-field dispatch shape's own precedence explicit rather
        # than incidental.
        mbp_dispatch = getattr(config, "mbp_dispatch", None)
        if mbp_dispatch is not None:
            return await self._submit_mbp_tracked_dispatch(
                ecoli_simulation, database_service, mbp_dispatch, correlation_id=correlation_id
            )

        mnp_dispatch = getattr(config, "multi_node_dispatch", None)
        if mnp_dispatch is not None:
            return await self._submit_multi_node_composite(
                ecoli_simulation, database_service, mnp_dispatch, correlation_id=correlation_id
            )

        composite = getattr(config, "composite", None)
        # config.generations is a real (non-"extra") SimulationConfig field, unlike
        # the comparison knobs read further below -- read directly, not via getattr.
        n_generations = int(config.generations or 1)
        if composite is None and n_generations > 1:
            return await self._submit_chain_dispatch_background(
                ecoli_simulation, database_service, correlation_id=correlation_id
            )

        parca_dataset = await database_service.get_parca_dataset(parca_dataset_id=ecoli_simulation.parca_dataset_id)
        if parca_dataset is None:
            raise ValueError(f"ParcaDataset with ID {ecoli_simulation.parca_dataset_id} not found.")
        simulator = await database_service.get_simulator(simulator_id=ecoli_simulation.simulator_id)
        if simulator is None:
            raise ValueError(f"Simulator {ecoli_simulation.simulator_id} not found")

        settings = get_settings()
        commit = simulator.git_commit_hash
        experiment_id = ecoli_simulation.config.experiment_id

        # Run the TRUE commit image: derive a per-commit MNP job-def revision pointing at
        # v2ecoli:<commit> (both ParCa and the sim run the same image).
        job_def = self._ensure_mnp_job_def(self._image_uri(commit), commit)

        # SimulationConfig is a vEcoli passthrough (extra="allow"); the comparison
        # knobs are validated at the API boundary (Literal Query params) and ride
        # in as extra keys, so they're read here via getattr (present only when the
        # caller set them). ``vecoli_source`` is already constrained by the
        # endpoint's VecoliSource type; ``composite`` was already read above.
        n_seeds = ecoli_simulation.num_seeds or getattr(config, "n_init_sims", None) or 1
        n_steps = getattr(config, "ray_n_steps", None) or settings.ray_n_steps
        chunk = getattr(config, "ray_chunk", None) or settings.ray_chunk
        condition = getattr(config, "condition", None)
        max_generations = getattr(config, "max_generations", None)
        vecoli_source = getattr(config, "vecoli_source", None)

        # Engine-specific ParCa source: the pristine upstream wrapper (--composite
        # vecoli) stages an UPSTREAM-built simData (separate cache + build cmd);
        # every other engine stages the v2ecoli cache. Both ParCa and the sim use
        # the matching pair so the staged simData is consistent across all nodes.
        is_upstream = _is_upstream_vecoli(composite)
        # Backlog item 105: same generic cache_variant passthrough already proven
        # for the chain-dispatch/multi-node-composite paths -- irrelevant to the
        # upstream-vEcoli engine (its own config_path-driven mechanism is separate).
        # This is the whole fix for the comparison-ensemble path's own real gap: the
        # driver's `--cache-dir` default already resolves to PARCA_CACHE_DIR (the
        # exact path staged below), so redirecting the STAGED cache via `variant`
        # is sufficient -- no command-line change needed, confirmed the two paths
        # are byte-identical (`REPO_ROOT/out/cache` == `/app/v2ecoli/out/cache`).
        cache_variant = None if is_upstream else (getattr(config, "cache_variant", None) or None)
        cache_s3 = (
            self._upstream_cache_s3_uri(commit) if is_upstream else self.cache_s3_uri(commit, variant=cache_variant)
        )
        # Backlog item 93: same generic new_genes passthrough as
        # submit_chain_dispatch_job -- irrelevant to the upstream-vEcoli
        # engine (its own config_path-driven mechanism, item 87, is separate).
        new_genes = None if is_upstream else getattr(config.parca_options, "new_genes", None)
        # Backlog item 104: same generic bundle_overrides passthrough, same
        # upstream-vEcoli exemption as new_genes above.
        bundle_overrides = None if is_upstream else getattr(config.parca_options, "bundle_overrides", None)
        # Same generic rnaseq_source passthrough (item 106/#166 chassis-provenance
        # thread) -- a bundle_overrides manifest can itself require this to have
        # any effect at all; same upstream-vEcoli exemption as the two above.
        rnaseq_source = None if is_upstream else getattr(config.parca_options, "rnaseq_source", None)
        # Same generic require_clean_chain passthrough (item 106/#166, v2ecoli#735) --
        # opt-in only (default False emits nothing, see _stage_out_env's own
        # docstring): most existing callers don't pass v2ecoli's own `sources=` yet.
        require_clean_chain = (
            False if is_upstream else bool(getattr(config.parca_options, "require_clean_chain", False))
        )
        # Same generic bundle_manifest_path/build_combined_bundle_manifest/
        # include_violacein_bundle/deterministic_hash_seed passthrough (item
        # 451/#166, Run 4 founder-chassis rebuild) -- same upstream-vEcoli
        # exemption as new_genes/bundle_overrides/rnaseq_source above.
        bundle_manifest_path = None if is_upstream else getattr(config.parca_options, "bundle_manifest_path", None)
        build_combined_bundle_manifest = (
            False if is_upstream else bool(getattr(config.parca_options, "build_combined_bundle_manifest", False))
        )
        include_violacein_bundle = (
            False if is_upstream else bool(getattr(config.parca_options, "include_violacein_bundle", False))
        )
        deterministic_hash_seed = (
            False if is_upstream else bool(getattr(config.parca_options, "deterministic_hash_seed", False))
        )
        parca_command = (
            self._upstream_parca_command()
            if is_upstream
            else self._parca_command(
                new_genes=new_genes,
                bundle_overrides=bundle_overrides,
                rnaseq_source=rnaseq_source,
                bundle_manifest_path=bundle_manifest_path,
                build_combined_bundle_manifest=build_combined_bundle_manifest,
                include_violacein_bundle=include_violacein_bundle,
                deterministic_hash_seed=deterministic_hash_seed,
            )
        )

        # Only the composite-driven comparison-ensemble path can still reach here
        # with n_generations > 1 (the non-composite canonical shape is routed to
        # chain-dispatch above, before this line); its own _sim_command branch
        # never reads runner_s3_uri, but staging it costs nothing and this stays
        # unconditional on generations alone, matching pre-existing behavior.
        runner_s3_uri = await self.stage_runner(experiment_id) if n_generations > 1 else None

        # Cost-allocation tags (propagate to ECS tasks → payer-account Cost
        # Explorer attributes spend per run/engine/condition). Values must be
        # tag-safe strings.
        base_tags = {
            "Project": "v2ecoli-comparison",
            "ExperimentId": str(experiment_id)[:255],
            "Engine": str(composite or "v2ecoli"),
            "Condition": str(condition or "basal"),
            "Commit": str(commit)[:12],
            "Team": getattr(settings, "cost_team_tag", None) or "covertlab",
        }

        # 1. ParCa job (1 node) → cache to S3.
        task_env = resolve_task_env(config)
        parca_job_id = self._submit_mnp(
            job_name=f"ray-parca-{commit}-{_rand_suffix()}",
            job_definition=job_def,
            num_nodes=1,
            ray_job_cmd=parca_command,
            out_s3=cache_s3,
            out_dir=PARCA_CACHE_DIR,
            tags={**base_tags, "Phase": "parca"},
            task_env=task_env,
        )

        # 2. Simulation ensemble (N-node Ray cluster), gated on ParCa, staging the
        # cache. Always MNP now: the ONE shape that used to need Array jobs here
        # (canonical batch_baseline, composite is None + multi-generation) is
        # routed to submit_chain_dispatch_job before this method does ANY of the
        # setup above (see the routing check at the top) -- every request that
        # still reaches this point either sets composite (the comparison
        # ensemble, which genuinely fans out via Ray actors) or requests a
        # single generation (the phase0 ensemble).
        sim_job_id = self._submit_mnp(
            job_name=f"ray-sim-{experiment_id}-{_rand_suffix()}"[:128],
            job_definition=job_def,
            num_nodes=settings.ray_num_nodes,
            ray_job_cmd=self._sim_command(
                int(n_seeds),
                int(n_steps),
                int(chunk),
                composite=composite,
                condition=condition,
                max_generations=max_generations,
                vecoli_source=vecoli_source,
                n_generations=n_generations,
                experiment_id=str(experiment_id),
                runner_s3_uri=runner_s3_uri,
                # CD2 native seam: thread the submitted config's domain fields so the
                # --composite-id batch run carries the metabolism-redux/violacein swap.
                # injected_processes maps the legacy config's swap_processes ->
                # ecoli_baseline.baseline()'s injected_processes kwarg (same helper the
                # chain-dispatch/JobScheduler path uses); the rest are ecoli_baseline
                # batch-mode kwargs read straight off the config (extra="allow"), all
                # no-ops when the config sets none of them.
                injected_processes=injected_processes_from_config(config),
                variants=getattr(config, "variants", None),
                config_overrides=getattr(config, "config_overrides", None),
                features=getattr(config, "features", None),
                exchange_fluxes=getattr(config, "exchange_fluxes", None),
                exchange_flux_basis=getattr(config, "exchange_flux_basis", None),
            ),
            out_s3=self._results_s3_uri(experiment_id),
            out_dir=SIM_OUT_DIR,
            stage_s3=cache_s3,
            stage_dir=PARCA_CACHE_DIR,
            depends_on=[parca_job_id],
            tags={**base_tags, "Phase": "sim"},
            task_env=task_env,
            # Wrong-strain guard (sms-ecoli#210 / #215): tell the entrypoint which
            # strain this run staged so it rejects a cache built for a different one.
            # off/None (wild-type) emits nothing, so this is inert for non-strain runs.
            expect_new_genes=new_genes,
            expect_bundle_overrides=bundle_overrides,
            require_clean_chain=require_clean_chain,
        )
        logger.info(
            "Ray simulation %s: parca job %s -> sim job %s (%d nodes)",
            experiment_id,
            parca_job_id,
            sim_job_id,
            settings.ray_num_nodes,
        )

        # No inline analysis submission: the ONE shape that used to need it here
        # (canonical batch_baseline) is entirely handled by chain-dispatch's own
        # poller-triggered submit_campaign_analysis now (see
        # JobScheduler.update_chain_campaigns / _advance_chain_campaign). The
        # comparison-ensemble and phase0 paths that still reach this point write
        # no cd1_*/ptools_*-ready sweep and never got inline analysis either --
        # unaffected by this rework.
        return JobId.ray(sim_job_id)

    # A freshly-registered job definition (this method is always called right
    # after `_ensure_mnp_job_def` registers one) can briefly 404/come back
    # empty from `describe_job_definitions` due to AWS eventual consistency --
    # confirmed live 2026-08-25 on a commit's first-ever multi-node dispatch
    # (sim255): the identical job definition, queried again a few minutes
    # later, returned correctly. A few short retries covers this window
    # without meaningfully slowing down the common case (already-registered
    # job def, resolves on the first attempt).
    _VCPU_LOOKUP_RETRIES = 3
    _VCPU_LOOKUP_BACKOFF_SECONDS = 1.0

    def _mnp_node_vcpus(self, job_definition: str) -> int | None:
        """Real per-node vCPU count declared on an MNP job definition's own
        ``resourceRequirements`` (confirmed live 2026-08-24: ``VCPU: "16"`` on
        the real ``smsvpctest-ray-mnp`` base def) -- used to size
        ``RAY_SHARDS_DEFAULT`` for a multi-node composite dispatch. A
        ``--num-cpus=0`` head's own ``os.cpu_count()`` (``RayProtocolRuntime``'s
        default fallback) under-counts real aggregate cluster capacity; reading
        the job definition's own declared resources is the real, existing
        source of truth for this, not a new guessed config value. Returns
        ``None`` on any lookup failure so the caller can safely leave
        ``RAY_SHARDS_DEFAULT`` unset (process-bigraph's own fallback still
        applies) rather than fail the whole submission over a sizing nicety.

        Retries a few times on a transient empty/missing result -- see
        ``_VCPU_LOOKUP_RETRIES`` above -- before giving up.
        """
        name, _, revision = job_definition.partition(":")
        batch = self._batch()
        for attempt in range(self._VCPU_LOOKUP_RETRIES):
            defs: list[dict[str, Any]] = []
            try:
                described = batch.describe_job_definitions(jobDefinitionName=name, revision=int(revision))
                defs = described.get("jobDefinitions", [])
            except Exception as exc:
                # Treated the same as an empty result below -- both just mean "not visible yet".
                logger.debug("describe_job_definitions attempt %d for %s failed: %s", attempt, job_definition, exc)

            if defs:
                ranges = defs[0].get("nodeProperties", {}).get("nodeRangeProperties", [])
                for nr in ranges:
                    for req in nr.get("container", {}).get("resourceRequirements", []):
                        if req.get("type") == "VCPU":
                            return int(float(req["value"]))
                return None

            if attempt < self._VCPU_LOOKUP_RETRIES - 1:
                time.sleep(self._VCPU_LOOKUP_BACKOFF_SECONDS * (attempt + 1))

        logger.warning(
            "Could not determine per-node vCPUs for %s after %d attempts; RAY_SHARDS_DEFAULT left unset",
            job_definition,
            self._VCPU_LOOKUP_RETRIES,
        )
        return None

    def _multi_node_composite_command(
        self,
        *,
        composite_id: str,
        params: dict[str, Any],
        steps: int,
        runner_s3_uri: str,
        n_shards_default: int | None,
        experiment_id: str | None = None,
    ) -> str:
        """Head-node command for a multi-node process-bigraph composite dispatch
        (backlog item 88).

        Reuses ``run_pbg.py``'s existing, already-generic ``--composite-id``/
        ``--overrides`` mode (``viva_api/compose/run_pbg.py`` -- the SAME runner
        ``stage_runner`` stages for the multi-generation batch_baseline path
        above) rather than a new script; ``composite_id`` is resolved through
        ``process_bigraph.composite_spec``'s own registry, so this works for
        ANY registered composite, not a hardcoded one.

        No new Ray multi-node "pre-connect" code is needed either -- confirmed
        empirically 2026-08-24 (backlog item 88, local Ray test): the entrypoint
        already exports ``RAY_ADDRESS`` on the head node before this command
        runs (``ray-batch-entrypoint.sh``), and process-bigraph's own
        ``RayProtocolRuntime.__init__`` already falls back to a bare
        ``ray.init(ignore_reinit_error=True, ...)`` when no explicit address is
        passed -- which already respects ``RAY_ADDRESS`` from the environment
        per Ray's own SDK (log-confirmed: "Using address ... set in the
        environment variable RAY_ADDRESS"). So a composite built with
        ``transport='ray'`` on the head attaches to the real multi-node cluster
        with zero code changes anywhere in this chain.
        """
        env = PBG_RUNNER_ENV
        if n_shards_default:
            env = f"{env} RAY_SHARDS_DEFAULT={int(n_shards_default)}"
        # --experiment-id: the run's identity, handed to run_pbg.py rather than
        # stuffed into params -- CompositeSpec.to_document raises on any key the
        # generator does not declare, and only the container can see the
        # schema. run_pbg injects it iff the composite declares experiment_id;
        # otherwise lineage-shaped composites default to the literal
        # "lineage_ray_batch" and every campaign's hive partition collides
        # (sms-ecoli#166). Same collision _nf_generator_params fixes for Nextflow.
        ident = f" --experiment-id {shlex.quote(str(experiment_id))}" if experiment_id else ""
        return (
            f"cd {V2ECOLI_DIR}"
            f" && aws s3 cp {runner_s3_uri} /tmp/run_pbg.py"
            f" && {env} python /tmp/run_pbg.py"
            f" --composite-id {shlex.quote(composite_id)}"
            f" --overrides {shlex.quote(json.dumps(params))} -n {int(steps)}{ident}"
        )

    def _stage_seed_override_caches(
        self,
        *,
        seed_overrides: dict[Any, dict[str, Any]],
        cache_s3: str,
        stage_dir: str,
    ) -> dict[Any, dict[str, Any]]:
        """Server-side copy each ``seed_overrides[*].cache_dir`` S3 prefix under
        this dispatch's own ``cache_s3`` prefix, then rewrite ``cache_dir`` to the
        LOCAL path it resolves to once the existing single ``stage_s3``->``stage_dir``
        sync (``ray-batch-entrypoint.sh``'s ``stage_inputs``, a recursive
        ``aws s3 sync``) pulls it down on every node.

        Real bug this fixes (backlog item 106, 2026-09-09): ``seed_overrides[*].
        cache_dir`` is a raw ``s3://`` URI, but ``_submit_mnp`` only ever stages ONE
        ``(stage_s3, stage_dir)`` pair -- the dispatch's own base/chassis cache.
        v2ecoli's ``build_lineage_ray_batch_document`` passes an override's
        ``cache_dir`` straight through to ``LineageProcess.config["cache_dir"]``
        unmodified (``v2ecoli/workflow/batch_lineage_ray.py`` line ~247), and
        ``read_cache_version`` does a plain ``os.path.exists()`` on it
        (``v2ecoli/library/cache_version.py`` line ~575) -- unconditionally False
        for an ``s3://`` string regardless of whether the real object exists,
        raising ``StaleCacheError``. Confirmed via direct source trace plus two
        independent real dispatches (database_id 733/738) each hitting this on a
        different seed (Ray's own non-deterministic task ordering picks whichever
        seed's generation-build task runs first before the job aborts).

        Copying each override's own prefix INTO the already-staged ``cache_s3``
        prefix (under a ``_seed_overrides/<seed>/`` subpath) means the EXISTING
        recursive sync already pulls it down — no new env var, no
        entrypoint-script change, no image rebuild. The only new work is a
        server-side S3->S3 copy plus rewriting each override's own ``cache_dir``
        to the resulting local path. A ``cache_dir`` that isn't an ``s3://`` URI
        (already local, or absent) passes through unchanged.
        """
        dest_bucket = get_settings().s3_work_bucket
        dest_prefix = data_layout.key_from_uri(cache_s3).rstrip("/")
        s3_client = boto3.client("s3", region_name=get_settings().storage_s3_region)
        rewritten: dict[Any, dict[str, Any]] = {}
        for seed, seed_override in seed_overrides.items():
            seed_override = dict(seed_override)
            cache_dir = seed_override.get("cache_dir")
            if isinstance(cache_dir, str) and cache_dir.startswith("s3://"):
                src_bucket, _, src_prefix = cache_dir.removeprefix("s3://").partition("/")
                src_prefix = src_prefix.rstrip("/")
                dest_seed_prefix = f"{dest_prefix}/_seed_overrides/{seed}"
                paginator = s3_client.get_paginator("list_objects_v2")
                copied = 0
                for page in paginator.paginate(Bucket=src_bucket, Prefix=f"{src_prefix}/"):
                    for obj in page.get("Contents", []):
                        rel_key = obj["Key"][len(src_prefix) + 1 :]
                        s3_client.copy_object(
                            Bucket=dest_bucket,
                            Key=f"{dest_seed_prefix}/{rel_key}",
                            CopySource={"Bucket": src_bucket, "Key": obj["Key"]},
                        )
                        copied += 1
                logger.info(
                    "Staged seed_overrides[%s].cache_dir (%d objects) from %s to s3://%s/%s",
                    seed,
                    copied,
                    cache_dir,
                    dest_bucket,
                    dest_seed_prefix,
                )
                seed_override["cache_dir"] = f"{stage_dir}/_seed_overrides/{seed}"
            rewritten[seed] = seed_override
        return rewritten

    async def _submit_multi_node_composite(
        self,
        ecoli_simulation: Simulation,
        database_service: DatabaseService,
        mnp_dispatch: dict[str, Any],
        *,
        correlation_id: str,
    ) -> JobId:
        """Submit a generic multi-node process-bigraph composite dispatch
        (backlog item 88) -- e.g. a colony composite distributed across N
        Ray-cluster nodes. This method never references any one composite by
        name; ``composite_id`` is resolved generically at runtime by
        ``run_pbg.py`` via ``process_bigraph.composite_spec``.

        Reuses ``_ensure_mnp_job_def``/``_submit_mnp`` UNCHANGED -- the exact
        same MNP job definition/queue the comparison-ensemble path above uses
        (``RayBatchOnDemandCE``/``ray-mnp``, confirmed live 2026-08-24: a real
        code comment on the no-placement-group standalone-queue routing logic
        already anticipates "colony sims" falling back to this queue for any
        genuine multi-node request). No new CDK job definition, no new
        compute environment.

        Submits ParCa first (1 node), then the composite (N nodes), gated on
        ParCa via the same ``depends_on`` pattern ``submit_ecoli_simulation_job``
        already uses -- any composite embedding a whole-cell process needs the
        same staged ParCa cache every other dispatch shape does.
        """
        composite_id = mnp_dispatch.get("composite_id")
        if not composite_id:
            raise ValueError("multi_node_dispatch.composite_id is required")
        num_nodes = int(mnp_dispatch.get("num_nodes") or 1)
        params = dict(mnp_dispatch.get("params") or {})
        _thread_injected_processes_into_params(params, ecoli_simulation.config)
        task_env = with_events_env(
            resolve_task_env(ecoli_simulation.config, mnp_dispatch),
            correlation_id=correlation_id,
            experiment_id=str(ecoli_simulation.experiment_id),
            sim_id=ecoli_simulation.database_id,
            backend="mnp",
            settings=get_settings(),
        )
        steps = int(mnp_dispatch.get("steps") or 1)
        # required_run_interval (item 105/#166, the K4-canary "under-run" empty-
        # emit bug: sms-ecoli#166 comment 5579146363, eagmon): `steps` silently
        # defaulting to 1 above is a REAL bug for a lineage-shaped composite --
        # `Composite.run(steps)` takes TOTAL SIMULATED TIME to advance, not a
        # tick count (confirmed from build_lineage_ray_batch_document's own
        # docstring and lineage_ray_batch's own @composite_generator schema,
        # v2ecoli/composites/lineage_ray_batch.py:35-67), and every
        # `ray:LineageProcess` node's own `interval` is `max_duration_per_gen`
        # -- process-bigraph only invokes a process whose next event falls
        # inside the run window, so `steps=1` against a 3600s interval invokes
        # nothing and nothing emits (reproduced: run(1) -> 0 rows, run(3600) ->
        # 1 row -- matches Dispatch 438's real final_state exactly). The
        # composite's OWN documented contract is
        # `n_generations * max_duration_per_gen` of total simulated time.
        #
        # Complementary to, NOT colliding with, sms-ecoli#283 (cplong90,
        # merged same session): that PR fixes the CLIENT side -- sms-ecoli's
        # own scripts/gen_cd2_cellonly_dispatches.py now derives and emits a
        # correct `steps` value in every generated request row. This fix is
        # the SERVER-side backstop for any request that still omits or
        # under-computes it (a different generator, a hand-built request, an
        # older still-in-flight config) -- belt-and-suspenders on two
        # independent layers of the same pipeline, not two attempts at the
        # same fix. `math.ceil` here matches that PR's own more rigorous
        # `math.ceil` choice over a truncating `int()`.
        #
        # This dispatch method never references any one composite by name (see
        # its own docstring) and has no remote visibility into a composite's
        # registered parameter schema (composite_spec resolution happens
        # inside the container, not here) -- so this can't be made fully
        # composite-generic without Eran's own proposed document-level
        # `required_run_interval` contract (not yet merged as of this note).
        # Interim, deliberately narrow fix: `n_generations` in `params` is the
        # signal that this IS a lineage-shaped request (only composites that
        # follow this convention set it at all); `max_duration_per_gen`
        # defaults to 3600.0 if the caller relies on the composite's own
        # default rather than setting it explicitly (the same default value
        # every real lineage/batch document-builder in v2ecoli declares:
        # lineage_ray_batch.py/batch_lineage_ray.py/workflow_nf.py/
        # lineage_step.py, confirmed by direct read, not assumed). `max()`
        # with the caller's own explicit `steps` so a deliberately larger
        # value is never clamped down. A composite that never sets
        # `n_generations` is completely unaffected (today's exact behavior).
        # `math.ceil`, not `int()` truncation (sms-ecoli#283, cplong90, same
        # bug independently fixed on the generator side): int() on a non-
        # integral product (an off-3600.0 max_duration_per_gen) would round
        # DOWN, handing the run marginally less than the required interval --
        # math.ceil always rounds up, so the clamp can only ever give a
        # composite AT LEAST what its own contract demands, never less.
        if "n_generations" in params:
            required_run_interval = int(params["n_generations"]) * float(params.get("max_duration_per_gen", 3600.0))
            steps = max(steps, math.ceil(required_run_interval))
        # cache_variant (item 105, mirrors chain-dispatch's own already-proven
        # job_scheduler.py pattern -- getattr(simulation.config, "cache_variant",
        # ...)): selects a variant-labeled derived ParCa cache (POST
        # /parca/new-gene-cache, viva-api#378) instead of the plain per-commit
        # default. Found missing 2026-09-04 firing the first-ever real strain-
        # specific pbg-native dispatch -- this composite path never had it,
        # only chain-dispatch did. None preserves today's behavior byte-for-
        # byte (cache_s3_uri's own variant=None default).
        cache_variant = mnp_dispatch.get("cache_variant") or None
        # require_clean_chain (item 106/#166, v2ecoli#735): opt-in per-dispatch --
        # a sibling of cache_variant, same reasoning as _stage_out_env's own
        # docstring. False (omitted) is byte-for-byte today's behavior.
        require_clean_chain = bool(mnp_dispatch.get("require_clean_chain", False))

        simulator = await database_service.get_simulator(simulator_id=ecoli_simulation.simulator_id)
        if simulator is None:
            raise ValueError(f"Simulator {ecoli_simulation.simulator_id} not found")

        settings = get_settings()
        commit = simulator.git_commit_hash
        experiment_id = ecoli_simulation.config.experiment_id

        job_def = self._ensure_mnp_job_def(self._image_uri(commit), commit)
        n_shards_default = self._mnp_node_vcpus(job_def)
        if n_shards_default:
            n_shards_default *= num_nodes

        cache_s3 = self.cache_s3_uri(commit, variant=cache_variant)
        runner_s3_uri = await self.stage_runner(experiment_id)

        base_tags = {
            "Project": "v2ecoli-multi-node-composite",
            "ExperimentId": str(experiment_id)[:255],
            "CompositeId": str(composite_id)[:255],
            "Commit": str(commit)[:12],
            # Makes the resolved cache choice visible on the job itself (sms-
            # ecoli#210, cplong90) -- "stock" or the requested variant name,
            # instead of requiring a manual decode of the staged S3 path.
            "CacheVariant": str(cache_variant or "stock")[:255],
            "Team": getattr(settings, "cost_team_tag", None) or "covertlab",
        }

        parca_job_id: str | None
        if cache_variant:
            # A variant cache is BY DEFINITION meant to already exist -- built
            # via POST /parca/new-gene-cache (viva-api#378) or an external
            # bridge sync (backlog item 106) -- never something this generic
            # composite path knows how to build itself (it has no `new_genes`/
            # `bundle_overrides` to give `_parca_command()`; those only ever
            # reach chain-dispatch's own `_sim_command`/`_seed_generation_
            # command`). Before this guard, submitting the plain ParCa job
            # below UNCONDITIONALLY -- with no existence check -- would
            # silently write a stock, un-perturbed cache into this exact
            # commit+variant slot whenever a fresh simulator got built (a new
            # commit means a brand-new, empty slot under the same variant
            # name), indistinguishable from the real thing short of manually
            # inspecting cache_version.json's own build_params. This is
            # exactly what happened to Dispatch 339:Run 1 and Dispatch
            # 340:Run 2 (backlog items 105/106, sms-ecoli#210) -- both
            # resolved to a stock cache at a freshly-built commit under a
            # real strain's own "candidate" variant name. Fail loud instead
            # of ever fabricating a substitute; `cache_variant=None` (every
            # other existing caller) is completely unaffected.
            from viva_api.dependencies import get_file_service

            file_service = get_file_service()
            if file_service is None:
                raise RuntimeError("FileService not initialized; cannot verify cache_variant staging.")
            existing = await file_service.get_listing(S3FilePath(s3_path=Path(data_layout.key_from_uri(cache_s3))))
            if not existing:
                raise ValueError(
                    f"cache_variant={cache_variant!r} has no staged ParCa cache at commit "
                    f"{commit!r} ({cache_s3}). This dispatch path never builds a variant "
                    f"cache itself -- build it first via POST /parca/new-gene-cache or an "
                    f"external bridge sync, then retry."
                )
            parca_job_id = None
        else:
            parca_job_id = self._submit_mnp(
                job_name=f"ray-parca-{commit}-{_rand_suffix()}",
                job_definition=job_def,
                num_nodes=1,
                ray_job_cmd=self._parca_command(),
                out_s3=cache_s3,
                out_dir=PARCA_CACHE_DIR,
                tags={**base_tags, "Phase": "parca"},
                task_env=task_env,
            )

        # seed_overrides[*].cache_dir (item 115/106): a raw s3:// URI naming a
        # PER-SEED founder cache, distinct from the base cache_s3 above -- stage
        # each one under cache_s3's own prefix (picked up by the existing single
        # stage_s3->stage_dir sync below) and rewrite the override to the local
        # path it resolves to. See _stage_seed_override_caches's own docstring
        # for the real bug this closes. A request with no seed_overrides (every
        # existing caller) is completely unaffected. Deliberately AFTER the
        # cache_variant existence-check above (not alongside cache_s3's own
        # computation) -- staging real objects into cache_s3's own prefix
        # before that guard's get_listing() call would make an UNBUILT
        # chassis's prefix look non-empty, silently defeating the exact
        # fail-loud check the guard's own comment cites Dispatch 339/340 for.
        seed_overrides = params.get("seed_overrides")
        if seed_overrides:
            params["seed_overrides"] = self._stage_seed_override_caches(
                seed_overrides=seed_overrides,
                cache_s3=cache_s3,
                stage_dir=PARCA_CACHE_DIR,
            )

        composite_job_id = self._submit_mnp(
            job_name=f"ray-mnp-composite-{experiment_id}-{_rand_suffix()}"[:128],
            job_definition=job_def,
            num_nodes=num_nodes,
            ray_job_cmd=self._multi_node_composite_command(
                composite_id=composite_id,
                params=params,
                steps=steps,
                runner_s3_uri=runner_s3_uri,
                n_shards_default=n_shards_default,
                experiment_id=str(experiment_id),
            ),
            out_s3=self._results_s3_uri(experiment_id),
            out_dir=SIM_OUT_DIR,
            stage_s3=cache_s3,
            stage_dir=PARCA_CACHE_DIR,
            depends_on=[parca_job_id] if parca_job_id else None,
            tags={**base_tags, "Phase": "composite"},
            require_clean_chain=require_clean_chain,
            task_env=task_env,
        )
        logger.info(
            "Multi-node composite %s (%s): parca job %s -> composite job %s (%d nodes)",
            experiment_id,
            composite_id,
            parca_job_id,
            composite_job_id,
            num_nodes,
        )
        job_id = JobId.ray(composite_job_id)
        # Backlog item 88: record this dispatch's OWN HpcRun row, under the SAME
        # correlation_id the generic caller (run_simulation_workflow) will use for
        # its own idempotent-insert guard -- mirrors submit_chain_dispatch_job's
        # identical pattern (simulation_service_ray.py, chain-dispatch background
        # task), for the identical reason: this row needs a field
        # (multi_node_composite_id) a generic caller has no way to populate.
        # JobScheduler.update_multi_node_jobs polls rows with this field set,
        # completely disjoint from list_active_chain_campaigns's own
        # chain_n_generations-based query.
        await database_service.insert_hpcrun(
            job_id=job_id,
            job_type=JobType.SIMULATION,
            ref_id=ecoli_simulation.database_id,
            correlation_id=correlation_id,
            multi_node_composite_id=composite_id,
        )
        return job_id

    def chain_base_tags(self, *, simulation: Simulation, commit: str) -> dict[str, str]:
        """Cost-allocation tag base shared by every per-seed chain job + the
        ParCa job that precedes them, mirroring ``submit_ecoli_simulation_job``'s
        ``base_tags`` (composite/condition don't apply — chain dispatch is
        v2ecoli-only)."""
        settings = get_settings()
        return {
            "Project": "v2ecoli-comparison",
            "ExperimentId": str(simulation.config.experiment_id)[:255],
            "Engine": "v2ecoli",
            "Commit": str(commit)[:12],
            "Team": getattr(settings, "cost_team_tag", None) or "covertlab",
        }

    def submit_chain_generation(
        self,
        *,
        seed: int,
        generation_index: int,
        experiment_id: str,
        commit: str,
        cache_s3: str,
        runner_s3_uri: str,
        tags: dict[str, str],
        batch_client: Any = None,
        injected_processes: dict[str, Any] | None = None,
        variants: dict[str, Any] | None = None,
        composite_id: str | None = None,
        exchange_fluxes: dict[str, Any] | None = None,
        exchange_flux_basis: str | None = None,
        expect_new_genes: str | None = None,
        expect_bundle_overrides: str | None = None,
        lineage_debug_division: bool = False,
        task_env: dict[str, str] | None = None,
    ) -> str:
        """Submit ONE seed's ONE generation as a standalone container-type job
        (backlog item 71 Phase 4) — the app-level-gated replacement for the
        superseded design's per-generation submission inside
        ``submit_chain_dispatch_job``'s own loop, which submitted every
        generation for every seed upfront via native Batch ``dependsOn``
        chains (item 68's own scaling-stall root cause). No ``depends_on``
        here: ``JobScheduler`` itself now decides WHEN to call this — only
        after confirming the previous generation (or ParCa, for generation 0)
        actually SUCCEEDED — so Batch's own dependency resolution is no longer
        part of the sequencing at all. Mirrors ``_seed_generation_command``'s
        own per-seed S3 layout exactly (unchanged by this migration — see that
        method's docstring); only the job TYPE and dependency model change.

        ``injected_processes``/``variants``/``composite_id``/``exchange_fluxes``/
        ``exchange_flux_basis`` (backlog items 93, 105): passed straight through
        to ``_seed_generation_command`` — see that method's own docstring.
        ``JobScheduler`` is the real caller, re-deriving all five from the
        campaign's own ``Simulation.config`` every tick (restart-safe, same as
        every other piece of per-tick state here).
        """
        job_def = self._ensure_container_job_def(self._image_uri(commit), commit)
        return self._submit_container(
            job_name=f"chain-seed{seed}-gen{generation_index}-{experiment_id}-{_rand_suffix()}"[:128],
            job_definition=job_def,
            job_cmd=self._seed_generation_command(
                seed=seed,
                generation_index=generation_index,
                experiment_id=experiment_id,
                runner_s3_uri=runner_s3_uri,
                injected_processes=injected_processes,
                variants=variants,
                composite_id=composite_id,
                exchange_fluxes=exchange_fluxes,
                exchange_flux_basis=exchange_flux_basis,
            ),
            out_s3=data_layout.RayLayout.seed_results_uri(experiment_id, seed),
            out_dir=SIM_OUT_DIR,
            stage_s3=cache_s3,
            stage_dir=PARCA_CACHE_DIR,
            tags={**tags, "Seed": str(seed), "Generation": str(generation_index)},
            batch_client=batch_client,
            expect_new_genes=expect_new_genes,
            expect_bundle_overrides=expect_bundle_overrides,
            lineage_debug_division=lineage_debug_division,
            task_env=task_env,
        )

    async def submit_chain_generation_batch(
        self,
        *,
        seeds: list[int],
        generation_index: int,
        experiment_id: str,
        commit: str,
        cache_s3: str,
        runner_s3_uri: str,
        tags: dict[str, str],
        injected_processes: dict[str, Any] | None = None,
        variants: dict[str, Any] | None = None,
        composite_id: str | None = None,
        exchange_fluxes: dict[str, Any] | None = None,
        exchange_flux_basis: str | None = None,
        expect_new_genes: str | None = None,
        expect_bundle_overrides: str | None = None,
        lineage_debug_division: bool = False,
        task_env: dict[str, str] | None = None,
    ) -> dict[int, str]:
        """Submit the SAME generation index for MULTIPLE seeds at once,
        TPS-paced below the account-wide ``SubmitJob`` rate limit (reuses
        ``_SubmitJobPacer`` + a dedicated retry-configured client — the same
        mechanism the superseded upfront-chain design used for its own N*G
        burst). Still needed for the one remaining genuine burst moment under
        the per-seed app-level-gated model: every seed's generation 0, fanned
        out the instant ParCa succeeds. Every OTHER generation-index step from
        then on submits at most one job per seed per campaign per poll
        interval — naturally spread out by the 30s tick cadence, no pacing
        needed there (``submit_chain_generation`` alone is used for those).

        A per-seed submission failure (even after retry-on-throttle) is logged
        and that seed is simply omitted from the returned mapping — mirrors
        the superseded design's own "truncate just this seed's chain" failure
        semantics; other seeds are unaffected.

        ``injected_processes``/``variants``/``composite_id``/``exchange_fluxes``/
        ``exchange_flux_basis`` (backlog items 93, 105): the SAME values for
        every seed in this batch — one campaign, one config — forwarded to each
        seed's own ``submit_chain_generation`` call below.
        """
        pacer = _SubmitJobPacer()
        submit_client = boto3.client(
            "batch",
            region_name=get_settings().batch_region,
            config=Config(retries={"mode": "standard", "max_attempts": _SUBMIT_JOB_MAX_ATTEMPTS}),
        )
        submitted: dict[int, str] = {}
        for seed in seeds:
            await pacer.wait()
            try:
                submitted[seed] = self.submit_chain_generation(
                    seed=seed,
                    generation_index=generation_index,
                    experiment_id=experiment_id,
                    commit=commit,
                    cache_s3=cache_s3,
                    runner_s3_uri=runner_s3_uri,
                    tags=tags,
                    batch_client=submit_client,
                    injected_processes=injected_processes,
                    variants=variants,
                    composite_id=composite_id,
                    exchange_fluxes=exchange_fluxes,
                    exchange_flux_basis=exchange_flux_basis,
                    expect_new_genes=expect_new_genes,
                    expect_bundle_overrides=expect_bundle_overrides,
                    lineage_debug_division=lineage_debug_division,
                    task_env=task_env,
                )
            except Exception:
                logger.exception(
                    "Chain dispatch %s: seed %d generation %d submission failed "
                    "(even after retry-on-throttle) -- this seed's chain ends here; "
                    "other seeds are unaffected",
                    experiment_id,
                    seed,
                    generation_index,
                )
                continue
        return submitted

    def submit_chain_lineage(
        self,
        *,
        seed: int,
        n_generations: int,
        experiment_id: str,
        commit: str,
        cache_s3: str,
        runner_s3_uri: str,
        tags: dict[str, str],
        batch_client: Any = None,
        injected_processes: dict[str, Any] | None = None,
        variants: dict[str, Any] | None = None,
        composite_id: str | None = None,
        exchange_fluxes: dict[str, Any] | None = None,
        exchange_flux_basis: str | None = None,
        expect_new_genes: str | None = None,
        expect_bundle_overrides: str | None = None,
        lineage_debug_division: bool = False,
        task_env: dict[str, str] | None = None,
    ) -> str:
        """Submit ONE seed's WHOLE lineage as a single standalone container job:
        all ``n_generations`` in one ``LineageProcess``.

        Replaces the per-generation ``submit_chain_generation`` on the chain
        path. Because the whole lineage now runs in one process,
        ``lineage_time_offset`` accumulates across generations and a
        ``field_timeline`` dose scheduled at a cumulative time actually fires
        (the Run-3 fix; see ``_seed_lineage_command``). Per-seed independence is
        preserved — one job per seed, no cross-seed barrier — so this keeps the
        scheduler's fully-asynchronous per-seed model.
        """
        # Same container job def as the per-generation jobs: peak memory of a
        # whole-lineage LineageProcess matches one generation (advance_generation
        # flushes + consolidates each generation, so no whole-lineage buffer
        # growth), and _submit_container sets no attemptDurationSeconds, so the
        # longer wall-clock of N generations simply runs. (Spot interruption of a
        # long lineage loses the partial run's job success even though gens
        # 0..N-1 are durably on disk; resume-from-generation is a possible
        # follow-up -- see docs/design-chain-one-lineageprocess.md.)
        job_def = self._ensure_container_job_def(self._image_uri(commit), commit)
        return self._submit_container(
            job_name=f"chain-seed{seed}-lineage-{experiment_id}-{_rand_suffix()}"[:128],
            job_definition=job_def,
            job_cmd=self._seed_lineage_command(
                seed=seed,
                n_generations=n_generations,
                experiment_id=experiment_id,
                runner_s3_uri=runner_s3_uri,
                injected_processes=injected_processes,
                variants=variants,
                composite_id=composite_id,
                exchange_fluxes=exchange_fluxes,
                exchange_flux_basis=exchange_flux_basis,
            ),
            out_s3=data_layout.RayLayout.seed_results_uri(experiment_id, seed),
            out_dir=SIM_OUT_DIR,
            stage_s3=cache_s3,
            stage_dir=PARCA_CACHE_DIR,
            tags={**tags, "Seed": str(seed), "Generations": str(n_generations)},
            batch_client=batch_client,
            expect_new_genes=expect_new_genes,
            expect_bundle_overrides=expect_bundle_overrides,
            lineage_debug_division=lineage_debug_division,
            task_env=task_env,
        )

    async def submit_chain_lineage_batch(
        self,
        *,
        seeds: list[int],
        n_generations: int,
        experiment_id: str,
        commit: str,
        cache_s3: str,
        runner_s3_uri: str,
        tags: dict[str, str],
        injected_processes: dict[str, Any] | None = None,
        variants: dict[str, Any] | None = None,
        composite_id: str | None = None,
        exchange_fluxes: dict[str, Any] | None = None,
        exchange_flux_basis: str | None = None,
        expect_new_genes: str | None = None,
        expect_bundle_overrides: str | None = None,
        lineage_debug_division: bool = False,
        task_env: dict[str, str] | None = None,
    ) -> dict[int, str]:
        """Fan out ONE whole-lineage job per seed, TPS-paced below the
        account-wide ``SubmitJob`` rate limit (same ``_SubmitJobPacer`` +
        retry-configured client as ``submit_chain_generation_batch``).

        Replaces the per-generation ``submit_chain_generation_batch`` on the
        chain path. This is the ONLY submission burst now: every seed's lineage
        is fanned out once, the instant ParCa succeeds; there is no per-seed
        per-generation follow-up submission (the scheduler only polls each
        lineage job to resolution). A per-seed submission failure is logged and
        that seed omitted from the returned mapping — other seeds unaffected.
        """
        pacer = _SubmitJobPacer()
        submit_client = boto3.client(
            "batch",
            region_name=get_settings().batch_region,
            config=Config(retries={"mode": "standard", "max_attempts": _SUBMIT_JOB_MAX_ATTEMPTS}),
        )
        submitted: dict[int, str] = {}
        for seed in seeds:
            await pacer.wait()
            try:
                submitted[seed] = self.submit_chain_lineage(
                    seed=seed,
                    n_generations=n_generations,
                    experiment_id=experiment_id,
                    commit=commit,
                    cache_s3=cache_s3,
                    runner_s3_uri=runner_s3_uri,
                    tags=tags,
                    batch_client=submit_client,
                    injected_processes=injected_processes,
                    variants=variants,
                    composite_id=composite_id,
                    exchange_fluxes=exchange_fluxes,
                    exchange_flux_basis=exchange_flux_basis,
                    expect_new_genes=expect_new_genes,
                    expect_bundle_overrides=expect_bundle_overrides,
                    lineage_debug_division=lineage_debug_division,
                    task_env=task_env,
                )
            except Exception:
                logger.exception(
                    "Chain dispatch %s: seed %d lineage submission failed "
                    "(even after retry-on-throttle) -- this seed's lineage is "
                    "omitted; other seeds are unaffected",
                    experiment_id,
                    seed,
                )
                continue
        return submitted

    async def _submit_chain_dispatch_background(
        self,
        ecoli_simulation: Simulation,
        database_service: DatabaseService,
        *,
        correlation_id: str,
    ) -> JobId:
        """Run ``submit_chain_dispatch_job`` as a background task; return a trackable id at once.

        A chain-dispatch campaign issues ``n_seeds * n_generations`` individual
        AWS Batch ``SubmitJob`` calls, paced below the account-wide TPS cap. For
        the canonical 1000x10 shape that is 10,000 calls and roughly 15 minutes
        of wall time — all of it inside the single ``POST /api/v1/simulations``
        request while the submission loop runs inline. A real production
        dispatch (2026-08-14) proved the consequence: the calling client's HTTP
        timeout fired long before the loop finished, so the user was told the
        dispatch had FAILED while viva-api went right on submitting the real,
        AWS-billed campaign. The obvious reaction to that message — retry —
        would have started a second, duplicate, paid campaign on top of the
        first.

        The fix reuses the pattern this service already uses for the other
        multi-minute operation it owns, the DooD image build
        (``submit_build_image_job``): hand the slow coroutine to
        ``LocalTaskService``, return its ``JobId.local(...)`` immediately, let
        the caller poll. No new machinery is involved — because every backend
        service shares ONE process-wide ``LocalTaskService`` (see
        ``viva_api.dependencies._init_simulation_service``), ``get_job_status``
        — and therefore ``GET /api/v1/simulations/{id}/status`` — already
        resolves such an id, reporting RUNNING while the submission loop is
        still going and FAILED if it crashes outright. ``cancel_job`` already
        routes LOCAL ids to ``LocalTaskService.cancel``, so cancelling a
        still-submitting campaign comes along for free.

        ``submit_chain_dispatch_job`` itself is UNCHANGED and still runs
        synchronously to completion for its direct callers (its own unit tests
        and the real-AWS integration test); only this one call site is
        asynchronous.

        THE PLACEHOLDER ROW, and why its chain fields must stay ``None``: the
        campaign's REAL ``HpcRun`` row is inserted by
        ``submit_chain_dispatch_job`` itself, as its very last action — minutes
        from now. Until then a status lookup would find nothing at all, so this
        method records a placeholder row carrying the LOCAL task id. It
        deliberately leaves BOTH ``chain_n_generations`` and
        ``chain_final_job_ids`` unset:

          - ``DatabaseService.list_active_chain_campaigns`` (the scheduler's
            poll set) discriminates on ``chain_n_generations IS NOT NULL``
            ALONE. Setting it here would enroll the placeholder in that poll set
            before a single per-seed job exists.
          - ``get_chain_campaign_result([])`` returns ``terminal=True`` with
            zero successes by definition, so ``JobScheduler._advance_chain_campaign``
            would then immediately mark the campaign FAILED — recreating, inside
            viva-api this time, the very false-failure this method exists to
            eliminate.

        With both left ``None``, ``get_simulation_status`` takes its ordinary
        non-campaign branch and reports the LOCAL task's own status, which is
        the honest answer while submission is in flight. Once the background
        task finishes, the campaign row it inserts (a SECOND real row, under the
        same ``correlation_id``) supersedes this placeholder for every later
        read: ``get_hpcrun_by_ref`` resolves the highest ``id``. The handler's
        own idempotent-insert guard (``viva_api.common.handlers.simulations``,
        keyed on ``correlation_id``) sees this placeholder and correctly skips
        adding a third, generic row.
        """
        # Gate the background task on the placeholder being committed. Without
        # the gate, `create_task` followed by `await insert_hpcrun(...)` lets the
        # campaign coroutine run during that await, and the two inserts can land
        # in either order. The wrong order is NOT benign: `get_hpcrun_by_ref`
        # resolves the highest id, so a placeholder written AFTER the real
        # campaign row would shadow it permanently, and the plain status path
        # would report the whole campaign COMPLETED the moment the submission
        # loop finished — while every one of its N*G real jobs was still queued
        # or running. An asyncio.Event makes the ordering a guarantee instead of
        # a race the mocked-out test environment happens to usually win.
        placeholder_recorded = asyncio.Event()

        async def _run() -> JobId:
            await placeholder_recorded.wait()
            return await self.submit_chain_dispatch_job(
                ecoli_simulation, database_service, correlation_id=correlation_id
            )

        task_job_id = self._local.submit(_run(), name=f"chain-dispatch-{ecoli_simulation.config.experiment_id}")
        try:
            placeholder = await database_service.insert_hpcrun(
                job_id=task_job_id,
                job_type=JobType.SIMULATION,
                ref_id=ecoli_simulation.database_id,
                correlation_id=correlation_id,
            )
        except Exception:
            # Nothing was recorded, so nothing may run: releasing the gate here
            # would dispatch a real, billed campaign for a request whose caller
            # is about to be handed an error.
            self._local.cancel(task_job_id.value)
            raise
        # viva-api#414: bind the placeholder to its task so the row is finalized
        # from the task's own outcome (COMPLETED once the real campaign row has
        # superseded it; FAILED, with the exception, if submission crashed).
        # Before this the placeholder stayed `running` in the DB forever, and a
        # submission crash was visible only in this process's memory.
        await self._local.bind_hpcrun(task_job_id.value, placeholder.database_id, database_service)
        placeholder_recorded.set()
        logger.info(
            "Chain dispatch %s: submitting the campaign in the background as local task %s "
            "(request returns now; poll GET /simulations/{id}/status for progress)",
            ecoli_simulation.config.experiment_id,
            task_job_id.value,
        )
        return task_job_id

    async def submit_chain_dispatch_job(
        self,
        ecoli_simulation: Simulation,
        database_service: DatabaseService,
        correlation_id: str | None = None,
    ) -> JobId:
        """Kick off a per-seed chain-dispatch campaign (backlog item 33 rework,
        further reworked by item 71 Phase 4): submit ONLY ParCa here, as a
        plain container-type job (backlog item 71 — no real inter-node traffic
        to protect, same reasoning as ``submit_parca_job``). The N*G per-seed
        generation jobs are NOT submitted upfront anymore — that upfront-
        ``dependsOn`` design was item 68's own scaling-stall root cause (AWS
        Batch's compute-environment scaling reconciliation never engaged for a
        huge MNP+dependsOn backlog, confirmed via CloudTrail showing zero
        scaling API activity despite ~1000 RUNNABLE jobs). Generation
        submission moves to ``JobScheduler``'s existing 30s poll loop
        (``_advance_chain_campaign``, DB-driven, restart-safe), which submits
        exactly ONE generation per seed at a time, only once the previous one
        (or ParCa, for generation 0) is confirmed SUCCEEDED — app-level gating
        instead of native Batch dependency chains. Still a true v2 analogy of
        vEcoli-private's own fully-asynchronous per-seed Nextflow execution:
        seed 5 can be on generation 8 while seed 800 is on generation 1,
        throttled only by available compute, never by a cross-seed barrier —
        that property now comes from the scheduler's own per-seed
        independence, not from Batch dependsOn.

        This method returns as soon as ParCa is submitted and the campaign's
        initial tracking row is written — no more N*G-submission wall time to
        wait out inline. ``_submit_chain_dispatch_background`` still wraps it
        in a background task (cheap now, but keeps that caller's contract
        unchanged rather than special-casing "fast" vs "slow" chain-dispatch
        calls).

        ``correlation_id``: unchanged from before — this method always records
        its OWN ``HpcRun`` row (the campaign-tracking row needs
        ``chain_n_generations``/the ``chain_current_*``/``chain_final_job_ids``
        fields, which a generic caller has no way to populate). One is
        generated fresh here when called with none (e.g. directly by tests);
        the real dispatch entrypoint threads its own request-scoped id through
        instead, so a status lookup by that id resolves to this exact row (see
        ``viva_api.common.handlers.simulations``'s idempotent-insert guard).

        Unlike the array-job design predating item 33, ``n_seeds >= 2`` is not
        required (no AWS Batch array-size floor applies — every seed's chain is
        independent standalone jobs). ``n_generations >= 2`` is still required
        — a single-generation request has nothing to chain; use
        ``submit_ecoli_simulation_job``.

        The initial campaign row's per-seed tracking fields all start "empty":
        ``chain_current_job_ids``/``chain_current_generation`` are
        ``[None] * n_seeds`` (no generation submitted yet, gated on ParCa),
        ``chain_parca_done=False``, ``chain_final_job_ids=[]`` (filled
        incrementally by the scheduler as each seed's chain resolves — see
        that method for why this keeps the existing analysis-fan-in consumer,
        ``get_chain_campaign_result``, working unchanged once every seed has
        contributed its entry). Returns the ParCa job's ``JobId`` — the one
        well-defined "campaign kickoff" marker, unchanged from before.
        """
        parca_dataset = await database_service.get_parca_dataset(parca_dataset_id=ecoli_simulation.parca_dataset_id)
        if parca_dataset is None:
            raise ValueError(f"ParcaDataset with ID {ecoli_simulation.parca_dataset_id} not found.")
        simulator = await database_service.get_simulator(simulator_id=ecoli_simulation.simulator_id)
        if simulator is None:
            raise ValueError(f"Simulator {ecoli_simulation.simulator_id} not found")

        commit = simulator.git_commit_hash
        config = ecoli_simulation.config
        n_seeds = int(ecoli_simulation.num_seeds or getattr(config, "n_init_sims", None) or 1)
        n_generations = int(config.generations or 1)
        if n_generations < 2:
            raise ValueError(
                "submit_chain_dispatch_job requires generations > 1 "
                "(use submit_ecoli_simulation_job for single-generation runs)"
            )

        experiment_id = str(ecoli_simulation.config.experiment_id)
        cache_s3 = self.cache_s3_uri(commit)
        base_tags = self.chain_base_tags(simulation=ecoli_simulation, commit=commit)
        container_job_def = self._ensure_container_job_def(self._image_uri(commit), commit)

        # Backlog item 93: a legacy config's own parca_options.new_genes (e.g.
        # a custom strain's new-gene insertion) is a real SimulationConfig
        # field, not an extra -- read directly, matching config.generations
        # above (extra="allow" only applies to genuinely undeclared keys).
        new_genes = getattr(config.parca_options, "new_genes", None)
        # Backlog item 104 (sms-ecoli#184 / viva-api#365): same generic
        # passthrough, missed in the item-93 pass -- parca_options.bundle_overrides
        # survived on the stored request but was never forwarded, so ParCa built
        # from defaults only and any keys the overrides supply were absent.
        bundle_overrides = getattr(config.parca_options, "bundle_overrides", None)
        # Same generic rnaseq_source passthrough (item 106/#166 chassis-provenance
        # thread), same reasoning as new_genes/bundle_overrides above.
        rnaseq_source = getattr(config.parca_options, "rnaseq_source", None)
        # Fixed BEFORE the ParCa job is submitted so the job's PBG_* identity env
        # and the campaign row's correlation_id derive the same trace id.
        campaign_correlation_id = correlation_id or f"chain-campaign-{experiment_id}-{_rand_suffix()}"
        parca_job_id = self._submit_container(
            job_name=f"ray-parca-{commit}-{_rand_suffix()}",
            job_definition=container_job_def,
            job_cmd=self._parca_command(
                new_genes=new_genes, bundle_overrides=bundle_overrides, rnaseq_source=rnaseq_source
            ),
            out_s3=cache_s3,
            out_dir=PARCA_CACHE_DIR,
            tags={**base_tags, "Phase": "parca"},
            task_env=with_events_env(
                resolve_task_env(config),
                correlation_id=campaign_correlation_id,
                experiment_id=experiment_id,
                sim_id=ecoli_simulation.database_id,
                backend="chain",
                tags={"phase": "parca"},
                settings=get_settings(),
            ),
        )

        await database_service.insert_hpcrun(
            job_id=JobId.ray(parca_job_id),
            job_type=JobType.SIMULATION,
            ref_id=ecoli_simulation.database_id,
            correlation_id=campaign_correlation_id,
            chain_n_generations=n_generations,
            chain_final_job_ids=[],
            chain_current_job_ids=[None] * n_seeds,
            chain_current_generation=[None] * n_seeds,
            chain_parca_done=False,
        )
        logger.info(
            "Chain dispatch %s: parca job %s submitted; %d seeds x %d generations "
            "will be advanced incrementally by JobScheduler once ParCa succeeds",
            experiment_id,
            parca_job_id,
            n_seeds,
            n_generations,
        )
        return JobId.ray(parca_job_id)

    async def submit_campaign_analysis(
        self,
        *,
        simulation: Simulation,
        database_service: DatabaseService,
        commit: str,
        total_n_seeds: int,
        n_generations: int,
    ) -> str | None:
        """Submit the analysis DAG node for a chain-dispatch campaign that the
        analysis-fan-in poller (``JobScheduler._advance_chain_campaign``) has
        just confirmed all-terminal — called exactly once per campaign, after
        every tracked seed chain (whether it fully succeeded or was permanently
        failed/truncated) has resolved. By construction everything this
        analysis depends on has ALREADY finished by the time this runs, so it
        reuses item 24's existing analysis-job submission code
        (``_submit_analysis_job``) completely as-is, just with ``sim_job_id=
        None`` — no native ``dependsOn`` needed; the poller's own "all tracked
        jobs terminal" check already provided the "wait for all" semantics.

        ``total_n_seeds`` is the campaign's ORIGINALLY REQUESTED seed count
        (not however many chains actually succeeded) — matches the superseded
        design's own resolved semantics: the analysis resolves "applicable"
        modules against the campaign's INTENDED shape.
        """
        base_tags = self.chain_base_tags(simulation=simulation, commit=commit)
        # Backlog item 71: _submit_analysis_job now submits via _submit_container,
        # so this must resolve a container job def, not an MNP one.
        container_job_def = self._ensure_container_job_def(self._image_uri(commit), commit)
        # viva-api#448: same getattr(simulation.config, "cache_variant", ...)
        # pattern job_scheduler.py's own chain-dispatch cache-staging already uses
        # — without it, a strain-specific campaign's analysis silently reads the
        # plain per-commit stock simData instead of its own real cache.
        cache_variant = getattr(simulation.config, "cache_variant", None)
        return await self._submit_analysis_job(
            simulation=simulation,
            database_service=database_service,
            job_definition=container_job_def,
            commit=commit,
            sim_job_id=None,
            n_seeds=total_n_seeds,
            n_generations=n_generations,
            depends_type=None,
            tags={**base_tags, "Phase": "analysis"},
            cache_variant=cache_variant,
        )

    def _multi_node_analysis_command(
        self,
        *,
        experiment_id: str,
        composite_id: str,
        history_uri: str,
        out_uri: str,
        n_seeds: int | None = None,
        n_generations: int = 1,
        modules: dict[str, dict[str, Any]] | str | None = None,
        sim_data_uri: str | None = None,
    ) -> str:
        """Build the "Analysis flush" DAG node's command for a generic
        multi-node process-bigraph composite dispatch (backlog item 88).

        Unlike ``_analysis_command`` (a fixed hive-parquet seed x generation
        sweep, v2ecoli-specific analysis modules), this points at a separate,
        generic entrypoint (``scripts/run_multi_node_analysis.py``) that
        tries TWO read paths in order (item 109's own fix): (1) when
        ``n_seeds`` is given, a hive-parquet sweep read via the same
        DuckDB-httpfs mechanism ``run_standalone_analysis.py`` already uses --
        the shape a ``lineage_ray_batch`` pbg-native dispatch actually
        produces; (2) the original flat-file fallback (``emitter_history
        .json``/``final_state.json`` -> ``v2ecoli.workflow.flush.run_flush``),
        colony's own shape (item 88), unconditionally tried when path 1 is
        not applicable or finds nothing. Nothing in either path branches on
        ``composite_id`` -- a composite-specific renderer, if one is ever
        needed, is a new registered post-sim step, never a per-composite
        branch here. Writes whichever path succeeds + ``_manifest.json`` to
        ``out_uri``, matching the same S3-manifest contract
        ``GET /analyses/{id}/status`` already probes for every other
        analysis kind.

        ``modules`` mirrors ``_analysis_command``'s own encoding exactly (same
        real bug class to avoid): the ``"applicable"`` keyword must ride as a
        bare, unquoted-in-JSON-sense token, not ``json.dumps``'d -- JSON-
        encoding it would produce the 12-character string ``'"applicable"'``
        (quotes included), which the receiving script's own ``.strip().lower()
        == "applicable"`` check would silently miss, falling through to
        ``json.loads`` and handing back the bare word as if it were a real
        module mapping -- a real bug caught here before shipping, not a
        theoretical one.

        ``sim_data_uri`` (viva-api#448): exported as ``V2ECOLI_SIM_DATA`` before
        the script runs, the SAME fallback ``_analysis_command``'s own analysis
        node relies on (``analysis_runner.resolve_sim_data``'s resolution order:
        a co-located pickle, then this env var, then the image's own stock
        knowledge-base build). This node's own analysis container never stages
        a ParCa cache locally (no ``stage_s3``/``stage_dir`` on its dispatch),
        so without this a candidate strain's analysis silently falls through to
        the stock knowledge-base build. None (every caller before #448)
        preserves the exact prior fallback-only behavior.
        """
        cmd = f"cd {V2ECOLI_DIR}"
        if sim_data_uri:
            cmd += f" && export V2ECOLI_SIM_DATA={shlex.quote(sim_data_uri)}"
        cmd += (
            f" && python scripts/run_multi_node_analysis.py"
            f" --composite-id {shlex.quote(composite_id)}"
            f" --history-uri {shlex.quote(history_uri)}"
            f" --out-uri {shlex.quote(out_uri)}"
            f" --experiment-id {shlex.quote(experiment_id)}"
        )
        if n_seeds:
            modules_arg = modules if isinstance(modules, str) else json.dumps(modules or {})
            cmd += f" --n-seeds {int(n_seeds)} --n-generations {int(n_generations)}"
            cmd += f" --modules {shlex.quote(modules_arg)}"
        return cmd

    async def submit_multi_node_analysis(
        self,
        *,
        simulation: Simulation,
        database_service: DatabaseService,
        commit: str,
        composite_id: str,
    ) -> str | None:
        """Submit the "Analysis flush" node for a multi-node composite dispatch
        (backlog item 88) that ``JobScheduler.update_multi_node_jobs`` has just
        confirmed COMPLETED -- the multi-node-composite analogue of
        ``submit_campaign_analysis``, deliberately NOT a shared function with
        it (different command, different params, different DB fields) to keep
        the chain-dispatch analysis-fan-in path this mirrors completely
        untouched. Same best-effort-but-never-silent contract as
        ``_submit_analysis_job``: a submission failure is recorded as a FAILED
        row, not just logged, so it's visible through the same
        ``GET /analyses/{id}/status`` surface a successful submission uses.

        Item 109: also extracts ``n_seeds``/``n_generations`` from the
        ORIGINAL dispatch's own stored ``multi_node_dispatch.params`` (already
        on ``simulation.config`` -- no new DB column needed) and the analysis
        module selection via ``analysis_modules_for`` -- the SAME already-
        tested resolver ``_analysis_command`` (chain-dispatch's own analysis
        node) already uses, reused rather than re-derived, so an unset/empty
        ``analysis_options`` resolves to the ``"applicable"`` keyword here
        too, not a silently-different "run nothing" -- both threaded into the
        command builder so a ``lineage_ray_batch``-shaped dispatch's real
        hive-parquet output is actually read, not just colony's own
        flat-file fallback.
        """
        experiment_id = str(simulation.config.experiment_id)
        mnp_dispatch = getattr(simulation.config, "multi_node_dispatch", None) or {}
        dispatch_params = mnp_dispatch.get("params") or {} if isinstance(mnp_dispatch, dict) else {}
        n_seeds = dispatch_params.get("n_seeds")
        n_generations = int(dispatch_params.get("n_generations") or 1)
        modules = analysis_modules_for(simulation.config)
        # viva-api#448: cache_variant is a SIBLING of params on multi_node_dispatch
        # (the same shape _submit_multi_node_composite's own guard reads) -- without
        # threading it here too, a candidate strain's analysis silently reads the
        # image's own stock knowledge-base build instead of the real dispatch's cache.
        cache_variant = mnp_dispatch.get("cache_variant") if isinstance(mnp_dispatch, dict) else None
        sim_data_uri = f"{self.cache_s3_uri(commit, variant=cache_variant)}simData.cPickle"
        analysis_name = f"analysis-mnp-{experiment_id[:20]}-{_rand_suffix()}"
        results_uri = self._results_s3_uri(experiment_id).rstrip("/")
        result_uri = f"{results_uri}/analyses/{analysis_name}"
        settings = get_settings()
        container_job_def = self._ensure_container_job_def(self._image_uri(commit), commit)
        tags = {
            "Project": "v2ecoli-multi-node-composite",
            "ExperimentId": experiment_id[:255],
            "CompositeId": str(composite_id)[:255],
            "Commit": str(commit)[:12],
            "Team": getattr(settings, "cost_team_tag", None) or "covertlab",
            "Phase": "analysis",
        }
        params: dict[str, Any] = {
            "composite_id": composite_id,
            "history_uri": results_uri,
            "analysis_name": analysis_name,
            "trigger": "multi-node-dispatch-flush",
            # ORMAnalysis.to_dto() unconditionally reads config["analysis_options"]
            # (AnalysisConfigOptions requires experiment_id) -- mirror the shape
            # _submit_analysis_job's own params dict already writes, so to_dto()
            # doesn't KeyError for this analysis kind either.
            "analysis_options": {"experiment_id": [experiment_id]},
        }
        try:
            analysis_job_id = self._submit_container(
                job_name=f"ray-mnp-analysis-{experiment_id}-{_rand_suffix()}"[:128],
                job_definition=container_job_def,
                job_cmd=self._multi_node_analysis_command(
                    experiment_id=experiment_id,
                    composite_id=composite_id,
                    history_uri=results_uri,
                    out_uri=result_uri,
                    n_seeds=n_seeds,
                    n_generations=n_generations,
                    modules=modules,
                    sim_data_uri=sim_data_uri,
                ),
                out_s3=self._results_s3_uri(experiment_id),
                out_dir=ANALYSIS_OUT_DIR,
                depends_on=None,
                depends_type=None,
                tags=tags,
                memory_class=analysis_memory_class(modules, n_seeds=n_seeds, n_generations=n_generations),
            )
        except Exception as e:
            logger.exception("Multi-node analysis submission failed for %s", experiment_id)
            await database_service.record_analysis(
                experiment_id=experiment_id,
                n_tp=None,
                status=AnalysisStatusDB.FAILED,
                config=params,
                name=analysis_name,
                simulation_id=simulation.database_id,
                backend="ray",
                result_uri=result_uri,
                error_message=f"multi-node analysis submission failed: {type(e).__name__}: {e}",
            )
            return None
        await database_service.record_analysis(
            experiment_id=experiment_id,
            n_tp=None,
            status=AnalysisStatusDB.COMPUTING,
            config=params,
            name=analysis_name,
            simulation_id=simulation.database_id,
            backend="ray",
            job_id_ext=str(analysis_job_id),
            result_uri=result_uri,
        )
        logger.info(
            "Multi-node composite %s (%s): analysis flush -> job %s",
            experiment_id,
            composite_id,
            analysis_job_id,
        )
        return analysis_job_id

    @override
    async def read_config_template(
        self,
        simulator_version: SimulatorVersion,
        config_filename: str,
        allow_default_fallback: bool = False,
    ) -> str:
        return await fetch_config_template(
            simulator_version, config_filename, get_settings().github_token, allow_default_fallback
        )

    @override
    async def discover_repo_contents(self, simulator_version: SimulatorVersion) -> RepoDiscovery:
        return await fetch_repo_discovery(simulator_version, get_settings().github_token)

    @override
    async def get_job_status(self, job_id: JobId) -> JobStatusInfo | None:
        """Status — LOCAL, a K8s-hosted Nextflow head, or AWS Batch describe_jobs."""
        if job_id.backend == JobBackend.LOCAL:
            return self._local.get_status(job_id.value)
        if job_id.backend == JobBackend.K8S_NEXTFLOW:
            if self._k8s is None:
                raise RuntimeError(
                    f"job {job_id.value} is a K8s-hosted Nextflow head, but this service has no "
                    f"K8sJobService (k8s_job_namespace unset)"
                )
            # The value is a Job NAME, not a Batch job id -- describe_jobs would
            # return an empty list and this would report None rather than fail.
            return self._k8s.get_job_status(job_id.value)

        response = self._batch().describe_jobs(jobs=[job_id.value])
        jobs = response.get("jobs", [])
        if not jobs:
            logger.warning("No Batch job found with id %s", job_id.value)
            return None
        job = jobs[0]
        status = JobStatus.from_batch_state(job.get("status", ""))
        started = job.get("startedAt")
        stopped = job.get("stoppedAt")
        return JobStatusInfo(
            job_id=job_id,
            status=status,
            start_time=str(started) if started else None,
            end_time=str(stopped) if stopped else None,
            exit_code=_batch_exit_code(job),
            error_message=job.get("statusReason") if status == JobStatus.FAILED else None,
        )

    def get_batch_job_statuses(self, job_ids: list[str]) -> dict[str, JobStatus]:
        """Batched ``describe_jobs`` status lookup for arbitrary AWS Batch job
        ids, chunked by ``_DESCRIBE_JOBS_MAX_BATCH`` (100/call, the real API
        limit). An id absent from the response (not yet visible — brief
        eventual-consistency lag right after submission, or simply unknown) is
        simply absent from the returned mapping rather than raising; callers
        should treat a missing id as not-yet-terminal, the same discipline
        ``get_chain_campaign_result`` already established (and now reuses this
        exact helper for). Shared by that method and
        ``JobScheduler._advance_chain_campaign``'s per-seed poll (backlog item
        71 Phase 4), which needs the same batching for a campaign's
        ``chain_current_job_ids`` on every tick.
        """
        if not job_ids:
            return {}
        batch = self._batch()
        statuses: dict[str, JobStatus] = {}
        for i in range(0, len(job_ids), _DESCRIBE_JOBS_MAX_BATCH):
            chunk = job_ids[i : i + _DESCRIBE_JOBS_MAX_BATCH]
            response = batch.describe_jobs(jobs=chunk)
            for job in response.get("jobs", []):
                jid = job.get("jobId")
                if jid is not None:
                    statuses[str(jid)] = JobStatus.from_batch_state(str(job.get("status", "")))
        return statuses

    def get_batch_job_details(self, job_ids: list[str]) -> dict[str, BatchJobDetail]:
        """``get_batch_job_statuses`` plus what a failed job SAID: Batch's
        ``statusReason``, the container exit code and the attempt count. Used
        where a bare job id is not an answer -- a chain campaign's failed seeds
        (observability plan D4c). Same chunking, same missing-id semantics."""
        if not job_ids:
            return {}
        batch = self._batch()
        details: dict[str, BatchJobDetail] = {}
        for i in range(0, len(job_ids), _DESCRIBE_JOBS_MAX_BATCH):
            chunk = job_ids[i : i + _DESCRIBE_JOBS_MAX_BATCH]
            response = batch.describe_jobs(jobs=chunk)
            for job in response.get("jobs", []):
                jid = job.get("jobId")
                if jid is None:
                    continue
                exit_code = _batch_exit_code(job)
                details[str(jid)] = BatchJobDetail(
                    job_id=str(jid),
                    job_name=str(job.get("jobName") or ""),
                    status=JobStatus.from_batch_state(str(job.get("status", ""))),
                    status_reason=job.get("statusReason"),
                    exit_code=int(exit_code) if exit_code is not None else None,
                    attempts=len(job.get("attempts") or []),
                )
        return details

    def get_chain_campaign_result(self, job_ids: list[str]) -> ChainCampaignPollResult:
        """Poll a chain-dispatch campaign's tracked final-generation job ids —
        one per seed, each seed's own last successfully-submitted generation
        job (``HpcRun.chain_final_job_ids``) — for the analysis-fan-in
        condition (backlog item 33 rework, replacing the per-generation-array
        "wave" design's own single-array-job ``get_wave_result``).

        "Terminal" means EVERY tracked job has reached a Batch-terminal state
        (SUCCEEDED or FAILED, via the same ``JobStatus.from_batch_state``
        mapping ``get_job_status`` already uses) — a seed's chain ending in
        FAILED (that job's own retries exhausted, or an earlier generation in
        its chain having failed and auto-propagated via Batch's own dependsOn)
        is expected economics, not an orchestrator error; the caller
        (``JobScheduler._advance_chain_campaign``) decides whether the
        campaign as a whole produced anything worth analyzing. A tracked id
        that hasn't appeared in a ``describe_jobs`` response yet (brief
        eventual-consistency lag right after submission) is treated as
        not-yet-terminal, not a hard failure — this poller runs on an
        interval, it just checks again next time.

        ``describe_jobs`` accepts at most 100 job ids per call (verified
        against the real API model this session) — a campaign's up to 1000
        tracked ids are chunked accordingly, unlike the array-job design this
        superseded (which polled ONE array parent's own
        ``arrayProperties.statusSummary`` plus paginated ``list_jobs`` calls).
        """
        if not job_ids:
            # Nothing tracked at all -- every seed failed even generation 0's
            # submission. Trivially "terminal" (nothing left to wait for); the
            # caller's own zero-succeeded handling covers marking the campaign
            # FAILED without submitting an analysis over an empty sweep.
            return ChainCampaignPollResult(terminal=True)

        statuses = self.get_batch_job_statuses(job_ids)
        succeeded = [jid for jid in job_ids if statuses.get(jid) == JobStatus.COMPLETED]
        failed = [jid for jid in job_ids if statuses.get(jid) == JobStatus.FAILED]
        # A missing id (not in `statuses` at all) or one still queued/running is
        # simply neither succeeded nor failed above -- either way, not yet terminal.

        if len(succeeded) + len(failed) < len(job_ids):
            return ChainCampaignPollResult(terminal=False)
        return ChainCampaignPollResult(terminal=True, succeeded_job_ids=succeeded, failed_job_ids=failed)

    @override
    async def cancel_job(self, job_id: JobId) -> None:
        """Cancel — LOCAL task or AWS Batch terminate_job (also kills child MNP nodes)."""
        if job_id.backend == JobBackend.LOCAL:
            self._local.cancel(job_id.value)
            logger.info("Cancelled local task %s", job_id.value)
            return
        if job_id.backend == JobBackend.K8S_NEXTFLOW:
            if self._k8s is None:
                raise RuntimeError(f"cannot cancel {job_id.value}: no K8sJobService configured")
            # Deleting the Job SIGTERMs the head, which asks Nextflow to shut
            # down; its hook then terminates the tasks it submitted. That is the
            # correct mechanism -- Nextflow is the only party that knows exactly
            # which tasks belong to this run -- and the pod now gets
            # NF_HEAD_TERMINATION_GRACE_SECONDS to do it.
            #
            # But it was OBSERVED not to happen (viva-api#472): on simulation 441,
            # 8 of 10 lineage tasks outlived the cancel by ~100 minutes and filled
            # host disk until a later campaign started failing. A cancel that
            # leaves work running is worse than one that refuses, because the
            # operator believes the resources are free. So we verify rather than
            # assume, and say what was actually stopped.
            #
            # The reap that guarantees it is NOT here any more. Done inline it was
            # synchronous inside the cancel request (a pod restart mid-cancel lost
            # the intent), scanned one queue, did not paginate, and -- worst --
            # ran while the head was still in its grace period, so Nextflow saw
            # each termination as a task failure and RESUBMITTED it (measured:
            # terminating 10 tasks produced 9 fresh jobs). The handler writes
            # CANCELLED after this returns; ``JobScheduler``'s
            # ``reconcile_cancelled_nextflow_campaigns`` then reaps from that
            # row every tick, only once the head is actually gone.
            self._k8s.delete_job(job_id.value)
            logger.info("Deleted Nextflow head Job %s", job_id.value)
            return
        self._batch().terminate_job(jobId=job_id.value, reason="cancelled via sms-api")
        logger.info("Terminated Ray Batch job %s", job_id.value)

    async def reap_cancelled_campaign(self, head_job_name: str) -> int | None:
        """Terminate Batch tasks that outlived a cancelled Nextflow head.

        Returns ``None`` when the head Job still exists -- it is inside its
        termination grace period and Nextflow's own shutdown hook is terminating
        tasks. Reaping THEN is worse than waiting: Nextflow treats each external
        termination as a task failure and resubmits it (viva-api#478's finding).
        The caller retries next tick. Otherwise returns how many were terminated.

        Tasks are identified by the campaign's S3 WORK DIR, which every task
        carries in its container command -- there is no per-campaign tag, and
        Batch job names are the process names, which repeat across campaigns.
        The campaign key is recovered from the head Job name
        (``_nf_head_job_name`` builds ``nf-<sanitised run id>-<rand>``); the
        EXACT-match rule in ``_command_belongs_to_campaign`` is what keeps a
        resumed run from reaping the campaign it joined.

        Scans EVERY configured task queue and paginates ``list_jobs`` (the API
        caps a page at 100; a Run-4-scale campaign is 336 tasks), the two
        defects of the inline reap this replaces.
        """
        if self._k8s is not None and self._k8s.get_job_status(head_job_name) is not None:
            return None
        stem = head_job_name[3:] if head_job_name.startswith("nf-") else head_job_name
        stem = stem.rsplit("-", 1)[0]  # drop _rand_suffix
        settings = get_settings()
        queues = [q for q in (settings.batch_amd64_queue, settings.batch_arm64_queue) if q]
        if not stem or not queues:
            return 0
        return await asyncio.to_thread(self._terminate_campaign_tasks, queues, stem)

    def _terminate_campaign_tasks(self, queues: list[str], stem: str) -> int:
        batch = self._batch()
        terminated = 0
        for queue in queues:
            for status in ("SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING"):
                kwargs: dict[str, Any] = {"jobQueue": queue, "jobStatus": status}
                while True:
                    response = batch.list_jobs(**kwargs)
                    ids = [j["jobId"] for j in response.get("jobSummaryList", [])]
                    for chunk in (ids[i : i + 100] for i in range(0, len(ids), 100)):
                        for job in batch.describe_jobs(jobs=chunk).get("jobs", []):
                            command = " ".join(job.get("container", {}).get("command", []) or [])
                            if not _command_belongs_to_campaign(command, stem):
                                continue
                            batch.terminate_job(jobId=job["jobId"], reason=f"campaign {stem} cancelled via sms-api")
                            terminated += 1
                    next_token = response.get("nextToken")
                    if not next_token:
                        break
                    kwargs["nextToken"] = next_token
        return terminated

    async def cancel_chain_campaign(self, campaign: HpcRun) -> None:
        """Cancel every seed's current in-flight job for a chain-dispatch
        campaign (backlog item 71 Phase 4, folding in backlog item 53's
        cancellation design). Simpler than item 53's original walk-back-through-
        dependsOn proposal: under the per-seed app-level-gated model there is at
        most ONE in-flight job per seed at any time, directly readable from
        ``chain_current_job_ids`` — no dependsOn chain to walk. Reuses
        ``cancel_job``'s existing ``terminate_job`` call unchanged, which item
        53's own empirical testing already validated works correctly across
        every non-terminal Batch state (RUNNING, RUNNABLE, PENDING) — no
        state-dependent branching needed. Idempotent: a seed whose chain
        already resolved (its ``chain_current_job_ids`` entry already ``None``)
        is simply skipped.

        This only terminates the AWS-side jobs — writing the campaign's own
        CANCELLED status is the caller's responsibility (mirrors the existing
        single-job ``cancel_job``/``cancel_simulation`` split: this service
        talks to AWS, the handler owns the DB write).
        """
        for job_id in campaign.chain_current_job_ids or []:
            if job_id is None:
                continue
            await self.cancel_job(JobId.ray(job_id))

    @override
    async def close(self) -> None:
        pass
