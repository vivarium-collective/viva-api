"""The multi-node composite dispatch mechanism: one generic process-bigraph composite on Ray
actors across the nodes of ONE AWS Batch multi-node-parallel job (a colony, a lineage batch), behind
a ParCa MNP job unless a staged variant cache is named -- and the analysis job that follows it.

The third dispatch mechanism to become a strategy object (``docs/plan-core.md`` P2.1, PR 9), in the
shape ``MbpTrackedStrategy`` set. This IS a Ray mechanism: the job is a Ray head plus workers, and
``RAY_SHARDS_DEFAULT`` sizes the actor pool process-bigraph builds on them.

* ``MultiNodeCompositeStrategy(batch, stage_runner=)``. ``batch`` is a ``MultiNodeBatch``: an
  ``MnpSubmitter`` for the run and its ParCa, a ``ContainerSubmitter`` for the analysis that
  follows, and the boto3 client for ONE read -- the vCPUs the job definition declares, which is
  what ``RAY_SHARDS_DEFAULT`` is computed from (viva-api#730).
* ``stage_runner`` is handed in, as for Nextflow: staging the generic runner is the service's.
* ``submit_analysis`` travels with the mechanism, as the audit decided -- it is this mechanism's
  analysis submitter, not "the analysis service's". The scheduler reaches it through a one-line
  delegate on the service (``submit_multi_node_analysis``) until P6.

Three of the six methods never touched ``self`` and are functions here; the other three are the
strategy's, verbatim but for how they spell what used to be on ``self``
(proven by ``scripts/prove_ray_carve_is_move_only.py``, kept in the tree up to ``v0.9.151``).

SMS code, and it stays SMS code.
"""

import json
import logging
import math
import shlex
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

from botocore.exceptions import ClientError, ParamValidationError

from viva_api.common.dispatch_validation import resolve_task_env
from viva_api.common.models import JobId
from viva_api.common.storage import data_layout
from viva_api.common.storage.file_paths import S3FilePath
from viva_api.simulation.database_service import DatabaseService
from viva_api.simulation.models import JobType, Simulation
from viva_api.simulation.ray import _seams, parca_spec
from viva_api.simulation.ray.analysis_spec import analysis_memory_class, analysis_modules_for
from viva_api.simulation.ray.batch_layer import ContainerSubmitter, MnpSubmitter, _rand_suffix
from viva_api.simulation.ray.config_interpretation import _thread_injected_processes_into_params
from viva_api.simulation.ray.image_paths import ANALYSIS_OUT_DIR, PARCA_CACHE_DIR, SIM_OUT_DIR, V2ECOLI_DIR
from viva_api.simulation.ray.runner_env import PBG_RUNNER_ENV
from viva_api.simulation.tables_orm import AnalysisStatusDB
from viva_core.events.events_env import with_events_env

if TYPE_CHECKING:
    # ``types-boto3`` is a dev dependency (annotations only): never imported at runtime.
    from types_boto3_batch import BatchClient
    from types_boto3_batch.type_defs import JobDefinitionTypeDef

logger = logging.getLogger(__name__)


class MultiNodeBatch(MnpSubmitter, ContainerSubmitter, Protocol):
    """What this mechanism asks of the Batch layer: an MNP job for the run (and its ParCa), a
    container job for its analysis, and one read of a job definition."""

    def client(self) -> "BatchClient": ...


def multi_node_composite_command(
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


def stage_seed_override_caches(
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
    dest_bucket = _seams.get_settings().s3_work_bucket
    dest_prefix = data_layout.key_from_uri(cache_s3).rstrip("/")
    s3_client = _seams.boto3.client("s3", region_name=_seams.get_settings().storage_s3_region)
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


def multi_node_analysis_command(
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


class MultiNodeCompositeStrategy:
    # A freshly-registered job definition (this method is always called right after
    # ``ensure_mnp_job_def`` registers one) may not be visible to ``describe_job_definitions``
    # yet. A few short retries cover that window without slowing down the common case (an
    # already-registered definition resolves on the first attempt).
    #
    # History, because this comment used to say something untrue (viva-api#730): it cited an
    # eventual-consistency incident "confirmed live 2026-08-25". What was actually happening was
    # that the lookup passed a ``revision`` keyword the API does not have, botocore refused
    # every call client-side, and the blanket ``except`` below retried the refusal as if it
    # were weather. The lookup never once succeeded until 2026-09-20. The retry is kept because
    # the window is plausible; it has not been observed.
    _VCPU_LOOKUP_RETRIES = 3
    _VCPU_LOOKUP_BACKOFF_SECONDS = 1.0

    def __init__(self, batch: MultiNodeBatch, *, stage_runner: Callable[[str], Awaitable[str]]) -> None:
        self._batch = batch
        self._stage_runner = stage_runner

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

        ``job_definition`` is ``<name>:<revision>``, which is one of the forms
        ``jobDefinitions`` accepts. Two kinds of failure, told apart on purpose:

        * the SERVICE said no, or said nothing yet (``ClientError``, an empty result) -- possibly
          transient, so retried, quietly;
        * the CALL is wrong (``ParamValidationError``: botocore refused it before sending) -- a
          programming error. Not retried, logged at ERROR with the traceback. Still ``None``:
          a sizing nicety must not fail a dispatch. It was this kind, retried as the first
          kind, that kept #730 invisible for a month.
        """
        batch = self._batch.client()
        for attempt in range(self._VCPU_LOOKUP_RETRIES):
            defs: list[JobDefinitionTypeDef] = []
            try:
                described = batch.describe_job_definitions(jobDefinitions=[job_definition])
                defs = described.get("jobDefinitions", [])
            except ParamValidationError:
                logger.exception(
                    "describe_job_definitions refused for %s: a bug in the call, not in AWS; "
                    "RAY_SHARDS_DEFAULT left unset",
                    job_definition,
                )
                return None
            except ClientError as exc:
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

    async def submit(
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
            settings=_seams.get_settings(),
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

        settings = _seams.get_settings()
        commit = simulator.environment_key
        experiment_id = ecoli_simulation.config.experiment_id

        job_def = self._batch.ensure_mnp_job_def(self._batch.image_uri(commit), commit)
        n_shards_default = self._mnp_node_vcpus(job_def)
        if n_shards_default:
            n_shards_default *= num_nodes

        cache_s3 = parca_spec.cache_s3_uri(commit, variant=cache_variant)
        runner_s3_uri = await self._stage_runner(experiment_id)

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
            parca_job_id = self._batch.submit_mnp(
                job_name=f"ray-parca-{commit}-{_rand_suffix()}",
                job_definition=job_def,
                num_nodes=1,
                ray_job_cmd=parca_spec.parca_command(),
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
            params["seed_overrides"] = stage_seed_override_caches(
                seed_overrides=seed_overrides,
                cache_s3=cache_s3,
                stage_dir=PARCA_CACHE_DIR,
            )

        composite_job_id = self._batch.submit_mnp(
            job_name=f"ray-mnp-composite-{experiment_id}-{_rand_suffix()}"[:128],
            job_definition=job_def,
            num_nodes=num_nodes,
            ray_job_cmd=multi_node_composite_command(
                composite_id=composite_id,
                params=params,
                steps=steps,
                runner_s3_uri=runner_s3_uri,
                n_shards_default=n_shards_default,
                experiment_id=str(experiment_id),
            ),
            out_s3=data_layout.RayLayout.results_uri(experiment_id),
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
            # The ParCa job this composite waits on is this run's too (viva-api#709).
            external_job_ids=[parca_job_id] if parca_job_id else None,
        )
        return job_id

    async def submit_analysis(
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
        sim_data_uri = f"{parca_spec.cache_s3_uri(commit, variant=cache_variant)}simData.cPickle"
        analysis_name = f"analysis-mnp-{experiment_id[:20]}-{_rand_suffix()}"
        results_uri = data_layout.RayLayout.results_uri(experiment_id).rstrip("/")
        result_uri = f"{results_uri}/analyses/{analysis_name}"
        settings = _seams.get_settings()
        container_job_def = self._batch.ensure_container_job_def(self._batch.image_uri(commit), commit)
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
            analysis_job_id = self._batch.submit_container(
                job_name=f"ray-mnp-analysis-{experiment_id}-{_rand_suffix()}"[:128],
                job_definition=container_job_def,
                job_cmd=multi_node_analysis_command(
                    experiment_id=experiment_id,
                    composite_id=composite_id,
                    history_uri=results_uri,
                    out_uri=result_uri,
                    n_seeds=n_seeds,
                    n_generations=n_generations,
                    modules=modules,
                    sim_data_uri=sim_data_uri,
                ),
                out_s3=data_layout.RayLayout.results_uri(experiment_id),
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
