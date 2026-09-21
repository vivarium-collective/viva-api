"""The mbp-tracked dispatch mechanism: one container job running the image's own
``scripts/run_mbp_tracked.py`` for a variant, behind a ParCa container job unless a staged
variant cache is named.

The FIRST of the five dispatch mechanisms to become a strategy object (``docs/plan-core.md`` P2.1,
PR 7 of the 2026-09-20 sequence), and so the one that sets the shape:

* ``MbpTrackedStrategy(batch)`` is HANDED what it submits through -- a ``ContainerSubmitter`` --
  and nothing else. It does not know the simulation service exists. Today that object is the
  service's ``RayBatchLayer``; the point is that the strategy never finds out.
* ``submit`` is the body of ``SimulationServiceRay._submit_mbp_tracked_dispatch``, verbatim but for
  how four things are spelled now that there is no ``self`` to find them on (the cache URI and
  the command are functions, the Batch layer is ``self._batch``, the run record is a function).
  ``scripts/prove_ray_carve_is_move_only.py`` checks exactly that, and nothing else differs.
* ``mbp_tracked_command`` was a method that never touched ``self``: a function, as in
  ``parca_spec``.

What a strategy does NOT own yet, on purpose: this mechanism's share of progress polling
(``job_scheduler.py``) and of cancel routing. Those join it in P6, when the scheduler is split.
Nor is there a ``DispatchStrategy`` Protocol yet: the five mechanisms take five different
things, and an interface is better read off five real strategies than guessed from one. The
router in ``submit_ecoli_simulation_job`` still decides precedence and calls ``submit``.

SMS code, and it stays SMS code.
"""

import shlex
from pathlib import Path
from typing import TypedDict

from viva_api.common.dispatch_validation import resolve_task_env
from viva_api.common.models import JobId
from viva_api.common.storage import data_layout
from viva_api.common.storage.file_paths import S3FilePath
from viva_api.simulation.database_service import DatabaseService
from viva_api.simulation.models import Simulation
from viva_api.simulation.ray import _seams, parca_spec
from viva_api.simulation.ray.batch_layer import ContainerSubmitter, _rand_suffix
from viva_api.simulation.ray.image_paths import PARCA_CACHE_DIR, SIM_OUT_DIR, V2ECOLI_DIR
from viva_api.simulation.ray.run_records import record_run_with_companions
from viva_core.events.events_env import with_events_env


def mbp_tracked_command(
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


class MbpDispatch(TypedDict, total=False):
    """The ``mbp_dispatch`` block of a simulation config: what a client may say about an
    mbp-tracked run. Every key is optional to the type; ``submit`` refuses a block without
    ``variant``.

    This DECLARES the contract; nothing enforces it yet. The block arrives as JSON through the
    config's passthrough fields and only ``task_env`` is validated at the API boundary, so a
    wrongly-typed value reaches the command line as it always has.
    """

    variant: str
    cache_variant: str | None
    task_env: dict[str, str]
    max_generations: int | None
    duration_sec: int | None
    chunk: int | None
    emitter: str | None
    single_daughters: bool
    carbon_exhaustion_arrest: bool
    seed: int | None
    cells_per_agent: float | None
    initial_glucose_mM: float | None
    initial_ammonium_mM: float | None
    injected_processes: str | None
    reactor_config: str | None
    aeration_schedule: str | None
    aeration_trigger: str | None


class MbpTrackedStrategy:
    def __init__(self, batch: ContainerSubmitter) -> None:
        # Held, not copied: looked up on ``batch`` when a job is submitted, so a test that swaps
        # ``service.batch.submit_container`` is what this mechanism gets.
        self._batch = batch

    async def submit(
        self,
        ecoli_simulation: Simulation,
        database_service: DatabaseService,
        mbp_dispatch: MbpDispatch,
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

        settings = _seams.get_settings()
        commit = simulator.environment_key
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
        cache_s3 = parca_spec.cache_s3_uri(commit, variant=cache_variant)

        job_def = self._batch.ensure_container_job_def(self._batch.image_uri(commit), commit)

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
            parca_job_id = self._batch.submit_container(
                job_name=f"mbp-parca-{commit}-{_rand_suffix()}",
                job_definition=job_def,
                job_cmd=parca_spec.parca_command(),
                out_s3=cache_s3,
                out_dir=PARCA_CACHE_DIR,
                tags={**base_tags, "Phase": "parca"},
                task_env=task_env,
            )

        command = mbp_tracked_command(
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
        job_id = self._batch.submit_container(
            job_name=f"mbp-tracked-{experiment_id}-{_rand_suffix()}"[:128],
            job_definition=job_def,
            job_cmd=command,
            out_s3=data_layout.RayLayout.results_uri(experiment_id),
            out_dir=SIM_OUT_DIR,
            stage_s3=cache_s3,
            stage_dir=PARCA_CACHE_DIR,
            depends_on=[parca_job_id] if parca_job_id else None,
            tags={**base_tags, "Phase": "mbp_tracked"},
            task_env=task_env,
        )
        tracked_job_id = JobId.ray(job_id)
        await record_run_with_companions(
            database_service,
            job_id=tracked_job_id,
            simulation_id=ecoli_simulation.database_id,
            correlation_id=correlation_id,
            companion_job_ids=[parca_job_id] if parca_job_id else [],
        )
        return tracked_job_id
