import datetime
import enum
import hashlib
import json
import re
from dataclasses import field
from typing import Any, Literal

from pydantic import BaseModel as _BaseModel
from pydantic import ConfigDict, Field, computed_field, field_validator, model_validator

from viva_api.common.models import JobBackend, JobId, JobStatus
from viva_api.config import get_settings


class BaseModel(_BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")


def trim_attributes(instance: BaseModel, excluded: list[str] | None = None) -> None:
    if excluded is None:
        excluded = []
    for attrname in list(type(instance).model_fields.keys()):
        attr = getattr(instance, attrname)
        if attr is None and attrname not in excluded:
            delattr(instance, attrname)
        if isinstance(attr, list | dict) and not len(attr):
            delattr(instance, attrname)


class JobType(enum.Enum):
    ANALYSIS = "analysis"
    SIMULATION = "simulation"
    PARCA = "parca"
    BUILD_IMAGE = "build_image"


class HpcRun(BaseModel):
    database_id: int
    job_id: JobId = Field(exclude=True)  # Backend-tagged job identifier (not serialized directly)
    correlation_id: str  # to correlate with the WorkerEvent, if applicable ("N/A" if not applicable)
    job_type: JobType
    ref_id: int  # primary key of the object this HPC run is associated with (sim, parca, etc.)
    status: JobStatus | None = None
    start_time: str | None = None  # ISO format datetime string
    end_time: str | None = None  # ISO format datetime string or None if still running
    error_message: str | None = None  # Error message if the simulation failed
    # Chain-dispatch campaign fields (backlog item 33): None for every HpcRun that
    # isn't a chain-campaign tracker row. chain_n_generations is the campaign's
    # total generation count G; chain_final_job_ids is the AWS Batch job id of
    # each seed's own last successfully-submitted generation job -- what the
    # analysis-fan-in poller (JobScheduler.update_chain_campaigns) watches.
    chain_n_generations: int | None = None
    chain_final_job_ids: list[str] | None = None
    # Backlog item 71 Phase 4: per-seed incremental progress (see ORMHpcRun's own
    # docstring for the full rationale). None for every non-chain-campaign row.
    chain_current_job_ids: list[str | None] | None = None
    chain_current_generation: list[int | None] | None = None
    chain_parca_done: bool | None = None
    # Backlog item 88: set only by SimulationServiceRay._submit_multi_node_composite
    # (a generic multi-node process-bigraph composite dispatch, e.g. a colony
    # composite spread across N Ray-cluster nodes) to the dispatched composite's
    # id. None for every other HpcRun. Doubles as the discriminator
    # JobScheduler.update_multi_node_jobs polls for -- mutually exclusive with
    # chain_n_generations by construction (a row is written by exactly one
    # dispatch shape).
    multi_node_composite_id: str | None = None
    # viva-api#414: for a LOCAL-backend row, the AWS Batch job ids the in-process
    # task is watching (a DooD build's build job(s)). What a recovering process
    # resolves the row from once the task's owner pod is gone. None otherwise.
    external_job_ids: list[str] | None = None
    # Observability (plan D4b/D4c). All optional and None on rows written before
    # the columns existed. ``exit_code``: the head pod's / Batch container's exit
    # code; ``attempt``: how many times the work was tried (max Nextflow task
    # attempt, Batch attempts); ``error_source``: where ``error_message`` came
    # from (``viva_api.common.hpc.job_service.ERROR_SOURCE_RANK``); ``trace_id``
    # / ``campaign_span_id``: the OpenTelemetry-style identity every task's
    # events carry (derived from ``correlation_id``, see events_env);
    # ``events_s3_prefix``: where the tasks' events.jsonl objects land;
    # ``stage``/``generation``/``last_event_at``: folded from the event stream by
    # the ingester (PR-D), None until it runs.
    exit_code: int | None = None
    attempt: int | None = None
    error_source: str | None = None
    trace_id: str | None = None
    campaign_span_id: str | None = None
    events_s3_prefix: str | None = None
    stage: str | None = None
    generation: int | None = None
    last_event_at: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _reconstruct_job_id(cls, data: Any) -> Any:
        """Rebuild ``job_id`` from API response fields.

        Handles three formats:
        - Modern: ``job_id_ext`` + ``job_backend`` (current serialization)
        - Already an object with ``job_id``
        - Legacy: ``slurmjobid`` integer (older deployments, e.g. sms-api-rke
          running a pre-JobBackend release)

        The legacy path lets the current CLI/TUI/GUI remain compatible with
        an older deployment while rolling out new versions.
        """
        if isinstance(data, dict) and "job_id" not in data:
            if "job_id_ext" in data:
                data["job_id"] = JobId(value=data["job_id_ext"], backend=JobBackend(data["job_backend"]))
            elif "slurmjobid" in data:
                data["job_id"] = JobId(value=str(data["slurmjobid"]), backend=JobBackend.SLURM)
        return data

    # Computed fields for API serialization
    @computed_field  # type: ignore[prop-decorator]
    @property
    def job_id_ext(self) -> str:
        return str(self.job_id)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def job_backend(self) -> str:
        return self.job_id.backend.value


class ChainCampaignUpdate(BaseModel):
    """What one ``JobScheduler`` tick decided to persist for a chain-dispatch
    campaign (backlog item 71 Phase 4) — returned from the callback passed to
    ``DatabaseService.advance_chain_campaign`` so the whole read-decide-write
    sequence commits atomically under that campaign's advisory lock, never
    racing a concurrent tick (e.g. during a rolling restart).

    Every field is the FULL current state, not a delta — the callback always
    supplies whichever fields it didn't change this tick unchanged from what it
    read, since the underlying JSONB columns are replaced wholesale.
    ``terminal_status`` is set only on the tick that resolves the whole
    campaign (every seed's chain_current_job_ids entry is None); otherwise the
    campaign row's own status is left alone.
    """

    chain_current_job_ids: list[str | None]
    chain_current_generation: list[int | None]
    chain_parca_done: bool
    chain_final_job_ids: list[str]
    terminal_status: JobStatus | None = None
    error_message: str | None = None
    # Where ``error_message`` came from (``ERROR_SOURCE_RANK``); a lower-ranked
    # message never overwrites a higher-ranked one already on the row.
    error_source: str | None = None


class SimulationRun(BaseModel):
    id: int
    status: JobStatus
    error_message: str | None = None


class ChainProgress(BaseModel):
    """Backlog item 6: aggregate per-seed progress for a chain-dispatch campaign
    (backlog item 33) — the data ``get_simulation_status`` already computes via
    ``SimulationServiceRay.get_chain_campaign_result`` and then collapses into
    one coarse ``JobStatus``, exposed at its real granularity instead. Not a new
    data source — the SAME already-tracked ``HpcRun.chain_final_job_ids`` and
    the SAME ``describe_jobs`` polling ``JobScheduler.update_chain_campaigns``
    already runs, just returned unflattened."""

    id: int
    seeds_total: int
    seeds_succeeded: int
    seeds_failed: int
    seeds_in_progress: int
    terminal: bool
    status: JobStatus


class Simulator(BaseModel):
    git_commit_hash: str  # Git commit hash for the specific simulator version (first 7 characters)
    git_repo_url: str  # Git repository URL for the simulator
    git_branch: str  # Git branch name for the simulator version


class SimulatorVersion(Simulator):
    database_id: int  # Unique identifier for the simulator version
    created_at: datetime.datetime | None = None


class RegisteredSimulators(BaseModel):
    versions: list[SimulatorVersion]
    timestamp: datetime.datetime | None = Field(default_factory=datetime.datetime.now)


class RepoDiscovery(BaseModel):
    """Available config filenames and analysis modules discovered from a simulator's repo."""

    simulator_id: int
    git_repo_url: str
    git_commit_hash: str
    config_filenames: list[str] = Field(default_factory=list)
    analysis_modules: dict[str, list[str]] = Field(default_factory=dict)


class ParcaOptions(BaseModel):
    # extra="forbid" (was the pydantic default "ignore"): an unknown parca_options
    # key must fail loud at construction rather than being silently dropped. The
    # canonical cause was `new_genes` — declared only as a comment below, so a
    # config requesting a custom strain had its `new_genes` silently stripped and
    # ParCa built wild-type (CD2 audit §2.1 / P0-2). Every field the runtime
    # genuinely consumes is now a real field; anything else is a caller error.
    model_config = ConfigDict(extra="forbid")

    cpus: int | None = None
    outdir: str = str(get_settings().simulation_outdir)
    operons: bool = True
    ribosome_fitting: bool = True
    rnapoly_fitting: bool = True
    remove_rrna_operons: bool = False
    remove_rrff: bool = False
    stable_rrna: bool = False
    # new_genes: a custom-strain new-gene insertion subdir passed straight through
    # to v2ecoli-parca's `--new-genes` flag (default "off"). Read back on the Ray
    # backend via getattr(config.parca_options, "new_genes", ...) in
    # simulation_service_ray.py — it MUST be a declared field or it never survives
    # SimulationConfig construction.
    new_genes: str = "off"
    # bundle_overrides: one or more bundle-overrides manifest paths (backlog item
    # 104, extended item 106) passed straight through to v2ecoli-parca's own
    # `--bundle-overrides` flag (cli/parca.py, action="append" -- confirmed from
    # SourceBundle.__init__'s own docstring/type hint, Optional[Union[PathLike,
    # list]]: overrides stack IN ORDER on top of v2ecoli's defaults, they don't
    # replace them). A single string is still accepted for every existing caller
    # (byte-for-byte unchanged: one value -> one flag); a list emits one
    # `--bundle-overrides PATH` per entry, IN ORDER. Real gap this closes: sms-
    # ecoli's own declared recipe for rebuilding the J3/K4 CD2 candidate chassis
    # (workspace/studies/cd2-pnnl-01-bundle-scenarios/sims/run_scenarios.sh,
    # scenario rung5_lam075) stacks TWO overrides in the same command --
    # `--new-genes violacein_gfp --bundle-overrides .../vio-gfp/overrides.tsv
    # --bundle-overrides .../rung5-lambda-075/overrides.tsv --rnaseq-source
    # experimental` -- which a single-string field cannot express at all; a
    # remote dispatch could only ever apply one of the two layers. Read back via
    # getattr(config.parca_options, "bundle_overrides", None) in
    # simulation_service_ray.py, same as new_genes.
    bundle_overrides: str | list[str] | None = None
    # rnaseq_source: passed straight through to v2ecoli-parca's own
    # `--rnaseq-source` flag (default "reference"; "experimental" is the only
    # other value it accepts, cli/parca.py). Real gap this closes: a
    # bundle_overrides manifest can itself REQUIRE "experimental" to have any
    # effect at all (rung5-lambda-075/overrides.tsv's own header: "READ BY
    # NOTHING without that flag... the scenario silently becomes its own
    # control") -- omitting it doesn't error, it silently no-ops the override,
    # exactly the CD2 audit P0-2 failure mode new_genes/bundle_overrides were
    # declared for. Read back via getattr(config.parca_options,
    # "rnaseq_source", None) in simulation_service_ray.py, same as those two.
    rnaseq_source: str | None = None
    # require_clean_chain: opt-in per-dispatch signal (item 106/#166 chassis-
    # provenance thread, v2ecoli#735) that the staged cache's chassis provenance
    # must be verifiable and clean -- emitted verbatim as the V2E_REQUIRE_
    # CLEAN_CHAIN env var v2ecoli's own verify_cache_version reads directly.
    # Default False emits nothing: most existing callers don't pass v2ecoli's
    # own sources= yet, so requiring this unconditionally would hard-fail every
    # one of them (including Run 4's already-built new-gene caches) the moment
    # v2ecoli#735 lands. Read back via getattr(config.parca_options,
    # "require_clean_chain", False) in simulation_service_ray.py.
    require_clean_chain: bool = False
    # bundle_manifest_path (item 451/#166, Run 4 founder-chassis rebuild): passed
    # straight through to v2ecoli-parca's own `--bundle-manifest-path PATH` flag
    # -- the BASE reference-bundle manifest, distinct from `bundle_overrides`
    # above (which layers ON TOP of whatever base is in effect; this flag
    # REPLACES the base itself). Real, confirmed need: sms-ecoli's own declared
    # recipe for Run 4's violacein founder chassis
    # (scripts/build_run4_founder_caches.py's own module docstring) is
    # `--bundle-manifest-path out/combined_violacein.tsv --new-genes
    # violacein_MG1655_M5` -- a bundle-overrides-only remote dispatch cannot
    # express this at all. Mutually exclusive with build_combined_bundle_
    # manifest below -- set one or the other, not both. Default None builds
    # byte-for-byte the same command as before this field existed.
    bundle_manifest_path: str | None = None
    # build_combined_bundle_manifest / include_violacein_bundle (item 451/#166):
    # when the first is True, the remote dispatch generates the combined
    # manifest itself via sms-ecoli's own `scripts/build_combined_bundle_
    # manifest.py` (a deliberately machine-local, gitignored build artifact per
    # that script's own docstring -- regenerated fresh in THIS container ahead
    # of the real v2ecoli-parca invocation, never committed) and points
    # `--bundle-manifest-path` at its own default output
    # (`out/combined_bundle_manifest.tsv`). `include_violacein_bundle` passes
    # that generator's own `--include-violacein` flag -- required for Run 4's
    # founder chassis specifically. Default False on both is a pure no-op.
    build_combined_bundle_manifest: bool = False
    include_violacein_bundle: bool = False
    # deterministic_hash_seed (item 451/#166): emits `PYTHONHASHSEED=0` ahead of
    # the `v2ecoli-parca` invocation. Real, confirmed need: Run 4's own founder-
    # chassis recipe explicitly requires it (same module docstring as above) --
    # Python's hash randomization can otherwise perturb dict/set iteration order
    # inside ParCa's own reconstruction code, which the recipe treats as a
    # determinism requirement for a chassis meant to be deterministically
    # re-derived. Opt-in rather than unconditional: changing hash-seed behavior
    # for every existing ParCa dispatch is a bigger, untested behavioral change
    # than this fix's own scope calls for. Default False is a pure no-op.
    deterministic_hash_seed: bool = False
    debug_parca: bool = False
    load_intermediate: str | None = None
    save_intermediates: bool = False
    intermediates_directory: str = ""
    variable_elongation_transcription: bool = True
    variable_elongation_translation: bool = False
    # include_violacein_reactions: a real, legacy-config field consumed by
    # v2ecoli's own injection pipeline (library/inject.py, ~line 519) to decide
    # whether to append violacein reactions during metabolism-redux adaptation.
    # Not currently read anywhere in viva-api's own dispatch command
    # construction (neither the comparison-ensemble nor chain-dispatch paths
    # thread it through) -- v2ecoli's own inject.py auto-detects a reasonable
    # value when the field is absent (any(...) over the injected gene set), so
    # omitting it is not silently wrong, just less explicit than a config that
    # sets it directly. Declared here (the exact class of gap `new_genes`/
    # `bundle_overrides` were before they were declared, backlog items 93/104)
    # so a real, tracked config carrying this field (e.g.
    # pathway_expression_carina_final.json) doesn't fail SimulationConfig
    # validation outright before any dispatch is even attempted.
    include_violacein_reactions: bool | None = None

    def model_post_init(self, context: Any, /) -> None:
        trim_attributes(self)


class ParcaDatasetRequest(BaseModel):
    simulator_version: SimulatorVersion  # Version of the software used to generate the dataset
    parca_config: ParcaOptions = ParcaOptions()

    @property
    def config_hash(self) -> str:
        """Generate a deep hash of the parca request for caching purposes."""
        json_str = json.dumps(self.parca_config.model_dump())
        return hashlib.md5(json_str.encode()).hexdigest()  # noqa: S324 insecure hash `md5` is okay for caching


class ParcaDataset(BaseModel):
    database_id: int  # Unique identifier for the dataset
    parca_dataset_request: ParcaDatasetRequest  # Request parameters for the dataset
    remote_archive_path: str | None = None  # Path to the dataset archive in remote storage


class NewGeneCacheRequest(BaseModel):
    """Backlog item 105: stamp an induction level onto a COMPLETED ParCa
    dataset's cache (``scripts/build_new_gene_cache.py``, the "other half" of
    ``new_genes`` presence/absence -- see ``SimulationServiceRay.
    submit_new_gene_cache_job``). Ray/Batch backend only; the source dataset's
    own request must have set ``parca_options.new_genes`` (an all-zero-
    expression source has nothing to induce -- not re-validated here, same
    pure-passthrough philosophy as ``injected_processes``/``variants``).
    """

    parca_dataset_id: int
    variant: str  # non-collision label for the derived cache's S3 key -- see RayLayout.parca_cache_uri
    expression: float
    translation_efficiency: float
    rel_exp_adj: str | None = None  # comma-separated per-RNA relative weights
    rel_trl_eff_adj: str | None = None  # comma-separated per-monomer relative weights
    seed: int = 0
    media_condition: str | None = None
    fixed_media: str | None = None
    # Stage the SOURCE chassis from ``ray-parca-cache/<commit>/<source_variant>/``
    # instead of the bare commit slot (sms-ecoli#166, 2026-09-09): the bare slot is
    # shared and last-writer-wins -- a later chain dispatch's ``run_parca`` on the
    # same commit overwrote the violacein chassis seven hours after the genotype
    # inductions, and every later re-induction died with "no new-gene cistrons".
    # A chassis built into a variant slot cannot be clobbered that way. None
    # keeps today's behaviour byte-for-byte.
    source_variant: str | None = None


class NewGeneCacheJob(BaseModel):
    """Response for a submitted new-gene-cache job. No HpcRun/DB tracking yet
    (backlog item 105 v1, scoped deliberately narrow) -- poll the returned
    ``job_id`` directly against the compute backend (e.g. ``aws batch
    describe-jobs``) rather than through the usual ``/simulations/{id}/status``
    family, which this job does not register with.
    """

    job_id: str
    commit: str
    variant: str
    cache_s3_uri: str  # where the derived cache lands once the job succeeds


class VariantCacheRequest(BaseModel):
    """Backlog item 451: stamp NATIVE-gene translation-efficiency perturbations
    onto a COMPLETED ParCa dataset's cache (``scripts/build_variant_cache.py``,
    the sibling mechanism to ``NewGeneCacheRequest`` above -- native-gene
    knockouts/knockdowns/overexpression instead of a new gene's own induction
    level -- see ``SimulationServiceRay.submit_variant_cache_job``). Ray/Batch
    backend only; the source dataset must already have SUCCEEDED (not
    re-validated here, same pure-passthrough philosophy as
    ``NewGeneCacheRequest``).
    """

    parca_dataset_id: int
    variant: str  # non-collision label for the derived cache's S3 key -- see RayLayout.parca_cache_uri
    perturbations: dict[str, float]  # gene_id -> multiplier; 0 is a knockout, >1 an overexpression
    seed: int = 0
    fixed_media: str | None = None


class VariantCacheJob(BaseModel):
    """Response for a submitted variant-cache job. Same v1-scoped shape as
    ``NewGeneCacheJob`` -- no HpcRun/DB tracking, poll the returned ``job_id``
    directly against the compute backend.
    """

    job_id: str
    commit: str
    variant: str
    cache_s3_uri: str  # where the derived cache lands once the job succeeds


class WorkerEvent(BaseModel):
    database_id: int | None = None  # Unique identifier for the worker event (created by the database)
    created_at: str | None = None  # ISO format datetime string (created by the database)
    hpcrun_id: int | None = None  # ID of the HpcRun this event is associated with (known in context of database)

    correlation_id: str  # to correlate with the HpcRun job - see hpc_utils.get_correlation_id()
    sequence_number: int  # Sequence number provided by the message producer (emitter)
    mass: dict[str, float]  # mass from the simulation
    time: float  # Global time of the simulation

    @classmethod
    def from_message_payload(cls, worker_event_message_payload: "WorkerEventMessagePayload") -> "WorkerEvent":
        """Create a WorkerEvent from a WorkerEventMessagePayload."""
        return cls(
            correlation_id=worker_event_message_payload.correlation_id,
            sequence_number=worker_event_message_payload.sequence_number,
            mass=worker_event_message_payload.mass,
            time=worker_event_message_payload.time,
        )


class WorkerEventMessagePayload(BaseModel):
    correlation_id: str  # to correlate with the HpcRun job - see hpc_utils.get_correlation_id()
    sequence_number: int  # Sequence number provided by the message producer (emitter)
    time: float  # global time of the simulation
    mass: dict[str, float]  # Unique identifier for the simulation job
    bulk: list[int] | None  # Bulk data for the simulation (ignored by the database)
    bulk_index: list[str] | None = None  # Labels for the bulk data, if applicable (ignored by the database)


class AnalysisOptions(BaseModel):
    model_config = ConfigDict(extra="allow")
    cpus: int | None = None
    # single: dict[str, Any] | None = None
    # multidaughter: dict[str, Any] | None = None
    # multigeneration: dict[str, dict[str, Any]] | None = None
    # multiseed: dict[str, dict[str, Any]] | None = None
    # multivariant: dict[str, dict[str, Any]] | None = None
    # multiexperiment: dict[str, Any] | None = None

    def model_post_init(self, context: Any, /) -> None:
        trim_attributes(self)


# Ray two-engine comparison knobs — validated at the API boundary (Literal Query
# params on the run endpoint → 422 on a typo), NOT declared on SimulationConfig.
# SimulationConfig is a passthrough to the vEcoli solver's schema, and the
# established convention here is to declare only fields authoritative to us and
# leave everything the solver owns undeclared (extra="allow" carries them through
# without injecting our defaults). So these ride in as extra keys, present only
# when the caller set them, and the Ray backend reads them via getattr.
# ``CompositeEngine`` — which engine the comparison driver runs:
#   "v2ecoli" (the ported bigraph model) or "vecoli" (pristine upstream vEcoli,
#   which requires the separate upstream ParCa cache; see _is_upstream_vecoli).
CompositeEngine = Literal["v2ecoli", "vecoli"]
# ``VecoliSource`` — how the genuine vEcoli side runs (only meaningful for
# composite="vecoli"): "upstream" (default, ~50 pbg steps) or "vivarium-process"
# (vEcoli as one pbg node with vivarium-core's Engine inside).
VecoliSource = Literal["upstream", "vivarium-process"]


class SimulationConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    experiment_id: str
    parca_options: ParcaOptions = ParcaOptions()
    analysis_options: AnalysisOptions = AnalysisOptions()
    # Optional runtime env vars for the simulation container.
    # Populated from endpoint params; surfaced as V1EnvVar entries on K8s Jobs.
    ecoli_sources_uri: str | None = None
    ecoli_sources_overlays: str | None = None
    # sim_data_path: str | None = None
    # suffix_time: bool = False
    generations: int = 1
    # n_init_sims: int = 1
    # max_duration: float = 10800.0
    # initial_global_time: float = 0.0
    # time_step: float = 1.0
    # single_daughters: bool = True
    # emitter: str = "parquet"
    # emitter_arg: dict[str, Any] = Field(
    #     default_factory=lambda: {"out_dir": str(get_settings().simulation_outdir)}
    # )  # str(get_settings().hpc_sim_base_path)
    # variants: dict[str, Any] = Field(default={})
    # gcloud: str | None = None
    # agent_id: str | None = None
    # parallel: bool | None = None
    # divide: bool | None = None
    # d_period: bool | None = None
    # division_threshold: bool | None = None
    # division_variable: list[str] = Field(default=[])
    # chromosome_path: list[str] | None = None
    # spatial_environment: bool | None = None
    # fixed_media: str | None = None
    # condition: str | None = None
    # save: bool | None = None
    # save_times: list[str | float | int] = Field(default=[])
    # add_processes: list[str] = Field(default=[])
    # exclude_processes: list[str] = Field(default=[])
    # profile: bool | None = None
    # processes: list[str] = Field(default=[])
    # process_configs: dict[str, Any] = Field(default={})
    # topology: dict[str, Any] = field(default={})
    # engine_process_reports: list[list[str]] = Field(default=[])
    # emit_paths: list[str] = Field(default=[])
    # progress_bar: bool | None = None
    # emit_topology: bool | None = None
    # emit_processes: bool | None = None
    # emit_config: bool | None = None
    # emit_unique: bool | None = None
    # log_updates: bool | None = None
    # raw_output: bool | None = None
    # description: str | None = None
    # seed: int | None = None
    # mar_regulon: bool | None = None
    # amp_lysis: bool | None = None
    # initial_state_file: str | None = None
    # skip_baseline: bool | None = None
    # daughter_outdir: str | None = None
    # lineage_seed: int | None = None
    # fail_at_max_duration: bool | None = None
    # inherit_from: list[str] = Field(default=[])
    # spatial_environment_config: dict[str, Any] = Field(default={})
    # swap_processes: dict[str, Any] = Field(default={})
    # flow: dict[str, Any] = Field(default={})
    # initial_state_overrides: list[str] = Field(default=[])
    # initial_state: dict[str, Any] = Field(default={})

    @field_validator("generations", mode="before")
    @classmethod
    def default_generations(cls, v: Any) -> int:
        if v is None:
            return 1
        return int(v)

    # def model_post_init(self, *args: Any) -> None:
    #     for attrname in list(SimulationConfig.model_fields.keys()):
    #         attr = getattr(self, attrname)
    #         if (attr is None and attrname != "sim_data_path") or (attr == ["string"]):
    #             delattr(self, attrname)
    #         if isinstance(attr, list | dict) and not len(attr):
    #             delattr(self, attrname)


class ExperimentRequest(BaseModel):
    """Used by the /simulation endpoint."""

    experiment_id: str
    simulation_name: str | None = None
    metadata: dict[str, Any] = {}
    run_parca: bool = True
    generations: int = 2
    n_init_sims: int = 1
    lineage_seed: int = 3
    max_duration: float = 10800.0
    initial_global_time: float = 0.0
    time_step: float = 1.0
    single_daughters: bool = True
    variants: dict[str, dict[str, dict[str, list[float | str | int]]]] = Field(default={})
    analysis_options: dict[str, Any] = Field(default={})
    gcloud: str | None = None
    agent_id: str | None = None
    parallel: bool | None = None
    divide: bool | None = None
    d_period: bool | None = None
    division_threshold: bool | None = None
    division_variable: list[str] = Field(default=[])
    chromosome_path: list[str] | None = None
    spatial_environment: bool | None = None
    fixed_media: str | None = None
    condition: str | None = None
    add_processes: list[str] = Field(default=[])
    exclude_processes: list[str] = Field(default=[])
    profile: bool | None = None
    processes: list[str] = Field(default=[])
    process_configs: dict[str, Any] = Field(default={})
    topology: dict[str, Any] = field(default={})
    engine_process_reports: list[list[str]] = Field(default=[])
    emit_paths: list[str] = Field(default=[])
    emit_topology: bool | None = None
    emit_processes: bool | None = None
    emit_config: bool | None = None
    emit_unique: bool | None = None
    log_updates: bool | None = None
    description: str | None = None
    seed: int | None = None
    mar_regulon: bool | None = None
    amp_lysis: bool | None = None
    initial_state_file: str | None = None
    skip_baseline: bool | None = None
    fail_at_max_duration: bool | None = None
    inherit_from: list[str] = Field(default=[])
    spatial_environment_config: dict[str, Any] = Field(default={})
    swap_processes: dict[str, Any] = Field(default={})
    flow: dict[str, Any] = Field(default={})
    initial_state_overrides: list[str] = Field(default=[])
    initial_state: dict[str, Any] = Field(default={})

    def model_post_init(self, context: Any, /) -> None:
        if self.simulation_name is None:
            self.simulation_name = self.experiment_id

    def to_config(self) -> SimulationConfig:
        attributes = self.model_json_schema()["properties"]
        excluded = ["simdata_id", "metadata"]
        config_kwargs = {}
        for attribute in attributes:
            if attribute not in excluded:
                attr_val = getattr(self, attribute)
                if attr_val != "string":
                    config_kwargs[attribute] = attr_val

        # config_kwargs = {attribute: getattr(self, attribute) for attribute in attributes if attribute not in excluded}

        return SimulationConfig(**config_kwargs)


class SimulationConfigFilename(enum.StrEnum):
    DEFAULT = "api_simulation_default.json"
    CCAM = "api_simulation_default_ccam.json"
    AWS_CDK = "api_simulation_default_aws_cdk.json"
    PTOOLS_CCAM = "api_simulation_ptools_ccam.json"


class SimulationRequest(BaseModel):
    """Used by the /simulation endpoint."""

    config: SimulationConfig
    simulation_config_filename: str
    experiment_id: str
    simulator: Simulator | None = None
    simulator_id: int | None = None
    parca_dataset_id: int | None = None
    tags: list[str] = Field(default_factory=list)

    def model_post_init(self, context: Any, /) -> None:
        if self.simulator is None and self.simulator_id is None:
            raise ValueError(
                "You must specify either a Simulator (hash, branch, url) OR the db id of an already-inserted simulation"
            )


class Simulation(BaseModel):
    """Used by the /simulation endpoint"""

    database_id: int
    simulator_id: int
    parca_dataset_id: int
    config: SimulationConfig
    simulation_config_filename: str
    experiment_id: str
    last_updated: str = Field(default=str(datetime.datetime.now()))
    job_id: str | None = None  # Backend-specific job ID (str(slurm_int) or k8s_job_name)
    num_seeds: int | None = None  # Number of lineage seeds (derived from config.n_init_sims)
    tags: list[str] = Field(default_factory=list)  # Free-form filter tags (e.g. "cd1")

    def model_post_init(self, context: Any, /) -> None:
        # Surface num_seeds from the config JSONB (stored as n_init_sims by vEcoli)
        if self.num_seeds is None:
            n_init = getattr(self.config, "n_init_sims", None)
            if n_init is not None:
                self.num_seeds = int(n_init)


class ObservableInfoModel(BaseModel):
    name: str
    dims: list[str]
    shape: list[int]


class SimulationObservableIndex(BaseModel):
    simulation_id: int
    experiment_id: str
    seed: int
    store: Literal["zarr", "parquet"]
    observables: list[ObservableInfoModel]


class SimulationObservables(BaseModel):
    simulation_id: int
    experiment_id: str
    seed: int
    store: Literal["zarr", "parquet"]
    time: list[float]
    series: dict[str, list[float | None]]


class TaskRunRequest(BaseModel):
    """Request to run a self-contained script on the in-region task compute
    (viva-api#631). Slice 1: ``script`` is a path to a script already in the
    image (e.g. an fss-combine / ptools-regather turnkey script that lives in the
    repo). ``memory_class`` routes the instance via the same mechanism as
    analyses (viva-api#629)."""

    script: str  # repo-path to a script in the image (slice 1)
    args: list[str] = Field(default_factory=list)
    sim_data_refs: dict[str, Any] | None = None
    memory_class: str = "standard"
    commit: str | None = None  # image commit to run in; None -> latest/default
    name: str | None = None  # optional human label; defaults to the script name

    @field_validator("name")
    @classmethod
    def _validate_name(cls, v: str | None) -> str | None:
        # The name becomes part of the Batch jobName ([A-Za-z0-9_-], <=128), so a
        # bad one would fail submit_job with a 500. Reject it up front (422)
        # instead. A None name is fine -- the service derives a sanitized one.
        if v is not None and not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", v):
            raise ValueError("name must be 1-128 characters of [A-Za-z0-9_-]")
        return v


class TaskLogsDTO(BaseModel):
    """A task run's CloudWatch logs (viva-api#631 slice 3). ``lines`` is empty
    until the container has started (no log stream yet) or when no log group can
    be resolved; ``status`` lets the caller decide whether to keep polling."""

    task_id: int
    job_id_ext: str | None = None
    status: JobStatus | None = None
    log_stream: str | None = None
    lines: list[str] = Field(default_factory=list)
    report_uri: str | None = None  # s3:// report.json the entrypoint ships, if configured


class TaskDTO(BaseModel):
    """A task run's tracked state (submit response and status share this shape)."""

    database_id: int
    name: str
    script: str
    args: list[str] = Field(default_factory=list)
    sim_data_refs: dict[str, Any] | None = None
    memory_class: str | None = None
    status: JobStatus | None = None
    job_id_ext: str | None = None
    out_uri: str | None = None
    result_uri: str | None = None
    error_message: str | None = None
