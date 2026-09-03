import datetime
import enum
import hashlib
import json
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
    # cpus: int | None = None
    outdir: str = str(get_settings().simulation_outdir)
    # operons: bool = True
    # ribosome_fitting: bool = True
    # remove_rrna_operons: bool = False
    # remove_rrff: bool = False
    # stable_rrna: bool = False
    # new_genes: str = "off"
    # debug_parca: bool = False
    # load_intermediate: str | None = None
    # save_intermediates: bool = False
    # intermediates_directory: str = ""
    # variable_elongation_transcription: bool = True
    # variable_elongation_translation: bool = False

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
