import datetime
import enum
import logging
from typing import TYPE_CHECKING, Any

from sqlalchemy import ForeignKey, Index, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncAttrs, AsyncEngine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from viva_api.analysis.models import AnalysisConfig, AnalysisConfigOptions, ExperimentAnalysisDTO
from viva_api.common.models import JobBackend, JobId, JobStatus
from viva_api.simulation.models import (
    HpcRun,
    JobType,
    SimulatorVersion,
    WorkerEvent,
)

if TYPE_CHECKING:
    # Deferred: TaskDTO is imported at runtime inside ORMTask.to_dto() below to
    # avoid a circular import (viva_api.simulation.models does not import
    # tables_orm, but importing TaskDTO at module scope here would still force
    # models.py to fully resolve before this module does). This TYPE_CHECKING-only
    # import just gives the string return annotation a real name to resolve.
    from viva_api.simulation.models import TaskDTO

logger = logging.getLogger(__name__)


class JobStatusDB(enum.Enum):
    WAITING = "waiting"
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    FAILED = "failed"

    def to_job_status(self) -> JobStatus:
        return JobStatus(self.value)

    @classmethod
    def from_job_status(cls, status: JobStatus) -> "JobStatusDB":
        """Convert JobStatus to JobStatusDB, mapping UNKNOWN to PENDING."""
        if status == JobStatus.UNKNOWN:
            # UNKNOWN maps to PENDING as a safe default for unexpected states
            return cls.PENDING
        return cls(status.value)


class AnalysisStatusDB(enum.Enum):
    """Coarse readiness of an analysis result set (see analysis-results-design.md)."""

    COMPUTING = "computing"
    READY = "ready"
    FAILED = "failed"

    def to_job_status(self) -> JobStatus:
        return {
            AnalysisStatusDB.COMPUTING: JobStatus.RUNNING,
            AnalysisStatusDB.READY: JobStatus.COMPLETED,
            AnalysisStatusDB.FAILED: JobStatus.FAILED,
        }[self]

    @classmethod
    def from_job_status(cls, status: JobStatus) -> "AnalysisStatusDB":
        if status == JobStatus.COMPLETED:
            return cls.READY
        if status in (JobStatus.FAILED, JobStatus.CANCELLED):
            return cls.FAILED
        return cls.COMPUTING


class TaskStatusDB(enum.Enum):
    """Coarse readiness of an in-region task run (viva-api#631). Mirrors
    AnalysisStatusDB — a task is a self-contained script submitted to the
    in-region task compute, so it maps onto the same job-status vocabulary."""

    COMPUTING = "computing"
    READY = "ready"
    FAILED = "failed"

    def to_job_status(self) -> JobStatus:
        return {
            TaskStatusDB.COMPUTING: JobStatus.RUNNING,
            TaskStatusDB.READY: JobStatus.COMPLETED,
            TaskStatusDB.FAILED: JobStatus.FAILED,
        }[self]

    @classmethod
    def from_job_status(cls, status: JobStatus) -> "TaskStatusDB":
        if status == JobStatus.COMPLETED:
            return cls.READY
        if status in (JobStatus.FAILED, JobStatus.CANCELLED):
            return cls.FAILED
        return cls.COMPUTING


class JobTypeDB(enum.Enum):
    SIMULATION = "simulation"
    PARCA = "parca"
    BUILD_IMAGE = "build_image"

    def to_job_type(self) -> JobType:
        return JobType(self.value)

    @classmethod
    def from_job_type(cls, job_type: JobType) -> "JobTypeDB":
        return JobTypeDB(job_type.value)


class Base(AsyncAttrs, DeclarativeBase):
    pass


class ORMSimulator(Base):
    __tablename__ = "simulator"

    id: Mapped[int] = mapped_column(primary_key=True)
    created_at: Mapped[datetime.datetime] = mapped_column(server_default=func.now())
    git_repo_url: Mapped[str] = mapped_column(nullable=False)
    git_branch: Mapped[str] = mapped_column(nullable=False)
    git_commit_hash: Mapped[str] = mapped_column(nullable=False)  # first 7 characters of the commit hash

    def to_simulator_version(self) -> SimulatorVersion:
        return SimulatorVersion(
            database_id=self.id,
            created_at=self.created_at,
            git_repo_url=self.git_repo_url,
            git_branch=self.git_branch,
            git_commit_hash=self.git_commit_hash,
        )


class ORMHpcRun(Base):
    __tablename__ = "hpcrun"

    id: Mapped[int] = mapped_column(primary_key=True)
    created_at: Mapped[datetime.datetime] = mapped_column(server_default=func.now())

    job_type: Mapped[JobTypeDB] = mapped_column(nullable=False)
    correlation_id: Mapped[str] = mapped_column(nullable=False, index=True)
    job_id_ext: Mapped[str | None] = mapped_column(nullable=True)  # Backend-specific job ID as string
    job_backend: Mapped[str] = mapped_column(nullable=False, server_default="slurm")
    start_time: Mapped[datetime.datetime | None] = mapped_column(nullable=True)
    end_time: Mapped[datetime.datetime | None] = mapped_column(nullable=True)
    status: Mapped[JobStatusDB] = mapped_column(nullable=False)
    error_message: Mapped[str | None] = mapped_column(nullable=True)
    jobref_simulation_id: Mapped[int | None] = mapped_column(ForeignKey("simulation.id"), nullable=True, index=True)
    jobref_parca_dataset_id: Mapped[int | None] = mapped_column(
        ForeignKey("parca_dataset.id"), nullable=True, index=True
    )
    jobref_simulator_id: Mapped[int | None] = mapped_column(ForeignKey("simulator.id"), nullable=True, index=True)
    # Chain-dispatch campaign fields (backlog item 33: per-generation task
    # decomposition via individual per-seed AWS Batch job chains, each generation
    # its own job chained natively via dependsOn). NULL for every non-campaign
    # HpcRun (SLURM, K8s, MNP, single-shot Array) -- additive and backward-
    # compatible, no new table. Unlike the per-generation-array design this
    # superseded (one HpcRun row per generation), ONE row now tracks the WHOLE
    # campaign: AWS Batch's own dependsOn resolves each seed's chain natively, so
    # the only thing left to poll for is "has every seed's chain reached a
    # terminal state" (JobScheduler.update_chain_campaigns). ``chain_n_generations``
    # is the campaign's total generation count G (also the "is this row a
    # chain-campaign tracker" discriminator, via IS NOT NULL); ``chain_final_job_ids``
    # is the AWS Batch job id of each seed's own LAST successfully-submitted
    # generation job (normally generation G-1, but truncated early for a seed
    # whose chain hit a submission failure partway through) -- the exact set the
    # analysis-fan-in poller watches for all-terminal.
    chain_n_generations: Mapped[int | None] = mapped_column(nullable=True)
    chain_final_job_ids: Mapped[list[str] | None] = mapped_column(JSONB, nullable=True)
    # Backlog item 71 Phase 4: per-seed incremental progress, replacing the
    # upfront-dependsOn design's "everything already submitted, just poll for
    # terminal" model with app-level gating (JobScheduler submits ONE generation
    # per seed at a time). chain_current_job_ids[seed] is that seed's current
    # in-flight job id, or None once its chain has resolved (success or permanent
    # failure) -- what the poller reads each tick and what a campaign cancel
    # walks. chain_current_generation[seed] is the generation index that job
    # represents (real per-seed progress visibility). chain_parca_done gates
    # generation-0 submission for every seed at once (ParCa is campaign-wide,
    # not per-seed). All three NULL for every non-chain-campaign HpcRun.
    chain_current_job_ids: Mapped[list[str | None] | None] = mapped_column(JSONB, nullable=True)
    chain_current_generation: Mapped[list[int | None] | None] = mapped_column(JSONB, nullable=True)
    chain_parca_done: Mapped[bool | None] = mapped_column(nullable=True)
    # Backlog item 88: discriminator + payload for a generic multi-node
    # process-bigraph composite dispatch (e.g. a colony composite spread
    # across N Ray-cluster nodes). NULL for every non-multi-node-composite
    # HpcRun (SLURM, K8s, chain-dispatch, the older comparison-ensemble/
    # phase0 MNP paths). Plays the exact same "IS NOT NULL discriminates
    # this row's dispatch shape" role chain_n_generations already plays for
    # chain-dispatch campaigns -- the two are mutually exclusive by
    # construction (a row is written by exactly one dispatch shape), so
    # JobScheduler.update_multi_node_jobs's own query can never overlap with
    # list_active_chain_campaigns's.
    multi_node_composite_id: Mapped[str | None] = mapped_column(nullable=True)
    # viva-api#414: for a LOCAL-backend row (an in-process task watching work
    # it submitted elsewhere -- today the DooD image builds), the AWS Batch job
    # ids that work actually runs as. The LOCAL uuid in job_id_ext dies with the
    # pod that minted it; these ids are what a different process can re-derive
    # the row's true terminal state from (JobScheduler.reconcile_local_tasks).
    # NULL for every non-LOCAL row and for a LOCAL row whose task has not
    # submitted anything yet.
    external_job_ids: Mapped[list[str] | None] = mapped_column(JSONB, nullable=True)
    # Observability plan (D4b/D4c). Every column nullable and NULL on rows that
    # predate it -- additive, no rewrite of existing rows. ``trace_id`` and
    # ``campaign_span_id`` are DERIVED from ``correlation_id`` at insert
    # (``viva_api.common.events_env``), so the dispatcher can hand a task its
    # identity before this row exists and the ingester can recompute them for
    # older rows. ``events_cursor`` is ``{s3 key: bytes already ingested}``.
    exit_code: Mapped[int | None] = mapped_column(nullable=True)
    attempt: Mapped[int | None] = mapped_column(nullable=True)
    error_source: Mapped[str | None] = mapped_column(nullable=True)
    trace_id: Mapped[str | None] = mapped_column(nullable=True, index=True)
    campaign_span_id: Mapped[str | None] = mapped_column(nullable=True)
    events_s3_prefix: Mapped[str | None] = mapped_column(nullable=True)
    events_cursor: Mapped[dict[str, int] | None] = mapped_column(JSONB, nullable=True)
    stage: Mapped[str | None] = mapped_column(nullable=True)
    generation: Mapped[int | None] = mapped_column(nullable=True)
    last_event_at: Mapped[datetime.datetime | None] = mapped_column(nullable=True)

    def _build_job_id(self) -> JobId:
        """Construct a JobId from the ORM columns."""
        if self.job_id_ext is None:
            raise RuntimeError(f"ORMHpcRun {self.id} has no job_id_ext set")
        return JobId(value=self.job_id_ext, backend=JobBackend(self.job_backend))

    def to_hpc_run(self) -> HpcRun:
        ref_id = self.jobref_simulation_id or self.jobref_parca_dataset_id or self.jobref_simulator_id
        if ref_id is None:
            raise RuntimeError("ORMHpcRun must have at least one job reference set.")
        return HpcRun(
            database_id=self.id,
            job_id=self._build_job_id(),
            correlation_id=self.correlation_id,
            job_type=self.job_type.to_job_type(),
            ref_id=ref_id,
            status=self.status.to_job_status(),
            error_message=self.error_message,
            start_time=str(self.start_time) if self.start_time else None,
            end_time=str(self.end_time) if self.end_time else None,
            chain_n_generations=self.chain_n_generations,
            chain_final_job_ids=list(self.chain_final_job_ids) if self.chain_final_job_ids is not None else None,
            chain_current_job_ids=list(self.chain_current_job_ids) if self.chain_current_job_ids is not None else None,
            chain_current_generation=list(self.chain_current_generation)
            if self.chain_current_generation is not None
            else None,
            chain_parca_done=self.chain_parca_done,
            multi_node_composite_id=self.multi_node_composite_id,
            external_job_ids=list(self.external_job_ids) if self.external_job_ids is not None else None,
            exit_code=self.exit_code,
            attempt=self.attempt,
            error_source=self.error_source,
            trace_id=self.trace_id,
            campaign_span_id=self.campaign_span_id,
            events_s3_prefix=self.events_s3_prefix,
            stage=self.stage,
            generation=self.generation,
            last_event_at=str(self.last_event_at) if self.last_event_at else None,
        )


class ORMHpcRunEvent(Base):
    """One structured event from a run's task stream (observability plan D1/D4b).

    Written by the ingester (PR-D) from the tasks' ``events.jsonl`` objects and
    by the dispatcher/scheduler directly for lifecycle events (``submitted``,
    ``head_exit``, ``task_outcome``, ...). ``tick`` heartbeats are never stored.
    ``(trace_id, source, seq)`` is unique so re-ingesting an object is idempotent.
    """

    __tablename__ = "hpcrun_event"

    id: Mapped[int] = mapped_column(primary_key=True)
    hpcrun_id: Mapped[int] = mapped_column(ForeignKey("hpcrun.id"), nullable=False, index=True)
    trace_id: Mapped[str] = mapped_column(nullable=False, index=True)
    source: Mapped[str] = mapped_column(nullable=False)  # writer: batch job id / host-pid / "api"
    seq: Mapped[int] = mapped_column(nullable=False)
    ts: Mapped[datetime.datetime] = mapped_column(nullable=False)
    layer: Mapped[str] = mapped_column(nullable=False)  # engine | runner | dispatcher | api
    event: Mapped[str] = mapped_column(nullable=False)
    level: Mapped[str] = mapped_column(nullable=False, server_default="info")
    generation: Mapped[int | None] = mapped_column(nullable=True)
    global_time: Mapped[float | None] = mapped_column(nullable=True)
    wall_time: Mapped[float | None] = mapped_column(nullable=True)
    span_id: Mapped[str | None] = mapped_column(nullable=True)
    parent_span_id: Mapped[str | None] = mapped_column(nullable=True)
    payload: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    tags: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)

    __table_args__ = (
        UniqueConstraint("trace_id", "source", "seq", name="uq_hpcrun_event_trace_source_seq"),
        Index("ix_hpcrun_event_trace_span", "trace_id", "span_id"),
    )


class ORMHpcRunSpan(Base):
    """A span of a run's trace tree (campaign > parca / lineage / analysis >
    generation > ...), materialised from ``span_start``/``span_end`` events. An
    open span (``end_ts`` NULL) after the row goes terminal is closed as
    ``status='unknown'`` by the ingester (PR-D).
    """

    __tablename__ = "hpcrun_span"

    id: Mapped[int] = mapped_column(primary_key=True)
    hpcrun_id: Mapped[int] = mapped_column(ForeignKey("hpcrun.id"), nullable=False, index=True)
    trace_id: Mapped[str] = mapped_column(nullable=False, index=True)
    span_id: Mapped[str] = mapped_column(nullable=False)
    parent_span_id: Mapped[str | None] = mapped_column(nullable=True)
    name: Mapped[str] = mapped_column(nullable=False)
    attrs: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    start_ts: Mapped[datetime.datetime | None] = mapped_column(nullable=True)
    end_ts: Mapped[datetime.datetime | None] = mapped_column(nullable=True)
    status: Mapped[str | None] = mapped_column(nullable=True)  # ok | error | unknown
    error: Mapped[str | None] = mapped_column(nullable=True)

    __table_args__ = (UniqueConstraint("trace_id", "span_id", name="uq_hpcrun_span_trace_span"),)


class ORMParcaDataset(Base):
    __tablename__ = "parca_dataset"

    id: Mapped[int] = mapped_column(primary_key=True)
    created_at: Mapped[datetime.datetime] = mapped_column(server_default=func.now())

    simulator_id: Mapped[int] = mapped_column(ForeignKey("simulator.id"), nullable=False, index=True)
    parca_config: Mapped[dict[str, int | float | str | bool | None]] = mapped_column(JSONB, nullable=False)
    parca_config_hash: Mapped[str] = mapped_column(nullable=False)
    remote_archive_path: Mapped[str | None] = mapped_column(nullable=True)


class ORMSimulation(Base):
    __tablename__ = "simulation"

    id: Mapped[int] = mapped_column(primary_key=True)
    created_at: Mapped[datetime.datetime] = mapped_column(server_default=func.now())
    config_filename: Mapped[str] = mapped_column(nullable=False, index=True)
    experiment_id: Mapped[str] = mapped_column(nullable=False, unique=True)
    simulator_id: Mapped[int] = mapped_column(ForeignKey("simulator.id"), nullable=False, index=True)
    parca_dataset_id: Mapped[int] = mapped_column(ForeignKey("parca_dataset.id"), nullable=False, index=True)
    config: Mapped[dict[str, list[str] | bool | int | str | float | dict[str, int | float | str]]] = mapped_column(
        JSONB, nullable=False
    )
    # Free-form tags for filtering/bundling (e.g. "cd1"). Stored as data on the
    # row (not a hard-coded registry) so tags are site-local — each site's RDS is
    # independent while S3 is shared. GIN-indexed for JSONB containment queries.
    tags: Mapped[list[str]] = mapped_column(JSONB, nullable=False, server_default="[]")

    __table_args__ = (Index("ix_simulation_tags", "tags", postgresql_using="gin"),)


class ORMWorkerEvent(Base):
    __tablename__ = "worker_event"

    id: Mapped[int] = mapped_column(primary_key=True)
    created_at: Mapped[datetime.datetime] = mapped_column(server_default=func.now())

    correlation_id: Mapped[str] = mapped_column(nullable=False, index=True)
    sequence_number: Mapped[int] = mapped_column(nullable=False, index=True)
    mass: Mapped[dict[str, float]] = mapped_column(JSONB, nullable=False)
    bulk: Mapped[list[int] | None] = mapped_column(JSONB, nullable=True)
    bulk_index: Mapped[list[str] | None] = mapped_column(JSONB, nullable=True)
    time: Mapped[float] = mapped_column(nullable=True)
    hpcrun_id: Mapped[int] = mapped_column(ForeignKey("hpcrun.id"), nullable=False, index=True)

    @classmethod
    def from_worker_event(cls, worker_event: "WorkerEvent", hpcrun_id: int) -> "ORMWorkerEvent":
        return cls(
            # database_id=self.id,                 # populated in the database
            # created_at=str(self.created_at),     # populated in the database
            hpcrun_id=hpcrun_id,
            correlation_id=worker_event.correlation_id,
            sequence_number=worker_event.sequence_number,
            mass=worker_event.mass,
            bulk=None,
            bulk_index=None,
            time=worker_event.time,
        )

    def to_worker_event(self) -> WorkerEvent:
        return WorkerEvent(
            database_id=self.id,
            created_at=str(self.created_at),
            hpcrun_id=self.hpcrun_id,
            correlation_id=self.correlation_id,
            sequence_number=self.sequence_number,
            mass=self.mass,
            time=self.time,
        )

    @staticmethod
    def from_query_results(record: tuple[dict[str, float], int, int, float, int]) -> WorkerEvent:
        mass_data, sequence_number, record_id, event_time, hpcrun_id = record

        # ORMWorkerEvent.mass, ORMWorkerEvent.sequence_number, ORMWorkerEvent.id, ORMWorkerEvent.time
        return WorkerEvent(
            database_id=record_id,
            correlation_id="",
            sequence_number=sequence_number,
            mass=mass_data,
            time=event_time,
            hpcrun_id=hpcrun_id,
        )


class ORMAnalysis(Base):
    """General record of an analysis (any type). ``config`` (JSONB) is the
    authoritative store of the full analysis config; the columns below are
    denormalized/indexed only for the attributes we query on (see
    analysis-results-design.md). Legacy SLURM rows leave the new columns NULL."""

    __tablename__ = "analysis"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(nullable=False)  # this should be request.analysis_name
    config: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    last_updated: Mapped[str] = mapped_column(nullable=False)
    job_name: Mapped[str | None] = mapped_column(nullable=True)
    job_id: Mapped[int | None] = mapped_column(nullable=True)

    # --- query columns (nullable; additive; backfilled for old rows) ---
    experiment_id: Mapped[str | None] = mapped_column(nullable=True, index=True)
    n_tp: Mapped[int | None] = mapped_column(nullable=True, index=True)
    status: Mapped[AnalysisStatusDB | None] = mapped_column(nullable=True)
    result_uri: Mapped[str | None] = mapped_column(nullable=True)
    backend: Mapped[str | None] = mapped_column(nullable=True, server_default="batch")
    simulation_id: Mapped[int | None] = mapped_column(ForeignKey("simulation.id"), nullable=True, index=True)
    job_id_ext: Mapped[str | None] = mapped_column(nullable=True)  # K8s job name / batch id
    error_message: Mapped[str | None] = mapped_column(nullable=True)
    created_at: Mapped[datetime.datetime | None] = mapped_column(nullable=True, server_default=func.now())
    updated_at: Mapped[datetime.datetime | None] = mapped_column(
        nullable=True, server_default=func.now(), onupdate=func.now()
    )

    def to_dto(self) -> ExperimentAnalysisDTO:
        options = AnalysisConfigOptions(**self.config["analysis_options"])
        # emitter_arg = self.config["emitter_arg"]
        config_dto = AnalysisConfig(
            analysis_options=options,
            # emitter_arg=emitter_arg
        )
        return ExperimentAnalysisDTO(
            database_id=self.id,
            name=self.name,
            config=config_dto,
            last_updated=self.last_updated,
            job_name=self.job_name,
            job_id=self.job_id,
            experiment_id=self.experiment_id,
            n_tp=self.n_tp,
            status=self.status.to_job_status() if self.status is not None else None,
            result_uri=self.result_uri,
            simulation_id=self.simulation_id,
            backend=self.backend,
            error_message=self.error_message,
            job_id_ext=self.job_id_ext,
        )


class ORMTask(Base):
    """An in-region task run (viva-api#631): a self-contained script executed on
    the in-region task compute (instance IAM role — no SSO, no WAN signing skew),
    submitted through the same container/task queue as analyses, with memory-class
    routing (viva-api#629). ``args``/``sim_data_refs`` are the request payload;
    the columns are what the status endpoint queries on. Distinct from ``analysis``
    because a task is an arbitrary script, not a named analysis over a sweep."""

    __tablename__ = "task"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(nullable=False)
    script: Mapped[str] = mapped_column(nullable=False)  # repo-path (slice 1)
    args: Mapped[list[Any]] = mapped_column(JSONB, nullable=False, server_default="[]")
    sim_data_refs: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    memory_class: Mapped[str | None] = mapped_column(nullable=True, server_default="standard")
    status: Mapped[TaskStatusDB | None] = mapped_column(nullable=True)
    job_name: Mapped[str | None] = mapped_column(nullable=True)
    job_id_ext: Mapped[str | None] = mapped_column(nullable=True, index=True)  # batch job id
    out_uri: Mapped[str | None] = mapped_column(nullable=True)
    result_uri: Mapped[str | None] = mapped_column(nullable=True)
    error_message: Mapped[str | None] = mapped_column(nullable=True)
    created_at: Mapped[datetime.datetime | None] = mapped_column(nullable=True, server_default=func.now())
    updated_at: Mapped[datetime.datetime | None] = mapped_column(
        nullable=True, server_default=func.now(), onupdate=func.now()
    )

    def to_dto(self) -> "TaskDTO":
        from viva_api.simulation.models import TaskDTO

        return TaskDTO(
            database_id=self.id,
            name=self.name,
            script=self.script,
            args=list(self.args or []),
            sim_data_refs=self.sim_data_refs,
            memory_class=self.memory_class,
            status=self.status.to_job_status() if self.status is not None else None,
            job_id_ext=self.job_id_ext,
            out_uri=self.out_uri,
            result_uri=self.result_uri,
            error_message=self.error_message,
        )


async def create_db(async_engine: AsyncEngine) -> None:
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
