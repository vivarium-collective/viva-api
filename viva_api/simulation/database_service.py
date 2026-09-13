import contextlib
import copy
import datetime
import logging
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from typing import Any, cast, override

from pydantic import ValidationError
from sqlalchemy import ColumnElement, CursorResult, Result, and_, or_, select, text
from sqlalchemy import update as sa_update
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import InstrumentedAttribute

from viva_api.analysis.models import AnalysisConfig, ExperimentAnalysisDTO
from viva_api.common.events_env import campaign_span_id, trace_id_from_correlation
from viva_api.common.hpc.job_service import JobStatusUpdate, error_source_rank
from viva_api.common.models import JobBackend, JobId, JobStatus
from viva_api.simulation.models import (
    ChainCampaignUpdate,
    HpcRun,
    JobType,
    ParcaDataset,
    ParcaDatasetRequest,
    ParcaOptions,
    Simulation,
    SimulationConfig,
    SimulationRequest,
    SimulatorVersion,
    TaskDTO,
    WorkerEvent,
)
from viva_api.simulation.tables_orm import (
    AnalysisStatusDB,
    JobStatusDB,
    JobTypeDB,
    ORMAnalysis,
    ORMHpcRun,
    ORMParcaDataset,
    ORMSimulation,
    ORMSimulator,
    ORMTask,
    ORMWorkerEvent,
    TaskStatusDB,
)

logger = logging.getLogger(__name__)


def parca_options_from_stored(stored: Any) -> ParcaOptions:
    """Parse a STORED ``parca_config`` row tolerantly.

    ``ParcaOptions`` is deliberately ``extra="forbid"`` so an unknown key on a NEW
    request fails loudly. But rows written by another build of this service (a
    branch that declared, say, ``rnaseq_manifest_path``) sit in the same table,
    and re-parsing them strictly made ``GET /simulation/parca/versions`` 500 for
    everyone (measured on smsvpctest 2026-09-09: three ``rnaseq_*`` keys on one
    row). Same fix shape as #504 for the simulation list: strip ONLY the keys
    the validation error names as ``extra_forbidden``, re-parse strictly, and
    say so in the log. Anything else still raises.
    """
    data = dict(stored or {})
    try:
        return ParcaOptions(**data)
    except ValidationError as exc:
        offending = [err["loc"][0] for err in exc.errors() if err["type"] == "extra_forbidden" and err["loc"]]
        if not offending:
            raise
        logger.warning(
            "parca_config row carries keys this build does not declare; ignoring for read: %s",
            sorted(str(k) for k in offending),
        )
        for k in offending:
            data.pop(k, None)
        return ParcaOptions(**data)


class DatabaseService(ABC):
    @abstractmethod
    async def insert_analysis(
        self, name: str, config: AnalysisConfig, last_updated: str, job_name: str, job_id: int
    ) -> ExperimentAnalysisDTO:
        """Used by the /ecoli router"""
        pass

    @abstractmethod
    async def get_analysis(self, database_id: int) -> ExperimentAnalysisDTO:
        """Used by the /ecoli router"""
        pass

    @abstractmethod
    async def list_analyses(
        self, *, experiment_id: str | None = None, simulation_id: int | None = None
    ) -> list[ExperimentAnalysisDTO]:
        """List analyses, optionally filtered by experiment_id and/or simulation_id."""
        pass

    @abstractmethod
    async def record_analysis(
        self,
        *,
        experiment_id: str,
        n_tp: int | None,
        status: AnalysisStatusDB,
        config: dict[str, Any],
        name: str,
        simulation_id: int | None = None,
        backend: str = "batch",
        job_name: str | None = None,
        job_id_ext: str | None = None,
        result_uri: str | None = None,
        error_message: str | None = None,
    ) -> ExperimentAnalysisDTO:
        """Insert an analysis-result row (dedup-updating an existing ``(experiment_id, n_tp)`` when n_tp is set)."""
        pass

    @abstractmethod
    async def get_analysis_by_experiment_ntp(self, experiment_id: str, n_tp: int) -> ExperimentAnalysisDTO | None:
        """Return the most recent analysis row for ``(experiment_id, n_tp)``, or None."""
        pass

    @abstractmethod
    async def update_analysis_status(
        self,
        analysis_id: int,
        status: AnalysisStatusDB,
        result_uri: str | None = None,
        error_message: str | None = None,
    ) -> ExperimentAnalysisDTO:
        """Update an analysis row's status (and optionally result_uri/error) by id."""
        pass

    ####################################

    @abstractmethod
    async def record_task(
        self,
        *,
        name: str,
        script: str,
        args: list[Any],
        sim_data_refs: dict[str, Any] | None,
        memory_class: str | None,
        status: TaskStatusDB,
        job_name: str | None = None,
        job_id_ext: str | None = None,
        out_uri: str | None = None,
        result_uri: str | None = None,
        error_message: str | None = None,
    ) -> TaskDTO:
        """Insert an in-region task-run row (viva-api#631). Plain insert, no
        dedup — a task submission is a one-shot script invocation, not the
        (experiment_id, n_tp)-keyed idempotent upsert ``record_analysis`` does."""
        pass

    @abstractmethod
    async def get_task(self, database_id: int) -> TaskDTO:
        """Used by the /tasks router."""
        pass

    @abstractmethod
    async def update_task_status(
        self,
        task_id: int,
        status: TaskStatusDB,
        result_uri: str | None = None,
        error_message: str | None = None,
    ) -> TaskDTO:
        """Update a task row's status (and optionally result_uri/error) by id."""
        pass

    ####################################

    @abstractmethod
    async def insert_worker_event(self, worker_event: WorkerEvent, hpcrun_id: int) -> WorkerEvent:
        pass

    @abstractmethod
    async def list_worker_events(self, hpcrun_id: int, prev_sequence_number: int | None = None) -> list[WorkerEvent]:
        pass

    @abstractmethod
    async def insert_simulator(self, git_commit_hash: str, git_repo_url: str, git_branch: str) -> SimulatorVersion:
        pass

    @abstractmethod
    async def get_simulator(self, simulator_id: int) -> SimulatorVersion | None:
        pass

    @abstractmethod
    async def get_simulator_by_commit(self, commit_hash: str) -> SimulatorVersion | None:
        pass

    @abstractmethod
    async def delete_simulator(self, simulator_id: int) -> None:
        pass

    @abstractmethod
    async def list_simulators(self) -> list[SimulatorVersion]:
        pass

    @abstractmethod
    async def insert_hpcrun(
        self,
        job_id: JobId,
        job_type: JobType,
        ref_id: int,
        correlation_id: str,
        chain_n_generations: int | None = None,
        chain_final_job_ids: list[str] | None = None,
        chain_current_job_ids: list[str | None] | None = None,
        chain_current_generation: list[int | None] | None = None,
        chain_parca_done: bool | None = None,
        multi_node_composite_id: str | None = None,
    ) -> HpcRun:
        """
        :param job_id: Backend-tagged job identifier.
        :param job_type: (`JobType`) job type to be run. Choose one of the following:
            `JobType.SIMULATION`(/vecoli/run), `JobType.PARCA`(/vecoli/parca), `JobType.BUILD_IMAGE`(/simulator/new)
        :param ref_id: primary key of the object this HPC run is associated with (sim, parca, etc.).
        :param chain_n_generations: total generation count G for a chain-dispatch campaign (backlog item
            33). Omit for every non-campaign HpcRun.
        :param chain_final_job_ids: the AWS Batch job id of each seed's own last successfully-submitted
            generation job, for a chain-dispatch campaign. Omit for every non-campaign HpcRun.
        :param chain_current_job_ids: per-seed current in-flight job id, or None once that seed's chain
            has resolved (backlog item 71 Phase 4). Omit for every non-campaign HpcRun.
        :param chain_current_generation: per-seed generation index of the tracked current job.
        :param chain_parca_done: whether ParCa has completed, gating generation-0 submission.
        :param multi_node_composite_id: the dispatched composite's id, for a generic multi-node
            process-bigraph composite dispatch (backlog item 88, e.g. a colony composite spread
            across N Ray-cluster nodes). Omit for every other HpcRun.
        """
        pass

    @abstractmethod
    async def get_hpcrun_by_ref(self, ref_id: int, job_type: JobType) -> HpcRun | None:
        pass

    @abstractmethod
    async def get_hpcrun_by_job_id(self, job_id: JobId) -> HpcRun | None:
        pass

    @abstractmethod
    async def get_hpcrun(self, hpcrun_id: int) -> HpcRun | None:
        pass

    @abstractmethod
    async def get_hpcrun_id_by_correlation_id(self, correlation_id: str) -> int | None:
        pass

    @abstractmethod
    async def delete_hpcrun(self, hpcrun_id: int) -> None:
        pass

    @abstractmethod
    async def insert_parca_dataset(self, parca_dataset_request: ParcaDatasetRequest) -> ParcaDataset:
        pass

    @abstractmethod
    async def get_parca_dataset(self, parca_dataset_id: int) -> ParcaDataset | None:
        pass

    @abstractmethod
    async def delete_parca_dataset(self, parca_dataset_id: int) -> None:
        pass

    @abstractmethod
    async def list_parca_datasets(self) -> list[ParcaDataset]:
        pass

    @abstractmethod
    async def insert_simulation(self, sim_request: SimulationRequest) -> Simulation:
        pass

    @abstractmethod
    async def get_simulation(self, simulation_id: int) -> Simulation | None:
        pass

    @abstractmethod
    async def get_simulation_by_experiment_id(self, experiment_id: str) -> Simulation | None:
        """Look up a simulation by its experiment_id (stored in config JSON)."""
        pass

    @abstractmethod
    async def delete_simulation(self, simulation_id: int) -> None:
        pass

    @abstractmethod
    async def list_simulations(self) -> list[Simulation]:
        pass

    @abstractmethod
    async def list_simulations_filtered(
        self, experiment_ids: list[str] | None = None, tags: list[str] | None = None
    ) -> list[Simulation]:
        """Return simulations whose experiment_id is in ``experiment_ids`` OR that carry any of ``tags`` (union)."""
        pass

    @abstractmethod
    async def add_tags(self, simulation_id: int, tags: list[str]) -> Simulation:
        """Union-merge ``tags`` into a simulation's tag list and return the updated simulation."""
        pass

    @abstractmethod
    async def list_distinct_tags(self) -> dict[str, list[str]]:
        """Return each tag present in the database mapped to the experiment IDs that carry it."""
        pass

    @abstractmethod
    async def list_active_hpcruns(self) -> list[HpcRun]:
        """Return all HpcRun jobs with status PENDING or RUNNING."""
        pass

    @abstractmethod
    async def list_active_chain_campaigns(self) -> list[HpcRun]:
        """Return active (PENDING/RUNNING) HpcRun rows tracking a chain-dispatch
        campaign (``chain_n_generations is not None``) -- backlog item 33. The set
        ``JobScheduler.update_chain_campaigns`` polls each interval."""
        pass

    @abstractmethod
    async def list_recently_cancelled_nextflow_hpcruns(self, since: datetime.datetime) -> list[HpcRun]:
        """Return CANCELLED HpcRun rows for Nextflow heads (``job_backend ==
        k8s_nextflow``) whose ``end_time`` is at or after ``since``.

        The set ``JobScheduler.reconcile_cancelled_nextflow_campaigns`` re-checks
        each tick for Batch tasks that outlived their head (viva-api#472). Bounded
        by ``since`` so the scan cost does not grow with the table; a row with no
        ``end_time`` was cancelled before the handler stamped one and is out of
        scope by construction.
        """
        pass

    @abstractmethod
    async def list_active_multi_node_composites(self) -> list[HpcRun]:
        """Return active (PENDING/RUNNING) HpcRun rows tracking a generic
        multi-node process-bigraph composite dispatch
        (``multi_node_composite_id is not None``) -- backlog item 88. The set
        ``JobScheduler.update_multi_node_jobs`` polls each interval.
        Structurally disjoint from ``list_active_chain_campaigns``'s own
        result set: a row is written by exactly one dispatch shape, never
        both."""
        pass

    @abstractmethod
    async def list_active_nextflow_hpcruns(self) -> list[HpcRun]:
        """Return active (PENDING/RUNNING) HpcRun rows whose job is a Nextflow
        HEAD running as a K8s Job (``job_backend == k8s_nextflow``) -- the set
        ``JobScheduler.update_nextflow_heads`` polls each tick (observability
        plan D4c). Before that poller existed a Nextflow run's status changed
        only when a user called ``GET /status``. Structurally disjoint from the
        chain-campaign and multi-node result sets: those rows are RAY-backed."""
        pass

    @abstractmethod
    async def finalize_nextflow_head(
        self,
        hpcrun_id: int,
        status: JobStatus,
        *,
        error_message: str | None = None,
        error_source: str | None = None,
        exit_code: int | None = None,
        attempt: int | None = None,
    ) -> bool:
        """Atomically transition one Nextflow-head HpcRun from PENDING/RUNNING to
        a terminal ``status`` (COMPLETED / FAILED), returning ``True``
        only if THIS call performed the transition -- the same single-row
        conditional UPDATE ``finalize_multi_node_job`` uses, for the same reason
        (two overlapping polling ticks must not both act)."""
        pass

    @abstractmethod
    async def update_hpcrun_status(self, hpcrun_id: int, update: JobStatusUpdate) -> None:
        """Update the status of a given HpcRun job.

        ``error_message`` is written by PRECEDENCE, not last-writer-wins: an
        update whose ``error_source`` ranks below the row's current one leaves
        the message alone (``viva_api.common.hpc.job_service.ERROR_SOURCE_RANK``),
        so a task's traceback is never replaced by Kubernetes' generic backoff
        text on a later poll. A CANCELLED update with no message clears it.
        """
        pass

    @abstractmethod
    async def list_active_local_hpcruns(self) -> list[HpcRun]:
        """Return active (PENDING/RUNNING) HpcRun rows whose job is a LOCAL
        in-process task (``job_backend == "local"``) -- the set
        ``JobScheduler.reconcile_local_tasks`` checks for ownership on every
        tick (viva-api#414). A row here that no live process owns is an orphan."""
        pass

    @abstractmethod
    async def set_hpcrun_external_job_ids(self, hpcrun_id: int, job_ids: list[str]) -> None:
        """Record the external (AWS Batch) job ids a LOCAL task is watching on
        its HpcRun row (viva-api#414, "persist the external handle"). Replaces
        any previous value."""
        pass

    @abstractmethod
    async def advance_chain_campaign(
        self, hpcrun_id: int, updater: Callable[[HpcRun], Awaitable[ChainCampaignUpdate | None]]
    ) -> None:
        """Run one JobScheduler tick's read-decide-write against a single
        chain-dispatch campaign (backlog item 71 Phase 4) atomically: acquire a
        Postgres advisory lock scoped to this campaign
        (``pg_advisory_xact_lock``, keyed on ``hpcrun_id``, released
        automatically when the transaction ends), re-read the campaign row
        FRESH within that lock (never the possibly-stale copy a caller read
        before acquiring it), call ``updater`` with that fresh row, and persist
        whatever ``ChainCampaignUpdate`` it returns (or write nothing if it
        returns ``None``) — all before the lock releases.

        This is what makes concurrent ticks against the same campaign safe
        (e.g. two overlapping polling intervals during a rolling restart briefly
        running two pods): a second tick blocks on the lock until the first's
        full read-decide-write has committed, so it can only ever act on
        genuinely current state — the explicit regression property this method
        exists to guarantee (no double-submitting the same generation).

        ``updater`` may perform real AWS Batch calls (submitting the next
        generation, checking job status) while the lock is held; those calls
        are what "decide" needs to do and are expected to be fast (a handful of
        API calls, not the whole campaign).
        """
        pass

    @abstractmethod
    async def finalize_multi_node_job(
        self, hpcrun_id: int, status: JobStatus, error_message: str | None = None
    ) -> bool:
        """Atomically transition one multi-node-composite HpcRun (backlog item
        88) from PENDING/RUNNING to a terminal ``status``, returning ``True``
        only if THIS call performed the transition.

        A single-row conditional UPDATE (``WHERE status IN (PENDING,
        RUNNING)``) is sufficient here — unlike ``advance_chain_campaign``'s
        Postgres advisory lock, no separate lock is needed because there is
        only one field to transition, not a multi-field per-seed
        read-decide-write. Two overlapping polling ticks (e.g. during a
        rolling restart) racing to finalize the SAME row can only ever have
        one UPDATE affect a row; the other sees zero rows affected and returns
        ``False``. Callers MUST only submit the follow-on analysis job when
        this returns ``True`` — the concrete guarantee against double-firing
        the analysis for a single completed job.
        """
        pass

    @abstractmethod
    async def close(self) -> None:
        pass


class DatabaseServiceSQL(DatabaseService):
    async_sessionmaker: async_sessionmaker[AsyncSession]

    def __init__(self, async_engine: AsyncEngine):
        self.async_sessionmaker = async_sessionmaker(async_engine, expire_on_commit=True)

    async def _get_orm_simulator(self, session: AsyncSession, simulator_id: int) -> ORMSimulator | None:
        stmt1 = select(ORMSimulator).where(ORMSimulator.id == simulator_id).limit(1)
        result1: Result[tuple[ORMSimulator]] = await session.execute(stmt1)
        orm_simulator: ORMSimulator | None = result1.scalars().one_or_none()
        return orm_simulator

    async def _get_orm_simulation(self, session: AsyncSession, simulation_id: int) -> ORMSimulation | None:
        stmt1 = select(ORMSimulation).where(ORMSimulation.id == simulation_id).limit(1)
        result1: Result[tuple[ORMSimulation]] = await session.execute(stmt1)
        orm_simulation: ORMSimulation | None = result1.scalars().one_or_none()
        return orm_simulation

    async def _get_orm_parca_dataset(self, session: AsyncSession, parca_dataset_id: int) -> ORMParcaDataset | None:
        stmt1 = select(ORMParcaDataset).where(ORMParcaDataset.id == parca_dataset_id).limit(1)
        result1: Result[tuple[ORMParcaDataset]] = await session.execute(stmt1)
        orm_parca_dataset: ORMParcaDataset | None = result1.scalars().one_or_none()
        return orm_parca_dataset

    async def _get_orm_hpcrun(self, session: AsyncSession, hpcrun_id: int) -> ORMHpcRun | None:
        stmt1 = select(ORMHpcRun).where(ORMHpcRun.id == hpcrun_id).limit(1)
        result1: Result[tuple[ORMHpcRun]] = await session.execute(stmt1)
        orm_hpc_job: ORMHpcRun | None = result1.scalars().one_or_none()
        return orm_hpc_job

    async def _get_orm_hpcrun_by_job_id(self, session: AsyncSession, job_id: JobId) -> ORMHpcRun | None:
        stmt1 = (
            select(ORMHpcRun)
            .where(ORMHpcRun.job_id_ext == str(job_id), ORMHpcRun.job_backend == job_id.backend.value)
            .limit(1)
        )
        result1: Result[tuple[ORMHpcRun]] = await session.execute(stmt1)
        orm_hpc_job: ORMHpcRun | None = result1.scalars().one_or_none()
        return orm_hpc_job

    def _get_job_type_ref(self, job_type: JobType) -> InstrumentedAttribute[int | None] | None:
        match job_type:
            case JobType.BUILD_IMAGE:
                return ORMHpcRun.jobref_simulator_id
            case JobType.PARCA:
                return ORMHpcRun.jobref_parca_dataset_id
            case JobType.SIMULATION:
                return ORMHpcRun.jobref_simulation_id
        return None

    async def _get_orm_hpcrun_by_ref(self, session: AsyncSession, ref_id: int, job_type: JobType) -> ORMHpcRun | None:
        reference = self._get_job_type_ref(job_type)
        stmt1 = select(ORMHpcRun).where(reference == ref_id).order_by(ORMHpcRun.id.desc()).limit(1)  # type: ignore[arg-type]
        result1: Result[tuple[ORMHpcRun]] = await session.execute(stmt1)
        orm_hpc_job: ORMHpcRun | None = result1.scalars().one_or_none()

        return orm_hpc_job

    async def _get_orm_analysis(self, session: AsyncSession, database_id: int) -> ORMAnalysis | None:
        """Used by the /ecoli router"""
        stmt1 = select(ORMAnalysis).where(ORMAnalysis.id == database_id).limit(1)
        result1: Result[tuple[ORMAnalysis]] = await session.execute(stmt1)
        orm_experiment: ORMAnalysis | None = result1.scalars().one_or_none()
        return orm_experiment

    @override
    async def insert_analysis(
        self, name: str, config: AnalysisConfig, last_updated: str, job_name: str, job_id: int
    ) -> ExperimentAnalysisDTO:
        """Used by the /ecoli router"""
        async with self.async_sessionmaker() as session, session.begin():
            # config.emitter_arg["out_dir"] = str(get_settings().simulation_outdir)
            orm_analysis = ORMAnalysis(
                name=name, config=config.model_dump(), last_updated=last_updated, job_name=job_name, job_id=job_id
            )
            session.add(orm_analysis)
            await session.flush()
            return orm_analysis.to_dto()

    @override
    async def get_analysis(self, database_id: int) -> ExperimentAnalysisDTO:
        """Used by the /ecoli router"""
        async with self.async_sessionmaker() as session, session.begin():
            orm_analysis = await self._get_orm_analysis(session, database_id=database_id)
            if orm_analysis is None:
                raise RuntimeError(f"Experiment {database_id} not found")
            return orm_analysis.to_dto()

    @override
    async def list_analyses(
        self, *, experiment_id: str | None = None, simulation_id: int | None = None
    ) -> list[ExperimentAnalysisDTO]:
        async with self.async_sessionmaker() as session:
            clauses: list[ColumnElement[bool]] = []
            if experiment_id is not None:
                clauses.append(ORMAnalysis.experiment_id == experiment_id)
            if simulation_id is not None:
                clauses.append(ORMAnalysis.simulation_id == simulation_id)
            stmt = select(ORMAnalysis)
            if clauses:
                stmt = stmt.where(and_(*clauses))
            stmt = stmt.order_by(ORMAnalysis.id)
            result: Result[tuple[ORMAnalysis]] = await session.execute(stmt)
            return [orm_analysis.to_dto() for orm_analysis in result.scalars().all()]

    @override
    async def record_analysis(
        self,
        *,
        experiment_id: str,
        n_tp: int | None,
        status: AnalysisStatusDB,
        config: dict[str, Any],
        name: str,
        simulation_id: int | None = None,
        backend: str = "batch",
        job_name: str | None = None,
        job_id_ext: str | None = None,
        result_uri: str | None = None,
        error_message: str | None = None,
    ) -> ExperimentAnalysisDTO:
        async with self.async_sessionmaker() as session, session.begin():
            # Idempotency: update the existing row for (experiment_id, n_tp) if present.
            # Only dedup when n_tp is set (NULL n_tp — e.g. backfilled demo data — is
            # kept unique by the caller).
            existing = None
            if n_tp is not None:
                stmt = (
                    select(ORMAnalysis)
                    .where(ORMAnalysis.experiment_id == experiment_id, ORMAnalysis.n_tp == n_tp)
                    .order_by(ORMAnalysis.id.desc())
                    .limit(1)
                )
                existing = (await session.execute(stmt)).scalars().first()
            if existing is not None:
                existing.status = status
                existing.config = config
                existing.name = name
                existing.simulation_id = simulation_id
                existing.backend = backend
                existing.job_name = job_name
                existing.job_id_ext = job_id_ext
                existing.result_uri = result_uri
                existing.error_message = error_message
                existing.last_updated = str(datetime.datetime.now())
                await session.flush()
                return existing.to_dto()
            orm_analysis = ORMAnalysis(
                name=name,
                config=config,
                last_updated=str(datetime.datetime.now()),
                experiment_id=experiment_id,
                n_tp=n_tp,
                status=status,
                simulation_id=simulation_id,
                backend=backend,
                job_name=job_name,
                job_id_ext=job_id_ext,
                result_uri=result_uri,
                error_message=error_message,
            )
            session.add(orm_analysis)
            await session.flush()
            return orm_analysis.to_dto()

    @override
    async def get_analysis_by_experiment_ntp(self, experiment_id: str, n_tp: int) -> ExperimentAnalysisDTO | None:
        async with self.async_sessionmaker() as session:
            stmt = (
                select(ORMAnalysis)
                .where(ORMAnalysis.experiment_id == experiment_id, ORMAnalysis.n_tp == n_tp)
                .order_by(ORMAnalysis.id.desc())
                .limit(1)
            )
            orm_analysis = (await session.execute(stmt)).scalars().first()
            return orm_analysis.to_dto() if orm_analysis is not None else None

    @override
    async def update_analysis_status(
        self,
        analysis_id: int,
        status: AnalysisStatusDB,
        result_uri: str | None = None,
        error_message: str | None = None,
    ) -> ExperimentAnalysisDTO:
        async with self.async_sessionmaker() as session, session.begin():
            orm_analysis = await self._get_orm_analysis(session, database_id=analysis_id)
            if orm_analysis is None:
                raise RuntimeError(f"Analysis {analysis_id} not found")
            orm_analysis.status = status
            if result_uri is not None:
                orm_analysis.result_uri = result_uri
            if error_message is not None:
                orm_analysis.error_message = error_message
            orm_analysis.last_updated = str(datetime.datetime.now())
            await session.flush()
            return orm_analysis.to_dto()

    ##################################

    async def _get_orm_task(self, session: AsyncSession, database_id: int) -> ORMTask | None:
        stmt1 = select(ORMTask).where(ORMTask.id == database_id).limit(1)
        result1: Result[tuple[ORMTask]] = await session.execute(stmt1)
        orm_task: ORMTask | None = result1.scalars().one_or_none()
        return orm_task

    @override
    async def record_task(
        self,
        *,
        name: str,
        script: str,
        args: list[Any],
        sim_data_refs: dict[str, Any] | None,
        memory_class: str | None,
        status: TaskStatusDB,
        job_name: str | None = None,
        job_id_ext: str | None = None,
        out_uri: str | None = None,
        result_uri: str | None = None,
        error_message: str | None = None,
    ) -> TaskDTO:
        async with self.async_sessionmaker() as session, session.begin():
            orm_task = ORMTask(
                name=name,
                script=script,
                args=list(args),
                sim_data_refs=sim_data_refs,
                memory_class=memory_class,
                status=status,
                job_name=job_name,
                job_id_ext=job_id_ext,
                out_uri=out_uri,
                result_uri=result_uri,
                error_message=error_message,
            )
            session.add(orm_task)
            await session.flush()
            return orm_task.to_dto()

    @override
    async def get_task(self, database_id: int) -> TaskDTO:
        async with self.async_sessionmaker() as session, session.begin():
            orm_task = await self._get_orm_task(session, database_id=database_id)
            if orm_task is None:
                raise RuntimeError(f"Task {database_id} not found")
            return orm_task.to_dto()

    @override
    async def update_task_status(
        self,
        task_id: int,
        status: TaskStatusDB,
        result_uri: str | None = None,
        error_message: str | None = None,
    ) -> TaskDTO:
        async with self.async_sessionmaker() as session, session.begin():
            orm_task = await self._get_orm_task(session, database_id=task_id)
            if orm_task is None:
                raise RuntimeError(f"Task {task_id} not found")
            orm_task.status = status
            if result_uri is not None:
                orm_task.result_uri = result_uri
            if error_message is not None:
                orm_task.error_message = error_message
            await session.flush()
            return orm_task.to_dto()

    ##################################

    @override
    async def insert_simulator(self, git_commit_hash: str, git_repo_url: str, git_branch: str) -> SimulatorVersion:
        async with self.async_sessionmaker() as session, session.begin():
            stmt1 = (
                select(ORMSimulator)
                .where(
                    and_(
                        ORMSimulator.git_commit_hash == git_commit_hash,
                        ORMSimulator.git_repo_url == git_repo_url,
                        ORMSimulator.git_branch == git_branch,
                    )
                )
                .limit(1)
            )
            result1: Result[tuple[ORMSimulator]] = await session.execute(stmt1)
            existing_orm_simulator: ORMSimulator | None = result1.scalars().one_or_none()
            if existing_orm_simulator is not None:
                # If the simulator already exists
                logger.error(
                    f"Simulator with git_commit_hash={git_commit_hash}, git_repo_url={git_repo_url}, "
                    f"git_branch={git_branch} already exists in the database"
                )
                raise RuntimeError(f"Simulator with git_commit_hash={git_commit_hash} already exists in the database")

            # did not find the simulator, so insert it
            new_orm_simulator = ORMSimulator(
                git_commit_hash=git_commit_hash,
                git_repo_url=git_repo_url,
                git_branch=git_branch,
            )
            session.add(new_orm_simulator)
            await session.flush()
            # Ensure the ORM object is inserted and has an ID
            return new_orm_simulator.to_simulator_version()

    @override
    async def get_simulator(self, simulator_id: int) -> SimulatorVersion | None:
        async with self.async_sessionmaker() as session, session.begin():
            orm_simulator = await self._get_orm_simulator(session, simulator_id=simulator_id)
            if orm_simulator is None:
                return None
            return orm_simulator.to_simulator_version()

    @override
    async def get_simulator_by_commit(self, commit_hash: str) -> SimulatorVersion | None:
        async with self.async_sessionmaker() as session, session.begin():
            stmt1 = select(ORMSimulator).where(ORMSimulator.git_commit_hash == commit_hash).limit(1)
            result1: Result[tuple[ORMSimulator]] = await session.execute(stmt1)
            orm_simulator: ORMSimulator | None = result1.scalars().one_or_none()
            if orm_simulator is None:
                return None
            return orm_simulator.to_simulator_version()

    @override
    async def delete_simulator(self, simulator_id: int) -> None:
        async with self.async_sessionmaker() as session, session.begin():
            orm_simulator: ORMSimulator | None = await self._get_orm_simulator(session, simulator_id=simulator_id)
            if orm_simulator is None:
                raise Exception(f"Simulator with id {simulator_id} not found in the database")
            await session.delete(orm_simulator)

    @override
    async def list_simulators(self) -> list[SimulatorVersion]:
        async with self.async_sessionmaker() as session:
            stmt = select(ORMSimulator)
            result: Result[tuple[ORMSimulator]] = await session.execute(stmt)
            orm_simulators = result.scalars().all()

            simulator_versions: list[SimulatorVersion] = []
            for orm_simulator in orm_simulators:
                simulator_versions.append(orm_simulator.to_simulator_version())
            return simulator_versions

    @override
    async def insert_hpcrun(
        self,
        job_id: JobId,
        job_type: JobType,
        ref_id: int,
        correlation_id: str,
        chain_n_generations: int | None = None,
        chain_final_job_ids: list[str] | None = None,
        chain_current_job_ids: list[str | None] | None = None,
        chain_current_generation: list[int | None] | None = None,
        chain_parca_done: bool | None = None,
        multi_node_composite_id: str | None = None,
    ) -> HpcRun:
        jobref_simulation_id = ref_id if job_type == JobType.SIMULATION else None
        jobref_parca_dataset_id = ref_id if job_type == JobType.PARCA else None
        jobref_simulator_id = ref_id if job_type == JobType.BUILD_IMAGE else None

        async with self.async_sessionmaker() as session, session.begin():
            orm_hpc_run = ORMHpcRun(
                job_id_ext=str(job_id),
                job_backend=job_id.backend,
                job_type=JobTypeDB.from_job_type(job_type),
                status=JobStatusDB.RUNNING,
                jobref_simulator_id=jobref_simulator_id,
                jobref_simulation_id=jobref_simulation_id,
                jobref_parca_dataset_id=jobref_parca_dataset_id,
                start_time=datetime.datetime.now(),
                correlation_id=correlation_id,
                chain_n_generations=chain_n_generations,
                chain_final_job_ids=list(chain_final_job_ids) if chain_final_job_ids is not None else None,
                chain_current_job_ids=list(chain_current_job_ids) if chain_current_job_ids is not None else None,
                chain_current_generation=list(chain_current_generation)
                if chain_current_generation is not None
                else None,
                chain_parca_done=chain_parca_done,
                multi_node_composite_id=multi_node_composite_id,
                # Derived, never minted: the dispatcher already handed every task
                # the same ids (viva_api.common.events_env) before this row existed.
                trace_id=trace_id_from_correlation(correlation_id),
                campaign_span_id=campaign_span_id(correlation_id),
            )
            session.add(orm_hpc_run)
            await session.flush()
            return orm_hpc_run.to_hpc_run()

    @override
    async def get_hpcrun_by_job_id(self, job_id: JobId) -> HpcRun | None:
        async with self.async_sessionmaker() as session, session.begin():
            orm_hpc_job: ORMHpcRun | None = await self._get_orm_hpcrun_by_job_id(session, job_id=job_id)
            if orm_hpc_job is None:
                return None
            return orm_hpc_job.to_hpc_run()

    @override
    async def get_hpcrun_by_ref(self, ref_id: int, job_type: JobType) -> HpcRun | None:
        async with self.async_sessionmaker() as session, session.begin():
            orm_hpc_job: ORMHpcRun | None = await self._get_orm_hpcrun_by_ref(session, ref_id=ref_id, job_type=job_type)
            if orm_hpc_job is None:
                return None
            return orm_hpc_job.to_hpc_run()

    @override
    async def get_hpcrun(self, hpcrun_id: int) -> HpcRun | None:
        async with self.async_sessionmaker() as session, session.begin():
            orm_hpc_job: ORMHpcRun | None = await self._get_orm_hpcrun(session, hpcrun_id=hpcrun_id)
            if orm_hpc_job is None:
                return None
            return orm_hpc_job.to_hpc_run()

    @override
    async def delete_hpcrun(self, hpcrun_id: int) -> None:
        async with self.async_sessionmaker() as session, session.begin():
            hpcrun: ORMHpcRun | None = await self._get_orm_hpcrun(session, hpcrun_id=hpcrun_id)
            if hpcrun is None:
                raise Exception(f"HpcRun with id {hpcrun_id} not found in the database")
            await session.delete(hpcrun)

    @override
    async def insert_parca_dataset(self, parca_dataset_request: ParcaDatasetRequest) -> ParcaDataset:
        async with self.async_sessionmaker() as session, session.begin():
            simulator_id = parca_dataset_request.simulator_version.database_id
            stmt1 = (
                select(ORMParcaDataset)
                .where(
                    and_(
                        ORMParcaDataset.simulator_id == simulator_id,
                        ORMParcaDataset.parca_config_hash == parca_dataset_request.config_hash,
                    )
                )
                .limit(1)
            )
            result1: Result[tuple[ORMParcaDataset]] = await session.execute(stmt1)
            existing_orm_parca_dataset: ORMParcaDataset | None = result1.scalars().one_or_none()
            if existing_orm_parca_dataset is not None:
                logger.info("Parca Dataset with the same configuration already exists in the database")
                return await self.get_parca_dataset(existing_orm_parca_dataset.id)  # type: ignore[return-value]

            # did not find the parca dataset, so insert it
            orm_simulator: ORMSimulator | None = await self._get_orm_simulator(session, simulator_id=simulator_id)
            if orm_simulator is None:
                raise Exception(f"Simulator with id {simulator_id} not found in the database")
            orm_parca_dataset = ORMParcaDataset(
                simulator_id=orm_simulator.id,
                parca_config=parca_dataset_request.parca_config.model_dump(),
                parca_config_hash=parca_dataset_request.config_hash,
            )
            session.add(orm_parca_dataset)
            await session.flush()  # Ensure the ORM object is inserted and has an ID
            # Ensure the ORM object is inserted and has an ID
            orm_parca_dataset_id = orm_parca_dataset.id
            # Prepare the ParcaDataset object to return
            simulator_version = SimulatorVersion(
                database_id=orm_simulator.id,
                git_commit_hash=orm_simulator.git_commit_hash,
                git_repo_url=orm_simulator.git_repo_url,
                git_branch=orm_simulator.git_branch,
            )
            parca_dataset_request = ParcaDatasetRequest(
                simulator_version=simulator_version,
                parca_config=parca_options_from_stored(orm_parca_dataset.parca_config),
            )
            parca_dataset = ParcaDataset(
                database_id=orm_parca_dataset_id,
                parca_dataset_request=parca_dataset_request,
                remote_archive_path=None,
            )
            return parca_dataset

    @override
    async def get_parca_dataset(self, parca_dataset_id: int) -> ParcaDataset | None:
        async with self.async_sessionmaker() as session, session.begin():
            orm_parca_dataset: ORMParcaDataset | None = await self._get_orm_parca_dataset(
                session, parca_dataset_id=parca_dataset_id
            )
            if orm_parca_dataset is None:
                return None

            simulator_version: SimulatorVersion | None = await self.get_simulator(orm_parca_dataset.simulator_id)
            if simulator_version is None:
                raise Exception(f"Simulator with id {orm_parca_dataset.simulator_id} not found in the database")

            return ParcaDataset(
                database_id=orm_parca_dataset.id,
                parca_dataset_request=ParcaDatasetRequest(
                    simulator_version=simulator_version,
                    parca_config=parca_options_from_stored(orm_parca_dataset.parca_config),
                ),
                remote_archive_path=orm_parca_dataset.remote_archive_path,
            )

    @override
    async def delete_parca_dataset(self, parca_dataset: int) -> None:
        async with self.async_sessionmaker() as session, session.begin():
            orm_parca_dataset: ORMParcaDataset | None = await self._get_orm_parca_dataset(
                session, parca_dataset_id=parca_dataset
            )
            if orm_parca_dataset is None:
                raise Exception(f"Parca Dataset with id {parca_dataset} not found in the database")
            await session.delete(orm_parca_dataset)

    @override
    async def list_parca_datasets(self) -> list[ParcaDataset]:
        async with self.async_sessionmaker() as session:
            stmt = select(ORMParcaDataset)
            result: Result[tuple[ORMParcaDataset]] = await session.execute(stmt)
            orm_parca_datasets = result.scalars().all()

            parca_datasets: list[ParcaDataset] = []
            for orm_parca_dataset in orm_parca_datasets:
                simulator_version: SimulatorVersion | None = await self.get_simulator(orm_parca_dataset.simulator_id)
                if simulator_version is None:
                    raise Exception(f"Simulator with id {orm_parca_dataset.simulator_id} not found in the database")
                parca_datasets.append(
                    ParcaDataset(
                        database_id=orm_parca_dataset.id,
                        parca_dataset_request=ParcaDatasetRequest(
                            simulator_version=simulator_version,
                            parca_config=parca_options_from_stored(orm_parca_dataset.parca_config),
                        ),
                        remote_archive_path=orm_parca_dataset.remote_archive_path,
                    )
                )
            return parca_datasets

    @override
    async def insert_worker_event(self, worker_event: WorkerEvent, hpcrun_id: int) -> WorkerEvent:
        async with self.async_sessionmaker() as session, session.begin():
            orm_worker_event = ORMWorkerEvent.from_worker_event(worker_event, hpcrun_id=hpcrun_id)
            session.add(orm_worker_event)
            await session.flush()  # Ensure the ORM object is inserted and has an ID

            # prepare the Simulation object to return
            new_worker_event = orm_worker_event.to_worker_event()
            return new_worker_event

    @override
    async def list_worker_events(self, hpcrun_id: int, prev_sequence_number: int | None = None) -> list[WorkerEvent]:
        async with self.async_sessionmaker() as session, session.begin():
            stmt = (
                select(
                    ORMWorkerEvent.mass,
                    ORMWorkerEvent.sequence_number,
                    ORMWorkerEvent.id,
                    ORMWorkerEvent.time,
                    ORMWorkerEvent.hpcrun_id,
                )
                .where(
                    and_(
                        ORMWorkerEvent.hpcrun_id == hpcrun_id,
                        ORMWorkerEvent.sequence_number > (prev_sequence_number or -1),
                    )
                )
                .order_by(ORMWorkerEvent.sequence_number)
            )
            result: Result[tuple[dict[str, float], int, int, float, int]] = await session.execute(stmt)
            orm_worker_events = result.all()

            worker_events: list[WorkerEvent] = []
            for orm_worker_event in orm_worker_events:
                worker_events.append(ORMWorkerEvent.from_query_results(orm_worker_event.tuple()))
            return worker_events

    @override
    async def insert_simulation(self, sim_request: SimulationRequest) -> Simulation:
        async with self.async_sessionmaker() as session, session.begin():
            simulator_id = sim_request.simulator_id
            orm_simulator = None
            if simulator_id is not None:
                orm_simulator = await self._get_orm_simulator(session, simulator_id)
            if orm_simulator is None and sim_request.simulator is not None:
                simulators = await self.list_simulators()
                matching_sim: SimulatorVersion | None = next(
                    (
                        sim
                        for sim in simulators
                        if sim.git_branch == sim_request.simulator.git_branch
                        and sim.git_repo_url == sim_request.simulator.git_repo_url
                        and sim.git_commit_hash == sim_request.simulator.git_commit_hash
                    ),
                    None,
                )
                if matching_sim is not None:
                    orm_simulator = await self._get_orm_simulator(session, matching_sim.database_id)

            if orm_simulator is None:
                raise Exception(f"Simulator specified in request: {sim_request} not found in the database")
            simulator_id = orm_simulator.id

            parca_id = sim_request.parca_dataset_id
            if parca_id is None:
                raise Exception(
                    f"Parca Dataset with not found in the database with reference to simulator: {simulator_id}"
                )
            orm_parca_dataset: ORMParcaDataset | None = await self._get_orm_parca_dataset(
                session, parca_dataset_id=parca_id
            )
            if orm_parca_dataset is None:
                raise Exception(f"Parca Dataset with id {sim_request.parca_dataset_id} not found in the database")
            if orm_parca_dataset.simulator_id != orm_simulator.id:
                raise Exception(
                    f"Parca Dataset simulator mismatch, id={orm_simulator.id} and {sim_request.simulator_id}"
                )

            sim_config = sim_request.config
            config_filename = sim_request.simulation_config_filename
            orm_simulation = ORMSimulation(
                simulator_id=simulator_id,
                parca_dataset_id=orm_parca_dataset.id,
                config_filename=config_filename,
                experiment_id=sim_request.experiment_id,
                config=sim_config.model_dump(),
                tags=list(sim_request.tags),
            )
            session.add(orm_simulation)
            await session.flush()  # Ensure the ORM object is inserted and has an ID

            simulation = Simulation(
                database_id=orm_simulation.id,
                simulator_id=orm_simulator.id,
                parca_dataset_id=sim_request.parca_dataset_id,  # type: ignore[arg-type]
                config=sim_config,
                simulation_config_filename=config_filename,
                experiment_id=sim_request.experiment_id,
                tags=list(orm_simulation.tags),
            )
            return simulation

    @override
    async def get_simulation(self, simulation_id: int) -> Simulation | None:
        async with self.async_sessionmaker() as session:
            orm_simulation: ORMSimulation | None = await self._get_orm_simulation(session, simulation_id)
            if orm_simulation is None:
                return None

            simulation = Simulation(
                simulation_config_filename=orm_simulation.config_filename,
                experiment_id=orm_simulation.experiment_id,
                database_id=orm_simulation.id,
                simulator_id=orm_simulation.simulator_id,
                parca_dataset_id=orm_simulation.parca_dataset_id,
                config=SimulationConfig(**orm_simulation.config),  # type: ignore[arg-type]
                tags=list(orm_simulation.tags),
            )
            return simulation

    @override
    async def get_simulation_by_experiment_id(self, experiment_id: str) -> Simulation | None:
        async with self.async_sessionmaker() as session:
            stmt = select(ORMSimulation).where(ORMSimulation.config["experiment_id"].astext == experiment_id).limit(1)
            result: Result[tuple[ORMSimulation]] = await session.execute(stmt)
            orm_simulation = result.scalars().first()
            if orm_simulation is None:
                return None

            simulation = Simulation(
                simulation_config_filename=orm_simulation.config_filename,
                experiment_id=orm_simulation.experiment_id,
                database_id=orm_simulation.id,
                simulator_id=orm_simulation.simulator_id,
                parca_dataset_id=orm_simulation.parca_dataset_id,
                config=SimulationConfig(**orm_simulation.config),  # type: ignore[arg-type]
                tags=list(orm_simulation.tags),
            )
            return simulation

    @override
    async def delete_simulation(self, simulation_id: int) -> None:
        async with self.async_sessionmaker() as session, session.begin():
            orm_simulation: ORMSimulation | None = await self._get_orm_simulation(session, simulation_id)
            if orm_simulation is None:
                raise Exception(f"Simulation with id {simulation_id} not found in the database")
            await session.delete(orm_simulation)

    @override
    async def list_simulations(self) -> list[Simulation]:
        async with self.async_sessionmaker() as session:
            stmt = select(ORMSimulation)
            result: Result[tuple[ORMSimulation]] = await session.execute(stmt)
            orm_simulations = list(result.scalars().all())
            return self._build_simulations(orm_simulations)

    @override
    async def list_simulations_filtered(
        self, experiment_ids: list[str] | None = None, tags: list[str] | None = None
    ) -> list[Simulation]:
        clauses: list[ColumnElement[bool]] = []
        if experiment_ids:
            clauses.append(ORMSimulation.experiment_id.in_(experiment_ids))
        if tags:
            # tags @> '[t]' (JSONB containment) per tag, OR'd => "carries ANY of these tags".
            clauses.extend(ORMSimulation.tags.contains([t]) for t in tags)
        if not clauses:
            return []
        async with self.async_sessionmaker() as session:
            stmt = select(ORMSimulation).where(or_(*clauses))
            result: Result[tuple[ORMSimulation]] = await session.execute(stmt)
            orm_simulations = list(result.scalars().all())
            return self._build_simulations(orm_simulations)

    @override
    async def add_tags(self, simulation_id: int, tags: list[str]) -> Simulation:
        async with self.async_sessionmaker() as session, session.begin():
            orm_simulation = await self._get_orm_simulation(session, simulation_id)
            if orm_simulation is None:
                raise Exception(f"Simulation with id {simulation_id} not found in the database")
            # Union-merge, preserving existing order then appending new tags.
            merged = list(orm_simulation.tags)
            for tag in tags:
                if tag and tag not in merged:
                    merged.append(tag)
            orm_simulation.tags = merged
            await session.flush()
            return self._build_simulations([orm_simulation])[0]

    @override
    async def list_distinct_tags(self) -> dict[str, list[str]]:
        async with self.async_sessionmaker() as session:
            stmt = select(ORMSimulation.experiment_id, ORMSimulation.tags)
            result = await session.execute(stmt)
            tag_map: dict[str, list[str]] = {}
            for experiment_id, tags in result.all():
                for tag in tags or []:
                    tag_map.setdefault(tag, []).append(experiment_id)
            return tag_map

    @staticmethod
    def _build_simulations(orm_simulations: list[ORMSimulation]) -> list[Simulation]:
        simulations: list[Simulation] = []
        for orm_simulation in orm_simulations:
            try:
                simulation = Simulation(
                    simulation_config_filename=orm_simulation.config_filename,
                    experiment_id=orm_simulation.experiment_id,
                    database_id=orm_simulation.id,
                    simulator_id=orm_simulation.simulator_id,
                    parca_dataset_id=orm_simulation.parca_dataset_id,
                    config=SimulationConfig(**orm_simulation.config),  # type: ignore[arg-type]
                    tags=list(orm_simulation.tags),
                )
            except ValidationError as exc:
                # A stored config that no longer passes STRICT validation (e.g. a
                # parca_options key since made extra="forbid") must not 500 the whole
                # list — one legacy record would otherwise hide every run from every
                # client. Strip ONLY the extra-forbidden keys the error names and
                # re-parse strictly, so the record still enumerates with a valid
                # (type-correct) config and no serialization warnings. Strict
                # validation still gates creation and the per-id detail path
                # (get_simulation / get_simulation_by_experiment_id), so this is a
                # lenient LIST + strict DETAIL, not a global loosening.
                offending = [err["loc"] for err in exc.errors() if err["type"] == "extra_forbidden"]
                logger.warning(
                    "list: simulation id=%s (experiment_id=%s) has a stored config that "
                    "fails strict validation; listing it with legacy fields dropped. "
                    "Offending: %s",
                    orm_simulation.id,
                    orm_simulation.experiment_id,
                    "; ".join(".".join(str(p) for p in err["loc"]) for err in exc.errors()),
                )
                cleaned = copy.deepcopy(dict(orm_simulation.config))
                for loc in offending:
                    *parents, key = loc
                    node: Any = cleaned
                    for parent in parents:
                        node = node.get(parent) if isinstance(node, dict) else None
                    if isinstance(node, dict):
                        node.pop(key, None)
                try:
                    simulation = Simulation(
                        simulation_config_filename=orm_simulation.config_filename,
                        experiment_id=orm_simulation.experiment_id,
                        database_id=orm_simulation.id,
                        simulator_id=orm_simulation.simulator_id,
                        parca_dataset_id=orm_simulation.parca_dataset_id,
                        config=SimulationConfig(**cleaned),  # type: ignore[arg-type]
                        tags=list(orm_simulation.tags),
                    )
                except ValidationError:
                    # A non-extra validation error (e.g. a changed field type) —
                    # last resort: enumerate with an unvalidated construct so the
                    # list still never 500s on a single bad record.
                    simulation = Simulation.model_construct(
                        simulation_config_filename=orm_simulation.config_filename,
                        experiment_id=orm_simulation.experiment_id,
                        database_id=orm_simulation.id,
                        simulator_id=orm_simulation.simulator_id,
                        parca_dataset_id=orm_simulation.parca_dataset_id,
                        config=SimulationConfig.model_construct(**orm_simulation.config),  # type: ignore[arg-type]
                        tags=list(orm_simulation.tags),
                    )
            simulations.append(simulation)
        return simulations

    @override
    async def list_active_hpcruns(self) -> list[HpcRun]:
        async with self.async_sessionmaker() as session:
            stmt = select(ORMHpcRun).where(ORMHpcRun.status.in_([JobStatusDB.PENDING, JobStatusDB.RUNNING]))
            result: Result[tuple[ORMHpcRun]] = await session.execute(stmt)
            orm_hpcruns = result.scalars().all()
            return [orm_hpcrun.to_hpc_run() for orm_hpcrun in orm_hpcruns]

    @override
    async def list_active_chain_campaigns(self) -> list[HpcRun]:
        async with self.async_sessionmaker() as session:
            stmt = select(ORMHpcRun).where(
                ORMHpcRun.status.in_([JobStatusDB.PENDING, JobStatusDB.RUNNING]),
                ORMHpcRun.chain_n_generations.is_not(None),
            )
            result: Result[tuple[ORMHpcRun]] = await session.execute(stmt)
            orm_hpcruns = result.scalars().all()
            return [orm_hpcrun.to_hpc_run() for orm_hpcrun in orm_hpcruns]

    @override
    async def list_recently_cancelled_nextflow_hpcruns(self, since: datetime.datetime) -> list[HpcRun]:
        async with self.async_sessionmaker() as session:
            stmt = select(ORMHpcRun).where(
                ORMHpcRun.status == JobStatusDB.CANCELLED,
                ORMHpcRun.job_backend == JobBackend.K8S_NEXTFLOW.value,
                ORMHpcRun.end_time.is_not(None),
                ORMHpcRun.end_time >= since.replace(tzinfo=None),
            )
            result: Result[tuple[ORMHpcRun]] = await session.execute(stmt)
            orm_hpcruns = result.scalars().all()
            return [orm_hpcrun.to_hpc_run() for orm_hpcrun in orm_hpcruns]

    @override
    async def list_active_multi_node_composites(self) -> list[HpcRun]:
        async with self.async_sessionmaker() as session:
            stmt = select(ORMHpcRun).where(
                ORMHpcRun.status.in_([JobStatusDB.PENDING, JobStatusDB.RUNNING]),
                ORMHpcRun.multi_node_composite_id.is_not(None),
            )
            result: Result[tuple[ORMHpcRun]] = await session.execute(stmt)
            orm_hpcruns = result.scalars().all()
            return [orm_hpcrun.to_hpc_run() for orm_hpcrun in orm_hpcruns]

    @override
    async def list_active_nextflow_hpcruns(self) -> list[HpcRun]:
        async with self.async_sessionmaker() as session:
            stmt = select(ORMHpcRun).where(
                ORMHpcRun.status.in_([JobStatusDB.PENDING, JobStatusDB.RUNNING]),
                ORMHpcRun.job_backend == JobBackend.K8S_NEXTFLOW.value,
            )
            result: Result[tuple[ORMHpcRun]] = await session.execute(stmt)
            orm_hpcruns = result.scalars().all()
            return [orm_hpcrun.to_hpc_run() for orm_hpcrun in orm_hpcruns]

    @override
    async def finalize_nextflow_head(
        self,
        hpcrun_id: int,
        status: JobStatus,
        *,
        error_message: str | None = None,
        error_source: str | None = None,
        exit_code: int | None = None,
        attempt: int | None = None,
    ) -> bool:
        async with self.async_sessionmaker() as session, session.begin():
            stmt = (
                sa_update(ORMHpcRun)
                .where(
                    ORMHpcRun.id == hpcrun_id,
                    ORMHpcRun.status.in_([JobStatusDB.PENDING, JobStatusDB.RUNNING]),
                )
                .values(
                    status=JobStatusDB.from_job_status(status),
                    end_time=datetime.datetime.now(),
                    error_message=error_message,
                    error_source=error_source if error_message else None,
                    exit_code=exit_code,
                    attempt=attempt,
                )
            )
            result = cast(CursorResult[Any], await session.execute(stmt))
            return bool(result.rowcount)

    @staticmethod
    def _apply_error_message(orm_hpcrun: ORMHpcRun, update: JobStatusUpdate) -> None:
        """Write ``error_message`` by precedence (see ``update_hpcrun_status``)."""
        if update.error_message:
            if error_source_rank(update.error_source) >= error_source_rank(orm_hpcrun.error_source):
                orm_hpcrun.error_message = update.error_message
                orm_hpcrun.error_source = update.error_source
        elif update.status == JobStatus.CANCELLED:
            # A cancel is not a failure; whatever error text preceded it would
            # otherwise be shown as the reason the run stopped.
            orm_hpcrun.error_message = None
            orm_hpcrun.error_source = None

    @override
    async def update_hpcrun_status(self, hpcrun_id: int, update: JobStatusUpdate) -> None:
        async with self.async_sessionmaker() as session, session.begin():
            orm_hpcrun: ORMHpcRun | None = await self._get_orm_hpcrun(session, hpcrun_id=hpcrun_id)
            if orm_hpcrun is None:
                raise Exception(f"HpcRun with id {hpcrun_id} not found in the database")
            orm_hpcrun.status = JobStatusDB.from_job_status(update.status)
            if update.start_time is not None:
                with contextlib.suppress(ValueError):
                    dt = datetime.datetime.fromisoformat(update.start_time)
                    orm_hpcrun.start_time = dt.replace(tzinfo=None)
            if update.end_time is not None:
                with contextlib.suppress(ValueError):
                    dt = datetime.datetime.fromisoformat(update.end_time)
                    orm_hpcrun.end_time = dt.replace(tzinfo=None)
            self._apply_error_message(orm_hpcrun, update)
            if update.exit_code is not None:
                with contextlib.suppress(ValueError):
                    orm_hpcrun.exit_code = int(update.exit_code)
            if update.attempt is not None:
                orm_hpcrun.attempt = update.attempt
            await session.flush()

    @override
    async def list_active_local_hpcruns(self) -> list[HpcRun]:
        async with self.async_sessionmaker() as session:
            stmt = select(ORMHpcRun).where(
                ORMHpcRun.status.in_([JobStatusDB.PENDING, JobStatusDB.RUNNING]),
                ORMHpcRun.job_backend == JobBackend.LOCAL.value,
            )
            result: Result[tuple[ORMHpcRun]] = await session.execute(stmt)
            orm_hpcruns = result.scalars().all()
            return [orm_hpcrun.to_hpc_run() for orm_hpcrun in orm_hpcruns]

    @override
    async def set_hpcrun_external_job_ids(self, hpcrun_id: int, job_ids: list[str]) -> None:
        async with self.async_sessionmaker() as session, session.begin():
            orm_hpcrun: ORMHpcRun | None = await self._get_orm_hpcrun(session, hpcrun_id=hpcrun_id)
            if orm_hpcrun is None:
                raise Exception(f"HpcRun with id {hpcrun_id} not found in the database")
            orm_hpcrun.external_job_ids = list(job_ids)
            await session.flush()

    @override
    async def advance_chain_campaign(
        self, hpcrun_id: int, updater: Callable[[HpcRun], Awaitable[ChainCampaignUpdate | None]]
    ) -> None:
        async with self.async_sessionmaker() as session, session.begin():
            # Acquire the per-campaign lock FIRST, before reading -- this is what
            # makes the read-decide-write sequence atomic across concurrent ticks.
            # Transaction-scoped (pg_advisory_xact_lock, not the session-scoped
            # pg_advisory_lock): released automatically on commit/rollback, no
            # explicit unlock needed, no risk of a leaked lock outliving a crashed
            # request. Keyed on the campaign's own hpcrun_id -- a dedicated,
            # disjoint sequence, no collision risk with any other lock use.
            await session.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": hpcrun_id})

            orm_hpcrun = await self._get_orm_hpcrun(session, hpcrun_id=hpcrun_id)
            if orm_hpcrun is None:
                logger.warning("advance_chain_campaign: HpcRun %s not found (already deleted?)", hpcrun_id)
                return
            fresh = orm_hpcrun.to_hpc_run()

            update = await updater(fresh)
            if update is None:
                return  # nothing to persist this tick

            orm_hpcrun.chain_current_job_ids = list(update.chain_current_job_ids)
            orm_hpcrun.chain_current_generation = list(update.chain_current_generation)
            orm_hpcrun.chain_parca_done = update.chain_parca_done
            orm_hpcrun.chain_final_job_ids = list(update.chain_final_job_ids)
            if update.terminal_status is not None:
                orm_hpcrun.status = JobStatusDB.from_job_status(update.terminal_status)
                orm_hpcrun.end_time = datetime.datetime.now()
                if update.error_message and error_source_rank(update.error_source) >= error_source_rank(
                    orm_hpcrun.error_source
                ):
                    orm_hpcrun.error_message = update.error_message
                    orm_hpcrun.error_source = update.error_source
            await session.flush()

    @override
    async def finalize_multi_node_job(
        self, hpcrun_id: int, status: JobStatus, error_message: str | None = None
    ) -> bool:
        async with self.async_sessionmaker() as session, session.begin():
            stmt = (
                sa_update(ORMHpcRun)
                .where(
                    ORMHpcRun.id == hpcrun_id,
                    ORMHpcRun.status.in_([JobStatusDB.PENDING, JobStatusDB.RUNNING]),
                )
                .values(
                    status=JobStatusDB.from_job_status(status),
                    end_time=datetime.datetime.now(),
                    error_message=error_message,
                )
            )
            result = cast(CursorResult[Any], await session.execute(stmt))
            return bool(result.rowcount)

    @override
    async def get_hpcrun_id_by_correlation_id(self, correlation_id: str) -> int | None:
        async with self.async_sessionmaker() as session, session.begin():
            stmt = select(ORMHpcRun.id).where(ORMHpcRun.correlation_id == correlation_id).limit(1)
            result: Result[tuple[int]] = await session.execute(stmt)
            orm_hpcrun_id: int | None = result.scalar_one_or_none()
            return orm_hpcrun_id

    @override
    async def close(self) -> None:
        pass
