"""Database services for the compose subsystem (simulator, HPC, package registries)."""

import datetime
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, override

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import InstrumentedAttribute

from sms_api.common.hpc.models import SlurmJob
from sms_api.common.models import JobBackend
from sms_api.compose.container_def import ContainerizationFileRepr
from sms_api.compose.models import (
    BiGraphComputeType,
    BiGraphProcess,
    BiGraphStep,
    ComposeHpcRun,
    ComposeJobStatus,
    ComposeJobType,
    ComposeSimulation,
    ComposeSimulationRequest,
    ComposeSimulationResults,
    ComposeSimulatorVersion,
    ComposeSubmittedSimulation,
    ComposeWorkerEvent,
    PackageOutline,
    PackageType,
    RegisteredPackage,
    get_singularity_hash,
)
from sms_api.compose.tables_orm import (
    BiGraphComputeTypeDB,
    ComposeJobStatusDB,
    ComposeJobTypeDB,
    ORMComposeAllowList,
    ORMComposeBiGraphCompute,
    ORMComposeHpcRun,
    ORMComposePackage,
    ORMComposeSimulation,
    ORMComposeSimulator,
    ORMComposeSimulatorToPackage,
    ORMComposeWorkerEvent,
    PackageTypeDB,
)

logger = logging.getLogger(__name__)

# Terminal compose job states — a row in any of these is done and the monitor stops
# polling it. Everything else (WAITING/QUEUED/PENDING/RUNNING/SUSPENDED/UNKNOWN) is
# in-flight and must keep being polled so it can advance to a terminal state.
_TERMINAL_COMPOSE_STATUSES = (
    ComposeJobStatusDB.COMPLETED,
    ComposeJobStatusDB.FAILED,
    ComposeJobStatusDB.CANCELLED,
    ComposeJobStatusDB.OUT_OF_MEMORY,
    ComposeJobStatusDB.TIMEOUT,
)


# ---------------------------------------------------------------------------
# HPC path helper (used by simulator DB to build result paths)
# ---------------------------------------------------------------------------


def _get_experiment_dir(experiment_id: str) -> str:
    from sms_api.config import get_settings

    settings = get_settings()
    return f"{settings.compose_sim_base_path}/experiment-{experiment_id}"


# ---------------------------------------------------------------------------
# Simulator database service
# ---------------------------------------------------------------------------


class SimulatorDatabaseService(ABC):
    @abstractmethod
    async def insert_simulator(
        self, singularity_def_rep: ContainerizationFileRepr, packages_used: list[RegisteredPackage] | None = None
    ) -> ComposeSimulatorVersion:
        pass

    @abstractmethod
    async def get_simulator(self, simulator_id: int) -> ComposeSimulatorVersion | None:
        pass

    @abstractmethod
    async def get_simulator_by_def_hash(self, singularity_def_hash: str) -> ComposeSimulatorVersion | None:
        pass

    @abstractmethod
    async def list_simulators(self) -> list[ComposeSimulatorVersion]:
        pass

    @abstractmethod
    async def insert_simulation(
        self,
        sim_request: ComposeSimulationRequest,
        experiment_id: str,
        simulator_version: ComposeSimulatorVersion,
        document: str | None = None,
    ) -> ComposeSimulation:
        pass

    @abstractmethod
    async def get_simulations_experiment_id(self, simulation_id: int) -> str:
        pass

    @abstractmethod
    async def get_simulation_document(self, simulation_id: int) -> str | None:
        pass

    @abstractmethod
    async def list_simulations(self) -> list[ComposeSubmittedSimulation]:
        pass


class SimulatorORMExecutor(SimulatorDatabaseService):
    async_session_maker: async_sessionmaker[AsyncSession]

    def __init__(self, session_maker: async_sessionmaker[AsyncSession]) -> None:
        self.async_session_maker = session_maker

    @override
    async def insert_simulator(
        self, singularity_def_rep: ContainerizationFileRepr, packages_used: list[RegisteredPackage] | None = None
    ) -> ComposeSimulatorVersion:
        async with self.async_session_maker() as session, session.begin():
            singularity_hash = get_singularity_hash(singularity_def_rep)
            existing = (
                (
                    await session.execute(
                        select(ORMComposeSimulator).where(ORMComposeSimulator.singularity_def_hash == singularity_hash)
                    )
                )
                .scalars()
                .first()
            )
            if existing is not None:
                raise RuntimeError(f"Simulator with hash={singularity_hash} already exists")

            orm = ORMComposeSimulator(
                singularity_def=singularity_def_rep.representation,
                singularity_def_hash=singularity_hash,
            )
            session.add(orm)
            await session.flush()

            if packages_used is not None:
                for pkg in packages_used:
                    session.add(ORMComposeSimulatorToPackage(simulator_id=orm.id, package_id=pkg.database_id))

            return orm.to_simulator_version()

    @override
    async def get_simulator(self, simulator_id: int) -> ComposeSimulatorVersion | None:
        async with self.async_session_maker() as session:
            orm = (
                (await session.execute(select(ORMComposeSimulator).where(ORMComposeSimulator.id == simulator_id)))
                .scalars()
                .first()
            )
            return orm.to_simulator_version() if orm else None

    @override
    async def get_simulator_by_def_hash(self, singularity_def_hash: str) -> ComposeSimulatorVersion | None:
        async with self.async_session_maker() as session:
            orm = (
                (
                    await session.execute(
                        select(ORMComposeSimulator).where(
                            ORMComposeSimulator.singularity_def_hash == singularity_def_hash
                        )
                    )
                )
                .scalars()
                .first()
            )
            return orm.to_simulator_version() if orm else None

    @override
    async def list_simulators(self) -> list[ComposeSimulatorVersion]:
        async with self.async_session_maker() as session:
            result = await session.execute(select(ORMComposeSimulator))
            return [orm.to_simulator_version() for orm in result.scalars().all()]

    @override
    async def insert_simulation(
        self,
        sim_request: ComposeSimulationRequest,
        experiment_id: str,
        simulator_version: ComposeSimulatorVersion,
        document: str | None = None,
    ) -> ComposeSimulation:
        async with self.async_session_maker() as session, session.begin():
            orm = ORMComposeSimulation(
                experiment_id=experiment_id,
                simulator_id=simulator_version.database_id,
                document=document,
            )
            session.add(orm)
            await session.flush()
            return ComposeSimulation(database_id=orm.id, sim_request=sim_request, simulator_version=simulator_version)

    @override
    async def get_simulations_experiment_id(self, simulation_id: int) -> str:
        async with self.async_session_maker() as session:
            orm = (
                (await session.execute(select(ORMComposeSimulation).where(ORMComposeSimulation.id == simulation_id)))
                .scalars()
                .first()
            )
            if orm is None:
                raise LookupError(f"Compose simulation {simulation_id} not found")
            return orm.experiment_id

    @override
    async def get_simulation_document(self, simulation_id: int) -> str | None:
        async with self.async_session_maker() as session:
            orm = (
                (await session.execute(select(ORMComposeSimulation).where(ORMComposeSimulation.id == simulation_id)))
                .scalars()
                .first()
            )
            if orm is None:
                raise LookupError(f"Compose simulation {simulation_id} not found")
            return orm.document

    @override
    async def list_simulations(self) -> list[ComposeSubmittedSimulation]:
        async with self.async_session_maker() as session:
            result = await session.execute(
                select(ORMComposeSimulation, ORMComposeSimulator).join(
                    ORMComposeSimulator, onclause=ORMComposeSimulation.simulator_id == ORMComposeSimulator.id
                )
            )
            sims: list[ComposeSubmittedSimulation] = []
            for row in result.fetchall():
                orm_sim, orm_simulator = row.t
                hpc_run = await _get_hpc_run_by_correlation(session, orm_sim.experiment_id)
                sims.append(
                    ComposeSubmittedSimulation(
                        database_id=orm_sim.id,
                        sim_content=ComposeSimulationResults(
                            path_on_server=Path(_get_experiment_dir(orm_sim.experiment_id))
                        ),
                        simulator_version=orm_simulator.to_simulator_version(),
                        hpc_run=hpc_run,
                    )
                )
            return sims


async def _get_hpc_run_by_correlation(session: AsyncSession, correlation_id: str) -> ComposeHpcRun | None:
    orm = (
        (
            await session.execute(
                select(ORMComposeHpcRun).where(ORMComposeHpcRun.correlation_id == correlation_id).limit(1)
            )
        )
        .scalars()
        .first()
    )
    return orm.to_hpc_run() if orm else None


# ---------------------------------------------------------------------------
# HPC database service
# ---------------------------------------------------------------------------


class HPCDatabaseService(ABC):
    @abstractmethod
    async def insert_hpcrun(
        self, slurmjobid: int, job_type: ComposeJobType, ref_id: int, correlation_id: str
    ) -> ComposeHpcRun:
        pass

    @abstractmethod
    async def get_hpcrun_by_ref(self, ref_id: int, job_type: ComposeJobType) -> ComposeHpcRun | None:
        pass

    @abstractmethod
    async def get_hpcruns_by_refs(self, ref_ids: list[int], job_type: ComposeJobType) -> list[ComposeHpcRun]:
        pass

    @abstractmethod
    async def get_hpcrun_by_slurmjobid(self, slurmjobid: int) -> ComposeHpcRun | None:
        pass

    @abstractmethod
    async def get_hpcrun_id_by_correlation_id(self, correlation_id: str) -> int | None:
        pass

    @abstractmethod
    async def get_hpcrun_id_by_simulator_id(self, simulator_id: int) -> int | None:
        pass

    @abstractmethod
    async def list_running_hpcruns(self) -> list[ComposeHpcRun]:
        pass

    @abstractmethod
    async def update_hpcrun_status(self, hpcrun_id: int, new_slurm_job: SlurmJob) -> None:
        pass

    @abstractmethod
    async def update_hpcrun_dispatch(
        self, hpcrun_id: int, job_id_ext: str, backend: "JobBackend", status: ComposeJobStatus
    ) -> None:
        """Attach the real backend job id + status to a placeholder row written at submit time."""

    @abstractmethod
    async def mark_hpcrun_failed(self, hpcrun_id: int, error_message: str) -> None:
        """Flip a placeholder/in-flight row to FAILED (e.g. a background dispatch throw)."""

    @abstractmethod
    async def insert_worker_event(self, worker_event: ComposeWorkerEvent, hpcrun_id: int) -> ComposeWorkerEvent:
        pass


class HPCORMExecutor(HPCDatabaseService):
    async_session_maker: async_sessionmaker[AsyncSession]

    def __init__(self, session_maker: async_sessionmaker[AsyncSession]) -> None:
        self.async_session_maker = session_maker

    def _get_job_type_ref(self, job_type: ComposeJobType) -> InstrumentedAttribute[int | None]:
        match job_type:
            case ComposeJobType.SIMULATION:
                return ORMComposeHpcRun.simulation_id
            case ComposeJobType.BUILD_CONTAINER:
                return ORMComposeHpcRun.simulator_id

    @override
    async def insert_hpcrun(
        self, slurmjobid: int, job_type: ComposeJobType, ref_id: int, correlation_id: str
    ) -> ComposeHpcRun:
        async with self.async_session_maker() as session, session.begin():
            simulation_key = ref_id if job_type == ComposeJobType.SIMULATION else None
            simulator_key = ref_id if job_type == ComposeJobType.BUILD_CONTAINER else None
            orm = ORMComposeHpcRun(
                slurmjobid=slurmjobid,
                job_type=ComposeJobTypeDB.from_job_type(job_type),
                status=ComposeJobStatusDB.RUNNING,
                simulation_id=simulation_key,
                simulator_id=simulator_key,
                start_time=datetime.datetime.now(),
                correlation_id=correlation_id,
            )
            session.add(orm)
            await session.flush()
            return orm.to_hpc_run()

    @override
    async def get_hpcrun_by_ref(self, ref_id: int, job_type: ComposeJobType) -> ComposeHpcRun | None:
        async with self.async_session_maker() as session:
            ref_col = self._get_job_type_ref(job_type)
            orm = (await session.execute(select(ORMComposeHpcRun).where(ref_col == ref_id).limit(1))).scalars().first()
            return orm.to_hpc_run() if orm else None

    @override
    async def get_hpcruns_by_refs(self, ref_ids: list[int], job_type: ComposeJobType) -> list[ComposeHpcRun]:
        async with self.async_session_maker() as session:
            ref_col = self._get_job_type_ref(job_type)
            result = await session.execute(select(ORMComposeHpcRun).where(ref_col.in_(ref_ids)))
            return [orm.to_hpc_run() for orm in result.scalars().all()]

    @override
    async def get_hpcrun_by_slurmjobid(self, slurmjobid: int) -> ComposeHpcRun | None:
        async with self.async_session_maker() as session:
            orm = (
                (
                    await session.execute(
                        select(ORMComposeHpcRun).where(ORMComposeHpcRun.slurmjobid == slurmjobid).limit(1)
                    )
                )
                .scalars()
                .first()
            )
            return orm.to_hpc_run() if orm else None

    @override
    async def get_hpcrun_id_by_correlation_id(self, correlation_id: str) -> int | None:
        async with self.async_session_maker() as session:
            return (
                await session.execute(
                    select(ORMComposeHpcRun.id).where(ORMComposeHpcRun.correlation_id == correlation_id).limit(1)
                )
            ).scalar_one_or_none()

    @override
    async def get_hpcrun_id_by_simulator_id(self, simulator_id: int) -> int | None:
        async with self.async_session_maker() as session:
            return (
                await session.execute(
                    select(ORMComposeHpcRun.id).where(ORMComposeHpcRun.simulator_id == simulator_id).limit(1)
                )
            ).scalar_one_or_none()

    @override
    async def list_running_hpcruns(self) -> list[ComposeHpcRun]:
        # In-flight = NOT yet terminal. Must include QUEUED/PENDING/WAITING, not just
        # RUNNING: a Batch job sits at QUEUED (Batch RUNNABLE/STARTING) for minutes, and
        # the monitor is what advances it. A RUNNING-only filter dropped a job the instant
        # it was marked QUEUED, so it never got polled again and froze at QUEUED forever
        # even after the Batch job SUCCEEDED (the entire status lifecycle stalled). Poll
        # everything that isn't in a terminal state so a job can traverse queued→running→done.
        async with self.async_session_maker() as session:
            result = await session.execute(
                select(ORMComposeHpcRun).where(ORMComposeHpcRun.status.notin_(_TERMINAL_COMPOSE_STATUSES))
            )
            return [orm.to_hpc_run() for orm in result.scalars().all()]

    @override
    async def update_hpcrun_status(self, hpcrun_id: int, new_slurm_job: SlurmJob) -> None:
        async with self.async_session_maker() as session, session.begin():
            orm = (
                (await session.execute(select(ORMComposeHpcRun).where(ORMComposeHpcRun.id == hpcrun_id)))
                .scalars()
                .first()
            )
            if orm is None:
                raise RuntimeError(f"ComposeHpcRun {hpcrun_id} not found")
            orm.status = ComposeJobStatusDB(new_slurm_job.job_state.lower())
            if new_slurm_job.start_time is not None:
                orm.start_time = datetime.datetime.fromisoformat(new_slurm_job.start_time)
            if new_slurm_job.end_time is not None:
                orm.end_time = datetime.datetime.fromisoformat(new_slurm_job.end_time)
            await session.flush()

    @override
    async def update_hpcrun_dispatch(
        self, hpcrun_id: int, job_id_ext: str, backend: "JobBackend", status: ComposeJobStatus
    ) -> None:
        async with self.async_session_maker() as session, session.begin():
            orm = (
                (await session.execute(select(ORMComposeHpcRun).where(ORMComposeHpcRun.id == hpcrun_id)))
                .scalars()
                .first()
            )
            if orm is None:
                raise RuntimeError(f"ComposeHpcRun {hpcrun_id} not found")
            orm.job_id_ext = job_id_ext
            orm.job_backend = backend.value
            # SLURM job ids are ints — keep ``slurmjobid`` populated so the existing
            # squeue-based ComposeJobMonitor path still finds the run. Batch/Ray ids are
            # UUID strings and live only in ``job_id_ext``.
            if backend == JobBackend.SLURM:
                orm.slurmjobid = int(job_id_ext)
            orm.status = ComposeJobStatusDB(status.value)
            await session.flush()

    @override
    async def mark_hpcrun_failed(self, hpcrun_id: int, error_message: str) -> None:
        async with self.async_session_maker() as session, session.begin():
            orm = (
                (await session.execute(select(ORMComposeHpcRun).where(ORMComposeHpcRun.id == hpcrun_id)))
                .scalars()
                .first()
            )
            if orm is None:
                raise RuntimeError(f"ComposeHpcRun {hpcrun_id} not found")
            orm.status = ComposeJobStatusDB.FAILED
            orm.error_message = error_message[:2000]
            orm.end_time = datetime.datetime.now()
            await session.flush()

    @override
    async def insert_worker_event(self, worker_event: ComposeWorkerEvent, hpcrun_id: int) -> ComposeWorkerEvent:
        async with self.async_session_maker() as session, session.begin():
            orm = ORMComposeWorkerEvent.from_worker_event(worker_event, hpcrun_id=hpcrun_id)
            session.add(orm)
            await session.flush()
            return orm.to_worker_event()


# ---------------------------------------------------------------------------
# Package database service
# ---------------------------------------------------------------------------


class PackageDatabaseService(ABC):
    @abstractmethod
    async def insert_package(self, package_outline: PackageOutline) -> RegisteredPackage:
        pass

    @abstractmethod
    async def list_all_computes(self, compute_type: BiGraphComputeType | None = None) -> Any:
        pass


class PackageORMExecutor(PackageDatabaseService):
    async_session_maker: async_sessionmaker[AsyncSession]

    def __init__(self, session_maker: async_sessionmaker[AsyncSession]) -> None:
        self.async_session_maker = session_maker

    @override
    async def insert_package(self, package: PackageOutline) -> RegisteredPackage:
        async with self.async_session_maker() as session, session.begin():
            orm_pkg = ORMComposePackage(
                package_type=PackageTypeDB.from_package_type(package.package_type),
                name=package.name,
            )
            session.add(orm_pkg)
            await session.flush()

            processes: list[BiGraphProcess] = []
            steps: list[BiGraphStep] = []
            for compute in package.compute:
                orm_compute = ORMComposeBiGraphCompute(
                    module=compute.module,
                    name=compute.name,
                    compute_type=BiGraphComputeTypeDB.from_compute_type(compute.compute_type),
                    inputs=compute.inputs,
                    outputs=compute.outputs,
                    package_ref=orm_pkg.id,
                )
                session.add(orm_compute)
                await session.flush()
                if orm_compute.compute_type == BiGraphComputeTypeDB.PROCESS:
                    processes.append(orm_compute.to_bigraph_process())
                else:
                    steps.append(orm_compute.to_bigraph_step())

            return orm_pkg.to_bigraph_package(processes, steps)

    @override
    async def list_all_computes(self, compute_type: BiGraphComputeType | None = None) -> Any:
        async with self.async_session_maker() as session:
            stmt = select(ORMComposeBiGraphCompute)
            if compute_type is not None:
                stmt = stmt.where(
                    ORMComposeBiGraphCompute.compute_type == BiGraphComputeTypeDB.from_compute_type(compute_type)
                )
            result = await session.execute(stmt)
            orms = result.scalars().all()
            match compute_type:
                case BiGraphComputeType.PROCESS:
                    return [o.to_bigraph_process() for o in orms]
                case BiGraphComputeType.STEP:
                    return [o.to_bigraph_step() for o in orms]
                case _:
                    return [o.to_bigraph_compute() for o in orms]


# ---------------------------------------------------------------------------
# Allow-list database service (compose_allow_list) — wires the previously
# dead ``ORMComposeAllowList`` table into something ``PBAllowList`` reads from.
# ---------------------------------------------------------------------------


class AllowListDatabaseService(ABC):
    @abstractmethod
    async def list_allow_list(self) -> list[str]:
        """Approved package specs, as ``"<type>::<name>"`` strings (``PBAllowList`` shape)."""

    @abstractmethod
    async def seed_if_empty(self, entries: list[str]) -> None:
        """Populate the table from ``entries`` (``"<type>::<name>"``) iff it's currently empty.

        Never overwrites operator-curated rows — only bootstraps a fresh deployment.
        """


class AllowListORMExecutor(AllowListDatabaseService):
    async_session_maker: async_sessionmaker[AsyncSession]

    def __init__(self, session_maker: async_sessionmaker[AsyncSession]) -> None:
        self.async_session_maker = session_maker

    @override
    async def list_allow_list(self) -> list[str]:
        async with self.async_session_maker() as session:
            result = await session.execute(select(ORMComposeAllowList))
            return [orm.to_spec() for orm in result.scalars().all()]

    @override
    async def seed_if_empty(self, entries: list[str]) -> None:
        async with self.async_session_maker() as session, session.begin():
            existing = (await session.execute(select(ORMComposeAllowList.id).limit(1))).scalar_one_or_none()
            if existing is not None:
                return
            for entry in entries:
                package_type_str, _, package_name = entry.partition("::")
                if not package_name:
                    package_type_str, package_name = PackageType.PYPI.value, entry
                session.add(
                    ORMComposeAllowList(
                        package_name=package_name,
                        package_type=PackageTypeDB(package_type_str),
                        package_version="*",
                    )
                )


# ---------------------------------------------------------------------------
# Facade
# ---------------------------------------------------------------------------


class ComposeDatabaseService:
    """Aggregated database service for the compose subsystem."""

    simulator_db: SimulatorDatabaseService
    hpc_db: HPCDatabaseService
    package_db: PackageDatabaseService
    allow_list_db: AllowListDatabaseService

    def __init__(self, session_maker: async_sessionmaker[AsyncSession]) -> None:
        self.simulator_db = SimulatorORMExecutor(session_maker)
        self.hpc_db = HPCORMExecutor(session_maker)
        self.package_db = PackageORMExecutor(session_maker)
        self.allow_list_db = AllowListORMExecutor(session_maker)

    def get_simulator_db(self) -> SimulatorDatabaseService:
        return self.simulator_db

    def get_hpc_db(self) -> HPCDatabaseService:
        return self.hpc_db

    def get_package_db(self) -> PackageDatabaseService:
        return self.package_db

    def get_allow_list_db(self) -> AllowListDatabaseService:
        return self.allow_list_db
