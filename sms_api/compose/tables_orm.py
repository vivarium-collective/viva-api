"""SQLAlchemy ORM tables for the compose (process-bigraph) subsystem.

All table names are prefixed with ``compose_`` to avoid collisions with
the existing sms-api tables.
"""

import datetime
import enum
import logging

from sqlalchemy import ForeignKey, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncAttrs, AsyncEngine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from sms_api.common.models import JobBackend
from sms_api.compose.container_def import ContainerizationEngine, ContainerizationFileRepr
from sms_api.compose.models import (
    BiGraphCompute,
    BiGraphComputeType,
    BiGraphProcess,
    BiGraphStep,
    ComposeHpcRun,
    ComposeJobStatus,
    ComposeJobType,
    ComposeSimulatorVersion,
    ComposeWorkerEvent,
    PackageType,
    RegisteredPackage,
)

logger = logging.getLogger(__name__)

COMPOSE_PACKAGE_TABLE = "compose_packages"


# ---------------------------------------------------------------------------
# Declarative base (separate from sms-api's Base)
# ---------------------------------------------------------------------------


class ComposeBase(AsyncAttrs, DeclarativeBase):
    pass


# ---------------------------------------------------------------------------
# Enum mappers
# ---------------------------------------------------------------------------


class ComposeJobStatusDB(enum.Enum):
    WAITING = "waiting"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    PENDING = "pending"
    CANCELLED = "cancelled"
    OUT_OF_MEMORY = "out_of_memory"
    SUSPENDED = "suspended"
    TIMEOUT = "timeout"
    UNKNOWN = "unknown"

    def to_job_status(self) -> ComposeJobStatus:
        return ComposeJobStatus(self.value)


class ComposeJobTypeDB(enum.Enum):
    SIMULATION = "simulation"
    BUILD_CONTAINER = "build_container"

    def to_job_type(self) -> ComposeJobType:
        return ComposeJobType(self.value)

    @classmethod
    def from_job_type(cls, job_type: ComposeJobType) -> "ComposeJobTypeDB":
        return ComposeJobTypeDB(job_type.value)


class PackageTypeDB(enum.Enum):
    PYPI = "pypi"
    CONDA = "conda"

    def to_package_type(self) -> PackageType:
        return PackageType(self.value)

    @classmethod
    def from_package_type(cls, package_type: PackageType) -> "PackageTypeDB":
        return PackageTypeDB(package_type.value)


class BiGraphComputeTypeDB(enum.Enum):
    PROCESS = "process"
    STEP = "step"

    def to_compute_type(self) -> BiGraphComputeType:
        return BiGraphComputeType(self.value)

    @classmethod
    def from_compute_type(cls, compute_type: BiGraphComputeType | None) -> "BiGraphComputeTypeDB":
        if compute_type is None:
            raise ValueError("No compute type specified")
        return BiGraphComputeTypeDB(compute_type.value)


# ---------------------------------------------------------------------------
# Simulator tables
# ---------------------------------------------------------------------------


class ORMComposeSimulator(ComposeBase):
    __tablename__ = "compose_simulator"

    id: Mapped[int] = mapped_column(primary_key=True)
    created_at: Mapped[datetime.datetime] = mapped_column(server_default=func.now())
    singularity_def: Mapped[str] = mapped_column(nullable=False)
    singularity_def_hash: Mapped[str] = mapped_column(nullable=False, unique=True)

    def to_simulator_version(self) -> ComposeSimulatorVersion:
        return ComposeSimulatorVersion(
            database_id=self.id,
            created_at=self.created_at,
            singularity_def=ContainerizationFileRepr(
                representation=self.singularity_def,
                containerization_engine=ContainerizationEngine.APPTAINER,
            ),
            singularity_def_hash=self.singularity_def_hash,
            packages=None,
        )


class ORMComposeSimulatorToPackage(ComposeBase):
    __tablename__ = "compose_simulator_to_package"

    id: Mapped[int] = mapped_column(primary_key=True)
    simulator_id: Mapped[int] = mapped_column(ForeignKey("compose_simulator.id"), nullable=False, index=True)
    package_id: Mapped[int] = mapped_column(ForeignKey(f"{COMPOSE_PACKAGE_TABLE}.id"), nullable=False, index=True)


class ORMComposeSimulation(ComposeBase):
    __tablename__ = "compose_simulation"

    id: Mapped[int] = mapped_column(primary_key=True)
    created_at: Mapped[datetime.datetime] = mapped_column(server_default=func.now())
    experiment_id: Mapped[str] = mapped_column(nullable=False, unique=True)
    simulator_id: Mapped[int] = mapped_column(ForeignKey("compose_simulator.id"), nullable=False, index=True)
    # The uploaded document content — PBG JSON or raw file bytes as a string.
    # For OMEX archives, this stores the contained .pbg JSON (extracted at upload time).
    # For standalone .pbg files, this stores the JSON directly.
    # For .sbml files, this stores the SBML XML as a string.
    document: Mapped[str | None] = mapped_column(nullable=True)


# ---------------------------------------------------------------------------
# HPC job tables
# ---------------------------------------------------------------------------


class ORMComposeHpcRun(ComposeBase):
    __tablename__ = "compose_hpcrun"

    id: Mapped[int] = mapped_column(primary_key=True)
    created_at: Mapped[datetime.datetime] = mapped_column(server_default=func.now())

    job_type: Mapped[ComposeJobTypeDB] = mapped_column(nullable=False)
    correlation_id: Mapped[str] = mapped_column(nullable=False, index=True, unique=True)
    slurmjobid: Mapped[int] = mapped_column(nullable=True)
    # Backend-agnostic job id (string), mirroring ``ORMHpcRun.job_id_ext``/``job_backend``:
    # SLURM job ids are ints (kept in ``slurmjobid`` for the existing monitor path), but
    # AWS Batch/Ray job ids are UUID strings — those live here, tagged by ``job_backend``.
    job_id_ext: Mapped[str | None] = mapped_column(nullable=True)
    job_backend: Mapped[str] = mapped_column(nullable=False, server_default=JobBackend.RAY)
    start_time: Mapped[datetime.datetime | None] = mapped_column(nullable=True)
    end_time: Mapped[datetime.datetime | None] = mapped_column(nullable=True)
    status: Mapped[ComposeJobStatusDB] = mapped_column(nullable=False)
    error_message: Mapped[str | None] = mapped_column(nullable=True)

    simulation_id: Mapped[int | None] = mapped_column(ForeignKey("compose_simulation.id"), nullable=True, index=True)
    simulator_id: Mapped[int | None] = mapped_column(ForeignKey("compose_simulator.id"), nullable=True, index=True)

    def to_hpc_run(self) -> ComposeHpcRun:
        if self.simulation_id is None and self.simulator_id is None:
            raise RuntimeError("ORMComposeHpcRun must have at least one job reference set.")
        return ComposeHpcRun(
            database_id=self.id,
            slurmjobid=self.slurmjobid,
            job_id_ext=self.job_id_ext,
            job_backend=self.job_backend,
            correlation_id=self.correlation_id,
            job_type=self.job_type.to_job_type(),
            sim_id=self.simulation_id,
            simulator_id=self.simulator_id,
            status=self.status.to_job_status(),
            error_message=self.error_message,
            start_time=str(self.start_time) if self.start_time else None,
            end_time=str(self.end_time) if self.end_time else None,
        )


class ORMComposeWorkerEvent(ComposeBase):
    __tablename__ = "compose_worker_event"

    id: Mapped[int] = mapped_column(primary_key=True)
    created_at: Mapped[datetime.datetime] = mapped_column(server_default=func.now())

    correlation_id: Mapped[str] = mapped_column(ForeignKey("compose_hpcrun.correlation_id", ondelete="CASCADE"))
    sequence_number: Mapped[int] = mapped_column(nullable=False, index=True)
    mass: Mapped[dict[str, float]] = mapped_column(JSONB, nullable=False)
    time: Mapped[float] = mapped_column(nullable=True)
    hpcrun_id: Mapped[int] = mapped_column(
        ForeignKey("compose_hpcrun.id", ondelete="CASCADE"), nullable=False, index=True
    )

    @classmethod
    def from_worker_event(cls, worker_event: ComposeWorkerEvent, hpcrun_id: int) -> "ORMComposeWorkerEvent":
        return cls(
            hpcrun_id=hpcrun_id,
            correlation_id=worker_event.correlation_id,
            sequence_number=worker_event.sequence_number,
            mass=worker_event.mass,
            time=worker_event.time,
        )

    def to_worker_event(self) -> ComposeWorkerEvent:
        return ComposeWorkerEvent(
            database_id=self.id,
            created_at=str(self.created_at),
            hpcrun_id=self.hpcrun_id,
            correlation_id=self.correlation_id,
            sequence_number=self.sequence_number,
            mass=self.mass,
            time=self.time,
        )


# ---------------------------------------------------------------------------
# Package registry tables
# ---------------------------------------------------------------------------


class ORMComposePackage(ComposeBase):
    __tablename__ = COMPOSE_PACKAGE_TABLE

    id: Mapped[int] = mapped_column(primary_key=True)
    created_at: Mapped[datetime.datetime] = mapped_column(server_default=func.now())
    package_type: Mapped[PackageTypeDB] = mapped_column(nullable=False)
    name: Mapped[str] = mapped_column(nullable=False, unique=True)

    __table_args__ = (UniqueConstraint("name", "package_type", name="uq_compose_package_name_type"),)

    def to_bigraph_package(self, processes: list[BiGraphProcess], steps: list[BiGraphStep]) -> RegisteredPackage:
        return RegisteredPackage(
            database_id=self.id,
            package_type=PackageType(self.package_type.value),
            name=self.name,
            processes=processes,
            steps=steps,
        )


class ORMComposeBiGraphCompute(ComposeBase):
    __tablename__ = "compose_bigraph_compute"

    id: Mapped[int] = mapped_column(primary_key=True)
    inserted_at: Mapped[datetime.datetime] = mapped_column(server_default=func.now())
    package_ref: Mapped[int] = mapped_column(ForeignKey(f"{COMPOSE_PACKAGE_TABLE}.id"), nullable=False, index=True)
    module: Mapped[str] = mapped_column(nullable=False)
    name: Mapped[str] = mapped_column(nullable=False)
    compute_type: Mapped[BiGraphComputeTypeDB] = mapped_column(nullable=False)
    inputs: Mapped[str] = mapped_column(nullable=True)
    outputs: Mapped[str] = mapped_column(nullable=True)

    def to_bigraph_process(self) -> BiGraphProcess:
        return BiGraphProcess(
            database_id=self.id,
            module=self.module,
            name=self.name,
            compute_type=self.compute_type.to_compute_type(),
            inputs=self.inputs,
            outputs=self.outputs,
        )

    def to_bigraph_step(self) -> BiGraphStep:
        return BiGraphStep(
            database_id=self.id,
            module=self.module,
            name=self.name,
            compute_type=self.compute_type.to_compute_type(),
            inputs=self.inputs,
            outputs=self.outputs,
        )

    def to_bigraph_compute(self) -> BiGraphCompute:
        compute_type = self.compute_type.to_compute_type()
        match compute_type:
            case BiGraphComputeType.PROCESS:
                return self.to_bigraph_process()
            case BiGraphComputeType.STEP:
                return self.to_bigraph_step()
        raise ValueError(f"Unknown compute type: {compute_type}")


class ORMComposeAllowList(ComposeBase):
    __tablename__ = "compose_allow_list"

    id: Mapped[int] = mapped_column(primary_key=True)
    approved_at: Mapped[datetime.datetime] = mapped_column(server_default=func.now())
    package_name: Mapped[str] = mapped_column(nullable=False, index=True)
    package_type: Mapped[PackageTypeDB] = mapped_column(nullable=False)
    package_version: Mapped[str] = mapped_column(nullable=False)

    def to_spec(self) -> str:
        """Render as the ``"<type>::<name>"`` spec string ``PBAllowList.allow_list`` uses."""
        return f"{self.package_type.value}::{self.package_name}"


# ---------------------------------------------------------------------------
# Table creation
# ---------------------------------------------------------------------------


async def create_compose_db(async_engine: AsyncEngine) -> None:
    async with async_engine.begin() as conn:
        await conn.run_sync(ComposeBase.metadata.create_all)
