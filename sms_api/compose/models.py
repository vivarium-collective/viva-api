"""Pydantic models for the compose (process-bigraph) simulation subsystem."""

import datetime
import enum
import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any

from pydantic import BaseModel as _BaseModel
from pydantic import Field

from sms_api.compose.container_def import ContainerizationFileRepr

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@dataclass
class FlexData:
    _data: dict[str, Any] = field(default_factory=dict)

    def __init__(self, **kwargs: Any) -> None:
        self._data = kwargs

    def __getattr__(self, item: str) -> Any:
        return self._data[item]

    def __getitem__(self, item: str) -> Any:
        return self._data[item]

    def keys(self) -> Any:
        return self._data.keys()

    def dict(self) -> dict[str, Any]:
        return self._data


class Payload(FlexData):
    pass


class BaseModel(_BaseModel):
    def as_payload(self) -> Payload:
        serialized = json.loads(self.model_dump_json())
        return Payload(**serialized)


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class ComposeJobType(enum.Enum):
    SIMULATION = "simulation"
    BUILD_CONTAINER = "build_container"


class PackageType(enum.Enum):
    PYPI = "pypi"
    CONDA = "conda"


class BiGraphComputeType(enum.Enum):
    PROCESS = "process"
    STEP = "step"


class ComposeJobStatus(StrEnum):
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


# ---------------------------------------------------------------------------
# HPC job tracking
# ---------------------------------------------------------------------------


class ComposeHpcRun(BaseModel):
    database_id: int
    slurmjobid: int
    # Backend-agnostic job id (AWS Batch/Ray UUIDs); ``job_backend`` tags which backend owns it.
    job_id_ext: str | None = None
    job_backend: str = "slurm"
    correlation_id: str
    job_type: ComposeJobType
    sim_id: int | None
    simulator_id: int | None
    status: ComposeJobStatus | None = None
    start_time: str | None = None
    end_time: str | None = None
    error_message: str | None = None


class BatchProgress(BaseModel):
    """Live progress of a batch (multiseed x multigeneration) compose run.

    Derived purely from the hive-partitioned output the Ray/Batch entrypoint syncs
    to S3 *as the run proceeds* (``…/lineage_seed=<N>/…/generation=<G>/`` partitions,
    written incrementally and s3-synced every ~30 s) — so it needs NO new writer in
    the workload and works for any running compose batch, not just one composite.

    ``lineages``/``generations`` are ``"current:total"`` strings a client renders
    verbatim; ``overall`` is the whole-sweep percent complete
    (``sum(generations_reached) / (n_seeds x n_generations)``), estimated from a bounded
    sample of lineages so the cost is constant regardless of sweep size.
    """

    lineages: str  # "started:total" — lineage seeds that have produced output
    generations: str  # "deepest:total" — max generation reached across lineages
    overall: float  # 0..100 — whole-sweep percent complete (cell-generations)
    time_elapsed: float  # seconds since the run's start_time
    status: ComposeJobStatus | None = None  # coarse job state, for client convenience


# ---------------------------------------------------------------------------
# BiGraph compute registry
# ---------------------------------------------------------------------------


class BiGraphComputeOutline(BaseModel):
    module: str
    name: str
    compute_type: BiGraphComputeType
    inputs: str
    outputs: str


class BiGraphCompute(BiGraphComputeOutline):
    database_id: int


class BiGraphProcess(BiGraphCompute):
    pass


class BiGraphStep(BiGraphCompute):
    pass


class PackageOutline(BaseModel):
    package_type: PackageType
    name: str
    compute: list[BiGraphComputeOutline]

    @staticmethod
    def from_pb_outline(pb_outline_json: dict[str, Any], name: str, package_type: PackageType) -> "PackageOutline":
        compute: list[BiGraphComputeOutline] = []
        if "processes" in pb_outline_json:
            for process in pb_outline_json["processes"]:
                compute.append(BiGraphComputeOutline(compute_type=BiGraphComputeType.PROCESS, **process))
        if "steps" in pb_outline_json:
            for step in pb_outline_json["steps"]:
                compute.append(BiGraphComputeOutline(compute_type=BiGraphComputeType.STEP, **step))
        return PackageOutline(package_type=package_type, name=name, compute=compute)


class RegisteredPackage(BaseModel):
    database_id: int
    package_type: PackageType
    name: str
    processes: list[BiGraphProcess]
    steps: list[BiGraphStep]


# ---------------------------------------------------------------------------
# Simulators (container-based)
# ---------------------------------------------------------------------------


class ComposeSimulator(BaseModel):
    singularity_def: ContainerizationFileRepr
    singularity_def_hash: str
    packages: list[RegisteredPackage] | None


class ComposeSimulatorVersion(ComposeSimulator):
    database_id: int
    created_at: datetime.datetime | None = None


class ComposeRegisteredSimulators(BaseModel):
    versions: list[ComposeSimulatorVersion]
    timestamp: datetime.datetime | None = Field(default_factory=datetime.datetime.now)


# ---------------------------------------------------------------------------
# Simulation request / response
# ---------------------------------------------------------------------------


class SimulationFileType(enum.Enum):
    OMEX = "omex"
    PBG = "pbg"
    SBML = "sbml"

    def get_files_suffix(self) -> str:
        return self.value

    @staticmethod
    def get_file_type(suffix: str) -> "SimulationFileType":
        match suffix:
            case ".omex":
                return SimulationFileType.OMEX
            case ".pbg":
                return SimulationFileType.PBG
            case ".sbml":
                return SimulationFileType.SBML
            case _:
                raise ValueError(f"Unknown simulation file type: {suffix}")


class ComposeSimulationRequest(BaseModel):
    request_file_path: Path
    simulation_file_type: SimulationFileType
    end_time_point: float = 1.0
    is_batch: bool


class ComposeSimulationResults(BaseModel):
    path_on_server: Path


class ComposeSimulation(BaseModel):
    database_id: int
    sim_request: ComposeSimulationRequest
    simulator_version: ComposeSimulatorVersion


class ComposeSubmittedSimulation(BaseModel):
    database_id: int
    sim_content: ComposeSimulationResults
    simulator_version: ComposeSimulatorVersion
    hpc_run: ComposeHpcRun | None


class PBAllowList(BaseModel):
    allow_list: list[str]


# Bootstrap rows for a fresh deployment's ``compose_allow_list`` table (seeded once,
# at startup, iff the table is empty — see ``AllowListDatabaseService.seed_if_empty``).
# Operators curate the table thereafter; this list is not re-applied on restart.
DEFAULT_COMPOSE_ALLOW_LIST: list[str] = [
    "pypi::git+https://github.com/biosimulators/bspil-basico.git@initial_work",
    "pypi::cobra",
    "pypi::tellurium",
    "pypi::copasi-basico",
    "pypi::smoldyn",
    "pypi::numpy",
    "pypi::matplotlib",
    "pypi::scipy",
    "pypi::pb_multiscale_actin",
    "conda::readdy",
]


class ComposeSimulationExperiment(BaseModel):
    simulation_database_id: int
    simulator_database_id: int
    last_updated: str = Field(default_factory=lambda: str(datetime.datetime.now()))
    metadata: Mapping[str, str] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Worker events (NATS)
# ---------------------------------------------------------------------------


class ComposeWorkerEvent(BaseModel):
    database_id: int | None = None
    created_at: str | None = None
    hpcrun_id: int | None = None
    correlation_id: str
    sequence_number: int
    mass: dict[str, float]
    time: float

    @classmethod
    def from_message_payload(cls, payload: "ComposeWorkerEventMessagePayload") -> "ComposeWorkerEvent":
        return cls(
            correlation_id=payload.correlation_id,
            sequence_number=payload.sequence_number,
            mass=payload.mass,
            time=payload.time,
        )


class ComposeWorkerEventMessagePayload(BaseModel):
    correlation_id: str
    sequence_number: int
    time: float
    mass: dict[str, float]


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


class BiomodelSimulator(str, enum.Enum):
    COPASI = "copasi"
    TELLURIUM = "tellurium"


class BiomodelInfo(BaseModel):
    biomodel_id: str
    metadata: dict[str, Any]


class BiomodelsRunRequest(BaseModel):
    model_ids: list[str] | None = Field(
        default=None, description="Specific BioModel IDs to run. Mutually exclusive with n_models."
    )
    n_models: int | None = Field(
        default=None, ge=1, le=50, description="Run the first N BioModels. Ignored if model_ids is set."
    )
    simulator: BiomodelSimulator = Field(
        default=BiomodelSimulator.COPASI, description="Simulator to use for each model."
    )


class BiomodelsRunResult(BaseModel):
    submitted: list[ComposeSimulationExperiment]
    failed: list[str] = Field(default_factory=list, description="BioModel IDs that failed to submit.")


class BiomodelsAuditRequest(BaseModel):
    biomodel_id: str
    simulators: list[BiomodelSimulator] = Field(
        default_factory=lambda: [BiomodelSimulator.COPASI, BiomodelSimulator.TELLURIUM]
    )


class BiomodelsAuditResult(BaseModel):
    experiment: ComposeSimulationExperiment
    simulators_used: list[BiomodelSimulator]


class BiomodelsRegressionRequest(BaseModel):
    n_models: int = Field(default=10, ge=1, le=1000, description="Number of models to run. Ignored if model_ids set.")
    model_ids: list[str] | None = Field(default=None, description="Specific BioModel IDs to run. Overrides n_models.")
    simulators: list[BiomodelSimulator] = Field(
        default_factory=lambda: [BiomodelSimulator.COPASI, BiomodelSimulator.TELLURIUM],
        description="Simulators to wire into each model's PB document.",
    )


class BiomodelsRegressionResult(BaseModel):
    submitted: list[ComposeSimulationExperiment]
    failed: list[str] = Field(default_factory=list, description="BioModel IDs that failed to submit.")
    total_requested: int


def get_singularity_hash(singularity_def_rep: ContainerizationFileRepr) -> str:
    return hashlib.md5(singularity_def_rep.representation.encode("utf-8")).hexdigest()  # noqa: S324
