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

from viva_api.compose.container_def import ContainerizationFileRepr
from viva_api.config import ComputeBackend

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


class EnvWorkerTask(BaseModel):
    """One env-worker method call, recorded durably (plan §E option (e)).

    ``created_by`` is who CLAIMED to submit it, and is None on every deployment
    that has no identity-setting proxy in front of it -- which is most of them.
    It is not verified; see viva_api/api/auth.py for what that does and does not
    mean.
    """

    database_id: int
    job_name: str
    method: str
    params: dict[str, object] | None = None
    status: ComposeJobStatus
    result: object | None = None
    error_message: str | None = None
    created_at: str | None = None
    started_at: str | None = None
    ended_at: str | None = None
    created_by: str | None = None
    correlation_id: str


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
    # Optional per-commit workspace build to run this document against, resolved
    # against the LEGACY simulator registry (viva_api.simulation.database_service,
    # the same one POST /api/v1/simulations already uses) -- distinct from this
    # module's own ComposeSimulatorVersion (a container-def identity computed from
    # the uploaded document + extra_pip_deps, tracked on every compose backend
    # regardless of this field). None preserves today's exact behavior: one static
    # deploy-wide image (COMPOSE_RAY_IMAGE_TAG). Only the Ray backend consumes it.
    simulator_id: int | None = None
    # Which registered ComposeSimulationService to dispatch to (item 98). None
    # preserves today's exact behavior: the deployment's single default service.
    # Only values actually registered for compose (RAY/SLURM as of this field's
    # introduction -- see _init_compose_subsystem) are honored; requesting an
    # unregistered backend fails loud rather than silently substituting the
    # default (the class of bug viva-api#353 flagged as costing real debugging
    # time on this exact deployment).
    compute_backend: ComputeBackend | None = None
    # Per-request AWS Batch MNP node count for the Ray backend (item 102). None
    # preserves today's exact behavior: the deploy-wide Settings.ray_num_nodes
    # default. Read directly off this object by ComposeSimulationServiceRay --
    # never persisted (insert_simulation stores only experiment_id/simulator_id/
    # document; ComposeSimulation.sim_request is the same in-memory object the
    # whole way through the background dispatch), so no DB migration needed.
    num_nodes: int | None = None


class ComposeDocumentSubmission(BaseModel):
    """A process-bigraph document submitted inline as JSON (POST body), the
    sibling of ComposeSimulationRequest's file-upload transport -- same
    downstream dispatch, different input shape. Field name/shape mirrors
    env_worker.py's own StateDocument.document for naming consistency across
    this repo's two JSON-body document-submission paths.
    """

    document: dict[str, Any] = Field(..., description="The composite document itself, as JSON")
    interval_time: float = 1.0
    batch_submission: bool = False
    simulator_id: int | None = None
    compute_backend: ComputeBackend | None = None
    extra_pip_deps: list[str] | None = None
    # Per-request AWS Batch MNP node count for the Ray backend (item 102). None
    # preserves today's exact behavior: the deploy-wide Settings.ray_num_nodes
    # default. Only the Ray backend's submit_simulation_job consumes it; other
    # backends (e.g. SLURM/HPC) accept and ignore it, mirroring how
    # compute_backend itself is handled above.
    num_nodes: int | None = None


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
    # vivarium-collective git origins — the framework + workspace deps the
    # vivarium-workbench pinned-run path ships as extra_pip_deps for a v2ecoli
    # composite (v2ecoli itself + its process-bigraph stack, pinned per the
    # workspace uv.lock). Base URLs (no @commit): the allow-list check is a
    # substring match, so these cover any pinned commit. Without them the
    # workbench's POST /compose/v1/simulation/run is rejected 403.
    "pypi::git+https://github.com/vivarium-collective/v2ecoli.git",
    "pypi::git+https://github.com/vivarium-collective/bigraph-schema.git",
    "pypi::git+https://github.com/vivarium-collective/pbg-emitters.git",
    "pypi::git+https://github.com/vivarium-collective/pbg-superpowers.git",
    "pypi::git+https://github.com/vivarium-collective/process-bigraph.git",
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
