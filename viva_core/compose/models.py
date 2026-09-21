"""Pydantic models for the compose (process-bigraph) simulation subsystem."""

import datetime
import enum
import hashlib
from collections.abc import Mapping
from enum import StrEnum
from pathlib import Path

import pydantic
from pydantic import Field

from viva_core.compose.container_def import ContainerizationFileRepr
from viva_core.models import ComputeBackend

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class BaseModel(pydantic.BaseModel):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
    """The base of every compose model. (It carried ``as_payload`` and a ``FlexData`` bag, both without a
    caller anywhere; dropped at the move.)"""


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


class ComposeHpcRun(BaseModel):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
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


class EnvWorkerTask(BaseModel):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
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


class BiGraphComputeOutline(BaseModel):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
    module: str
    name: str
    compute_type: BiGraphComputeType
    inputs: str
    outputs: str


class BiGraphCompute(BiGraphComputeOutline):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
    database_id: int


class BiGraphProcess(BiGraphCompute):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
    pass


class BiGraphStep(BiGraphCompute):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
    pass


class PackageOutline(BaseModel):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
    package_type: PackageType
    name: str
    compute: list[BiGraphComputeOutline]

    @staticmethod
    def from_pb_outline(
        pb_outline_json: Mapping[str, object], name: str, package_type: PackageType
    ) -> "PackageOutline":
        compute: list[BiGraphComputeOutline] = []
        for key, compute_type in (("processes", BiGraphComputeType.PROCESS), ("steps", BiGraphComputeType.STEP)):
            entries = pb_outline_json.get(key)
            if not isinstance(entries, list):
                continue
            for entry in entries:
                if not isinstance(entry, Mapping):
                    raise TypeError(f"{key!r} entries must be objects; got {type(entry).__name__}")
                compute.append(BiGraphComputeOutline.model_validate({**entry, "compute_type": compute_type}))
        return PackageOutline(package_type=package_type, name=name, compute=compute)


class RegisteredPackage(BaseModel):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
    database_id: int
    package_type: PackageType
    name: str
    processes: list[BiGraphProcess]
    steps: list[BiGraphStep]


# ---------------------------------------------------------------------------
# Simulators (container-based)
# ---------------------------------------------------------------------------


class ComposeSimulator(BaseModel):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
    singularity_def: ContainerizationFileRepr
    singularity_def_hash: str
    packages: list[RegisteredPackage] | None


class ComposeSimulatorVersion(ComposeSimulator):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
    database_id: int
    created_at: datetime.datetime | None = None


class ComposeRegisteredSimulators(BaseModel):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
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


class ComposeSimulationRequest(BaseModel):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
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
    # A REGISTERED environment by name ("runtime": the core runtime image) instead of a simulator's
    # image. Only the Ray/Batch backend consumes it, and runs the composite as ONE container then.
    environment: str | None = None
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
    # Analysis modules/config to run once this simulation completes, carried in
    # from the compose-run request and persisted on the ComposeSimulation row
    # (unlike num_nodes above) so a later task can chain the analysis job off of
    # it. None preserves today's exact behavior: no analysis is chained. This
    # task only carries and persists the field -- it does not submit any
    # analysis job itself.
    analysis_options: dict[str, object] | None = None


class ComposeDocumentSubmission(BaseModel):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
    """A process-bigraph document submitted inline as JSON (POST body), the
    sibling of ComposeSimulationRequest's file-upload transport -- same
    downstream dispatch, different input shape. Field name/shape mirrors
    env_worker.py's own StateDocument.document for naming consistency across
    this repo's two JSON-body document-submission paths.
    """

    document: dict[str, object] = Field(..., description="The composite document itself, as JSON")
    interval_time: float = 1.0
    batch_submission: bool = False
    simulator_id: int | None = None
    #: Run in a REGISTERED environment instead of a simulator's image: ``"runtime"`` is the core
    #: runtime image, for a composite that needs nothing beyond what process-bigraph ships. It then
    #: runs as ONE container (no Ray cluster). Not with ``simulator_id`` / ``num_nodes`` /
    #: ``analysis_options``, which are all a simulator's.
    environment: str | None = None
    compute_backend: ComputeBackend | None = None
    extra_pip_deps: list[str] | None = None
    # Per-request AWS Batch MNP node count for the Ray backend (item 102). None
    # preserves today's exact behavior: the deploy-wide Settings.ray_num_nodes
    # default. Only the Ray backend's submit_simulation_job consumes it; other
    # backends (e.g. SLURM/HPC) accept and ignore it, mirroring how
    # compute_backend itself is handled above.
    num_nodes: int | None = None
    # See ComposeSimulationRequest.analysis_options above -- same field, JSON-body
    # transport's native shape (a dict, not a stringified Form field).
    analysis_options: dict[str, object] | None = None


class ComposeSimulationResults(BaseModel):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
    path_on_server: Path


class ComposeSimulation(BaseModel):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
    database_id: int
    sim_request: ComposeSimulationRequest
    simulator_version: ComposeSimulatorVersion


class ComposeSubmittedSimulation(BaseModel):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
    database_id: int
    sim_content: ComposeSimulationResults
    simulator_version: ComposeSimulatorVersion
    hpc_run: ComposeHpcRun | None


class PBAllowList(BaseModel):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
    allow_list: list[str]


class ComposeSimulationExperiment(BaseModel):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
    simulation_database_id: int
    simulator_database_id: int
    last_updated: str = Field(default_factory=lambda: str(datetime.datetime.now()))
    metadata: Mapping[str, str] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Worker events (NATS)
# ---------------------------------------------------------------------------


class ComposeWorkerEvent(BaseModel):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
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


class ComposeWorkerEventMessagePayload(BaseModel):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
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


class BiomodelInfo(BaseModel):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
    biomodel_id: str
    metadata: dict[str, object]


class BiomodelsRunRequest(BaseModel):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
    """One request shape for every BioModels run — subsumes the former
    single/batch/audit/regression endpoints. Cardinality comes from
    ``model_ids``/``n_models``; per-model cross-validation comes from listing
    more than one simulator."""

    model_ids: list[str] | None = Field(
        default=None, description="Specific BioModel IDs to run. Mutually exclusive with n_models."
    )
    n_models: int | None = Field(
        default=None, ge=1, le=1000, description="Run the first N BioModels. Ignored if model_ids is set."
    )
    simulators: list[BiomodelSimulator] = Field(
        default_factory=lambda: [BiomodelSimulator.COPASI],
        description=(
            "Simulators to run for each model. A single simulator runs the model on it; "
            "several are wired into one PB document per model for cross-validation "
            "(the former 'audit'/'regression' behavior)."
        ),
    )


class BiomodelsRunResult(BaseModel):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
    submitted: list[ComposeSimulationExperiment]
    failed: list[str] = Field(default_factory=list, description="BioModel IDs that failed to submit.")
    total_requested: int = Field(description="Number of models attempted (len(submitted) + len(failed)).")


def get_singularity_hash(singularity_def_rep: ContainerizationFileRepr) -> str:
    return hashlib.md5(singularity_def_rep.representation.encode("utf-8")).hexdigest()  # noqa: S324
