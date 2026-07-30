import json
import pathlib
import random
from typing import Any, ParamSpec, TypeVar

from pydantic import BaseModel, ConfigDict

from sms_api.common import StrEnumBase
from sms_api.common.models import DataId, JobStatus
from sms_api.config import Settings, get_settings

MAX_ANALYSIS_CPUS = 3

# Predetermined samplings offered by the analysis-results endpoints (see
# analysis-results-design.md). POST only accepts an n_tp from this set.
AVAILABLE_NTP: list[int] = [10, 50, 100]


def infer_n_tp_from_tsv(tsv_text: str) -> int:
    """Infer ``n_tp`` (number of timepoint columns) from a ptools analysis TSV.

    Mirrors ``AnalysisServiceSlurm._verify_result``: ``n_tp`` is the count of
    header columns whose name starts with ``"t"``. Reads only the header line, so
    it is cheap and has no heavy dependencies.
    """
    header = tsv_text.split("\n", 1)[0]
    return sum(1 for col in header.split("\t") if col.startswith("t"))


### -- analyses -- ###


P = ParamSpec("P")

R = TypeVar("R")


class TsvOutputFileRequest(BaseModel):
    # analysis_id: int
    filename: str
    variant: int = 0
    lineage_seed: int | None = None
    generation: int | None = None
    agent_id: str | None = None


class OutputFileMetadata(BaseModel):
    filename: str
    variant: int = 0
    lineage_seed: int | None = None
    generation: int | None = None
    agent_id: str | None = None
    content: str | None = None

    def model_post_init(self, *args: Any) -> None:
        trim_attributes(self, OutputFileMetadata)


class TsvOutputFile(OutputFileMetadata):
    pass


class OutputFile(BaseModel):
    name: str
    content: str


class AnalysisModuleConfig(BaseModel):
    name: str
    files: list[OutputFileMetadata] | None = None
    model_config = ConfigDict(extra="allow")

    def to_dict(self) -> dict[str, Any]:
        return {self.name: {}}


class AnalysisDomain(StrEnumBase):
    MULTIEXPERIMENT = "multiexperiment"
    MULTIVARIANT = "multivariant"
    MULTISEED = "multiseed"
    MULTIGENERATION = "multigeneration"
    MULTIDAUGHTER = "multidaughter"
    SINGLE = "single"


class PtoolsAnalysisType(StrEnumBase):
    REACTIONS = "ptools_rxns"
    RNA = "ptools_rna"
    PROTEINS = "ptools_proteins"


class PtoolsAnalysisConfig(BaseModel):
    """Configuration for a single ptools analysis module.

    :param name: Analysis module type name
        (one of ``"ptools_rxns"``, ``"ptools_rna"``, ``"ptools_proteins"``).
    :param n_tp: Number of timepoints/columns in the output TSV.
    :param files: Specification of files requested to be returned
        with the completion of the analysis.

    Generation/seed filtering is handled at the request level
    (``ExperimentAnalysisRequest.generation_start/end/seeds``), not per-module.
    Fully supported for ``single`` analyses.  Aggregated types
    (``multigeneration``, ``multiseed``) do not currently respect these
    filters due to a known vEcoli limitation.
    """

    name: str = PtoolsAnalysisType.REACTIONS.value
    n_tp: int = 8
    variant: int = 0
    generation: int | None = None
    lineage_seed: int | None = None
    agent_id: int | None = None
    files: list[OutputFileMetadata] | None = None

    def model_post_init(self, context: Any, /) -> None:
        self.name = self.name or PtoolsAnalysisType.REACTIONS
        for attrname in list(PtoolsAnalysisConfig.model_fields.keys()):
            if getattr(self, attrname, None) is None:
                delattr(self, attrname)

    def to_dict(self) -> dict[str, dict[str, int]]:
        return {self.name: {"n_tp": self.n_tp}}


class AnalysisConfigOptions(BaseModel):
    model_config = ConfigDict(extra="allow")
    experiment_id: list[str]
    # variant_data_dir: list[str] | None = None
    # validation_data_path: list[str] | None = None
    # outdir: str | None = None
    # cpus: int = 3
    # single: dict[str, Any] = {}
    # multidaughter: dict[str, Any] = {}
    # multigeneration: dict[str, dict[str, Any]] = {}
    # multiseed: dict[str, dict[str, Any]] = {}
    # multivariant: dict[str, dict[str, Any]] = {}
    # multiexperiment: dict[str, Any] = {}


class AnalysisConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    analysis_options: AnalysisConfigOptions
    # emitter_arg: dict[str, str] = Field(default={"out_dir": ""})

    @classmethod
    def from_file(cls, fp: pathlib.Path, config_id: str | None = None) -> "AnalysisConfig":
        filepath = fp
        with open(filepath) as f:
            conf = json.load(f)
        options = AnalysisConfigOptions(**conf["analysis_options"])
        return cls(
            analysis_options=options,
            # emitter_arg=conf["emitter_arg"]
        )

    @classmethod
    def from_request(cls, request: "ExperimentAnalysisRequest", analysis_name: str) -> "AnalysisConfig":
        # simulation_outdir = get_settings().simulation_outdir
        # output_dir = simulation_outdir.remote_path / request.experiment_id

        options = AnalysisConfigOptions(
            experiment_id=[request.experiment_id],
            # variant_data_dir=[str(output_dir / "variant_sim_data")],
            # validation_data_path=[str(output_dir / "parca/kb/validationData.cPickle")],
            # outdir=str(output_dir.parent / analysis_name),
            # single=dict_options(request.single),
            # multidaughter=dict_options(request.multidaughter),
            # multigeneration=dict_options(request.multigeneration),
            # multiexperiment=dict_options(request.multiexperiment),
            # multivariant=dict_options(request.multivariant),
            # multiseed=dict_options(request.multiseed),
        )
        # emitter_arg = {"out_dir": str(output_dir.parent)}
        return cls(
            analysis_options=options,
            # emitter_arg=emitter_arg
        )


class ExperimentAnalysisRequest(BaseModel):
    """Request body for the ``POST /analyses`` (ptools) endpoint.

    Top-level ``generation_start``, ``generation_end``, and ``seeds`` are
    fully supported for ``single`` analyses — they restrict which simulation
    data rows are returned, with metadata identifying each partition.

    For aggregated types (``multigeneration``, ``multiseed``), the filters are
    passed to vEcoli but not currently applied to the per-subset data query
    (known vEcoli limitation).  Use ``single`` with filters and aggregate
    client-side as a workaround.

    Per-module params (``n_tp``, ``time_unit``, …) are set inside each
    ``PtoolsAnalysisConfig`` entry and only affect the module they belong to.
    """

    experiment_id: str
    generation_start: int | None = None
    generation_end: int | None = None
    seeds: list[int] | None = None
    single: list[AnalysisModuleConfig | PtoolsAnalysisConfig] | None = None
    multidaughter: list[AnalysisModuleConfig | PtoolsAnalysisConfig] | None = None
    multigeneration: list[AnalysisModuleConfig | PtoolsAnalysisConfig] | None = None
    multiseed: list[AnalysisModuleConfig | PtoolsAnalysisConfig] | None = None
    multivariant: list[AnalysisModuleConfig | PtoolsAnalysisConfig] | None = None
    multiexperiment: list[AnalysisModuleConfig | PtoolsAnalysisConfig] | None = None

    def to_config(self, analysis_name: str | DataId, env: Settings) -> AnalysisConfig:
        """
        Convert a request to a vecoli-compliant, serializable AnalysisConfig.

        :param analysis_name: for the value of
            analysis_options.outdir in HPC: <env.analysis_outdir.remote_path> / analysis_name
        :param env: Settings instance which parameterizes HPC paths
        :return: ``AnalysisConfig``
        """
        if env is None:
            env = get_settings()
        if isinstance(analysis_name, DataId):
            analysis_name = analysis_name.label

        # simulation_outdir = env.hpc_sim_base_path
        # experiment_outdir = str(simulation_outdir / self.experiment_id)
        experiment_outdir = env.hpc_sim_base_path / self.experiment_id
        options = AnalysisConfigOptions(  # type: ignore[call-arg]
            experiment_id=[self.experiment_id],
            variant_data_dir=[f"{experiment_outdir!s}/variant_sim_data"],
            validation_data_path=[f"{experiment_outdir!s}/parca/kb/validationData.cPickle"],
            outdir=f"{env.analysis_outdir.remote_path!s}/{analysis_name}",
            cpus=MAX_ANALYSIS_CPUS,
            single=dict_options(self.single),
            multidaughter=dict_options(self.multidaughter),
            multigeneration=dict_options(self.multigeneration),
            multiexperiment=dict_options(self.multiexperiment),
            multivariant=dict_options(self.multivariant),
            multiseed=dict_options(self.multiseed),
        )
        emitter_arg = {"out_dir": str(experiment_outdir)}
        config = AnalysisConfig(analysis_options=options, emitter_arg=emitter_arg)  # type: ignore[call-arg]

        # DuckDB filters go inside analysis_options (where vEcoli reads them).
        # vEcoli's analysis.py does config[key] (not .get()), so all filter keys
        # must be present even if None to avoid KeyError.
        for key in ("variant", "generation", "agent_id"):
            if not hasattr(options, key):
                setattr(options, key, None)
        # generation_range is [start, end) (exclusive end, per Python range()).
        if self.generation_start is not None or self.generation_end is not None:
            start = self.generation_start if self.generation_start is not None else 0
            # vEcoli's range() is exclusive on end, so +1 to include the end generation
            end = (self.generation_end + 1) if self.generation_end is not None else 1000
            options.generation_range = [start, end]  # type: ignore[attr-defined]
        if self.seeds is not None:
            options.lineage_seed = self.seeds  # type: ignore[attr-defined]

        return config

    @property
    def requested(self) -> dict[str, list[PtoolsAnalysisConfig]]:
        requested = {}
        for domain in AnalysisDomain.to_list():
            domain_requests = getattr(self, domain, None)
            if domain_requests is not None:
                # requested += domain_requests
                requested[domain] = domain_requests
        return requested


class ExperimentAnalysisDTO(BaseModel):
    """DTO returned by /analyses .. endpoints

    Attributes:
        database_id: (``int``) unique identifier of analysis record.
        name: (``str``) analysis name.
        config: (``AnalysisConfig``) data model whose serialized format
            represents a valid analysis config ingestible by vEcoli.
        job_name: (``str | None``) SLURM analysis job name referenced by sbatch directives.
        job_id: (``int | None``) SLURM analysis job id generated by the ``sbatch`` evocation.
            Defaults to ``None`` (such that this object can be partially instantiated).
    """

    database_id: int
    name: str
    config: AnalysisConfig
    last_updated: str
    job_name: str | None = None
    job_id: int | None = None
    # --- query/result fields (populated for the Batch/S3 analysis-result flow) ---
    experiment_id: str | None = None
    n_tp: int | None = None
    status: JobStatus | None = None
    result_uri: str | None = None
    simulation_id: int | None = None
    backend: str | None = None
    error_message: str | None = None
    job_id_ext: str | None = None  # K8s job name (Ray-native standalone analysis)


class AnalysisRun(BaseModel):
    id: int
    status: JobStatus
    job_id: int | None = None
    error_log: str | None = None


class AnalysisJobFailedException(Exception):
    """Exception raised when an analysis SLURM job fails."""

    def __init__(self, run: AnalysisRun, message: str | None = None):
        self.run = run
        self.message = message or f"Analysis job {run.job_id} failed with status: {run.status}"
        super().__init__(self.message)

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": "analysis_job_failed",
            "message": self.message,
            "job_id": self.run.job_id,
            "database_id": self.run.id,
            "status": str(self.run.status),
            "error_log": self.run.error_log,
        }


class AnalysisStatus(BaseModel):
    database_id: int
    status: JobStatus


def trim_attributes(instance: BaseModel, cls: type[BaseModel]) -> None:
    for attrname in list(cls.model_fields.keys()):
        if "analysis_type" not in attrname:
            attr = getattr(instance, attrname, None)
            if attr is None or attr == ["string"]:
                delattr(instance, attrname)
            if isinstance(attr, list | dict) and not len(attr):
                delattr(instance, attrname)


def dict_options(items: list[AnalysisModuleConfig | PtoolsAnalysisConfig] | None) -> dict[str, Any]:
    options: dict[str, Any] = {}
    if items is not None:
        for item in items:
            options.update(item.to_dict())
    return options


class JobId(int):
    start: int = 10**11
    end: int = 10**15

    @classmethod
    def new(cls) -> "JobId":
        value = random.randint(JobId.start, JobId.end)
        return JobId(value)
