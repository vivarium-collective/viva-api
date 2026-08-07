import logging
import pprint
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, ConfigDict, Field

if TYPE_CHECKING:
    from viva_api.common.models import JobStatus

logger = logging.getLogger(__name__)


class SlurmJob(BaseModel):
    #                                 --squeue--   --sacct--   --scontrol--
    job_id: int  #                       %i          jobid       JobId
    name: str  #                         %j          jobname     JobName
    account: str  #                      %a          account     Account
    user_name: str  #                    %u          user        UserId
    job_state: str  #                    %T          state       JobState
    start_time: str | None = None  #              start       StartTime
    end_time: str | None = None  #                end         EndTime
    elapsed: str | None = None  #                 elapsed     RunTime
    exit_code: str | None = None  #               exitcode    ExitCode
    reason: str | None = None  #                              Reason (scontrol only)

    model_config = ConfigDict(
        populate_by_name=True,
        validate_assignment=True,
        protected_namespaces=(),
    )

    def to_str(self) -> str:
        """Returns the string representation of the model using alias"""
        return pprint.pformat(self.model_dump(by_alias=True))

    def to_json(self) -> str:
        """Returns the JSON representation of the model using alias"""
        return self.model_dump_json(by_alias=True, exclude_unset=True)

    def is_done(self) -> bool:
        """Check if the job is done based on its state."""
        if not self.job_state:
            return False
        return self.job_state.upper() in ["COMPLETED", "FAILED"]

    def get_job_status(self) -> "JobStatus":
        """Map SLURM job state to JobStatus enum."""
        from viva_api.common.models import JobStatus

        state_upper = self.job_state.upper()
        if state_upper in ("PENDING", "PD"):
            return JobStatus.PENDING
        elif state_upper in ("RUNNING", "R"):
            return JobStatus.RUNNING
        elif state_upper in ("COMPLETED", "CD"):
            return JobStatus.COMPLETED
        elif state_upper in ("FAILED", "F", "CANCELLED", "CA", "TIMEOUT", "TO", "NODE_FAIL", "NF"):
            return JobStatus.FAILED
        else:
            logger.warning(f"Unknown SLURM state '{self.job_state}', returning UNKNOWN")
            return JobStatus.UNKNOWN

    @staticmethod
    def get_sacct_format_string() -> str:
        return "jobid,jobname,account,user,state,start,end,elapsed,exitcode"

    @classmethod
    def from_sacct_formatted_output(cls, line: str) -> "SlurmJob":
        # Split the line by delimiter
        fields = line.strip().split("|")
        # Map fields to model attributes
        return cls(
            job_id=int(fields[0]),
            name=fields[1],
            account=fields[2],
            user_name=fields[3],
            job_state=fields[4],
            start_time=fields[5],
            end_time=fields[6],
            elapsed=fields[7],
            exit_code=fields[8],
        )

    @staticmethod
    def get_squeue_format_string() -> str:
        return "%i|%j|%a|%u|%T"

    @classmethod
    def from_squeue_formatted_output(cls, line: str) -> "SlurmJob":
        # Split the line by delimiter
        fields = line.strip().split("|")
        # Map fields to model attributes
        return cls(
            job_id=int(fields[0]),
            name=fields[1],
            account=fields[2],
            user_name=fields[3],
            job_state=fields[4],
        )

    @classmethod
    def from_scontrol_output(cls, output: str) -> "SlurmJob":
        """Parse scontrol show job output into a SlurmJob.

        scontrol output format is key=value pairs, some on same line, some on new lines:
            JobId=12345 JobName=myjob
               UserId=user(1000) GroupId=group(1000)
               Account=myaccount QOS=normal
               JobState=RUNNING Reason=None
               StartTime=2024-01-15T10:30:00 EndTime=Unknown
               ...
        """
        # Flatten multi-line output and split on whitespace
        # Then parse key=value pairs
        data: dict[str, str] = {}
        # Replace newlines with spaces and split
        tokens = output.replace("\n", " ").split()
        for token in tokens:
            if "=" in token:
                key, _, value = token.partition("=")
                data[key] = value

        # Extract user name from UserId format "user(uid)"
        user_id = data.get("UserId", "")
        user_name = user_id.split("(")[0] if "(" in user_id else user_id

        # Extract reason, treating "None" as no reason
        reason = data.get("Reason")
        if reason == "None":
            reason = None

        job_state = data.get("JobState", "UNKNOWN")
        # Only accept EndTime for completed jobs - running jobs report scheduled end time
        terminal_states = {"COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "NODE_FAIL", "PREEMPTED", "OUT_OF_MEMORY"}
        end_time_raw = data.get("EndTime")
        end_time = end_time_raw if end_time_raw and end_time_raw != "Unknown" and job_state in terminal_states else None

        return cls(
            job_id=int(data.get("JobId", "0")),
            name=data.get("JobName", ""),
            account=data.get("Account", ""),
            user_name=user_name,
            job_state=job_state,
            start_time=data.get("StartTime"),
            end_time=end_time,
            elapsed=data.get("RunTime"),
            exit_code=data.get("ExitCode"),
            reason=reason,
        )


# =============================================================================
# Nextflow Weblog Event Models
# =============================================================================


class NextflowEventType(str, Enum):
    """Nextflow weblog event types."""

    STARTED = "started"
    COMPLETED = "completed"
    PROCESS_SUBMITTED = "process_submitted"
    PROCESS_STARTED = "process_started"
    PROCESS_COMPLETED = "process_completed"


class NextflowTraceStatus(str, Enum):
    """Nextflow trace task status values."""

    SUBMITTED = "SUBMITTED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CACHED = "CACHED"
    ABORTED = "ABORTED"


class NextflowDateTime(BaseModel):
    """Nextflow's custom datetime representation."""

    model_config = ConfigDict(populate_by_name=True)

    day_of_month: int = Field(alias="dayOfMonth")
    day_of_week: str = Field(alias="dayOfWeek")
    day_of_year: int = Field(alias="dayOfYear")
    hour: int
    minute: int
    second: int
    nano: int
    month: str
    month_value: int = Field(alias="monthValue")
    year: int
    offset: dict[str, Any] | None = None

    def to_datetime(self) -> datetime:
        """Convert to Python datetime."""
        return datetime(
            year=self.year,
            month=self.month_value,
            day=self.day_of_month,
            hour=self.hour,
            minute=self.minute,
            second=self.second,
            microsecond=self.nano // 1000,
        )


class NextflowVersion(BaseModel):
    """Nextflow version information."""

    version: str
    build: int
    timestamp: str
    enable: dict[str, Any] = Field(default_factory=dict)


class NextflowWave(BaseModel):
    """Nextflow Wave configuration."""

    enabled: bool = False


class NextflowFusion(BaseModel):
    """Nextflow Fusion configuration."""

    enabled: bool = False
    version: str | None = None


class NextflowProcessStats(BaseModel):
    """Statistics for a single Nextflow process."""

    model_config = ConfigDict(populate_by_name=True)

    index: int
    name: str
    hash: str | None = None
    task_name: str | None = Field(default=None, alias="taskName")
    pending: int = 0
    submitted: int = 0
    running: int = 0
    succeeded: int = 0
    cached: int = 0
    failed: int = 0
    aborted: int = 0
    stored: int = 0
    ignored: int = 0
    retries: int = 0
    terminated: bool = False
    errored: bool = False
    load_cpus: int = Field(default=0, alias="loadCpus")
    load_memory: int = Field(default=0, alias="loadMemory")
    peak_running: int = Field(default=0, alias="peakRunning")
    peak_cpus: int = Field(default=0, alias="peakCpus")
    peak_memory: int = Field(default=0, alias="peakMemory")
    completed_count: int = Field(default=0, alias="completedCount")
    total_count: int = Field(default=0, alias="totalCount")


class NextflowStats(BaseModel):
    """Nextflow workflow statistics."""

    model_config = ConfigDict(populate_by_name=True)

    change_timestamp: int = Field(default=0, alias="changeTimestamp")
    succeeded_count: int = Field(default=0, alias="succeededCount")
    cached_count: int = Field(default=0, alias="cachedCount")
    failed_count: int = Field(default=0, alias="failedCount")
    ignored_count: int = Field(default=0, alias="ignoredCount")
    pending_count: int = Field(default=0, alias="pendingCount")
    submitted_count: int = Field(default=0, alias="submittedCount")
    running_count: int = Field(default=0, alias="runningCount")
    retries_count: int = Field(default=0, alias="retriesCount")
    aborted_count: int = Field(default=0, alias="abortedCount")
    load_cpus: int = Field(default=0, alias="loadCpus")
    load_memory: int = Field(default=0, alias="loadMemory")
    peak_running: int = Field(default=0, alias="peakRunning")
    peak_cpus: int = Field(default=0, alias="peakCpus")
    peak_memory: int = Field(default=0, alias="peakMemory")
    cached_duration: int = Field(default=0, alias="cachedDuration")
    cached_pct: float = Field(default=0.0, alias="cachedPct")
    failed_duration: int = Field(default=0, alias="failedDuration")
    succeed_count: int = Field(default=0, alias="succeedCount")
    succeed_duration: int = Field(default=0, alias="succeedDuration")
    succeed_pct: float = Field(default=0.0, alias="succeedPct")
    total_count: int = Field(default=0, alias="totalCount")
    progress_length: int = Field(default=0, alias="progressLength")
    processes: list[NextflowProcessStats] = Field(default_factory=list)


class NextflowManifest(BaseModel):
    """Nextflow workflow manifest information."""

    model_config = ConfigDict(populate_by_name=True)

    author: str | None = None
    contributors: list[Any] = Field(default_factory=list)
    default_branch: str | None = Field(default=None, alias="defaultBranch")
    description: str | None = None
    docs_url: str | None = Field(default=None, alias="docsUrl")
    doi: str | None = None
    gitmodules: str | None = None
    home_page: str | None = Field(default=None, alias="homePage")
    icon: str | None = None
    license: str | None = None
    main_script: str = Field(default="main.nf", alias="mainScript")
    name: str | None = None
    nextflow_version: str | None = Field(default=None, alias="nextflowVersion")
    organization: str | None = None
    recurse_submodules: bool = Field(default=False, alias="recurseSubmodules")
    version: str | None = None


class NextflowWorkflow(BaseModel):
    """Nextflow workflow metadata."""

    model_config = ConfigDict(populate_by_name=True)

    run_name: str = Field(alias="runName")
    script_id: str = Field(alias="scriptId")
    script_file: str = Field(alias="scriptFile")
    script_name: str = Field(alias="scriptName")
    repository: str | None = None
    commit_id: str | None = Field(default=None, alias="commitId")
    revision: str | None = None
    start: NextflowDateTime | None = None
    complete: NextflowDateTime | None = None
    duration: int | None = None
    container: dict[str, Any] = Field(default_factory=dict)
    command_line: str = Field(alias="commandLine")
    nextflow: NextflowVersion
    success: bool
    project_dir: str = Field(alias="projectDir")
    project_name: str = Field(alias="projectName")
    launch_dir: str = Field(alias="launchDir")
    output_dir: str = Field(alias="outputDir")
    work_dir: str = Field(alias="workDir")
    home_dir: str = Field(alias="homeDir")
    user_name: str = Field(alias="userName")
    exit_status: int | None = Field(default=None, alias="exitStatus")
    error_message: str | None = Field(default=None, alias="errorMessage")
    error_report: str | None = Field(default=None, alias="errorReport")
    profile: str
    session_id: str = Field(alias="sessionId")
    resume: bool = False
    stub_run: bool = Field(default=False, alias="stubRun")
    preview: bool = False
    container_engine: str | None = Field(default=None, alias="containerEngine")
    wave: NextflowWave = Field(default_factory=NextflowWave)
    fusion: NextflowFusion = Field(default_factory=NextflowFusion)
    config_files: list[str] = Field(default_factory=list, alias="configFiles")
    stats: NextflowStats = Field(default_factory=NextflowStats)
    manifest: NextflowManifest = Field(default_factory=NextflowManifest)
    fail_on_ignore: bool = Field(default=False, alias="failOnIgnore")


class NextflowMetadata(BaseModel):
    """Nextflow metadata payload for started/completed events."""

    parameters: dict[str, Any] = Field(default_factory=dict)
    workflow: NextflowWorkflow


class NextflowTrace(BaseModel):
    """Nextflow trace payload for process events."""

    model_config = ConfigDict(populate_by_name=True)

    task_id: int = Field(alias="task_id")
    status: NextflowTraceStatus
    hash: str
    name: str
    exit: int
    submit: int
    start: int
    process: str
    tag: str | None = None
    module: list[str] = Field(default_factory=list)
    container: str | None = None
    attempt: int = 1
    script: str
    scratch: str | None = None
    workdir: str
    queue: str | None = None
    cpus: int = 1
    memory: int | None = None
    disk: int | None = None
    time: int | None = None
    env: str | None = None
    native_id: int | None = None
    error_action: str | None = None
    complete: int | None = None
    duration: int | None = None
    realtime: int | None = None
    percent_cpu: float | None = Field(default=None, alias="%cpu")
    cpu_model: str | None = None
    rchar: int | None = None
    wchar: int | None = None
    syscr: int | None = None
    syscw: int | None = None
    read_bytes: int | None = None
    write_bytes: int | None = None
    percent_mem: float | None = Field(default=None, alias="%mem")
    vmem: int | None = None
    rss: int | None = None
    peak_vmem: int | None = None
    peak_rss: int | None = None
    vol_ctxt: int | None = None
    inv_ctxt: int | None = None

    def is_completed(self) -> bool:
        """Check if the task has completed (successfully or failed)."""
        return self.status in (
            NextflowTraceStatus.COMPLETED,
            NextflowTraceStatus.FAILED,
            NextflowTraceStatus.CACHED,
            NextflowTraceStatus.ABORTED,
        )


class NextflowMetadataEvent(BaseModel):
    """Nextflow weblog event containing workflow metadata (started/completed)."""

    model_config = ConfigDict(populate_by_name=True)

    run_id: str = Field(alias="runId")
    event: Literal["started", "completed"]
    run_name: str = Field(alias="runName")
    utc_time: str = Field(alias="utcTime")
    metadata: NextflowMetadata


class NextflowTraceEvent(BaseModel):
    """Nextflow weblog event containing task trace data."""

    model_config = ConfigDict(populate_by_name=True)

    run_id: str = Field(alias="runId")
    event: Literal["process_submitted", "process_started", "process_completed"]
    run_name: str = Field(alias="runName")
    utc_time: str = Field(alias="utcTime")
    trace: NextflowTrace


NextflowEvent = NextflowMetadataEvent | NextflowTraceEvent


def parse_nextflow_event(data: dict[str, Any]) -> NextflowEvent:
    """Parse a Nextflow weblog event from a dictionary.

    Args:
        data: Dictionary containing the event data (parsed from JSON)

    Returns:
        Either a NextflowMetadataEvent or NextflowTraceEvent

    Raises:
        ValueError: If the event type is unknown
    """
    event_type = data.get("event")
    if event_type in ("started", "completed"):
        return NextflowMetadataEvent.model_validate(data)
    elif event_type in ("process_submitted", "process_started", "process_completed"):
        return NextflowTraceEvent.model_validate(data)
    else:
        raise ValueError(f"Unknown Nextflow event type: {event_type}")
