import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from sms_api.common.messaging.messaging_service import MessagingService
from sms_api.common.messaging.messaging_service_redis import MessagingServiceRedis
from sms_api.common.models import SSHTarget
from sms_api.common.ssh.ssh_service import SSHSessionService
from sms_api.common.storage.file_service import FileService
from sms_api.common.storage.file_service_gcs import FileServiceGCS
from sms_api.common.storage.file_service_qumulo_s3 import FileServiceQumuloS3
from sms_api.common.storage.file_service_s3 import FileServiceS3
from sms_api.config import ComputeBackend, Settings, get_job_backend, get_settings
from sms_api.log_config import setup_logging
from sms_api.simulation.database_service import DatabaseService, DatabaseServiceSQL
from sms_api.simulation.tables_orm import create_db

if TYPE_CHECKING:
    from sms_api.common.models import JobId
    from sms_api.simulation.job_scheduler import JobScheduler
    from sms_api.simulation.simulation_service import SimulationService

logger = logging.getLogger(__name__)
setup_logging(logger)


def verify_service(service: "DatabaseService | SimulationService | None") -> None:
    if service is None:
        logger.error(f"{service.__module__} is not initialized")
        raise HTTPException(status_code=500, detail=f"{service.__module__} is not initialized")


# ------ file service (standalone or pytest) ------

global_file_service: FileService | None = None


def set_file_service(file_service: FileService | None) -> None:
    global global_file_service
    global_file_service = file_service


def get_file_service() -> FileService | None:
    global global_file_service
    return global_file_service


# ------- sqlalchemy database service (standalone or pytest) ------

global_postgres_engine: AsyncEngine | None = None


def set_postgres_engine(engine: AsyncEngine | None) -> None:
    global global_postgres_engine
    global_postgres_engine = engine


def get_postgres_engine() -> AsyncEngine | None:
    global global_postgres_engine
    return global_postgres_engine


# ------- simulation database service (standalone or pytest) ------

global_database_service: DatabaseService | None = None


def set_database_service(database_service: DatabaseService | None) -> None:
    global global_database_service
    global_database_service = database_service


def get_database_service() -> DatabaseService | None:
    global global_database_service
    return global_database_service


# ------- simulation service (standalone or pytest) ------

global_simulation_service: "SimulationService | None" = None
# Per-backend registry so ONE deployment can serve multiple backends (e.g. vecoli on Batch
# AND v2ecoli on Ray). `global_simulation_service` remains the DEFAULT (COMPUTE_BACKEND).
global_simulation_services: "dict[ComputeBackend, SimulationService]" = {}


def set_simulation_service(simulation_service: "SimulationService | None") -> None:
    global global_simulation_service
    global_simulation_service = simulation_service


def set_simulation_service_registry(registry: "dict[ComputeBackend, SimulationService]") -> None:
    global global_simulation_services
    global_simulation_services = registry


def get_simulation_service() -> "SimulationService | None":
    """The default backend's service (COMPUTE_BACKEND). Prefer the *_for_* helpers below."""
    global global_simulation_service
    return global_simulation_service


def get_simulation_service_for_backend(backend: "ComputeBackend") -> "SimulationService | None":
    """The service for a specific backend, or the default if that backend isn't configured."""
    return global_simulation_services.get(backend, global_simulation_service)


def get_simulation_service_for_repo(repo_url: str) -> "SimulationService | None":
    """Resolve the service from a simulator's repo (v2ecoli→Ray, vEcoli→Batch); default otherwise."""
    from sms_api.config import compute_backend_for_repo

    backend = compute_backend_for_repo(repo_url)
    if backend is not None and backend in global_simulation_services:
        return global_simulation_services[backend]
    return global_simulation_service


def get_simulation_service_for_job(job_id: "JobId") -> "SimulationService | None":
    """Resolve the service that owns a run, by its JobId.backend.

    LOCAL (image-build) jobs and unknown backends fall back to the default service — safe
    because every service shares ONE LocalTaskService, so any can resolve a LOCAL job.
    """
    from sms_api.common.models import JobBackend

    mapping = {
        JobBackend.SLURM: ComputeBackend.SLURM,
        JobBackend.K8S: ComputeBackend.BATCH,
        JobBackend.RAY: ComputeBackend.RAY,
    }
    backend = mapping.get(job_id.backend)
    if backend is not None and backend in global_simulation_services:
        return global_simulation_services[backend]
    return global_simulation_service


# ------ job scheduler (standalone) -----------------------------

global_job_scheduler: "JobScheduler | None" = None


def set_job_scheduler(job_scheduler: "JobScheduler | None") -> None:
    global global_job_scheduler
    global_job_scheduler = job_scheduler


def get_job_scheduler() -> "JobScheduler | None":
    global global_job_scheduler
    return global_job_scheduler


# ------ messaging/cache service (modular standalone: new/arbitrary channels ----

global_messaging_service: MessagingService | None = None


def set_messaging_service(service: MessagingService | None) -> None:
    global global_messaging_service
    global_job_scheduler = service  # noqa: F841


def get_messaging_service() -> MessagingService | None:
    global global_messaging_service
    return global_messaging_service


# ------ SSH session service (singleton) ------

_ssh_services: dict[SSHTarget, SSHSessionService] = {}


def set_ssh_session_service(service: SSHSessionService | None, *, name: SSHTarget) -> None:
    if service is None:
        _ssh_services.pop(name, None)
    else:
        _ssh_services[name] = service


def get_ssh_session_service_or_none(name: SSHTarget) -> SSHSessionService | None:
    """Get a named SSHSessionService, or None if not initialized."""
    return _ssh_services.get(name)


def get_ssh_session_service(name: SSHTarget) -> SSHSessionService:
    """Get a named SSHSessionService. Raises RuntimeError if not initialized."""
    service = _ssh_services.get(name)
    if service is None:
        raise RuntimeError(f"SSHSessionService '{name}' not initialized")
    return service


# ------ initialized standalone application (standalone) ------


def get_async_engine(url: str, enable_ssl: bool = True, **engine_params: Any) -> AsyncEngine:
    if not enable_ssl:
        engine_params["connect_args"] = {"ssl": "disable"}
    return create_async_engine(url, **engine_params)


def _init_simulation_service(job_backend: str, settings: Settings) -> None:
    """Build the per-backend service registry and set the deployment default.

    One deployment can serve multiple backends (e.g. vecoli→Batch AND v2ecoli→Ray): every
    backend whose settings are configured is built (sharing ONE LocalTaskService so LOCAL
    build jobs are resolvable by any), and ``job_backend`` (COMPUTE_BACKEND) is the default.
    """
    from sms_api.common.hpc.local_task_service import LocalTaskService
    from sms_api.simulation.simulation_service import SimulationServiceHpc

    default_backend = ComputeBackend(job_backend)
    shared_local = LocalTaskService()
    registry: dict[ComputeBackend, SimulationService] = {}

    # AWS Batch + Nextflow (K8s) — built when a K8s namespace is configured.
    if settings.k8s_job_namespace:
        from sms_api.common.hpc.k8s_job_service import K8sJobService
        from sms_api.simulation.simulation_service_k8s import SimulationServiceK8s

        k8s_job_service = K8sJobService(namespace=settings.k8s_job_namespace)
        registry[ComputeBackend.BATCH] = SimulationServiceK8s(
            k8s_job_service=k8s_job_service, local_task_service=shared_local
        )
        logger.info("✓ Backend registered: batch (K8s + AWS Batch)")

    # Ray on AWS Batch MNP — built when the MNP queue is configured.
    if settings.ray_mnp_queue:
        from sms_api.simulation.simulation_service_ray import SimulationServiceRay

        registry[ComputeBackend.RAY] = SimulationServiceRay(local_task_service=shared_local)
        logger.info("✓ Backend registered: ray (AWS Batch MNP)")

    # SLURM has no separate enable flag — build it when it's the deployment default.
    if default_backend == ComputeBackend.SLURM:
        registry[ComputeBackend.SLURM] = SimulationServiceHpc()
        logger.info("✓ Backend registered: slurm (HPC)")

    if default_backend not in registry:
        raise RuntimeError(
            f"COMPUTE_BACKEND={default_backend.value} but its settings are not configured "
            f"(configured backends: {sorted(b.value for b in registry)})"
        )

    set_simulation_service_registry(registry)
    set_simulation_service(registry[default_backend])
    logger.info(
        "✓ Simulation services initialized: default=%s, available=%s",
        default_backend.value,
        sorted(b.value for b in registry),
    )


def _init_ssh_service(job_backend: str, settings: Settings) -> None:
    """Initialize SSH session services.

    Default (SSHTarget.SLURM): SLURM login node — used by SimulationServiceHpc, JobScheduler, log/data retrieval.
    Build (SSHTarget.BUILD): ARM64 build machine — used by SimulationServiceK8s for Docker image builds.
    """
    # SLURM SSH (default) — always init if configured, needed for SLURM backend
    if settings.slurm_submit_host and settings.slurm_submit_key_path:
        logger.info("Initializing SSH session service (SLURM login node)...")
        ssh_key_path = Path(settings.slurm_submit_key_path)
        if not ssh_key_path.exists():
            logger.warning(f"SSH key file not found: {ssh_key_path}")
        else:
            logger.info(f"SSH key found at: {ssh_key_path}")
        set_ssh_session_service(
            SSHSessionService(
                hostname=settings.slurm_submit_host,
                username=settings.slurm_submit_user,
                key_path=ssh_key_path,
                known_hosts=Path(settings.slurm_submit_known_hosts) if settings.slurm_submit_known_hosts else None,
            ),
            name=SSHTarget.SLURM,
        )
        slurm_host = f"{settings.slurm_submit_user}@{settings.slurm_submit_host}"
        logger.info(f"✓ SSH '{SSHTarget.SLURM}' initialized for {slurm_host}")

    # Build machine SSH — for Docker image builds (ARM64 EC2 instance)
    if settings.build_node_host and settings.build_node_key_path:
        logger.info("Initializing SSH session service (build machine)...")
        set_ssh_session_service(
            SSHSessionService(
                hostname=settings.build_node_host,
                username=settings.build_node_user,
                key_path=Path(settings.build_node_key_path),
            ),
            name=SSHTarget.BUILD,
        )
        logger.info(f"✓ SSH '{SSHTarget.BUILD}' initialized for {settings.build_node_user}@{settings.build_node_host}")


async def init_standalone(enable_ssl: bool = True) -> None:
    from sms_api.common.hpc.slurm_service import SlurmService
    from sms_api.simulation.job_scheduler import JobScheduler

    _settings = get_settings()
    job_backend = get_job_backend()

    try:
        # Initialize file service based on configured backend
        logger.info(f"Initializing file service with backend: {_settings.storage_backend}")
        if _settings.storage_backend == "s3":
            set_file_service(FileServiceS3())
        elif _settings.storage_backend == "qumulo":
            set_file_service(FileServiceQumuloS3())
        elif _settings.storage_backend == "gcs":
            set_file_service(FileServiceGCS())
        else:
            logger.error(f"Unsupported storage backend: {_settings.storage_backend}")

        _init_simulation_service(job_backend, _settings)

        # Validate and initialize Postgres connection
        logger.info("Validating Postgres configuration...")
        pg = _settings
        if not (
            pg.postgres_user and pg.postgres_password and pg.postgres_database and pg.postgres_host and pg.postgres_port
        ):
            logger.error("Postgres connection settings are not properly configured.")
        postgres_url = (
            f"postgresql+asyncpg://{pg.postgres_user}:{pg.postgres_password}"
            f"@{pg.postgres_host}:{pg.postgres_port}/{pg.postgres_database}"
        )

        # Check if database service is already set (e.g., by test fixtures)
        db_service: DatabaseService | None = get_database_service()
        if db_service is not None:
            logger.info("✓ Using existing database service (test mode)")
        else:
            logger.info("Initializing postgres connection...")
            engine = get_async_engine(
                url=postgres_url,
                enable_ssl=enable_ssl,
                echo=False,
                pool_size=pg.postgres_pool_size,
                max_overflow=pg.postgres_max_overflow,
                pool_timeout=pg.postgres_pool_timeout,
                pool_recycle=pg.postgres_pool_recycle,
            )
            logger.info("Initializing database tables...")
            await create_db(engine)
            set_postgres_engine(engine)
            logger.info("✓ Postgres connection established and tables initialized")
            db_service = DatabaseServiceSQL(engine)
            set_database_service(db_service)

        _init_ssh_service(job_backend, _settings)

        # Initialize Slurm service (SLURM backend only)
        slurm_service: SlurmService | None = None
        if job_backend == ComputeBackend.SLURM:
            slurm_service = SlurmService()
            logger.info("✓ SlurmService initialized")

        # Initialize messaging service
        redis_addr = f"{_settings.redis_internal_host}:{_settings.redis_internal_port}"
        logger.info(f"Initializing Redis messaging service at {redis_addr}...")
        messaging_service: MessagingService = MessagingServiceRedis()
        await messaging_service.connect(host=_settings.redis_internal_host, port=_settings.redis_internal_port)
        logger.info("✓ Messaging service connected")
        set_messaging_service(messaging_service)

        # Initialize JobScheduler
        logger.info("Initializing JobScheduler...")
        job_scheduler = JobScheduler(
            messaging_service=messaging_service,
            database_service=db_service,
            slurm_service=slurm_service,
        )
        set_job_scheduler(job_scheduler)
        logger.info("✓ JobScheduler initialized")

        # Initialize compose (process-bigraph) subsystem
        await _init_compose_subsystem(engine=get_postgres_engine())

    except Exception as e:
        logger.error(f"Failed to initialize standalone services: {e}", exc_info=True)
        raise


async def _init_compose_subsystem(engine: AsyncEngine | None) -> None:
    """Initialize the compose (process-bigraph) subsystem using the shared Postgres engine."""
    try:
        if engine is None:
            logger.warning("Skipping compose subsystem init: no Postgres engine available")
            return

        from sqlalchemy.ext.asyncio import async_sessionmaker

        from sms_api.api.routers.compose import set_compose_services
        from sms_api.compose.database_service import ComposeDatabaseService
        from sms_api.compose.job_monitor import ComposeJobMonitor
        from sms_api.compose.models import DEFAULT_COMPOSE_ALLOW_LIST
        from sms_api.compose.simulation_service import ComposeSimulationService, ComposeSimulationServiceHpc
        from sms_api.compose.tables_orm import create_compose_db

        logger.info("Initializing compose subsystem tables...")
        await create_compose_db(engine)

        session_maker = async_sessionmaker(engine, expire_on_commit=True)
        compose_db = ComposeDatabaseService(session_maker)

        # Per-backend compose service registry, mirroring _init_simulation_service: the
        # deployment default (COMPUTE_BACKEND) is what the compose router uses, and the Ray
        # backend is registered when the MNP queue is configured (Stanford). SLURM has no
        # separate enable flag — it's the default when COMPUTE_BACKEND=slurm (UCONN).
        settings = get_settings()
        default_backend = get_job_backend()
        compose_registry: dict[ComputeBackend, ComposeSimulationService] = {}
        if settings.ray_mnp_queue:
            from sms_api.compose.simulation_service_ray import ComposeSimulationServiceRay

            compose_registry[ComputeBackend.RAY] = ComposeSimulationServiceRay()
            logger.info("✓ Compose backend registered: ray (AWS Batch MNP)")
        if default_backend == ComputeBackend.SLURM:
            compose_registry[ComputeBackend.SLURM] = ComposeSimulationServiceHpc()
            logger.info("✓ Compose backend registered: slurm (HPC)")
        # Default: the deployment's compute backend if built, else whatever's available
        # (Stanford runs COMPUTE_BACKEND=ray → Ray; UCONN → SLURM).
        compose_sim = compose_registry.get(default_backend) or next(
            iter(compose_registry.values()), ComposeSimulationServiceHpc()
        )
        compose_monitor = ComposeJobMonitor(
            nats_client=None, database_service=compose_db, sim_registry=compose_registry
        )

        # Bootstrap compose_allow_list on a fresh deployment only — never overwrites an
        # operator-curated table (see AllowListDatabaseService.seed_if_empty).
        await compose_db.get_allow_list_db().seed_if_empty(DEFAULT_COMPOSE_ALLOW_LIST)

        set_compose_services(db=compose_db, sim=compose_sim, monitor=compose_monitor)

        # Start compose job monitor polling
        await compose_monitor.start_polling(interval_seconds=30)

        logger.info("✓ Compose subsystem initialized")
    except Exception:
        logger.warning("Compose subsystem initialization failed (non-fatal)", exc_info=True)


async def shutdown_standalone() -> None:
    mongodb_service = get_database_service()
    if mongodb_service:
        await mongodb_service.close()

    engine = get_postgres_engine()
    if engine:
        await engine.dispose()

    file_service = get_file_service()
    if file_service:
        await file_service.close()

    set_simulation_service(None)
    set_simulation_service_registry({})
    set_database_service(None)
    set_file_service(None)
    set_ssh_session_service(None, name=SSHTarget.SLURM)
    set_ssh_session_service(None, name=SSHTarget.BUILD)

    job_scheduler = get_job_scheduler()
    if job_scheduler:
        await job_scheduler.close()
        set_job_scheduler(None)
    # for dirpath in [p for p in Path(f"{REPO_ROOT}/.results_cache").rglob("*") if p.is_dir()]:
    #     shutil.rmtree(dirpath)
