"""A standalone core's lifespan: build every service its settings are enough for, start what polls,
and stop it all in the right order (``docs/plan-core.md`` §4b U2e; decision D7).

What it builds, each configured-or-absent -- a route whose service is absent answers by name:

* the environment resolver (a registry + repository);
* the file service (``storage_backend``);
* the SLURM SSH sessions (a submit host + a key), and on them the SLURM compose service;
* the database (``postgres_*``), and on it the compose database, the env-worker task tier and
  the compose job monitor, which polls;
* the env-worker service (a K8s namespace + a module image), which needs in-cluster credentials.

Modelled on the application's ``dependencies.init_standalone`` and ``_shutdown_background_work``,
with what is SMS's left out: the simulation-service registry, the scheduler, Redis, the Batch
compose service (its settings are the application's until the strategies land), the allow-list
seed (core's default list is empty). Shutdown stops the pollers and closes the sockets BEFORE
the engine goes away; each step is isolated so one failing to stop leaves nothing running against
a disposed engine.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from viva_core.backends.k8s_job_service import K8sJobService
from viva_core.compose.build_k8s import K8sContainerBuild
from viva_core.compose.job_monitor import ComposeJobMonitor
from viva_core.container import ComposeServices, CoreContainer, EnvWorkerServices
from viva_core.env_worker import relay as env_worker_relay
from viva_core.environments import EnvironmentResolver, RegistryEnvironmentResolver
from viva_core.infra.db import async_engine_from_settings, postgres_configured
from viva_core.infra.ssh.ssh_service import SSHSessionService
from viva_core.models import ComputeBackend
from viva_core.settings import CoreSettings
from viva_core.storage.factory import file_service_from_settings
from viva_core.storage.file_service import FileService

if TYPE_CHECKING:
    from viva_core.api.capabilities import Capability

logger = logging.getLogger(__name__)

COMPOSE_POLL_SECONDS = 30


def environments_from_settings(settings: CoreSettings) -> EnvironmentResolver | None:
    if not (settings.environment_registry and settings.environment_repository):
        return None
    return RegistryEnvironmentResolver(
        registry=settings.environment_registry,
        repository=settings.environment_repository,
        runtime_image=settings.core_runtime_image or None,
    )


def slurm_sessions_from_settings(settings: CoreSettings) -> SSHSessionService | None:
    """The SLURM submit host's sessions, when a host and a key are named."""
    if not (settings.slurm_submit_host and settings.slurm_submit_key_path):
        return None
    key_path = Path(settings.slurm_submit_key_path).expanduser()
    if not key_path.exists():
        logger.warning("SLURM SSH key not found at %s; the SLURM backend will fail to connect", key_path)
    return SSHSessionService(
        hostname=settings.slurm_submit_host,
        port=settings.slurm_submit_port,
        username=settings.slurm_submit_user,
        key_path=key_path,
        known_hosts=Path(settings.slurm_submit_known_hosts) if settings.slurm_submit_known_hosts else None,
    )


def _file_service(settings: CoreSettings) -> FileService | None:
    try:
        return file_service_from_settings()
    except Exception:
        logger.warning("file service (%s) could not be built; results streaming is off", settings.storage_backend)
        return None


def _k8s_build_from_settings(settings: CoreSettings) -> tuple[K8sContainerBuild | None, K8sJobService | None]:
    """The Kubernetes build Job and the client the monitor polls it with, when
    ``compose_build_backend`` is ``k8s``; needs a namespace and in-cluster credentials."""
    if settings.compose_build_backend != "k8s":
        return None, None
    if not settings.k8s_job_namespace:
        logger.warning("compose_build_backend=k8s but k8s_job_namespace is unset; builds fall back to sbatch")
        return None, None
    try:
        k8s = K8sJobService(namespace=settings.k8s_job_namespace)
        build = K8sContainerBuild(k8s, settings)
    except Exception:
        logger.warning("Kubernetes build Job unavailable (non-fatal); builds fall back to sbatch", exc_info=True)
        return None, None
    logger.info("✓ container builds run as Kubernetes Jobs in %s", settings.k8s_job_namespace)
    return build, k8s


@dataclass
class RunningCore:
    """What ``start_core`` built: the container the routes reach, and what ``stop`` must end."""

    container: CoreContainer
    engine: AsyncEngine | None = None
    monitor: ComposeJobMonitor | None = None

    async def stop(self) -> None:
        if self.monitor is not None:
            try:
                await self.monitor.close()
            except Exception:
                logger.warning("ComposeJobMonitor did not stop cleanly", exc_info=True)
        workers = self.container.env_worker
        if workers is not None and workers.runner is not None:
            try:
                await workers.runner.close()
            except Exception:
                logger.warning("env-worker TaskRunner did not stop cleanly", exc_info=True)
        try:
            env_worker_relay.registry.close_all()
        except Exception:
            logger.warning("env-worker relay sockets did not close cleanly", exc_info=True)
        if self.engine is not None:
            await self.engine.dispose()


async def start_core(
    settings: CoreSettings, *, enable_ssl: bool = True, capabilities: tuple[Capability, ...] = ()
) -> RunningCore:
    """Build the container from ``settings`` and start what polls. Never raises for a service
    that is merely unconfigured; raises for one that is configured and broken (a database that
    refuses the connection is an outage, not an absence)."""
    environments = environments_from_settings(settings)
    files = _file_service(settings)
    slurm_sessions = slurm_sessions_from_settings(settings)
    slurm_ssh: Callable[[], SSHSessionService] | None = (lambda: slurm_sessions) if slurm_sessions else None

    engine: AsyncEngine | None = None
    compose: ComposeServices | None = None
    monitor: ComposeJobMonitor | None = None
    workers = EnvWorkerServices()

    if postgres_configured(settings):
        from viva_core.compose.database_service import ComposeDatabaseService
        from viva_core.compose.tables_orm import create_compose_db
        from viva_core.env_worker.relay import TaskRunner

        engine = async_engine_from_settings(settings, enable_ssl=enable_ssl)
        if settings.db_create_all:
            await create_compose_db(engine)
        compose_db = ComposeDatabaseService(async_sessionmaker(engine, expire_on_commit=True))
        task_db = compose_db.get_env_worker_task_db()
        workers = EnvWorkerServices(task_db=task_db, runner=TaskRunner(task_db))
        logger.info("✓ core database connected (%s@%s)", settings.postgres_user, settings.postgres_host)

        if slurm_ssh is not None:
            from viva_core.compose.simulation_service_hpc import ComposeSimulationServiceHpc

            container_build, k8s_jobs = _k8s_build_from_settings(settings)
            slurm_compose = ComposeSimulationServiceHpc(
                slurm_ssh=slurm_ssh,
                results_cache_dir=Path(settings.storage_local_cache_dir) / "compose",
                container_build=container_build,
            )
            monitor = ComposeJobMonitor(
                nats_client=None,
                database_service=compose_db,
                sim_registry={ComputeBackend.SLURM: slurm_compose},
                slurm_ssh=slurm_ssh,
                k8s_jobs=k8s_jobs,
            )
            compose = ComposeServices(db=compose_db, sim=slurm_compose, monitor=monitor, files=files)
            await monitor.start_polling(interval_seconds=COMPOSE_POLL_SECONDS)
            logger.info("✓ compose backend registered: slurm (%s)", settings.slurm_submit_host)
        else:
            logger.info("compose has a database but no backend: no SLURM submit host is configured")
    else:
        logger.info("no database configured (postgres_*): compose and the env-worker task tier are off")

    if settings.k8s_job_namespace and settings.env_worker_module_image:
        try:
            from viva_core.env_worker.service import EnvWorkerService

            workers = EnvWorkerServices(service=EnvWorkerService(), task_db=workers.task_db, runner=workers.runner)
            logger.info("✓ env-worker service initialized (namespace=%s)", settings.k8s_job_namespace)
        except Exception:
            # Non-fatal: a process without in-cluster credentials must still serve everything else.
            logger.warning("env-worker service initialization failed (non-fatal)", exc_info=True)

    container = CoreContainer(
        settings=settings,
        environments=environments,
        compose=compose,
        env_worker=workers,
        capabilities=capabilities,
    )
    return RunningCore(container=container, engine=engine, monitor=monitor)
