"""Core's settings -- the first, deliberately small, slice (``docs/plan-core.md`` P1b).

Only what the modules already living in ``viva_core`` read: the storage backends and the
local/remote path-prefix mapping. It grows as modules move; the full split of the
application's settings is P3.

One source of truth per process
-------------------------------
A standalone core builds :class:`CoreSettings` from the environment. An APPLICATION that
embeds core usually has a settings object of its own, populated its own way (dotenv files, a
secrets mount, ...). Two objects reading the same variables at different moments is how two
halves of one process come to disagree, so the application hands core ITS object instead:

    set_core_settings_provider(lambda: my_app_settings())   # my settings subclass CoreSettings

after which :func:`get_core_settings` returns that. The field definitions live here and the
application's settings class inherits them, so there is also exactly one definition of each
field, its default and its environment variable name.
"""

from collections.abc import Callable
from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict

from viva_core.storage.file_paths import HPCFilePath

#: The object stores a file service can be built for (``viva_core.storage``).
StorageBackend = Literal["gcs", "s3", "qumulo"]


class CoreSettings(BaseSettings):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
    # No env_prefix (yet): these are the variable names every existing deployment already
    # sets. A VIVA_CORE_ prefix arrives, with aliases, when core is deployed on its own (P10).
    model_config = SettingsConfigDict(extra="ignore")

    # GCS
    storage_gcs_bucket: str = "files.biosimulations.dev"
    storage_gcs_endpoint_url: str = "https://storage.googleapis.com"
    storage_gcs_region: str = "us-east4"
    storage_gcs_credentials_file: str = ""

    # Local cache for downloads
    storage_local_cache_dir: str = "./local_cache"

    # Where a run's outputs go: one bucket, one prefix, ``{prefix}/{experiment_id}/`` per run
    # (``viva_core.storage.layout``). The prefix's default is the application's (P3d-4c-1).
    s3_work_bucket: str = ""
    s3_output_prefix: str = ""

    # Identity (P3d-4d-2b): an OIDC bearer token, verified against the issuer's keys, and/or an
    # identity header a proxy in front of the deployment sets. ``viva_core.api.auth`` reads these;
    # empty is the default and a legitimate steady state.
    oidc_issuer: str = ""
    oidc_audience: str = ""  # REQUIRED whenever oidc_issuer is set, and deliberately has no default
    oidc_algorithms: str = "RS256"
    oidc_jwks_cache_seconds: int = 300
    oidc_leeway_seconds: int = 30
    oidc_fetch_timeout_seconds: float = 5.0
    identity_header: str = ""
    # This process's ROLE in the deployment: a stable name, not a pod name. Env-worker tasks are
    # stamped with it and the boot sweep settles only its own role's.
    owner_instance: str = "api"

    # Where a SLURM job's log lands, as an HPC path (the SLURM backend writes ``<base>/<job>.out``).
    slurm_log_base_path: HPCFilePath = HPCFilePath(remote_path=Path(""))
    # The SLURM backend (decision D4; UConn track U2): the submit host reached over SSH, and the
    # scheduler's partition / QoS / node list every sbatch template names. Moved here from the
    # application's settings (same names, same variables) so the SLURM compose service can be core's.
    slurm_submit_host: str = ""
    slurm_submit_port: int = 22  # a SLURM cluster in Docker publishes sshd on a port of Docker's choosing
    slurm_submit_user: str = ""
    slurm_submit_key_path: str = ""
    slurm_submit_known_hosts: str | None = None
    slurm_partition: str = ""
    slurm_node_list: str = ""  # comma-separated, e.g. "node1,node2"; empty = the scheduler's choice
    slurm_qos: str = ""
    # The root under which this site keeps its SLURM work (sbatch files, logs, images, runs).
    slurm_base_path: HPCFilePath = HPCFilePath(remote_path=Path(""))

    # Which object store the file service talks to (``viva_core.storage.factory``, U2c).
    storage_backend: StorageBackend = "s3"

    # The database (U2e; decision D19: a standalone core has a database of its own). Moved here from
    # the application's settings -- same names, same variables -- so core's own lifespan can open
    # it. ``postgres_user`` keeps the application's placeholder default: a site that never set it
    # has no database, and core says so rather than dialling ``<USER>@localhost``.
    postgres_user: str = "<USER>"
    postgres_password: str = ""
    postgres_database: str = "sms"
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_pool_size: int = 10  # number of connections in the pool
    postgres_max_overflow: int = 5  # maximum number of connections that can be created beyond the pool size
    postgres_pool_timeout: int = 30  # timeout for acquiring a connection from the pool in seconds
    postgres_pool_recycle: int = 1800  # recycle connections every seconds
    # Run create_all at startup. True suits a laptop or a test; a DEPLOYED site sets it false,
    # because there the schema belongs to the migration Job alone (see the application's
    # ``simulation/db_startup.py`` for why create_all is corrosive in production). Core's own
    # Alembic chain arrives at P7; until then this is how core's own database is bootstrapped.
    db_create_all: bool = True

    # AWS S3
    storage_s3_bucket: str = ""
    storage_s3_region: str = "us-east-1"
    storage_s3_access_key_id: str = ""
    storage_s3_secret_access_key: str = ""
    storage_s3_session_token: str = ""

    # Qumulo (S3-compatible)
    storage_qumulo_endpoint_url: str = ""
    storage_qumulo_bucket: str = ""
    storage_qumulo_access_key_id: str = ""
    storage_qumulo_secret_access_key: str = ""
    storage_qumulo_verify_ssl: bool = True

    # Path prefix mapping for local vs remote (cluster) filesystem access.
    # Example: path_local_prefix=/Volumes/DATA, path_remote_prefix=/projects/DATA
    path_local_prefix: str = ""
    path_remote_prefix: str = ""

    # Environments (decision D10). The full reference of this site's CORE RUNTIME IMAGE
    # (``Dockerfile-core-runtime``): what a composite that needs nothing beyond the built-ins runs in.
    # Empty = the site has none, and such a composite is refused rather than run in something else.
    core_runtime_image: str = ""
    # Where a STANDALONE core's environments live: one registry repository, images tagged by their
    # key. An application that embeds core usually builds the resolver itself, from its own settings
    # (SMS does: `viva_api/common/site_environments.py`), and leaves these empty.
    environment_registry: str = ""
    environment_repository: str = ""

    # What compose and the env workers read (P3d-2). Moved here from the application's settings so
    # those packages can stop importing them from it; the application's Settings INHERITS each one,
    # so the environment variable names and the defaults are unchanged. Two defaults name an
    # application's image and so are neutral here and overridden there.
    k8s_job_namespace: str = ""
    ecr_account_id: str = ""
    batch_region: str = "us-gov-west-1"  # where the site's registry and its Batch queues are
    ray_num_nodes: int = 3
    env_worker_module_image: str = ""
    env_worker_workspace_path: str = ""  # the application supplies its own default
    env_worker_memory_request: str = "512Mi"
    env_worker_memory_limit: str = "8Gi"
    # What an env-worker Job is labelled, runs as, and where its module is inside the module image
    # (U2d; until then these named one deployment inside core). The application supplies each
    # default; a standalone core with none labels nothing, uses the namespace's default service
    # account, and refuses to stage a module from nowhere.
    env_worker_app_label: str = ""
    env_worker_service_account: str = ""
    env_worker_module_path: str = ""
    ray_ecr_repository: str = ""  # the application supplies its own default
    compose_image_base_path: str = ""
    compose_sim_base_path: str = ""
    compose_ray_image_tag: str = ""
    compose_pbg_core_builder: str = ""
    compose_nats_worker_event_subject: str = "compose.worker.events"
    compose_containers_output_dir: str = "/output"  # where a composite's container writes its outputs
    # An HPC path the SLURM compose service bind-mounts into a composite's container at /out/cache;
    # empty = nothing mounted. What goes there is the application's business (a staged input set).
    compose_cache_base_path: str = ""
    # How the SLURM compose service BUILDS a composite's container (UConn track, 2026-09-26).
    # "sbatch": on the HPC with `singularity build --fakeroot` -- needs a subuid entry for the service
    # user on the nodes. "k8s": a Kubernetes Job in ``k8s_job_namespace`` from an Apptainer image,
    # PRIVILEGED (a definition's %post needs mount namespaces; nothing less worked on RKE2), which
    # then copies the image onto the shared filesystem as the service user -- the filesystem the
    # SLURM nodes read it from, mounted in the Job at the same path (``compose_build_pvc_*``).
    compose_build_backend: Literal["sbatch", "k8s"] = "sbatch"
    compose_build_image: str = "ghcr.io/apptainer/apptainer:1.3.6"
    compose_build_pvc_claim: str = ""  # the PersistentVolumeClaim of the shared filesystem
    compose_build_pvc_mount_path: str = ""  # where it is mounted in the Job: a prefix of compose_image_base_path
    compose_build_pvc_sub_path: str = ""
    # Who writes the image onto the filesystem (0 = root, on a filesystem that lets root write).
    compose_build_run_as_uid: int = 0
    compose_build_run_as_gid: int = 0
    compose_build_supplemental_groups: str = ""  # comma-separated gids
    compose_build_timeout_seconds: int = 1800


_provider: Callable[[], CoreSettings] | None = None


@lru_cache
def _settings_from_environment() -> CoreSettings:
    return CoreSettings()


def set_core_settings_provider(provider: Callable[[], CoreSettings] | None) -> None:
    """Make :func:`get_core_settings` return the embedding application's settings object.

    ``None`` restores the default (built from the environment). The provider is called on
    every read, so an application that swaps or re-reads its settings is followed.
    """
    global _provider
    _provider = provider


def get_core_settings() -> CoreSettings:
    return _provider() if _provider is not None else _settings_from_environment()


def get_local_cache_dir() -> Path:
    local_cache_dir = Path(get_core_settings().storage_local_cache_dir)
    local_cache_dir.mkdir(parents=True, exist_ok=True)
    return local_cache_dir
