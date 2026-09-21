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

from pydantic_settings import BaseSettings, SettingsConfigDict


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
