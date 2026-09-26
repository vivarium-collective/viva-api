"""Core's settings: one definition of each field, and one settings object per process.

A standalone core builds ``CoreSettings`` from the environment. An application that embeds
core hands it the application's own object, so the two halves of one process can never read
the same variable at different moments and disagree.
"""

from collections.abc import Iterator
from pathlib import Path

import pytest

from viva_core import settings as core_settings
from viva_core.settings import CoreSettings, get_core_settings, get_local_cache_dir, set_core_settings_provider
from viva_core.storage.file_paths import HPCFilePath


@pytest.fixture
def standalone() -> Iterator[None]:
    """No application provider (put back by tests/core/conftest.py), and a clean environment-built
    cache -- cleared again afterwards."""
    set_core_settings_provider(None)
    core_settings._settings_from_environment.cache_clear()
    yield
    core_settings._settings_from_environment.cache_clear()


def test_standalone_core_reads_the_environment(standalone: None, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STORAGE_S3_BUCKET", "a-bucket")
    monkeypatch.setenv("PATH_LOCAL_PREFIX", "/Volumes/DATA")
    settings = get_core_settings()
    assert type(settings) is CoreSettings
    assert settings.storage_s3_bucket == "a-bucket"
    assert settings.path_local_prefix == "/Volumes/DATA"


def test_a_provider_replaces_the_default_and_is_consulted_on_every_read(standalone: None) -> None:
    current = CoreSettings(storage_s3_bucket="first")
    set_core_settings_provider(lambda: current)
    assert get_core_settings().storage_s3_bucket == "first"
    current = CoreSettings(storage_s3_bucket="second")  # the application re-read its settings
    assert get_core_settings().storage_s3_bucket == "second"


def test_moved_modules_read_through_the_provider(standalone: None, tmp_path: Path) -> None:
    set_core_settings_provider(
        lambda: CoreSettings(
            path_local_prefix="/Volumes/DATA",
            path_remote_prefix="/projects/DATA",
            storage_local_cache_dir=str(tmp_path),
        )
    )
    assert HPCFilePath(remote_path=Path("/projects/DATA/run/1")).local_path() == Path("/Volumes/DATA/run/1")
    assert get_local_cache_dir() == tmp_path


def test_the_slurm_backend_is_configured_through_core(standalone: None, monkeypatch: pytest.MonkeyPatch) -> None:
    """U2 (the UConn track): a standalone core on a SLURM site reads its submit host, scheduler
    names and work root from the same variables the application always used."""
    monkeypatch.setenv("SLURM_SUBMIT_HOST", "haproxy-ssh")
    monkeypatch.setenv("SLURM_SUBMIT_PORT", "2222")
    monkeypatch.setenv("SLURM_PARTITION", "vcell")
    monkeypatch.setenv("SLURM_BASE_PATH", '{"remote_path": "/projects/SMS/sms_api/prod"}')
    monkeypatch.setenv("STORAGE_BACKEND", "qumulo")
    settings = get_core_settings()
    assert (settings.slurm_submit_host, settings.slurm_submit_port, settings.slurm_partition) == (
        "haproxy-ssh",
        2222,
        "vcell",
    )
    assert settings.slurm_base_path.remote_path == Path("/projects/SMS/sms_api/prod")
    assert settings.storage_backend == "qumulo"
    assert settings.compose_cache_base_path == ""


def test_the_application_hands_core_its_own_settings_object() -> None:
    """Importing anything under ``viva_api`` registers the provider; core then returns the
    application's object -- the SAME object, not an equal one."""
    import viva_api  # noqa: F401
    from viva_api.config import Settings, get_settings

    assert issubclass(Settings, CoreSettings)
    assert get_core_settings() is get_settings()


#: Core fields whose DEFAULT the application supplies, and nothing else may. Each names the
#: application's own image (its registry repository; the workspace root inside it), so the value
#: cannot be written in core -- the vocabulary guard would refuse it, rightly -- and core's own
#: default is EMPTY: there is no second value to disagree with, only an absent one to fill.
APPLICATION_SUPPLIES_THE_DEFAULT = {
    "env_worker_workspace_path",
    "ray_ecr_repository",
    "s3_output_prefix",
    # U2d: what an env-worker Job is labelled and runs as, and where its module image keeps the module
    "env_worker_app_label",
    "env_worker_service_account",
    "env_worker_module_path",
}


def test_every_core_field_has_exactly_one_definition() -> None:
    """The application inherits core's fields; redefining one forks its default. The only
    redefinitions allowed are the named ones above, and only over an EMPTY core default."""
    from viva_api.config import Settings

    redefined = set(CoreSettings.model_fields) & set(vars(Settings).get("__annotations__", {}))
    forked = sorted(redefined - APPLICATION_SUPPLIES_THE_DEFAULT)
    assert not forked, f"viva_api.config.Settings redefines core field(s): {forked}"
    stale = sorted(APPLICATION_SUPPLIES_THE_DEFAULT - redefined)
    assert not stale, f"named as application-supplied but no longer redefined: {stale}"
    for name in sorted(APPLICATION_SUPPLIES_THE_DEFAULT):
        assert CoreSettings.model_fields[name].default == "", f"core's default for {name} must be empty"
        assert Settings.model_fields[name].annotation is CoreSettings.model_fields[name].annotation
