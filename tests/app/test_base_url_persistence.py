"""Tests for the persisted last-used --base-url (backlog item 72 Phase 1).

Pure/offline — no network, no live server. Verifies:
  * app.app_data_service._remember_base_url / recall_base_url round-trip
    through an XDG-config-style hint file, mirroring vivarium-workbench's
    lib/github_auth.py `_remember_login()` / `_recall_login()` pattern.
  * get_data_service() persists whichever base_url it actually resolved to
    (explicit or defaulted) as a side effect — the single funnel point shared
    by the CLI, TUI, and GUI clients.
  * app.cli's module-level API_BASE_URL default honors the documented
    resolution order: explicit API_BASE_URL env var > persisted last-used
    value > the hardcoded default — the behavior that makes a second
    `atlantis` invocation reuse the previous one's --base-url without passing
    it again.
"""

from __future__ import annotations

import importlib
from pathlib import Path

import pytest

from app import app_data_service as ads
from app.app_data_service import BaseUrl, get_data_service, recall_base_url


@pytest.fixture(autouse=True)
def _isolated_xdg_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Every test gets its own XDG_CONFIG_HOME so nothing touches the real
    ~/.config/atlantis/last_base_url on the machine running the tests, and
    tests never see each other's persisted state."""
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    return tmp_path


# --- _last_base_url_path / _remember_base_url / recall_base_url -----------


def test_recall_returns_none_when_never_persisted() -> None:
    assert recall_base_url() is None


def test_remember_and_recall_roundtrip() -> None:
    ads._remember_base_url("http://localhost:8888")
    assert recall_base_url() == "http://localhost:8888"


def test_remember_overwrites_previous_value() -> None:
    ads._remember_base_url("http://localhost:8888")
    ads._remember_base_url("http://localhost:8080")
    assert recall_base_url() == "http://localhost:8080"


def test_remember_ignores_empty_value() -> None:
    ads._remember_base_url("")
    assert recall_base_url() is None
    assert not ads._last_base_url_path().exists()


def test_remember_strips_whitespace() -> None:
    ads._remember_base_url("  http://localhost:8080  \n")
    assert recall_base_url() == "http://localhost:8080"


def test_last_base_url_path_under_xdg_config_atlantis(tmp_path: Path) -> None:
    assert ads._last_base_url_path() == tmp_path / "atlantis" / "last_base_url"


def test_remember_base_url_best_effort_on_write_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """A persistence failure (read-only FS, permissions, ...) must never raise —
    the CLI/TUI/GUI must keep working even if the hint file can't be written."""

    def _boom(*_a: object, **_k: object) -> None:
        raise OSError("read-only filesystem")

    monkeypatch.setattr(Path, "mkdir", _boom)
    ads._remember_base_url("http://localhost:8080")  # must not raise


def test_recall_base_url_best_effort_on_read_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    ads._remember_base_url("http://localhost:8080")

    def _boom(*_a: object, **_k: object) -> str:
        raise OSError("permission denied")

    monkeypatch.setattr(Path, "read_text", _boom)
    assert recall_base_url() is None  # degrades to "never persisted", never raises


# --- get_data_service() persists whichever base_url it resolves -----------


def test_get_data_service_persists_explicit_base_url() -> None:
    get_data_service(base_url=BaseUrl.LOCAL)
    assert recall_base_url() == BaseUrl.LOCAL.value


def test_get_data_service_persists_default_base_url_when_none_passed() -> None:
    get_data_service()
    assert recall_base_url() == ads.DEFAULT_BASE_URL.value


def test_get_data_service_accepts_str_base_url_and_persists_it() -> None:
    get_data_service(base_url="http://localhost:1111")
    assert recall_base_url() == "http://localhost:1111"


def test_get_data_service_second_call_overwrites_persisted_value() -> None:
    get_data_service(base_url=BaseUrl.LOCAL)
    get_data_service(base_url=BaseUrl.RKE_DEV)
    assert recall_base_url() == BaseUrl.RKE_DEV.value


# --- app.cli's module-level API_BASE_URL resolution order -----------------


@pytest.fixture
def _reloaded_cli(monkeypatch: pytest.MonkeyPatch):
    """Reload app.cli under controlled env so its module-level API_BASE_URL
    (computed once at import time) reflects the current XDG_CONFIG_HOME /
    API_BASE_URL env fixture state, then restore real state afterward so this
    test's env doesn't leak into any other test that imports app.cli later in
    the same session."""
    import app.cli as cli_mod

    def _reload():
        return importlib.reload(cli_mod)

    yield _reload
    monkeypatch.delenv("API_BASE_URL", raising=False)
    _reload()  # restore app.cli to its natural (real-XDG-config) state


def test_cli_default_falls_back_to_hardcoded_default_when_nothing_persisted(_reloaded_cli) -> None:
    from app.cli import ApiBaseUrl

    mod = _reloaded_cli()
    assert mod.API_BASE_URL == ApiBaseUrl.LOCAL_8080


def test_cli_default_uses_persisted_value_when_no_env_override(_reloaded_cli) -> None:
    ads._remember_base_url("http://localhost:1111")
    mod = _reloaded_cli()
    assert mod.API_BASE_URL == "http://localhost:1111"


def test_cli_default_prefers_env_var_over_persisted_value(monkeypatch: pytest.MonkeyPatch, _reloaded_cli) -> None:
    ads._remember_base_url("http://localhost:1111")
    monkeypatch.setenv("API_BASE_URL", "http://localhost:62505")
    mod = _reloaded_cli()
    assert mod.API_BASE_URL == "http://localhost:62505"


def test_cli_second_invocation_reuses_first_invocations_base_url(_reloaded_cli) -> None:
    """End-to-end shape of the behavior this phase adds: a first invocation
    that resolves --base-url (explicitly or by default) makes the NEXT
    invocation's default converge to it, with no --base-url passed."""
    from app.app_data_service import get_data_service as gds

    gds(base_url=BaseUrl.LOCAL)  # "first invocation" used --base-url http://localhost:8888
    mod = _reloaded_cli()  # "second invocation" process boot, no --base-url passed
    assert BaseUrl.LOCAL.value == mod.API_BASE_URL
