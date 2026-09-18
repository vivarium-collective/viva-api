"""Every simple ``set_X`` / ``get_X`` pair in ``viva_api.dependencies`` must round-trip.

``set_messaging_service`` once assigned its argument to a LOCAL named
``global_job_scheduler`` (silenced with ``# noqa: F841``), so ``get_messaging_service()``
returned ``None`` for the life of the process. Nothing called the getter, so nothing
failed -- which is exactly why the pairs are checked together here rather than one by one.
"""

from collections.abc import Callable, Iterator
from typing import Any

import pytest

from viva_api import dependencies

_PAIRS: list[tuple[str, str]] = [
    ("set_file_service", "get_file_service"),
    ("set_postgres_engine", "get_postgres_engine"),
    ("set_database_service", "get_database_service"),
    ("set_local_task_service", "get_local_task_service"),
    ("set_job_scheduler", "get_job_scheduler"),
    ("set_messaging_service", "get_messaging_service"),
]


@pytest.fixture
def restore_singletons() -> Iterator[None]:
    saved: dict[str, Any] = {getter: getattr(dependencies, getter)() for _, getter in _PAIRS}
    yield
    for setter, getter in _PAIRS:
        getattr(dependencies, setter)(saved[getter])


@pytest.mark.parametrize(("setter_name", "getter_name"), _PAIRS)
def test_setter_getter_round_trip(setter_name: str, getter_name: str, restore_singletons: None) -> None:
    setter: Callable[[Any], None] = getattr(dependencies, setter_name)
    getter: Callable[[], Any] = getattr(dependencies, getter_name)
    sentinel = object()

    setter(sentinel)
    assert getter() is sentinel

    setter(None)
    assert getter() is None


def test_every_simple_pair_is_covered() -> None:
    """A new ``set_X``/``get_X`` pair must be added to ``_PAIRS`` (or excluded here, with a reason)."""
    excluded = {
        # keyed by SSHTarget, not a single slot
        "set_ssh_session_service",
        # the default service and its registry are resolved together by several getters
        "set_simulation_service",
        "set_simulation_service_registry",
    }
    setters = {name for name in vars(dependencies) if name.startswith("set_") and callable(getattr(dependencies, name))}
    assert setters - excluded == {setter for setter, _ in _PAIRS}
