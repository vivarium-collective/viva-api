"""The ``python-deps`` recipe in its Apptainer form (``viva_core/compose/container_def.py``)."""

import importlib.resources as _res

import pytest

from viva_core.compose.container_def import build_pbg_def
from viva_core.compose.runner_files import runner_source

RUNNER = "print('a runner')\n"


def test_def_installs_process_bigraph_and_embeds_runner() -> None:
    d = build_pbg_def("pbg", runner_source=RUNNER).representation
    assert "Bootstrap: docker" in d
    assert "pip install --no-cache-dir process-bigraph bigraph-schema" in d
    assert "/opt/run_pbg.py" in d
    assert RUNNER in d
    assert "%runscript" in d


def test_def_injects_extra_pip_deps() -> None:
    d = build_pbg_def("pbg", ["git+https://github.com/x/y.git@abc"], runner_source=RUNNER).representation
    assert "git+https://github.com/x/y.git@abc" in d


def test_the_runner_is_handed_in_not_read_by_the_recipe() -> None:
    """The recipe read ``run_pbg.py`` from the application's package at import time (P3d-4b-2 moved it
    into core, which cannot); since P3d-4c-2 the runner is core's and the SLURM handler embeds that one."""
    with pytest.raises(TypeError):
        build_pbg_def("pbg")  # type: ignore[call-arg]
    assert runner_source() == (_res.files("viva_core.compose") / "run_pbg.py").read_text()
    assert "def _load_hooks(" in runner_source()


def test_the_definition_installs_requests_which_process_bigraph_imports_undeclared() -> None:
    """process_bigraph/protocols/rest.py imports `requests` without declaring it; the runtime image's
    requirements.txt carries it for the same reason. A run without it died on Mantis (UConn UB #8)."""
    from viva_core.compose.container_def import build_pbg_def

    representation = build_pbg_def("pbg", runner_source="print('x')").representation
    (pip_line,) = [line for line in representation.splitlines() if "pip install" in line and "process-bigraph" in line]
    assert " requests" in pip_line
