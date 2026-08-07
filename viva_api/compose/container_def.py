"""Containerization DTOs and the generic process-bigraph singularity def builder.

This module inlines the small ``ContainerizationFileRepr`` / ``ContainerizationEngine``
DTOs (previously sourced from an external containerization package) and owns
``build_pbg_def`` — a deterministic Singularity/apptainer definition that installs
process-bigraph and embeds the generic ``run_pbg.py`` runner. This makes sms-api
self-sufficient for the generic compose path, with no external def-builder dependency.
"""

import importlib.resources as _res
from enum import Enum

from pydantic import BaseModel


class ContainerizationEngine(Enum):
    """The container engine a definition targets."""

    NONE = 0
    DOCKER = 1
    APPTAINER = 2
    BOTH = 3


class ContainerizationFileRepr(BaseModel):
    """A textual container-definition file (e.g. a Singularity/apptainer def)."""

    representation: str
    containerization_engine: ContainerizationEngine = ContainerizationEngine.APPTAINER


_RUNNER_SRC = (_res.files("viva_api.compose") / "run_pbg.py").read_text()


def build_pbg_def(input_suffix: str, extra_pip_deps: list[str] | None = None) -> ContainerizationFileRepr:
    """Build a deterministic Singularity/apptainer def for the generic pbg runner.

    Installs process-bigraph + bigraph-schema + pbg-emitters (plus any
    ``extra_pip_deps``, e.g. a ``git+https://...@sha`` workspace package) and
    embeds the generic ``run_pbg.py`` runner at ``/opt/run_pbg.py``.

    ``input_suffix`` is accepted for parity with the call site (the input
    filename is chosen by sms-api, not the def) and is currently unused.
    """
    _ = input_suffix
    post_extra = "".join(
        f"\n    pip install --no-cache-dir --ignore-requires-python '{dep}'" for dep in (extra_pip_deps or [])
    )
    representation = (
        "Bootstrap: docker\nFrom: python:3.12-slim-bookworm\n\n"
        "%post\n    set -eux\n"
        "    pip install --no-cache-dir process-bigraph bigraph-schema pbg-emitters"
        f"{post_extra}\n"
        "    mkdir -p /opt\n"
        "    cat > /opt/run_pbg.py <<'PBG_RUNNER_EOF'\n"
        f"{_RUNNER_SRC}\n"
        "PBG_RUNNER_EOF\n\n"
        '%runscript\n    exec python /opt/run_pbg.py "$@"\n'
    )
    return ContainerizationFileRepr(representation=representation)
