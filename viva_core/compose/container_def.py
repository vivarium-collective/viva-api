"""Containerization DTOs and the generic process-bigraph singularity def builder.

This module inlines the small ``ContainerizationFileRepr`` / ``ContainerizationEngine``
DTOs (previously sourced from an external containerization package) and owns
``build_pbg_def`` — a deterministic Singularity/apptainer definition that installs
process-bigraph and embeds a generic runner. It is the ``python-deps`` recipe of decision D10,
in its SLURM (Apptainer) form.
"""

from enum import Enum

from pydantic import BaseModel


class ContainerizationEngine(Enum):
    """The container engine a definition targets."""

    NONE = 0
    DOCKER = 1
    APPTAINER = 2
    BOTH = 3


class ContainerizationFileRepr(BaseModel):  # type: ignore[explicit-any]  # pydantic's, not ours (D12)
    """A textual container-definition file (e.g. a Singularity/apptainer def)."""

    representation: str
    containerization_engine: ContainerizationEngine = ContainerizationEngine.APPTAINER


def build_pbg_def(
    input_suffix: str, extra_pip_deps: list[str] | None = None, *, runner_source: str
) -> ContainerizationFileRepr:
    """Build a deterministic Singularity/apptainer def for the generic pbg runner.

    Installs process-bigraph + bigraph-schema + pbg-emitters (plus any
    ``extra_pip_deps``, e.g. a ``git+https://...@sha`` workspace package) and
    embeds ``runner_source`` -- the generic runner's text, HANDED IN by the caller -- at ``/opt/run_pbg.py``.
    (It was read from this package at import time; the runner is the application's to stage until it
    moves into core, and a recipe that reads a file at import cannot be built without that file.)

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
        f"{runner_source}\n"
        "PBG_RUNNER_EOF\n\n"
        '%runscript\n    exec python /opt/run_pbg.py "$@"\n'
    )
    return ContainerizationFileRepr(representation=representation)
