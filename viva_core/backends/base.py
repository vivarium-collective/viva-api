"""Core's job backend: one Protocol, two implementations, one generic job (``docs/plan-core.md``
§4b U2g; ``architecture-core.md`` §2.3; decision D4).

A backend runs one container job and answers for it: ``submit`` a :class:`JobSpec`, ``status`` for
the handles it gave out, ``cancel`` one, read its ``logs``. Nothing here names a scheduler, an
application or a science image: what a job IS (image, command, env, resources, labels, what it
waits on) is the spec; how a scheduler is told is the adapter's -- ``batch_backend`` speaks AWS
Batch through the engine, ``slurm_backend`` writes an sbatch script and submits it over SSH.

Why the Protocol is declared only now, with two implementations at once: a core Protocol with one
implementation would quietly have taken that implementation's shape (the 2026-09-20 audit deferred
it for exactly that reason). SLURM is the second; what both can honour is what the spec carries.
Staging (inputs from and outputs to an object store) is deliberately NOT here yet: Batch's
entrypoint does it through ``CONTAINER_STAGE_*`` env, SLURM through bind mounts on a shared
filesystem, and the strategy that composes jobs (plan §6 step 3) is where that difference is
absorbed. Until then a caller stages through ``env`` and the command.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Protocol

from viva_core.models import JobStatus


@dataclass(frozen=True, slots=True)
class Resources:
    """What the job asks the scheduler for. A backend whose job definition fixes these (AWS Batch,
    until the engine exposes per-submission overrides) says so in its log and runs anyway."""

    cpus: int = 1
    memory_mb: int = 1024
    time_minutes: int = 60


@dataclass(frozen=True, slots=True)
class JobHandle:
    """What a backend hands back for a submitted job and takes for every later question. ``id`` is
    the scheduler's (an AWS Batch job id, a SLURM job id as text); ``name`` is the spec's."""

    backend: str
    id: str
    name: str = ""


@dataclass(frozen=True, slots=True)
class JobSpec:
    """One container job. ``image`` is whatever the backend's runtime can run -- a registry
    reference for Batch, a ``.sif`` path or ``docker://`` reference for Apptainer -- and ``""``
    means the command runs bare on the backend's host. ``command`` is a shell string. ``depends_on``
    are handles from the same backend the job must wait for (success only)."""

    name: str
    command: str
    image: str = ""
    env: Mapping[str, str] = field(default_factory=dict)
    resources: Resources = field(default_factory=Resources)
    labels: Mapping[str, str] = field(default_factory=dict)
    depends_on: tuple[JobHandle, ...] = ()


@dataclass(frozen=True, slots=True)
class BackendStatus:
    """What the scheduler says about a job now. ``reason`` is the scheduler's own word for why
    (Batch's ``statusReason``, SLURM's ``Reason``); ``attempt`` how many times it has been tried
    when the scheduler counts."""

    status: JobStatus
    exit_code: int | None = None
    reason: str | None = None
    start_time: str | None = None
    end_time: str | None = None
    attempt: int | None = None


class JobBackend(Protocol):
    """A way of running container jobs. ``kind`` names it (``"batch-container"``, ``"slurm"``, …)
    and is what a handle carries back."""

    @property
    def kind(self) -> str: ...

    async def submit(self, spec: JobSpec) -> JobHandle: ...

    async def status(self, handles: Sequence[JobHandle]) -> dict[str, BackendStatus]:
        """Keyed by ``handle.id``. A handle the scheduler no longer knows is ABSENT, not an error:
        every scheduler forgets finished jobs on its own clock, and a caller treats absence as
        "ask again later, then give up", never as a status."""
        ...

    async def cancel(self, handle: JobHandle) -> None:
        """Stop the job whatever state it is in; a job already finished is not an error."""
        ...

    async def logs(self, handle: JobHandle, *, tail: int | None = None) -> list[str]:
        """The job's output lines so far -- the last ``tail`` of them when asked. Empty when the
        scheduler has nothing yet (a job still queued)."""
        ...
