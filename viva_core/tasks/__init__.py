"""The JobStore seam (plan P4b, D15): the owner-ref ``Job`` record and the ``JobReader`` /
``JobStore`` Protocols the task subsystems converge on. See ``viva_core.tasks.store``. The
in-memory store is a test double and lives in ``viva_core.tasks.testing``, not here."""

from viva_core.tasks.store import (
    ACTIVE_STATUSES,
    OWNER_CAMPAIGN,
    OWNER_ENV_WORKER,
    OWNER_TASK,
    Job,
    JobIsTerminal,
    JobReader,
    JobStore,
)

__all__ = [
    "ACTIVE_STATUSES",
    "OWNER_CAMPAIGN",
    "OWNER_ENV_WORKER",
    "OWNER_TASK",
    "Job",
    "JobIsTerminal",
    "JobReader",
    "JobStore",
]
