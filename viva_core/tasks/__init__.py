"""The JobStore seam (plan P4b): the owner-ref ``Job`` record and the Protocol
the two task subsystems converge on. See ``viva_core.tasks.store``."""

from viva_core.tasks.store import InMemoryJobStore, Job, JobOwnerKind, JobStore

__all__ = ["InMemoryJobStore", "Job", "JobOwnerKind", "JobStore"]
