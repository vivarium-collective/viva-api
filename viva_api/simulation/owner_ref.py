"""The owner-ref of an SMS record (plan P4a): one ``(owner_kind, owner_id)`` pair for what the tables
say with a foreign key each.

``owner_kind`` is the owning table's name -- ``simulation``, ``parca_dataset``, ``simulator``,
``analysis`` -- and ``owner_id`` its id as a string. A plain string, not an enum: core's ``Job`` record
carries the same two fields for its own kinds (``task``, ``env_worker``), and an application adds a kind
by writing rows, not by migrating an enum. The migration ``a4b6c8d0e2f4`` backfills these from the
foreign keys with the same rule, so the two agree for every row written before P4a.
"""

from viva_api.simulation.models import JobType

#: Which table owns a run of each job type. ``JobType.SIMULATION`` runs belong to a simulation, and so on.
OWNER_KIND_BY_JOB_TYPE: dict[JobType, str] = {
    JobType.SIMULATION: "simulation",
    JobType.PARCA: "parca_dataset",
    JobType.BUILD_IMAGE: "simulator",
    JobType.ANALYSIS: "analysis",
}


def run_owner(job_type: JobType, ref_id: int) -> tuple[str, str]:
    return OWNER_KIND_BY_JOB_TYPE[job_type], str(ref_id)


def dataset_owner(
    *, simulation_id: int | None, parca_dataset_id: int | None, analysis_id: int | None
) -> tuple[str, str] | None:
    """The FIRST producer named, in the order the table has always preferred them."""
    for kind, value in (("simulation", simulation_id), ("parca_dataset", parca_dataset_id), ("analysis", analysis_id)):
        if value is not None:
            return kind, str(value)
    return None
