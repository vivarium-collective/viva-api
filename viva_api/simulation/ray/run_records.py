"""Recording a dispatch's own ``HpcRun`` row.

A function, not a method: it never touched ``self``. It sat on ``SimulationServiceRay`` as
``_record_run_with_companions`` and is what every dispatch mechanism that submits a job AHEAD of the
one it returns calls to write those companion job ids down (viva-api#709) -- so a mechanism that
becomes a strategy object (``docs/plan-core.md`` P2.1, PRs 7-11) can call it without being handed
the service. Moved verbatim; only ``self`` is gone.
"""

from viva_api.common.models import JobId
from viva_api.simulation.database_service import DatabaseService
from viva_api.simulation.models import JobType


async def record_run_with_companions(
    database_service: DatabaseService,
    *,
    job_id: JobId,
    simulation_id: int,
    correlation_id: str | None,
    companion_job_ids: list[str],
) -> None:
    """Record this dispatch's OWN HpcRun row, carrying the Batch jobs it submitted besides
    the tracked one (viva-api#709).

    A path that submits ParCa ahead of the job it returns used to keep the ParCa id only
    as that job's ``dependsOn``. The row the generic caller then inserted knew one job, so
    a cancel terminated one job: ParCa ran on under a CANCELLED row, and the terminated
    job sat PENDING behind it until it finished.

    Same pattern, same reason, as ``submit_chain_dispatch_job`` and
    ``_submit_multi_node_composite``: the row needs a field the generic caller cannot
    populate. It is inserted under the caller's ``correlation_id``, which is exactly what
    the caller's insert-if-absent guard keys on, so there is still one row per run.

    No companions, or no correlation id to key the guard on: nothing is recorded and the
    generic caller inserts its row as before.
    """
    if not companion_job_ids or not correlation_id:
        return
    await database_service.insert_hpcrun(
        job_id=job_id,
        job_type=JobType.SIMULATION,
        ref_id=simulation_id,
        correlation_id=correlation_id,
        external_job_ids=companion_job_ids,
    )
