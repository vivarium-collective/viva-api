"""The science analysis SMS chains onto a compose run (``docs/plan-core.md`` P3c).

This lived inside ``viva_api/compose/simulation_service_ray.py`` as ``_submit_analysis_job``, and it
was the whole reason compose imported SMS application code: it runs the SIMULATOR's analysis modules
over a compose run's output, reading the run's own ParCa cache. That is SMS's business, not a
composite runner's. Compose now declares a hook -- ``AfterSubmit``: "a run was submitted; here is its
job, its image and its key" -- and the composition root (``dependencies.py``) hands it this.

The body is the method's, verbatim, but for one spelling: the image arrives as an argument instead
of being asked of the compose service (``self._image_uri(commit)`` -> ``image``).
"""

import logging
import random
import string
from typing import Any, Protocol

from viva_api.common import analysis_dag
from viva_api.common.storage import data_layout
from viva_api.compose.models import ComposeSimulation
from viva_api.config import get_settings
from viva_api.simulation.dispatch.image_paths import ANALYSIS_OUT_DIR, V2ECOLI_DIR

logger = logging.getLogger(__name__)


def _rand_suffix(n: int = 6) -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=n))


class AnalysisBatch(Protocol):
    """What chaining an analysis asks of the Batch layer: one container job, in the run's image."""

    def ensure_container_job_def(self, image: str, commit: str) -> str: ...

    @property
    def submit_container(self) -> analysis_dag.SubmitContainerFn: ...


class ComposeAnalysisChainer:
    """``AfterSubmit`` for SMS: chain the shared analysis DAG node onto a compose run's Batch job.

    Thin wrapper around ``viva_api.common.analysis_dag.submit_analysis_dag_node`` -- the SAME node the
    study Batch path submits, reused unmodified rather than re-derived. It owns only what is specific
    to a compose run: resolving the run's OWN output-store prefix and ParCa cache from
    ``experiment_id``/``commit`` (never a stock/unrelated per-commit path -- viva-api#448), and the
    ``analyses`` table's ``config`` record shape.

    Does nothing when the request carries no ``analysis_options``. ``simulation_id`` on the recorded
    row is left ``None``: ``ORMAnalysis.simulation_id`` is a real FK into the STUDY ``simulation``
    table, not compose's id space; ``experiment_id`` is what discovery keys off instead.
    """

    def __init__(self, batch: AnalysisBatch) -> None:
        self._batch = batch

    async def __call__(
        self,
        *,
        simulation: ComposeSimulation,
        experiment_id: str,
        sim_job_id: str,
        commit: str | None,
        image: str,
    ) -> str | None:
        analysis_options = simulation.sim_request.analysis_options
        if not analysis_options:
            # Defensive -- the caller already gates on this, but keep this method
            # self-sufficient (and mypy-narrowed to `dict[str, Any]` below).
            return None
        from viva_api.dependencies import get_database_service

        database_service = get_database_service()
        if database_service is None:
            logger.error(
                "Database service not initialized; skipping analysis chaining for compose experiment %s",
                experiment_id,
            )
            return None

        settings = get_settings()
        # The SAME commit-or-deploy-tag key `_parca_staging` uses to stage this exact
        # sim job's own ParCa cache -- i.e. this compose run's OWN cache, not a
        # separately-derived stock path.
        cache_key = commit or settings.compose_ray_image_tag
        sweep_dir = data_layout.RayLayout.results_uri(experiment_id).rstrip("/")
        sim_data_uri = f"{data_layout.RayLayout.parca_cache_uri(cache_key)}simData.cPickle"
        analysis_name = f"compose-analysis-{experiment_id[:20]}-{_rand_suffix()}"
        result_uri = f"{sweep_dir}/analyses/{analysis_name}"
        job_def = self._batch.ensure_container_job_def(image, cache_key)
        db_config: dict[str, Any] = {
            "out_uri": sweep_dir,
            "analysis_name": analysis_name,
            "trigger": "compose-dispatch",
            # ORMAnalysis.to_dto() unconditionally reads config["analysis_options"]
            # (AnalysisConfigOptions requires experiment_id) -- mirror the shape the
            # study path already writes so to_dto() doesn't KeyError.
            "analysis_options": {
                "experiment_id": [experiment_id],
                **(analysis_options if isinstance(analysis_options, dict) else {}),
            },
        }
        return await analysis_dag.submit_analysis_dag_node(
            sweep_dir=sweep_dir,
            analysis_options=analysis_options,
            sim_data_uri=sim_data_uri,
            result_out_dir=result_uri,
            v2ecoli_dir=V2ECOLI_DIR,
            submit_container=self._batch.submit_container,
            job_definition=job_def,
            job_name=f"compose-analysis-{experiment_id}-{_rand_suffix()}"[:128],
            out_s3=data_layout.RayLayout.results_uri(experiment_id),
            container_out_dir=ANALYSIS_OUT_DIR,
            depends_on_job_id=sim_job_id,
            depends_type="SEQUENTIAL",
            tags={"Phase": "analysis", "Backend": "compose"},
            database_service=database_service,
            experiment_id=experiment_id,
            analysis_name=analysis_name,
            simulation_id=None,
            backend="ray",
            db_config=db_config,
            result_uri=result_uri,
        )
