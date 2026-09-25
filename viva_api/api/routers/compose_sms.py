"""SMS's own compose routes, served at the same ``/compose/v1`` prefix as core's compose router
(``viva_core.api.routers.compose``, P3d-4d-2): the curated run of ITS model. Core's router holds
everything generic; this one holds what names the application.
"""

import logging
import os

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from jinja2 import Template

from viva_api.config import get_settings
from viva_core.api.routers.compose import _require_db, _require_monitor, _require_sim
from viva_core.compose.models import ComposeSimulationExperiment

logger = logging.getLogger(__name__)

router = APIRouter()

_TEMPLATES_DIR = os.path.join(os.path.dirname(__file__), "templates")


@router.post(
    path="/curated/ecoli",
    operation_id="compose-run-v2ecoli",
    response_model=ComposeSimulationExperiment,
    tags=["Compose Curated"],
    summary="Run a v2ecoli whole-cell simulation via process-bigraph",
)
async def run_v2ecoli(
    background_tasks: BackgroundTasks,
    duration: float = Query(default=60.0, description="Simulation duration in seconds."),
    seed: int = Query(default=0, description="Random seed for stochastic processes."),
    interval: float = Query(default=1.0, description="Execution interval (timestep) in seconds."),
    features: str = Query(default="[]", description="JSON list of feature modules, e.g. '[\"ppgpp_regulation\"]'"),
    cache_dir: str = Query(
        default="/out/cache", description="Absolute path to pre-computed ParCa cache inside container."
    ),
) -> ComposeSimulationExperiment:
    """Run a v2ecoli whole-cell E. coli simulation.

    Unlike Copasi/Tellurium, v2ecoli does not require an SBML upload.
    The biological model is pre-computed in the ParCa cache and the
    55 biological processes are composed at runtime via process-bigraph.
    """
    import json as _json

    from viva_api.simulation.compose_curated import run_compose_v2ecoli

    # Parse features list
    try:
        features_list = _json.loads(features)
    except (ValueError, TypeError):
        raise HTTPException(400, f"Invalid features JSON: {features}")

    with open(os.path.join(_TEMPLATES_DIR, "v2ecoli.jinja")) as f:
        render = Template(f.read()).render(
            cache_dir=cache_dir,
            seed=seed,
            interval=interval,
            features=_json.dumps(features_list),
            output_dir=get_settings().compose_containers_output_dir,
        )

    return await run_compose_v2ecoli(
        templated_pbif=render,
        duration=duration,
        background_tasks=background_tasks,
        db_service=_require_db(),
        sim_service=_require_sim(),
        job_monitor=_require_monitor(),
        cache_dir=cache_dir,
        seed=seed,
        features=features_list,
    )


# ---------------------------------------------------------------------------
# BioModels endpoints
# ---------------------------------------------------------------------------
