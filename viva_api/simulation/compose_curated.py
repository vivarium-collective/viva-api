"""SMS's curated run of its own model through compose (``POST /compose/v1/curated/ecoli``).

Out of core's compose handlers (P3d-4d-1): it names the model's repository and its container mode,
which is one application's knowledge. It builds an OMEX around the templated document and hands it
to the generic ``run_compose_simulation`` like any other curated run.
"""

import json
import tempfile
import zipfile
from pathlib import Path

from fastapi import BackgroundTasks

from viva_core.compose.database_service import ComposeDatabaseService
from viva_core.compose.handlers import run_compose_simulation
from viva_core.compose.job_monitor import ComposeJobMonitor
from viva_core.compose.models import (
    ComposeSimulationExperiment,
    ComposeSimulationRequest,
    PBAllowList,
    SimulationFileType,
)
from viva_core.compose.service import ComposeSimulationService

_V2ECOLI_GIT_URL = "git+https://github.com/vivarium-collective/v2ecoli.git"


async def run_compose_v2ecoli(
    templated_pbif: str,
    duration: float,
    background_tasks: BackgroundTasks,
    db_service: ComposeDatabaseService,
    sim_service: ComposeSimulationService,
    job_monitor: ComposeJobMonitor,
    cache_dir: str = "out/cache",
    seed: int = 0,
    features: list[str] | None = None,
) -> ComposeSimulationExperiment:
    """Run a v2ecoli simulation — no SBML upload needed, just PBG template + duration."""
    with tempfile.TemporaryDirectory(delete=False) as tmp_dir:
        omex_path = Path(tmp_dir) / "input.omex"
        with zipfile.ZipFile(omex_path, "w") as omex:
            omex.writestr(data=templated_pbif, zinfo_or_arcname="v2ecoli.pbg")
        simulation_request = ComposeSimulationRequest(
            request_file_path=omex_path,
            simulation_file_type=SimulationFileType.OMEX,
            end_time_point=duration,
            is_batch=False,
        )
        return await run_compose_simulation(
            simulation_request=simulation_request,
            database_service=db_service,
            simulation_service=sim_service,
            job_monitor=job_monitor,
            # curated call site: the only extra dep it ever requests is its own trusted
            # v2ecoli origin, so its allow list is exactly that (not the global DB-backed
            # one users' uploads are checked against).
            pb_allow_list=PBAllowList(allow_list=[_V2ECOLI_GIT_URL]),
            background_tasks=background_tasks,
            extra_pip_deps=[_V2ECOLI_GIT_URL],
            override_command=json.dumps({
                "mode": "v2ecoli",
                "cache_dir": cache_dir,
                "seed": seed,
                "features": features or [],
            }),
        )
