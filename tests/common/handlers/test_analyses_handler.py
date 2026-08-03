import io
import logging
from pathlib import Path

import pandas as pd
import pytest
import pytest_asyncio

from viva_api.analysis.analysis_service import AnalysisServiceSlurm
from viva_api.analysis.models import ExperimentAnalysisRequest, PtoolsAnalysisConfig
from viva_api.common.handlers.analyses import handle_run_analysis_slurm
from viva_api.common.ssh.ssh_service import SSHSessionService
from viva_api.config import get_settings
from viva_api.simulation.database_service import DatabaseService
from viva_api.simulation.models import SimulatorVersion
from viva_api.simulation.simulation_service import SimulationServiceHpc


@pytest_asyncio.fixture
async def analysis_request() -> ExperimentAnalysisRequest:
    return ExperimentAnalysisRequest(
        experiment_id="sim3-baseline-ca00",
        multiseed=[
            PtoolsAnalysisConfig(name="ptools_rna", n_tp=22, variant=0),
            PtoolsAnalysisConfig(name="ptools_rxns", n_tp=22, variant=0),
        ],
    )


@pytest_asyncio.fixture
async def simulator_version() -> SimulatorVersion:
    return SimulatorVersion(**{  # type: ignore[arg-type]
        "git_commit_hash": "203ab2a",
        "git_repo_url": "https://github.com/vivarium-collective/vEcoli",
        "git_branch": "api-support",
        "database_id": 1,
        "created_at": "2026-02-04T21:08:38.272533",
    })


@pytest.mark.skipif(not Path(get_settings().slurm_submit_key_path).exists(), reason="slurm ssh key file not supplied")
@pytest.mark.asyncio
async def test_handle_run_analysis_slurm(
    analysis_request: ExperimentAnalysisRequest,
    database_service: DatabaseService,
    ssh_session_service: SSHSessionService,
    simulation_service_slurm: SimulationServiceHpc,
    simulator_version: SimulatorVersion,
    logger: logging.Logger,
) -> None:
    result = await handle_run_analysis_slurm(
        request=analysis_request,
        analysis_service=AnalysisServiceSlurm(env=get_settings()),
        simulator=simulator_version,
        logger=logger,
        db_service=database_service,
    )
    assert len(result) == 2
    for i in range(len(result)):
        table = pd.read_csv(io.StringIO(result[i].content), sep="\t")
        assert len(table.columns) - 1 == 22
