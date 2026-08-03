"""
API for funcs in this module:

Names: <ROUTER_NAME>_<ENDPOINT_NAME/PATHS>()

For example:

For the endpoint: /core/simulation/workflow: core_simulation_workflow
For the endpoint: /core/simulator/versions: core_simulator_versions

Then, add that func to the list within the examples dict comprehension below!
"""

import datetime
import random
from typing import Literal, cast

from viva_api.analysis.models import (
    AnalysisDomain,
    ExperimentAnalysisRequest,
    PtoolsAnalysisConfig,
    PtoolsAnalysisType,
)
from viva_api.common.gateway.utils import generate_analysis_request
from viva_api.common.simulator_defaults import DEFAULT_SIMULATOR
from viva_api.simulation.models import (
    AnalysisOptions,
    ParcaDatasetRequest,
    ParcaOptions,
    SimulatorVersion,
)

DEFAULT_NUM_SEEDS = 30
DEFAULT_NUM_GENERATIONS = 4


def get_test_ptools() -> ExperimentAnalysisRequest:
    def rand_ntp(start: int = 3, stop: int = 22) -> int:
        return random.randint(start, stop)

    return ExperimentAnalysisRequest(
        experiment_id="sms_multigeneration",
        multiseed=[
            PtoolsAnalysisConfig(
                name=PtoolsAnalysisType.REACTIONS,
                n_tp=rand_ntp(),
            )
        ],
        multigeneration=[
            PtoolsAnalysisConfig(
                name=PtoolsAnalysisType.REACTIONS,
                n_tp=rand_ntp(),
            )
        ],
    )


def get_analysis_multiseed_multigen() -> ExperimentAnalysisRequest:
    request = ExperimentAnalysisRequest(
        experiment_id="sms_multigeneration",
        multiseed=[
            PtoolsAnalysisConfig(
                name=PtoolsAnalysisType.REACTIONS,
                n_tp=5,
            ),
            PtoolsAnalysisConfig(name=PtoolsAnalysisType.RNA, n_tp=5),
        ],
        multigeneration=[
            PtoolsAnalysisConfig(
                name=PtoolsAnalysisType.REACTIONS,
                n_tp=5,
            ),
            PtoolsAnalysisConfig(name=PtoolsAnalysisType.RNA, n_tp=5),
        ],
    )

    return request


def get_analysis_ptools() -> ExperimentAnalysisRequest:
    return ExperimentAnalysisRequest(
        experiment_id="sim3-baseline-ca00",
        multiseed=[
            PtoolsAnalysisConfig(name=PtoolsAnalysisType.RNA, n_tp=8),
        ],
    )


def get_analysis_ptools2() -> ExperimentAnalysisRequest:
    requested_configs = AnalysisDomain.to_list()
    expid = "publication_multiseed_multigen-a7ae0b4e093e20e6_1762830572273"
    request = generate_analysis_request(experiment_id=expid, requested_configs=requested_configs)
    return request


def get_analysis_wcecoli_setD4() -> ExperimentAnalysisRequest:
    # _n_init_sims_wcecoli_setD4 = 30
    #     generations_wcecoli_setD4 = 4
    experiment_id_wcecoli_setD4 = """ \
        wcecoli_fig2_setD4_scaled-c6263425684df8c0_1763578699104-449b7de3d0de10a5_1763578788396
    """.strip()
    return ExperimentAnalysisRequest(
        experiment_id=experiment_id_wcecoli_setD4,
        multiseed=[
            PtoolsAnalysisConfig(
                name=PtoolsAnalysisType.REACTIONS,
                n_tp=8,
            ),
            PtoolsAnalysisConfig(name=PtoolsAnalysisType.PROTEINS, n_tp=8),
            PtoolsAnalysisConfig(name=PtoolsAnalysisType.RNA, n_tp=8),
        ],
    )


def get_analysis_api_multiseed() -> ExperimentAnalysisRequest:
    expid = "api_multiseed"
    return ExperimentAnalysisRequest(
        experiment_id=expid,
        multiseed=[
            PtoolsAnalysisConfig(
                name=PtoolsAnalysisType.REACTIONS,
                n_tp=8,
            ),
            PtoolsAnalysisConfig(name=PtoolsAnalysisType.PROTEINS, n_tp=8),
            PtoolsAnalysisConfig(name=PtoolsAnalysisType.RNA, n_tp=8),
        ],
    )


def get_parca_base() -> ParcaDatasetRequest:
    return ParcaDatasetRequest(
        simulator_version=SimulatorVersion(
            git_commit_hash=DEFAULT_SIMULATOR.git_commit_hash,
            git_repo_url=DEFAULT_SIMULATOR.git_repo_url,
            git_branch=DEFAULT_SIMULATOR.git_branch,
            database_id=25,
            created_at=datetime.datetime.now(),
        ),
        parca_config=ParcaOptions(),
    )


OMICS_ANALYSIS_MODULE_NAMES = cast(
    list[Literal["ptools_rxns", "ptools_rna", "ptools_proteins"]], ["ptools_rxns", "ptools_rna", "ptools_proteins"]
)


class OmicsAnalysisModuleConfig(
    dict[Literal["ptools_rxns", "ptools_rna", "ptools_proteins"], dict[Literal["n_tp"], int]]
):
    pass


def omics_analysis_config(n_tp: int) -> OmicsAnalysisModuleConfig:
    """
    Expected output (for example n_tp = 8):
        {
          "ptools_rxns": {
            "n_tp": 8
          },
          "ptools_rna": {
            "n_tp": 8
          },
          "ptools_proteins": {
            "n_tp": 8
          }
        }
    :param n_tp:
    :return:
    """
    time_window_spec = cast(dict[Literal["n_tp"], int], {"n_tp": n_tp})
    return OmicsAnalysisModuleConfig(
        zip(
            OMICS_ANALYSIS_MODULE_NAMES,
            [time_window_spec for _ in range(len(OMICS_ANALYSIS_MODULE_NAMES))],
            strict=False,
        )
    )


def analysis_options_omics(n_tp: int) -> AnalysisOptions:
    analysis_domains = ["single", "multigeneration", "multiseed"]
    return AnalysisOptions(
        **dict(
            zip(analysis_domains, [omics_analysis_config(n_tp) for _ in range(len(analysis_domains))], strict=False)
        ),  # type: ignore[arg-type]
    )


# example analyses
analysis_ptools = get_analysis_ptools()
analysis_wcecoli_setD4 = get_analysis_wcecoli_setD4()
analysis_api_multiseed = get_analysis_api_multiseed()
analysis_multiseed_multigen = get_analysis_multiseed_multigen()
analysis_request_base = generate_analysis_request(
    # experiment_id="publication_multiseed_multigen-a7ae0b4e093e20e6_1762830572273",
    experiment_id="sms_multigeneration",
    requested_configs=[AnalysisDomain.MULTIGENERATION, AnalysisDomain.MULTISEED],
)
analysis_test_ptools = get_test_ptools()

base_parca = get_parca_base()
