"""AWS Batch End-to-End Integration Tests.

These tests require real AWS infrastructure (Batch, S3, ECR) and issue real,
billable AWS Batch job submissions. Skipped unless AWS_BATCH_INTEGRATION=1 is
set -- never run automatically by `make test` / CI.

Run with:
    AWS_BATCH_INTEGRATION=1 AWS_PROFILE=stanford-sso \
    KUBECONFIG=~/.kube/kubeconfig_stanford_test.yaml \
    uv run pytest tests/integration/test_aws_batch_e2e.py -v -s

Prerequisites:
- CDK stack deployed (smsvpctest-batch)
- AWS credentials configured (AWS_PROFILE=stanford-sso)
- Kubeconfig for EKS cluster
- ECR repository exists (v2ecoli) with a built image for the resolved commit
  (the LATEST commit on AWS_BATCH_E2E_SMS_ECOLI_BRANCH, default "main",
  resolved live at test-run time -- not hardcoded, see
  _get_or_create_sms_ecoli_simulator -- so make sure that commit has actually
  been built and pushed before running this)

Pre-merge verification note (backlog items 33/34): the default branch is
"main" for ongoing post-merge regression use, but sms-ecoli's own checkpoint/
resume contract (PR #39 -- the 3 config keys _seed_generation_command always
sends) isn't on main until that PR merges. Running this against main before
then will reliably KeyError at container start (the exact bug PR #39's own
2nd commit fixed) -- that's not a false alarm, it correctly reflects that
main can't serve this dispatch shape yet. To actually pilot-verify PR #39
pre-merge, override the branch:
    AWS_BATCH_E2E_SMS_ECOLI_BRANCH=<PR #39's branch> AWS_BATCH_INTEGRATION=1 ...

What this covers (backlog item 33 -- individual per-seed AWS Batch job
chains): a genuine, small, real multiseed x multigeneration chain-dispatch
run against real AWS Batch on smsvpctest -- the actual pilot, formalized as a
reproducible test rather than a one-off manual CLI run. Submits real ParCa +
N_SEEDS independent per-seed job chains (N_GENERATIONS deep each), polls them
to terminal for real via SimulationServiceRay.get_chain_campaign_result (the
same mechanism JobScheduler.update_chain_campaigns polls on a real
deployment's own interval), submits the real analysis DAG node
(submit_campaign_analysis) once every chain is terminal, polls that to
terminal too, then verifies real cd1_*/ptools_* analysis artifacts actually
landed in S3 -- not just that the jobs exited zero.

Deliberately small by default (N_SEEDS=2, N_GENERATIONS=2 -- 1 ParCa + 4
per-seed-generation jobs + 1 analysis job = 6 real Batch submissions): this
proves the chain-dispatch MECHANISM for real, cheaply and repeatably. The
actual canonical 1000-seed x 10-generation dispatch is a separate, much more
expensive, explicitly-gated exercise (the real production pilot / "big
kahuna"), not something a routinely-rerunnable regression test should pay for
by default -- override AWS_BATCH_E2E_N_SEEDS / AWS_BATCH_E2E_N_GENERATIONS if
a larger real run is deliberately wanted.

Idempotent, like tests/integration/test_hpc_workflow.py: re-running with the
same resolved commit reuses the existing simulator/parca dataset/simulation/
campaign rather than re-dispatching real jobs that already ran, so an
interrupted or re-run test doesn't silently double-spend.
"""

import asyncio
import os
import time
from pathlib import Path

import pytest

from viva_api.common.hpc.job_service import JobStatusInfo
from viva_api.common.models import JobId, JobStatus
from viva_api.common.simulator_defaults import RepoUrl
from viva_api.common.storage.data_layout import RayLayout
from viva_api.common.storage.file_paths import S3FilePath
from viva_api.common.storage.file_service_s3 import FileServiceS3
from viva_api.simulation.database_service import DatabaseServiceSQL
from viva_api.simulation.models import (
    JobType,
    ParcaDatasetRequest,
    ParcaOptions,
    SimulationConfig,
    SimulationRequest,
    SimulatorVersion,
)
from viva_api.simulation.simulation_service_ray import SimulationServiceRay

pytestmark = pytest.mark.skipif(
    not os.getenv("AWS_BATCH_INTEGRATION"),
    reason="Set AWS_BATCH_INTEGRATION=1 to run real AWS Batch tests",
)

# Deliberately small and cheap by default -- see module docstring. Override via
# env for a bigger real run without editing this file.
N_SEEDS = int(os.getenv("AWS_BATCH_E2E_N_SEEDS", "2"))
N_GENERATIONS = int(os.getenv("AWS_BATCH_E2E_N_GENERATIONS", "2"))
# "main" for ongoing post-merge regression use; override to pilot-verify an
# unmerged sms-ecoli PR branch before it lands -- see module docstring.
SMS_ECOLI_BRANCH = os.getenv("AWS_BATCH_E2E_SMS_ECOLI_BRANCH", "main")
# A real ParCa + N*G real generation jobs (each its own container start, cache
# stage, and the actual v2ecoli computation) can take a while end to end even
# at this small scale; poll generously rather than risk a false failure from
# an over-tight timeout. 60 minutes default.
POLL_TIMEOUT_SECONDS = int(os.getenv("AWS_BATCH_E2E_TIMEOUT_SECONDS", str(60 * 60)))
POLL_INTERVAL_SECONDS = 20

TEST_EXPERIMENT_ID_PREFIX = "test-e2e-chain-dispatch"


async def _get_or_create_sms_ecoli_simulator(
    database_service: DatabaseServiceSQL, ray_service: SimulationServiceRay
) -> SimulatorVersion:
    """Resolve the LATEST real sms-ecoli commit live on SMS_ECOLI_BRANCH (not a
    hardcoded value that would drift out of date -- mirrors DEFAULT_COMMIT's
    own "should be latest" intent for the vEcoli-private default), reusing an
    existing DB row for that exact commit if a prior run already created one."""
    commit = await ray_service.get_latest_commit_hash(
        git_repo_url=RepoUrl.SMS_ECOLI_REPO_URL, git_branch=SMS_ECOLI_BRANCH
    )
    for simulator in await database_service.list_simulators():
        if simulator.git_commit_hash == commit and simulator.git_repo_url == RepoUrl.SMS_ECOLI_REPO_URL:
            return simulator
    return await database_service.insert_simulator(
        git_commit_hash=commit, git_repo_url=RepoUrl.SMS_ECOLI_REPO_URL, git_branch=SMS_ECOLI_BRANCH
    )


async def _get_or_create_parca_dataset(database_service: DatabaseServiceSQL, simulator: SimulatorVersion) -> int:
    for parca in await database_service.list_parca_datasets():
        if parca.parca_dataset_request.simulator_version.database_id == simulator.database_id:
            return parca.database_id
    parca_dataset_request = ParcaDatasetRequest(simulator_version=simulator, parca_config=ParcaOptions())
    parca_dataset = await database_service.insert_parca_dataset(parca_dataset_request=parca_dataset_request)
    return parca_dataset.database_id


@pytest.mark.asyncio
async def test_chain_dispatch_multiseed_multigeneration_against_real_aws_batch(
    database_service: DatabaseServiceSQL,
) -> None:
    """Real, genuine multiseed x multigeneration chain-dispatch run against
    real AWS Batch (backlog item 33). See module docstring for full context.

    Calls SimulationServiceRay.submit_chain_dispatch_job directly (not through
    the HTTP API or run_simulation_workflow): the real-entrypoint ROUTING
    decision (does a canonical batch_baseline request actually reach
    chain-dispatch) is already covered for real, at zero AWS cost, by
    tests/integration/test_k8s_workflow_mock.py's
    test_ray_canonical_batch_baseline_routes_through_real_entrypoint_to_chain_dispatch.
    This test's job is different: prove the chain-dispatch MECHANISM itself
    (per-seed job chains, dependsOn resolution, the analysis fan-in poll, and
    the real analysis artifacts it produces) against real AWS infrastructure.
    """
    ray_service = SimulationServiceRay()
    simulator = await _get_or_create_sms_ecoli_simulator(database_service, ray_service)
    parca_dataset_id = await _get_or_create_parca_dataset(database_service, simulator)

    experiment_id = f"{TEST_EXPERIMENT_ID_PREFIX}-{simulator.git_commit_hash}-{N_SEEDS}x{N_GENERATIONS}"

    # Idempotency: reuse an existing simulation for this exact (commit, seeds,
    # generations) shape if one already exists (e.g. a prior run interrupted
    # mid-poll) instead of re-dispatching N*G real jobs a second time.
    simulation = await database_service.get_simulation_by_experiment_id(experiment_id)
    if simulation is None:
        config = SimulationConfig(experiment_id=experiment_id, generations=N_GENERATIONS)
        setattr(config, "n_init_sims", N_SEEDS)  # noqa: B010 -- extra field, SimulationConfig allows it
        simulation_request = SimulationRequest(
            simulation_config_filename="api_simulation_default.json",
            experiment_id=experiment_id,
            simulator_id=simulator.database_id,
            parca_dataset_id=parca_dataset_id,
            config=config,
        )
        simulation = await database_service.insert_simulation(sim_request=simulation_request)

    campaign = await database_service.get_hpcrun_by_ref(ref_id=simulation.database_id, job_type=JobType.SIMULATION)
    if campaign is None or not campaign.chain_final_job_ids:
        job_id = await ray_service.submit_chain_dispatch_job(simulation, database_service)
        assert job_id is not None
        campaign = await database_service.get_hpcrun_by_ref(ref_id=simulation.database_id, job_type=JobType.SIMULATION)

    assert campaign is not None, "No campaign HpcRun row was recorded by submit_chain_dispatch_job"
    tracked_job_ids = campaign.chain_final_job_ids or []
    assert tracked_job_ids, "chain-dispatch tracked zero seed chains -- every seed failed generation 0 submission"
    assert len(tracked_job_ids) == N_SEEDS, (
        f"expected {N_SEEDS} tracked seed chains, got {len(tracked_job_ids)}: {tracked_job_ids}"
    )

    # Poll every tracked seed chain's own final job to terminal. Real AWS
    # Batch dependency resolution advances each chain natively (generation
    # g+1 doesn't start until generation g SUCCEEDED) -- this loop only
    # detects completion, exactly like JobScheduler.update_chain_campaigns
    # does on a real deployment's own polling interval.
    start_time = time.time()
    result = ray_service.get_chain_campaign_result(tracked_job_ids)
    while not result.terminal and time.time() - start_time < POLL_TIMEOUT_SECONDS:
        await asyncio.sleep(POLL_INTERVAL_SECONDS)
        result = ray_service.get_chain_campaign_result(tracked_job_ids)

    assert result.terminal, (
        f"Chain-dispatch campaign did not reach terminal state within {POLL_TIMEOUT_SECONDS}s "
        f"({len(result.succeeded_job_ids)} succeeded, {len(result.failed_job_ids)} failed so far, "
        f"{len(tracked_job_ids) - len(result.succeeded_job_ids) - len(result.failed_job_ids)} still pending)"
    )
    assert result.succeeded_job_ids, f"Every one of {len(tracked_job_ids)} seed chains failed: {result.failed_job_ids}"

    # Fire (or reuse, if already fired by a prior interrupted run) the
    # analysis DAG node for real -- exactly what JobScheduler.
    # _advance_chain_campaign does once its own poll detects all-terminal.
    existing_analyses = await database_service.list_analyses(simulation_id=simulation.database_id)
    non_failed = [a for a in existing_analyses if a.status != JobStatus.FAILED]
    if non_failed:
        analysis_job_id: str | None = non_failed[-1].job_id_ext
    else:
        analysis_job_id = await ray_service.submit_campaign_analysis(
            simulation=simulation,
            database_service=database_service,
            commit=simulator.git_commit_hash,
            total_n_seeds=N_SEEDS,
            n_generations=N_GENERATIONS,
        )
    assert analysis_job_id is not None, "Analysis DAG node submission failed -- see the recorded FAILED analyses row"

    analysis_start = time.time()
    analysis_status: JobStatusInfo | None = None
    while time.time() - analysis_start < POLL_TIMEOUT_SECONDS:
        analysis_status = await ray_service.get_job_status(JobId.ray(analysis_job_id))
        if analysis_status is not None and analysis_status.status in (
            JobStatus.COMPLETED,
            JobStatus.FAILED,
            JobStatus.CANCELLED,
        ):
            break
        await asyncio.sleep(POLL_INTERVAL_SECONDS)

    assert analysis_status is not None, "Analysis job status never resolved"
    assert analysis_status.status == JobStatus.COMPLETED, (
        f"Analysis job did not succeed: status={analysis_status.status}, error={analysis_status.error_message}"
    )

    # Verify real cd1_*/ptools_* artifacts actually landed in S3 -- the whole
    # point of the canonical dispatch (see ecosystem/CLAUDE.md's own MVP
    # definition: "ensure that all cd1_* and ptools_* analysis artifacts are
    # generated as expected"), not just "the jobs exited zero."
    file_service = FileServiceS3()
    try:
        analyses_prefix = f"{RayLayout.experiment_prefix(experiment_id)}/analyses"
        listing = await file_service.get_listing(S3FilePath(s3_path=Path(analyses_prefix)))
    finally:
        await file_service.close()

    artifact_keys = [item.Key for item in listing]
    assert artifact_keys, f"No analysis artifacts found under s3://<bucket>/{analyses_prefix}/"
    assert any("cd1_" in key or "ptools_" in key for key in artifact_keys), (
        f"Landed artifacts don't include any cd1_*/ptools_* analysis output: {artifact_keys[:10]}"
    )
