"""The mbp-tracked dispatch mechanism (``viva_api/simulation/ray/mbp_tracked.py``).

Moved out of ``test_ray_backend.py`` with the code (``docs/plan-core.md`` P2.1, PR 7): that file is
5,900 lines because the class it tested held five mechanisms, and it comes apart the same way, one
mechanism per PR. The tests below are unchanged but for six ``SimulationServiceRay()`` constructions the
command tests no longer need (the command is a function now); the shared doubles still live in
``test_ray_backend`` and are imported from there.
"""

from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tests.simulation.test_ray_backend import (
    _container_env_of,
    _container_settings,
    _fake_container_batch,
)
from viva_api.common.models import JobId
from viva_api.simulation.ray.mbp_tracked import mbp_tracked_command
from viva_api.simulation.simulation_service_ray import SimulationServiceRay

if TYPE_CHECKING:
    from viva_api.simulation.database_service import DatabaseServiceSQL
    from viva_api.simulation.models import SimulationRequest


class TestMbpTrackedCommand:
    """_mbp_tracked_command: Run 1's real missing-output fix (Alex's Option 1
    decision, 2026-09-06) -- a run_mbp_tracked.py dispatch, e.g. reactor_bird_coupled."""

    def test_includes_variant_and_all_optional_flags_when_set(self) -> None:
        cmd = mbp_tracked_command(
            variant="reactor-bird-coupled-batch-multigen",
            max_generations=3,
            duration_sec=7200,
            chunk=5,
            emitter="parquet",
            cache_dir="/app/v2ecoli/out/cache",
            single_daughters=True,
            carbon_exhaustion_arrest=True,
        )
        assert "cd /app/v2ecoli" in cmd
        assert "V2E_STUDIES_ROOT=/app/v2ecoli/.pbg/runs/phase0-xarray/studies" in cmd
        assert "python scripts/run_mbp_tracked.py" in cmd
        assert "--variant reactor-bird-coupled-batch-multigen" in cmd
        assert "--emitter parquet" in cmd
        assert "--cache-dir /app/v2ecoli/out/cache" in cmd
        assert "--max-generations 3" in cmd
        assert "--duration-sec 7200" in cmd
        assert "--chunk 5" in cmd
        assert "--carbon-exhaustion-arrest" in cmd
        assert "--no-single-daughters" not in cmd

    def test_omitted_optionals_produce_no_flags_and_default_single_daughters_true(self) -> None:
        cmd = mbp_tracked_command(
            variant="aggregator-cpa1",
            max_generations=None,
            duration_sec=None,
            chunk=None,
            emitter="parquet",
            cache_dir="/app/v2ecoli/out/cache",
            single_daughters=True,
            carbon_exhaustion_arrest=False,
        )
        assert "--max-generations" not in cmd
        assert "--duration-sec" not in cmd
        assert "--chunk" not in cmd
        assert "--carbon-exhaustion-arrest" not in cmd
        assert "--no-single-daughters" not in cmd

    def test_single_daughters_false_adds_no_single_daughters_flag(self) -> None:
        cmd = mbp_tracked_command(
            variant="aggregator-cpa1",
            max_generations=None,
            duration_sec=None,
            chunk=None,
            emitter="sqlite",
            cache_dir="/app/v2ecoli/out/cache",
            single_daughters=False,
            carbon_exhaustion_arrest=False,
        )
        assert "--no-single-daughters" in cmd
        assert "--emitter sqlite" in cmd

    def test_includes_the_real_run1_params_when_set(self) -> None:
        """Chris's own exact spec for a real Run 1 coupled dispatch (sms-ecoli#210,
        2026-09-06) -- Dispatch 370's own request only exercised variant/
        max_generations; this is the full set his real experiment needs."""
        cmd = mbp_tracked_command(
            variant="reactor-bird-coupled-batch-multigen",
            max_generations=16,
            duration_sec=50400,
            chunk=1,
            emitter="parquet",
            cache_dir="/app/v2ecoli/out/cache",
            single_daughters=True,
            carbon_exhaustion_arrest=False,
            seed=3,
            cells_per_agent=9e10,
            initial_glucose_mM=111,
            initial_ammonium_mM=92.9,
            injected_processes="workspace/studies/cd2-pnnl-03-od10-batch/injection_vio_gfp_v0.json",
            reactor_config="workspace/studies/cd2-pnnl-03-od10-batch/reactor_route1_pnnl_aerobic_top.json",
            aeration_schedule="workspace/studies/cd2-pnnl-03-od10-batch/aeration_ramp_route1_density.json",
            aeration_trigger="biomass",
        )
        assert "--seed 3" in cmd
        assert "--cells-per-agent 90000000000.0" in cmd
        assert "--initial-glucose-mM 111" in cmd
        assert "--initial-ammonium-mM 92.9" in cmd
        assert (
            "--injected-processes /app/v2ecoli/workspace/studies/cd2-pnnl-03-od10-batch/injection_vio_gfp_v0.json"
            in cmd
        )
        assert (
            "--reactor-config /app/v2ecoli/workspace/studies/cd2-pnnl-03-od10-batch/"
            "reactor_route1_pnnl_aerobic_top.json" in cmd
        )
        assert (
            "--aeration-schedule /app/v2ecoli/workspace/studies/cd2-pnnl-03-od10-batch/"
            "aeration_ramp_route1_density.json" in cmd
        )
        assert "--aeration-trigger biomass" in cmd

    def test_aeration_trigger_suppressed_without_a_schedule(self) -> None:
        """sms-ecoli#334's real gap (2026-09-10): the local run_mbp_tracked.py
        couples --aeration-trigger to --aeration-schedule (the trigger is
        meaningless without a schedule to apply it to) -- this dispatch path
        must mirror that coupling rather than emit a dangling flag."""
        cmd = mbp_tracked_command(
            variant="reactor-bird-coupled-batch-multigen",
            max_generations=2,
            duration_sec=None,
            chunk=None,
            emitter="parquet",
            cache_dir="/app/v2ecoli/out/cache",
            single_daughters=True,
            carbon_exhaustion_arrest=False,
            aeration_trigger="biomass",
        )
        assert "--aeration-trigger" not in cmd

    def test_run1_params_omitted_by_default_byte_for_byte_unaffected(self) -> None:
        """Every existing caller (Dispatch 370's own shape) omits all 7 -- must
        stay byte-for-byte identical to before this extension."""
        cmd = mbp_tracked_command(
            variant="reactor-bird-coupled-batch-multigen",
            max_generations=2,
            duration_sec=None,
            chunk=None,
            emitter="parquet",
            cache_dir="/app/v2ecoli/out/cache",
            single_daughters=True,
            carbon_exhaustion_arrest=False,
        )
        assert cmd == (
            "cd /app/v2ecoli && V2E_STUDIES_ROOT=/app/v2ecoli/.pbg/runs/phase0-xarray/studies "
            "python scripts/run_mbp_tracked.py --variant reactor-bird-coupled-batch-multigen "
            "--emitter parquet --cache-dir /app/v2ecoli/out/cache --max-generations 2"
        )


@pytest.mark.asyncio
class TestSubmitMbpTrackedDispatch:
    """_submit_mbp_tracked_dispatch + its routing off SimulationConfig.mbp_dispatch
    (backlog item 105/106's own Run 1 sibling gap, Alex's Option 1 decision,
    2026-09-06). Mirrors TestSubmitMultiNodeComposite's own cache_variant guard
    tests exactly (viva-api#437's parity discipline, applied at build time)."""

    @pytest.mark.asyncio
    async def test_routes_via_mbp_dispatch_before_other_shapes(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        setattr(  # noqa: B010
            experiment_request.config,
            "mbp_dispatch",
            {"variant": "reactor-bird-coupled-batch-multigen", "max_generations": 3},
        )
        experiment_request.config.generations = 5  # would satisfy chain-dispatch's own condition
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        mock_batch = _fake_container_batch(["mbp-parca-1", "mbp-tracked-1"])
        fake_file_service = AsyncMock()
        fake_file_service.upload_file = AsyncMock()

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.ray._seams.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
            patch("viva_api.simulation.ray._seams.boto3.client", return_value=mock_batch),
            patch("viva_api.dependencies.get_file_service", return_value=fake_file_service),
        ):
            job_id = await service.submit_ecoli_simulation_job(
                ecoli_simulation=simulation, database_service=database_service, correlation_id="corr-mbp"
            )

        assert job_id == JobId.ray("mbp-tracked-1")
        assert mock_batch.submit_job.call_count == 2
        parca_call, mbp_call = mock_batch.submit_job.call_args_list
        assert "dependsOn" not in parca_call.kwargs  # first job, nothing to depend on
        assert mbp_call.kwargs["dependsOn"] == [{"jobId": "mbp-parca-1", "type": "SEQUENTIAL"}]
        env = _container_env_of(mbp_call)
        assert "--variant reactor-bird-coupled-batch-multigen" in env["CONTAINER_JOB_CMD"]
        assert "--max-generations 3" in env["CONTAINER_JOB_CMD"]
        assert mbp_call.kwargs["tags"]["Variant"] == "reactor-bird-coupled-batch-multigen"

    @pytest.mark.asyncio
    async def test_missing_variant_raises_before_submitting_anything(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        setattr(experiment_request.config, "mbp_dispatch", {})  # noqa: B010
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        mock_batch = _fake_container_batch([])
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.ray._seams.get_settings", _container_settings),
            patch("viva_api.simulation.ray._seams.boto3.client", return_value=mock_batch),
            pytest.raises(ValueError, match="mbp_dispatch.variant is required"),
        ):
            await service.submit_ecoli_simulation_job(
                ecoli_simulation=simulation, database_service=database_service, correlation_id="corr-mbp-missing"
            )
        mock_batch.submit_job.assert_not_called()

    @pytest.mark.asyncio
    async def test_cache_variant_missing_content_fails_loud_instead_of_building_stock(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """The same viva-api#437 guard, applied here at build time rather than
        found later (the standing parity-check discipline) -- a variant cache
        is meant to already exist; this path never builds one itself."""
        setattr(  # noqa: B010
            experiment_request.config,
            "mbp_dispatch",
            {"variant": "reactor-bird-coupled-batch-multigen", "cache_variant": "cd2-run1-k4-candidate-v1-lambda050"},
        )
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        mock_batch = _fake_container_batch([])
        fake_file_service = AsyncMock()
        fake_file_service.upload_file = AsyncMock()
        fake_file_service.get_listing = AsyncMock(return_value=[])

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.ray._seams.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
            patch("viva_api.simulation.ray._seams.boto3.client", return_value=mock_batch),
            patch("viva_api.dependencies.get_file_service", return_value=fake_file_service),
            pytest.raises(ValueError, match="cd2-run1-k4-candidate-v1-lambda050"),
        ):
            await service.submit_ecoli_simulation_job(
                ecoli_simulation=simulation, database_service=database_service, correlation_id="corr-mbp-guard"
            )
        mock_batch.submit_job.assert_not_called()

    @pytest.mark.asyncio
    async def test_cache_variant_existing_content_skips_parca_job(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        setattr(  # noqa: B010
            experiment_request.config,
            "mbp_dispatch",
            {"variant": "reactor-bird-coupled-batch-multigen", "cache_variant": "cd2-run1-k4-candidate-v1-lambda050"},
        )
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        mock_batch = _fake_container_batch(["mbp-tracked-only"])
        fake_file_service = AsyncMock()
        fake_file_service.upload_file = AsyncMock()
        fake_file_service.get_listing = AsyncMock(return_value=[MagicMock()])

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.ray._seams.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
            patch("viva_api.simulation.ray._seams.boto3.client", return_value=mock_batch),
            patch("viva_api.dependencies.get_file_service", return_value=fake_file_service),
        ):
            job_id = await service.submit_ecoli_simulation_job(
                ecoli_simulation=simulation, database_service=database_service, correlation_id="corr-mbp-existing"
            )

        assert job_id == JobId.ray("mbp-tracked-only")
        assert mock_batch.submit_job.call_count == 1
        (call,) = mock_batch.submit_job.call_args_list
        assert "dependsOn" not in call.kwargs

    @pytest.mark.asyncio
    async def test_cache_variant_tag_names_the_variant(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """Mirrors TestSubmitMultiNodeComposite's own CacheVariant tag test
        (the standing parity-check discipline) -- Chris (cplong90, sms-
        ecoli#210) flagged that omitting cache_variant resolves to the stock
        cache with nothing on the job itself to show which cache actually
        ran."""
        setattr(  # noqa: B010
            experiment_request.config,
            "mbp_dispatch",
            {"variant": "reactor-bird-coupled-batch-multigen", "cache_variant": "cd2-run1-k4-candidate-v1-lambda050"},
        )
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        mock_batch = _fake_container_batch(["mbp-tag-set"])
        fake_file_service = AsyncMock()
        fake_file_service.upload_file = AsyncMock()
        fake_file_service.get_listing = AsyncMock(return_value=[MagicMock()])

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.ray._seams.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
            patch("viva_api.simulation.ray._seams.boto3.client", return_value=mock_batch),
            patch("viva_api.dependencies.get_file_service", return_value=fake_file_service),
        ):
            await service.submit_ecoli_simulation_job(
                ecoli_simulation=simulation, database_service=database_service, correlation_id="corr-mbp-tag-set"
            )

        (call,) = mock_batch.submit_job.call_args_list
        assert call.kwargs["tags"]["CacheVariant"] == "cd2-run1-k4-candidate-v1-lambda050"

    @pytest.mark.asyncio
    async def test_omitted_cache_variant_tag_is_stock(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        setattr(  # noqa: B010
            experiment_request.config,
            "mbp_dispatch",
            {"variant": "reactor-bird-coupled-batch-multigen"},
        )
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        mock_batch = _fake_container_batch(["mbp-parca-tag", "mbp-tag-stock"])
        fake_file_service = AsyncMock()
        fake_file_service.upload_file = AsyncMock()

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.ray._seams.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
            patch("viva_api.simulation.ray._seams.boto3.client", return_value=mock_batch),
            patch("viva_api.dependencies.get_file_service", return_value=fake_file_service),
        ):
            await service.submit_ecoli_simulation_job(
                ecoli_simulation=simulation, database_service=database_service, correlation_id="corr-mbp-tag-stock"
            )

        _parca_call, mbp_call = mock_batch.submit_job.call_args_list
        assert mbp_call.kwargs["tags"]["CacheVariant"] == "stock"

    @pytest.mark.asyncio
    async def test_forwards_the_real_run1_params_to_the_submitted_command(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """Chris's own exact spec (sms-ecoli#210, 2026-09-06) end to end through
        submit_ecoli_simulation_job -- not just _mbp_tracked_command in isolation."""
        setattr(  # noqa: B010
            experiment_request.config,
            "mbp_dispatch",
            {
                "variant": "reactor-bird-coupled-batch-multigen",
                "cache_variant": "cd2-run1-k4-candidate-v1-lambda050",
                "max_generations": 16,
                "duration_sec": 50400,
                "chunk": 1,
                "seed": 7,
                "cells_per_agent": 9e10,
                "initial_glucose_mM": 111,
                "initial_ammonium_mM": 92.9,
                "injected_processes": "workspace/studies/cd2-pnnl-03-od10-batch/injection_vio_gfp_v0.json",
                "reactor_config": "workspace/studies/cd2-pnnl-03-od10-batch/reactor_route1_pnnl_aerobic_top.json",
                "aeration_schedule": "workspace/studies/cd2-pnnl-03-od10-batch/aeration_ramp_route1_density.json",
                "aeration_trigger": "biomass",
            },
        )
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        mock_batch = _fake_container_batch(["mbp-tracked-run1"])
        fake_file_service = AsyncMock()
        fake_file_service.upload_file = AsyncMock()
        fake_file_service.get_listing = AsyncMock(return_value=[MagicMock()])

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.ray._seams.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
            patch("viva_api.simulation.ray._seams.boto3.client", return_value=mock_batch),
            patch("viva_api.dependencies.get_file_service", return_value=fake_file_service),
        ):
            job_id = await service.submit_ecoli_simulation_job(
                ecoli_simulation=simulation, database_service=database_service, correlation_id="corr-mbp-run1"
            )

        assert job_id == JobId.ray("mbp-tracked-run1")
        (call,) = mock_batch.submit_job.call_args_list
        env = _container_env_of(call)
        cmd = env["CONTAINER_JOB_CMD"]
        assert "--seed 7" in cmd
        assert "--cells-per-agent 90000000000.0" in cmd
        assert "--initial-glucose-mM 111" in cmd
        assert "--initial-ammonium-mM 92.9" in cmd
        assert (
            "--injected-processes /app/v2ecoli/workspace/studies/cd2-pnnl-03-od10-batch/injection_vio_gfp_v0.json"
            in cmd
        )
        assert (
            "--reactor-config /app/v2ecoli/workspace/studies/cd2-pnnl-03-od10-batch/"
            "reactor_route1_pnnl_aerobic_top.json" in cmd
        )
        assert (
            "--aeration-schedule /app/v2ecoli/workspace/studies/cd2-pnnl-03-od10-batch/"
            "aeration_ramp_route1_density.json" in cmd
        )
        assert "--aeration-trigger biomass" in cmd
