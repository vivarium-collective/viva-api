"""The multi-node composite dispatch mechanism (``viva_api/simulation/dispatch/multi_node.py``): its submit,
its founder-cache staging, its vCPU lookup, and the analysis that follows it.

Moved out of ``test_ray_backend.py`` with the code (``docs/plan-core.md`` P2.1, PR 9); the tests are
unchanged but for how they reach what moved. The shared doubles still live in ``test_ray_backend``.
"""

import json
import shlex
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tests.simulation.test_ray_backend import (
    _env_of,
    _fake_batch,
    _overrides_from_run_pbg_cmd,
    _ray_settings,
    _validated,
    validate_batch_parameters,
)
from viva_api.common.models import JobId
from viva_api.simulation.dispatch import multi_node, parca_spec
from viva_api.simulation.dispatch.image_paths import PARCA_CACHE_DIR, V2ECOLI_DIR
from viva_api.simulation.models import AnalysisOptions, JobType
from viva_api.simulation.simulation_service_ray import SimulationServiceRay

if TYPE_CHECKING:
    from viva_api.simulation.database_service import DatabaseServiceSQL
    from viva_api.simulation.models import SimulationRequest


def _fake_multi_node_batch(submit_ids: list[str], *, per_node_vcpus: int = 16) -> MagicMock:
    """Like _fake_batch, but also answers describe_job_definitions for the
    PER-COMMIT derived name (not just the CDK base) with a real
    resourceRequirements shape (backlog item 88's _mnp_node_vcpus reads this
    -- confirmed live 2026-08-24 against the real smsvpctest-ray-mnp job def).

    Matches the per-commit name GENERICALLY (any name != the base) rather than
    a hardcoded commit string -- the real commit hash is generated fresh per
    test run by the experiment_request/database_service fixtures, not a fixed
    value this fixture can know in advance."""
    b = MagicMock()
    base_node_props = {
        "numNodes": 4,
        "mainNode": 0,
        "nodeRangeProperties": [{"targetNodes": "0:", "container": {"image": "111.dkr.ecr.x/vecoli:ray", "vcpus": 16}}],
    }

    def _per_commit_props(image: str) -> dict[str, Any]:
        return {
            "numNodes": 4,
            "mainNode": 0,
            "nodeRangeProperties": [
                {
                    "targetNodes": "0:",
                    "container": {
                        "image": image,
                        "resourceRequirements": [
                            {"type": "VCPU", "value": str(per_node_vcpus)},
                            {"type": "MEMORY", "value": "60000"},
                        ],
                    },
                }
            ],
        }

    def _describe(**kwargs: Any) -> dict[str, Any]:
        # Checked the way botocore checks it. This fake used to read ``jobDefinitionName`` and
        # ignore everything else, which is how a lookup passing a keyword the API does not have
        # (``revision``, viva-api#730) had two green tests here while failing on every real call.
        validate_batch_parameters("DescribeJobDefinitions", kwargs)
        name = kwargs.get("jobDefinitionName")
        if name is None and kwargs.get("jobDefinitions"):
            name = str(kwargs["jobDefinitions"][0]).partition(":")[0]  # "<name>:<revision>"
        if name == "smscdk-ray-mnp":
            return {"jobDefinitions": [{"revision": 7, "nodeProperties": base_node_props}]}
        if name and name.startswith("smscdk-ray-mnp-"):
            commit = name.removeprefix("smscdk-ray-mnp-")
            image = f"476270107793.dkr.ecr.us-gov-west-1.amazonaws.com/v2ecoli:{commit}"
            return {"jobDefinitions": [{"revision": 1, "nodeProperties": _per_commit_props(image)}]}
        return {"jobDefinitions": []}

    b.describe_job_definitions.side_effect = _describe
    b.register_job_definition.side_effect = _validated(
        "RegisterJobDefinition", lambda **kw: {"jobDefinitionName": kw["jobDefinitionName"], "revision": 1}
    )
    _answers = iter([{"jobId": jid} for jid in submit_ids])
    b.submit_job.side_effect = _validated("SubmitJob", lambda **kw: next(_answers))
    return b


class TestSubmitMultiNodeComposite:
    """Backlog item 88: a generic multi-node process-bigraph composite dispatch
    (e.g. a colony composite), routed via SimulationConfig's extra
    `multi_node_dispatch` field. Never references any one composite by name --
    colony is the motivating case, not a hardcoded target."""

    @pytest.mark.asyncio
    async def test_multi_node_dispatch_routes_before_chain_dispatch_even_when_generations_over_one(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """The critical ordering guard: composite is None + generations>1 would
        otherwise satisfy chain-dispatch's own routing condition. A
        multi_node_dispatch request must win that race, not silently fall
        through to chain-dispatch."""
        setattr(  # noqa: B010
            experiment_request.config,
            "multi_node_dispatch",
            {"composite_id": "some_workspace.composites.some_multi_node_composite", "num_nodes": 2, "params": {}},
        )
        experiment_request.config.generations = 5  # would satisfy chain-dispatch's own condition
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        mock_batch = _fake_multi_node_batch(["parca-1", "composite-1"])
        fake_file_service = AsyncMock()
        fake_file_service.upload_file = AsyncMock()

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
            patch("viva_api.dependencies.get_file_service", return_value=fake_file_service),
        ):
            job_id = await service.submit_ecoli_simulation_job(
                ecoli_simulation=simulation, database_service=database_service, correlation_id="corr-mnp-composite"
            )

        # NOT chain-dispatch's JobBackend.LOCAL placeholder -- a real MNP job id.
        assert job_id == JobId.ray("composite-1")
        assert mock_batch.submit_job.call_count == 2
        # viva-api#709: the ParCa job it waits on is recorded on the run's own row, so a cancel
        # can stop it too.
        run = await database_service.get_hpcrun_by_ref(ref_id=simulation.database_id, job_type=JobType.SIMULATION)
        assert run is not None
        assert run.external_job_ids == ["parca-1"]

    @pytest.mark.asyncio
    async def test_multi_node_composite_command_and_tags(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        setattr(  # noqa: B010
            experiment_request.config,
            "multi_node_dispatch",
            {
                "composite_id": "some_workspace.composites.some_multi_node_composite",
                "num_nodes": 2,
                "params": {"n_cells": 6, "env_size": 20},
                "steps": 3,
            },
        )
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        mock_batch = _fake_multi_node_batch(["parca-9", "composite-9"], per_node_vcpus=16)
        fake_file_service = AsyncMock()
        fake_file_service.upload_file = AsyncMock()

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
            patch("viva_api.dependencies.get_file_service", return_value=fake_file_service),
        ):
            job_id = await service.submit_ecoli_simulation_job(
                ecoli_simulation=simulation, database_service=database_service, correlation_id="corr-mnp-cmd"
            )

        assert job_id == JobId.ray("composite-9")
        parca_call, composite_call = mock_batch.submit_job.call_args_list

        # Real MNP job, gated on parca, N nodes -- same shape as the comparison-ensemble path.
        assert composite_call.kwargs["dependsOn"] == [{"jobId": "parca-9", "type": "SEQUENTIAL"}]
        assert composite_call.kwargs["nodeOverrides"]["numNodes"] == 2
        assert composite_call.kwargs["jobQueue"] == "smscdk-ray-mnp"  # genuine multi-node, never the standalone queue
        assert parca_call.kwargs["nodeOverrides"]["numNodes"] == 1

        cmd = _env_of(composite_call)["RAY_JOB_CMD"]
        # Reuses the existing generic run_pbg.py runner, not a new script.
        assert "run_pbg.py" in cmd
        assert "--composite-id some_workspace.composites.some_multi_node_composite" in cmd
        assert "-n 3" in cmd
        assert shlex.quote(json.dumps({"n_cells": 6, "env_size": 20})) in cmd
        # Sized from the job definition's REAL per-node vCPU declaration (16) x num_nodes (2).
        assert "RAY_SHARDS_DEFAULT=32" in cmd
        # No colony/composite-specific hardcoding anywhere in the built command.
        assert "colony" not in cmd.lower()
        # The run's identity rides as its own flag, NOT inside --overrides: run_pbg
        # injects it only if the composite declares experiment_id (sms-ecoli#166).
        assert f"--experiment-id {shlex.quote(str(simulation.config.experiment_id))}" in cmd
        assert "experiment_id" not in json.dumps({"n_cells": 6, "env_size": 20})

        assert composite_call.kwargs["tags"]["CompositeId"] == "some_workspace.composites.some_multi_node_composite"

    @pytest.mark.asyncio
    async def test_multi_node_dispatch_threads_top_level_swap_processes_into_composite_params(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """sms-ecoli#166 (2026-09-09): a config's top-level ``swap_processes`` /
        ``exclude_processes`` (the committed CD2 Run 4 shape) must reach the
        multi-node composite as ``params.injected_processes`` -- it never did,
        so every ``lineage_ray_batch`` Run 4 dispatch ran classic metabolism
        while its stored config declared the redux swap (787's traceback in
        ``v2ecoli/processes/metabolism.py``; 744's classic-only FBA listeners)."""
        setattr(  # noqa: B010
            experiment_request.config,
            "swap_processes",
            {"ecoli-metabolism": "ecoli-metabolism-redux"},
        )
        setattr(experiment_request.config, "exclude_processes", ["exchange_data"])  # noqa: B010
        setattr(  # noqa: B010
            experiment_request.config,
            "multi_node_dispatch",
            {
                "composite_id": "v2ecoli.composites.lineage_ray_batch",
                "num_nodes": 4,
                "params": {"media": "minimal_plus_tryptophan", "emitter": "parquet", "n_seeds": 4, "n_generations": 8},
            },
        )
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        mock_batch = _fake_multi_node_batch(["parca-swap", "composite-swap"])
        fake_file_service = AsyncMock()
        fake_file_service.upload_file = AsyncMock()

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
            patch("viva_api.dependencies.get_file_service", return_value=fake_file_service),
        ):
            await service.submit_ecoli_simulation_job(
                ecoli_simulation=simulation, database_service=database_service, correlation_id="corr-mnp-swap"
            )

        _parca_call, composite_call = mock_batch.submit_job.call_args_list
        cmd = _env_of(composite_call)["RAY_JOB_CMD"]
        params = _overrides_from_run_pbg_cmd(cmd)
        assert params["media"] == "minimal_plus_tryptophan"
        assert params["injected_processes"]["swap_processes"] == {"ecoli-metabolism": "ecoli-metabolism-redux"}
        assert params["injected_processes"]["exclude_processes"] == ["exchange_data"]
        assert params["injected_processes"]["fork_repo"] == ""

    @pytest.mark.asyncio
    async def test_multi_node_dispatch_keeps_explicit_params_injected_processes(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """Explicit ``params.injected_processes`` wins over the config's own flat
        keys, and a config with no injection intent produces params without the
        key at all (byte-for-byte the pre-fix command)."""
        explicit = {"swap_processes": {"ecoli-metabolism": "custom-metabolism"}, "fork_repo": ""}
        setattr(experiment_request.config, "swap_processes", {"ecoli-metabolism": "ecoli-metabolism-redux"})  # noqa: B010
        setattr(  # noqa: B010
            experiment_request.config,
            "multi_node_dispatch",
            {
                "composite_id": "v2ecoli.composites.lineage_ray_batch",
                "num_nodes": 2,
                "params": {"media": "minimal", "injected_processes": explicit},
            },
        )
        simulation = await database_service.insert_simulation(sim_request=experiment_request)
        mock_batch = _fake_multi_node_batch(["parca-x", "composite-x"])
        fake_file_service = AsyncMock()
        fake_file_service.upload_file = AsyncMock()
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
            patch("viva_api.dependencies.get_file_service", return_value=fake_file_service),
        ):
            await service.submit_ecoli_simulation_job(
                ecoli_simulation=simulation, database_service=database_service, correlation_id="corr-mnp-explicit"
            )
        _parca_call, composite_call = mock_batch.submit_job.call_args_list
        params = _overrides_from_run_pbg_cmd(_env_of(composite_call)["RAY_JOB_CMD"])
        assert params["injected_processes"] == explicit

    @pytest.mark.asyncio
    async def test_n_generations_in_params_computes_required_run_interval(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """The K4-canary empty-emit bug (sms-ecoli#166 comment 5579146363,
        eagmon): a lineage-shaped composite's own contract is
        `n_generations * max_duration_per_gen` of TOTAL SIMULATED TIME, not a
        tick count -- `steps` silently defaulting to 1 invokes nothing (every
        ray:LineageProcess node's own interval is max_duration_per_gen, and
        process-bigraph only invokes a process whose next event falls inside
        the run window). `steps` omitted here -> computed from n_generations
        (3) x the composite's own documented max_duration_per_gen default
        (3600.0) = 10800."""
        setattr(  # noqa: B010
            experiment_request.config,
            "multi_node_dispatch",
            {
                "composite_id": "v2ecoli.composites.lineage_ray_batch",
                "num_nodes": 2,
                "params": {"n_seeds": 10, "n_generations": 3},
            },
        )
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        mock_batch = _fake_multi_node_batch(["parca-10", "composite-10"])
        fake_file_service = AsyncMock()
        fake_file_service.upload_file = AsyncMock()

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
            patch("viva_api.dependencies.get_file_service", return_value=fake_file_service),
        ):
            await service.submit_ecoli_simulation_job(
                ecoli_simulation=simulation, database_service=database_service, correlation_id="corr-mnp-required"
            )

        _parca_call, composite_call = mock_batch.submit_job.call_args_list
        cmd = _env_of(composite_call)["RAY_JOB_CMD"]
        assert "-n 10800" in cmd

    @pytest.mark.asyncio
    async def test_n_generations_honors_explicit_max_duration_per_gen(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        setattr(  # noqa: B010
            experiment_request.config,
            "multi_node_dispatch",
            {
                "composite_id": "v2ecoli.composites.lineage_ray_batch",
                "num_nodes": 2,
                "params": {"n_generations": 2, "max_duration_per_gen": 100.0},
            },
        )
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        mock_batch = _fake_multi_node_batch(["parca-11", "composite-11"])
        fake_file_service = AsyncMock()
        fake_file_service.upload_file = AsyncMock()

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
            patch("viva_api.dependencies.get_file_service", return_value=fake_file_service),
        ):
            await service.submit_ecoli_simulation_job(
                ecoli_simulation=simulation, database_service=database_service, correlation_id="corr-mnp-explicit-mdpg"
            )

        _parca_call, composite_call = mock_batch.submit_job.call_args_list
        cmd = _env_of(composite_call)["RAY_JOB_CMD"]
        assert "-n 200" in cmd

    @pytest.mark.asyncio
    async def test_explicit_steps_larger_than_required_run_interval_is_not_clamped_down(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """max(steps, required_run_interval): a caller's own deliberately
        larger steps value must survive, never get clamped down to the
        computed minimum."""
        setattr(  # noqa: B010
            experiment_request.config,
            "multi_node_dispatch",
            {
                "composite_id": "v2ecoli.composites.lineage_ray_batch",
                "num_nodes": 2,
                "params": {"n_generations": 1, "max_duration_per_gen": 100.0},
                "steps": 999,
            },
        )
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        mock_batch = _fake_multi_node_batch(["parca-12", "composite-12"])
        fake_file_service = AsyncMock()
        fake_file_service.upload_file = AsyncMock()

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
            patch("viva_api.dependencies.get_file_service", return_value=fake_file_service),
        ):
            await service.submit_ecoli_simulation_job(
                ecoli_simulation=simulation, database_service=database_service, correlation_id="corr-mnp-not-clamped"
            )

        _parca_call, composite_call = mock_batch.submit_job.call_args_list
        cmd = _env_of(composite_call)["RAY_JOB_CMD"]
        assert "-n 999" in cmd

    @pytest.mark.asyncio
    async def test_no_n_generations_in_params_is_byte_identical_to_before(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """A composite that never declares n_generations (e.g. a colony
        composite with its own unrelated params) is completely unaffected --
        steps stays at its own explicit value/silent default exactly as
        before this fix."""
        setattr(  # noqa: B010
            experiment_request.config,
            "multi_node_dispatch",
            {"composite_id": "some_workspace.composites.colony", "num_nodes": 2, "params": {"n_cells": 6}},
        )
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        mock_batch = _fake_multi_node_batch(["parca-13", "composite-13"])
        fake_file_service = AsyncMock()
        fake_file_service.upload_file = AsyncMock()

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
            patch("viva_api.dependencies.get_file_service", return_value=fake_file_service),
        ):
            await service.submit_ecoli_simulation_job(
                ecoli_simulation=simulation, database_service=database_service, correlation_id="corr-mnp-unaffected"
            )

        _parca_call, composite_call = mock_batch.submit_job.call_args_list
        cmd = _env_of(composite_call)["RAY_JOB_CMD"]
        assert "-n 1" in cmd

    @pytest.mark.asyncio
    async def test_require_clean_chain_reaches_the_composite_job_env(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """Item 106/#166 chassis-provenance thread (v2ecoli#735): a sibling of
        cache_variant inside multi_node_dispatch -- the actual mechanism Run 2/
        Run 4 dispatch through -- must reach the composite job's own env as the
        unprefixed V2E_REQUIRE_CLEAN_CHAIN, not the parca sub-job's."""
        setattr(  # noqa: B010
            experiment_request.config,
            "multi_node_dispatch",
            {
                "composite_id": "some_workspace.composites.some_multi_node_composite",
                "num_nodes": 2,
                "params": {},
                "require_clean_chain": True,
            },
        )
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        mock_batch = _fake_multi_node_batch(["parca-7", "composite-7"])
        fake_file_service = AsyncMock()
        fake_file_service.upload_file = AsyncMock()

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
            patch("viva_api.dependencies.get_file_service", return_value=fake_file_service),
        ):
            await service.submit_ecoli_simulation_job(
                ecoli_simulation=simulation, database_service=database_service, correlation_id="corr-clean-chain"
            )

        parca_call, composite_call = mock_batch.submit_job.call_args_list
        assert _env_of(composite_call)["V2E_REQUIRE_CLEAN_CHAIN"] == "1"
        # ParCa sub-job (the "off"/stock-cache branch) never claims a clean chain --
        # nothing has been staged yet for it to verify.
        assert "V2E_REQUIRE_CLEAN_CHAIN" not in _env_of(parca_call)

    @pytest.mark.asyncio
    async def test_omitted_require_clean_chain_is_byte_identical_to_before(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        setattr(  # noqa: B010
            experiment_request.config,
            "multi_node_dispatch",
            {"composite_id": "some_workspace.composites.some_multi_node_composite", "num_nodes": 2, "params": {}},
        )
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        mock_batch = _fake_multi_node_batch(["parca-8", "composite-8"])
        fake_file_service = AsyncMock()
        fake_file_service.upload_file = AsyncMock()

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
            patch("viva_api.dependencies.get_file_service", return_value=fake_file_service),
        ):
            await service.submit_ecoli_simulation_job(
                ecoli_simulation=simulation, database_service=database_service, correlation_id="corr-no-clean-chain"
            )

        _parca_call, composite_call = mock_batch.submit_job.call_args_list
        assert "V2E_REQUIRE_CLEAN_CHAIN" not in _env_of(composite_call)

    @pytest.mark.asyncio
    async def test_cache_variant_reaches_the_staged_cache_uri(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """Real gap, found live 2026-09-04 firing the first-ever real
        strain-specific pbg-native dispatch: this composite path never had
        cache_variant support at all -- only chain-dispatch did (job_
        scheduler.py's own already-proven getattr(simulation.config,
        "cache_variant", ...) pattern). Without it, a caller pointed at a
        real derived strain cache (POST /parca/new-gene-cache, viva-api#378)
        would silently stage the generic per-commit default instead."""
        setattr(  # noqa: B010
            experiment_request.config,
            "multi_node_dispatch",
            {
                "composite_id": "v2ecoli.composites.lineage_ray_batch",
                "num_nodes": 2,
                "params": {},
                "cache_variant": "cd2-run2-j3",
            },
        )
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        # Only ONE id: a variant cache is expected to already be staged, so
        # the guard added for backlog items 105/106 must skip the plain ParCa
        # rebuild below rather than run one -- see the two dedicated guard
        # tests right after this one.
        mock_batch = _fake_multi_node_batch(["composite-7"])
        fake_file_service = AsyncMock()
        fake_file_service.upload_file = AsyncMock()
        fake_file_service.get_listing = AsyncMock(return_value=[MagicMock()])  # cache already staged

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
            patch("viva_api.dependencies.get_file_service", return_value=fake_file_service),
            patch.object(parca_spec, "cache_s3_uri", wraps=parca_spec.cache_s3_uri) as mock_cache_s3_uri,
        ):
            await service.submit_ecoli_simulation_job(
                ecoli_simulation=simulation, database_service=database_service, correlation_id="corr-mnp-variant"
            )

        mock_cache_s3_uri.assert_called_once()
        assert mock_cache_s3_uri.call_args.kwargs.get("variant") == "cd2-run2-j3"
        assert mock_batch.submit_job.call_count == 1

    @pytest.mark.asyncio
    async def test_cache_variant_missing_content_fails_loud_instead_of_building_stock(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """The real bug this guard exists to close (backlog items 105/106,
        sms-ecoli#210, viva-api Dispatch 339:Run 1 / Dispatch 340:Run 2):
        this path used to run a plain `_parca_command()` unconditionally,
        with no check for pre-existing content, silently writing a stock/
        un-perturbed cache into a freshly-built commit's own cache_variant
        slot -- indistinguishable from a real strain's cache short of
        inspecting cache_version.json's own build_params by hand. A caller-
        requested variant cache that hasn't actually been staged yet must
        fail loud, not silently manufacture a substitute."""
        setattr(  # noqa: B010
            experiment_request.config,
            "multi_node_dispatch",
            {
                "composite_id": "v2ecoli.composites.lineage_ray_batch",
                "num_nodes": 2,
                "params": {},
                "cache_variant": "cd2-run1-k4-candidate-v1-lambda050",
            },
        )
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        mock_batch = _fake_multi_node_batch([])
        fake_file_service = AsyncMock()
        fake_file_service.upload_file = AsyncMock()
        fake_file_service.get_listing = AsyncMock(return_value=[])  # nothing staged at this commit yet

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
            patch("viva_api.dependencies.get_file_service", return_value=fake_file_service),
            pytest.raises(ValueError, match="cd2-run1-k4-candidate-v1-lambda050"),
        ):
            await service.submit_ecoli_simulation_job(
                ecoli_simulation=simulation, database_service=database_service, correlation_id="corr-mnp-missing"
            )

        # Fails BEFORE submitting anything -- no partial dispatch, no stock
        # cache silently built under the candidate's own name.
        mock_batch.submit_job.assert_not_called()

    @pytest.mark.asyncio
    async def test_cache_variant_existing_content_skips_parca_job_entirely(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """The other half of the guard: when a variant cache genuinely IS
        already staged, the composite job must submit directly -- no parca
        job, no dependsOn -- not merely fewer submit_job calls."""
        setattr(  # noqa: B010
            experiment_request.config,
            "multi_node_dispatch",
            {
                "composite_id": "v2ecoli.composites.lineage_ray_batch",
                "num_nodes": 2,
                "params": {},
                "cache_variant": "cd2-run2-j3-candidate-v1-lambda075",
            },
        )
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        mock_batch = _fake_multi_node_batch(["composite-only"])
        fake_file_service = AsyncMock()
        fake_file_service.upload_file = AsyncMock()
        fake_file_service.get_listing = AsyncMock(return_value=[MagicMock()])

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
            patch("viva_api.dependencies.get_file_service", return_value=fake_file_service),
        ):
            job_id = await service.submit_ecoli_simulation_job(
                ecoli_simulation=simulation, database_service=database_service, correlation_id="corr-mnp-existing"
            )

        assert job_id == JobId.ray("composite-only")
        assert mock_batch.submit_job.call_count == 1
        (composite_call,) = mock_batch.submit_job.call_args_list
        assert "dependsOn" not in composite_call.kwargs

    @pytest.mark.asyncio
    async def test_omitted_cache_variant_is_byte_identical_to_before(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """cache_variant omitted must resolve to the plain per-commit cache,
        unchanged from every existing caller's own behavior."""
        setattr(  # noqa: B010
            experiment_request.config,
            "multi_node_dispatch",
            {"composite_id": "v2ecoli.composites.lineage_ray_batch", "num_nodes": 2, "params": {}},
        )
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        mock_batch = _fake_multi_node_batch(["parca-8", "composite-8"])
        fake_file_service = AsyncMock()
        fake_file_service.upload_file = AsyncMock()

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
            patch("viva_api.dependencies.get_file_service", return_value=fake_file_service),
            patch.object(parca_spec, "cache_s3_uri", wraps=parca_spec.cache_s3_uri) as mock_cache_s3_uri,
        ):
            await service.submit_ecoli_simulation_job(
                ecoli_simulation=simulation, database_service=database_service, correlation_id="corr-mnp-no-variant"
            )

        mock_cache_s3_uri.assert_called_once()
        assert mock_cache_s3_uri.call_args.kwargs.get("variant") is None

    @pytest.mark.asyncio
    async def test_cache_variant_tag_names_the_variant(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """Chris (cplong90, sms-ecoli#210) flagged that omitting cache_variant
        resolves to the stock cache with nothing on the job itself to show
        which cache actually ran. CacheVariant makes the resolved choice
        visible directly on the AWS Batch job instead of requiring a manual
        decode of the staged S3 path."""
        setattr(  # noqa: B010
            experiment_request.config,
            "multi_node_dispatch",
            {
                "composite_id": "v2ecoli.composites.lineage_ray_batch",
                "num_nodes": 2,
                "params": {},
                "cache_variant": "cd2-run2-j3",
            },
        )
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        mock_batch = _fake_multi_node_batch(["composite-tag"])
        fake_file_service = AsyncMock()
        fake_file_service.upload_file = AsyncMock()
        fake_file_service.get_listing = AsyncMock(return_value=[MagicMock()])

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
            patch("viva_api.dependencies.get_file_service", return_value=fake_file_service),
        ):
            await service.submit_ecoli_simulation_job(
                ecoli_simulation=simulation, database_service=database_service, correlation_id="corr-mnp-tag-set"
            )

        (composite_call,) = mock_batch.submit_job.call_args_list
        assert composite_call.kwargs["tags"]["CacheVariant"] == "cd2-run2-j3"

    @pytest.mark.asyncio
    async def test_omitted_cache_variant_tag_is_stock(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        setattr(  # noqa: B010
            experiment_request.config,
            "multi_node_dispatch",
            {"composite_id": "v2ecoli.composites.lineage_ray_batch", "num_nodes": 2, "params": {}},
        )
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        mock_batch = _fake_multi_node_batch(["parca-tag", "composite-tag-stock"])
        fake_file_service = AsyncMock()
        fake_file_service.upload_file = AsyncMock()

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
            patch("viva_api.dependencies.get_file_service", return_value=fake_file_service),
        ):
            await service.submit_ecoli_simulation_job(
                ecoli_simulation=simulation, database_service=database_service, correlation_id="corr-mnp-tag-stock"
            )

        _parca_call, composite_call = mock_batch.submit_job.call_args_list
        assert composite_call.kwargs["tags"]["CacheVariant"] == "stock"

    def test_multi_node_composite_command_sets_pythonpath_for_injection_imports(self) -> None:
        """Direct unit test of _multi_node_composite_command's own command string
        (backlog item 93): a colony/multi-node composite can carry
        injected_processes the same way ecoli_baseline's chain-dispatch path can,
        so it needs the same PYTHONPATH fix, not just chain-dispatch's own two
        call sites (see TestSimulationServiceRayBuild/TestSeedGenerationCommand's
        sibling assertions)."""
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            cmd = multi_node.multi_node_composite_command(
                composite_id="some_workspace.composites.some_multi_node_composite",
                params={"n_cells": 6},
                steps=3,
                runner_s3_uri="s3://mybucket/vecoli-output/sim9-colony/run_pbg.py",
                n_shards_default=None,
            )
        assert "cd /app/v2ecoli" in cmd
        assert "PYTHONPATH=/app/v2ecoli" in cmd
        assert "python /tmp/run_pbg.py" in cmd

    @pytest.mark.asyncio
    async def test_multi_node_dispatch_requires_composite_id(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        setattr(experiment_request.config, "multi_node_dispatch", {"num_nodes": 2})  # noqa: B010
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
            pytest.raises(ValueError, match="composite_id"),
        ):
            await service.submit_ecoli_simulation_job(
                ecoli_simulation=simulation, database_service=database_service, correlation_id="corr-mnp-noid"
            )

    def test_mnp_node_vcpus_reads_real_resource_requirements(self) -> None:
        mock_batch = _fake_multi_node_batch(["unused"], per_node_vcpus=16)
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
        ):
            vcpus = service._multi_node()._mnp_node_vcpus("smscdk-ray-mnp-abc123:1")
        assert vcpus == 16

    def test_mnp_node_vcpus_returns_none_on_unknown_job_def(self) -> None:
        # A name outside _fake_multi_node_batch's own recognized prefixes (base
        # "smscdk-ray-mnp" or any "smscdk-ray-mnp-<commit>") -- genuinely
        # unmatched, unlike a per-commit-shaped name (which the fixture answers
        # generically, by design, since real per-commit revisions always exist
        # once _ensure_mnp_job_def has registered one).
        mock_batch = _fake_multi_node_batch(["unused"])
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
        ):
            assert service._multi_node()._mnp_node_vcpus("totally-different-job-def:1") is None

    def test_mnp_node_vcpus_retries_through_transient_empty_result_then_succeeds(self) -> None:
        """Backlog item 101: on a commit's FIRST-ever multi-node dispatch,
        describe_job_definitions can briefly return empty for a job def
        _ensure_mnp_job_def had JUST registered (AWS eventual consistency --
        confirmed live 2026-08-25, sim255). Simulates that: the first 2 calls
        return an empty jobDefinitions list (the real shape AWS returns, not
        an exception), the 3rd succeeds -- the retry loop must not give up
        early and must not treat the transient empty as a permanent unknown
        job def (contrast test_mnp_node_vcpus_returns_none_on_unknown_job_def,
        which never resolves)."""
        real_result = _fake_multi_node_batch(["unused"], per_node_vcpus=16).describe_job_definitions(
            jobDefinitionName="smscdk-ray-mnp-somecommit"
        )
        mock_batch = MagicMock()
        mock_batch.describe_job_definitions.side_effect = [
            {"jobDefinitions": []},
            {"jobDefinitions": []},
            real_result,
        ]
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch.object(multi_node.MultiNodeCompositeStrategy, "_VCPU_LOOKUP_BACKOFF_SECONDS", 0.0),
        ):
            vcpus = service._multi_node()._mnp_node_vcpus("smscdk-ray-mnp-somecommit:1")
        assert vcpus == 16
        assert mock_batch.describe_job_definitions.call_count == 3

    def test_mnp_node_vcpus_gives_up_after_exhausting_retries(self) -> None:
        """The other half of the same guard: a GENUINELY unknown/never-
        materializing job def must still return None eventually, not retry
        forever."""
        mock_batch = MagicMock()
        mock_batch.describe_job_definitions.return_value = {"jobDefinitions": []}
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch.object(multi_node.MultiNodeCompositeStrategy, "_VCPU_LOOKUP_BACKOFF_SECONDS", 0.0),
        ):
            assert service._multi_node()._mnp_node_vcpus("smscdk-ray-mnp-neverexists:1") is None
        assert (
            mock_batch.describe_job_definitions.call_count == multi_node.MultiNodeCompositeStrategy._VCPU_LOOKUP_RETRIES
        )

    @pytest.mark.asyncio
    async def test_existing_comparison_ensemble_path_unaffected_when_multi_node_dispatch_absent(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """Explicit regression proof (not just relying on the pre-existing
        comparison-ensemble/chain-dispatch tests staying green): the new
        routing check is a true no-op when multi_node_dispatch is absent."""
        assert getattr(experiment_request.config, "multi_node_dispatch", None) is None
        setattr(experiment_request.config, "composite", "vecoli")  # noqa: B010
        experiment_request.config.generations = 3
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        mock_batch = _fake_batch(["parca-123", "sim-456"])
        fake_file_service = AsyncMock()
        fake_file_service.upload_file = AsyncMock()

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
            patch("viva_api.dependencies.get_file_service", return_value=fake_file_service),
        ):
            job_id = await service.submit_ecoli_simulation_job(
                ecoli_simulation=simulation, database_service=database_service, correlation_id="corr-unaffected"
            )
        # Byte-for-byte the same outcome as test_composite_comparison_ensemble_with_multiple_generations_stays_on_mnp.
        assert job_id == JobId.ray("sim-456")
        assert mock_batch.submit_job.call_count == 2


def _fake_s3_paginator(pages: list[dict[str, Any]]) -> MagicMock:
    paginator = MagicMock()
    paginator.paginate.return_value = pages
    return paginator


class TestSeedOverrideCacheStaging:
    """Backlog item 106, 2026-09-09: seed_overrides[*].cache_dir is a raw s3://
    URI, but the dispatch only ever staged ONE (stage_s3, stage_dir) pair -- the
    base/chassis cache. v2ecoli passes an override's cache_dir straight through
    to a plain os.path.exists() check, which is unconditionally False for an
    s3:// string -- StaleCacheError regardless of whether the real object
    exists (confirmed via two real dispatches, database_id 733/738, each
    failing on a different seed). Fix: server-side copy each override's own S3
    prefix under the dispatch's own cache_s3 (picked up by the EXISTING
    recursive stage_s3->stage_dir sync), then rewrite cache_dir to the local
    path it resolves to."""

    @pytest.mark.asyncio
    async def test_seed_overrides_cache_dir_staged_and_rewritten_to_local_path(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        setattr(  # noqa: B010
            experiment_request.config,
            "multi_node_dispatch",
            {
                "composite_id": "v2ecoli.composites.lineage_ray_batch",
                "num_nodes": 2,
                "params": {
                    "n_seeds": 2,
                    "n_generations": 1,
                    "seed_overrides": {
                        "0": {"cache_dir": "s3://otherbucket/founders/seed0"},
                    },
                },
            },
        )
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        mock_batch = _fake_multi_node_batch(["parca-20", "composite-20"])
        mock_s3 = MagicMock()
        mock_s3.get_paginator.return_value = _fake_s3_paginator([
            {
                "Contents": [
                    {"Key": "founders/seed0/cache_version.json"},
                    {"Key": "founders/seed0/simData.cPickle"},
                ]
            }
        ])
        fake_file_service = AsyncMock()
        fake_file_service.upload_file = AsyncMock()

        def _boto3_client(service_name: str, **_kwargs: Any) -> MagicMock:
            return mock_s3 if service_name == "s3" else mock_batch

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", side_effect=_boto3_client),
            patch("viva_api.dependencies.get_file_service", return_value=fake_file_service),
        ):
            job_id = await service.submit_ecoli_simulation_job(
                ecoli_simulation=simulation, database_service=database_service, correlation_id="corr-seed-override"
            )

        assert job_id == JobId.ray("composite-20")

        # Every object under the source prefix was copied server-side, under
        # this dispatch's own cache_s3 prefix, keyed by seed.
        copy_calls = mock_s3.copy_object.call_args_list
        assert len(copy_calls) == 2
        for call in copy_calls:
            assert call.kwargs["Bucket"] == "mybucket"
            assert call.kwargs["CopySource"]["Bucket"] == "otherbucket"
            assert "_seed_overrides/0/" in call.kwargs["Key"]
        copied_keys = {c.kwargs["Key"].rsplit("/", 1)[-1] for c in copy_calls}
        assert copied_keys == {"cache_version.json", "simData.cPickle"}

        # The composite's own --overrides JSON carries the REWRITTEN, LOCAL
        # cache_dir -- not the original raw s3:// URI -- so v2ecoli's plain
        # os.path.exists() check (which the real bug traces to) finds a real
        # local path once the existing stage_s3->stage_dir sync lands it.
        _parca_call, composite_call = mock_batch.submit_job.call_args_list
        cmd = _env_of(composite_call)["RAY_JOB_CMD"]
        assert f"{PARCA_CACHE_DIR}/_seed_overrides/0" in cmd
        assert "s3://otherbucket" not in cmd

    @pytest.mark.asyncio
    async def test_no_seed_overrides_is_completely_unaffected(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """Every existing caller (no seed_overrides at all) must be byte-for-byte
        unaffected -- no S3 client touched, no extra behavior."""
        setattr(  # noqa: B010
            experiment_request.config,
            "multi_node_dispatch",
            {
                "composite_id": "v2ecoli.composites.lineage_ray_batch",
                "num_nodes": 2,
                "params": {"n_seeds": 2, "n_generations": 1},
            },
        )
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        mock_batch = _fake_multi_node_batch(["parca-21", "composite-21"])
        fake_file_service = AsyncMock()
        fake_file_service.upload_file = AsyncMock()

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
            patch("viva_api.dependencies.get_file_service", return_value=fake_file_service),
        ):
            job_id = await service.submit_ecoli_simulation_job(
                ecoli_simulation=simulation, database_service=database_service, correlation_id="corr-no-override"
            )

        assert job_id == JobId.ray("composite-21")
        assert not hasattr(mock_batch, "copy_object") or not mock_batch.copy_object.called

    @pytest.mark.asyncio
    async def test_seed_overrides_with_a_non_s3_cache_dir_passes_through_unchanged(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """A seed_overrides entry whose cache_dir is already a local path (or any
        other non-s3:// value) is left exactly as given -- no copy attempted,
        no rewrite. Only ever needed by a caller that pre-stages its own
        override some other way; not exercised by any real caller today."""
        setattr(  # noqa: B010
            experiment_request.config,
            "multi_node_dispatch",
            {
                "composite_id": "v2ecoli.composites.lineage_ray_batch",
                "num_nodes": 2,
                "params": {
                    "n_seeds": 2,
                    "n_generations": 1,
                    "seed_overrides": {"0": {"cache_dir": "/already/local/path"}},
                },
            },
        )
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        mock_batch = _fake_multi_node_batch(["parca-22", "composite-22"])
        mock_s3 = MagicMock()
        fake_file_service = AsyncMock()
        fake_file_service.upload_file = AsyncMock()

        def _boto3_client(service_name: str, **_kwargs: Any) -> MagicMock:
            return mock_s3 if service_name == "s3" else mock_batch

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", side_effect=_boto3_client),
            patch("viva_api.dependencies.get_file_service", return_value=fake_file_service),
        ):
            await service.submit_ecoli_simulation_job(
                ecoli_simulation=simulation, database_service=database_service, correlation_id="corr-local-override"
            )

        assert not mock_s3.copy_object.called
        _parca_call, composite_call = mock_batch.submit_job.call_args_list
        cmd = _env_of(composite_call)["RAY_JOB_CMD"]
        assert "/already/local/path" in cmd

    @pytest.mark.asyncio
    async def test_missing_cache_variant_still_fails_loud_even_with_seed_overrides_set(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """Regression for a real review finding (cd2-questions, 2026-09-09):
        seed_overrides staging must run AFTER the cache_variant existence
        check, not alongside cache_s3's own computation -- otherwise staging
        real objects into cache_s3's own prefix (under _seed_overrides/<seed>/)
        would make an UNBUILT chassis's own get_listing() come back non-empty,
        silently defeating the guard test_cache_variant_missing_content_
        fails_loud_instead_of_building_stock exists to prove. Both guards must
        hold at once: missing chassis content still raises, and no S3 copy
        happens before that raise."""
        setattr(  # noqa: B010
            experiment_request.config,
            "multi_node_dispatch",
            {
                "composite_id": "v2ecoli.composites.lineage_ray_batch",
                "num_nodes": 2,
                "params": {
                    "seed_overrides": {"0": {"cache_dir": "s3://otherbucket/founders/seed0"}},
                },
                "cache_variant": "cd2-run1-k4-candidate-v1-lambda050",
            },
        )
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        mock_batch = _fake_multi_node_batch([])
        mock_s3 = MagicMock()
        fake_file_service = AsyncMock()
        fake_file_service.upload_file = AsyncMock()
        fake_file_service.get_listing = AsyncMock(return_value=[])  # nothing staged at this commit yet

        def _boto3_client(service_name: str, **_kwargs: Any) -> MagicMock:
            return mock_s3 if service_name == "s3" else mock_batch

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", side_effect=_boto3_client),
            patch("viva_api.dependencies.get_file_service", return_value=fake_file_service),
            pytest.raises(ValueError, match="cd2-run1-k4-candidate-v1-lambda050"),
        ):
            await service.submit_ecoli_simulation_job(
                ecoli_simulation=simulation, database_service=database_service, correlation_id="corr-guard-order"
            )

        # The guard raised BEFORE any seed-override staging happened -- no S3
        # copy leaked ahead of the existence check, and nothing was submitted.
        assert not mock_s3.copy_object.called
        mock_batch.submit_job.assert_not_called()


class TestMultiNodeAnalysisCommand:
    """Item 109: _multi_node_analysis_command tries a hive-parquet read
    (matching run_standalone_analysis.py's own DuckDB mechanism) before
    falling back to the original flat-file path, when n_seeds is given."""

    def test_omits_seed_flags_entirely_when_n_seeds_not_given(self) -> None:
        """Byte-for-byte unchanged from before this item's fix -- colony's own
        real shape (item 88) never has n_seeds in the same sense."""
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            cmd = multi_node.multi_node_analysis_command(
                experiment_id="exp1",
                composite_id="v2ecoli.composites.ecoli_colony.ecoli_colony",
                history_uri="s3://bucket/exp1",
                out_uri="s3://bucket/exp1/analyses/a1",
            )
        assert "--n-seeds" not in cmd
        assert "--n-generations" not in cmd
        assert "--modules" not in cmd
        assert "run_multi_node_analysis.py" in cmd

    def test_applicable_keyword_rides_as_a_bare_token_not_json_encoded(self) -> None:
        """Regression for the real bug caught while building this: json.dumps
        would turn "applicable" into the 12-char string '"applicable"'
        (quotes included), which the receiving script's own
        `.strip().lower() == "applicable"` check would silently miss."""
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            cmd = multi_node.multi_node_analysis_command(
                experiment_id="exp1",
                composite_id="v2ecoli.composites.lineage_ray_batch",
                history_uri="s3://bucket/exp1",
                out_uri="s3://bucket/exp1/analyses/a1",
                n_seeds=10,
                n_generations=10,
                modules="applicable",
            )
        tokens = shlex.split(cmd.split("&&", 1)[1])
        assert tokens[tokens.index("--modules") + 1] == "applicable"
        assert "--n-seeds 10" in cmd
        assert "--n-generations 10" in cmd

    def test_explicit_module_mapping_rides_as_real_json(self) -> None:
        modules: dict[str, dict[str, Any]] = {"multiseed": {"doubling_time_distribution": {}}}
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            cmd = multi_node.multi_node_analysis_command(
                experiment_id="exp1",
                composite_id="v2ecoli.composites.lineage_ray_batch",
                history_uri="s3://bucket/exp1",
                out_uri="s3://bucket/exp1/analyses/a1",
                n_seeds=10,
                modules=modules,
            )
        tokens = shlex.split(cmd.split("&&", 1)[1])
        assert json.loads(tokens[tokens.index("--modules") + 1]) == modules

    def test_sim_data_uri_omitted_is_byte_identical_to_before(self) -> None:
        """viva-api#448's own new sim_data_uri param, unset, must not change
        this command at all -- every caller before #448 relied on this exact
        cd-then-python shape."""
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            cmd = multi_node.multi_node_analysis_command(
                experiment_id="exp1",
                composite_id="v2ecoli.composites.ecoli_colony.ecoli_colony",
                history_uri="s3://bucket/exp1",
                out_uri="s3://bucket/exp1/analyses/a1",
            )
        assert "V2ECOLI_SIM_DATA" not in cmd
        assert cmd.startswith(f"cd {V2ECOLI_DIR} && python scripts/run_multi_node_analysis.py")

    def test_sim_data_uri_set_exports_it_before_the_script_runs(self) -> None:
        """viva-api#448: this analysis node never stages a ParCa cache locally
        (no stage_s3/stage_dir on its own dispatch) -- V2ECOLI_SIM_DATA is the
        only way analysis_runner.resolve_sim_data can find a candidate
        strain's real cache instead of falling through to the image's own
        stock knowledge-base build."""
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            cmd = multi_node.multi_node_analysis_command(
                experiment_id="exp1",
                composite_id="v2ecoli.composites.lineage_ray_batch",
                history_uri="s3://bucket/exp1",
                out_uri="s3://bucket/exp1/analyses/a1",
                sim_data_uri="s3://mybucket/ray-parca-cache/abc/some-variant/simData.cPickle",
            )
        assert (
            f"cd {V2ECOLI_DIR} && export "
            "V2ECOLI_SIM_DATA=s3://mybucket/ray-parca-cache/abc/some-variant/simData.cPickle "
            "&& python scripts/run_multi_node_analysis.py" in cmd
        )


class TestSubmitMultiNodeAnalysisExtraction:
    """submit_multi_node_analysis (item 109) extracts n_seeds/n_generations
    from the ORIGINAL dispatch's own stored multi_node_dispatch.params, and
    the module selection via analysis_modules_for (the SAME resolver
    _analysis_command already uses) -- both threaded into the command
    builder rather than left at colony's own flat-file-only default."""

    @pytest.mark.asyncio
    async def test_extracts_n_seeds_and_modules_from_the_original_dispatch_config(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        setattr(  # noqa: B010
            experiment_request.config,
            "multi_node_dispatch",
            {
                "composite_id": "v2ecoli.composites.lineage_ray_batch",
                "num_nodes": 4,
                "params": {"n_seeds": 10, "n_generations": 10},
            },
        )
        experiment_request.config.analysis_options = AnalysisOptions.model_validate({
            "multiseed": {"doubling_time_distribution": {}}
        })
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        service = SimulationServiceRay()
        captured: dict[str, Any] = {}

        def fake_submit_container(*, job_cmd: str, **kw: Any) -> str:
            captured["job_cmd"] = job_cmd
            return "mnp-analysis-job-1"

        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch.object(service.batch, "submit_container", side_effect=fake_submit_container),
            patch.object(service.batch, "ensure_container_job_def", return_value="job-def:1"),
            patch.object(service.batch, "image_uri", return_value="ghcr.io/example/image:abc"),
        ):
            job_id = await service.submit_multi_node_analysis(
                simulation=simulation,
                database_service=database_service,
                commit="abc123",
                composite_id="v2ecoli.composites.lineage_ray_batch",
            )

        assert job_id == "mnp-analysis-job-1"
        cmd = captured["job_cmd"]
        assert "--n-seeds 10" in cmd
        assert "--n-generations 10" in cmd
        tokens = shlex.split(cmd.split("&&", 1)[1])
        assert json.loads(tokens[tokens.index("--modules") + 1]) == {"multiseed": {"doubling_time_distribution": {}}}

    @pytest.mark.asyncio
    async def test_unset_analysis_options_and_no_n_seeds_falls_back_to_the_old_flat_file_shape(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """Colony's own real shape (item 88): no multi_node_dispatch.params at
        all (a caller who never set n_seeds) must still build byte-for-byte
        the same command as before this item's fix -- no --n-seeds/--modules
        flags at all, so an older simulator image (built before this fix)
        keeps working unchanged."""
        setattr(  # noqa: B010
            experiment_request.config,
            "multi_node_dispatch",
            {"composite_id": "v2ecoli.composites.ecoli_colony.ecoli_colony", "num_nodes": 2, "params": {}},
        )
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        service = SimulationServiceRay()
        captured: dict[str, Any] = {}

        def fake_submit_container(*, job_cmd: str, **kw: Any) -> str:
            captured["job_cmd"] = job_cmd
            return "mnp-analysis-job-2"

        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch.object(service.batch, "submit_container", side_effect=fake_submit_container),
            patch.object(service.batch, "ensure_container_job_def", return_value="job-def:1"),
            patch.object(service.batch, "image_uri", return_value="ghcr.io/example/image:abc"),
        ):
            await service.submit_multi_node_analysis(
                simulation=simulation,
                database_service=database_service,
                commit="abc123",
                composite_id="v2ecoli.composites.ecoli_colony.ecoli_colony",
            )

        cmd = captured["job_cmd"]
        assert "--n-seeds" not in cmd
        assert "--modules" not in cmd
        assert "run_multi_node_analysis.py" in cmd

    @pytest.mark.asyncio
    async def test_cache_variant_on_multi_node_dispatch_reaches_the_analysis_sim_data(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """viva-api#448: this analysis node's own dispatch (_submit_container,
        no stage_s3/stage_dir) never stages a cache locally, so without
        threading cache_variant here too, a candidate-strain lineage_ray_batch
        campaign's analysis silently reads the image's own stock
        knowledge-base build instead of the real dispatch's cache -- the same
        defect class #437 already closed on the dispatch side."""
        setattr(  # noqa: B010
            experiment_request.config,
            "multi_node_dispatch",
            {
                "composite_id": "v2ecoli.composites.lineage_ray_batch",
                "num_nodes": 4,
                "params": {"n_seeds": 10, "n_generations": 10},
                "cache_variant": "cd2-run4-carina-genotype7",
            },
        )
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        service = SimulationServiceRay()
        captured: dict[str, Any] = {}

        def fake_submit_container(*, job_cmd: str, **kw: Any) -> str:
            captured["job_cmd"] = job_cmd
            return "mnp-analysis-job-3"

        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
            patch.object(service.batch, "submit_container", side_effect=fake_submit_container),
            patch.object(service.batch, "ensure_container_job_def", return_value="job-def:1"),
            patch.object(service.batch, "image_uri", return_value="ghcr.io/example/image:abc"),
        ):
            await service.submit_multi_node_analysis(
                simulation=simulation,
                database_service=database_service,
                commit="abc123",
                composite_id="v2ecoli.composites.lineage_ray_batch",
            )

        cmd = captured["job_cmd"]
        expected = "s3://mybucket/ray-parca-cache/abc123/cd2-run4-carina-genotype7/simData.cPickle"
        assert f"export V2ECOLI_SIM_DATA={expected}" in cmd

    @pytest.mark.asyncio
    async def test_omitted_cache_variant_analysis_sim_data_is_the_stock_path(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """Every caller before #448 (no cache_variant on multi_node_dispatch at
        all) must resolve to byte-identical behavior: the plain per-commit
        stock path, unaffected."""
        setattr(  # noqa: B010
            experiment_request.config,
            "multi_node_dispatch",
            {"composite_id": "v2ecoli.composites.ecoli_colony.ecoli_colony", "num_nodes": 2, "params": {}},
        )
        simulation = await database_service.insert_simulation(sim_request=experiment_request)

        service = SimulationServiceRay()
        captured: dict[str, Any] = {}

        def fake_submit_container(*, job_cmd: str, **kw: Any) -> str:
            captured["job_cmd"] = job_cmd
            return "mnp-analysis-job-4"

        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
            patch.object(service.batch, "submit_container", side_effect=fake_submit_container),
            patch.object(service.batch, "ensure_container_job_def", return_value="job-def:1"),
            patch.object(service.batch, "image_uri", return_value="ghcr.io/example/image:abc"),
        ):
            await service.submit_multi_node_analysis(
                simulation=simulation,
                database_service=database_service,
                commit="abc123",
                composite_id="v2ecoli.composites.ecoli_colony.ecoli_colony",
            )

        cmd = captured["job_cmd"]
        assert "export V2ECOLI_SIM_DATA=s3://mybucket/ray-parca-cache/abc123/simData.cPickle" in cmd
