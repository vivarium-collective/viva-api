"""The chain dispatch mechanism (``viva_api/simulation/dispatch/chain.py``): its seed and analysis commands, the
campaign submit and its background placeholder, the per-generation and per-lineage submits, and the
campaign's analysis.

Moved out of ``test_ray_backend.py`` with the code (``docs/plan-core.md`` P2.1, PR 11); unchanged but for how
they reach what moved. What stays there about chain -- ``TestGetChainCampaignResult``,
``TestCancelChainCampaign`` -- tests progress and cancel, which stay on the service until P6.
"""

import json
import shlex
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tests.simulation.test_ray_backend import (
    _container_env_of,
    _container_settings,
    _fake_container_batch,
    _raw_analysis_configs,
    _ray_settings,
)
from viva_api.common.models import JobId, JobStatus
from viva_api.simulation.dispatch import chain
from viva_api.simulation.models import AnalysisOptions, JobType
from viva_api.simulation.simulation_service_ray import SimulationServiceRay

if TYPE_CHECKING:
    from viva_api.simulation.database_service import DatabaseServiceSQL
    from viva_api.simulation.models import SimulationRequest


def _cmd_final_segment(cmd: str) -> str:
    """The last ``&&``-chained shell segment -- the actual ``v2ecoli-analyze``
    invocation, with no ``cd``/``export``/``echo`` noise ahead of it."""
    return cmd.rsplit(" && ", 1)[-1]


def _cmd_config_json(cmd: str) -> dict[str, Any]:
    """Recover the JSON payload the command ``echo``'s into ``$tmp_config``."""
    echo_segment = next(s.strip() for s in cmd.split(" && ") if s.strip().startswith("echo "))
    quoted = echo_segment[len("echo ") :].rsplit(" > ", 1)[0]
    return dict(json.loads(shlex.split(quoted)[0]))


class TestAnalysisCommand:
    """_analysis_command builds the analysis DAG node's workload.

    Real CLI surface (confirmed against v2ecoli@main, console-script target
    v2ecoli.workflow.analysis_runner:main): EXACTLY `v2ecoli-analyze [--config
    CONFIG] sweep_dir`. No --out-uri/--n-seeds/--n-generations/--modules/
    --analysis-name -- those flags never existed on this CLI and would exit 2.
    The old flags fold into the --config JSON instead (see
    viva_api.common.analysis_dag)."""

    def _cmd(self, **kw: Any) -> str:
        defaults: dict[str, Any] = {
            "experiment_id": "sim47-real-experiment",
            "modules": "applicable",
            "analysis_name": "analysis-sim47-abc123",
            "commit": "deadbeef",
        }
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
        ):
            return chain.analysis_command(**{**defaults, **kw})

    def test_runs_v2ecoli_analyze_with_positional_sweep_dir_and_config_only(self) -> None:
        cmd = self._cmd()
        final = _cmd_final_segment(cmd)
        tokens = shlex.split(final)
        assert tokens[0] == "v2ecoli-analyze"
        # The sweep dir is the SAME prefix the sim job syncs its output to, with no
        # trailing slash.
        assert tokens[1] == "s3://mybucket/vecoli-output/sim47-real-experiment"
        assert tokens[2] == "--config"
        # None of the stale flags this CLI never had.
        for stale_flag in (
            "--out-uri",
            "--n-seeds",
            "--n-generations",
            "--modules",
            "--analysis-name",
            "--experiment-id",
            "--out-dir",
        ):
            assert stale_flag not in cmd

    def test_points_sim_data_at_the_commits_parca_cache_via_env_and_config(self) -> None:
        """An s3:// sweep has no co-located sim_data pickle to glob, so the DuckDB
        analyses would raise FileNotFoundError without this pointer. Both this job
        and the ParCa job derive the URI from the commit — no hand-off plumbing.
        Threaded through BOTH the env var (the already-proven fallback) and the
        config JSON's sim_data_path -- never a stock/default per-commit path."""
        cmd = self._cmd(commit="c0ffee")
        assert "export V2ECOLI_SIM_DATA=s3://mybucket/ray-parca-cache/c0ffee/simData.cPickle" in cmd
        config = _cmd_config_json(cmd)
        assert config["sim_data_path"] == "s3://mybucket/ray-parca-cache/c0ffee/simData.cPickle"

    def test_cache_variant_points_sim_data_at_the_variant_cache_not_stock(self) -> None:
        """viva-api#448: a candidate-strain dispatch's analysis must read ITS
        cache, not the plain per-commit stock one -- the exact same class of
        bug #437 fixed on the dispatch side, just one hop downstream on the
        analysis side."""
        cmd = self._cmd(commit="c0ffee", cache_variant="cd2-run2-j3-candidate-v1-lambda075")
        expected = "s3://mybucket/ray-parca-cache/c0ffee/cd2-run2-j3-candidate-v1-lambda075/simData.cPickle"
        assert f"export V2ECOLI_SIM_DATA={expected}" in cmd
        assert _cmd_config_json(cmd)["sim_data_path"] == expected

    def test_analysis_options_ride_in_config_and_survive_a_hostile_experiment_id(self) -> None:
        """experiment_id is a caller-supplied, unconstrained string, and the modules
        blob is JSON — both must reach the container as DATA, never shell syntax."""
        hostile = "exp'; touch /tmp/analysis-command-canary; echo '$(echo pwned)"
        modules = {"multiseed": {"cd1_metabolomics": {"generation_lower_bound": 5}}}
        cmd = self._cmd(experiment_id=hostile, modules=modules)
        config = _cmd_config_json(cmd)
        assert config["analysis_options"] == modules
        final = _cmd_final_segment(cmd)
        assert shlex.split(final)[1].endswith(hostile)
        assert "touch /tmp/analysis-command-canary" not in shlex.split(cmd)

    def test_config_out_dir_is_the_analysis_names_own_write_location(self) -> None:
        """The new CLI has no --analysis-name to derive its own out_dir from, so
        the caller pre-resolves the exact same location the old --out-uri +
        --analysis-name pair used to produce together."""
        cmd = self._cmd(analysis_name="analysis-sim47-abc123")
        config = _cmd_config_json(cmd)
        assert config["out_dir"] == "s3://mybucket/vecoli-output/sim47-real-experiment/analyses/analysis-sim47-abc123"

    def test_applicable_keyword_rides_as_a_bare_string_in_analysis_options(self) -> None:
        cmd = self._cmd(modules="applicable")
        config = _cmd_config_json(cmd)
        assert config["analysis_options"] == "applicable"


class TestSeedGenerationCommand:
    """_seed_generation_command builds ONE seed's ONE generation's command —
    replacing the per-generation-array design's own _wave_sim_command. Unlike
    that design, the WHOLE --overrides payload (seed, generation, carry-state
    paths) is fully static and known Python-side at SUBMISSION time -- no
    AWS_BATCH_JOB_ARRAY_INDEX, no lookup table, no container-start shell/
    python3 merge step at all, so (mirroring TestAnalysisCommand's own
    established pattern for another fully-static command) these tests parse
    the embedded JSON via shlex.split rather than executing anything."""

    @staticmethod
    def _overrides(cmd: str) -> dict[str, Any]:
        tokens = shlex.split(cmd)
        return dict(json.loads(tokens[tokens.index("--overrides") + 1]))

    def test_shape_generation_zero_has_no_carry_state(self) -> None:
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
        ):
            cmd = chain.seed_generation_command(
                seed=7,
                generation_index=0,
                experiment_id="sim47-chain-experiment",
                runner_s3_uri="s3://mybucket/vecoli-output/sim47-chain-experiment/run_pbg.py",
            )
        assert "aws s3 cp s3://mybucket/vecoli-output/sim47-chain-experiment/run_pbg.py /tmp/run_pbg.py" in cmd
        assert "--composite-id v2ecoli.composites.ecoli_baseline.ecoli_baseline " in cmd
        assert "PBG_CORE_BUILDER=v2ecoli.core:build_core" in cmd
        # Backlog item 93's sys.path fix -- see the sibling assertion above for why.
        assert "PYTHONPATH=/app/v2ecoli" in cmd
        assert "-n 1" in cmd
        # No array-index resolution left anywhere -- the whole point of the rework.
        assert "AWS_BATCH_JOB_ARRAY_INDEX" not in cmd
        assert "python3 -c" not in cmd

        overrides = self._overrides(cmd)
        # ecoli_baseline.baseline()'s own param is `seed`, not `base_seed` (backlog
        # item 55) -- a real regression: passing base_seed here is an unexpected-
        # kwarg TypeError against the composite actually registered in the deployed
        # image, exactly the failure a real dispatch (sim 152) hit on 2026-08-16.
        assert overrides["seed"] == 7
        assert "base_seed" not in overrides
        assert overrides["initial_generation_index"] == 0
        assert overrides["initial_carry_state_path"] == ""
        assert overrides["daughter_state_out_path"] == (
            "s3://mybucket/vecoli-output/sim47-chain-experiment/daughter-state/seed7/gen0.pkl"
        )
        assert overrides["n_seeds"] == 1
        assert overrides["n_generations"] == 1
        assert overrides["analyses"] == "none"
        # Per-seed S3 prefix, not the flat ensemble one (backlog item 35: every
        # job sharing the flat prefix clobbered the last job's summary.json/
        # final_state.json -- the real bug the pilot found).
        assert overrides["out_dir"] == "s3://mybucket/vecoli-output/sim47-chain-experiment/seed_07"

    def test_later_generation_carries_the_prior_generations_state(self) -> None:
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
        ):
            cmd = chain.seed_generation_command(
                seed=42,
                generation_index=3,
                experiment_id="exp-b",
                runner_s3_uri="s3://mybucket/vecoli-output/exp-b/run_pbg.py",
            )
        overrides = self._overrides(cmd)
        assert overrides["seed"] == 42
        assert overrides["initial_generation_index"] == 3
        assert overrides["initial_carry_state_path"] == (
            "s3://mybucket/vecoli-output/exp-b/daughter-state/seed42/gen2.pkl"
        )
        assert overrides["daughter_state_out_path"] == (
            "s3://mybucket/vecoli-output/exp-b/daughter-state/seed42/gen3.pkl"
        )

    def test_hostile_experiment_id_stays_data_not_shell_syntax(self) -> None:
        """experiment_id is a caller-supplied, unconstrained string (no
        pattern validation at the model/API boundary) -- proves the
        shlex-quoted blob keeps arbitrary content as DATA, mirroring
        TestAnalysisCommand's own injection-canary proof for another
        fully-static command."""
        hostile = "exp'; touch /tmp/seed-gen-command-injection-canary; echo '$(echo pwned)"
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
        ):
            cmd = chain.seed_generation_command(
                seed=3,
                generation_index=1,
                experiment_id=hostile,
                runner_s3_uri="s3://mybucket/vecoli-output/exp/run_pbg.py",
            )
        assert "touch /tmp/seed-gen-command-injection-canary" not in shlex.split(cmd)
        overrides = self._overrides(cmd)
        assert overrides["experiment_id"] == hostile
        assert hostile in overrides["daughter_state_out_path"]

    def test_out_dir_is_shared_within_a_seed_but_isolated_across_seeds(self) -> None:
        """The real regression test for the item 35 pilot bug: every
        per-generation job for the SAME seed must share one S3 prefix (so the
        parquet sweep / zarr store / summary.json accumulate correctly across
        the chain), but DIFFERENT seeds must never share a prefix (or their
        jobs clobber each other's summary.json/final_state.json exactly as
        the 4th pilot fire found -- confirmed via direct S3 reads, not
        assumed)."""
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
        ):
            seed0_gen0 = self._overrides(
                chain.seed_generation_command(
                    seed=0,
                    generation_index=0,
                    experiment_id="exp-c",
                    runner_s3_uri="s3://mybucket/vecoli-output/exp-c/run_pbg.py",
                )
            )
            seed0_gen5 = self._overrides(
                chain.seed_generation_command(
                    seed=0,
                    generation_index=5,
                    experiment_id="exp-c",
                    runner_s3_uri="s3://mybucket/vecoli-output/exp-c/run_pbg.py",
                )
            )
            seed1_gen0 = self._overrides(
                chain.seed_generation_command(
                    seed=1,
                    generation_index=0,
                    experiment_id="exp-c",
                    runner_s3_uri="s3://mybucket/vecoli-output/exp-c/run_pbg.py",
                )
            )

        assert seed0_gen0["out_dir"] == seed0_gen5["out_dir"] == ("s3://mybucket/vecoli-output/exp-c/seed_00")
        assert seed1_gen0["out_dir"] == "s3://mybucket/vecoli-output/exp-c/seed_01"
        assert seed0_gen0["out_dir"] != seed1_gen0["out_dir"]

    def test_stays_comfortably_under_the_batch_command_size_cap(self) -> None:
        """AWS Batch caps a container override command at 8192 bytes (see
        _stage_runner's docstring). Even simpler than the design this
        superseded (no seed_indices array embedded at all -- a standalone job
        only ever needs its OWN seed), so this stays well under the cap
        regardless of experiment_id length."""
        long_experiment_id = "sim1000-cd1-baseline-1000x10-" + "x" * 20
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
        ):
            cmd = chain.seed_generation_command(
                seed=999,
                generation_index=9,
                experiment_id=long_experiment_id,
                runner_s3_uri=f"s3://mybucket/vecoli-output/{long_experiment_id}/run_pbg.py",
            )
        assert len(cmd) < 8192, f"seed-generation command is {len(cmd)} bytes, over the real AWS Batch cap"

    def test_injected_processes_and_variants_omitted_when_absent(self) -> None:
        """Backlog item 93 regression: a caller that doesn't pass
        injected_processes/variants (every caller before this item, and the
        single-generation phase0 path today) builds the exact same overrides
        dict as before these params existed -- no new keys leak in."""
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
        ):
            cmd = chain.seed_generation_command(
                seed=0,
                generation_index=0,
                experiment_id="exp-no-injection",
                runner_s3_uri="s3://mybucket/vecoli-output/exp-no-injection/run_pbg.py",
            )
        overrides = self._overrides(cmd)
        assert "injected_processes" not in overrides
        assert "variants" not in overrides

    def test_injected_processes_and_variants_forwarded_when_present(self) -> None:
        """Backlog item 93: the actual fix -- when a caller (JobScheduler, via
        injected_processes_from_config) passes these through, they land in
        the overrides dict verbatim, using ecoli_baseline.baseline()'s own
        real kwarg names."""
        injected = {
            "swap_processes": {"ecoli-metabolism": "ecoli-metabolism-redux"},
            "add_processes": [],
            "exclude_processes": ["exchange_data"],
            "fork_repo": "",
        }
        variants = {"strain_design": {"perturbations": {"value": [{"EG11005": 0.0}]}}}
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
        ):
            cmd = chain.seed_generation_command(
                seed=0,
                generation_index=0,
                experiment_id="exp-run4",
                runner_s3_uri="s3://mybucket/vecoli-output/exp-run4/run_pbg.py",
                injected_processes=injected,
                variants=variants,
            )
        overrides = self._overrides(cmd)
        assert overrides["injected_processes"] == injected
        assert overrides["variants"] == variants

    def test_composite_id_defaults_to_baseline_when_absent(self) -> None:
        """Backlog item 105: a caller that doesn't pass composite_id (every
        caller before this item) still gets V2ECOLI_BATCH_BASELINE_COMPOSITE_ID
        -- no behavior change for existing callers."""
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
        ):
            cmd = chain.seed_generation_command(
                seed=0,
                generation_index=0,
                experiment_id="exp-default-composite",
                runner_s3_uri="s3://mybucket/vecoli-output/exp-default-composite/run_pbg.py",
            )
        assert "--composite-id v2ecoli.composites.ecoli_baseline.ecoli_baseline " in cmd

    def test_composite_id_override_replaces_default_when_present(self) -> None:
        """Backlog item 105: the actual fix -- chain-dispatch was previously
        hardcoded to ecoli_baseline only. A caller-supplied composite_id (e.g.
        reactor_bird_coupled, now that v2ecoli #648 gives it the same
        injected_processes/variants shape) replaces the default entirely."""
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
        ):
            cmd = chain.seed_generation_command(
                seed=0,
                generation_index=0,
                experiment_id="exp-reactor-bird-coupled",
                runner_s3_uri="s3://mybucket/vecoli-output/exp-reactor-bird-coupled/run_pbg.py",
                composite_id="v2ecoli.composites.reactor_bird_coupled.reactor_bird_coupled",
            )
        assert "--composite-id v2ecoli.composites.reactor_bird_coupled.reactor_bird_coupled " in cmd
        assert "ecoli_baseline" not in cmd

    def test_exchange_fluxes_omitted_by_default_byte_for_byte_unaffected(self) -> None:
        """Backlog item 105 (K4 cell-only ensemble): a caller that doesn't pass
        exchange_fluxes (every caller before this fix) builds the exact same
        command as before -- pure additive passthrough, no behavior change."""
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
        ):
            cmd = chain.seed_generation_command(
                seed=0,
                generation_index=0,
                experiment_id="exp-no-exchange-flux",
                runner_s3_uri="s3://mybucket/vecoli-output/exp-no-exchange-flux/run_pbg.py",
            )
        assert "exchange_fluxes" not in cmd
        assert "exchange_flux_basis" not in cmd

    def test_exchange_fluxes_thread_into_overrides_when_present(self) -> None:
        """Backlog item 105 (K4 cell-only ensemble, sms-ecoli#210): the actual
        fix -- chain-dispatch's own generation-submission never threaded
        exchange_fluxes/exchange_flux_basis at all (only pbg-native's
        _submit_multi_node_composite, item106, had them), so a config relying
        on the ExchangeFluxListener for a real product-flux measurement
        silently produced no listeners__exchange_flux__* columns via this
        route -- the exact gap that blocked the K4 cell-only ensemble."""
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
        ):
            cmd = chain.seed_generation_command(
                seed=0,
                generation_index=0,
                experiment_id="exp-with-exchange-flux",
                runner_s3_uri="s3://mybucket/vecoli-output/exp-with-exchange-flux/run_pbg.py",
                exchange_fluxes={"violacein_exchange": "VIOLACEIN", "glucose_exchange": "GLC"},
                exchange_flux_basis="gdcw",
            )
        overrides_json = cmd.split("--overrides ")[1].split(" -n 1")[0]
        overrides = json.loads(shlex.split(overrides_json)[0])
        assert overrides["exchange_fluxes"] == {"violacein_exchange": "VIOLACEIN", "glucose_exchange": "GLC"}
        assert overrides["exchange_flux_basis"] == "gdcw"

    def test_exchange_flux_basis_omitted_without_exchange_fluxes(self) -> None:
        """A basis with no flux dict is a caller error this layer doesn't
        validate (matches injected_processes/variants' own pure-passthrough
        philosophy -- v2ecoli's own composite fails loud on a bad shape, not
        this one) -- but it must not appear alone in overrides, since
        _multi_node_composite_command's own established convention (this
        file, ~line 440) nests it under `if exchange_fluxes:` too."""
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
        ):
            cmd = chain.seed_generation_command(
                seed=0,
                generation_index=0,
                experiment_id="exp-basis-only",
                runner_s3_uri="s3://mybucket/vecoli-output/exp-basis-only/run_pbg.py",
                exchange_flux_basis="gdcw",
            )
        assert "exchange_flux_basis" not in cmd

    def test_stop_at_division_is_always_set(self) -> None:
        """Backlog item 103: without this, n_seeds=1/n_generations=1/no
        stop_at_division makes ecoli_baseline.baseline()'s own dispatch gate
        (n_seeds>1 or n_generations>1 or stop_at_division) evaluate False on
        EVERY chain-dispatch generation, routing through the plain,
        non-division-gated single-cell build (the composite's own docs call it
        "NO division-stop") -- confirmed empirically in real campaign 171
        production output: generation 0/5/9 of the same lineage were
        MD5-identical files, global_time never exceeded 1.0 across 10 chained
        "generations". Unconditional, not caller-controlled -- there is no
        legitimate chain-dispatch generation that should NOT stop at division."""
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
        ):
            cmd = chain.seed_generation_command(
                seed=0,
                generation_index=0,
                experiment_id="exp-division-gate",
                runner_s3_uri="s3://mybucket/vecoli-output/exp-division-gate/run_pbg.py",
            )
        overrides = self._overrides(cmd)
        assert overrides["stop_at_division"] is True


class TestSeedLineageCommand:
    """_seed_lineage_command builds ONE seed's WHOLE lineage as a single command:
    all n_generations in one LineageProcess. This is the Run-3 fix — running the
    whole lineage in one process is what makes lineage_time_offset accumulate
    across generations so a field_timeline dose scheduled at a cumulative time
    fires (the per-generation chain reset it to 0 every job)."""

    @staticmethod
    def _overrides(cmd: str) -> dict[str, Any]:
        tokens = shlex.split(cmd)
        return dict(json.loads(tokens[tokens.index("--overrides") + 1]))

    def test_runs_all_generations_in_one_lineageprocess_with_no_daughter_state(self) -> None:
        """The whole point: n_generations=N in ONE job (not n_generations=1 per
        job), one seed, and NONE of the per-generation daughter-state /
        checkpoint / stop_at_division keys — division is in-process, so the
        LineageProcess accumulates lineage_time_offset across generations."""
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
        ):
            cmd = chain.seed_lineage_command(
                seed=7,
                n_generations=8,
                experiment_id="sim47-chain-experiment",
                runner_s3_uri="s3://mybucket/vecoli-output/sim47-chain-experiment/run_pbg.py",
            )
        assert "--composite-id v2ecoli.composites.ecoli_baseline.ecoli_baseline " in cmd
        assert "-n 1" in cmd
        assert "AWS_BATCH_JOB_ARRAY_INDEX" not in cmd

        overrides = self._overrides(cmd)
        # One LineageProcess for the WHOLE lineage.
        assert overrides["n_seeds"] == 1
        assert overrides["n_generations"] == 8
        assert overrides["seed"] == 7
        assert "base_seed" not in overrides
        # NO per-generation daughter-state handoff — division is in-process, and
        # this is exactly what stops lineage_time_offset from resetting to 0.
        assert "initial_generation_index" not in overrides
        assert "initial_carry_state_path" not in overrides
        assert "daughter_state_out_path" not in overrides
        # NOT gated to stop after one division: run every generation.
        assert "stop_at_division" not in overrides
        # Same per-seed S3 out layout as the per-generation path.
        assert overrides["out_dir"] == "s3://mybucket/vecoli-output/sim47-chain-experiment/seed_07"
        assert overrides["analyses"] == "none"

    def test_injected_processes_and_variants_forwarded_and_omitted(self) -> None:
        """The dose sweep rides in injected_processes/variants; they must thread
        through verbatim, and be absent when not supplied (byte-for-byte with the
        per-generation command's own passthrough contract)."""
        injected = {
            "swap_processes": {"ecoli-metabolism": "ecoli-metabolism-redux"},
            "add_processes": [],
            "exclude_processes": [],
            "fork_repo": "",
        }
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
        ):
            with_inj = self._overrides(
                chain.seed_lineage_command(
                    seed=0,
                    n_generations=20,
                    experiment_id="exp-run3",
                    runner_s3_uri="s3://mybucket/vecoli-output/exp-run3/run_pbg.py",
                    injected_processes=injected,
                )
            )
            without = self._overrides(
                chain.seed_lineage_command(
                    seed=0,
                    n_generations=20,
                    experiment_id="exp-run3",
                    runner_s3_uri="s3://mybucket/vecoli-output/exp-run3/run_pbg.py",
                )
            )
        assert with_inj["injected_processes"] == injected
        assert "injected_processes" not in without
        assert "variants" not in without


class TestChainDispatchPlaceholderBinding:
    """viva-api#414: the chain-dispatch placeholder row is bound to its
    background task, so the DB row is finalized from the task's outcome
    instead of staying `running` forever (and a submission crash is written
    to the row, not only to this process's memory)."""

    @pytest.mark.asyncio
    async def test_placeholder_is_completed_once_the_task_finishes(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        setattr(experiment_request.config, "n_init_sims", 2)  # noqa: B010
        experiment_request.config.generations = 3
        simulation = await database_service.insert_simulation(sim_request=experiment_request)
        mock_batch = _fake_container_batch(["parca-1"])
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
        ):
            job_id = await service.submit_ecoli_simulation_job(
                ecoli_simulation=simulation, database_service=database_service, correlation_id="corr-bind-ok"
            )
            placeholder = await database_service.get_hpcrun_by_job_id(job_id)
            assert placeholder is not None and placeholder.status == JobStatus.RUNNING
            await service._local.wait_finalized(job_id.value)
        fresh = await database_service.get_hpcrun(placeholder.database_id)
        assert fresh is not None
        assert fresh.status == JobStatus.COMPLETED
        assert fresh.end_time is not None
        # the real campaign row is untouched and still the one every read resolves
        campaign = await database_service.get_hpcrun_by_ref(ref_id=simulation.database_id, job_type=JobType.SIMULATION)
        assert campaign is not None
        assert campaign.database_id != placeholder.database_id
        assert campaign.status == JobStatus.RUNNING

    @pytest.mark.asyncio
    async def test_placeholder_is_failed_with_the_error_when_submission_crashes(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        setattr(experiment_request.config, "n_init_sims", 2)  # noqa: B010
        experiment_request.config.generations = 3
        simulation = await database_service.insert_simulation(sim_request=experiment_request)
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _container_settings),
            patch.object(
                chain.ChainStrategy,
                "submit_chain_dispatch_job",
                new=AsyncMock(side_effect=RuntimeError("ParCa submit boom")),
            ),
        ):
            job_id = await service.submit_ecoli_simulation_job(
                ecoli_simulation=simulation, database_service=database_service, correlation_id="corr-bind-crash"
            )
            await service._local.wait_finalized(job_id.value)
            failed = await service.get_job_status(job_id)
            assert failed is not None and failed.status == JobStatus.FAILED
        placeholder = await database_service.get_hpcrun_by_job_id(job_id)
        assert placeholder is not None
        assert placeholder.status == JobStatus.FAILED
        assert "ParCa submit boom" in (placeholder.error_message or "")


@pytest.mark.asyncio
class TestChainDispatchSubmission:
    """submit_chain_dispatch_job (backlog item 71 Phase 4 rework): submits
    ONLY ParCa now, as a container-type job, and writes the campaign's
    initial per-seed tracking row (every slot empty, gated on ParCa) --
    generation submission moves entirely to JobScheduler's poll loop (see
    TestAdvanceChainCampaign, tests/simulation/test_scheduler.py, and
    TestSubmitChainGeneration/TestCancelChainCampaign below for those)."""

    async def test_rejects_single_generation(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        setattr(experiment_request.config, "n_init_sims", 4)  # noqa: B010
        experiment_request.config.generations = 1
        simulation = await database_service.insert_simulation(sim_request=experiment_request)
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _ray_settings),
            pytest.raises(ValueError, match="requires generations > 1"),
        ):
            await service.submit_chain_dispatch_job(ecoli_simulation=simulation, database_service=database_service)

    async def test_submits_only_parca_as_a_container_job(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        setattr(experiment_request.config, "n_init_sims", 3)  # noqa: B010
        experiment_request.config.generations = 4
        simulation = await database_service.insert_simulation(sim_request=experiment_request)
        mock_batch = _fake_container_batch(["parca-1"])

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
        ):
            job_id = await service.submit_chain_dispatch_job(
                ecoli_simulation=simulation, database_service=database_service
            )

        assert job_id == JobId.ray("parca-1")
        # Exactly ONE submission -- no per-seed generation jobs anymore.
        assert mock_batch.submit_job.call_count == 1
        (parca_call,) = mock_batch.submit_job.call_args_list
        assert "dependsOn" not in parca_call.kwargs
        assert "containerOverrides" in parca_call.kwargs  # container-type, not MNP nodeOverrides
        env = _container_env_of(parca_call)
        assert "v2ecoli-parca" in env["CONTAINER_JOB_CMD"]
        assert "--new-genes" not in env["CONTAINER_JOB_CMD"]  # regression: absent when not set
        assert "--bundle-overrides" not in env["CONTAINER_JOB_CMD"]  # regression: absent when not set

    async def test_forwards_bundle_overrides_to_parca_when_config_sets_it(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """Backlog item 104 (sms-ecoli#184 / viva-api#365, cplong90): parca_options.bundle_overrides
        survived on the stored request but was never forwarded to the ParCa command chain-dispatch
        actually submits, so v2ecoli-parca built from defaults only and any keys the overrides
        manifest supplies were absent -- same class of gap as item 93's new_genes fix, missed in
        that pass."""
        setattr(experiment_request.config, "n_init_sims", 2)  # noqa: B010
        experiment_request.config.generations = 3
        setattr(  # noqa: B010
            experiment_request.config.parca_options, "bundle_overrides", "models/parca/composed_overlay.tsv"
        )
        simulation = await database_service.insert_simulation(sim_request=experiment_request)
        mock_batch = _fake_container_batch(["parca-1"])

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
        ):
            await service.submit_chain_dispatch_job(ecoli_simulation=simulation, database_service=database_service)

        (parca_call,) = mock_batch.submit_job.call_args_list
        env = _container_env_of(parca_call)
        assert "--bundle-overrides models/parca/composed_overlay.tsv" in env["CONTAINER_JOB_CMD"]

    async def test_forwards_new_genes_to_parca_when_config_sets_it(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """Backlog item 93: the real gap a live Run 4 smoke dispatch found --
        parca_options.new_genes (a real SimulationConfig field, matches
        v2ecoli-parca's own --new-genes SUBDIR flag) must reach the ParCa
        command chain-dispatch actually submits, not just the composite's own
        overrides -- without a violacein-aware simData, the sim can't secrete
        violacein regardless of any other fix."""
        setattr(experiment_request.config, "n_init_sims", 2)  # noqa: B010
        experiment_request.config.generations = 3
        setattr(experiment_request.config.parca_options, "new_genes", "violacein_MG1655_M5")  # noqa: B010
        simulation = await database_service.insert_simulation(sim_request=experiment_request)
        mock_batch = _fake_container_batch(["parca-1"])

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
        ):
            await service.submit_chain_dispatch_job(ecoli_simulation=simulation, database_service=database_service)

        (parca_call,) = mock_batch.submit_job.call_args_list
        env = _container_env_of(parca_call)
        assert "--new-genes violacein_MG1655_M5" in env["CONTAINER_JOB_CMD"]

    async def test_forwards_rnaseq_source_to_parca_when_config_sets_it(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """Item 106/#166 chassis-provenance thread: parca_options.rnaseq_source must reach the
        ParCa command chain-dispatch actually submits -- a real bundle_overrides manifest
        (sms-ecoli's rung5-lambda-075/overrides.tsv) is a documented no-op without it, same
        silent-wrong-build class of gap as new_genes/bundle_overrides above."""
        setattr(experiment_request.config, "n_init_sims", 2)  # noqa: B010
        experiment_request.config.generations = 3
        setattr(experiment_request.config.parca_options, "rnaseq_source", "experimental")  # noqa: B010
        simulation = await database_service.insert_simulation(sim_request=experiment_request)
        mock_batch = _fake_container_batch(["parca-1"])

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
        ):
            await service.submit_chain_dispatch_job(ecoli_simulation=simulation, database_service=database_service)

        (parca_call,) = mock_batch.submit_job.call_args_list
        env = _container_env_of(parca_call)
        assert "--rnaseq-source experimental" in env["CONTAINER_JOB_CMD"]

    async def test_writes_initial_campaign_row_with_empty_per_seed_state(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        setattr(experiment_request.config, "n_init_sims", 3)  # noqa: B010
        experiment_request.config.generations = 4
        simulation = await database_service.insert_simulation(sim_request=experiment_request)
        mock_batch = _fake_container_batch(["parca-1"])

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
        ):
            await service.submit_chain_dispatch_job(ecoli_simulation=simulation, database_service=database_service)

        active_campaigns = [
            c for c in await database_service.list_active_chain_campaigns() if c.ref_id == simulation.database_id
        ]
        assert len(active_campaigns) == 1
        campaign = active_campaigns[0]
        assert campaign.job_id == JobId.ray("parca-1")
        assert campaign.chain_n_generations == 4
        assert campaign.chain_final_job_ids == []
        assert campaign.chain_current_job_ids == [None, None, None]
        assert campaign.chain_current_generation == [None, None, None]
        assert campaign.chain_parca_done is False

    async def test_single_seed_multi_generation_is_allowed(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """Unlike the array-job design predating item 33, n_seeds >= 2 is NOT
        required -- no AWS Batch array-size floor applies here at all (every
        seed's chain is independent standalone container jobs)."""
        setattr(experiment_request.config, "n_init_sims", 1)  # noqa: B010
        experiment_request.config.generations = 2
        simulation = await database_service.insert_simulation(sim_request=experiment_request)
        mock_batch = _fake_container_batch(["parca-1"])
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
        ):
            job_id = await service.submit_chain_dispatch_job(
                ecoli_simulation=simulation, database_service=database_service
            )
        assert job_id == JobId.ray("parca-1")
        active_campaigns = [
            c for c in await database_service.list_active_chain_campaigns() if c.ref_id == simulation.database_id
        ]
        assert active_campaigns[0].chain_current_job_ids == [None]


@pytest.mark.asyncio
class TestSubmitChainGeneration:
    """submit_chain_generation/submit_chain_generation_batch (backlog item 71
    Phase 4): submit ONE seed's ONE generation as a standalone container job,
    no depends_on -- JobScheduler decides WHEN to call these, not native Batch
    dependency resolution. Locks the exact deterministic S3 paths + tag shape
    unchanged from the superseded design, on the new container job type."""

    async def test_submit_chain_generation_builds_the_right_command_and_shape(self) -> None:
        mock_batch = _fake_container_batch(["s2g1"])
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
        ):
            job_id = service._chain().submit_chain_generation(
                seed=2,
                generation_index=1,
                experiment_id="exp-1",
                commit="abc1234",
                cache_s3="s3://mybucket/cache/abc1234",
                runner_s3_uri="s3://mybucket/runner/run_pbg.py",
                tags={"Project": "v2ecoli-comparison", "Phase": "sim"},
            )

        assert job_id == "s2g1"
        (call,) = mock_batch.submit_job.call_args_list
        assert "dependsOn" not in call.kwargs  # app-level gating -- no native Batch dependency at all
        assert call.kwargs["tags"]["Seed"] == "2"
        assert call.kwargs["tags"]["Generation"] == "1"
        env = _container_env_of(call)
        tokens = shlex.split(env["CONTAINER_JOB_CMD"])
        overrides = json.loads(tokens[tokens.index("--overrides") + 1])
        assert overrides["seed"] == 2
        assert overrides["initial_generation_index"] == 1
        assert (
            overrides["initial_carry_state_path"] == "s3://mybucket/vecoli-output/exp-1/daughter-state/seed2/gen0.pkl"
        )
        assert overrides["daughter_state_out_path"] == "s3://mybucket/vecoli-output/exp-1/daughter-state/seed2/gen1.pkl"

    async def test_submit_chain_generation_omits_lineage_debug_division_by_default(self) -> None:
        """Item 106/#210 (v2ecoli#733): default False emits nothing -- byte-identical
        to before this param existed."""
        mock_batch = _fake_container_batch(["s2g1"])
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
        ):
            service._chain().submit_chain_generation(
                seed=2,
                generation_index=1,
                experiment_id="exp-1",
                commit="abc1234",
                cache_s3="s3://mybucket/cache/abc1234",
                runner_s3_uri="s3://mybucket/runner/run_pbg.py",
                tags={"Project": "v2ecoli-comparison", "Phase": "sim"},
            )
        (call,) = mock_batch.submit_job.call_args_list
        assert "LINEAGE_DEBUG_DIVISION" not in _container_env_of(call)

    async def test_submit_chain_generation_forwards_lineage_debug_division(self) -> None:
        """Item 106/#210 (v2ecoli#733): the real, previously-missing gap this closes --
        chain-dispatch's own job submission had no way to set this at all, blocking
        Run 3's real diagnostic dispatch. Emitted unprefixed (LineageProcess reads it
        directly via os.environ.get), same as V2E_REQUIRE_CLEAN_CHAIN."""
        mock_batch = _fake_container_batch(["s2g1"])
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
        ):
            service._chain().submit_chain_generation(
                seed=2,
                generation_index=1,
                experiment_id="exp-1",
                commit="abc1234",
                cache_s3="s3://mybucket/cache/abc1234",
                runner_s3_uri="s3://mybucket/runner/run_pbg.py",
                tags={"Project": "v2ecoli-comparison", "Phase": "sim"},
                lineage_debug_division=True,
            )
        (call,) = mock_batch.submit_job.call_args_list
        assert _container_env_of(call)["LINEAGE_DEBUG_DIVISION"] == "1"

    async def test_submit_chain_generation_forwards_injected_processes_and_variants(self) -> None:
        """Backlog item 93: JobScheduler passes these through on every seed's
        every generation (re-derived from Simulation.config each tick) --
        confirms submit_chain_generation threads them into the real overrides
        payload rather than dropping them at this layer."""
        mock_batch = _fake_container_batch(["s0g0"])
        injected = {
            "swap_processes": {"ecoli-metabolism": "ecoli-metabolism-redux"},
            "add_processes": [],
            "exclude_processes": ["exchange_data"],
            "fork_repo": "",
        }
        variants = {"strain_design": {"perturbations": {"value": [{"EG11005": 0.0}]}}}
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
        ):
            service._chain().submit_chain_generation(
                seed=0,
                generation_index=0,
                experiment_id="exp-run4",
                commit="abc1234",
                cache_s3="s3://mybucket/cache/abc1234",
                runner_s3_uri="s3://mybucket/runner/run_pbg.py",
                tags={"Project": "v2ecoli-comparison"},
                injected_processes=injected,
                variants=variants,
            )
        (call,) = mock_batch.submit_job.call_args_list
        env = _container_env_of(call)
        tokens = shlex.split(env["CONTAINER_JOB_CMD"])
        overrides = json.loads(tokens[tokens.index("--overrides") + 1])
        assert overrides["injected_processes"] == injected
        assert overrides["variants"] == variants

    async def test_submit_chain_generation_forwards_exchange_fluxes(self) -> None:
        """Backlog item 105 (K4 cell-only ensemble, sms-ecoli#210): confirms
        submit_chain_generation threads exchange_fluxes/exchange_flux_basis
        into the real overrides payload rather than dropping them at this
        layer -- the exact gap that silently produced no exchange-flux
        measurement for a chain-dispatch config relying on it."""
        mock_batch = _fake_container_batch(["s0g0"])
        exchange_fluxes = {"violacein_exchange": "VIOLACEIN", "glucose_exchange": "GLC"}
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
        ):
            service._chain().submit_chain_generation(
                seed=0,
                generation_index=0,
                experiment_id="exp-k4-cellonly",
                commit="abc1234",
                cache_s3="s3://mybucket/cache/abc1234",
                runner_s3_uri="s3://mybucket/runner/run_pbg.py",
                tags={"Project": "v2ecoli-comparison"},
                exchange_fluxes=exchange_fluxes,
                exchange_flux_basis="gdcw",
            )
        (call,) = mock_batch.submit_job.call_args_list
        env = _container_env_of(call)
        tokens = shlex.split(env["CONTAINER_JOB_CMD"])
        overrides = json.loads(tokens[tokens.index("--overrides") + 1])
        assert overrides["exchange_fluxes"] == exchange_fluxes
        assert overrides["exchange_flux_basis"] == "gdcw"

    async def test_submit_chain_generation_forwards_composite_id(self) -> None:
        """Backlog item 105: JobScheduler passes composite_id through on every
        seed's every generation (re-derived from Simulation.config each tick,
        same pattern as injected_processes/variants) -- confirms
        submit_chain_generation threads it into the real --composite-id flag
        rather than dropping it at this layer."""
        mock_batch = _fake_container_batch(["s0g0"])
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
        ):
            service._chain().submit_chain_generation(
                seed=0,
                generation_index=0,
                experiment_id="exp-run1-k4",
                commit="abc1234",
                cache_s3="s3://mybucket/cache/abc1234",
                runner_s3_uri="s3://mybucket/runner/run_pbg.py",
                tags={"Project": "v2ecoli-comparison"},
                composite_id="v2ecoli.composites.reactor_bird_coupled.reactor_bird_coupled",
            )
        (call,) = mock_batch.submit_job.call_args_list
        env = _container_env_of(call)
        assert (
            "--composite-id v2ecoli.composites.reactor_bird_coupled.reactor_bird_coupled " in env["CONTAINER_JOB_CMD"]
        )

    async def test_batch_forwards_injected_processes_and_variants_to_every_seed(self) -> None:
        """Backlog item 93: submit_chain_generation_batch's own fan-out loop
        (the generation-0 burst) must pass the SAME injected_processes/
        variants to every seed -- one campaign, one config."""
        mock_batch = _fake_container_batch(["s0g0", "s1g0"])
        injected = {
            "swap_processes": {"ecoli-metabolism": "ecoli-metabolism-redux"},
            "add_processes": [],
            "exclude_processes": [],
            "fork_repo": "",
        }
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
            patch("viva_core.backends.batch.SubmitJobPacer.wait", new=AsyncMock()),
        ):
            submitted = await service._chain().submit_chain_generation_batch(
                seeds=[0, 1],
                generation_index=0,
                experiment_id="exp-run4",
                commit="abc1234",
                cache_s3="s3://mybucket/cache/abc1234",
                runner_s3_uri="s3://mybucket/runner/run_pbg.py",
                tags={"Project": "v2ecoli-comparison"},
                injected_processes=injected,
                variants=None,
            )
        assert submitted == {0: "s0g0", 1: "s1g0"}
        for call in mock_batch.submit_job.call_args_list:
            env = _container_env_of(call)
            tokens = shlex.split(env["CONTAINER_JOB_CMD"])
            overrides = json.loads(tokens[tokens.index("--overrides") + 1])
            assert overrides["injected_processes"] == injected
            assert "variants" not in overrides  # None -> omitted, matches _seed_generation_command's own contract

    async def test_batch_forwards_composite_id_to_every_seed(self) -> None:
        """Backlog item 105: submit_chain_generation_batch's own fan-out loop
        (the generation-0 burst) must pass the SAME composite_id to every seed
        -- one campaign, one config, matching the injected_processes/variants
        precedent immediately above."""
        mock_batch = _fake_container_batch(["s0g0", "s1g0"])
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
            patch("viva_core.backends.batch.SubmitJobPacer.wait", new=AsyncMock()),
        ):
            submitted = await service._chain().submit_chain_generation_batch(
                seeds=[0, 1],
                generation_index=0,
                experiment_id="exp-run1-k4",
                commit="abc1234",
                cache_s3="s3://mybucket/cache/abc1234",
                runner_s3_uri="s3://mybucket/runner/run_pbg.py",
                tags={"Project": "v2ecoli-comparison"},
                composite_id="v2ecoli.composites.reactor_bird_coupled.reactor_bird_coupled",
            )
        assert submitted == {0: "s0g0", 1: "s1g0"}
        for call in mock_batch.submit_job.call_args_list:
            env = _container_env_of(call)
            assert (
                "--composite-id v2ecoli.composites.reactor_bird_coupled.reactor_bird_coupled "
                in env["CONTAINER_JOB_CMD"]
            )

    async def test_batch_forwards_exchange_fluxes_to_every_seed(self) -> None:
        """Backlog item 105 (K4 cell-only ensemble): submit_chain_generation_batch's
        own fan-out loop (the generation-0 burst) must pass the SAME
        exchange_fluxes/exchange_flux_basis to every seed -- one campaign, one
        config, matching the injected_processes/variants/composite_id precedent
        immediately above."""
        mock_batch = _fake_container_batch(["s0g0", "s1g0"])
        exchange_fluxes = {"violacein_exchange": "VIOLACEIN"}
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
            patch("viva_core.backends.batch.SubmitJobPacer.wait", new=AsyncMock()),
        ):
            submitted = await service._chain().submit_chain_generation_batch(
                seeds=[0, 1],
                generation_index=0,
                experiment_id="exp-k4-cellonly-batch",
                commit="abc1234",
                cache_s3="s3://mybucket/cache/abc1234",
                runner_s3_uri="s3://mybucket/runner/run_pbg.py",
                tags={"Project": "v2ecoli-comparison"},
                exchange_fluxes=exchange_fluxes,
                exchange_flux_basis="gdcw",
            )
        assert submitted == {0: "s0g0", 1: "s1g0"}
        for call in mock_batch.submit_job.call_args_list:
            env = _container_env_of(call)
            tokens = shlex.split(env["CONTAINER_JOB_CMD"])
            overrides = json.loads(tokens[tokens.index("--overrides") + 1])
            assert overrides["exchange_fluxes"] == exchange_fluxes
            assert overrides["exchange_flux_basis"] == "gdcw"

    async def test_batch_fans_out_generation_zero_for_every_seed_paced_and_isolates_failures(self) -> None:
        """The one remaining genuine submission burst: every seed's
        generation 0, fanned out the instant ParCa succeeds. Paced (like the
        superseded design's own N*G burst); a per-seed failure (even after
        retry-on-throttle) is isolated -- that seed is simply omitted from the
        returned mapping, other seeds unaffected."""
        mock_batch = MagicMock()
        base_container_props = {"image": "111.dkr.ecr.x/vecoli:ray", "vcpus": 16, "memory": 32000}

        def _describe(**kwargs: Any) -> dict[str, Any]:
            if kwargs.get("jobDefinitionName") == "smscdk-ray-container":
                return {"jobDefinitions": [{"revision": 7, "containerProperties": base_container_props}]}
            return {"jobDefinitions": []}

        mock_batch.describe_job_definitions.side_effect = _describe
        mock_batch.register_job_definition.side_effect = lambda **kw: {
            "jobDefinitionName": kw["jobDefinitionName"],
            "revision": 1,
        }
        remaining_ids = iter(["s0g0", "s2g0"])

        def _submit_job(**kwargs: Any) -> dict[str, Any]:
            if kwargs["jobName"].startswith("chain-seed1-gen0-"):
                raise RuntimeError("submit_job: rate exceeded (simulated, retries exhausted)")
            return {"jobId": next(remaining_ids)}

        mock_batch.submit_job.side_effect = _submit_job

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
            patch("viva_core.backends.batch.SubmitJobPacer.wait", new=AsyncMock()) as mock_pacer_wait,
        ):
            submitted = await service._chain().submit_chain_generation_batch(
                seeds=[0, 1, 2],
                generation_index=0,
                experiment_id="exp-1",
                commit="abc1234",
                cache_s3="s3://mybucket/cache/abc1234",
                runner_s3_uri="s3://mybucket/runner/run_pbg.py",
                tags={"Project": "v2ecoli-comparison"},
            )

        assert submitted == {0: "s0g0", 2: "s2g0"}  # seed 1 omitted -- its submission failed
        assert mock_pacer_wait.await_count == 3  # every seed's attempt is paced, including the failed one


@pytest.mark.asyncio
class TestSubmitCampaignAnalysis:
    """submit_campaign_analysis is what JobScheduler calls once the
    analysis-fan-in poller confirms a chain-dispatch campaign is all-terminal
    (backlog item 33 rework) -- reuses item 24's existing analysis-job
    submission code (_submit_analysis_job) completely as-is, with NO native
    dependsOn."""

    async def test_submits_analysis_with_no_dependency(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        simulation = await database_service.insert_simulation(sim_request=experiment_request)
        mock_batch = _fake_container_batch(["analysis-999"])
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
        ):
            job_id = await service.submit_campaign_analysis(
                simulation=simulation,
                database_service=database_service,
                commit="abc1234",
                total_n_seeds=30,
                n_generations=10,
            )
        assert job_id == "analysis-999"
        assert mock_batch.submit_job.call_count == 1
        (analysis_call,) = mock_batch.submit_job.call_args_list
        # By construction everything this depends on has ALREADY finished --
        # no native dependsOn at all (unlike item 24's single-shot-dispatch
        # shape, which depends on the sim job it rides directly behind).
        assert "dependsOn" not in analysis_call.kwargs
        # Item 71: the analysis DAG node now rides the plain container path,
        # not MNP -- no node overrides at all.
        assert "containerOverrides" in analysis_call.kwargs
        assert "nodeOverrides" not in analysis_call.kwargs

        records = await database_service.list_analyses(simulation_id=simulation.database_id)
        assert len(records) == 1
        assert records[0].job_id_ext == "analysis-999"
        assert records[0].backend == "ray"

    async def test_cache_variant_on_the_simulation_reaches_the_real_analysis_command(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """viva-api#448: a chain-dispatch campaign's own top-level cache_variant
        (job_scheduler.py's own already-proven getattr(simulation.config,
        "cache_variant", ...) pattern) must reach the analysis job's own
        sim_data pointer too, not just the sim jobs -- otherwise a candidate
        strain campaign's analysis silently grades stock data."""
        setattr(experiment_request.config, "cache_variant", "cd2-run1-k4-candidate-v1-lambda050")  # noqa: B010
        simulation = await database_service.insert_simulation(sim_request=experiment_request)
        mock_batch = _fake_container_batch(["analysis-999"])
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
        ):
            await service.submit_campaign_analysis(
                simulation=simulation,
                database_service=database_service,
                commit="abc1234",
                total_n_seeds=30,
                n_generations=10,
            )
        (analysis_call,) = mock_batch.submit_job.call_args_list
        cmd = _container_env_of(analysis_call)["CONTAINER_JOB_CMD"]
        expected = "s3://mybucket/ray-parca-cache/abc1234/cd2-run1-k4-candidate-v1-lambda050/simData.cPickle"
        assert f"export V2ECOLI_SIM_DATA={expected}" in cmd

    async def test_uses_the_originally_requested_seed_count_not_survivor_count(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """Matches the superseded design's own resolved semantics: analysis
        modules resolve 'applicable' against the campaign's INTENDED shape,
        not however many chains actually survived to completion.

        n_seeds no longer rides the command at all (the real v2ecoli-analyze CLI
        infers it from the sweep's own hive partitions -- see
        viva_api.common.analysis_dag), but the analyses TABLE record shape is
        unchanged (hard constraint: no behavior change beyond the argv fix), so
        it's still checked there instead."""
        simulation = await database_service.insert_simulation(sim_request=experiment_request)
        mock_batch = _fake_container_batch(["analysis-999"])
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
        ):
            await service.submit_campaign_analysis(
                simulation=simulation,
                database_service=database_service,
                commit="abc1234",
                total_n_seeds=1000,  # the originally requested total, even if fewer chains actually succeeded
                n_generations=10,
            )
        cmd = _container_env_of(mock_batch.submit_job.call_args_list[0])["CONTAINER_JOB_CMD"]
        assert "--n-seeds" not in cmd
        configs = await _raw_analysis_configs(database_service, simulation.database_id)
        assert configs[0]["n_seeds"] == 1000

    async def test_configured_analysis_options_reach_the_analysis_job(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """REGRESSION (item 24, retargeted for backlog item 33): config.
        analysis_options (set by the run endpoint from the caller's
        --analysis-options, and by the workbench from a study's spec.analyses)
        must reach the analysis job's --config JSON. Originally verified
        through submit_ecoli_simulation_job's own inline array-path analysis
        submission (now removed -- that shape's analysis fires exclusively
        through submit_campaign_analysis, exercised directly here)."""

        experiment_request.config.analysis_options = AnalysisOptions.model_validate({
            "multiseed": {"cd1_fluxomics": {"generation_lower_bound": 5}}
        })
        simulation = await database_service.insert_simulation(sim_request=experiment_request)
        mock_batch = _fake_container_batch(["analysis-789"])
        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
        ):
            await service.submit_campaign_analysis(
                simulation=simulation,
                database_service=database_service,
                commit="abc1234",
                total_n_seeds=4,
                n_generations=3,
            )
        cmd = _container_env_of(mock_batch.submit_job.call_args_list[0])["CONTAINER_JOB_CMD"]
        assert "--modules" not in cmd
        config = _cmd_config_json(cmd)
        assert config["analysis_options"] == {"multiseed": {"cd1_fluxomics": {"generation_lower_bound": 5}}}

    async def test_a_failed_analysis_submission_is_recorded_not_swallowed(
        self,
        experiment_request: "SimulationRequest",
        database_service: "DatabaseServiceSQL",
    ) -> None:
        """By the time this runs, every seed chain the poller was watching has
        already reached a terminal state -- raising here would just lose the
        analysis silently. It must land as a FAILED analyses-table row
        instead (item 24's guarantee, retargeted for backlog item 33: this is
        now the canonical shape's own analysis trigger, replacing
        submit_ecoli_simulation_job's removed inline path)."""
        simulation = await database_service.insert_simulation(sim_request=experiment_request)
        mock_batch = _fake_container_batch([])
        mock_batch.submit_job.side_effect = RuntimeError("Batch said no")

        service = SimulationServiceRay()
        with (
            patch("viva_api.simulation.dispatch._seams.get_settings", _container_settings),
            patch("viva_api.common.storage.data_layout.get_settings", _container_settings),
            patch("viva_api.simulation.dispatch._seams.boto3.client", return_value=mock_batch),
        ):
            result = await service.submit_campaign_analysis(
                simulation=simulation,
                database_service=database_service,
                commit="abc1234",
                total_n_seeds=4,
                n_generations=3,
            )
        assert result is None
        records = await database_service.list_analyses(simulation_id=simulation.database_id)
        assert len(records) == 1
        assert records[0].status == JobStatus.FAILED
        assert "Batch said no" in (records[0].error_message or "")
