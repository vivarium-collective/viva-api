"""The ensemble dispatch mechanism (``viva_api/simulation/dispatch/ensemble.py``): its job command.

Split out of ``test_ray_backend.py`` with PR 11 (``docs/plan-core.md`` P2.1) -- one PR after the code, because
these tests were methods of a class about something else. The ensemble THROUGH THE ROUTER is still tested in
``test_ray_backend.TestSimulationServiceRaySubmit``, which is the router's test class now.
"""

import json
import shlex
from unittest.mock import patch

import pytest

from tests.simulation.test_ray_backend import (
    _ray_settings,
)
from viva_api.simulation.dispatch.ensemble import sim_command
from viva_api.simulation.dispatch.image_paths import PARCA_CACHE_DIR, SIM_OUT_DIR


class TestSimCommand:
    """``sim_command``: the ensemble mechanism's job command. These eleven tests sat at the end of the image-BUILD
    test class, for no reason but history."""

    def test_sim_command_composite_defaults_to_single_generation(self) -> None:
        """Selecting an engine must NOT imply the 16-gen comparison default."""
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            cmd = sim_command(n_seeds=1, n_steps=10, chunk=4, composite="v2ecoli")
        assert "run_comparison_ensemble.py" in cmd
        assert "--max-generations 1" in cmd
        assert "--max-generations 16" not in cmd

    def test_sim_command_composite_honors_explicit_generations(self) -> None:
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            cmd = sim_command(n_seeds=1, n_steps=10, chunk=4, composite="v2ecoli", max_generations=5)
        assert "--max-generations 5" in cmd

    def test_sim_command_defaults_to_single_generation_phase0(self) -> None:
        """No composite, no generations requested: unchanged, verified-working
        single-generation dispatch -- must not regress by default."""
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            cmd = sim_command(n_seeds=2, n_steps=600, chunk=60)
        assert "run_phase0_xarray_ensemble.py" in cmd
        assert "run_batch_baseline_ray.py" not in cmd

    def test_sim_command_routes_to_batch_baseline_when_multi_generation_requested(self) -> None:
        """config.generations > 1 must route to the real multi-generation
        LineageProcess/batch_baseline_runner pipeline, dispatched as a registered
        process-bigraph composite through the generic run_pbg.py runner -- not a
        v2ecoli-specific CLI script (backlog items 26/27), and not the
        single-generation script that silently ignores generation count."""
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            cmd = sim_command(
                n_seeds=2,
                n_steps=600,
                chunk=60,
                n_generations=3,
                experiment_id="sim47-real-experiment",
                runner_s3_uri="s3://mybucket/vecoli-output/sim47-real-experiment/run_pbg.py",
            )
        assert "run_batch_baseline_ray.py" not in cmd
        assert "run_phase0_xarray_ensemble.py" not in cmd
        assert "aws s3 cp s3://mybucket/vecoli-output/sim47-real-experiment/run_pbg.py /tmp/run_pbg.py" in cmd
        assert "python /tmp/run_pbg.py" in cmd
        # Exact match, not a substring check. Verified directly against the deployed
        # sms-ecoli image (commit c44b69a, build 63) -- v2ecoli #373 folded the old
        # standalone batch_baseline composite into ecoli_baseline.py's baseline()
        # (backlog item 55; the old id now fails loudly instead of silently drifting,
        # by the composite registry's own deliberate design -- see the constant's
        # own comment in simulation_service_ray.py for the full incident history).
        assert "--composite-id v2ecoli.composites.ecoli_baseline.ecoli_baseline " in cmd
        assert "PBG_CORE_BUILDER=v2ecoli.core:build_core" in cmd
        # Effect guard (#395 / #375 §3e): the generation command carries the min-global-time
        # floor so a one-tick collapse fails loud instead of reporting success.
        assert "PBG_MIN_GLOBAL_TIME=" in cmd
        # PYTHONPATH=V2ECOLI_DIR (backlog item 93): ecoli_baseline.baseline()'s
        # injection branch does `from scripts._compare.inject import (...)`, a bare
        # absolute import that only resolves with the repo root on sys.path --
        # `python /tmp/run_pbg.py` alone puts /tmp there instead, regardless of cwd.
        assert "PYTHONPATH=/app/v2ecoli" in cmd
        assert "-n 1" in cmd

        # The overrides are a real, single-quoted JSON blob -- unpack it via shlex to
        # assert on structured content rather than substring-matching a hand-escaped string.
        tokens = shlex.split(cmd)
        overrides = json.loads(tokens[tokens.index("--overrides") + 1])
        assert overrides == {
            "n_seeds": 2,
            "n_generations": 3,
            "cache_dir": PARCA_CACHE_DIR,
            "out_dir": SIM_OUT_DIR,
            "experiment_id": "sim47-real-experiment",
            "analyses": "none",
            "parallel": "ray",
        }

    def test_sim_command_batch_threads_injected_processes_swap(self) -> None:
        """CD2 native seam: a config carrying swap_processes must reach the
        --composite-id batch overrides as ecoli_baseline.baseline()'s own
        injected_processes kwarg, or the composite runs plain basal despite the
        requested metabolism-redux/violacein swap (depends on v2ecoli #640)."""
        injected = {
            "swap_processes": {"ecoli-metabolism": "ecoli-metabolism-redux"},
            "add_processes": [],
            "exclude_processes": [],
            "fork_repo": "",
        }
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            cmd = sim_command(
                n_seeds=2,
                n_steps=600,
                chunk=60,
                n_generations=3,
                experiment_id="cd2-swap",
                runner_s3_uri="s3://b/cd2-swap/run_pbg.py",
                injected_processes=injected,
            )
        tokens = shlex.split(cmd)
        overrides = json.loads(tokens[tokens.index("--overrides") + 1])
        assert overrides["injected_processes"] == injected
        # The swap survived into the native composite's own kwarg shape.
        assert overrides["injected_processes"]["swap_processes"] == {"ecoli-metabolism": "ecoli-metabolism-redux"}

    def test_sim_command_batch_threads_all_domain_fields(self) -> None:
        """variants/config_overrides/features/exchange_fluxes(+basis) are the
        remaining ecoli_baseline batch-mode kwargs -- each must reach --overrides
        when the config carries it."""
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            cmd = sim_command(
                n_seeds=1,
                n_steps=600,
                chunk=60,
                n_generations=2,
                experiment_id="cd2-full",
                runner_s3_uri="s3://b/cd2-full/run_pbg.py",
                variants={"grid": {"a": {"p.k": [1.0]}}},
                config_overrides={"ecoli-metabolism-redux.foo": 1},
                features=["exchange_flux"],
                exchange_fluxes={"GLC": "EX_glc__D_e"},
                exchange_flux_basis="mmol_per_gDCW_per_hr",
            )
        overrides = json.loads(shlex.split(cmd)[shlex.split(cmd).index("--overrides") + 1])
        assert overrides["variants"] == {"grid": {"a": {"p.k": [1.0]}}}
        assert overrides["config_overrides"] == {"ecoli-metabolism-redux.foo": 1}
        assert overrides["features"] == ["exchange_flux"]
        assert overrides["exchange_fluxes"] == {"GLC": "EX_glc__D_e"}
        assert overrides["exchange_flux_basis"] == "mmol_per_gDCW_per_hr"

    def test_sim_command_batch_no_domain_fields_is_byte_for_byte_unchanged(self) -> None:
        """Regression guard: a config with no swap/variant intent produces the
        exact overrides dict this path built before threading was added -- no
        stray domain keys leak in."""
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            cmd = sim_command(
                n_seeds=2,
                n_steps=600,
                chunk=60,
                n_generations=3,
                experiment_id="plain",
                runner_s3_uri="s3://b/plain/run_pbg.py",
            )
        overrides = json.loads(shlex.split(cmd)[shlex.split(cmd).index("--overrides") + 1])
        assert overrides == {
            "n_seeds": 2,
            "n_generations": 3,
            "cache_dir": PARCA_CACHE_DIR,
            "out_dir": SIM_OUT_DIR,
            "experiment_id": "plain",
            "analyses": "none",
            "parallel": "ray",
        }

    def test_sim_command_batch_flux_basis_omitted_without_flux_map(self) -> None:
        """exchange_flux_basis only matters alongside a flux map -- it is omitted
        when no exchange_fluxes are supplied (composite defaults it to '')."""
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            cmd = sim_command(
                n_seeds=1,
                n_steps=600,
                chunk=60,
                n_generations=2,
                experiment_id="no-flux",
                runner_s3_uri="s3://b/no-flux/run_pbg.py",
                exchange_flux_basis="mmol_per_gDCW_per_hr",
            )
        overrides = json.loads(shlex.split(cmd)[shlex.split(cmd).index("--overrides") + 1])
        assert "exchange_flux_basis" not in overrides
        assert "exchange_fluxes" not in overrides

    def test_sim_command_multi_generation_requires_experiment_id_and_runner_uri(self) -> None:
        """No silent placeholder default -- both must be supplied explicitly or the
        dispatch fails loudly instead of running against the wrong experiment_id."""
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            with pytest.raises(RuntimeError, match="experiment_id"):
                sim_command(n_seeds=2, n_steps=600, chunk=60, n_generations=3, runner_s3_uri="s3://x/y.py")
            with pytest.raises(RuntimeError, match="runner_s3_uri"):
                sim_command(n_seeds=2, n_steps=600, chunk=60, n_generations=3, experiment_id="exp-1")

    def test_sim_command_composite_takes_precedence_over_n_generations(self) -> None:
        """The comparison driver's own --max-generations flag is a separate knob
        from plain n_generations -- composite selection wins regardless."""
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            cmd = sim_command(n_seeds=1, n_steps=10, chunk=4, composite="v2ecoli", n_generations=3)
        assert "run_comparison_ensemble.py" in cmd
        assert "run_batch_baseline_ray.py" not in cmd

    def test_sim_command_vecoli_source_only_appended_for_upstream_vecoli(self) -> None:
        """--vecoli-source is meaningful only for --composite vecoli."""
        with patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings):
            vecoli = sim_command(n_seeds=1, n_steps=10, chunk=4, composite="vecoli", vecoli_source="vivarium-process")
            v2ecoli = sim_command(n_seeds=1, n_steps=10, chunk=4, composite="v2ecoli", vecoli_source="vivarium-process")
        assert "--vecoli-source vivarium-process" in vecoli
        # v2ecoli engine ignores vecoli_source (guarded by _is_upstream_vecoli)
        assert "--vecoli-source" not in v2ecoli
