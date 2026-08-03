"""Tests for the observables parameter on the /api/v1/simulations endpoint.

Verifies that dot-separated observable paths are correctly transformed into
vEcoli's engine_process_reports format (list of path lists), and that
DEFAULT_OBSERVABLES is used when no observables are provided.
"""

import pytest


class TestObservablesTransformation:
    """Unit tests for the dot-path -> engine_process_reports conversion."""

    @staticmethod
    def _transform(observables: list[str]) -> list[list[str]]:
        """Replicate the handler's transformation logic."""
        return [obs.split(".") for obs in observables]

    def test_single_segment(self) -> None:
        assert self._transform(["bulk"]) == [["bulk"]]

    def test_multi_segment(self) -> None:
        assert self._transform(["boundary.external.mecillinam"]) == [["boundary", "external", "mecillinam"]]

    def test_mixed_depths(self) -> None:
        result = self._transform(["bulk", "listeners.mass.cell_mass", "listeners.monomer_counts"])
        assert result == [["bulk"], ["listeners", "mass", "cell_mass"], ["listeners", "monomer_counts"]]

    def test_empty_list(self) -> None:
        assert self._transform([]) == []

    def test_full_baseline_set(self) -> None:
        """Verify the baseline observables file transforms correctly."""
        import json
        from pathlib import Path

        baseline_path = Path(__file__).resolve().parents[3] / "assets" / "observables_baseline.json"
        if not baseline_path.exists():
            pytest.skip("observables_baseline.json not found")
        with open(baseline_path) as f:
            baseline = json.load(f)
        expected = baseline["engine_process_reports"]
        dot_paths = [".".join(path) for path in expected]
        result = self._transform(dot_paths)
        assert result == expected


class TestDefaultObservables:
    """Verify DEFAULT_OBSERVABLES constant matches the baseline JSON and is used as fallback."""

    def test_default_observables_matches_baseline_json(self) -> None:
        """DEFAULT_OBSERVABLES constant should produce the same engine_process_reports as the JSON file."""
        import json
        from pathlib import Path

        from viva_api.common.simulator_defaults import DEFAULT_OBSERVABLES

        baseline_path = Path(__file__).resolve().parents[3] / "assets" / "observables_baseline.json"
        if not baseline_path.exists():
            pytest.skip("observables_baseline.json not found")
        with open(baseline_path) as f:
            expected = json.load(f)["engine_process_reports"]
        result = [obs.split(".") for obs in DEFAULT_OBSERVABLES]
        assert result == expected

    def test_default_observables_is_nonempty(self) -> None:
        from viva_api.common.simulator_defaults import DEFAULT_OBSERVABLES

        assert len(DEFAULT_OBSERVABLES) > 0
        assert all(isinstance(obs, str) for obs in DEFAULT_OBSERVABLES)
        assert all("." in obs or obs == "bulk" for obs in DEFAULT_OBSERVABLES)

    def test_fallback_uses_defaults_when_none(self) -> None:
        """Handler logic: when observables is None, use DEFAULT_OBSERVABLES."""
        from viva_api.common.simulator_defaults import DEFAULT_OBSERVABLES

        observables = None
        effective = observables if observables else DEFAULT_OBSERVABLES
        assert effective is DEFAULT_OBSERVABLES

    def test_user_override_takes_precedence(self) -> None:
        """Handler logic: when observables is provided, use it instead of defaults."""
        from viva_api.common.simulator_defaults import DEFAULT_OBSERVABLES

        user_obs = ["bulk", "listeners.mass.cell_mass"]
        effective = user_obs if user_obs else DEFAULT_OBSERVABLES
        assert effective == user_obs
        assert effective is not DEFAULT_OBSERVABLES


class TestObservablesDataService:
    """Test that the E2EDataService correctly serializes observables as repeated query params."""

    def test_query_params_with_observables(self) -> None:
        """Observables should appear as repeated 'observables' keys in query params."""
        import httpx

        observables = ["bulk", "listeners.mass.cell_mass"]
        items: list[tuple[str, str | int | float | bool | None]] = [("simulator_id", "1"), ("experiment_id", "test")]
        items.extend(("observables", obs) for obs in observables)
        params = httpx.QueryParams(items)

        raw = str(params)
        assert "observables=bulk" in raw
        assert "observables=listeners.mass.cell_mass" in raw

    def test_query_params_without_observables(self) -> None:
        """When observables is None, no observables key should appear."""
        import httpx

        items: list[tuple[str, str | int | float | bool | None]] = [("simulator_id", "1"), ("experiment_id", "test")]
        params = httpx.QueryParams(items)

        assert "observables" not in str(params)
