"""data_layout is the single owner of S3 sim-data paths. These tests lock the
exact layout for both backends so the writer / reader / downloader can't drift
apart again (#152/#153) — including the load-bearing single-nested (Ray) vs
double-nested (Nextflow download) asymmetry — and lock the backend->layout
binding (layout_for)."""

import pytest

from viva_api.common.storage.data_layout import NextflowLayout, RayLayout, layout_for
from viva_api.config import ComputeBackend, get_settings


@pytest.fixture(autouse=True)
def _s3_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = get_settings()
    monkeypatch.setattr(settings, "s3_work_bucket", "my-bucket")
    monkeypatch.setattr(settings, "s3_output_prefix", "vecoli-output")


class TestRayLayout:
    def test_seed_store_uri(self) -> None:
        assert RayLayout.seed_store_uri("exp-abc", 0) == "s3://my-bucket/vecoli-output/exp-abc/v2ecoli_seed00.zarr"

    def test_seed_store_uri_zero_pads_seed(self) -> None:
        assert RayLayout.seed_store_uri("exp", 7).endswith("/exp/v2ecoli_seed07.zarr")

    def test_results_uri_is_single_nested_with_trailing_slash(self) -> None:
        assert RayLayout.results_uri("exp") == "s3://my-bucket/vecoli-output/exp/"

    def test_experiment_prefix_single_nested(self) -> None:
        assert RayLayout.experiment_prefix("exp") == "vecoli-output/exp"

    def test_summary_key(self) -> None:
        assert RayLayout.summary_key("exp") == "vecoli-output/exp/summary.json"

    def test_seed_store_lives_under_results_prefix(self) -> None:
        # The reader's store path must sit under the writer's results dir.
        assert RayLayout.seed_store_uri("exp", 3).startswith(RayLayout.results_uri("exp"))

    def test_parca_cache_v2ecoli(self) -> None:
        assert RayLayout.parca_cache_uri("abc123") == "s3://my-bucket/ray-parca-cache/abc123/"

    def test_parca_cache_upstream_is_distinct(self) -> None:
        assert RayLayout.parca_cache_uri("abc123", upstream=True) == "s3://my-bucket/ray-upstream-parca-cache/abc123/"
        assert RayLayout.parca_cache_uri("abc123", upstream=True) != RayLayout.parca_cache_uri("abc123")

    def test_parca_cache_no_variant_is_byte_identical_to_before(self) -> None:
        """Regression: item 87 added an optional `variant` param. Every existing
        caller passes nothing and MUST get the exact same commit-only key."""
        assert RayLayout.parca_cache_uri("abc123") == "s3://my-bucket/ray-parca-cache/abc123/"
        assert RayLayout.parca_cache_uri("abc123", upstream=True) == "s3://my-bucket/ray-upstream-parca-cache/abc123/"

    def test_parca_cache_variant_never_collides_with_the_bare_commit_path(self) -> None:
        """The real bug this exists to prevent: a config-driven (e.g. a custom
        strain's) ParCa build must never land at the same key a plain baseline
        build/stage uses, or it silently corrupts every other concurrent
        dispatch on that commit."""
        bare = RayLayout.parca_cache_uri("abc123", upstream=True)
        variant = RayLayout.parca_cache_uri("abc123", upstream=True, variant="custom-strain")
        assert variant != bare
        assert not variant.startswith(bare.rstrip("/") + "x")  # not a naive string collision either
        assert variant == "s3://my-bucket/ray-upstream-parca-cache/abc123/custom-strain/"

    def test_parca_cache_variant_still_scoped_to_its_own_commit(self) -> None:
        """A variant cache for one commit must not collide with the SAME variant
        on a different commit -- still commit-scoped, just also variant-scoped."""
        v1 = RayLayout.parca_cache_uri("commit1", upstream=True, variant="custom-strain")
        v2 = RayLayout.parca_cache_uri("commit2", upstream=True, variant="custom-strain")
        assert v1 != v2

    def test_daughter_state_uri(self) -> None:
        assert (
            RayLayout.daughter_state_uri("exp-abc", 4, 2)
            == "s3://my-bucket/vecoli-output/exp-abc/daughter-state/seed4/gen2.pkl"
        )

    def test_daughter_state_uri_generation_zero(self) -> None:
        assert (
            RayLayout.daughter_state_uri("exp", 0, 0)
            == "s3://my-bucket/vecoli-output/exp/daughter-state/seed0/gen0.pkl"
        )

    def test_daughter_state_prefix_lives_under_experiment_prefix(self) -> None:
        assert RayLayout.daughter_state_prefix("exp").startswith(RayLayout.experiment_prefix("exp"))

    def test_daughter_state_uri_distinct_per_seed_and_generation(self) -> None:
        base = RayLayout.daughter_state_uri("exp", 1, 0)
        assert RayLayout.daughter_state_uri("exp", 2, 0) != base  # different seed
        assert RayLayout.daughter_state_uri("exp", 1, 1) != base  # different generation


class TestNextflowLayout:
    def test_output_uri_is_single_nested(self) -> None:
        assert NextflowLayout.output_uri("exp") == "s3://my-bucket/vecoli-output/exp"

    def test_download_prefix_is_double_nested(self) -> None:
        assert NextflowLayout.experiment_prefix("exp") == "vecoli-output/exp/exp"

    def test_ray_and_nextflow_prefixes_differ(self) -> None:
        # The whole point: Ray reads single-nested, Nextflow download reads double.
        assert RayLayout.experiment_prefix("exp") != NextflowLayout.experiment_prefix("exp")


class TestLayoutFor:
    def test_ray_backend(self) -> None:
        assert layout_for(ComputeBackend.RAY) is RayLayout

    def test_batch_and_slurm_backends(self) -> None:
        assert layout_for(ComputeBackend.BATCH) is NextflowLayout
        assert layout_for(ComputeBackend.SLURM) is NextflowLayout

    def test_seed_store_is_ray_only(self) -> None:
        # NextflowLayout has no per-seed zarr store — the type/namespace enforces it.
        assert not hasattr(NextflowLayout, "seed_store_uri")

    def test_unknown_backend_raises(self) -> None:
        with pytest.raises(ValueError, match="no data layout"):
            layout_for("bogus")  # type: ignore[arg-type]
