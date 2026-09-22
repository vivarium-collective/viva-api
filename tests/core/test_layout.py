"""The two output-location primitives (``viva_core/storage/layout.py``), and that SMS's layout is built
on them rather than beside them (P3d-4c-1)."""

from unittest.mock import patch

from viva_api.common.storage import data_layout
from viva_core.storage import layout


def test_the_primitives_are_pure_and_say_where_a_run_lives() -> None:
    assert layout.s3_uri("b", "k/x") == "s3://b/k/x"
    assert layout.experiment_prefix("out", "exp-1") == "out/exp-1"
    assert layout.results_uri("b", "out", "exp-1") == "s3://b/out/exp-1/"


def test_the_applications_layout_is_built_on_cores_primitives() -> None:
    """Same bucket, same prefix, same run directory -- from one place, through the application's own
    settings seam, which is what its tests patch."""
    with patch.object(data_layout, "get_settings") as settings:
        settings.return_value.s3_work_bucket = "b"
        settings.return_value.s3_output_prefix = "out"
        assert data_layout.RayLayout.experiment_prefix("exp-1") == layout.experiment_prefix("out", "exp-1")
        assert data_layout.RayLayout.results_uri("exp-1") == layout.results_uri("b", "out", "exp-1")
        assert data_layout.s3_uri("k") == layout.s3_uri("b", "k")
