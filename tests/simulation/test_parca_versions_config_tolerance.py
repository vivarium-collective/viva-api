"""A legacy stored parca_config must not 500 the whole parca-versions list.

Same class as ``test_list_legacy_config_tolerance`` (which covers
``/api/v1/simulations``): ``ParcaOptions`` is intentionally ``extra="forbid"``,
but older parca datasets carry ``rnaseq_*`` keys the current schema now rejects
(e.g. ``rnaseq_manifest_path`` / ``rnaseq_basal_dataset_id``). The parca-versions
list path (``list_parca_datasets`` -> ``GET /core/v1/simulation/parca/versions``)
deserializes every dataset's stored ``parca_config``, so one bad row used to raise
``ValidationError`` and 500 the entire listing — hiding every parca dataset from
every client (dashboard/CLI). ``_lenient_parca_options`` degrades that single
record (strips the extra-forbidden keys, re-parses strictly) instead of failing
the list. Creation + the per-id detail path (``get_parca_dataset``) stay strict.
"""

import pytest
from pydantic import ValidationError

from viva_api.simulation.database_service import DatabaseServiceSQL
from viva_api.simulation.models import ParcaOptions


def test_parca_options_still_forbids_extra_on_creation() -> None:
    """Guard the intent: ParcaOptions stays strict so creation catches typos."""
    with pytest.raises(ValidationError):
        ParcaOptions(outdir="/x", rnaseq_manifest_path="$ECOLI_SOURCES/data/manifest.tsv")  # type: ignore[call-arg]


def test_lenient_parca_options_drops_legacy_rnaseq_keys() -> None:
    # Exactly the shape govcloud-workbench saw 500 the versions endpoint.
    legacy = {
        "outdir": "/x",
        "rnaseq_manifest_path": "$ECOLI_SOURCES/data/manifest.tsv",
        "rnaseq_basal_dataset_id": "vecoli_m9_glucose_minus_aas",
        "rnaseq_fill_missing_genes_from_ref": True,
    }
    opts = DatabaseServiceSQL._lenient_parca_options(legacy, dataset_id=238)
    assert isinstance(opts, ParcaOptions)
    assert opts.outdir == "/x"
    dumped = opts.model_dump()
    assert "rnaseq_manifest_path" not in dumped
    assert "rnaseq_basal_dataset_id" not in dumped
    assert "rnaseq_fill_missing_genes_from_ref" not in dumped


def test_lenient_parca_options_passes_a_clean_config_through() -> None:
    opts = DatabaseServiceSQL._lenient_parca_options({"outdir": "/y"}, dataset_id=1)
    assert isinstance(opts, ParcaOptions)
    assert opts.outdir == "/y"
