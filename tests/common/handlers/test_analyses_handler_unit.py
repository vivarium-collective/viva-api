from viva_api.common.handlers.analyses import _parse_cached_filename_metadata


class TestParseCachedFilenameMetadata:
    """Tests for parsing metadata from cached analysis output filenames."""

    def test_full_metadata(self) -> None:
        result = _parse_cached_filename_metadata("ptools_rna_v0_s2_g5.tsv")
        assert result == {"variant": 0, "lineage_seed": 2, "generation": 5}

    def test_variant_and_seed_only(self) -> None:
        result = _parse_cached_filename_metadata("ptools_rxns_v0_s1.tsv")
        assert result == {"variant": 0, "lineage_seed": 1}

    def test_variant_only(self) -> None:
        result = _parse_cached_filename_metadata("ptools_rna_v0.tsv")
        assert result == {"variant": 0}

    def test_no_metadata(self) -> None:
        result = _parse_cached_filename_metadata("ptools_rna.tsv")
        assert result == {}

    def test_csv_extension(self) -> None:
        result = _parse_cached_filename_metadata("ptools_rna_v0_s0_g3.csv")
        assert result == {"variant": 0, "lineage_seed": 0, "generation": 3}
