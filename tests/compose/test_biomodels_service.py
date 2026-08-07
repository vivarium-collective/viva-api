"""Unit tests for viva_api/compose/biomodels_service.py (EBI calls mocked)."""

from unittest.mock import MagicMock, patch

import pytest

from viva_api.compose.biomodels_service import (
    BiomodelLoadResult,
    BiomodelsService,
    UniformTimeCourseSpec,
    _extract_first_utc,
    _file_name,
    _find_first_sbml,
    _find_first_sedml,
    _iter_entry_files,
)

# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------


class TestFileName:
    def test_named_object(self) -> None:
        obj = MagicMock()
        obj.name = "model.sbml"
        assert _file_name(obj) == "model.sbml"

    def test_string_fallback(self) -> None:
        assert _file_name("path/to/file.sedml") == "path/to/file.sedml"


class TestIterEntryFiles:
    def test_list(self) -> None:
        files = [MagicMock(), MagicMock()]
        assert _iter_entry_files(files) == files

    def test_dict_files_key(self) -> None:
        f1 = MagicMock()
        result = _iter_entry_files({"files": [f1]})
        assert result == [f1]

    def test_dict_main_files_key(self) -> None:
        f1 = MagicMock()
        result = _iter_entry_files({"main_files": [f1]})
        assert result == [f1]

    def test_none_returns_empty(self) -> None:
        assert _iter_entry_files(None) == []

    def test_non_iterable_returns_empty(self) -> None:
        assert _iter_entry_files(42) == []


class TestFindFirstSedml:
    def _make_file(self, name: str) -> MagicMock:
        f = MagicMock()
        f.name = name
        return f

    def test_finds_sedml(self) -> None:
        files = [self._make_file("model.sbml"), self._make_file("sim.sedml")]
        found = _find_first_sedml(files)
        assert found is not None
        assert found.name == "sim.sedml"

    def test_returns_none_if_absent(self) -> None:
        files = [self._make_file("model.sbml")]
        assert _find_first_sedml(files) is None


class TestFindFirstSbml:
    def _make_file(self, name: str) -> MagicMock:
        f = MagicMock()
        f.name = name
        return f

    def test_finds_sbml(self) -> None:
        files = [self._make_file("sim.sedml"), self._make_file("model.sbml")]
        found = _find_first_sbml(files)
        assert found is not None
        assert found.name == "model.sbml"

    def test_skips_sedml(self) -> None:
        files = [self._make_file("only.sedml")]
        assert _find_first_sbml(files) is None


# ---------------------------------------------------------------------------
# _extract_first_utc
# ---------------------------------------------------------------------------


def _make_sed_doc(
    initial_time: float = 0.0,
    output_start: float = 0.0,
    output_end: float = 10.0,
    n_points: int = 100,
) -> MagicMock:
    sim = MagicMock()
    sim.getInitialTime.return_value = initial_time
    sim.getOutputStartTime.return_value = output_start
    sim.getOutputEndTime.return_value = output_end
    sim.getNumberOfPoints.return_value = n_points

    doc = MagicMock()
    doc.getNumSimulations.return_value = 1
    doc.getSimulation.return_value = sim
    return doc


class TestExtractFirstUtc:
    def test_basic(self) -> None:
        doc = _make_sed_doc(output_end=5.0, n_points=50)
        utc = _extract_first_utc(doc)
        assert utc.output_end_time == 5.0
        assert utc.number_of_points == 50

    def test_duration_property(self) -> None:
        doc = _make_sed_doc(output_start=2.0, output_end=12.0)
        utc = _extract_first_utc(doc)
        assert utc.duration == pytest.approx(10.0)

    def test_raises_if_no_simulations(self) -> None:
        doc = MagicMock()
        doc.getNumSimulations.return_value = 0
        with pytest.raises(ValueError, match="No UniformTimeCourse"):
            _extract_first_utc(doc)

    def test_raises_if_simulation_missing_attrs(self) -> None:
        sim = MagicMock(spec=[])  # no attributes
        doc = MagicMock()
        doc.getNumSimulations.return_value = 1
        doc.getSimulation.return_value = sim
        with pytest.raises(ValueError, match="No UniformTimeCourse"):
            _extract_first_utc(doc)


# ---------------------------------------------------------------------------
# BiomodelsService (EBI mocked)
# ---------------------------------------------------------------------------


class TestBiomodelsServiceGetIdentifiers:
    def test_returns_list(self) -> None:
        mock_biomodels = MagicMock()
        mock_biomodels.get_all_identifiers.return_value = ["BIOMD001", "BIOMD002", "BIOMD003"]
        with patch.dict("sys.modules", {"biomodels": mock_biomodels}):
            ids = BiomodelsService.get_identifiers(n=2)
        assert ids == ["BIOMD001", "BIOMD002"]

    def test_no_limit(self) -> None:
        mock_biomodels = MagicMock()
        mock_biomodels.get_all_identifiers.return_value = ["A", "B", "C"]
        with patch.dict("sys.modules", {"biomodels": mock_biomodels}):
            ids = BiomodelsService.get_identifiers()
        assert ids == ["A", "B", "C"]


class TestBiomodelsServiceGetMetadata:
    def test_dict_passthrough(self) -> None:
        mock_biomodels = MagicMock()
        mock_biomodels.get_metadata.return_value = {"name": "Hodgkin-Huxley"}
        with patch.dict("sys.modules", {"biomodels": mock_biomodels}):
            meta = BiomodelsService.get_metadata("BIOMD001")
        assert meta["name"] == "Hodgkin-Huxley"

    def test_pydantic_model_dump(self) -> None:
        mock_meta = MagicMock()
        mock_meta.model_dump.return_value = {"id": "BIOMD001"}
        del mock_meta.dict  # ensure model_dump branch
        mock_biomodels = MagicMock()
        mock_biomodels.get_metadata.return_value = mock_meta
        with patch.dict("sys.modules", {"biomodels": mock_biomodels}):
            meta = BiomodelsService.get_metadata("BIOMD001")
        assert meta["id"] == "BIOMD001"


class TestUniformTimeCourseSpec:
    def test_duration(self) -> None:
        utc = UniformTimeCourseSpec(initial_time=0.0, output_start_time=1.0, output_end_time=11.0, number_of_points=100)
        assert utc.duration == pytest.approx(10.0)

    def test_frozen(self) -> None:
        utc = UniformTimeCourseSpec(0.0, 0.0, 5.0, 50)
        with pytest.raises(AttributeError):
            utc.initial_time = 1.0  # type: ignore[misc]


class TestBiomodelLoadResult:
    def test_fields(self) -> None:
        utc = UniformTimeCourseSpec(0.0, 0.0, 10.0, 100)
        res = BiomodelLoadResult(
            biomodel_id="BIOMD001",
            sbml_path="/tmp/model.sbml",  # noqa: S108
            sedml_path="/tmp/model.sedml",  # noqa: S108
            utc=utc,
        )
        assert res.biomodel_id == "BIOMD001"
        assert res.utc.duration == 10.0
