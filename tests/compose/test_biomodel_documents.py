"""Unit tests for viva_api/compose/biomodel_documents.py."""

from viva_api.compose.biomodel_documents import (
    COPASI_STEP_ADDRESS,
    TELLURIUM_STEP_ADDRESS,
    TYPES_DICT,
    make_biomodel_document,
    make_multi_biomodel_document,
    make_utc_step_state,
)
from viva_api.compose.biomodels_service import UniformTimeCourseSpec


def _utc() -> UniformTimeCourseSpec:
    return UniformTimeCourseSpec(
        initial_time=0.0,
        output_start_time=0.0,
        output_end_time=10.0,
        number_of_points=100,
    )


class TestTypesDict:
    def test_has_numeric_result(self) -> None:
        assert "numeric_result" in TYPES_DICT
        assert "time" in TYPES_DICT["numeric_result"]

    def test_has_result(self) -> None:
        assert "result" in TYPES_DICT

    def test_has_results(self) -> None:
        assert "results" in TYPES_DICT


class TestMakeUtcStepState:
    def test_structure(self) -> None:
        state = make_utc_step_state(
            step_name="BIOMD001_copasi",
            step_address=COPASI_STEP_ADDRESS,
            sbml_path="/tmp/model.sbml",  # noqa: S108
            utc=_utc(),
        )
        key = "BIOMD001_copasi_step"
        assert key in state
        step = state[key]
        assert step["_type"] == "step"
        assert step["address"] == COPASI_STEP_ADDRESS
        assert step["config"]["model_source"] == "/tmp/model.sbml"  # noqa: S108
        assert step["config"]["time"] == 10.0
        assert step["config"]["n_points"] == 100

    def test_inputs_wired_to_stores(self) -> None:
        state = make_utc_step_state("m_copasi", COPASI_STEP_ADDRESS, "/x.sbml", _utc())
        step = state["m_copasi_step"]
        assert step["inputs"]["species_concentrations"] == ["species_concentrations"]
        assert step["inputs"]["species_counts"] == ["species_counts"]

    def test_output_wired_to_results(self) -> None:
        state = make_utc_step_state("m_copasi", COPASI_STEP_ADDRESS, "/x.sbml", _utc())
        step = state["m_copasi_step"]
        assert step["outputs"]["result"] == ["results", "m_copasi"]

    def test_utc_duration_used(self) -> None:
        utc = UniformTimeCourseSpec(initial_time=0.0, output_start_time=2.0, output_end_time=12.0, number_of_points=50)
        state = make_utc_step_state("m", COPASI_STEP_ADDRESS, "/x.sbml", utc)
        assert state["m_step"]["config"]["time"] == 10.0  # duration = 12 - 2


class TestMakeBiomodelDocument:
    def test_single_simulator_structure(self) -> None:
        doc = make_biomodel_document(
            biomodel_id="BIOMD001",
            sbml_path="/tmp/BIOMD001.sbml",  # noqa: S108
            utc=_utc(),
            steps={"copasi": COPASI_STEP_ADDRESS},
        )
        assert "schema" in doc
        assert "state" in doc

    def test_schema_store_types(self) -> None:
        doc = make_biomodel_document("BIOMD001", "/x.sbml", _utc(), {"copasi": COPASI_STEP_ADDRESS})
        schema = doc["schema"]
        assert schema["species_concentrations"] == "map[float]"
        assert schema["species_counts"] == "map[float]"
        assert schema["results"] == "map[numeric_result]"

    def test_state_has_stores(self) -> None:
        doc = make_biomodel_document("BIOMD001", "/x.sbml", _utc(), {"copasi": COPASI_STEP_ADDRESS})
        state = doc["state"]
        assert "species_concentrations" in state
        assert "species_counts" in state
        assert "results" in state

    def test_single_simulator_step_present(self) -> None:
        doc = make_biomodel_document("BIOMD001", "/x.sbml", _utc(), {"copasi": COPASI_STEP_ADDRESS})
        step_key = "BIOMD001_copasi_step"
        assert step_key in doc["state"]

    def test_dual_simulator_both_steps_present(self) -> None:
        doc = make_biomodel_document(
            "BIOMD001",
            "/x.sbml",
            _utc(),
            {"copasi": COPASI_STEP_ADDRESS, "tellurium": TELLURIUM_STEP_ADDRESS},
        )
        state = doc["state"]
        assert "BIOMD001_copasi_step" in state
        assert "BIOMD001_tellurium_step" in state

    def test_dual_simulator_addresses(self) -> None:
        doc = make_biomodel_document(
            "BIOMD001",
            "/x.sbml",
            _utc(),
            {"copasi": COPASI_STEP_ADDRESS, "tellurium": TELLURIUM_STEP_ADDRESS},
        )
        state = doc["state"]
        assert state["BIOMD001_copasi_step"]["address"] == COPASI_STEP_ADDRESS
        assert state["BIOMD001_tellurium_step"]["address"] == TELLURIUM_STEP_ADDRESS

    def test_step_key_uses_biomodel_id(self) -> None:
        doc = make_biomodel_document("MY_MODEL_42", "/x.sbml", _utc(), {"copasi": COPASI_STEP_ADDRESS})
        assert "MY_MODEL_42_copasi_step" in doc["state"]

    def test_serialisable_to_json(self) -> None:
        import json

        doc = make_biomodel_document(
            "BIOMD001",
            "/x.sbml",
            _utc(),
            {"copasi": COPASI_STEP_ADDRESS, "tellurium": TELLURIUM_STEP_ADDRESS},
        )
        dumped = json.dumps(doc)
        loaded = json.loads(dumped)
        assert loaded["schema"]["results"] == "map[numeric_result]"


class TestMakeMultiBiomodelDocument:
    def test_multiple_models_present(self) -> None:
        info = [
            {
                "biomodel_id": "BIOMD001",
                "sbml_path": "/a.sbml",
                "utc": _utc(),
                "steps": {"copasi": COPASI_STEP_ADDRESS},
            },
            {
                "biomodel_id": "BIOMD002",
                "sbml_path": "/b.sbml",
                "utc": _utc(),
                "steps": {"copasi": COPASI_STEP_ADDRESS},
            },
        ]
        doc = make_multi_biomodel_document(info)
        assert "BIOMD001" in doc["state"]
        assert "BIOMD002" in doc["state"]
        assert "BIOMD001" in doc["schema"]
        assert "BIOMD002" in doc["schema"]

    def test_each_model_has_step(self) -> None:
        info = [
            {"biomodel_id": "A", "sbml_path": "/a.sbml", "utc": _utc(), "steps": {"tellurium": TELLURIUM_STEP_ADDRESS}},
        ]
        doc = make_multi_biomodel_document(info)
        assert "A_tellurium_step" in doc["state"]["A"]

    def test_empty_list_returns_empty_doc(self) -> None:
        doc = make_multi_biomodel_document([])
        assert doc == {"state": {}, "schema": {}}
