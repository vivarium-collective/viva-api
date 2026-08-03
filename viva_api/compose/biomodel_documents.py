"""Process-bigraph document factory for BioModels integration.

Ported from biomodels-regression/biomodels_regression/document_creation.py
and __init__.py (TYPES_DICT).
"""

from __future__ import annotations

from typing import Any

from viva_api.compose.biomodels_service import UniformTimeCourseSpec

# ---------------------------------------------------------------------------
# Type registry (ported from biomodels-regression/__init__.py)
# ---------------------------------------------------------------------------

TYPES_DICT: dict[str, Any] = {
    "numeric_result": {
        "time": "list[float]",
        "columns": "list[string]",
        "values": "list[list[float]]",
    },
    "numeric_results": "map[numeric_result]",
    "columns_of_interest": "list[string]",
    "result": {
        "time": "list[float]",
        "species_concentrations": "map[list[float]]",
    },
    "results": "map[result]",
}

# ---------------------------------------------------------------------------
# Simulator step addresses
# ---------------------------------------------------------------------------

COPASI_STEP_ADDRESS = "local:pbsim_common.simulators.copasi_process.CopasiUTCStep"
TELLURIUM_STEP_ADDRESS = "local:pbsim_common.simulators.tellurium_process.TelluriumUTCStep"

# ---------------------------------------------------------------------------
# Document factory functions
# ---------------------------------------------------------------------------


def make_utc_step_state(
    step_name: str,
    step_address: str,
    sbml_path: str,
    utc: UniformTimeCourseSpec,
) -> dict[str, Any]:
    """Build the state fragment for a single UTC simulator step."""
    return {
        f"{step_name}_step": {
            "_type": "step",
            "address": step_address,
            "config": {
                "model_source": sbml_path,
                "time": float(utc.duration),
                "n_points": int(utc.number_of_points),
            },
            "inputs": {
                "species_concentrations": ["species_concentrations"],
                "species_counts": ["species_counts"],
            },
            "outputs": {
                "result": ["results", step_name],
            },
        },
    }


def make_biomodel_document(
    biomodel_id: str,
    sbml_path: str,
    utc: UniformTimeCourseSpec,
    steps: dict[str, str],
) -> dict[str, Any]:
    """Build a process-bigraph document for one BioModel with one or more simulator steps.

    Args:
        biomodel_id: EBI BioModels identifier (e.g. ``BIOMD0000000001``).
        sbml_path: Absolute path to the SBML file on the local filesystem.
        utc: Parsed UniformTimeCourse spec from the model's SED-ML.
        steps: Mapping of engine name → step address.
            Single-simulator: ``{"copasi": COPASI_STEP_ADDRESS}``.
            Dual-simulator audit: ``{"copasi": COPASI_STEP_ADDRESS, "tellurium": TELLURIUM_STEP_ADDRESS}``.

    Returns:
        Dict with ``"schema"`` and ``"state"`` keys, serialisable to a ``.pbg``
        JSON file inside an OMEX archive.
    """
    state: dict[str, Any] = {
        "species_concentrations": {},
        "species_counts": {},
        "results": {},
    }
    schema: dict[str, Any] = {
        "species_concentrations": "map[float]",
        "species_counts": "map[float]",
        "results": "map[numeric_result]",
    }

    for engine_name, engine_address in steps.items():
        step_key = f"{biomodel_id}_{engine_name}"
        state.update(
            make_utc_step_state(
                step_name=step_key,
                step_address=engine_address,
                sbml_path=sbml_path,
                utc=utc,
            )
        )

    return {"schema": schema, "state": state}


def make_multi_biomodel_document(
    biomodel_info: list[dict[str, Any]],
) -> dict[str, Any]:
    """Build a combined process-bigraph document for multiple BioModels.

    Each entry in *biomodel_info* must have keys:
    ``biomodel_id``, ``sbml_path``, ``utc`` (UniformTimeCourseSpec), ``steps`` (dict).

    Returns:
        Dict with top-level ``"schema"`` and ``"state"`` sub-dicts keyed by ``biomodel_id``.
    """
    doc: dict[str, Any] = {"state": {}, "schema": {}}
    for biomodel in biomodel_info:
        single = make_biomodel_document(
            biomodel_id=biomodel["biomodel_id"],
            sbml_path=biomodel["sbml_path"],
            utc=biomodel["utc"],
            steps=biomodel["steps"],
        )
        doc["state"][biomodel["biomodel_id"]] = single["state"]
        doc["schema"][biomodel["biomodel_id"]] = single["schema"]
    return doc
